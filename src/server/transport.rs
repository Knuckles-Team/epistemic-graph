//! Wire transport: response framing, the per-connection loop (with backpressure
//! admission), and the UDS/TCP listeners. Routing/auth live in `dispatch`.
//!
//! ## Graceful shutdown (reference-counted)
//!
//! The accept loop `select!`s between accepting a new connection and a
//! [`tokio::sync::Notify`] "shutdown" signal. When the signal fires the loop
//! BREAKS and returns, so `main()` falls through to the persistence flush + final
//! checkpoint (`PersistenceBackend::shutdown()`), instead of looping forever.
//! The signal is fired by any of:
//!   * a SIGTERM/SIGINT handler (a supervisor / `kill` is a clean checkpointed stop);
//!   * the optional idle watcher (`--idle-shutdown-secs N`, N>0) once the active
//!     connection count has been 0 continuously for N seconds.
//!
//! [`ShutdownCoordinator`] holds the `Notify` plus an [`AtomicUsize`] active-
//! connection counter that `handle_connection` increments on entry and decrements
//! on return (RAII via [`ConnGuard`]) — that count is what the idle watcher polls.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::{Notify, RwLock, Semaphore};
use tracing::{error, info};

use super::dispatch::dispatch_verified_request;
use super::{dispatch, ServerState};
use crate::protocol::{Method, Request, Response};

const DEFAULT_MAX_REQUEST_FRAME_BYTES: usize = 64 * 1024 * 1024;
const HARD_MAX_REQUEST_FRAME_BYTES: usize = 384 * 1024 * 1024;
const DEFAULT_MAX_RESPONSE_FRAME_BYTES: usize = 64 * 1024 * 1024;
const HARD_MAX_RESPONSE_FRAME_BYTES: usize = 384 * 1024 * 1024;
const DEFAULT_MAX_MSGPACK_ITEMS: usize = 1_000_000;
const HARD_MAX_MSGPACK_ITEMS: usize = 4_000_000;
const MAX_MSGPACK_NESTING_DEPTH: usize = 64;
const DEFAULT_CONNECTION_IO_TIMEOUT_SECS: u64 = 120;
const DEFAULT_TLS_HANDSHAKE_TIMEOUT_SECS: u64 = 10;
/// Hard ceiling on ONE dispatch, after which its admission permits are released and
/// the client is answered with an error (see [`dispatch_within_deadline`]). Sized ~20x
/// the widest dispatch-latency bucket the server records (30 s), so it can only ever
/// fire on work that is genuinely stuck, never on a slow-but-live request.
const DEFAULT_DISPATCH_DEADLINE_SECS: u64 = 600;

/// Runtime-only TLS material for the native TCP service. Certificate contents
/// are never copied into engine configuration or logs. Supplying
/// `client_ca_path` enables mutual TLS and requires a valid client certificate.
#[derive(Clone, Debug)]
pub struct TcpTlsConfig {
    pub cert_path: String,
    pub key_path: String,
    pub client_ca_path: Option<String>,
}

#[cfg(feature = "server-tls")]
fn tls_acceptor(config: &TcpTlsConfig) -> std::io::Result<tokio_rustls::TlsAcceptor> {
    use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
    use std::io::{BufReader, Error, ErrorKind};
    use std::sync::Arc;

    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert_file = std::fs::File::open(&config.cert_path).map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            "server TLS certificate unavailable",
        )
    })?;
    let certs = CertificateDer::pem_reader_iter(BufReader::new(cert_file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "server TLS certificate invalid"))?;
    if certs.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "server TLS certificate invalid",
        ));
    }
    let key_file = std::fs::File::open(&config.key_path).map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            "server TLS private key unavailable",
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = key_file
            .metadata()
            .map_err(|_| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "server TLS private key unavailable",
                )
            })?
            .permissions()
            .mode();
        if mode & 0o077 != 0 {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "server TLS private key permissions are too broad",
            ));
        }
    }
    let key = PrivateKeyDer::from_pem_reader(BufReader::new(key_file))
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "server TLS private key invalid"))?;

    let builder = rustls::ServerConfig::builder();
    let server_config = if let Some(client_ca_path) = &config.client_ca_path {
        let ca_file = std::fs::File::open(client_ca_path)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "client CA bundle unavailable"))?;
        let mut roots = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_reader_iter(BufReader::new(ca_file)) {
            let cert =
                cert.map_err(|_| Error::new(ErrorKind::InvalidInput, "client CA bundle invalid"))?;
            roots
                .add(cert)
                .map_err(|_| Error::new(ErrorKind::InvalidInput, "client CA bundle invalid"))?;
        }
        if roots.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "client CA bundle invalid",
            ));
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "client CA bundle invalid"))?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
    } else {
        builder.with_no_client_auth().with_single_cert(certs, key)
    }
    .map_err(|_| Error::new(ErrorKind::InvalidInput, "server TLS identity invalid"))?;

    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(server_config)))
}

#[cfg(not(feature = "server-tls"))]
fn tls_acceptor(_config: &TcpTlsConfig) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "native TCP TLS is unavailable in this build",
    ))
}

/// Validate configured native-TCP identity before background listeners spawn.
/// This makes missing/invalid TLS material a startup failure rather than leaving
/// an otherwise healthy UDS process with a silently absent remote listener.
pub fn validate_tcp_tls_config(config: &TcpTlsConfig) -> std::io::Result<()> {
    tls_acceptor(config).map(|_| ())
}

/// Coordinates reference-counted graceful shutdown across the listeners and the
/// per-connection tasks. Shared via `Arc`. `active` is the live connection count
/// (the refcount); `requested` latches the shutdown decision; `notify` wakes an
/// accept loop parked in `accept()` so it re-checks the latch promptly.
#[derive(Debug, Default)]
pub struct ShutdownCoordinator {
    /// Live (currently-handled) connection count — the reference count the idle
    /// watcher observes. Incremented on accept, decremented when the connection's
    /// `handle_connection` returns.
    active: AtomicUsize,
    /// Latched "shutdown requested" flag. Checked at the TOP of every accept-loop
    /// iteration, so a `trigger()` that fires BETWEEN iterations (after one select
    /// completed, before the next `notified()` is armed) is never missed — the
    /// latch persists, unlike a bare `Notify` edge.
    requested: AtomicBool,
    /// Edge-triggered wake so an accept loop currently parked in `accept()` returns
    /// to the top of the loop (where it reads `requested`) without waiting for a new
    /// connection. The latch — not this edge — is the source of truth.
    notify: Notify,
}

impl ShutdownCoordinator {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Current number of in-flight connections (the reference count).
    pub fn active_connections(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    /// True once shutdown has been triggered (latched).
    fn is_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }

    /// Fire the graceful-shutdown signal. Idempotent — latches `requested` and wakes
    /// any parked accept loop so it breaks. Extra calls are harmless.
    pub fn trigger(&self) {
        self.requested.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Future that resolves when shutdown is triggered. Armed fresh each accept-loop
    /// iteration; the loop also re-checks the latch at the top, so an edge missed
    /// between iterations is caught by the latch on the next pass.
    fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }
}

/// RAII guard: increments the active-connection count on creation and decrements
/// it on drop, so the refcount is correct even if `handle_connection` returns via
/// an early `break`/`?` or a panic unwinds the task.
struct ConnGuard {
    coord: Arc<ShutdownCoordinator>,
}

impl ConnGuard {
    fn new(coord: Arc<ShutdownCoordinator>) -> Self {
        coord.active.fetch_add(1, Ordering::SeqCst);
        Self { coord }
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.coord.active.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Watch the active-connection count and fire the shutdown signal once it has been
/// 0 continuously for `idle_secs` seconds (CONCEPT:EG-KG.backend.tiny-shared — the tiny shared
/// daemon self-terminates a grace period after its last client disconnects, the
/// auto-bundled-engine mode agent-utilities' EngineResolver opts into by passing
/// `--idle-shutdown-secs`). A connection arriving DURING the grace period resets
/// the timer (the watcher re-observes a non-zero count and re-arms). Only spawned
/// when `idle_secs > 0`; absent/0 ⇒ no watcher, the engine runs forever
/// (long-living/persistent mode). The watcher polls once a second, so a
/// 1-second idle window is honored without busy-polling.
pub async fn run_idle_watcher(coord: Arc<ShutdownCoordinator>, idle_secs: u64) {
    let idle = std::time::Duration::from_secs(idle_secs);
    // Poll once a second: fine-grained enough to honor a 1s idle window, coarse
    // enough never to busy-poll for a long grace period.
    let poll = std::time::Duration::from_secs(1);
    // Instant the count was last observed at zero; None while a connection is live.
    // `tokio::time::Instant` (not std) so the watcher honors paused/virtual time in
    // tests and the real monotonic clock in production.
    let mut idle_since: Option<tokio::time::Instant> = None;
    loop {
        tokio::time::sleep(poll).await;
        let active = coord.active_connections();
        if active > 0 {
            // A connection is live → cancel any pending idle timer.
            idle_since = None;
            continue;
        }
        match idle_since {
            None => idle_since = Some(tokio::time::Instant::now()),
            Some(since) => {
                if since.elapsed() >= idle {
                    info!(
                        "Idle shutdown: no connections for {}s — triggering graceful shutdown",
                        idle_secs
                    );
                    coord.trigger();
                    return;
                }
            }
        }
    }
}

/// Serialize a response to a length-prefixable frame. On the (essentially
/// impossible) event that encoding fails, emit a VALID error frame rather than an
/// empty one — a 0-length frame would be read by the client as a zero-byte
/// response and desync the stream. Replaces a previous `unwrap_or_default()` that
/// silently produced exactly that empty frame.
fn encode_response(resp: &Response) -> Vec<u8> {
    match rmp_serde::to_vec_named(resp) {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::error!("response encode failed (id={}): {}", resp.id, e);
            rmp_serde::to_vec_named(&Response::err(
                resp.id,
                "internal: response serialization failed",
            ))
            .unwrap_or_default()
        }
    }
}

/// Serialize a [`Response`] to a complete, length-prefixed wire frame
/// (`4-byte big-endian len ++ MessagePack body`). The id-tagged response is what
/// the client demuxes by, so a frame can be written in ANY order relative to the
/// requests that produced it (CONCEPT:EG-KG.backend.framed-response).
fn encode_frame(resp: &Response) -> Vec<u8> {
    let body = encode_response(resp);
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn encode_bounded_frame(resp: &Response, max_frame_bytes: usize) -> Vec<u8> {
    let frame = encode_frame(resp);
    if frame.len().saturating_sub(4) <= max_frame_bytes {
        return frame;
    }
    encode_frame(&Response::err(
        resp.id,
        "response frame exceeds the configured resource limit",
    ))
}

/// Per-connection in-flight cap (CONCEPT:EG-KG.backend.framed-response). Bounds how many requests ONE
/// connection may have dispatching CONCURRENTLY, so a single client cannot spawn
/// unbounded server tasks/memory — the global `ServerState::max_in_flight`
/// semaphore remains the box-wide admission cap (which sheds `BUSY`). Auto-sized
/// from cores (no knob): a 1-2 core box still pipelines a useful depth (floor 64),
/// a big box can't let one connection hog everything (ceiling 1024).
fn per_connection_inflight_limit() -> usize {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    (cpus * 8).clamp(64, 1024)
}

/// Bound the allocation driven by an untrusted frame prefix. The hard ceiling is
/// large enough for the modality service's separately capped source + bundle
/// maximum, while the lower default protects ordinary deployments. Operators that
/// raise a modality limit must explicitly raise this transport limit too.
fn max_request_frame_bytes() -> usize {
    std::env::var("EPISTEMIC_GRAPH_MAX_REQUEST_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_REQUEST_FRAME_BYTES)
        .min(HARD_MAX_REQUEST_FRAME_BYTES)
}

fn max_response_frame_bytes() -> usize {
    std::env::var("EPISTEMIC_GRAPH_MAX_RESPONSE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_RESPONSE_FRAME_BYTES)
        .min(HARD_MAX_RESPONSE_FRAME_BYTES)
}

/// Bound the number of values/collection slots a MessagePack request may ask the
/// decoder to allocate. A frame-length cap alone is insufficient: a five-byte
/// `array32` header can declare billions of entries and some serde visitors use
/// that untrusted size hint for preallocation before noticing the body is absent.
fn max_msgpack_items() -> usize {
    std::env::var("EPISTEMIC_GRAPH_MAX_MSGPACK_ITEMS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_MSGPACK_ITEMS)
        .min(HARD_MAX_MSGPACK_ITEMS)
}

fn validate_msgpack_frame(input: &[u8], max_items: usize) -> Result<(), ()> {
    eg_types::msgpack::validate_single_value(
        input,
        eg_types::msgpack::MsgpackLimits::new(input.len(), max_items, MAX_MSGPACK_NESTING_DEPTH),
    )
    .map_err(|_| ())
}

/// Run the same allocation-free structural preflight over MessagePack embedded
/// inside a request's binary field. The outer frame scanner deliberately treats
/// `bin` as opaque bytes, so handlers must call this before nested deserialization.
pub(crate) fn validate_nested_msgpack(
    input: &[u8],
    max_bytes: usize,
    max_items: usize,
) -> Result<(), &'static str> {
    eg_types::msgpack::validate_single_value(
        input,
        eg_types::msgpack::MsgpackLimits::new(
            max_bytes,
            max_items.min(HARD_MAX_MSGPACK_ITEMS),
            MAX_MSGPACK_NESTING_DEPTH,
        ),
    )
    .map_err(|_| "invalid or over-complex nested MessagePack payload")
}

fn connection_io_timeout() -> std::time::Duration {
    let seconds = std::env::var("EPISTEMIC_GRAPH_CONNECTION_IO_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CONNECTION_IO_TIMEOUT_SECS)
        .clamp(1, 3_600);
    std::time::Duration::from_secs(seconds)
}

/// CONCEPT:EG-KG.coordination.backpressure-busy-signal — the hard per-dispatch deadline.
///
/// Every admission permit the server issues (the QoS permit, the global pool permit, the
/// per-graph permit, the reserved-read permit, and the per-connection permit) is held by
/// the dispatch task and released only when that task returns. That makes an unbounded
/// dispatch an unbounded RESERVATION: a dispatch that never completes retires none of
/// them, ever. Bounding the dispatch is therefore what makes "a permanently-held
/// admission slot" unrepresentable, at the one place every served request passes through.
///
/// Default 600 s; override with `EPISTEMIC_GRAPH_DISPATCH_DEADLINE_SECS` (clamped to
/// `[1, 86_400]`), following the same idiom as the two timeouts above. It is a ceiling,
/// not a target — the cooperative SQL deadline
/// (`EPISTEMIC_GRAPH_SQL_REQUEST_TIMEOUT_MS`, `server::request_cancel`) is the tunable
/// per-query bound and remains opt-in; this one exists so a NON-cooperative stall (a
/// wedged durable-writer thread, a lost oneshot, a dropped completion) can still not
/// strand the reservation.
fn dispatch_deadline() -> std::time::Duration {
    let seconds = std::env::var("EPISTEMIC_GRAPH_DISPATCH_DEADLINE_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DISPATCH_DEADLINE_SECS)
        .clamp(1, 86_400);
    std::time::Duration::from_secs(seconds)
}

/// Run one dispatch under the hard deadline (CONCEPT:EG-KG.coordination.backpressure-busy-signal).
///
/// Returns the dispatch's own `Response` when it completes in time. When it does NOT,
/// the future is dropped (releasing whatever it borrowed) and a typed error `Response`
/// is synthesized for `req_id`, so the caller returns, its permits drop, and the client
/// learns the request was abandoned instead of waiting forever on a reply that will
/// never come. Fails LOUDLY: the expiry is logged at `error` and counted
/// (`epistemic_graph_dispatch_deadline_exceeded_total`) — a silently-shed request is how
/// this stall stayed invisible for days.
async fn dispatch_within_deadline<F>(
    dispatch: F,
    deadline: std::time::Duration,
    req_id: u64,
) -> Response
where
    F: std::future::Future<Output = Response>,
{
    match tokio::time::timeout(deadline, dispatch).await {
        Ok(resp) => resp,
        Err(_) => {
            crate::metrics::dispatch_deadline_exceeded();
            error!(
                req_id,
                deadline_secs = deadline.as_secs(),
                "dispatch exceeded the hard per-request deadline; abandoning it and \
                 releasing its admission permits (CONCEPT:EG-KG.coordination.backpressure-busy-signal)"
            );
            Response::err(
                req_id,
                "TIMEOUT: request exceeded the server dispatch deadline and was abandoned",
            )
        }
    }
}

fn tls_handshake_timeout() -> std::time::Duration {
    let seconds = std::env::var("EPISTEMIC_GRAPH_TLS_HANDSHAKE_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_TLS_HANDSHAKE_TIMEOUT_SECS)
        .clamp(1, 120);
    std::time::Duration::from_secs(seconds)
}

/// The admission permits a request was granted (held by the dispatch task and
/// dropped when it completes), or `Busy` if it must be shed. The three permit slots
/// are mutually exclusive paths: a NORMAL admission holds `global`+`per_graph`; a
/// RESERVED-read admission (CONCEPT:EG-KG.coordination.reserved-read-lane) holds only `read`.
enum Admission {
    Granted {
        global: Option<tokio::sync::OwnedSemaphorePermit>,
        per_graph: Option<tokio::sync::OwnedSemaphorePermit>,
        read: Option<tokio::sync::OwnedSemaphorePermit>,
    },
    Busy,
}

/// Admit one request against the global pool + per-graph fairness cap, with a
/// RESERVED READ LANE (CONCEPT:EG-KG.coordination.reserved-read-lane) so an ingestion WRITE firehose that saturates
/// both can never shed an interactive read/query to BUSY.
///
/// * Both reads and writes try the NORMAL path first: a global in-flight permit AND
///   this graph's per-graph permit.
/// * A WRITE that loses the normal path is shed `Busy` — strictly back-pressured,
///   never dropped (the durable write path stays the bottleneck, not admission).
/// * A READ that loses the normal path falls back to the dedicated `read_sem` lane,
///   BYPASSING the per-graph cap (a read pays no fairness tax — it is cheap and must
///   stay live). Only a genuine read flood that also fills that small lane is shed.
///
/// This is a pure function over the shared handles so the responsiveness guarantee is
/// directly unit-testable (saturate `sem`, assert reads still admit / writes shed).
fn admit_request(
    sem: &Arc<Semaphore>,
    read_sem: &Arc<Semaphore>,
    pg_map: &dashmap::DashMap<String, Arc<Semaphore>>,
    pg_limit: usize,
    graph: &str,
    is_write: bool,
) -> Admission {
    // NORMAL path: global permit, then this graph's per-graph permit.
    if let Ok(gp) = sem.clone().try_acquire_owned() {
        let pg_sem = pg_map
            .entry(graph.to_string())
            .or_insert_with(|| Arc::new(Semaphore::new(pg_limit)))
            .clone();
        // Per-graph cap full (Err): drop `gp` (released here) and fall through.
        if let Ok(pp) = pg_sem.try_acquire_owned() {
            return Admission::Granted {
                global: Some(gp),
                per_graph: Some(pp),
                read: None,
            };
        }
    }

    // Writes are NEVER dropped, only back-pressured: shed BUSY (retry with backoff).
    if is_write {
        return Admission::Busy;
    }

    // READ under write saturation: the reserved lane, bypassing the per-graph cap.
    match read_sem.clone().try_acquire_owned() {
        Ok(rp) => {
            crate::metrics::read_reserved_admitted();
            Admission::Granted {
                global: None,
                per_graph: None,
                read: Some(rp),
            }
        }
        Err(_) => Admission::Busy,
    }
}

/// Handle one client connection with single-connection request PIPELINING
/// (CONCEPT:EG-KG.backend.framed-response): length-prefixed MessagePack frames, per-request backpressure
/// admission (per-connection + global + per-graph), and CONCURRENT dispatch whose
/// id-tagged responses are written back OUT OF ORDER.
///
/// The duplex stream is `tokio::io::split` into a read half (the frame read loop)
/// and a write half (owned by a single writer task). For each decoded request the
/// loop `tokio::spawn`s a dispatch task that runs `dispatch` and hands the framed
/// `Response` to the writer over an mpsc channel — so the read loop never blocks
/// on dispatch, and N back-to-back frames on ONE connection process concurrently.
///
/// **Write-half strategy — single writer task over an mpsc channel** (not an
/// `Arc<Mutex<WriteHalf>>`): a mutex held across a slow / back-pressured socket
/// `write_all` would serialize EVERY completing task on the socket and add hot-path
/// lock contention; the channel instead decouples response *encoding* (done
/// concurrently inside each task) from the single ordered *socket write*, and its
/// bounded depth is natural backpressure on a slow reader.
///
/// **In-flight bound:** [`per_connection_inflight_limit`] sizes a per-connection
/// semaphore; acquiring its permit is the read-loop backpressure point (the loop
/// stops reading the next frame once this connection is saturated), so one
/// connection cannot spawn unbounded work — bounded memory, not unbounded.
///
/// The single-request path is byte-for-byte equivalent to the old serial loop:
/// with one in-flight request the loop spawns it then parks on the next
/// `read_exact`, the dispatch completes, the writer emits the one response.
pub async fn handle_connection<S>(stream: S, state: Arc<RwLock<ServerState>>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Snapshot the shared backpressure handles once per connection.
    let (sem, read_sem, pg_map, pg_limit) = {
        let s = state.read().await;
        (
            s.max_in_flight.clone(),
            s.read_admission.clone(),
            s.per_graph_inflight.clone(),
            s.per_graph_inflight_limit,
        )
    };

    // CONCEPT:EG-KG.coordination.backpressure-busy-signal — the optional QoS/SLO scheduler. `None` unless
    // `EPISTEMIC_GRAPH_QOS` is configured (built once, process-global), in which case
    // the default admission path below is byte-for-byte unchanged. When `Some`, each
    // request is first gated on priority-class / per-tenant fair-share / quota BEFORE the
    // baseline global+per-graph admission, and the granted permit rides the dispatch task.
    let qos = crate::server::qos::configured();

    // Split the duplex stream: the read loop drives `read_half`; a single writer
    // task owns `write_half`.
    let (mut read_half, mut write_half) = tokio::io::split(stream);

    let conn_limit = per_connection_inflight_limit();
    let conn_sem = Arc::new(Semaphore::new(conn_limit));
    let max_frame_bytes = max_request_frame_bytes();
    let max_response_bytes = max_response_frame_bytes();
    let max_items = max_msgpack_items();
    let io_timeout = connection_io_timeout();
    // CONCEPT:EG-KG.coordination.backpressure-busy-signal — the hard ceiling on one
    // dispatch, resolved once per connection (the same once-at-open discipline the other
    // limits above follow). It is what guarantees every admission permit this connection
    // hands out is eventually released; see `dispatch_within_deadline`.
    let dispatch_deadline = dispatch_deadline();

    // The writer task: drain framed responses in completion order and write them.
    // It exits when ALL senders (the read loop's `tx` + every spawned task's clone)
    // are dropped — i.e. the read loop ended AND every in-flight dispatch finished
    // queueing its response — then flushes. That join barrier is what preserves the
    // graceful-shutdown / shutdown-response-is-written contract.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(conn_limit + 64);
    let writer = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if !matches!(
                tokio::time::timeout(io_timeout, write_half.write_all(&frame)).await,
                Ok(Ok(()))
            ) {
                break;
            }
        }
        let _ = tokio::time::timeout(io_timeout, write_half.flush()).await;
    });

    loop {
        let mut len_buf = [0u8; 4];
        if !matches!(
            tokio::time::timeout(io_timeout, read_half.read_exact(&mut len_buf)).await,
            Ok(Ok(_))
        ) {
            break;
        }
        let len = u32::from_be_bytes(len_buf) as usize;

        if len == 0 || len > max_frame_bytes {
            let resp = Response::err(0, "request frame exceeds the configured resource limit");
            let _ = tx.send(encode_frame(&resp)).await;
            // The unread oversized body makes this connection impossible to
            // resynchronize safely; close it without allocating or draining it.
            break;
        }

        let mut payload = vec![0u8; len];
        if !matches!(
            tokio::time::timeout(io_timeout, read_half.read_exact(&mut payload)).await,
            Ok(Ok(_))
        ) {
            break;
        }

        if validate_msgpack_frame(&payload, max_items).is_err() {
            let resp = Response::err(0, "invalid or over-complex request encoding");
            if tx.send(encode_frame(&resp)).await.is_err() {
                break;
            }
            continue;
        }

        let req: Request = match rmp_serde::from_slice(&payload) {
            Ok(r) => r,
            Err(_) => {
                let resp = Response::err(0, "invalid request encoding");
                if tx.send(encode_frame(&resp)).await.is_err() {
                    break;
                }
                continue;
            }
        };

        let is_shutdown = matches!(req.method, Method::Shutdown);

        // Per-connection backpressure: AWAIT a slot. This is the one point that can
        // park the read loop — only when this connection already has `conn_limit`
        // requests dispatching — so a single connection can't spawn unbounded tasks.
        let conn_permit = match conn_sem.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => break, // semaphore closed (never, while we hold an Arc)
        };

        // ── Admission: global pool + per-graph fairness (Phase C-D) with a RESERVED
        // READ LANE (CONCEPT:EG-KG.coordination.reserved-read-lane) ───────────────────────────────────────────────
        // Classify read vs write so an ingestion WRITE firehose that saturates the
        // global pool AND a graph's per-graph cap can NEVER shed an interactive
        // read/query to BUSY: a read that loses the normal path falls back to a small
        // dedicated lane writes can't touch ("always an open lane for MCP reads").
        // Writes stay strictly back-pressured — shed BUSY (retry), never dropped.
        let is_write = crate::server::access::requires_write(&req.method);

        // ── CONCEPT:EG-KG.coordination.backpressure-busy-signal — QoS/SLO admission gate (opt-in) ─────────────────────────
        // Runs BEFORE the baseline admission. Classifies the request by priority class +
        // tenant and applies priority preemption / per-tenant fair-share / hard quota. A
        // shed request returns a typed, retryable `BUSY:` signal; an admitted one yields a
        // RAII permit the dispatch task holds until it completes. Skipped entirely (and so
        // zero-overhead / behaviour-preserving) when QoS is not configured.
        let mut verified_qos_context = None;
        let qos_permit = if let Some(sched) = qos.as_ref() {
            // QoS owns durable in-flight counters, so it must not key them from
            // the unsigned request `agent_id`. Verify the current envelope first,
            // derive its privacy-safe principal scope, and pass the same context
            // into dispatch so replay acceptance occurs exactly once.
            let context = {
                let server = state.read().await;
                crate::server::auth::verify_request_with_security_dir(
                    &server.auth_secret,
                    &req,
                    server.persist_dir.as_deref(),
                )
            };
            let context = match context {
                Ok(context) => context,
                Err(error) => {
                    crate::metrics::auth_failure();
                    let resp = Response::err(req.id, error);
                    drop(conn_permit);
                    if tx.send(encode_frame(&resp)).await.is_err() {
                        break;
                    }
                    continue;
                }
            };
            let principal_scope = context.principal_persistence_id();
            // The admission class comes from the request's MAC-covered priority claim
            // (W2.4), so a principal cannot forge a higher class than it signed.
            let qreq = match crate::server::qos::classify(&principal_scope, context.priority()) {
                Ok(request) => request,
                Err(error) => {
                    crate::metrics::auth_failure();
                    let resp = Response::err(req.id, error);
                    drop(conn_permit);
                    if tx.send(encode_frame(&resp)).await.is_err() {
                        break;
                    }
                    continue;
                }
            };
            verified_qos_context = Some(context);
            match sched.try_admit(&qreq) {
                crate::server::qos::QosDecision::Admit(p) => {
                    crate::metrics::qos_admitted(qreq.class.label());
                    Some(p)
                }
                crate::server::qos::QosDecision::Reject(why) => {
                    // Per-class shed telemetry (W2.4) + the shared BUSY counter.
                    crate::metrics::qos_shed(qreq.class.label(), why.label());
                    crate::metrics::busy_rejected();
                    let resp = Response::err(req.id, why.busy_message());
                    drop(conn_permit);
                    if tx.send(encode_frame(&resp)).await.is_err() {
                        break;
                    }
                    continue;
                }
            }
        } else {
            None
        };

        let (g_permit, pg_permit, read_permit) =
            match admit_request(&sem, &read_sem, &pg_map, pg_limit, &req.graph, is_write) {
                Admission::Granted {
                    global,
                    per_graph,
                    read,
                } => (global, per_graph, read),
                Admission::Busy => {
                    crate::metrics::busy_rejected();
                    let resp =
                        Response::err(req.id, "BUSY: server at capacity, retry with backoff");
                    drop(conn_permit);
                    if tx.send(encode_frame(&resp)).await.is_err() {
                        break;
                    }
                    continue;
                }
            };

        // Spawn the dispatch: runs CONCURRENTLY with the read loop and with the
        // other in-flight requests on this connection. The `Response` carries
        // `req.id`, so the client demuxes the (possibly out-of-order) completion.
        // The task holds whichever admission permits it was granted until its response
        // is framed and queued, then drops them — closing the per-connection, global,
        // per-graph and reserved-read backpressure loops.
        crate::metrics::connection_request_started(sem.available_permits());
        let task_state = state.clone();
        let task_tx = tx.clone();
        let task_sem = sem.clone();
        // The QoS class this request was admitted under (W2.4), captured for the per-class
        // dispatch-latency histogram + in-flight gauge; `None` when QoS is not configured.
        let qos_class = qos_permit.as_ref().map(|p| p.class());
        tokio::spawn(async move {
            let dispatch_start = std::time::Instant::now();
            // CONCEPT:EG-KG.coordination.backpressure-busy-signal — bound the dispatch. Every
            // permit below is released only when this task returns, so an unbounded
            // dispatch is an unbounded reservation; the deadline is what stops one stalled
            // subsystem from permanently retaining admission capacity it will never use.
            let req_id = req.id;
            let resp = match verified_qos_context {
                Some(context) => {
                    dispatch_within_deadline(
                        dispatch_verified_request(&task_state, req, context),
                        dispatch_deadline,
                        req_id,
                    )
                    .await
                }
                None => {
                    dispatch_within_deadline(dispatch(&task_state, req), dispatch_deadline, req_id)
                        .await
                }
            };
            // Observe per-class dispatch latency + release the per-class gauge (W2.4)
            // BEFORE the permit drop, so the class is still known.
            if let Some(class) = qos_class {
                crate::metrics::qos_dispatch_finished(
                    class.label(),
                    dispatch_start.elapsed().as_secs_f64(),
                );
            }
            let _ = task_tx
                .send(encode_bounded_frame(&resp, max_response_bytes))
                .await;
            drop(read_permit);
            drop(pg_permit);
            drop(g_permit);
            // CONCEPT:EG-KG.coordination.backpressure-busy-signal — release the QoS slot (principal/class/global counters)
            // once the request completes; `None` when QoS is not configured.
            drop(qos_permit);
            drop(conn_permit);
            crate::metrics::connection_request_finished(task_sem.available_permits());
        });

        if is_shutdown {
            // Stop reading further frames; the spawned Shutdown dispatch's response
            // is still drained+written by the writer task below.
            break;
        }
    }

    // Read loop ended (client close, read error, or a Shutdown request). Drop the
    // read-loop sender; the writer task then finishes once every in-flight dispatch
    // task has also dropped its `tx` clone (all queued responses written), and the
    // final flush lands. Awaiting it drains the connection gracefully.
    drop(tx);
    let _ = writer.await;
}

/// Parse + validate a `--socket-mode`/`GRAPH_SERVICE_SOCKET_MODE` string (e.g.
/// `"0600"`, `"0660"`, `"660"`) into the `u32` bit pattern
/// [`std::fs::Permissions::from_mode`] expects.
///
/// Fails loudly (naming the exact value and why) rather than letting a bad
/// setting surface later as a client-side `EACCES` — the same discipline that
/// caught the original hardcoded-0600 lockout in the first place. Never widens
/// the shipped default's intent: any `"other"` (world) permission bit is
/// refused outright, so a misconfiguration can make the socket unreachable but
/// can never make it world-accessible. Broaden access via the socket
/// directory's owning group (`fsGroup` in Kubernetes) plus a group-permitting
/// mode (e.g. `0660`), not via world bits.
pub fn parse_unix_socket_mode(raw: &str) -> Result<u32, String> {
    let trimmed = raw.trim();
    let digits = trimmed.strip_prefix("0o").unwrap_or(trimmed);
    if digits.is_empty() {
        return Err(format!(
            "{raw:?} is empty — expected an octal file mode, e.g. \"0600\""
        ));
    }
    let mode = u32::from_str_radix(digits, 8).map_err(|_| {
        format!("{raw:?} is not a valid octal file mode (expected e.g. \"0600\" or \"0660\")")
    })?;
    if mode > 0o777 {
        return Err(format!(
            "{raw:?} (parsed as octal {mode:#o}) is out of range for a file mode (max 0777)"
        ));
    }
    if mode & 0o007 != 0 {
        return Err(format!(
            "{raw:?} (parsed as octal {mode:#o}) grants \"other\" (world) access to the UDS \
             socket — refused. The socket must stay owner/group-only; widen access via the \
             socket directory's owning group (e.g. Kubernetes `fsGroup`), not world bits."
        ));
    }
    Ok(mode)
}

/// Start the server on a Unix Domain Socket (unix only; Windows uses TCP).
///
/// The accept loop `select!`s the next connection against `coord`'s shutdown
/// signal: when the signal fires the loop BREAKS and returns `Ok(())`, so
/// `main()` falls through to the persistence flush + final checkpoint. Each
/// accepted connection is wrapped in a [`ConnGuard`] so the active-connection
/// refcount the idle watcher observes stays correct.
///
/// `mode` is the already-validated (see [`parse_unix_socket_mode`]) file mode
/// applied to the socket right after bind — configurable via
/// `--socket-mode`/`GRAPH_SERVICE_SOCKET_MODE` so a non-root client container
/// can be granted group access without an external `chmod` watcher; default
/// `0o600` is byte-for-byte the prior hardcoded behavior.
#[cfg(unix)]
pub async fn serve_uds(
    socket_path: &str,
    mode: u32,
    state: Arc<RwLock<ServerState>>,
    coord: Arc<ShutdownCoordinator>,
) -> std::io::Result<()> {
    // Remove stale socket file.
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(mode))?;
    let mode_octal = format!("{mode:#o}");
    info!(mode = %mode_octal, "Listening on a private Unix domain socket");

    loop {
        // Latch check at the TOP catches a trigger() that fired between iterations
        // (the Notify edge below only wakes a currently-parked accept).
        if coord.is_requested() {
            info!("UDS accept loop: shutdown requested, stopping accept");
            break;
        }
        // Arm the wake future BEFORE awaiting accept so a trigger() racing the
        // select! is not lost: notify_waiters wakes this armed future, and even if
        // the edge is missed the latch is re-read at the top of the next iteration.
        let shutdown = coord.notified();
        tokio::select! {
            biased;
            _ = shutdown => {
                info!("UDS accept loop: shutdown signal received, stopping accept");
                break;
            }
            accepted = listener.accept() => match accepted {
                Ok((stream, _addr)) => {
                    let state = state.clone();
                    let guard = ConnGuard::new(coord.clone());
                    tokio::spawn(async move {
                        let _guard = guard; // dropped when the connection ends
                        handle_connection(stream, state).await;
                    });
                }
                Err(e) => {
                    error!("UDS accept error ({:?})", e.kind());
                }
            }
        }
    }
    Ok(())
}

/// Start the server on a TCP address. Same graceful-shutdown contract as
/// [`serve_uds`].
pub async fn serve_tcp(
    addr: &str,
    state: Arc<RwLock<ServerState>>,
    coord: Arc<ShutdownCoordinator>,
    tls: Option<TcpTlsConfig>,
) -> std::io::Result<()> {
    #[cfg(feature = "server-tls")]
    let acceptor = tls.as_ref().map(tls_acceptor).transpose()?;
    #[cfg(not(feature = "server-tls"))]
    let acceptor = {
        if let Some(config) = tls.as_ref() {
            tls_acceptor(config)?;
        }
        None::<()>
    };
    let listener = TcpListener::bind(addr).await?;
    if !listener.local_addr()?.ip().is_loopback() && acceptor.is_none() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "non-loopback native TCP requires TLS",
        ));
    }
    info!(
        "Listening on native TCP (tls={}, mtls={})",
        acceptor.is_some(),
        tls.as_ref()
            .and_then(|value| value.client_ca_path.as_ref())
            .is_some()
    );

    loop {
        if coord.is_requested() {
            info!("TCP accept loop: shutdown requested, stopping accept");
            break;
        }
        let shutdown = coord.notified();
        tokio::select! {
            biased;
            _ = shutdown => {
                info!("TCP accept loop: shutdown signal received, stopping accept");
                break;
            }
            accepted = listener.accept() => match accepted {
                Ok((stream, _peer_addr)) => {
                    let state = state.clone();
                    let guard = ConnGuard::new(coord.clone());
                    #[cfg(feature = "server-tls")]
                    let connection_acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        let _guard = guard; // dropped when the connection ends
                        #[cfg(feature = "server-tls")]
                        if let Some(connection_acceptor) = connection_acceptor {
                            if let Ok(Ok(stream)) = tokio::time::timeout(
                                tls_handshake_timeout(),
                                connection_acceptor.accept(stream),
                            ).await {
                                handle_connection(stream, state).await;
                            }
                            return;
                        }
                        handle_connection(stream, state).await;
                    });
                }
                Err(e) => {
                    error!("TCP accept error ({:?})", e.kind());
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conn_guard_refcounts() {
        let coord = ShutdownCoordinator::new();
        assert_eq!(coord.active_connections(), 0);
        let g1 = ConnGuard::new(coord.clone());
        let g2 = ConnGuard::new(coord.clone());
        assert_eq!(coord.active_connections(), 2);
        drop(g1);
        assert_eq!(coord.active_connections(), 1);
        drop(g2);
        assert_eq!(coord.active_connections(), 0);
    }

    #[test]
    fn encode_frame_is_len_prefixed_and_decodes() {
        // CONCEPT:EG-KG.backend.framed-response — a framed response is `4-byte BE len ++ MessagePack body`,
        // and the body round-trips back to the same id/result so the client can
        // demux it out of order.
        let resp = Response::ok(42, crate::protocol::ResultPayload::String("pong".into()));
        let frame = encode_frame(&resp);
        assert!(frame.len() > 4);
        let declared = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
        assert_eq!(
            declared,
            frame.len() - 4,
            "len prefix must match body length"
        );
        let decoded: Response = rmp_serde::from_slice(&frame[4..]).expect("decode body");
        assert_eq!(decoded.id, 42, "id preserved so the client demuxes by it");
    }

    #[test]
    fn socket_mode_default_matches_prior_hardcoded_value() {
        // The shipped clap default ("0600") must parse to exactly the value this
        // code used to hardcode, so upgrading changes no deployment's behavior.
        assert_eq!(parse_unix_socket_mode("0600").unwrap(), 0o600);
    }

    #[test]
    fn socket_mode_accepts_group_readable_variants() {
        assert_eq!(parse_unix_socket_mode("0660").unwrap(), 0o660);
        assert_eq!(parse_unix_socket_mode("660").unwrap(), 0o660);
        assert_eq!(parse_unix_socket_mode("0o660").unwrap(), 0o660);
        assert_eq!(parse_unix_socket_mode(" 0640 ").unwrap(), 0o640);
    }

    #[test]
    fn socket_mode_refuses_world_bits() {
        // Never a path to "just make it writable" — any nonzero "other" bit is
        // refused outright, whether read, write, or execute.
        for world_open in ["0601", "0604", "0606", "0607", "0777"] {
            let err = parse_unix_socket_mode(world_open)
                .expect_err(&format!("{world_open} should be refused"));
            assert!(
                err.contains("world"),
                "error for {world_open} should explain the world-bit refusal: {err}"
            );
        }
    }

    #[test]
    fn socket_mode_refuses_garbage_and_out_of_range() {
        assert!(parse_unix_socket_mode("").is_err());
        assert!(parse_unix_socket_mode("not-octal").is_err());
        assert!(
            parse_unix_socket_mode("999").is_err(),
            "9 is not a valid octal digit"
        );
        assert!(
            parse_unix_socket_mode("07777").is_err(),
            "exceeds a file mode's 0777 range"
        );
    }

    #[test]
    fn per_connection_limit_is_bounded_and_positive() {
        // CONCEPT:EG-KG.backend.framed-response — the per-connection in-flight cap auto-sizes from cores
        // but is always clamped so one connection can neither stall (floor) nor
        // spawn unbounded work (ceiling).
        let n = per_connection_inflight_limit();
        assert!(
            (64..=1024).contains(&n),
            "per-conn cap {n} out of [64,1024]"
        );
    }

    #[test]
    fn dispatch_deadline_is_bounded_and_positive() {
        // CONCEPT:EG-KG.coordination.backpressure-busy-signal — the hard dispatch ceiling
        // always resolves to a finite, positive duration, so "no bound at all" is not a
        // reachable configuration.
        let d = dispatch_deadline();
        assert!(d > std::time::Duration::ZERO);
        assert!(d <= std::time::Duration::from_secs(86_400));
    }

    #[tokio::test]
    async fn hung_dispatch_is_abandoned_with_a_typed_error() {
        // A dispatch that never completes must still produce a reply for its id, so the
        // caller returns (and its permits drop) instead of parking forever.
        let resp = dispatch_within_deadline(
            std::future::pending::<Response>(),
            std::time::Duration::from_millis(20),
            77,
        )
        .await;
        assert_eq!(resp.id, 77, "the abandoned request is still answered by id");
        assert!(
            resp.error
                .as_deref()
                .unwrap_or_default()
                .starts_with("TIMEOUT:"),
            "expected a typed timeout error, got {:?}",
            resp.error
        );
    }

    /// D-HYD-2 defect pin. THE livelock, reproduced in miniature.
    ///
    /// The live incident: the shard-3 durable writer thread wedged in an unbounded
    /// userspace loop, so every dispatch that needed it parked forever on its completion
    /// oneshot. Each of those tasks holds a `QosPermit`, and — because the deployment
    /// resolves EVERY caller (server, host daemon, scheduler, MCP, external agents) to
    /// ONE verified principal scope — they all draw on ONE per-principal in-flight quota.
    /// 96 stranded dispatches pinned that principal at its quota (`capacity/4` of 384)
    /// permanently, so for 2.5 days the engine shed 100% of requests, INCLUDING reads
    /// that never touch the wedged shard, with `BUSY: QoS per-principal quota exhausted`.
    ///
    /// The invariant this pins: a dispatch that never completes must not permanently
    /// retain its admission slot. Revert `dispatch_within_deadline`'s use at the dispatch
    /// site (await the raw future) and this goes RED — the joins time out and the final
    /// admit is still `Reject(Quota)`.
    #[tokio::test]
    async fn a_hung_dispatch_does_not_permanently_exhaust_the_principal_quota() {
        use crate::server::qos::{
            QosClass, QosConfig, QosDecision, QosReject, QosRequest, QosScheduler,
        };

        let mut cfg = QosConfig::auto(8);
        cfg.per_principal_quota = 2; // the live value was 96; 2 keeps the test fast
        cfg.bucket_refill_per_sec = 0.0; // isolate the QUOTA rule from the token bucket
        let sched = QosScheduler::new(cfg);
        let req = QosRequest {
            class: QosClass::Orch,
            principal: "one-shared-principal".to_string(),
            deadline_micros: None,
        };

        let deadline = std::time::Duration::from_millis(50);
        let mut stranded = Vec::new();
        for _ in 0..2 {
            let permit = match sched.try_admit(&req) {
                QosDecision::Admit(permit) => permit,
                QosDecision::Reject(why) => panic!("expected Admit, got Reject({why:?})"),
            };
            // EXACTLY the production shape: the permit rides the dispatch task and is
            // released only when that task returns.
            stranded.push(tokio::spawn(async move {
                let resp =
                    dispatch_within_deadline(std::future::pending::<Response>(), deadline, 1).await;
                drop(permit);
                resp
            }));
        }

        // The observed live state: at quota, every further request is shed `Quota`.
        assert!(
            matches!(sched.try_admit(&req), QosDecision::Reject(QosReject::Quota)),
            "a principal at its in-flight quota must be shed while the work is live"
        );

        // Bounded join: with the fix reverted these never resolve, so this FAILS rather
        // than hanging the suite.
        for task in stranded {
            let resp = tokio::time::timeout(deadline * 20, task)
                .await
                .expect("a stranded dispatch must be abandoned at the deadline")
                .expect("dispatch task must not panic");
            assert!(
                resp.error.is_some(),
                "an abandoned dispatch answers with an error"
            );
        }

        // The invariant: the quota recovered on its own, with no restart.
        assert!(
            matches!(sched.try_admit(&req), QosDecision::Admit(_)),
            "a hung dispatch must not permanently retain its admission slot"
        );
    }

    #[test]
    fn request_frame_allocation_has_a_hard_ceiling() {
        let limit = max_request_frame_bytes();
        assert!(limit > 0);
        assert!(limit <= HARD_MAX_REQUEST_FRAME_BYTES);
    }

    #[test]
    fn msgpack_preflight_rejects_declared_allocation_bombs() {
        // array32 declares 2^32-1 values while carrying no body. The preflight
        // rejects it without allocating from the untrusted hint.
        assert!(validate_msgpack_frame(&[0xdd, 0xff, 0xff, 0xff, 0xff], 1_000).is_err());
        assert!(validate_msgpack_frame(&[0xdc, 0x00, 0x02, 0xc0], 1_000).is_err());

        let three_nils = [0x93, 0xc0, 0xc0, 0xc0];
        assert!(validate_msgpack_frame(&three_nils, 3).is_err());
        assert!(validate_msgpack_frame(&three_nils, 4).is_ok());
    }

    #[test]
    fn msgpack_preflight_bounds_depth_and_requires_exact_frame() {
        let mut nested = vec![0x91; MAX_MSGPACK_NESTING_DEPTH + 1];
        nested.push(0xc0);
        assert!(validate_msgpack_frame(&nested, 1_000).is_err());

        let valid = rmp_serde::to_vec(&serde_json::json!({
            "method": "Ping",
            "values": [1, 2, 3]
        }))
        .unwrap();
        assert!(validate_msgpack_frame(&valid, 1_000).is_ok());
        let mut trailing = valid;
        trailing.push(0xc0);
        assert!(validate_msgpack_frame(&trailing, 1_000).is_err());
    }

    #[test]
    fn trigger_latches() {
        let coord = ShutdownCoordinator::new();
        assert!(!coord.is_requested());
        coord.trigger();
        assert!(coord.is_requested());
        // Idempotent.
        coord.trigger();
        assert!(coord.is_requested());
    }

    // ── CONCEPT:EG-KG.coordination.reserved-read-lane — reserved read-lane admission guarantee ──────────────────

    /// A read MUST stay admittable when the global pool AND the per-graph cap are
    /// fully saturated by writes — it falls back to the reserved read lane — while a
    /// write in the same saturated state is correctly shed BUSY. This is the core
    /// "an interactive read is never starved behind ingestion" guarantee, proven
    /// deterministically against the pure admission function.
    #[test]
    fn read_is_admitted_when_write_pool_is_saturated() {
        let sem = Arc::new(Semaphore::new(4)); // tiny global pool
        let read_sem = Arc::new(Semaphore::new(2)); // small reserved read lane
        let pg_map = dashmap::DashMap::new();
        let pg_limit = 2;
        let graph = "__commons__";

        // Saturate the global pool: hold all of its permits (an in-flight ingestion
        // write firehose). With the pool drained, the NORMAL admission path fails.
        let _writers: Vec<_> = (0..sem.available_permits())
            .map(|_| sem.clone().try_acquire_owned().unwrap())
            .collect();
        assert_eq!(sem.available_permits(), 0, "global pool saturated");

        // A WRITE now sheds BUSY (back-pressured, not dropped).
        assert!(
            matches!(
                admit_request(&sem, &read_sem, &pg_map, pg_limit, graph, true),
                Admission::Busy
            ),
            "write must be shed BUSY when the global pool is full"
        );

        // A READ is STILL admitted via the reserved read lane.
        let r = admit_request(&sem, &read_sem, &pg_map, pg_limit, graph, false);
        match r {
            Admission::Granted { global, read, .. } => {
                assert!(
                    global.is_none(),
                    "read used the reserved lane, not the global pool"
                );
                assert!(read.is_some(), "read holds a reserved-lane permit");
            }
            Admission::Busy => panic!("read must NOT be shed BUSY while the read lane has slots"),
        }
    }

    /// The reserved read lane is itself bounded: a genuine read FLOOD that fills it is
    /// shed BUSY so memory stays bounded — the reservation guarantees availability, not
    /// unbounded admission.
    #[test]
    fn read_lane_is_bounded_under_a_read_flood() {
        let sem = Arc::new(Semaphore::new(1));
        let read_sem = Arc::new(Semaphore::new(2));
        let pg_map = dashmap::DashMap::new();
        let pg_limit = 1;
        let graph = "g";

        // Saturate the global pool so reads must use the reserved lane.
        let _g = sem.clone().try_acquire_owned().unwrap();
        assert_eq!(sem.available_permits(), 0);

        // Hold both reserved read permits.
        let mut reads = Vec::new();
        for _ in 0..2 {
            match admit_request(&sem, &read_sem, &pg_map, pg_limit, graph, false) {
                Admission::Granted { read, .. } => reads.push(read.expect("reserved permit")),
                Admission::Busy => panic!("reserved read lane should admit up to its size"),
            }
        }
        // The third read floods the lane → BUSY.
        assert!(
            matches!(
                admit_request(&sem, &read_sem, &pg_map, pg_limit, graph, false),
                Admission::Busy
            ),
            "a read flood that fills the reserved lane is shed BUSY (bounded memory)"
        );
        drop(reads);
        // Once a reserved slot frees, reads admit again.
        assert!(
            matches!(
                admit_request(&sem, &read_sem, &pg_map, pg_limit, graph, false),
                Admission::Granted { .. }
            ),
            "read admits again after a reserved slot frees"
        );
    }

    /// Concurrency stress: while many WRITE permits saturate the global pool, a burst
    /// of concurrent READS on the SAME hot graph must ALL be admitted (never BUSY),
    /// proving an interactive read survives under maximum write load on the firehose
    /// graph. Mirrors the live K=4 ingestion symptom at the admission layer.
    #[tokio::test(flavor = "multi_thread")]
    async fn reads_survive_under_max_write_load_on_hot_graph() {
        let sem = Arc::new(Semaphore::new(8));
        let read_sem = Arc::new(Semaphore::new(8));
        let pg_map = Arc::new(dashmap::DashMap::new());
        let pg_limit = 4; // a quarter, like the live default
        let graph = "__commons__";

        // Saturate the global pool: hold all 8 write permits for the test duration.
        let writers: Vec<_> = (0..8)
            .map(|_| sem.clone().try_acquire_owned().unwrap())
            .collect();
        assert_eq!(sem.available_permits(), 0, "writers saturate the pool");

        // Fire a burst of concurrent reads on the SAME hot graph; every one must be
        // admitted (via the reserved lane), holding its permit briefly then releasing.
        let mut tasks = Vec::new();
        for _ in 0..200usize {
            let sem = sem.clone();
            let read_sem = read_sem.clone();
            let pg_map = pg_map.clone();
            tasks.push(tokio::spawn(async move {
                // Retry briefly: the reserved lane is small, so concurrent reads share
                // it — but each holds its slot only momentarily, so all make progress
                // without ever being permanently starved.
                for _ in 0..1000 {
                    match admit_request(&sem, &read_sem, &pg_map, pg_limit, graph, false) {
                        Admission::Granted { read, .. } => {
                            assert!(read.is_some(), "served by reserved lane under saturation");
                            tokio::task::yield_now().await; // hold briefly, then drop
                            return true;
                        }
                        Admission::Busy => tokio::task::yield_now().await,
                    }
                }
                false
            }));
        }
        let mut ok = 0usize;
        for t in tasks {
            if t.await.unwrap() {
                ok += 1;
            }
        }
        assert_eq!(
            ok, 200,
            "every interactive read completed under max write load"
        );
        drop(writers);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_watcher_triggers_when_idle() {
        let coord = ShutdownCoordinator::new();
        let c = coord.clone();
        let h = tokio::spawn(async move { run_idle_watcher(c, 1).await });
        // No connections ⇒ after the grace window the watcher must trigger.
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        assert!(coord.is_requested(), "idle watcher did not trigger");
        h.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn idle_watcher_resets_on_active_connection() {
        let coord = ShutdownCoordinator::new();
        // Hold a live connection the whole time ⇒ the watcher never fires.
        let _g = ConnGuard::new(coord.clone());
        let c = coord.clone();
        tokio::spawn(async move { run_idle_watcher(c, 1).await });
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        assert!(
            !coord.is_requested(),
            "idle watcher fired despite an active connection"
        );
    }
}
