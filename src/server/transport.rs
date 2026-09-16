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

mod framing;
mod tls;

pub(crate) use framing::validate_nested_msgpack;
#[cfg(test)]
use framing::{
    dispatch_deadline, max_request_frame_bytes, read_request_frame, recover_request_id,
    validate_msgpack_frame, FrameReadError, HARD_MAX_REQUEST_FRAME_BYTES,
    MAX_MSGPACK_NESTING_DEPTH,
};
use framing::{
    encode_bounded_frame, encode_frame, next_request, seconds_from_env, write_responses,
    ConnectionLimits,
};
pub use tls::{prepare_tcp_tls, PreparedTcpTls, TcpTlsConfig};

const DEFAULT_TLS_HANDSHAKE_TIMEOUT_SECS: u64 = 10;

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

/// Per-connection in-flight cap (CONCEPT:EG-KG.backend.framed-response). Bounds how many requests ONE
/// connection may have dispatching CONCURRENTLY, so a single client cannot spawn
/// unbounded server tasks/memory — the global `ServerState::max_in_flight`
/// semaphore remains the box-wide admission cap (which sheds `BUSY`). Auto-sized
/// from the shared cgroup-aware capacity (no knob), so a constrained connection
/// cannot inherit the host's affinity count or a fixed floor that exceeds its
/// effective budget. The hard ceiling remains 1024.
fn per_connection_inflight_limit() -> usize {
    crate::autosize::detect_capacity().per_connection_inflight()
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
    seconds_from_env(
        "EPISTEMIC_GRAPH_TLS_HANDSHAKE_TIMEOUT_SECS",
        DEFAULT_TLS_HANDSHAKE_TIMEOUT_SECS,
        1..=120,
    )
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

struct AdmissionPools {
    global: Arc<Semaphore>,
    read: Arc<Semaphore>,
    per_graph: Arc<dashmap::DashMap<String, Arc<Semaphore>>>,
    per_graph_limit: usize,
}

impl AdmissionPools {
    async fn snapshot(state: &RwLock<ServerState>) -> Self {
        let server = state.read().await;
        Self {
            global: server.max_in_flight.clone(),
            read: server.read_admission.clone(),
            per_graph: server.per_graph_inflight.clone(),
            per_graph_limit: server.per_graph_inflight_limit,
        }
    }
}

#[derive(Default)]
struct QosAdmission {
    context: Option<super::auth::VerifiedRequestContext>,
    permit: Option<super::qos::QosPermit>,
}

/// QoS counters are keyed only from the verified envelope. The same verified
/// context travels into dispatch, so durable replay acceptance happens once.
async fn admit_qos_request(
    req: &Request,
    state: &RwLock<ServerState>,
    scheduler: Option<&Arc<super::qos::QosScheduler>>,
) -> Result<QosAdmission, Response> {
    let Some(scheduler) = scheduler else {
        return Ok(QosAdmission::default());
    };
    let context = {
        let server = state.read().await;
        super::auth::verify_request_with_security_dir(
            &server.auth_secret,
            req,
            server.persist_dir.as_deref(),
        )
    }
    .map_err(|error| {
        crate::metrics::auth_failure();
        Response::err(req.id, error)
    })?;
    let principal_scope = context.principal_persistence_id();
    let qos_request =
        super::qos::classify(&principal_scope, context.priority()).map_err(|error| {
            crate::metrics::auth_failure();
            Response::err(req.id, error)
        })?;
    match scheduler.try_admit(&qos_request) {
        super::qos::QosDecision::Admit(permit) => {
            crate::metrics::qos_admitted(qos_request.class.label());
            Ok(QosAdmission {
                context: Some(context),
                permit: Some(permit),
            })
        }
        super::qos::QosDecision::Reject(why) => {
            crate::metrics::qos_shed(qos_request.class.label(), why.label());
            crate::metrics::busy_rejected();
            Err(Response::err(req.id, why.busy_message()))
        }
    }
}

struct DispatchReservation {
    connection: tokio::sync::OwnedSemaphorePermit,
    global: Option<tokio::sync::OwnedSemaphorePermit>,
    per_graph: Option<tokio::sync::OwnedSemaphorePermit>,
    read: Option<tokio::sync::OwnedSemaphorePermit>,
    qos: QosAdmission,
}

enum AdmissionFailure {
    Rejected,
    Closed,
}

async fn queue_admission_rejection(
    tx: &tokio::sync::mpsc::Sender<Vec<u8>>,
    response: Response,
) -> AdmissionFailure {
    match tx.send(encode_frame(&response)).await {
        Ok(()) => AdmissionFailure::Rejected,
        Err(_) => AdmissionFailure::Closed,
    }
}

/// Await the connection slot, then apply authenticated QoS before baseline
/// fairness. A rejected request releases its connection slot before queuing its
/// error; any granted QoS slot remains owned until that queue operation finishes.
async fn reserve_request(
    req: &Request,
    state: &RwLock<ServerState>,
    connection: &Arc<Semaphore>,
    pools: &AdmissionPools,
    scheduler: Option<&Arc<super::qos::QosScheduler>>,
    tx: &tokio::sync::mpsc::Sender<Vec<u8>>,
) -> Result<DispatchReservation, AdmissionFailure> {
    let connection = match connection.clone().acquire_owned().await {
        Ok(permit) => permit,
        Err(_) => return Err(AdmissionFailure::Closed),
    };
    let is_write = super::access::requires_write(&req.method);
    let qos = match admit_qos_request(req, state, scheduler).await {
        Ok(admission) => admission,
        Err(response) => {
            drop(connection);
            return Err(queue_admission_rejection(tx, response).await);
        }
    };
    match admit_request(
        &pools.global,
        &pools.read,
        &pools.per_graph,
        pools.per_graph_limit,
        &req.graph,
        is_write,
    ) {
        Admission::Granted {
            global,
            per_graph,
            read,
        } => Ok(DispatchReservation {
            connection,
            global,
            per_graph,
            read,
            qos,
        }),
        Admission::Busy => {
            crate::metrics::busy_rejected();
            let response = Response::err(req.id, "BUSY: server at capacity, retry with backoff");
            drop(connection);
            let failure = queue_admission_rejection(tx, response).await;
            drop(qos);
            Err(failure)
        }
    }
}

/// Every permit rides this task through dispatch, bounded response encoding, and
/// response queuing. Keep the erased heap future: the full dispatch future needs
/// both stack protection and a single Send proof at this production boundary.
async fn dispatch_reserved_request(
    req: Request,
    state: Arc<RwLock<ServerState>>,
    tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    global_pool: Arc<Semaphore>,
    reservation: DispatchReservation,
    limits: ConnectionLimits,
) {
    let dispatch_start = std::time::Instant::now();
    let qos_class = reservation.qos.permit.as_ref().map(|permit| permit.class());
    let req_id = req.id;
    type BoxedDispatch<'a> =
        std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send + 'a>>;
    let boxed: BoxedDispatch<'_> = match reservation.qos.context {
        Some(context) => Box::pin(dispatch_verified_request(&state, req, context)),
        None => Box::pin(dispatch(&state, req)),
    };
    let response = dispatch_within_deadline(boxed, limits.dispatch_deadline, req_id).await;
    if let Some(class) = qos_class {
        crate::metrics::qos_dispatch_finished(
            class.label(),
            dispatch_start.elapsed().as_secs_f64(),
        );
    }
    let _ = tx
        .send(encode_bounded_frame(&response, limits.response_bytes))
        .await;
    drop(reservation.read);
    drop(reservation.per_graph);
    drop(reservation.global);
    drop(reservation.qos.permit);
    drop(reservation.connection);
    crate::metrics::connection_request_finished(global_pool.available_permits());
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
    let pools = AdmissionPools::snapshot(&state).await;
    let qos = super::qos::configured();
    let limits = ConnectionLimits::configured();
    let conn_limit = per_connection_inflight_limit();
    let connection = Arc::new(Semaphore::new(conn_limit));
    let (mut reader, writer) = tokio::io::split(stream);
    let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(conn_limit + 64);
    let writer = tokio::spawn(write_responses(writer, rx, limits.io_timeout));

    while let Some(req) = next_request(&mut reader, &tx, limits).await {
        let is_shutdown = matches!(req.method, Method::Shutdown);
        let reservation =
            match reserve_request(&req, &state, &connection, &pools, qos.as_ref(), &tx).await {
                Ok(reservation) => reservation,
                Err(AdmissionFailure::Rejected) => continue,
                Err(AdmissionFailure::Closed) => break,
            };
        crate::metrics::connection_request_started(pools.global.available_permits());
        tokio::spawn(dispatch_reserved_request(
            req,
            state.clone(),
            tx.clone(),
            pools.global.clone(),
            reservation,
            limits,
        ));
        if is_shutdown {
            break;
        }
    }

    // Dropping ingress's sender still leaves every dispatch sender alive. The
    // writer drains their queued replies (including Shutdown) before flushing.
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
    let owned_socket_path = socket_path.to_owned();
    let listener = ::tokio::task::spawn_blocking(move || {
        use std::os::unix::fs::PermissionsExt;

        // Preserve the existing best-effort stale-file cleanup. Binding below
        // remains the authority for whether this path can become a listener.
        let _ = std::fs::remove_file(&owned_socket_path);
        let listener = std::os::unix::net::UnixListener::bind(&owned_socket_path)?;
        std::fs::set_permissions(&owned_socket_path, std::fs::Permissions::from_mode(mode))?;
        listener.set_nonblocking(true)?;
        Ok::<_, std::io::Error>(listener)
    })
    .await
    .map_err(|_| std::io::Error::other("UDS setup worker failed"))??;
    let listener = UnixListener::from_std(listener)?;
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
    tls: Option<PreparedTcpTls>,
) -> std::io::Result<()> {
    #[cfg(feature = "server-tls")]
    let acceptor = tls.as_ref().map(|value| value.acceptor.clone());
    #[cfg(not(feature = "server-tls"))]
    let acceptor = None::<()>;
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
        tls.as_ref().map(|value| value.mutual_tls).unwrap_or(false)
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
mod tests;
