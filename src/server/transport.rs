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

use super::{dispatch, ServerState};
use crate::protocol::{Method, Request, Response};

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
/// 0 continuously for `idle_secs` seconds (CONCEPT:KG-2.223 — the tiny shared
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
/// requests that produced it (CONCEPT:EG-043).
fn encode_frame(resp: &Response) -> Vec<u8> {
    let body = encode_response(resp);
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

/// Per-connection in-flight cap (CONCEPT:EG-043). Bounds how many requests ONE
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

/// The admission permits a request was granted (held by the dispatch task and
/// dropped when it completes), or `Busy` if it must be shed. The three permit slots
/// are mutually exclusive paths: a NORMAL admission holds `global`+`per_graph`; a
/// RESERVED-read admission (CONCEPT:EG-044) holds only `read`.
enum Admission {
    Granted {
        global: Option<tokio::sync::OwnedSemaphorePermit>,
        per_graph: Option<tokio::sync::OwnedSemaphorePermit>,
        read: Option<tokio::sync::OwnedSemaphorePermit>,
    },
    Busy,
}

/// Admit one request against the global pool + per-graph fairness cap, with a
/// RESERVED READ LANE (CONCEPT:EG-044) so an ingestion WRITE firehose that saturates
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
        match pg_sem.try_acquire_owned() {
            Ok(pp) => {
                return Admission::Granted {
                    global: Some(gp),
                    per_graph: Some(pp),
                    read: None,
                };
            }
            // Per-graph cap full: drop `gp` (released here) and fall through.
            Err(_) => {}
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
/// (CONCEPT:EG-043): length-prefixed MessagePack frames, per-request backpressure
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

    // Split the duplex stream: the read loop drives `read_half`; a single writer
    // task owns `write_half`.
    let (mut read_half, mut write_half) = tokio::io::split(stream);

    let conn_limit = per_connection_inflight_limit();
    let conn_sem = Arc::new(Semaphore::new(conn_limit));

    // The writer task: drain framed responses in completion order and write them.
    // It exits when ALL senders (the read loop's `tx` + every spawned task's clone)
    // are dropped — i.e. the read loop ended AND every in-flight dispatch finished
    // queueing its response — then flushes. That join barrier is what preserves the
    // graceful-shutdown / shutdown-response-is-written contract.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(conn_limit + 64);
    let writer = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if write_half.write_all(&frame).await.is_err() {
                break;
            }
        }
        let _ = write_half.flush().await;
    });

    loop {
        let mut len_buf = [0u8; 4];
        if read_half.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut payload = vec![0u8; len];
        if read_half.read_exact(&mut payload).await.is_err() {
            break;
        }

        let req: Request = match rmp_serde::from_slice(&payload) {
            Ok(r) => r,
            Err(e) => {
                let resp = Response::err(0, format!("Invalid request MsgPack: {}", e));
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
        // READ LANE (CONCEPT:EG-044) ───────────────────────────────────────────────
        // Classify read vs write so an ingestion WRITE firehose that saturates the
        // global pool AND a graph's per-graph cap can NEVER shed an interactive
        // read/query to BUSY: a read that loses the normal path falls back to a small
        // dedicated lane writes can't touch ("always an open lane for MCP reads").
        // Writes stay strictly back-pressured — shed BUSY (retry), never dropped.
        let is_write = crate::server::access::requires_write(&req.method);
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
        tokio::spawn(async move {
            let resp = dispatch(&task_state, req).await;
            let _ = task_tx.send(encode_frame(&resp)).await;
            drop(read_permit);
            drop(pg_permit);
            drop(g_permit);
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

/// Start the server on a Unix Domain Socket (unix only; Windows uses TCP).
///
/// The accept loop `select!`s the next connection against `coord`'s shutdown
/// signal: when the signal fires the loop BREAKS and returns `Ok(())`, so
/// `main()` falls through to the persistence flush + final checkpoint. Each
/// accepted connection is wrapped in a [`ConnGuard`] so the active-connection
/// refcount the idle watcher observes stays correct.
#[cfg(unix)]
pub async fn serve_uds(
    socket_path: &str,
    state: Arc<RwLock<ServerState>>,
    coord: Arc<ShutdownCoordinator>,
) -> std::io::Result<()> {
    // Remove stale socket file.
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)?;
    info!("Listening on UDS: {}", socket_path);

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
                    error!("Accept error: {}", e);
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
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on TCP: {}", addr);

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
                Ok((stream, addr)) => {
                    info!("TCP connection from {}", addr);
                    let state = state.clone();
                    let guard = ConnGuard::new(coord.clone());
                    tokio::spawn(async move {
                        let _guard = guard; // dropped when the connection ends
                        handle_connection(stream, state).await;
                    });
                }
                Err(e) => {
                    error!("Accept error: {}", e);
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
        // CONCEPT:EG-043 — a framed response is `4-byte BE len ++ MessagePack body`,
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
    fn per_connection_limit_is_bounded_and_positive() {
        // CONCEPT:EG-043 — the per-connection in-flight cap auto-sizes from cores
        // but is always clamped so one connection can neither stall (floor) nor
        // spawn unbounded work (ceiling).
        let n = per_connection_inflight_limit();
        assert!(
            (64..=1024).contains(&n),
            "per-conn cap {n} out of [64,1024]"
        );
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

    // ── CONCEPT:EG-044 — reserved read-lane admission guarantee ──────────────────

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
                assert!(global.is_none(), "read used the reserved lane, not the global pool");
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
        assert_eq!(ok, 200, "every interactive read completed under max write load");
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
