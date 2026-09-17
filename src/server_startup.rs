//! Startup steps of the `epistemic-graph-server` binary.
//!
//! Each function owns one boot concern `run_inner` used to spell out inline:
//! startup refusals, admission sizing, opening one durable store, the shutdown
//! signal, the transports, and the periodic memory/lifecycle sweep ticks. Every
//! refusal keeps its exact message and exit status; a store-open failure stays
//! fatal at boot (loud + early).

use std::sync::Arc;

use tracing::info;

use epistemic_graph::server::{self, ServerState};

type SharedState = Arc<tokio::sync::RwLock<ServerState>>;
type BoxError = Box<dyn std::error::Error>;

fn exit_with(message: impl std::fmt::Display, code: i32) -> ! {
    eprintln!("error: {message}");
    std::process::exit(code);
}

/// Run one bounded, stdin-driven exact performance probe and return.
pub(super) fn run_exact_performance_probe(root: Option<&std::path::Path>) -> Result<(), BoxError> {
    let root = root.ok_or("exact performance probe root is required")?;
    #[cfg(feature = "full")]
    {
        super::performance_probe::run_stdio(root)?;
        Ok(())
    }
    #[cfg(not(feature = "full"))]
    {
        let _ = root;
        Err("exact performance probes require the full server binary".into())
    }
}

pub(super) fn socket_mode_or_exit(raw: &str) -> u32 {
    server::parse_unix_socket_mode(raw).unwrap_or_else(|reason| {
        exit_with(
            format_args!("invalid --socket-mode/GRAPH_SERVICE_SOCKET_MODE {raw:?}: {reason}"),
            2,
        )
    })
}

/// Security gate: an auth secret and a durable-state directory are mandatory.
pub(super) fn require_secret_and_persist_dir(auth_secret: &str, persist_dir: Option<&str>) {
    if auth_secret.is_empty() {
        exit_with(
            "no auth secret configured — refusing to start.\n\
             Set GRAPH_SERVICE_AUTH_SECRET (or pass --auth-secret) to enable \
             HMAC-SHA256 authentication.",
            2,
        );
    }
    if persist_dir.is_none() {
        exit_with(
            "the served engine requires an externally configured durable-state directory",
            2,
        );
    }
}

/// Validate the TCP TLS configuration (both halves or neither; TLS for any
/// non-loopback address) and prepare it.
pub(super) async fn prepare_tcp_tls(
    args: &super::Args,
) -> Result<Option<server::PreparedTcpTls>, BoxError> {
    let config = match (&args.tcp_tls_cert, &args.tcp_tls_key) {
        (Some(cert_path), Some(key_path)) => Some(server::TcpTlsConfig {
            cert_path: cert_path.clone(),
            key_path: key_path.clone(),
            client_ca_path: args.tcp_tls_client_ca.clone(),
        }),
        (None, None) if args.tcp_tls_client_ca.is_none() => None,
        _ => exit_with(
            "native TCP TLS requires both certificate and private-key material",
            2,
        ),
    };
    let insecure_remote =
        |addr: &str| !super::native_tcp_addr_is_loopback(addr) && config.is_none();
    if args.tcp_addr.as_deref().is_some_and(insecure_remote) {
        exit_with("non-loopback native TCP requires TLS", 2);
    }
    Ok(match config {
        Some(tls) => Some(server::prepare_tcp_tls(tls).await?),
        None => None,
    })
}

/// Detect the host capacity and log the startup banner, including the native
/// TCP listener's TLS posture when one is configured.
pub(super) fn detect_capacity_and_log_startup(
    args: &super::Args,
    tcp_tls: bool,
) -> epistemic_graph::autosize::Capacity {
    info!("Starting epistemic-graph-server");
    info!("  UDS: private local socket configured");
    let capacity = epistemic_graph::autosize::detect_capacity();
    info!(
        "  Capacity: {} cpu(s), {} MiB RAM, tier {:?} (auto-sizing inflight/writer/node-cap defaults)",
        capacity.cpus,
        capacity.total_ram_bytes / (1024 * 1024),
        capacity.tier
    );
    if capacity.total_ram_bytes == 0 {
        tracing::warn!(
            "  RAM undetectable (non-Linux or a restricted /proc) — defaulting the \
             per-graph node cap to a conservative {} (the same cap a real 1 GiB Pi \
             gets); override with EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH",
            capacity.node_cap()
        );
    }
    if args.tcp_addr.is_some() {
        info!(
            "  TCP: configured (tls={}, mtls={})",
            tcp_tls,
            args.tcp_tls_client_ca.is_some()
        );
    }
    info!("  Auth: enabled");
    capacity
}

/// Flush any bounded writer work that had not yet crossed its acknowledgement
/// barrier once the accept loop has exited.
pub(super) fn flush_durable_state(persistence: Persistence) {
    info!("Accept loop stopped — flushing durable state");
    if let Some(p) = &persistence {
        p.shutdown();
    }
    info!("Shutdown complete");
}

/// Refuse to start if another engine already owns this persist dir; the lock is
/// held for the whole process lifetime by the caller.
pub(super) fn acquire_persist_lock(
    persist_dir: Option<&str>,
) -> Option<epistemic_graph::persist_lock::PersistDirLock> {
    let dir = persist_dir?;
    let lock = epistemic_graph::persist_lock::acquire(dir).unwrap_or_else(|e| exit_with(e, 1));
    info!("Acquired the configured single-writer persistence lock");
    Some(lock)
}

/// A positive `variable` override, bounded by `bound`. Absent, unparsable and
/// zero values yield `None`.
fn bounded_env_override(variable: &str, bound: usize) -> Option<usize> {
    std::env::var(variable)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .map(|value| epistemic_graph::autosize::bound_explicit(value, bound))
}

/// Bound an explicit `requested` value by `automatic`, warning when it clamps.
fn clamp_to_automatic(variable: &str, requested: usize, automatic: usize) -> usize {
    let bounded = epistemic_graph::autosize::bound_explicit(requested, automatic);
    if bounded != requested {
        tracing::warn!(
            requested,
            bounded,
            automatic,
            "{variable} exceeds cgroup-aware automatic capacity; clamping"
        );
    }
    bounded
}

/// In-flight admission sizing (CONCEPT:AU-KG.backend.b-auto-size).
pub(super) struct AdmissionLimits {
    pub(super) max_in_flight: usize,
    pub(super) per_graph_inflight_limit: usize,
    pub(super) read_reserved: usize,
}

/// Size the admission pools from effective CPU capacity. A positive env
/// override can only lower each cgroup-aware default.
pub(super) fn admission_limits(capacity: &epistemic_graph::autosize::Capacity) -> AdmissionLimits {
    let automatic_max_in_flight = capacity.max_inflight();
    let max_in_flight = std::env::var("EPISTEMIC_GRAPH_MAX_INFLIGHT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .map(|value| {
            clamp_to_automatic(
                "EPISTEMIC_GRAPH_MAX_INFLIGHT",
                value,
                automatic_max_in_flight,
            )
        })
        .unwrap_or(automatic_max_in_flight);
    // Per-graph fairness cap (Phase C-D): default to a quarter of the global pool
    // so any one hot graph holds at most 25% of capacity and ~4 graphs can saturate
    // the server, instead of a single tenant monopolizing all in-flight slots.
    let per_graph_inflight_limit =
        bounded_env_override("EPISTEMIC_GRAPH_MAX_INFLIGHT_PER_GRAPH", max_in_flight)
            .unwrap_or_else(|| (max_in_flight / 4).max(1));
    // Reserved READ-admission lane (CONCEPT:EG-KG.coordination.reserved-read-lane): a
    // dedicated pool of in-flight slots that ONLY reads/queries may use, so a write
    // firehose that saturates the global pool + per-graph cap can never shed an
    // interactive MCP read to BUSY (an eighth of the admission cap, floored).
    let automatic_read_reserved = capacity.read_reserved().min(max_in_flight).max(1);
    let read_reserved =
        bounded_env_override("EPISTEMIC_GRAPH_READ_RESERVED", automatic_read_reserved)
            .unwrap_or(automatic_read_reserved);
    info!(
        "Backpressure: max in-flight = {} (per-graph cap = {}, reserved read lane = {})",
        max_in_flight, per_graph_inflight_limit, read_reserved
    );
    // Per-graph write coalescer (CONCEPT:EG-KG.sharding.per-graph-write-coalescer): batch
    // size auto-sized from cpu count and always enabled with bounded queues.
    let coalescer = epistemic_graph::write_coalescer::CoalescerConfig::auto();
    info!(
        "Write coalescer: batch up to {} ops/lock (queue {}, linger {:?})",
        coalescer.max_batch, coalescer.queue_capacity, coalescer.max_linger
    );
    AdmissionLimits {
        max_in_flight,
        per_graph_inflight_limit,
        read_reserved,
    }
}

type Persistence = Option<Arc<dyn server::persistence::PersistenceBackend>>;

/// The served engine has one durability implementation: authoritative redb.
/// Every acknowledged mutation crosses the commit barrier, and bounded writer
/// queues apply backpressure rather than dropping work.
#[cfg(feature = "redb")]
pub(super) fn open_persistence(
    persist_dir: Option<&str>,
    capacity: &epistemic_graph::autosize::Capacity,
) -> Persistence {
    let dir = persist_dir?;
    let automatic_writer_queue = capacity.writer_queue();
    let queue = bounded_env_override("EPISTEMIC_GRAPH_REDB_WRITER_QUEUE", automatic_writer_queue)
        .unwrap_or(automatic_writer_queue);
    info!("Persistence: authoritative redb (queue {})", queue);
    let backend = server::persistence::redb_backend::RedbBackend::open(dir.to_string(), queue)
        .unwrap_or_else(|error| {
            exit_with(
                format_args!("failed to open durable graph store: {error}"),
                1,
            )
        });
    Some(Arc::new(backend))
}

/// `RedbBackend` is gated behind the `redb` feature, so a build without it fails
/// loudly at boot on an attempted `--persist-dir` rather than silently
/// downgrading to in-memory.
#[cfg(not(feature = "redb"))]
pub(super) fn open_persistence(
    persist_dir: Option<&str>,
    _capacity: &epistemic_graph::autosize::Capacity,
) -> Persistence {
    if persist_dir.is_some() {
        exit_with(
            "--persist-dir requires a redb-enabled build (the `redb` feature is not compiled in)",
            2,
        );
    }
    None
}

/// Native time-series store (feature `tsdb`): a durable `series.redb` beside the
/// graph shards when a persist dir is set, else a process-temp file.
#[cfg(feature = "tsdb")]
pub(super) fn open_tsdb_store(
    persist_dir: Option<&str>,
) -> Option<Arc<eg_tsdb::store::SeriesStore>> {
    let path = match persist_dir {
        Some(dir) => std::path::Path::new(dir).join("series.redb"),
        None => std::env::temp_dir().join(format!("eg-tsdb-{}.redb", std::process::id())),
    };
    let authority = epistemic_graph::store_authority::process_authority();
    let verifier = epistemic_graph::store_authority::process_verifier();
    match eg_tsdb::store::SeriesStore::open(
        &path,
        verifier,
        authority.principal(),
        &authority.proof(),
    ) {
        Ok(store) => {
            info!("Time-series store (tsdb): durable store ready");
            Some(Arc::new(store))
        }
        Err(e) => {
            tracing::error!("failed to open durable time-series store: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(feature = "blob")]
type BlobSubstrate = (Option<Arc<server::blob::BlobCursors>>, u64);

/// Streamed content-addressed BLOB substrate (CONCEPT:EG-KG.storage.blob-namespace).
/// With no persist dir there is no durable place for the bytes, so the substrate
/// is disabled and the Blob* methods report "not available".
#[cfg(feature = "blob")]
pub(super) fn open_blob_substrate(persist_dir: Option<&str>) -> BlobSubstrate {
    let ttl = std::env::var("EPISTEMIC_GRAPH_BLOB_CURSOR_TTL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(300u64);
    let Some(dir) = persist_dir else {
        tracing::warn!("Blob substrate disabled: no persist dir (blob bytes need durable storage)");
        return (None, ttl);
    };
    let store = open_blob_chunk_store(dir);
    info!("Blob substrate: durable content-addressed CAS ready");
    (Some(Arc::new(server::blob::BlobCursors::new(store))), ttl)
}

#[cfg(all(feature = "blob", feature = "blob-s3"))]
fn open_blob_chunk_store(dir: &str) -> Arc<dyn server::blob::ChunkStore> {
    match server::blob::s3::S3ChunkStore::open(dir) {
        Ok(store) => Arc::new(store),
        Err(e) => exit_with(format_args!("failed to open durable blob-s3 CAS: {e}"), 1),
    }
}

#[cfg(all(feature = "blob", not(feature = "blob-s3")))]
fn open_blob_chunk_store(dir: &str) -> Arc<dyn server::blob::ChunkStore> {
    match server::blob::RedbChunkStore::open(dir) {
        Ok(store) => Arc::new(store),
        Err(e) => exit_with(format_args!("failed to open durable blob CAS: {e}"), 1),
    }
}

/// Generic Key→Value store (feature `kv`): a durable `{persist_dir}/kv.redb` when
/// a persist dir is set, else an in-memory scratch map.
#[cfg(feature = "kv")]
pub(super) fn open_kv_store(persist_dir: Option<&str>) -> Option<Arc<server::kv::KvStore>> {
    let store = server::kv::KvStore::open(persist_dir)
        .unwrap_or_else(|e| exit_with(format_args!("failed to open kv store: {e}"), 1));
    if store.is_durable() {
        info!(
            "Key→Value store (kv): durable kv.redb (CONCEPT:EG-OS.config.configurable-listeners)"
        );
    } else {
        info!("Key→Value store (kv): in-memory scratch — no persist dir");
    }
    Some(Arc::new(store))
}

/// Every durable store the served state owns, opened at boot.
pub(super) struct DurableStores {
    pub(super) persistence: Persistence,
    #[cfg(feature = "tsdb")]
    tsdb_store: Option<Arc<eg_tsdb::store::SeriesStore>>,
    #[cfg(feature = "blob")]
    blob: BlobSubstrate,
    #[cfg(feature = "kv")]
    kv: Option<Arc<server::kv::KvStore>>,
}

/// Open the persistence backend and the feature-gated tsdb/blob/kv stores, in
/// that order.
pub(super) fn open_durable_stores(
    persist_dir: Option<&str>,
    capacity: &epistemic_graph::autosize::Capacity,
) -> DurableStores {
    DurableStores {
        persistence: open_persistence(persist_dir, capacity),
        #[cfg(feature = "tsdb")]
        tsdb_store: open_tsdb_store(persist_dir),
        #[cfg(feature = "blob")]
        blob: open_blob_substrate(persist_dir),
        #[cfg(feature = "kv")]
        kv: open_kv_store(persist_dir),
    }
}

/// Install the values sized or opened at boot into the canonical state.
///
/// The complete feature-gated field composition stays in `ServerState::new`;
/// startup overrides only these values, so the orchestration path cannot drift
/// from other state users.
pub(super) fn compose_server_state(
    mut state: ServerState,
    persist_dir: Option<String>,
    stores: DurableStores,
    limits: &AdmissionLimits,
    (txn_ttl_secs, txn_max_per_graph, txn_max_per_agent): (u64, usize, usize),
) -> ServerState {
    #[cfg(all(feature = "streaming", feature = "cdc-kafka"))]
    if let Some(hub) = &state.cdc {
        // CA-11 (DEC-CA-03): install the optional Kafka sink exactly once,
        // after pure state composition and before any listener can serve.
        server::cdc_sink::install_from_env(hub);
    }
    state.persist_dir = persist_dir;
    state.persistence = stores.persistence;
    state.max_in_flight = Arc::new(tokio::sync::Semaphore::new(limits.max_in_flight));
    state.read_admission = Arc::new(tokio::sync::Semaphore::new(limits.read_reserved));
    state.per_graph_inflight_limit = limits.per_graph_inflight_limit;
    state.txn_ttl_secs = txn_ttl_secs;
    state.txn_max_per_graph = txn_max_per_graph;
    state.txn_max_per_agent = txn_max_per_agent;
    #[cfg(feature = "blob")]
    {
        (state.blob, state.blob_cursor_ttl_secs) = stores.blob;
    }
    #[cfg(feature = "tsdb")]
    {
        state.tsdb_store = stores.tsdb_store;
    }
    #[cfg(feature = "kv")]
    {
        state.kv = stores.kv;
    }
    state
}

/// RLS is unconditionally default-deny: every served request carries a verified
/// tenant context and resolves through provisioned durable identity/RBAC policy
/// before any row is exposed.
#[cfg(feature = "security")]
pub(super) fn open_isolation_layer(
    auth_secret: &str,
    persist_dir: Option<&str>,
) -> epistemic_graph::isolation::IsolationLayer {
    info!("RLS default-deny ACTIVE: rows require explicit public visibility or an owner grant");
    let Some(dir) = persist_dir else {
        exit_with(
            "secure request context requires --persist-dir / GRAPH_SERVICE_PERSIST_DIR for durable identity policy and replay state",
            1,
        );
    };
    let authority = epistemic_graph::store_authority::process_authority();
    let isolation = epistemic_graph::isolation::IsolationLayer::with_persist_dir(
        dir,
        authority.as_ref(),
        authority.principal(),
        &authority.proof(),
    )
    .unwrap_or_else(|error| {
        exit_with(
            format_args!("could not open durable identity/RBAC policy: {error}"),
            1,
        )
    });
    if let Err(error) = server::validate_verified_request_context_startup(auth_secret, persist_dir)
    {
        exit_with(
            format_args!("invalid verified request-context configuration: {error}"),
            1,
        );
    }
    if isolation.identity_bootstrap_pending() {
        info!(
            "Identity policy is empty: only a signer-backed current-envelope System identity bootstrap is admitted"
        );
    }
    isolation
}

/// SIGTERM (a supervisor / `kill` / agent-utilities stopping the daemon) and
/// SIGINT (Ctrl-C) both fire the same graceful signal. On non-unix only Ctrl-C
/// is available.
pub(super) fn spawn_shutdown_signal_handler(coordinator: Arc<server::ShutdownCoordinator>) {
    tokio::spawn(async move {
        if wait_for_shutdown_signal().await {
            coordinator.trigger();
        }
    });
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> bool {
    use tokio::signal::unix::{signal, SignalKind};
    // Installed one after the other: a failed SIGTERM handler must leave SIGINT
    // at its default disposition.
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("failed to install SIGTERM handler: {e}");
            return false;
        }
    };
    let mut int = match signal(SignalKind::interrupt()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("failed to install SIGINT handler: {e}");
            return false;
        }
    };
    tokio::select! {
        _ = term.recv() => info!("Received SIGTERM — graceful shutdown"),
        _ = int.recv()  => info!("Received SIGINT — graceful shutdown"),
    }
    true
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> bool {
    if tokio::signal::ctrl_c().await.is_ok() {
        info!("Received Ctrl-C — graceful shutdown");
    }
    true
}

/// Optional reference-counted idle shutdown (CONCEPT:EG-KG.backend.tiny-shared).
/// `0` means no watcher: the engine is long-living and never self-terminates.
pub(super) fn spawn_idle_shutdown_watcher(
    coordinator: &Arc<server::ShutdownCoordinator>,
    secs: u64,
) {
    if secs == 0 {
        info!("Idle shutdown disabled (persistent mode): engine stays up while idle");
        return;
    }
    info!(
        "Idle shutdown ARMED: will self-terminate after {}s with zero active connections",
        secs
    );
    let idle_coord = coordinator.clone();
    tokio::spawn(async move {
        server::run_idle_watcher(idle_coord, secs).await;
    });
}

/// Where and how the engine listens.
pub(super) struct Transports {
    pub(super) socket_path: String,
    pub(super) socket_mode: u32,
    pub(super) tcp_addr: Option<String>,
    pub(super) tcp_tls: Option<server::PreparedTcpTls>,
}

/// UDS is the primary transport on unix, with an optional secondary TCP
/// listener. Returns when the shutdown signal fires.
#[cfg(unix)]
pub(super) async fn serve_transports(
    transports: Transports,
    state: &SharedState,
    shutdown: &Arc<server::ShutdownCoordinator>,
) -> Result<(), BoxError> {
    if let Some(addr) = transports.tcp_addr {
        let tcp_state = state.clone();
        let tcp_shutdown = shutdown.clone();
        let tls = transports.tcp_tls;
        tokio::spawn(async move {
            if let Err(e) = server::serve_tcp(&addr, tcp_state, tcp_shutdown, tls).await {
                tracing::error!("TCP server error ({:?})", e.kind());
            }
        });
    }
    server::serve_uds(
        &transports.socket_path,
        transports.socket_mode,
        state.clone(),
        shutdown.clone(),
    )
    .await?;
    Ok(())
}

/// Non-unix (Windows): Tokio has no UnixListener, so TCP loopback is the
/// per-platform DEFAULT transport — an explicit --tcp-addr wins. The socket
/// path/mode are still resolved+validated for config/lock parity & logging.
#[cfg(not(unix))]
pub(super) async fn serve_transports(
    transports: Transports,
    state: &SharedState,
    shutdown: &Arc<server::ShutdownCoordinator>,
) -> Result<(), BoxError> {
    let _ = (&transports.socket_path, transports.socket_mode);
    let addr = transports
        .tcp_addr
        .unwrap_or_else(|| "127.0.0.1:8765".to_string());
    info!("AF_UNIX unavailable; using the configured native TCP transport");
    server::serve_tcp(&addr, state.clone(), shutdown.clone(), transports.tcp_tls).await?;
    Ok(())
}

/// Read `variable`: absent means `default`; a present value must satisfy
/// `parse` (after trimming) or startup exits with `invalid`.
pub(super) fn env_or_exit<T>(
    variable: &str,
    default: T,
    parse: impl FnOnce(&str) -> Option<T>,
    invalid: &str,
) -> T {
    match std::env::var(variable) {
        Ok(value) => parse(value.trim()).unwrap_or_else(|| exit_with(invalid, 2)),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => {
            exit_with(format_args!("{variable} is not valid Unicode"), 2)
        }
    }
}

/// The per-graph resident node cap: auto-sized from effective RAM; an explicit
/// override must be positive and may only lower it.
pub(super) fn max_nodes_per_graph(capacity: &epistemic_graph::autosize::Capacity) -> usize {
    const VARIABLE: &str = "EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH";
    let automatic = capacity.node_cap();
    env_or_exit(
        VARIABLE,
        automatic,
        |value| {
            value
                .parse::<usize>()
                .ok()
                .filter(|&limit| limit > 0)
                .map(|limit| clamp_to_automatic(VARIABLE, limit, automatic))
        },
        "EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH must be positive",
    )
}

/// Seconds from `variable`, when set to a positive integer; otherwise off (`0`).
pub(super) fn optional_sweep_secs(variable: &str) -> u64 {
    std::env::var(variable)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

pub(super) async fn memcap_tick(state: SharedState, max_nodes_per_graph: usize) {
    let evicted = epistemic_graph::persist::evict_oversized_all(&state, max_nodes_per_graph).await;
    if evicted > 0 {
        tracing::info!("Memory cap: evicted {} LRU node(s) over cap", evicted);
    }
}

#[cfg(feature = "cost")]
pub(super) async fn budget_tick(state: SharedState, config: epistemic_graph::cost::CostConfig) {
    let (evicted, hibernated) = epistemic_graph::cost::enforce_memory_budgets(&state, config).await;
    if evicted > 0 || hibernated > 0 {
        tracing::info!(
            "Memory budget: evicted {} node(s), hibernated {} graph(s) to keep \
                 tenants under budget",
            evicted,
            hibernated
        );
    }
}

pub(super) async fn registry_reap_tick(state: SharedState) {
    let now_ms = server::txn::now_ms();
    let n = server::registry_reaper::reap_expired_servers(&state, now_ms).await;
    if n > 0 {
        tracing::info!("Server registry: reaped {} expired :Server lease(s)", n);
    }
}

#[cfg(feature = "security")]
pub(super) async fn provenance_anchor_tick(state: SharedState) {
    let anchored = server::persistence::provenance_anchor::sweep(&state).await;
    if anchored > 0 {
        tracing::info!(
            "Provenance anchoring: anchored {} graph(s) this tick",
            anchored
        );
    }
}

pub(super) async fn txn_ttl_tick(state: SharedState, ttl: u64) {
    let now = server::txn::now_ms();
    let reclaimed = server::txn::sweep_expired_txns(&state, ttl, now);
    if reclaimed > 0 {
        tracing::info!(
            "Txn TTL sweep: rolled back {} idle transaction(s)",
            reclaimed
        );
    }
}
