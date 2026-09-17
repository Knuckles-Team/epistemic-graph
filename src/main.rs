#![recursion_limit = "256"]
#![allow(dead_code)]
#![deny(unsafe_code)]
// CONCEPT:EG-KG.query.wire-protocol — Epistemic Graph Service Binary
//
// Entry point for the long-running Tokio service process.
// Parses CLI args, initializes the GraphRegistry, and starts
// the UDS/TCP listener.

use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(feature = "security")]
use tokio::sync::RwLock;
// NOT cfg-gated: `info!` is used 110 times in this file, the great majority in code
// paths that carry no cfg of their own. Gating the import on `security` broke
// `--no-default-features --features server` with 13 `cannot find macro `info`` errors
// (BUG-CX-104, same shape as the dispatch.rs/graph_ops.rs fixes: an import gated more
// narrowly than its uses).
use tracing::info;

use epistemic_graph::server;
use epistemic_graph::server::ServerState;

#[cfg(feature = "full")]
mod performance_probe;
mod server_startup;

#[derive(Parser, Debug)]
#[command(name = "epistemic-graph-server")]
#[command(about = "Tokio-native epistemic graph service")]
struct Args {
    /// Run one bounded, stdin-driven G-37 performance scenario and exit.
    #[arg(long, hide = true)]
    exact_performance_probe: bool,

    /// Private scratch directory created by the exact-performance certifier.
    #[arg(
        long,
        hide = true,
        requires = "exact_performance_probe",
        value_name = "DIRECTORY"
    )]
    exact_performance_probe_root: Option<std::path::PathBuf>,

    /// Unix Domain Socket path.
    /// Falls back to $GRAPH_SERVICE_SOCKET, then $XDG_RUNTIME_DIR/epistemic-graph.sock,
    /// then /tmp/epistemic-graph.sock.
    #[arg(long, env = "GRAPH_SERVICE_SOCKET")]
    socket_path: Option<String>,

    /// Octal file-mode applied to the UDS socket right after bind (e.g. `0600`,
    /// `0660`). **Default `0600`** (owner-only) — byte-for-byte today's
    /// hardcoded behavior; set this instead of relying on an external `chmod`
    /// watcher when a non-root client container needs group access (e.g. a
    /// `fsGroup`-owned socket directory under `readOnlyRootFilesystem`).
    /// Validated at startup and REFUSED if it would be world-writable or
    /// world-readable — this can loosen who may `connect()`, never who may
    /// read the graph over that connection (`eg2.` auth still applies), but a
    /// world-open UDS is refused outright rather than accepted and footgunned.
    #[arg(long, default_value = "0600", env = "GRAPH_SERVICE_SOCKET_MODE")]
    socket_mode: String,

    /// Optional TCP address (e.g., 0.0.0.0:9100). If set, TCP listener is started.
    #[arg(long, env = "GRAPH_SERVICE_TCP_ADDR")]
    tcp_addr: Option<String>,

    /// PEM certificate chain for native TCP TLS.
    #[arg(long, env = "GRAPH_SERVICE_TLS_CERT")]
    tcp_tls_cert: Option<String>,

    /// PEM private key for native TCP TLS.
    #[arg(long, env = "GRAPH_SERVICE_TLS_KEY")]
    tcp_tls_key: Option<String>,

    /// Optional PEM CA bundle for required client-certificate authentication.
    #[arg(long, env = "GRAPH_SERVICE_TLS_CLIENT_CA")]
    tcp_tls_client_ca: Option<String>,

    /// HMAC-SHA256 shared secret for authentication.
    #[arg(long, env = "GRAPH_SERVICE_AUTH_SECRET", default_value = "")]
    auth_secret: String,

    /// Directory for the authoritative durable store.
    #[arg(long, env = "GRAPH_SERVICE_PERSIST_DIR")]
    persist_dir: Option<String>,

    /// Ebbinghaus decay sweep interval in seconds (0 = disabled).
    #[arg(long, default_value = "0", env = "GRAPH_SERVICE_DECAY_INTERVAL")]
    decay_interval: u64,

    /// Half-life (seconds) for the periodic decay sweep. Default: 7 days.
    #[arg(long, default_value = "604800", env = "GRAPH_SERVICE_DECAY_HALF_LIFE")]
    decay_half_life: f64,

    /// Prune nodes/edges whose decayed confidence falls below this floor
    /// (0 = decay only, never prune).
    #[arg(long, default_value = "0.0", env = "GRAPH_SERVICE_DECAY_FLOOR")]
    decay_floor: f64,

    /// Prometheus /metrics HTTP listener address (e.g. 127.0.0.1:9101).
    /// Disabled when unset. Separate from the MessagePack RPC transports.
    #[arg(long, env = "GRAPH_SERVICE_METRICS_ADDR")]
    metrics_addr: Option<String>,

    /// W3C SPARQL 1.1 Protocol HTTP listener address (e.g. 127.0.0.1:7878), feature
    /// `sparql-http`. Disabled when unset. Lets existing Stardog/Jena/rdflib SPARQL
    /// clients query + update the engine unchanged. Separate from the RPC transports.
    #[arg(long, env = "EPISTEMIC_GRAPH_SPARQL_ADDR")]
    sparql_addr: Option<String>,

    /// CA-16 (DEC-CA-04) `/policy/export` HTTP listener address (e.g.
    /// 127.0.0.1:7879), feature `policy_export`. Disabled when unset. Serves the
    /// M1 row-visibility policy bundle as JSON; admin-gated by an OIDC bearer
    /// token (`kg:admin`/`policy:export`) via the SAME primary-protocol OIDC
    /// validator as the `eg2.` RPC surface (`oidc::JwtValidator::from_env_primary`)
    /// -- no separate credential shape. Separate from the RPC transports.
    #[arg(long, env = "EPISTEMIC_GRAPH_POLICY_EXPORT_ADDR")]
    policy_export_addr: Option<String>,

    /// Loopback address for the authenticated GraphQL subscription SSE carrier
    /// (e.g. 127.0.0.1:7879), feature `graphql`. Non-loopback binds are refused;
    /// remote access must use a same-host TLS reverse proxy. Every subscription
    /// requires a current eg2 envelope, graph ACL, and RLS projection.
    #[arg(long, env = "EPISTEMIC_GRAPH_GRAPHQL_ADDR")]
    graphql_addr: Option<String>,

    /// Maximum concurrent GraphQL SSE handshakes and sessions.
    #[arg(
        long,
        env = "EPISTEMIC_GRAPH_GRAPHQL_MAX_CONNECTIONS",
        default_value_t = 128
    )]
    graphql_max_connections: usize,

    /// Maximum GraphQL SSE session lifetime before a fresh eg2 envelope is required.
    #[arg(
        long,
        env = "EPISTEMIC_GRAPH_GRAPHQL_MAX_SESSION_SECS",
        default_value_t = 300
    )]
    graphql_max_session_secs: u64,

    /// Observability log-ingestion HTTP listener address (e.g. 127.0.0.1:5080),
    /// feature `obs` (CONCEPT:AU-KG.ingest.self-ingest/161). Disabled when unset. Accepts OTLP/HTTP
    /// (`/v1/logs`), Elasticsearch `_bulk`/`_doc`, and JSON-lines log records, landing
    /// them in eg-tsdb series + eg-text full-text indices and rolling Parquet segments
    /// into the blob CAS. Separate from the RPC transports.
    #[arg(long, env = "EPISTEMIC_GRAPH_OBS_ADDR")]
    obs_addr: Option<String>,

    /// Interactive native-visualization HTTP listener address (e.g.
    /// 127.0.0.1:5090), feature `viz-interactive` (D-VZ-1 lane V3b). Disabled
    /// when unset. Serves the reference WebGPU/WebGL2 client (`GET /`) and the
    /// binary viewport-tile protocol (`GET /tile`) a browser cannot reach over
    /// the length-prefixed MessagePack RPC transports — separate from those and
    /// from every other auxiliary HTTP surface. Loopback-only, like every
    /// auxiliary listener; a remote client goes through a same-host reverse
    /// proxy (this fleet's `edge-ingress`), never a direct non-loopback bind.
    #[arg(long, env = "EPISTEMIC_GRAPH_VIZ_INTERACTIVE_ADDR")]
    viz_interactive_addr: Option<String>,

    /// Iceberg-REST catalog HTTP listener address (e.g. 127.0.0.1:8181), feature
    /// `lake-rest` (INT-P2-3, `iceberg.apache.org/rest-catalog-spec`). Disabled when
    /// unset. Serves list/load (+ a compaction-bridged commit) over the tables the
    /// `lake` materialization tier writes, so a standard Iceberg REST client (PyIceberg/
    /// Spark/Trino) can list + load them. Separate from the RPC transports and the
    /// `/sparql`/`/obs` surfaces.
    #[arg(long, env = "EPISTEMIC_GRAPH_ICEBERG_ADDR")]
    iceberg_addr: Option<String>,

    /// Super-cluster federated-search HTTP listener address (e.g. 127.0.0.1:7900),
    /// feature `federation-search` (CONCEPT:EG-KG.ontology.federation-client). Disabled when unset. Serves a
    /// `/federated` POST (`{query, lang}`) that fans the read query across the peers in
    /// `EPISTEMIC_GRAPH_FEDERATION_PEERS` AND the local store, then unions/de-dups +
    /// RRF-re-ranks the partials (a slow/dead peer degrades to `partial: true`). Separate
    /// from the RPC transports and the `/sparql` surface.
    #[arg(long, env = "EPISTEMIC_GRAPH_FEDERATED_ADDR")]
    federated_addr: Option<String>,

    /// Self-terminate after N seconds with ZERO active connections (reference-
    /// counted idle shutdown). 0 or absent ⇒ NEVER self-terminate on idle: the
    /// engine is long-living/persistent and runs forever like a normal server.
    /// N>0 ⇒ a shared tiny daemon shuts itself down (checkpointing cleanly) N
    /// seconds after its last client disconnects; a new connection during the
    /// grace period cancels the timer. SIGTERM/SIGINT graceful shutdown works in
    /// BOTH modes. agent-utilities' EngineResolver passes this to its autostarted
    /// tiny daemon.
    #[arg(long, default_value = "0", env = "EPISTEMIC_GRAPH_IDLE_SHUTDOWN_SECS")]
    idle_shutdown_secs: u64,
}

/// Resolve the default UDS path per-platform. Explicit > $GRAPH_SERVICE_SOCKET
/// (handled by clap's `env`) > per-OS runtime dir > temp dir fallback.
///
/// - **Unix:** `$XDG_RUNTIME_DIR/epistemic-graph.sock` (when the dir exists),
///   else `/tmp/epistemic-graph.sock`.
/// - **Windows:** `%LOCALAPPDATA%\epistemic-graph\engine.sock` (its parent is
///   prepared during validated startup), else `%TEMP%\epistemic-graph.sock`, else
///   `C:\Windows\Temp\epistemic-graph.sock`. NOTE: Tokio has no `UnixListener` on
///   Windows, so this path is only a stable *identifier* / lock anchor — the
///   actual default transport on Windows is TCP loopback (see the transport
///   section). Keeping the value defined preserves parity for config/logging.
fn resolve_socket_path(explicit: Option<String>) -> (String, Option<std::path::PathBuf>) {
    if let Some(p) = explicit {
        return (p, None);
    }
    #[cfg(unix)]
    {
        if let Ok(xdg) = std::env::var("XDG_RUNTIME_DIR") {
            let xdg_sock = format!("{}/epistemic-graph.sock", xdg);
            // Prefer XDG if the directory exists
            if std::path::Path::new(&xdg).exists() {
                return (xdg_sock, None);
            }
        }
        ("/tmp/epistemic-graph.sock".to_string(), None)
    }
    #[cfg(windows)]
    {
        resolve_windows_socket_path(
            std::env::var_os("LOCALAPPDATA"),
            std::env::var_os("TEMP").or_else(|| std::env::var_os("TMP")),
        )
    }
    #[cfg(not(any(unix, windows)))]
    {
        (
            std::env::temp_dir()
                .join("epistemic-graph.sock")
                .to_string_lossy()
                .into_owned(),
            None,
        )
    }
}

#[cfg(any(windows, test))]
fn resolve_windows_socket_path(
    local_app_data: Option<std::ffi::OsString>,
    temp: Option<std::ffi::OsString>,
) -> (String, Option<std::path::PathBuf>) {
    if let Some(local) = local_app_data {
        let parent = std::path::PathBuf::from(local).join("epistemic-graph");
        return (
            parent.join("engine.sock").to_string_lossy().into_owned(),
            Some(parent),
        );
    }
    if let Some(temp) = temp {
        return (
            std::path::PathBuf::from(temp)
                .join("epistemic-graph.sock")
                .to_string_lossy()
                .into_owned(),
            None,
        );
    }
    (r"C:\Windows\Temp\epistemic-graph.sock".to_string(), None)
}

#[cfg(any(windows, test))]
async fn prepare_socket_directory(parent: Option<std::path::PathBuf>) -> std::io::Result<()> {
    let Some(parent) = parent else {
        return Ok(());
    };
    ::tokio::task::spawn_blocking(move || std::fs::create_dir_all(&parent))
        .await
        .map_err(|error| {
            std::io::Error::other(format!("socket-directory startup task failed: {error}"))
        })?
        .map_err(|error| {
            std::io::Error::new(
                error.kind(),
                format!("could not create socket directory: {error}"),
            )
        })
}

fn native_tcp_addr_is_loopback(addr: &str) -> bool {
    addr.parse::<SocketAddr>()
        .map(|value| value.ip().is_loopback())
        .unwrap_or_else(|_| {
            addr.rsplit_once(':')
                .map(|(host, port)| {
                    host.trim_matches(|character| character == '[' || character == ']')
                        .eq_ignore_ascii_case("localhost")
                        && !port.is_empty()
                        && port.chars().all(|character| character.is_ascii_digit())
                })
                .unwrap_or(false)
        })
}

/// Resolve an optional listener bind address from a deploy-supplied value
/// (CONCEPT:EG-OS.config.configurable-listeners — deploy-configurable listeners). The auxiliary HTTP listeners
/// (Prometheus metrics, SPARQL, pgwire) are all opt-in; this lets a deploy turn one
/// on WITHOUT a full `host:port` and WITHOUT a code change:
///   * `None` / empty / `0`|`off`|`false`|`no`|`disabled` ⇒ `None` (listener off).
///   * `1`|`on`|`true`|`yes`|`enabled` ⇒ the safe localhost default `default_addr`.
///   * a bare port (`9101`) ⇒ `127.0.0.1:9101` (loopback — never `0.0.0.0`).
///   * an explicit loopback socket address / ``localhost:port`` is honored.
///   * a non-loopback address is rejected. Remote access terminates at an
///     authenticated TLS identity-binding gateway that connects to loopback.
fn resolve_listener_addr(value: Option<&str>, default_addr: &str) -> Option<String> {
    let v = value.map(str::trim).filter(|s| !s.is_empty())?;
    let resolved = match v.to_ascii_lowercase().as_str() {
        "0" | "off" | "false" | "no" | "disabled" => None,
        "1" | "on" | "true" | "yes" | "enabled" => Some(default_addr.to_string()),
        _ if v.chars().all(|c| c.is_ascii_digit()) => Some(format!("127.0.0.1:{v}")),
        _ => Some(v.to_string()),
    };
    let addr = resolved?;
    let is_loopback = addr
        .parse::<SocketAddr>()
        .map(|socket| socket.ip().is_loopback())
        .unwrap_or_else(|_| {
            addr.rsplit_once(':')
                .map(|(host, port)| {
                    host.eq_ignore_ascii_case("localhost")
                        && !port.is_empty()
                        && port.chars().all(|c| c.is_ascii_digit())
                })
                .unwrap_or(false)
        });
    if !is_loopback {
        tracing::error!("refusing non-loopback auxiliary listener");
        return None;
    }
    Some(addr)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Explicit, hardware-sized multi-thread runtime (CONCEPT:EG-KG.storage.nonblocking-checkpoint — A4). The
    // The runtime itself is an automatic capacity consumer. Resolve through the
    // shared cgroup-aware seam before building it; a CPU-limited pod must not
    // inherit the host's affinity count or a two-thread floor that exceeds its
    // measured budget. The resolver already reserves CPU headroom and keeps a
    // one-thread progress floor.
    let cores = epistemic_graph::autosize::detect_capacity().reserved_cpus();
    let worker_threads = cores.max(1);
    let max_blocking = cores.max(1);
    let driver = server::spawn_engine_driver(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_stack_size(server::ENGINE_WORKER_STACK_BYTES)
            .max_blocking_threads(max_blocking)
            .enable_all()
            .build()
            .map_err(|error| {
                eprintln!("error: could not build the engine runtime: {error}");
            })?;
        runtime.block_on(async {
            // The driver signature carries no error payload, so this is the LAST
            // place the real cause exists. Report it before it is discarded:
            // without this the operator sees only "engine runtime driver failed"
            // for every possible startup fault. A durable-recovery failure on a
            // 9.9G production store presented exactly that way, and the actual
            // reason — a graph_meta row the current build could not decode — was
            // only recoverable by bisecting against an empty store.
            run().await.map_err(|error| {
                eprintln!("error: engine startup failed: {error}");
            })
        })
    })?;
    match server::join_engine_driver(driver)? {
        Ok(()) => Ok(()),
        Err(()) => Err(std::io::Error::other("engine runtime driver failed").into()),
    }
}

/// Served requests require a verified tenant context resolved through provisioned
/// durable identity/RBAC policy, so a build without `security` cannot serve at all.
/// This thin wrapper refuses that build BEFORE any of [`run_inner`]'s setup runs,
/// instead of diverging partway through it — the latter shape made every
/// subsequent statement in the (very long) real body unreachable-by-construction
/// for a `not(feature = "security"))` compile, cascading into unused-variable
/// noise across the whole function rather than one clean refusal.
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(feature = "security"))]
    {
        eprintln!("error: server deployment requires a build with the security feature");
        std::process::exit(1);
    }
    #[cfg(feature = "security")]
    {
        run_inner().await
    }
}

#[cfg(feature = "security")]
async fn run_inner() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = Args::parse();

    if args.exact_performance_probe {
        return server_startup::run_exact_performance_probe(
            args.exact_performance_probe_root.as_deref(),
        );
    }

    // CONCEPT:EG-OS.observability.tracing-subscriber-init — install the tracing subscriber. This is the fmt-only
    // subscriber (INFO, no target) UNLESS built with `otel` AND
    // EPISTEMIC_GRAPH_OTLP_ENDPOINT is set, in which case an OTLP batch span
    // exporter is layered on top. Off/unset ⇒ byte-for-byte the prior behavior.
    epistemic_graph::otel::init_tracing()?;

    let (socket_path, socket_parent) = resolve_socket_path(args.socket_path.take());
    let socket_mode = server_startup::socket_mode_or_exit(&args.socket_mode);

    // ── Security gate: an auth secret and a durable-state directory are mandatory ──
    server_startup::require_secret_and_persist_dir(&args.auth_secret, args.persist_dir.as_deref());
    let tcp_tls = server_startup::prepare_tcp_tls(&args).await?;

    // ── Hardware capacity auto-detection (CONCEPT:AU-KG.backend.b-auto-size) ─────────────────
    // Size the concurrency / buffer / per-graph node-cap DEFAULTS from
    // (effective cgroup-aware CPU, RAM) so the SAME binary is lean + OOM-safe in
    // a constrained pod and exploits a big box. Detected ONCE; positive explicit
    // values below may lower their defaults but cannot widen them. Mirrors the
    // shared cgroup-aware runtime sizing above, CoalescerConfig::auto, and
    // cost.rs's effective-memory budget.
    let host_capacity = server_startup::detect_capacity_and_log_startup(&args, tcp_tls.is_some());

    // ── Single-writer durable-store guard ──────────────────────────────────
    // Refuse to start if another engine already owns this persist dir; hold the
    // lock for the whole process lifetime so no second engine can clobber our
    // authoritative rows (the engine-level complement to the Python spawn guard). Kept in
    // `_persist_lock` until run() returns; the kernel releases it on exit/crash.
    let _persist_lock = server_startup::acquire_persist_lock(args.persist_dir.as_deref());

    let limits = server_startup::admission_limits(&host_capacity);
    // OCC ACID transaction limits (CONCEPT:EG-KG.txn.multi-op-occ-acid).
    let txn_limits = epistemic_graph::server::txn_limits_from_env();
    // Every store is opened BEFORE `persist_dir` is moved into the state; each
    // open failure is fatal at boot (loud + early).
    let stores = server_startup::open_durable_stores(args.persist_dir.as_deref(), &host_capacity);
    let persistence_shutdown = stores.persistence.clone();
    let isolation =
        server_startup::open_isolation_layer(&args.auth_secret, args.persist_dir.as_deref());

    // Keep resolution side-effect-free: transport and complete verified-context
    // validation must succeed before startup creates the Windows LOCALAPPDATA
    // anchor directory. The sole directory effect is awaited off-reactor before
    // state construction or any listener can open.
    #[cfg(windows)]
    prepare_socket_directory(socket_parent).await?;
    #[cfg(not(windows))]
    debug_assert!(socket_parent.is_none());

    let server_state = server_startup::compose_server_state(
        ServerState::new(std::mem::take(&mut args.auth_secret), isolation),
        args.persist_dir.take(),
        stores,
        &limits,
        txn_limits,
    );
    let state = Arc::new(RwLock::new(server_state));

    // Compose the ordinary process as a real local-placement server before any
    // listeners are started.  A configured Raft startup replaces this authority
    // atomically with `(RaftHandle, MultiRaft)` in `raft::node::start`; a failed
    // configured startup exits rather than serving through this local route.
    #[cfg(feature = "raft")]
    state.write().await.install_local_placement_authority();

    spawn_optional_service_listeners(&state, &args).await?;

    spawn_reasoning_cascade_and_ann_sweep(
        &state,
        args.decay_interval,
        args.decay_half_life,
        args.decay_floor,
    )
    .await;

    spawn_memory_and_lifecycle_sweeps(&state, host_capacity, txn_limits.0).await;

    start_raft_and_matview_reload(&state).await?;

    // ── Graceful shutdown coordination (reference-counted) ────────────────
    // ONE coordinator shared by every accept loop, the SIGTERM/SIGINT handler,
    // and the optional idle watcher. When its signal fires, the accept loop(s)
    // BREAK and the main listener returns, so we fall through to the persistence
    // flush below. An acknowledged write is already committed; shutdown only
    // drains the writer's bounded in-flight tail.
    let shutdown = server::ShutdownCoordinator::new();
    server_startup::spawn_shutdown_signal_handler(shutdown.clone());
    server_startup::spawn_idle_shutdown_watcher(&shutdown, args.idle_shutdown_secs);

    // ── Transport ───────────────────────────────────────────────────────
    let transports = server_startup::Transports {
        socket_path,
        socket_mode,
        tcp_addr: args.tcp_addr.take(),
        tcp_tls,
    };
    server_startup::serve_transports(transports, &state, &shutdown).await?;

    // Graceful shutdown: the accept loop has exited, so flush any bounded writer
    // work that had not yet crossed its acknowledgement barrier.
    server_startup::flush_durable_state(persistence_shutdown);
    Ok(())
}

async fn spawn_optional_service_listeners(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    spawn_metrics_listener(args.metrics_addr.as_deref()).await?;
    spawn_sparql_listener(state, args.sparql_addr.as_deref()).await?;
    spawn_policy_export_listener(args.policy_export_addr.as_deref()).await?;
    run_fuseki_startup_health_check();
    spawn_federated_listener(state, args.federated_addr.as_deref()).await?;
    spawn_graphql_listener(
        state,
        args.graphql_addr.as_deref(),
        args.graphql_max_connections,
        args.graphql_max_session_secs,
    )
    .await?;
    spawn_obs_listener(state, args.obs_addr.as_deref()).await?;
    spawn_viz_interactive_listener(state, args.viz_interactive_addr.as_deref()).await?;
    spawn_iceberg_listener(state, args.iceberg_addr.as_deref()).await?;
    spawn_lake_materialize_sweep(state).await;
    spawn_pgwire_listener(state).await?;
    spawn_sqlite_listener(state).await?;
    spawn_mysql_listener(state).await?;
    spawn_mssql_listener(state).await?;
    spawn_amqp_listener(state).await?;
    spawn_bolt_listener(state).await?;
    spawn_redis_listener(state).await?;
    spawn_mqtt_listener(state).await?;
    spawn_stomp_listener(state).await?;
    spawn_s3_listener(state).await?;
    spawn_kvcache_listener(state).await?;
    recover_durable_catalog(state).await?;
    #[cfg(feature = "epistemic-tms")]
    epistemic_graph::server::reasoning_projection::spawn(state.clone());
    Ok(())
}

async fn spawn_metrics_listener(
    metrics_addr_arg: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    // ── Prometheus metrics endpoint (CONCEPT:EG-KG.txn.per-graph-write-isolation) ────────────────────
    // Opt-in + deploy-configurable (CONCEPT:EG-OS.config.configurable-listeners): bound only when
    // --metrics-addr / GRAPH_SERVICE_METRICS_ADDR is set. A bare enable token
    // (`1`/`on`/…) binds the safe localhost default `127.0.0.1:9101`; a bare port
    // binds loopback:port. A non-loopback address additionally requires the
    // protected-ingress two-key policy.
    let metrics_addr = resolve_listener_addr(metrics_addr_arg, "127.0.0.1:9101");
    if let Some(ref metrics_addr) = metrics_addr {
        #[cfg(feature = "metrics")]
        {
            let listener = tokio::net::TcpListener::bind(metrics_addr).await?;
            info!(
                "Metrics: serving Prometheus exposition on http://{}/metrics",
                metrics_addr
            );
            tokio::spawn(async move {
                epistemic_graph::metrics::serve(listener).await;
            });
        }
        #[cfg(not(feature = "metrics"))]
        tracing::warn!(
            "--metrics-addr {} ignored: binary built without the `metrics` feature",
            metrics_addr
        );
    }
    Ok(())
}
async fn spawn_sparql_listener(
    _state: &Arc<tokio::sync::RwLock<ServerState>>,
    sparql_addr_arg: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "sparql-http")]
    let state = _state;
    // ── W3C SPARQL 1.1 HTTP endpoint (CONCEPT:EG-KG.query.named-graph-support) ────────────────────
    // Opt-in AND feature-gated: the listener starts ONLY when built `--features
    // sparql-http` AND --sparql-addr / EPISTEMIC_GRAPH_SPARQL_ADDR is set. With the
    // feature off, or unset, this is a no-op and the engine runs exactly as before.
    // Deploy-configurable (CONCEPT:EG-OS.config.configurable-listeners): a bare enable token binds the safe
    // localhost default `127.0.0.1:7878`; a bare port binds loopback:port. A
    // non-loopback address additionally requires the protected-ingress policy.
    let sparql_addr = resolve_listener_addr(sparql_addr_arg, "127.0.0.1:7878");
    #[cfg(feature = "sparql-http")]
    if let Some(ref sparql_addr) = sparql_addr {
        let listener = tokio::net::TcpListener::bind(sparql_addr).await?;
        info!(
            "SPARQL: serving W3C SPARQL 1.1 Protocol on http://{}/sparql",
            sparql_addr
        );
        let sparql_state = state.clone();
        tokio::spawn(async move {
            epistemic_graph::server::sparql_http::serve(listener, sparql_state).await;
        });
    }
    #[cfg(not(feature = "sparql-http"))]
    if sparql_addr.is_some() {
        tracing::warn!("--sparql-addr ignored: binary built without the `sparql-http` feature");
    }
    Ok(())
}
async fn spawn_policy_export_listener(
    policy_export_addr_arg: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    // ── CA-16 (DEC-CA-04) `/policy/export` HTTP listener ────────────────────────
    // Opt-in AND feature-gated, exactly like the SPARQL/metrics listeners above:
    // starts only when built `--features policy_export` AND
    // --policy-export-addr / EPISTEMIC_GRAPH_POLICY_EXPORT_ADDR is set.
    let policy_export_addr = resolve_listener_addr(policy_export_addr_arg, "127.0.0.1:7879");
    #[cfg(feature = "policy_export")]
    if let Some(ref policy_export_addr) = policy_export_addr {
        let listener = tokio::net::TcpListener::bind(policy_export_addr).await?;
        info!(
            "policy-export: serving the CA-16/DEC-CA-04 M1 policy bundle on http://{}/policy/export",
            policy_export_addr
        );
        tokio::spawn(async move {
            epistemic_graph::server::policy_export::serve(listener).await;
        });
    }
    #[cfg(not(feature = "policy_export"))]
    if policy_export_addr.is_some() {
        tracing::warn!(
            "--policy-export-addr ignored: binary built without the `policy_export` feature"
        );
    }
    Ok(())
}
fn run_fuseki_startup_health_check() {
    // ── Fuseki SERVICE-federation startup health-check (CA-12, feature `sparql-fuseki`) ──
    // Best-effort and LOGGED, not enforced (matches the lane's W03 completion evidence:
    // "Startup log shows reachability result", not "startup refuses to serve"). Runs the
    // SAME guarded `sparql_http::ServiceClient` the live `SERVICE <ep> {…}` dispatch path
    // uses, so a green log line here is real evidence the federation path works, not a
    // separate check that could pass while the real path is broken. No-op unless BOTH
    // `EPISTEMIC_GRAPH_FUSEKI_HEALTH_CHECK_ENDPOINT` (which endpoint to probe) and
    // `EPISTEMIC_GRAPH_SPARQL_SERVICE_ALLOW` (the fail-closed allowlist) are set.
    #[cfg(feature = "sparql-fuseki")]
    {
        let outcome = epistemic_graph::server::sparql_service::startup_health_check();
        match outcome {
            epistemic_graph::server::sparql_service::HealthCheckOutcome::Reachable { .. } => {
                info!("{}", outcome.summary());
            }
            epistemic_graph::server::sparql_service::HealthCheckOutcome::NotConfigured => {
                tracing::debug!("{}", outcome.summary());
            }
            _ => tracing::warn!("{}", outcome.summary()),
        }
    }
}
async fn spawn_federated_listener(
    _state: &Arc<tokio::sync::RwLock<ServerState>>,
    federated_addr_arg: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "federation-search")]
    let state = _state;
    // ── Super-cluster federated search (CONCEPT:EG-KG.ontology.federation-client) ──────────────────
    // Opt-in AND feature-gated: the `/federated` listener starts ONLY when built
    // `--features federation-search` AND --federated-addr / EPISTEMIC_GRAPH_FEDERATED_ADDR
    // is set. With the feature off, or unset, this is a no-op. Fans a read query across the
    // peers in EPISTEMIC_GRAPH_FEDERATION_PEERS AND the local store, then merges the
    // partials. Deploy-configurable (EG-022): a bare enable token binds the safe localhost
    // default `127.0.0.1:7900`.
    let federated_addr = resolve_listener_addr(federated_addr_arg, "127.0.0.1:7900");
    #[cfg(feature = "federation-search")]
    if let Some(ref federated_addr) = federated_addr {
        let listener = tokio::net::TcpListener::bind(federated_addr).await?;
        info!(
            "Federated search: serving super-cluster /federated on http://{}/federated",
            federated_addr
        );
        let fed_state = state.clone();
        tokio::spawn(async move {
            epistemic_graph::server::federation::serve(listener, fed_state).await;
        });
    }
    #[cfg(not(feature = "federation-search"))]
    if federated_addr.is_some() {
        tracing::warn!(
            "--federated-addr ignored: binary built without the `federation-search` feature"
        );
    }
    Ok(())
}
async fn spawn_graphql_listener(
    _state: &Arc<tokio::sync::RwLock<ServerState>>,
    graphql_addr_arg: Option<&str>,
    _graphql_max_connections: usize,
    _graphql_max_session_secs: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "graphql")]
    let state = _state;
    #[cfg(feature = "graphql")]
    let graphql_max_connections = _graphql_max_connections;
    #[cfg(feature = "graphql")]
    let graphql_max_session_secs = _graphql_max_session_secs;
    // ── GraphQL subscription SSE carrier (CONCEPT:EG-KG.compute.cdc-event-emit) ────────────────
    // Opt-in AND feature-gated: the listener starts ONLY when built `--features graphql`
    // AND --graphql-addr / EPISTEMIC_GRAPH_GRAPHQL_ADDR is set. With the feature off, or
    // unset, this is a no-op. The listener is loopback-only because its HTTP framing
    // does not terminate TLS; a same-host TLS reverse proxy is the sole remote exposure
    // path. Every accepted request carries an eg2 envelope signed over the exact graph,
    // request id, and subscription document, then passes graph ACL + RLS on every frame.
    let graphql_requested = graphql_addr_arg
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let graphql_explicitly_disabled = graphql_requested.is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "0" | "off" | "false" | "no" | "disabled"
        )
    });
    let graphql_addr = resolve_listener_addr(graphql_requested, "127.0.0.1:7879");
    if graphql_requested.is_some() && !graphql_explicitly_disabled && graphql_addr.is_none() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "GraphQL subscription address must be a valid loopback host and port",
        )
        .into());
    }
    #[cfg(feature = "graphql")]
    if let Some(ref graphql_addr) = graphql_addr {
        let graphql_config = epistemic_graph::server::graphql_sub::GraphQlSubscriptionConfig::new(
            graphql_max_connections,
            graphql_max_session_secs,
        )
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
        let listener = tokio::net::TcpListener::bind(graphql_addr).await?;
        if !listener.local_addr()?.ip().is_loopback() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "GraphQL subscription SSE must bind to loopback; use a same-host TLS reverse proxy for remote access",
            )
            .into());
        }
        info!(
            "GraphQL: serving authenticated subscription SSE carrier on http://{}/graphql/subscribe",
            graphql_addr
        );
        let gql_state = state.clone();
        tokio::spawn(async move {
            epistemic_graph::server::graphql_sub::serve(listener, gql_state, graphql_config).await;
        });
    }
    #[cfg(not(feature = "graphql"))]
    if graphql_addr.is_some() {
        tracing::warn!("--graphql-addr ignored: binary built without the `graphql` feature");
    }
    Ok(())
}
async fn spawn_obs_listener(
    _state: &Arc<tokio::sync::RwLock<ServerState>>,
    obs_addr_arg: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "obs")]
    let state = _state;
    // ── Observability log ingestion (CONCEPT:AU-KG.ingest.self-ingest/161) ─────────────────
    // Opt-in AND feature-gated: the listener starts ONLY when built `--features obs`
    // AND --obs-addr / EPISTEMIC_GRAPH_OBS_ADDR is set. With the feature off, or
    // unset, this is a no-op. Ingests logs (OTLP/HTTP, Elasticsearch `_bulk`/`_doc`,
    // JSON-lines) into eg-tsdb series + eg-text full-text indices and rolls Parquet
    // segments into the blob CAS. Self-contained (its own ObsState under the persist
    // dir). Deploy-configurable (EG-022): a bare enable token binds the safe localhost
    // default `127.0.0.1:5080` (O2's log-ingest port); a bare port binds loopback:port.
    let obs_addr = resolve_listener_addr(obs_addr_arg, "127.0.0.1:5080");
    #[cfg(feature = "obs")]
    if let Some(ref obs_addr) = obs_addr {
        use epistemic_graph::server::obs::{
            ObsState, DEFAULT_FLUSH_RECORDS, OBS_FLUSH_RECORDS_ENV,
        };
        let flush = std::env::var(OBS_FLUSH_RECORDS_ENV)
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_FLUSH_RECORDS);
        let (persist_dir, selected_blob) = {
            let server = state.read().await;
            (
                server.persist_dir.clone(),
                server.blob.as_ref().map(|cursors| cursors.store.clone()),
            )
        };
        match ObsState::open_with_blob_store(persist_dir.as_deref(), flush, selected_blob).await {
            Ok(obs_state) => {
                let listener = tokio::net::TcpListener::bind(obs_addr).await?;
                info!(
                    "Observability: serving log ingestion (OTLP/ES/JSON-lines) on http://{}",
                    obs_addr
                );
                let obs_state = std::sync::Arc::new(obs_state);

                #[cfg(feature = "traces")]
                spawn_obs_trace_persist_sweep(&obs_state);

                let obs_security_state = state.clone();
                tokio::spawn(async move {
                    epistemic_graph::server::obs::serve_with_security(
                        listener,
                        obs_state,
                        obs_security_state,
                    )
                    .await;
                });
            }
            Err(e) => tracing::error!(
                "--obs-addr {}: failed to open ingest state: {}",
                obs_addr,
                e
            ),
        }
    }
    #[cfg(not(feature = "obs"))]
    if obs_addr.is_some() {
        tracing::warn!("--obs-addr ignored: binary built without the `obs` feature");
    }
    Ok(())
}
async fn wait_for_periodic_tick(ticker: &mut tokio::time::Interval) {
    ticker.tick().await;
}

#[cfg(feature = "traces")]
async fn persist_traces_tick(
    traces_obs_state: std::sync::Arc<epistemic_graph::server::obs::ObsState>,
) {
    if let Err(error) = traces_obs_state.persist_traces().await {
        tracing::warn!(
            %error,
            "trace snapshot sweep: persist_traces failed, will retry next tick"
        );
    }
}

fn spawn_periodic_sweep<F, Fut>(interval_secs: u64, metric_name: &'static str, sweep: F)
where
    F: Fn() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    if interval_secs == 0 {
        return;
    }
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        wait_for_periodic_tick(&mut ticker).await; // consume the immediate first tick
        loop {
            wait_for_periodic_tick(&mut ticker).await;
            let loop_started = std::time::Instant::now();
            sweep().await;
            epistemic_graph::metrics::loop_tick(metric_name, loop_started.elapsed().as_secs_f64());
        }
    });
}

#[cfg(feature = "traces")]
fn spawn_obs_trace_persist_sweep(
    obs_state: &std::sync::Arc<epistemic_graph::server::obs::ObsState>,
) {
    // ── Trace durable-tier sweep (BUG-016, CONCEPT:EG-OS.observability.trace-assembly) ─────
    // Periodically snapshot the native span store to durable storage
    // (`ObsState::persist_traces`) so a restart does not silently drop
    // every span the in-memory hot tier holds -- the in-RAM store stays
    // the search/assembly path unchanged; this is the WAL-checkpoint-
    // shaped durable cold tier BUG-016's frozen design calls for.
    // Reuses the SAME interval-task cadence shape the provenance-
    // anchoring sweep above establishes -- NO new scheduler. OFF by
    // default: arm with `EPISTEMIC_GRAPH_OBS_TRACES_PERSIST_SECS=N`.
    #[cfg(feature = "traces")]
    {
        let interval_secs = std::env::var("EPISTEMIC_GRAPH_OBS_TRACES_PERSIST_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);
        if interval_secs > 0 {
            let traces_obs_state = obs_state.clone();
            info!(
                "Observability: durably snapshotting native trace spans every {}s \
                 (BUG-016, CONCEPT:EG-OS.observability.trace-assembly)",
                interval_secs
            );
            spawn_periodic_sweep(interval_secs, "obs_traces_persist", move || {
                persist_traces_tick(traces_obs_state.clone())
            });
        }
    }
}
async fn spawn_viz_interactive_listener(
    _state: &Arc<tokio::sync::RwLock<ServerState>>,
    viz_interactive_addr_arg: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "viz-interactive")]
    let state = _state;
    // ── Interactive native-visualization surface (D-VZ-1 lane V3b) ─────────
    // Opt-in AND feature-gated: the listener starts ONLY when built `--features
    // viz-interactive` AND --viz-interactive-addr /
    // EPISTEMIC_GRAPH_VIZ_INTERACTIVE_ADDR is set. Eagerly constructs the
    // shared `VizEngineState` (rather than leaving it to lazy-init on the
    // first `Method::Viz` RPC call, `handlers::viz::engine_state`) and installs
    // it on `ServerState` BEFORE spawning the listener, so the RPC render path
    // and this HTTP path always share the SAME persistent ColumnStore/render
    // cache/provenance -- never two independent engines silently diverging.
    let viz_interactive_addr = resolve_listener_addr(viz_interactive_addr_arg, "127.0.0.1:5090");
    #[cfg(feature = "viz-interactive")]
    if let Some(ref viz_interactive_addr) = viz_interactive_addr {
        let viz_persist_dir = state.read().await.persist_dir.clone();
        let engine = std::sync::Arc::new(epistemic_graph::server::viz_engine::VizEngineState::new(
            viz_persist_dir.as_deref(),
        ));
        state.write().await.viz_engine = Some(engine.clone());
        match tokio::net::TcpListener::bind(viz_interactive_addr).await {
            Ok(listener) => {
                info!(
                    "Native visualization: serving the interactive WebGPU/WebGL2 client + \
                     viewport-tile protocol on http://{}",
                    viz_interactive_addr
                );
                let viz_state = state.clone();
                tokio::spawn(async move {
                    epistemic_graph::server::viz_interactive::serve(
                        listener,
                        engine,
                        Some(viz_state),
                    )
                    .await;
                });
            }
            Err(e) => tracing::error!(
                "--viz-interactive-addr {}: failed to bind listener: {}",
                viz_interactive_addr,
                e
            ),
        }
    }
    #[cfg(not(feature = "viz-interactive"))]
    if viz_interactive_addr.is_some() {
        tracing::warn!(
            "--viz-interactive-addr ignored: binary built without the `viz-interactive` feature"
        );
    }
    Ok(())
}
async fn spawn_iceberg_listener(
    _state: &Arc<tokio::sync::RwLock<ServerState>>,
    iceberg_addr_arg: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "lake-rest")]
    let state = _state;
    // ── Iceberg-REST catalog (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns, INT-P2-3) ────────────────────────────
    // Opt-in AND feature-gated: the listener starts ONLY when built `--features
    // lake-rest` AND --iceberg-addr / EPISTEMIC_GRAPH_ICEBERG_ADDR is set. With the
    // feature off, or unset, this is a no-op. Serves the standards Iceberg-REST
    // catalog surface (config/namespaces/tables/load-table/commit-table) over the
    // tables the `lake` materialization tier writes. Deploy-configurable (EG-022): a
    // bare enable token binds the safe localhost default `127.0.0.1:8181`.
    let iceberg_addr = resolve_listener_addr(iceberg_addr_arg, "127.0.0.1:8181");
    #[cfg(feature = "lake-rest")]
    if let Some(ref iceberg_addr) = iceberg_addr {
        // `lake-rest` implies `lake` implies `blob`, so the CAS is always configured
        // (`Some`) in any build reaching this arm — `blob` is never independently off.
        let (lake_handle, blob_store) = {
            let s = state.read().await;
            (s.lake.clone(), s.blob.as_ref().map(|b| b.store.clone()))
        };
        match blob_store {
            Some(store) => {
                let listener = tokio::net::TcpListener::bind(iceberg_addr).await?;
                info!(
                    "Iceberg-REST: serving the standards catalog surface on http://{}/v1/config",
                    iceberg_addr
                );
                let lake_security_state = state.clone();
                tokio::spawn(async move {
                    epistemic_graph::server::lake::rest::serve_with_security(
                        listener,
                        lake_handle,
                        store,
                        lake_security_state,
                    )
                    .await;
                });
            }
            None => tracing::error!(
                "--iceberg-addr {}: no blob CAS available (the `lake`/`blob` features need a persist dir)",
                iceberg_addr
            ),
        }
    }
    #[cfg(not(feature = "lake-rest"))]
    if iceberg_addr.is_some() {
        tracing::warn!("--iceberg-addr ignored: binary built without the `lake-rest` feature");
    }
    Ok(())
}
async fn spawn_lake_materialize_sweep(_state: &Arc<tokio::sync::RwLock<ServerState>>) {
    #[cfg(feature = "lake")]
    let state = _state;
    // ── WAL/series → lakehouse materialization sweep (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns, INT-P2-3) ────
    // Opt-in AND feature-gated: only runs when built `--features lake` AND a positive
    // interval is configured via EPISTEMIC_GRAPH_LAKE_MATERIALIZE_INTERVAL_SECS
    // (0/unset ⇒ disabled — the standing sweep never starts; a caller can still drive
    // `LakeManager` directly). Each tick lists every tsdb series and incrementally
    // drains any new points into its lake table (the WAL-drain engine-side seam
    // `eg-lake`'s own docs describe as a documented follow-up this closes). Runs on
    // the blocking pool (redb + Parquet encode + the optional OpenLineage HTTP push
    // are all synchronous work).
    #[cfg(feature = "lake")]
    {
        let interval_secs: u64 =
            std::env::var(epistemic_graph::server::lake::LAKE_MATERIALIZE_INTERVAL_ENV)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
        if interval_secs > 0 {
            let sweep_state = state.clone();
            spawn_periodic_sweep(interval_secs, "lake_materialize", move || {
                let sweep_state = sweep_state.clone();
                async move {
                    // `lake` implies `blob` + `tsdb`, so both are always configured
                    // (`Some`) here — neither is ever independently off in this build.
                    // A18: this is engine-internal system maintenance, not a client
                    // request — no caller identity, no signed envelope to check, the
                    // SAME precedent `server::registry_reaper`'s stale-lease reaper and
                    // the cold-offload sweep already establish for a periodic interval
                    // task that reads `ServerState` directly rather than minting a
                    // synthetic signed `Request` and re-entering `dispatch()`. The old
                    // client-carrier gate here was always the WRONG check for a task
                    // with no carrier at all, not a security boundary being relaxed.
                    let (lake_handle, tsdb, store) = {
                        let s = sweep_state.read().await;
                        (
                            s.lake.clone(),
                            s.tsdb_store.clone(),
                            s.blob.as_ref().map(|b| b.store.clone()),
                        )
                    };
                    let (Some(tsdb), Some(store)) = (tsdb, store) else {
                        tracing::warn!(
                            "Lake materialize sweep: no tsdb/blob store configured, skipping tick"
                        );
                        return;
                    };
                    let outcome = tokio::task::spawn_blocking(move || {
                        let mut drained = 0usize;
                        let mut errors = 0usize;
                        match tsdb.list_series() {
                            Ok(series) => {
                                for series_id in series {
                                    match lake_handle.drain_series(store.as_ref(), &tsdb, &series_id) {
                                        Ok(Some(_)) => drained += 1,
                                        Ok(None) => {}
                                        Err(e) => {
                                            errors += 1;
                                            tracing::warn!(
                                                "Lake materialize sweep: series {series_id} failed: {e}"
                                            );
                                        }
                                    }
                                }
                            }
                            Err(e) => tracing::warn!("Lake materialize sweep: list_series failed: {e}"),
                        }
                        (drained, errors)
                    })
                    .await;
                    if let Ok((drained, errors)) = outcome {
                        if drained > 0 || errors > 0 {
                            tracing::info!(
                                "Lake materialize sweep: {drained} series drained, {errors} errors"
                            );
                        }
                    }
                }
            });
        }
    }
}
async fn spawn_pgwire_listener(
    _state: &Arc<tokio::sync::RwLock<ServerState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "pgwire")]
    let state = _state;
    // ── Postgres wire-protocol shim (CONCEPT:AU-KG.query.raw-python) ───────────────────
    // Opt-in AND feature-gated: the listener starts ONLY when the binary is built
    // `--features pgwire` AND EPISTEMIC_GRAPH_PGWIRE_ADDR is set. With the feature
    // off, or on but unset, this is a no-op and the engine runs exactly as today.
    // Deploy-configurable (CONCEPT:EG-OS.config.configurable-listeners): the addr is env-driven
    // (EPISTEMIC_GRAPH_PGWIRE_ADDR); a bare enable token binds the safe localhost
    // default `127.0.0.1:5433`; explicit non-loopback addresses are admitted only
    // when pgwire's own startup policy proves native TLS is configured. The other
    // auxiliary listeners continue using the loopback-only resolver below.
    #[cfg(feature = "pgwire")]
    if let Some(addr) = epistemic_graph::server::pgwire::resolve_listener_addr(
        std::env::var(epistemic_graph::server::pgwire::PGWIRE_ADDR_ENV)
            .ok()
            .as_deref(),
        "127.0.0.1:5433",
    ) {
        let pg_auth_secret = state.read().await.auth_secret.clone();
        let pg_auth_mode =
            epistemic_graph::server::pgwire::PgWireAuthMode::resolve(&pg_auth_secret)?;
        let prepared_pgwire = epistemic_graph::server::pgwire::prepare_startup_policy(
            &addr,
            pg_auth_secret,
            pg_auth_mode,
        )
        .await?;
        let pg_state = state.clone();
        info!("pgwire: enabling the configured listener (TLS policy applies)");
        tokio::spawn(async move {
            if let Err(e) =
                epistemic_graph::server::pgwire::serve_prepared(pg_state, prepared_pgwire).await
            {
                tracing::error!("pgwire server error: {}", e);
            }
        });
    }
    Ok(())
}
async fn spawn_sqlite_listener(
    _state: &Arc<tokio::sync::RwLock<ServerState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "sqlite-wire")]
    let state = _state;
    // ── SQLite-compatible served surface (CONCEPT:EG-KG.query.concept-3) ────────────────
    // Opt-in AND feature-gated: the listener starts ONLY when built `--features
    // sqlite-wire` AND EPISTEMIC_GRAPH_SQLITE_ADDR is set. With the feature off, or on
    // but unset, this is a no-op. SQLite has no wire protocol, so this speaks a tiny
    // NDJSON-over-TCP request/response line protocol, translating SQLite-dialect SQL and
    // running it through the SAME shared `WireSession` the pgwire shim uses (EG-074).
    // Deploy-configurable (CONCEPT:EG-OS.config.configurable-listeners): a bare enable token binds the safe localhost
    // default `127.0.0.1:5461`; non-loopback requires the protected-ingress policy.
    #[cfg(feature = "sqlite-wire")]
    if let Some(addr) = resolve_listener_addr(
        std::env::var(epistemic_graph::server::sqlite_wire::SQLITE_ADDR_ENV)
            .ok()
            .as_deref(),
        "127.0.0.1:5461",
    ) {
        match tokio::net::TcpListener::bind(&addr).await {
            Ok(listener) => {
                info!(
                    "sqlite-wire: serving SQLite-dialect SQL (NDJSON) on {}",
                    addr
                );
                let sq_state = state.clone();
                tokio::spawn(async move {
                    epistemic_graph::server::sqlite_wire::serve(listener, sq_state).await;
                });
            }
            Err(e) => tracing::error!("sqlite-wire bind {} failed: {}", addr, e),
        }
    }
    Ok(())
}
async fn spawn_mysql_listener(
    _state: &Arc<tokio::sync::RwLock<ServerState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "mysql-wire")]
    let state = _state;
    // ── MySQL / MariaDB wire-protocol listener (CONCEPT:EG-KG.query.kg-2) ──────────
    // Opt-in AND feature-gated: the listener starts ONLY when the binary is built
    // `--features mysql-wire` AND EPISTEMIC_GRAPH_MYSQL_ADDR is set. With the feature
    // off, or on but unset, this is a no-op and the engine runs exactly as today.
    // Deploy-configurable (CONCEPT:EG-OS.config.configurable-listeners): a bare enable token binds the safe localhost
    // default `127.0.0.1:3306`; non-loopback requires the protected-ingress policy.
    // The hand-rolled MySQL protocol reuses the SAME `WireSession` execute→classify→exec
    // core as pgwire (CONCEPT:EG-KG.compute.subsystems-reference), so a MySQL driver/ORM runs SQL over `nodes`/`edges`.
    #[cfg(feature = "mysql-wire")]
    if let Some(addr) = resolve_listener_addr(
        std::env::var(epistemic_graph::server::mysql_wire::MYSQL_ADDR_ENV)
            .ok()
            .as_deref(),
        "127.0.0.1:3306",
    ) {
        let mysql_auth_secret = state.read().await.auth_secret.clone();
        let mysql_auth_mode =
            epistemic_graph::server::mysql_wire::MysqlAuthMode::resolve(&mysql_auth_secret)?;
        epistemic_graph::server::mysql_wire::validate_startup_policy(
            &addr,
            &mysql_auth_secret,
            mysql_auth_mode,
        )?;
        let my_state = state.clone();
        info!("mysql-wire: enabling the configured loopback listener");
        tokio::spawn(async move {
            if let Err(e) = epistemic_graph::server::mysql_wire::serve_with_auth(
                &addr,
                my_state,
                mysql_auth_mode,
            )
            .await
            {
                tracing::error!("mysql-wire server error: {}", e);
            }
        });
    }
    Ok(())
}
async fn spawn_authenticated_wire_listener<Serve, ServeFuture>(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    addr: Option<String>,
    service_name: &'static str,
    enable_message: &'static str,
    validate: fn(&str, &str) -> std::io::Result<()>,
    serve: Serve,
) -> Result<(), Box<dyn std::error::Error>>
where
    Serve: FnOnce(String, Arc<tokio::sync::RwLock<ServerState>>) -> ServeFuture + Send + 'static,
    ServeFuture: std::future::Future<Output = std::io::Result<()>> + Send + 'static,
{
    if let Some(addr) = addr {
        let auth_secret = state.read().await.auth_secret.clone();
        validate(&addr, &auth_secret)?;
        let listener_state = state.clone();
        info!("{enable_message}");
        tokio::spawn(async move {
            if let Err(error) = serve(addr, listener_state).await {
                tracing::error!("{service_name} server error: {}", error);
            }
        });
    }
    Ok(())
}
macro_rules! define_authenticated_wire_listener {
    (
        $function:ident,
        $feature:literal,
        $module:ident,
        $addr_env:ident,
        $default_addr:literal,
        $service_name:literal,
        $enable_message:literal $(,)?
    ) => {
        async fn $function(
            _state: &Arc<tokio::sync::RwLock<ServerState>>,
        ) -> Result<(), Box<dyn std::error::Error>> {
            #[cfg(feature = $feature)]
            {
                spawn_authenticated_wire_listener(
                    _state,
                    resolve_listener_addr(
                        std::env::var(epistemic_graph::server::$module::$addr_env)
                            .ok()
                            .as_deref(),
                        $default_addr,
                    ),
                    $service_name,
                    $enable_message,
                    epistemic_graph::server::$module::validate_startup_policy,
                    |addr, state| async move {
                        epistemic_graph::server::$module::serve(&addr, state).await
                    },
                )
                .await?;
            }
            Ok(())
        }
    };
}

// ── MSSQL TDS wire-protocol listener (CONCEPT:EG-KG.query.hand-rolled-tds-server) ─────────────────
// Opt-in AND feature-gated, mirroring pgwire: the listener starts ONLY when the
// binary is built `--features mssql-wire` AND EPISTEMIC_GRAPH_MSSQL_ADDR is set.
// With the feature off, or on but unset, this is a no-op. Deploy-configurable
// (CONCEPT:EG-OS.config.configurable-listeners): a bare enable token binds the safe localhost default
// `127.0.0.1:1433`. Direct TDS is authenticated loopback-only; remote clients
// terminate TLS/mTLS at an identity-binding gateway that forwards to loopback.
define_authenticated_wire_listener!(
    spawn_mssql_listener,
    "mssql-wire",
    mssql_wire,
    MSSQL_ADDR_ENV,
    "127.0.0.1:1433",
    "mssql-wire",
    "mssql-wire: enabling the configured authenticated loopback listener",
);

// ── AMQP 0.9.1 wire-protocol listener (CONCEPT:EG-KG.compute.message-broker-exchanges) ────────────────
// Opt-in AND feature-gated, mirroring the SQL wires: the listener starts ONLY when
// the binary is built `--features amqp-wire` AND EPISTEMIC_GRAPH_AMQP_ADDR is set.
// With the feature off, or on but unset, this is a no-op. Deploy-configurable
// (CONCEPT:EG-OS.config.configurable-listeners): a bare enable token binds the safe localhost default
// `127.0.0.1:5672`. Direct AMQP is authenticated loopback-only; remote clients
// terminate TLS/mTLS at an identity-binding gateway. Maps AMQP
// exchange/queue/basic.* onto the `broker` primitives (KG-2.303 queue) via dispatch.
define_authenticated_wire_listener!(
    spawn_amqp_listener,
    "amqp-wire",
    amqp_wire,
    AMQP_ADDR_ENV,
    "127.0.0.1:5672",
    "amqp-wire",
    "amqp-wire: enabling the configured authenticated loopback listener",
);

// ── Neo4j Bolt wire-protocol listener (CONCEPT:EG-KG.query.bolt-wire-protocol) ─────────────────
// Opt-in AND feature-gated, mirroring the SQL wires: the listener starts ONLY when
// the binary is built `--features bolt-wire` AND EPISTEMIC_GRAPH_BOLT_ADDR is set.
// With the feature off, or on but unset, this is a no-op. Deploy-configurable
// (CONCEPT:EG-OS.config.configurable-listeners): a bare enable token binds the safe localhost default
// `127.0.0.1:7687` (the Neo4j default); non-loopback requires the protected-ingress
// policy. A native hand-rolled Bolt v4.4 server (PackStream v2 + chunked framing)
// that routes RUN's Cypher straight to the eg-query cypher engine, so a Neo4j driver
// runs Cypher over a graph directly.
define_authenticated_wire_listener!(
    spawn_bolt_listener,
    "bolt-wire",
    bolt_wire,
    BOLT_ADDR_ENV,
    "127.0.0.1:7687",
    "bolt-wire",
    "bolt-wire: enabling the configured loopback listener",
);
async fn spawn_redis_listener(
    _state: &Arc<tokio::sync::RwLock<ServerState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "redis-wire")]
    let state = _state;
    // ── Redis RESP wire-protocol listener (CONCEPT:EG-KG.ontology.resp2-resp3-codec-round) ────────────────
    // Opt-in AND feature-gated, mirroring the SQL wires: the listener starts ONLY when
    // the binary is built `--features redis-wire` AND EPISTEMIC_GRAPH_REDIS_ADDR is set.
    // With the feature off, or on but unset, this is a no-op. Deploy-configurable
    // (CONCEPT:EG-OS.config.configurable-listeners): a bare enable token binds the safe localhost default
    // `127.0.0.1:6379` (the Redis default). Direct Redis is authenticated
    // loopback-only; remote clients terminate TLS/mTLS at an identity-binding
    // gateway. Each verified principal receives a pseudonymous isolated keyspace.
    #[cfg(feature = "redis-wire")]
    if let Some(addr) = resolve_listener_addr(
        std::env::var(epistemic_graph::server::redis_wire::REDIS_ADDR_ENV)
            .ok()
            .as_deref(),
        "127.0.0.1:6379",
    ) {
        let redis_state = state.clone();
        info!("redis-wire: serving Redis RESP protocol on {}", addr);
        tokio::spawn(async move {
            if let Err(e) = epistemic_graph::server::redis_wire::serve(&addr, redis_state).await {
                tracing::error!("redis-wire server error: {}", e);
            }
        });
    }
    Ok(())
}
// ── MQTT 3.1.1 wire-protocol listener (CONCEPT:EG-KG.query.mqtt-packet-codec) ────────────────
// Opt-in AND feature-gated, mirroring the SQL wires: the listener starts ONLY when
// the binary is built `--features mqtt-wire` AND EPISTEMIC_GRAPH_MQTT_ADDR is set.
// With the feature off, or on but unset, this is a no-op. Deploy-configurable
// (CONCEPT:EG-OS.config.configurable-listeners): a bare enable token binds the safe localhost default
// `127.0.0.1:1883` (the MQTT default). Direct MQTT is authenticated loopback-only;
// remote clients terminate TLS/mTLS at an identity-binding gateway. A native
// hand-rolled MQTT server mapping CONNECT/PUBLISH/SUBSCRIBE onto
// the `broker` topic exchange (KG-2.303 queue) via dispatch, so an MQTT client
// pub/subs directly against the engine.
define_authenticated_wire_listener!(
    spawn_mqtt_listener,
    "mqtt-wire",
    mqtt_wire,
    MQTT_ADDR_ENV,
    "127.0.0.1:1883",
    "mqtt-wire",
    "mqtt-wire: enabling the configured authenticated loopback listener",
);

// ── STOMP 1.2 wire-protocol listener (CONCEPT:EG-KG.ontology.stomp-frame-codec-unit) ─────────────────
// Opt-in AND feature-gated, mirroring the SQL wires: the listener starts ONLY when
// the binary is built `--features stomp-wire` AND EPISTEMIC_GRAPH_STOMP_ADDR is set.
// With the feature off, or on but unset, this is a no-op. Deploy-configurable
// (CONCEPT:EG-OS.config.configurable-listeners): a bare enable token binds the safe localhost default
// `127.0.0.1:61613` (the STOMP default). Direct STOMP is authenticated
// loopback-only; remote clients terminate TLS/mTLS at an identity-binding gateway.
// A native hand-rolled STOMP text-frame server mapping SEND/SUBSCRIBE onto
// the `broker` primitives (destinations → exchange + per-subscription queues) via
// dispatch, so a STOMP client pub/subs directly against the engine.
define_authenticated_wire_listener!(
    spawn_stomp_listener,
    "stomp-wire",
    stomp_wire,
    STOMP_ADDR_ENV,
    "127.0.0.1:61613",
    "stomp-wire",
    "stomp-wire: enabling the configured authenticated loopback listener",
);
async fn spawn_s3_listener(
    _state: &Arc<tokio::sync::RwLock<ServerState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "s3-api")]
    let state = _state;
    // ── S3-compatible object-storage REST surface (CONCEPT:EG-KG.ontology.object-put-get-head) ────────
    // Opt-in AND feature-gated, mirroring the obs listener: it starts ONLY when the
    // binary is built `--features s3-api` AND EPISTEMIC_GRAPH_S3_ADDR is set. With the
    // feature off, or on but unset, this is a no-op. Deploy-configurable
    // (CONCEPT:EG-OS.config.configurable-listeners): a bare enable token binds the safe localhost default
    // `127.0.0.1:9000` (the MinIO default); non-loopback requires the protected-ingress
    // policy. A hand-rolled S3 REST API over the content-addressed BLOB CAS + the
    // durable KV listing index, with mandatory SigV4 authentication. Startup fails
    // closed unless both access and secret credentials are runtime-injected.
    #[cfg(feature = "s3-api")]
    if let Some(addr) = resolve_listener_addr(
        std::env::var(epistemic_graph::server::s3::S3_ADDR_ENV)
            .ok()
            .as_deref(),
        "127.0.0.1:9000",
    ) {
        let s3_state = state.clone();
        let selected_blob = {
            state
                .read()
                .await
                .blob
                .as_ref()
                .map(|cursors| cursors.store.clone())
        };
        info!("s3-api: serving S3-compatible REST surface on {}", addr);
        tokio::spawn(async move {
            if let Err(e) =
                epistemic_graph::server::s3::serve_with_blob_store(&addr, s3_state, selected_blob)
                    .await
            {
                tracing::error!("s3-api server error: {}", e);
            }
        });
    }
    Ok(())
}
async fn spawn_kvcache_listener(
    _state: &Arc<tokio::sync::RwLock<ServerState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "kvcache-server")]
    let state = _state;
    // ── Remote KV-cache HTTP surface (CONCEPT:EG-KG.backend.is-configured-so-co) ─────────────────────
    // Opt-in AND feature-gated, mirroring the s3 listener: it starts ONLY when the
    // binary is built `--features kvcache-server` AND EPISTEMIC_GRAPH_KVCACHE_ADDR is
    // set. With the feature off, or on but unset, this is a no-op. Exposes the
    // `eg-kvcache` shared, content-addressed backend (EG-186) over HTTP so parallel
    // vLLM/LMCache instances SHARE KV blocks by token-hash; a bare enable token binds
    // the safe localhost default `127.0.0.1:9130`. Startup fails closed unless JWT
    // validation or a runtime-injected bearer secret is configured.
    #[cfg(feature = "kvcache-server")]
    if let Some(addr) = resolve_listener_addr(
        std::env::var(epistemic_graph::server::kvcache_http::KVCACHE_ADDR_ENV)
            .ok()
            .as_deref(),
        "127.0.0.1:9130",
    ) {
        let kvcache_state = state.clone();
        info!(
            "kvcache-server: serving shared KV-cache HTTP surface on {}",
            addr
        );
        tokio::spawn(async move {
            if let Err(e) =
                epistemic_graph::server::kvcache_http::serve_with_security(&addr, kvcache_state)
                    .await
            {
                tracing::error!("kvcache-server error: {}", e);
            }
        });
    }
    Ok(())
}
async fn recover_durable_catalog(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Recover the durable catalog first; graph material is paged in on demand.
    // This bounded current path is the only served startup mode.
    let persistence_for_load = { state.read().await.persistence.clone() };
    if let Some(p) = &persistence_for_load {
        match p.load_catalog(state).await {
            Ok(n) => info!(
                "durable catalog recovered: {n} graph(s); material pages load on first access"
            ),
            Err(error) => {
                return Err(
                    format!("durable recovery failed; refusing availability: {error}").into(),
                );
            }
        }
        p.register_graph(
            "__commons__",
            "__commons__",
            epistemic_graph::protocol::GraphType::Commons,
        )
        .await
        .map_err(|error| format!("failed to register the mandatory commons graph: {error}"))?;

        let factory = std::sync::Arc::new(
            epistemic_graph::server::persistence::read_through::BackendReadThroughFactory::new(
                p.clone(),
            ),
        );
        state
            .write()
            .await
            .registry
            .set_read_through_factory(factory);
        let materializer = std::sync::Arc::new(
            epistemic_graph::server::persistence::read_through::BackendGraphMaterializer::new(
                p.clone(),
            ),
        );
        state.write().await.registry.set_materializer(materializer);
        info!("durable read-through and bounded lazy materialization enabled");

        // ── Time-series STARTUP RECONCILIATION (CONCEPT:EG-KG.backend.ts-startup-reconcile, L16) ──
        // EG-P0-4 replays a cross-modal-committed measurement into the served
        // `series.redb` right after the graph-shard commit succeeds, but a crash
        // strictly BETWEEN those two commits can leave the shard ahead of the served
        // store (documented residual). Run this ONCE here — after the redb backend has
        // (re)loaded and before the server accepts traffic — so any such residual from a
        // prior crash never lingers. No-op when either the backend isn't redb or no
        // tsdb store is configured; idempotent + a true no-op on a clean-shutdown boot
        // (see `RedbBackend::reconcile_time_series`'s doc comment for the guarantee).
        #[cfg(all(feature = "redb", feature = "tsdb"))]
        if let Some(redb) = p.as_redb() {
            if let Some(series) = state.read().await.tsdb_store.clone() {
                match redb.reconcile_time_series(&series).await {
                    Ok(report) if report.series_reconciled > 0 => info!(
                        "time-series startup reconciliation (CONCEPT:EG-KG.backend.ts-startup-reconcile): {} \
                         series / {} point(s) replayed into the served store (recovered \
                         from a crash between the two EG-P0-4 commits)",
                        report.series_reconciled, report.points_replayed
                    ),
                    Ok(_) => {}
                    Err(error) => return Err(format!(
                        "time-series startup reconciliation failed; refusing availability: {error}"
                    )
                    .into()),
                }
            }
        }
    }
    Ok(())
}

async fn spawn_reasoning_cascade_and_ann_sweep(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    decay_interval: u64,
    decay_half_life: f64,
    decay_floor: f64,
) {
    // ── Reasoning auto-cascade (W3.6/E16, opt-in) ─────────────────────────────────
    // CDC-triggered, debounced OWL/RL closure re-materialization, ONE graph at a
    // time, ONLY for graphs named in `REASON_ON_WRITE` (config-contract style,
    // never default-on — materializing a closure on every write is real,
    // ontology-size-dependent CPU cost). Unset/empty ⇒ no cascade is installed on
    // the CDC hub and NO background task is spawned at all — the write path is
    // byte-for-byte what it was before this feature existed.
    #[cfg(all(feature = "owl", feature = "streaming"))]
    {
        let cascade = std::sync::Arc::new(
            epistemic_graph::server::reasoning_cascade::ReasoningCascade::from_env(),
        );
        if cascade.is_active() {
            if let Some(hub) = state.read().await.cdc.as_ref() {
                hub.install_reasoning_cascade(cascade.clone());
            }
            info!(
                "Reasoning cascade (REASON_ON_WRITE) armed: debounce {}ms",
                cascade.debounce_window().as_millis()
            );
            epistemic_graph::server::reasoning_cascade::spawn(state.clone(), cascade);
        }
    }
    // CONCEPT:EG-KG.storage.incremental-text / .incremental-temporal / .incremental-derived-owl —
    // install the server-layer secondary-index factory so a committed write batch
    // maintains the text / temporal / derived-OWL indexes INCREMENTALLY through the
    // per-graph IndexManager seam (the D3 remainder), instead of drop-and-rebuild. The
    // registry registers the per-graph indexes onto every recovered graph and every
    // future `create_graph`. A build without text/tsdb/owl links none of this.
    #[cfg(any(feature = "text", feature = "tsdb", feature = "owl"))]
    {
        let mut factory = server::secondary_indexes::ServerIndexFactory::new();
        #[cfg(feature = "tsdb")]
        {
            if let Some(series) = state.read().await.tsdb_store.clone() {
                factory = factory.with_series(series);
            }
        }
        #[cfg(feature = "text")]
        {
            let text_dir = state
                .read()
                .await
                .persist_dir
                .clone()
                .map(|d| std::path::Path::new(&d).join("text"));
            factory = factory.with_text_dir(text_dir);
        }
        state
            .write()
            .await
            .registry
            .set_secondary_index_factory(factory.into_arc());
        info!(
            "incremental secondary indexes installed (CONCEPT:EG-KG.storage.incremental-text \
             / .incremental-temporal / .incremental-derived-owl) — text/temporal/derived-OWL \
             maintained per committed write batch"
        );
    }
    // CONCEPT:EG-KG.storage.semantic-index-directory — warm the semantic ANN index OFF the request path. The
    // cold-start bug: the FIRST `semantic_search` after a restart triggered a full
    // single-threaded IVF-PQ+OPQ build (SVD over a 1024² matrix + k-means over
    // ~168k vectors) INLINE while holding the per-graph lock — minutes pegged on one
    // core, never finishing within the request timeout, so the graph never self-
    // warmed. Here, after recovery, a background task builds the index for every
    // large graph (or REOPENS the live durable generation with no rebuild) so the
    // first query is served by the index, or by an exact brute-force fallback while
    // it warms — never by an inline build. The built index is activated as a new
    // durable generation so subsequent restarts reopen it in milliseconds.
    // Feature-gated: a non-`ann` build is byte-for-byte unchanged.
    //
    // This is trigger 1 of the three in `server::semantic_activation`, and it runs
    // that module's `activate_one` rather than its own copy of the body: the boot
    // task, the dispatch write-path tail and the periodic sweep below must reopen,
    // build and activate identically or a graph's index depends on which trigger
    // happened to fire.
    #[cfg(feature = "ann")]
    {
        let warm_state = state.clone();
        let warm_dir = { warm_state.read().await.persist_dir.clone() };
        tokio::spawn(async move {
            // Snapshot (name, core) under a brief read lock; the heavy build runs
            // OFF the async runtime on a blocking thread.
            let cores: Vec<(String, std::sync::Arc<epistemic_graph::graph::GraphCore>)> = {
                let s = warm_state.read().await;
                s.registry
                    .all_entries()
                    .into_iter()
                    .map(|e| (e.name.clone(), e.core.clone()))
                    .collect()
            };
            let _ = tokio::task::spawn_blocking(move || {
                let mut warmed = 0usize;
                for (name, core) in cores {
                    if epistemic_graph::server::semantic_activation::activate_one(
                        &name,
                        &core,
                        warm_dir.as_deref(),
                    ) {
                        warmed += 1;
                    }
                }
                if warmed > 0 {
                    info!("semantic ANN warm-on-start complete: {warmed} graph(s) ready");
                }
            })
            .await;
        });
    }

    // ── Periodic ANN warm re-check sweep (W0.4 mechanism 2, CONCEPT:EG-KG.storage.semantic-index-directory) ──
    // The boot-time warm task above and the post-write trigger (the dispatch
    // write-path tail) cover graphs warmed at startup and graphs written to
    // through the live request path. Neither runs for a change this node only
    // observes via Raft-replicated apply or a redb-recovery replay (which never
    // pass through the live dispatch tail) — so this backstop re-scans every
    // resident graph on the SAME interval-task cadence as the sweeps above and
    // spawns a warm for any at/above threshold whose index is not ready. O(1)
    // per resident graph and a no-op once nothing needs it; always armed (no env
    // gate needed — unlike cold-offload, index readiness is a baseline
    // correctness/performance concern, not an opt-in memory policy).
    #[cfg(feature = "ann")]
    {
        let sweep_state = state.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
            ticker.tick().await; // consume the immediate first tick
            loop {
                ticker.tick().await;
                let __loop_tick_started = std::time::Instant::now();
                let n = epistemic_graph::server::semantic_activation::sweep_resident_graphs(
                    &sweep_state,
                )
                .await;
                if n > 0 {
                    tracing::info!(
                        "Semantic ANN warm re-check: spawned {} warm(s) for resident graph(s)",
                        n
                    );
                }
                epistemic_graph::metrics::loop_tick(
                    "ann_warm_sweep",
                    __loop_tick_started.elapsed().as_secs_f64(),
                );
            }
        });
    }

    // Periodic Ebbinghaus decay sweep (CONCEPT:EG-KG.compute.graph-compute-engine) — opt-in. Confidence on
    // every node/edge decays toward 0 with a configurable half-life; with a
    // non-zero floor, forgotten facts are pruned. Off by default (interval 0).
    if decay_interval > 0 {
        let dk_state = state.clone();
        let interval = decay_interval;
        let half_life = decay_half_life;
        let floor = decay_floor;
        let prune = decay_floor > 0.0;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval));
            ticker.tick().await; // consume the immediate first tick
            loop {
                ticker.tick().await;
                let __loop_tick_started = std::time::Instant::now();
                let stats =
                    epistemic_graph::persist::decay_all(&dk_state, half_life, floor, prune).await;
                tracing::info!(
                    "Decay sweep: {} nodes / {} edges decayed, {} nodes / {} edges pruned",
                    stats.nodes_decayed,
                    stats.edges_decayed,
                    stats.nodes_pruned,
                    stats.edges_pruned
                );
                epistemic_graph::metrics::loop_tick(
                    "decay_sweep",
                    __loop_tick_started.elapsed().as_secs_f64(),
                );
            }
        });
    }
}

async fn spawn_memory_and_lifecycle_sweeps(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    host_capacity: epistemic_graph::autosize::Capacity,
    txn_ttl_secs: u64,
) {
    spawn_memory_sweeps(state, host_capacity).await;
    spawn_lifecycle_sweeps(state, txn_ttl_secs);
}

async fn spawn_memory_sweeps(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    host_capacity: epistemic_graph::autosize::Capacity,
) {
    // ── Per-graph memory cap (CONCEPT:EG-KG.storage.nonblocking-checkpoint) — degrade, don't OOM ─────────
    // The engine keeps a bounded resident projection over the durable backend, so a graph that
    // exceeds EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH is evicted (LRU) back down to it
    // — the backstop that makes a shard shed working set instead of OOM-killing
    // every tenant. The sweep is periodic so it never touches the write hot path.
    //
    // CONCEPT:AU-KG.backend.b-auto-size (Pi-OOM correctness): the DEFAULT now AUTO-SIZES from
    // effective cgroup-aware RAM instead of being unbounded. An unbounded projection
    // OOM-kills a 1 GiB Pi; a RAM-derived cap bounds a runaway graph's RESIDENT footprint with ZERO data loss
    // — evicted nodes still serve from the durable redb tier (read-through eviction,
    // CONCEPT:EG-KG.storage.read-through-seam-exercised). Any explicit override must
    // remain positive; the safety bound cannot be disabled.
    let max_nodes_per_graph = server_startup::max_nodes_per_graph(&host_capacity);
    let cap_interval = server_startup::env_or_exit(
        "EPISTEMIC_GRAPH_MEMCAP_INTERVAL",
        10,
        |value| {
            value
                .parse::<u64>()
                .ok()
                .filter(|interval| (1..=3_600).contains(interval))
        },
        "EPISTEMIC_GRAPH_MEMCAP_INTERVAL must be between 1 and 3600",
    );
    info!(
        "Memory cap: per-graph max {} nodes, swept every {}s (LRU eviction)",
        max_nodes_per_graph, cap_interval
    );
    let cap_state = state.clone();
    spawn_periodic_sweep(cap_interval, "memcap_sweep", move || {
        server_startup::memcap_tick(cap_state.clone(), max_nodes_per_graph)
    });

    // ── Per-tenant memory budget enforcer (CONCEPT:EG-KG.compute.lane-v, Lane V) ─────
    // Tracks an approximate resident-RAM estimate per TENANT (a tenant owns one or more
    // graphs) and evicts/hibernates a tenant's coldest graphs when it exceeds its byte
    // budget, with a global ceiling + fair per-tenant caps so one hot tenant can't starve
    // others. The default auto-sizes to 40% of the effective system/cgroup memory
    // limit and cannot be disabled. Reuses the durability-
    // gated eviction + hibernation ops, so it never loses data. Periodic — never on the
    // write hot path. Complements the per-GRAPH node cap above (this adds the per-TENANT
    // byte dimension on top).
    #[cfg(feature = "cost")]
    {
        let cost_config = epistemic_graph::cost::CostConfig::from_env().unwrap_or_else(|error| {
            eprintln!("error: {error}");
            std::process::exit(2);
        });
        info!(
            "Memory budget: global ceiling {} bytes, per-tenant {} bytes, swept every {}s \
                 (CONCEPT:EG-KG.compute.lane-v)",
            cost_config.global_ceiling_bytes,
            cost_config.per_tenant_budget_bytes,
            cost_config.interval_secs
        );
        let budget_state = state.clone();
        spawn_periodic_sweep(cost_config.interval_secs, "budget_enforcer", move || {
            server_startup::budget_tick(budget_state.clone(), cost_config)
        });
    }

    // ── Cold-tenant idle offload sweep (CONCEPT:EG-KG.backend.r6-feature, R6) ─────────────
    // Periodically hibernate every graph idle longer than a window (its access recency is
    // tracked by `cold_tracker.touch` on the dispatch read/write path), bounding RAM across
    // many tenants. Reuses the engine's existing interval-task cadence (like the budget
    // enforcer above) — NO new daemon. Durability-gated + read-through-safe (KG-2.191), so
    // an offloaded graph is never lost, only evicted; `__commons__` is never offloaded.
    // OFF by default: arm with `EPISTEMIC_GRAPH_COLD_OFFLOAD_SECS=N` (the idle window in
    // seconds); the sweep then runs every `window` seconds.
    #[cfg(feature = "redb")]
    {
        let window_secs = server_startup::optional_sweep_secs("EPISTEMIC_GRAPH_COLD_OFFLOAD_SECS");
        if window_secs > 0 {
            let cold_state = state.clone();
            let tracker = { cold_state.read().await.cold_tracker.clone() };
            let window = std::time::Duration::from_secs(window_secs);
            info!(
                "Cold-tenant offload: hibernate graphs idle > {}s, swept every {}s \
                 (CONCEPT:EG-KG.backend.r6-feature)",
                window_secs, window_secs
            );
            spawn_periodic_sweep(window_secs, "cold_offload", move || {
                cold_offload_tick(cold_state.clone(), tracker.clone(), window)
            });
        }
    }
}

#[cfg(feature = "redb")]
async fn cold_offload_tick(
    state: Arc<tokio::sync::RwLock<ServerState>>,
    tracker: Arc<epistemic_graph::server::persistence::cold_offload::ColdTenantTracker>,
    window: std::time::Duration,
) {
    let n = epistemic_graph::server::persistence::cold_offload::offload_cold_tenants(
        &state, &tracker, window,
    )
    .await;
    if n > 0 {
        tracing::info!("Cold-tenant offload: hibernated {} idle graph(s)", n);
    }
}

fn spawn_lifecycle_sweeps(state: &Arc<tokio::sync::RwLock<ServerState>>, txn_ttl_secs: u64) {
    // ── Fleet server registry stale-lease reaper (CONCEPT:EG-KG.sharding.server-registry, W2.5) ──
    // `Method::RegisterServer` writes/renews a `:Server` node with a
    // server-computed `lease_expires_at_ms`. This sweep expires (durably
    // removes, CDC-emitting) any `:Server` node whose lease has lapsed --
    // e.g. a fleet server that crashed and stopped heartbeating. Reuses the
    // engine's existing interval-task cadence (like the sweeps above) — NO
    // new daemon. Always armed (unlike cold-offload's opt-in memory policy,
    // an unreaped dead registration is a correctness/staleness concern, not
    // a resource-usage opt-in) at a short default interval so even the
    // minimum 1s `ttl_secs` lease is reaped promptly.
    let reap_interval = epistemic_graph::server::registry_reaper::reap_interval_secs();
    info!(
        "Server registry: reaping expired :Server leases every {}s (CONCEPT:EG-KG.sharding.server-registry)",
        reap_interval
    );
    let reaper_state = state.clone();
    spawn_periodic_sweep(reap_interval, "server_registry_reap", move || {
        server_startup::registry_reap_tick(reaper_state.clone())
    });

    // ── Provenance anchoring (CONCEPT:EG-KG.sharding.row-level-security) ───────────────────────────────
    // Periodically Merkle-anchor every resident graph's `:ToolCall`/`:RunTrace`
    // provenance-node window into the SAME tamper-evident audit chain the
    // `security` feature already maintains, so a tamper of an anchored node's
    // durable content becomes detectable via `Method::AuditProveInclusion`.
    // Reuses the engine's existing interval-task cadence (like the budget/cold-
    // offload sweeps above) — NO new daemon/thread. OFF by default: arm with
    // `EPISTEMIC_GRAPH_PROVENANCE_ANCHOR_SECS=N` (the sweep then runs every `N`
    // seconds). Overhead is bounded regardless of `N`: the window-size-dependent
    // work runs off the writer thread, and an unchanged window commits nothing
    // (see `server::persistence::provenance_anchor`'s module doc).
    #[cfg(feature = "security")]
    {
        let interval_secs =
            server_startup::optional_sweep_secs("EPISTEMIC_GRAPH_PROVENANCE_ANCHOR_SECS");
        if interval_secs > 0 {
            info!(
                "Provenance anchoring: Merkle-anchoring :ToolCall/:RunTrace windows every {}s \
                 (CONCEPT:EG-KG.sharding.row-level-security)",
                interval_secs
            );
            let anchor_state = state.clone();
            spawn_periodic_sweep(interval_secs, "provenance_anchor", move || {
                server_startup::provenance_anchor_tick(anchor_state.clone())
            });
        }
    }

    // ── OCC transaction TTL sweep (CONCEPT:EG-KG.txn.multi-op-occ-acid safety rail) ─────────
    // Auto-roll-back transactions idle past the TTL so an abandoned client never
    // leaks a staged transaction forever. An abandoned txn never committed, so it
    // applied nothing — reclaiming it just frees memory and never touches a graph
    // lock. Sweeps at most every 30s (or sooner for a short TTL).
    let sweep_state = state.clone();
    spawn_periodic_sweep(txn_ttl_secs.clamp(5, 30), "txn_ttl_sweep", move || {
        server_startup::txn_ttl_tick(sweep_state.clone(), txn_ttl_secs)
    });
}

async fn start_raft_and_matview_reload(
    _state: &Arc<tokio::sync::RwLock<ServerState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(any(feature = "raft", feature = "compute-dist", feature = "matview"))]
    let state = _state;
    // ── In-engine Raft replication (CONCEPT:AU-KG.ingest.source-sync-canonical) — cluster tier ──────
    // Only when built `--features raft` AND configured (EPISTEMIC_GRAPH_RAFT_NODE_ID
    // + EPISTEMIC_GRAPH_RAFT_PEERS). When the feature is off, OR on but unconfigured,
    // this block is absent / no-ops and the engine runs single-node exactly as
    // before. Raft replicates the AUTHORITATIVE state, so it requires a persist dir
    // (the redb store) — refuse to start a half-configured cluster loudly.
    #[cfg(feature = "raft")]
    {
        match epistemic_graph::raft::config::RaftClusterConfig::from_env() {
            Ok(Some(cluster_cfg)) => {
                let persist_dir = match state.read().await.persist_dir.clone() {
                    Some(d) => d,
                    None => {
                        eprintln!(
                            "error: Raft is configured (EPISTEMIC_GRAPH_RAFT_NODE_ID) but no \
                             persist dir is set — Raft replicates the authoritative redb store, so \
                             GRAPH_SERVICE_PERSIST_DIR is required."
                        );
                        std::process::exit(2);
                    }
                };
                info!(
                    "Raft cluster mode: node {} of {} peers (CONCEPT:AU-KG.ingest.source-sync-canonical)",
                    cluster_cfg.node_id,
                    cluster_cfg.peers.len()
                );
                // persist_dir is validated above (Raft requires the redb store); the
                // MultiRaft opens its durable log over the SAME backend in ServerState.
                let _ = &persist_dir;
                match epistemic_graph::raft::node::start(cluster_cfg, state.clone()).await {
                    Ok(started) => {
                        // `raft::node::start` atomically installed the routing handle
                        // and durable MultiRaft placement authority into ServerState.
                        // Keep the manager alive through the state-owned Arc while the
                        // recovery checks below complete before listeners serve traffic.
                        // Cross-shard 2PC recovery (CONCEPT:EG-KG.storage.lane-n-increment): resolve any
                        // in-doubt cross-shard txns from the durable prepare/decision
                        // records BEFORE serving — a COMMIT decision re-applies, an
                        // undecided/ABORT clears (presumed-abort). Deterministic from
                        // disk, so this is safe to run unconditionally on every boot.
                        {
                            let backend = state.read().await.persistence.clone();
                            if let Some(backend) = backend {
                                let coord = epistemic_graph::raft::cross_shard_txn::CrossShardCoordinator::new(
                                    started.multi.clone(),
                                    backend,
                                );
                                match coord.recover_in_doubt().await {
                                    Ok(0) => {}
                                    Ok(n) => info!(
                                        "Cross-shard 2PC recovery: resolved {n} in-doubt txn(s) (CONCEPT:EG-KG.storage.lane-n-increment)"
                                    ),
                                    Err(e) => {
                                        eprintln!("error: cross-shard 2PC recovery failed: {e}");
                                        std::process::exit(1);
                                    }
                                }
                            }
                        }
                        info!("Raft node started; writes now route through consensus");
                    }
                    Err(e) => {
                        eprintln!("error: failed to start Raft node: {e}");
                        std::process::exit(1);
                    }
                }
            }
            Ok(None) => {
                info!("Raft feature built but not configured — running single-node");
            }
            Err(e) => {
                eprintln!("error: invalid Raft configuration: {e}");
                std::process::exit(2);
            }
        }
    }

    // ── Distributed-compute materialized-view reload (CONCEPT:EG-KG.storage.feature) ───────
    // On every boot, reload any persisted matviews from the redb durable tier into
    // the in-RAM index so `GetMatView` serves them immediately. A no-op when no
    // matviews were ever created / no redb backend is configured.
    #[cfg(any(feature = "compute-dist", feature = "matview"))]
    match epistemic_graph::server::reload_matviews(state).await {
        Ok(0) => {}
        Ok(n) => {
            info!("Reloaded {n} materialized view(s) from redb (CONCEPT:EG-KG.storage.feature)")
        }
        Err(e) => tracing::warn!("materialized-view reload skipped: {e}"),
    }

    Ok(())
}

#[cfg(test)]
mod listener_policy_tests {
    use super::{
        prepare_socket_directory, resolve_listener_addr, resolve_socket_path,
        resolve_windows_socket_path,
    };

    #[test]
    fn explicit_socket_resolution_is_pure_on_every_platform() {
        assert_eq!(
            resolve_socket_path(Some("operator-selected.sock".to_string())),
            ("operator-selected.sock".to_string(), None)
        );
    }

    #[test]
    fn windows_default_requests_only_the_local_appdata_parent() {
        let local = std::path::PathBuf::from("operator-local-data");
        let resolved = resolve_windows_socket_path(
            Some(local.clone().into_os_string()),
            Some(std::ffi::OsString::from("operator-temp")),
        );
        let expected_parent = local.join("epistemic-graph");
        assert_eq!(
            resolved,
            (
                expected_parent
                    .join("engine.sock")
                    .to_string_lossy()
                    .into_owned(),
                Some(expected_parent),
            )
        );

        let temp =
            resolve_windows_socket_path(None, Some(std::ffi::OsString::from("operator-temp")));
        assert_eq!(temp.1, None);
        assert!(temp.0.ends_with("epistemic-graph.sock"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn socket_directory_preparation_is_awaited_and_propagates_io_errors() {
        let root = tempfile::tempdir().expect("create socket-directory fixture root");
        let parent = root.path().join("nested").join("epistemic-graph");
        prepare_socket_directory(Some(parent.clone()))
            .await
            .expect("prepare socket directory");
        assert!(parent.is_dir());

        let blocker = root.path().join("not-a-directory");
        std::fs::write(&blocker, b"file").expect("write socket-directory blocker");
        let error = prepare_socket_directory(Some(blocker.join("child")))
            .await
            .expect_err("a file cannot contain the socket directory");
        assert!(error
            .to_string()
            .starts_with("could not create socket directory"));
    }

    #[test]
    fn listener_resolution_stays_loopback_by_default() {
        assert_eq!(
            resolve_listener_addr(Some("on"), "127.0.0.1:9101"),
            Some("127.0.0.1:9101".to_string())
        );
        assert_eq!(
            resolve_listener_addr(Some("5433"), "127.0.0.1:9101"),
            Some("127.0.0.1:5433".to_string())
        );
        assert_eq!(
            resolve_listener_addr(Some("0.0.0.0:9101"), "127.0.0.1:9101"),
            None
        );
    }
}
