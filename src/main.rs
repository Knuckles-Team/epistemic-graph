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
#[cfg(feature = "security")]
use std::sync::Arc;
#[cfg(feature = "security")]
use tokio::sync::RwLock;
#[cfg(feature = "security")]
use tracing::info;

#[cfg(feature = "security")]
use epistemic_graph::channels::ChannelManager;
#[cfg(feature = "security")]
use epistemic_graph::isolation::IsolationLayer;
#[cfg(feature = "security")]
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server;
#[cfg(feature = "security")]
use epistemic_graph::server::ServerState;

#[cfg(feature = "full")]
mod performance_probe;

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
/// - **Windows:** `%LOCALAPPDATA%\epistemic-graph\engine.sock` (created lazily by
///   the listener), else `%TEMP%\epistemic-graph.sock`, else
///   `C:\Windows\Temp\epistemic-graph.sock`. NOTE: Tokio has no `UnixListener` on
///   Windows, so this path is only a stable *identifier* / lock anchor — the
///   actual default transport on Windows is TCP loopback (see the transport
///   section). Keeping the value defined preserves parity for config/logging.
fn resolve_socket_path(explicit: Option<String>) -> String {
    if let Some(p) = explicit {
        return p;
    }
    #[cfg(unix)]
    {
        if let Ok(xdg) = std::env::var("XDG_RUNTIME_DIR") {
            let xdg_sock = format!("{}/epistemic-graph.sock", xdg);
            // Prefer XDG if the directory exists
            if std::path::Path::new(&xdg).exists() {
                return xdg_sock;
            }
        }
        "/tmp/epistemic-graph.sock".to_string()
    }
    #[cfg(windows)]
    {
        if let Ok(local) = std::env::var("LOCALAPPDATA") {
            let dir = std::path::Path::new(&local).join("epistemic-graph");
            // Best-effort: ensure the dir exists so the path is usable as an anchor.
            let _ = std::fs::create_dir_all(&dir);
            return dir.join("engine.sock").to_string_lossy().into_owned();
        }
        if let Ok(tmp) = std::env::var("TEMP").or_else(|_| std::env::var("TMP")) {
            return std::path::Path::new(&tmp)
                .join("epistemic-graph.sock")
                .to_string_lossy()
                .into_owned();
        }
        r"C:\Windows\Temp\epistemic-graph.sock".to_string()
    }
    #[cfg(not(any(unix, windows)))]
    {
        std::env::temp_dir()
            .join("epistemic-graph.sock")
            .to_string_lossy()
            .into_owned()
    }
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
    // default `#[tokio::main]` already spins one worker per core, but building the
    // runtime explicitly lets us (a) put a small floor under the worker count so a
    // 1-2 core box (Raspberry Pi) still has runtime threads to overlap I/O, and
    // (b) size the BLOCKING pool that off-reactor CPU work (parse_files, the
    // checkpoint encode, community detection) runs on, so a big box uses every
    // core. This is the seam Phase D's HardwareProfile tunes further.
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2);
    let worker_threads = cores.max(2);
    let max_blocking = (cores * 2).max(4);
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
    let args = Args::parse();

    if args.exact_performance_probe {
        let root = args
            .exact_performance_probe_root
            .as_deref()
            .ok_or("exact performance probe root is required")?;
        #[cfg(feature = "full")]
        {
            performance_probe::run_stdio(root)?;
            return Ok(());
        }
        #[cfg(not(feature = "full"))]
        {
            let _ = root;
            return Err("exact performance probes require the full server binary".into());
        }
    }

    // CONCEPT:EG-OS.observability.tracing-subscriber-init — install the tracing subscriber. This is the fmt-only
    // subscriber (INFO, no target) UNLESS built with `otel` AND
    // EPISTEMIC_GRAPH_OTLP_ENDPOINT is set, in which case an OTLP batch span
    // exporter is layered on top. Off/unset ⇒ byte-for-byte the prior behavior.
    epistemic_graph::otel::init_tracing()?;

    let socket_path = resolve_socket_path(args.socket_path);
    let socket_mode = match server::parse_unix_socket_mode(&args.socket_mode) {
        Ok(mode) => mode,
        Err(reason) => {
            eprintln!(
                "error: invalid --socket-mode/GRAPH_SERVICE_SOCKET_MODE {:?}: {reason}",
                args.socket_mode
            );
            std::process::exit(2);
        }
    };

    // ── Security gate: an auth secret is mandatory ───────────────────────
    if args.auth_secret.is_empty() {
        eprintln!(
            "error: no auth secret configured — refusing to start.\n\
             Set GRAPH_SERVICE_AUTH_SECRET (or pass --auth-secret) to enable \
             HMAC-SHA256 authentication."
        );
        std::process::exit(2);
    }
    if args.persist_dir.is_none() {
        eprintln!(
            "error: the served engine requires an externally configured durable-state directory"
        );
        std::process::exit(2);
    }

    let tcp_tls = match (&args.tcp_tls_cert, &args.tcp_tls_key) {
        (Some(cert_path), Some(key_path)) => Some(server::TcpTlsConfig {
            cert_path: cert_path.clone(),
            key_path: key_path.clone(),
            client_ca_path: args.tcp_tls_client_ca.clone(),
        }),
        (None, None) if args.tcp_tls_client_ca.is_none() => None,
        _ => {
            eprintln!("error: native TCP TLS requires both certificate and private-key material");
            std::process::exit(2);
        }
    };
    if let Some(ref addr) = args.tcp_addr {
        if !native_tcp_addr_is_loopback(addr) && tcp_tls.is_none() {
            eprintln!("error: non-loopback native TCP requires TLS");
            std::process::exit(2);
        }
    }
    if let Some(ref tls) = tcp_tls {
        server::validate_tcp_tls_config(tls)?;
    }

    info!("Starting epistemic-graph-server");
    info!("  UDS: private local socket configured");

    // ── Hardware capacity auto-detection (CONCEPT:AU-KG.backend.b-auto-size) ─────────────────
    // Size the concurrency / buffer / per-graph node-cap DEFAULTS from
    // (cpu_count, total_RAM) so the SAME binary is lean + OOM-safe on a Pi 3 and
    // exploits a big box. Detected ONCE; each env var below still overrides its
    // own default. Mirrors `available_parallelism()` (runtime sizing above) +
    // CoalescerConfig::auto + cost.rs's /proc/meminfo read.
    let host_capacity = epistemic_graph::autosize::detect_capacity();
    info!(
        "  Capacity: {} cpu(s), {} MiB RAM, tier {:?} (auto-sizing inflight/writer/node-cap defaults)",
        host_capacity.cpus,
        host_capacity.total_ram_bytes / (1024 * 1024),
        host_capacity.tier
    );
    if host_capacity.total_ram_bytes == 0 {
        tracing::warn!(
            "  RAM undetectable (non-Linux or a restricted /proc) — defaulting the \
             per-graph node cap to a conservative {} (the same cap a real 1 GiB Pi \
             gets); override with EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH",
            host_capacity.node_cap()
        );
    }
    if args.tcp_addr.is_some() {
        info!(
            "  TCP: configured (tls={}, mtls={})",
            tcp_tls.is_some(),
            args.tcp_tls_client_ca.is_some()
        );
    }
    info!("  Auth: enabled");

    // ── Single-writer durable-store guard ──────────────────────────────────
    // Refuse to start if another engine already owns this persist dir; hold the
    // lock for the whole process lifetime so no second engine can clobber our
    // authoritative rows (the engine-level complement to the Python spawn guard). Kept in
    // `_persist_lock` until run() returns; the kernel releases it on exit/crash.
    let _persist_lock = match &args.persist_dir {
        Some(dir) => match epistemic_graph::persist_lock::acquire(dir) {
            Ok(lock) => {
                info!("Acquired the configured single-writer persistence lock");
                Some(lock)
            }
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        },
        None => None,
    };

    // Default auto-sizes from cpu count (CONCEPT:AU-KG.backend.b-auto-size): a Pi sheds early, a big
    // box admits deep concurrency. Env override (when set > 0) still wins.
    let max_in_flight = std::env::var("EPISTEMIC_GRAPH_MAX_INFLIGHT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| host_capacity.max_inflight());
    // Per-graph fairness cap (Phase C-D): default to a quarter of the global pool
    // so any one hot graph holds at most 25% of capacity and ~4 graphs can saturate
    // the server, instead of a single tenant monopolizing all in-flight slots.
    let per_graph_inflight_limit = std::env::var("EPISTEMIC_GRAPH_MAX_INFLIGHT_PER_GRAPH")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| (max_in_flight / 4).max(1));
    // Reserved READ-admission lane (CONCEPT:EG-KG.coordination.reserved-read-lane): a dedicated pool of in-flight
    // slots that ONLY reads/queries may use, so a write firehose that saturates the
    // global pool + per-graph cap can never shed an interactive MCP read to BUSY.
    // Auto-sized from cpu count (an eighth of the admission cap, floored); env override
    // (when set > 0) still wins.
    let read_reserved = std::env::var("EPISTEMIC_GRAPH_READ_RESERVED")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| host_capacity.read_reserved());
    info!(
        "Backpressure: max in-flight = {} (per-graph cap = {}, reserved read lane = {})",
        max_in_flight, per_graph_inflight_limit, read_reserved
    );
    // Per-graph write coalescer (CONCEPT:EG-KG.sharding.per-graph-write-coalescer): batch size auto-sized from cpu
    // count and always enabled with bounded queues.
    {
        let cfg = epistemic_graph::write_coalescer::CoalescerConfig::auto();
        info!(
            "Write coalescer: batch up to {} ops/lock (queue {}, linger {:?})",
            cfg.max_batch, cfg.queue_capacity, cfg.max_linger
        );
    }

    // The served engine has one durability implementation: authoritative redb.
    // Every acknowledged mutation crosses the commit barrier, and bounded writer
    // queues apply backpressure rather than dropping work. `RedbBackend` (and its
    // owning module) is gated behind the `redb` feature, so a build without it
    // (e.g. the slim `server`-only feature set) cannot reference the concrete
    // type — fail loudly at boot on an attempted `--persist-dir` rather than
    // silently downgrading to in-memory.
    #[cfg(feature = "redb")]
    let persistence: Option<Arc<dyn epistemic_graph::server::persistence::PersistenceBackend>> =
        args.persist_dir.as_ref().map(|dir| {
            let policy = epistemic_graph::durability::DurabilityPolicy::from_env(
                std::env::var("EPISTEMIC_GRAPH_REDB_COMMIT_POLICY")
                    .ok()
                    .as_deref(),
            )
            .unwrap_or_else(|error| {
                eprintln!("error: {error}");
                std::process::exit(2);
            });
            let capacity = std::env::var("EPISTEMIC_GRAPH_REDB_WRITER_QUEUE")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|&n| n > 0)
                .unwrap_or_else(|| host_capacity.writer_queue());
            info!(
                "Persistence: authoritative redb (fsync {:?}, queue {})",
                policy, capacity
            );
            let backend = epistemic_graph::server::persistence::redb_backend::RedbBackend::open(
                dir.clone(),
                policy,
                capacity,
            )
            .unwrap_or_else(|error| {
                eprintln!("error: failed to open durable graph store: {error}");
                std::process::exit(1);
            });
            Arc::new(backend) as Arc<dyn epistemic_graph::server::persistence::PersistenceBackend>
        });
    #[cfg(not(feature = "redb"))]
    let persistence: Option<Arc<dyn epistemic_graph::server::persistence::PersistenceBackend>> = {
        if args.persist_dir.is_some() {
            eprintln!(
                "error: --persist-dir requires a redb-enabled build (the `redb` feature is not compiled in)"
            );
            std::process::exit(2);
        }
        None
    };
    let persistence_shutdown = persistence.clone();

    // OCC ACID transaction limits (CONCEPT:EG-KG.txn.multi-op-occ-acid).
    let (txn_ttl_secs, txn_max_per_graph, txn_max_per_agent) =
        epistemic_graph::server::txn_limits_from_env();

    // Native time-series store (CONCEPT:AU-KG.retrieval.god-nodes-communities, feature `tsdb`). A durable
    // `series.redb` beside the graph shards when a persist dir is set; else a
    // process-temp file (in-memory deployments). Built BEFORE `persist_dir` is moved
    // into the struct. A store-open failure is fatal at boot (loud + early), same
    // discipline as the persistence backend above.
    #[cfg(feature = "tsdb")]
    let tsdb_store: Option<Arc<eg_tsdb::store::SeriesStore>> = {
        let path = match &args.persist_dir {
            Some(dir) => std::path::Path::new(dir).join("series.redb"),
            None => std::env::temp_dir().join(format!("eg-tsdb-{}.redb", std::process::id())),
        };
        match eg_tsdb::store::SeriesStore::open(&path) {
            Ok(s) => {
                info!("Time-series store (tsdb): durable store ready");
                Some(Arc::new(s))
            }
            Err(e) => {
                tracing::error!("failed to open durable time-series store: {e}");
                std::process::exit(1);
            }
        }
    };

    // Streamed content-addressed BLOB substrate (CONCEPT:EG-KG.storage.blob-namespace). The CAS lives
    // in `{persist_dir}/blob.redb`; with no persist dir there is no durable place
    // for the bytes, so the substrate is disabled and the Blob* methods report
    // "not available" (matching the in-memory-only philosophy elsewhere).
    #[cfg(feature = "blob")]
    let (blob, blob_cursor_ttl_secs) = {
        let ttl = std::env::var("EPISTEMIC_GRAPH_BLOB_CURSOR_TTL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(300u64);
        let cursors = match args.persist_dir.as_deref() {
            Some(dir) => {
                #[cfg(feature = "blob-s3")]
                let store: Arc<dyn epistemic_graph::server::blob::ChunkStore> =
                    match epistemic_graph::server::blob::s3::S3ChunkStore::open(dir) {
                        Ok(s) => Arc::new(s),
                        Err(e) => {
                            eprintln!("error: failed to open durable blob-s3 CAS: {e}");
                            std::process::exit(1);
                        }
                    };
                #[cfg(not(feature = "blob-s3"))]
                let store: Arc<dyn epistemic_graph::server::blob::ChunkStore> =
                    match epistemic_graph::server::blob::RedbChunkStore::open(dir) {
                        Ok(s) => Arc::new(s),
                        Err(e) => {
                            eprintln!("error: failed to open durable blob CAS: {e}");
                            std::process::exit(1);
                        }
                    };
                info!("Blob substrate: durable content-addressed CAS ready");
                Some(Arc::new(epistemic_graph::server::blob::BlobCursors::new(
                    store,
                )))
            }
            None => {
                tracing::warn!(
                    "Blob substrate disabled: no persist dir (blob bytes need durable storage)"
                );
                None
            }
        };
        (cursors, ttl)
    };

    // ── Generic Key→Value store (CONCEPT:EG-OS.config.configurable-listeners, feature `kv`) ───────────
    // A durable `{persist_dir}/kv.redb` when a persist dir is set, else an in-memory
    // scratch map. Built BEFORE `persist_dir` is moved into the struct. A store-open
    // failure is fatal at boot (loud + early), same discipline as the other stores.
    #[cfg(feature = "kv")]
    let kv: Option<Arc<epistemic_graph::server::kv::KvStore>> =
        match epistemic_graph::server::kv::KvStore::open(args.persist_dir.as_deref()) {
            Ok(s) => {
                if s.is_durable() {
                    info!("Key→Value store (kv): durable kv.redb (CONCEPT:EG-OS.config.configurable-listeners)");
                } else {
                    info!("Key→Value store (kv): in-memory scratch — no persist dir");
                }
                Some(Arc::new(s))
            }
            Err(e) => {
                eprintln!("error: failed to open kv store: {e}");
                std::process::exit(1);
            }
        };

    // Capture the persist dir for the observability ingest state before `args` is
    // moved into ServerState below (the obs listener owns its own substrate under it).
    #[cfg(feature = "obs")]
    let obs_persist_dir = args.persist_dir.clone();

    // RLS is unconditionally default-deny. Every served request carries a verified
    // tenant context and resolves through provisioned durable identity/RBAC policy
    // before any row is exposed. `run_inner` exists only under `security` (see
    // `run`'s doc comment), so this path is always reachable here.
    let isolation = {
        info!("RLS default-deny ACTIVE: rows require explicit public visibility or an owner grant");
        let isolation = match args.persist_dir.as_deref() {
            Some(dir) => match IsolationLayer::with_persist_dir(dir) {
                Ok(layer) => layer,
                Err(error) => {
                    eprintln!("error: could not open durable identity/RBAC policy: {error}");
                    std::process::exit(1);
                }
            },
            None => {
                eprintln!(
                    "error: secure request context requires --persist-dir / GRAPH_SERVICE_PERSIST_DIR for durable identity policy and replay state"
                );
                std::process::exit(1);
            }
        };
        if let Err(error) = server::validate_verified_request_context_startup(
            &args.auth_secret,
            args.persist_dir.as_deref(),
        ) {
            eprintln!("error: invalid verified request-context configuration: {error}");
            std::process::exit(1);
        }
        if isolation.identity_bootstrap_pending() {
            info!(
                "Identity policy is empty: only a signer-backed current-envelope System identity bootstrap is admitted"
            );
        }
        isolation
    };

    let state = Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation,
        channels: ChannelManager::new(),
        auth_secret: args.auth_secret,
        persist_dir: args.persist_dir,
        persistence,
        max_in_flight: std::sync::Arc::new(tokio::sync::Semaphore::new(max_in_flight)),
        read_admission: std::sync::Arc::new(tokio::sync::Semaphore::new(read_reserved)),
        per_graph_inflight: std::sync::Arc::new(dashmap::DashMap::new()),
        per_graph_inflight_limit,
        write_coalescer: std::sync::Arc::new(
            epistemic_graph::write_coalescer::WriteCoalescerRegistry::new(),
        ),
        routed_write_coalescer: std::sync::Arc::new(
            epistemic_graph::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
        ),
        open_txns: std::sync::Arc::new(dashmap::DashMap::new()),
        txn_id_gen: std::sync::Arc::new(epistemic_graph::server::txn::TxnIdGen),
        txn_ttl_secs,
        txn_max_per_graph,
        txn_max_per_agent,
        #[cfg(feature = "blob")]
        blob,
        #[cfg(feature = "blob")]
        blob_cursor_ttl_secs,
        // Populated below AFTER snapshot recovery, only when built `--features raft`
        // AND configured. Until then (and always in a non-raft build) it is `None`,
        // so the dispatch write path is the single-node path, unchanged.
        #[cfg(feature = "raft")]
        raft: None,
        #[cfg(feature = "raft")]
        multi_raft: None,
        #[cfg(feature = "tsdb")]
        tsdb_store,
        // Change-Data-Capture hub (CONCEPT:EG-KG.query.streaming-cdc-subscriptions/230). In-memory only (a bounded
        // per-graph ring + Notify) — needs no persist dir, so it is always live on a
        // `streaming` build. The dispatch shell emits a change into it after every
        // durable mutation; the streaming handler reads/maintains/serves off it.
        #[cfg(feature = "streaming")]
        cdc: Some(Arc::new(epistemic_graph::server::cdc::CdcHub::new())),
        #[cfg(feature = "wasm-udf")]
        udf_registry: std::sync::Arc::new(eg_wasm::UdfRegistry::new()),
        #[cfg(feature = "compute-dist")]
        matviews: std::sync::Arc::new(parking_lot::Mutex::new(
            epistemic_graph::raft::pregel::MatViewStore::new(),
        )),
        #[cfg(feature = "federation")]
        foreign_sources: std::sync::Arc::new(dashmap::DashMap::new()),
        #[cfg(feature = "kv")]
        kv,
        // LTAP lakehouse materialization manager (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns engine-side seam,
        // INT-P2-3). Process-global + always constructed (empty) on a `lake` build —
        // the periodic drain sweep and the `lake-rest` Iceberg-REST listener below
        // both share this ONE handle.
        #[cfg(feature = "lake")]
        lake: std::sync::Arc::new(epistemic_graph::server::lake::LakeManager::new()),
    }));

    // ── Prometheus metrics endpoint (CONCEPT:EG-KG.txn.per-graph-write-isolation) ────────────────────
    // Opt-in + deploy-configurable (CONCEPT:EG-OS.config.configurable-listeners): bound only when
    // --metrics-addr / GRAPH_SERVICE_METRICS_ADDR is set. A bare enable token
    // (`1`/`on`/…) binds the safe localhost default `127.0.0.1:9101`; a bare port
    // binds loopback:port. A non-loopback address additionally requires the
    // protected-ingress two-key policy.
    let metrics_addr = resolve_listener_addr(args.metrics_addr.as_deref(), "127.0.0.1:9101");
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

    // ── W3C SPARQL 1.1 HTTP endpoint (CONCEPT:EG-KG.query.named-graph-support) ────────────────────
    // Opt-in AND feature-gated: the listener starts ONLY when built `--features
    // sparql-http` AND --sparql-addr / EPISTEMIC_GRAPH_SPARQL_ADDR is set. With the
    // feature off, or unset, this is a no-op and the engine runs exactly as before.
    // Deploy-configurable (CONCEPT:EG-OS.config.configurable-listeners): a bare enable token binds the safe
    // localhost default `127.0.0.1:7878`; a bare port binds loopback:port. A
    // non-loopback address additionally requires the protected-ingress policy.
    let sparql_addr = resolve_listener_addr(args.sparql_addr.as_deref(), "127.0.0.1:7878");
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

    // ── Super-cluster federated search (CONCEPT:EG-KG.ontology.federation-client) ──────────────────
    // Opt-in AND feature-gated: the `/federated` listener starts ONLY when built
    // `--features federation-search` AND --federated-addr / EPISTEMIC_GRAPH_FEDERATED_ADDR
    // is set. With the feature off, or unset, this is a no-op. Fans a read query across the
    // peers in EPISTEMIC_GRAPH_FEDERATION_PEERS AND the local store, then merges the
    // partials. Deploy-configurable (EG-022): a bare enable token binds the safe localhost
    // default `127.0.0.1:7900`.
    let federated_addr = resolve_listener_addr(args.federated_addr.as_deref(), "127.0.0.1:7900");
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

    // ── GraphQL subscription SSE carrier (CONCEPT:EG-KG.compute.cdc-event-emit) ────────────────
    // Opt-in AND feature-gated: the listener starts ONLY when built `--features graphql`
    // AND --graphql-addr / EPISTEMIC_GRAPH_GRAPHQL_ADDR is set. With the feature off, or
    // unset, this is a no-op. The listener is loopback-only because its HTTP framing
    // does not terminate TLS; a same-host TLS reverse proxy is the sole remote exposure
    // path. Every accepted request carries an eg2 envelope signed over the exact graph,
    // request id, and subscription document, then passes graph ACL + RLS on every frame.
    let graphql_requested = args
        .graphql_addr
        .as_deref()
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
            args.graphql_max_connections,
            args.graphql_max_session_secs,
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

    // ── Observability log ingestion (CONCEPT:AU-KG.ingest.self-ingest/161) ─────────────────
    // Opt-in AND feature-gated: the listener starts ONLY when built `--features obs`
    // AND --obs-addr / EPISTEMIC_GRAPH_OBS_ADDR is set. With the feature off, or
    // unset, this is a no-op. Ingests logs (OTLP/HTTP, Elasticsearch `_bulk`/`_doc`,
    // JSON-lines) into eg-tsdb series + eg-text full-text indices and rolls Parquet
    // segments into the blob CAS. Self-contained (its own ObsState under the persist
    // dir). Deploy-configurable (EG-022): a bare enable token binds the safe localhost
    // default `127.0.0.1:5080` (O2's log-ingest port); a bare port binds loopback:port.
    let obs_addr = resolve_listener_addr(args.obs_addr.as_deref(), "127.0.0.1:5080");
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
        match ObsState::open(obs_persist_dir.as_deref(), flush) {
            Ok(obs_state) => {
                let listener = tokio::net::TcpListener::bind(obs_addr).await?;
                info!(
                    "Observability: serving log ingestion (OTLP/ES/JSON-lines) on http://{}",
                    obs_addr
                );
                let obs_state = std::sync::Arc::new(obs_state);

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
                        tokio::spawn(async move {
                            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(
                                interval_secs,
                            ));
                            ticker.tick().await; // consume the immediate first tick
                            loop {
                                ticker.tick().await;
                                let __loop_tick_started = std::time::Instant::now();
                                if let Err(error) = traces_obs_state.persist_traces() {
                                    tracing::warn!(
                                        %error,
                                        "trace snapshot sweep: persist_traces failed, will retry next tick"
                                    );
                                }
                                epistemic_graph::metrics::loop_tick(
                                    "obs_traces_persist",
                                    __loop_tick_started.elapsed().as_secs_f64(),
                                );
                            }
                        });
                    }
                }

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

    // ── Iceberg-REST catalog (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns, INT-P2-3) ────────────────────────────
    // Opt-in AND feature-gated: the listener starts ONLY when built `--features
    // lake-rest` AND --iceberg-addr / EPISTEMIC_GRAPH_ICEBERG_ADDR is set. With the
    // feature off, or unset, this is a no-op. Serves the standards Iceberg-REST
    // catalog surface (config/namespaces/tables/load-table/commit-table) over the
    // tables the `lake` materialization tier writes. Deploy-configurable (EG-022): a
    // bare enable token binds the safe localhost default `127.0.0.1:8181`.
    let iceberg_addr = resolve_listener_addr(args.iceberg_addr.as_deref(), "127.0.0.1:8181");
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
            tokio::spawn(async move {
                let mut ticker =
                    tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                ticker.tick().await; // consume the immediate first tick
                loop {
                    ticker.tick().await;
                    let __loop_tick_started = std::time::Instant::now();
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
                        continue;
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
                    epistemic_graph::metrics::loop_tick(
                        "lake_materialize",
                        __loop_tick_started.elapsed().as_secs_f64(),
                    );
                }
            });
        }
    }

    // ── Postgres wire-protocol shim (CONCEPT:AU-KG.query.raw-python) ───────────────────
    // Opt-in AND feature-gated: the listener starts ONLY when the binary is built
    // `--features pgwire` AND EPISTEMIC_GRAPH_PGWIRE_ADDR is set. With the feature
    // off, or on but unset, this is a no-op and the engine runs exactly as today.
    // Deploy-configurable (CONCEPT:EG-OS.config.configurable-listeners): the addr is env-driven
    // (EPISTEMIC_GRAPH_PGWIRE_ADDR); a bare enable token binds the safe localhost
    // default `127.0.0.1:5433`; non-loopback requires the protected-ingress policy.
    // pgwire's OWN internals are untouched — only the bind addr is resolved here.
    #[cfg(feature = "pgwire")]
    if let Some(addr) = resolve_listener_addr(
        std::env::var(epistemic_graph::server::pgwire::PGWIRE_ADDR_ENV)
            .ok()
            .as_deref(),
        "127.0.0.1:5433",
    ) {
        let pg_auth_secret = state.read().await.auth_secret.clone();
        let pg_auth_mode =
            epistemic_graph::server::pgwire::PgWireAuthMode::resolve(&pg_auth_secret)?;
        epistemic_graph::server::pgwire::validate_startup_policy(
            &addr,
            &pg_auth_secret,
            pg_auth_mode,
        )?;
        let pg_state = state.clone();
        info!("pgwire: enabling the configured loopback listener");
        tokio::spawn(async move {
            if let Err(e) =
                epistemic_graph::server::pgwire::serve_with_auth(&addr, pg_state, pg_auth_mode)
                    .await
            {
                tracing::error!("pgwire server error: {}", e);
            }
        });
    }

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

    // ── MSSQL TDS wire-protocol listener (CONCEPT:EG-KG.query.hand-rolled-tds-server) ─────────────────
    // Opt-in AND feature-gated, mirroring pgwire: the listener starts ONLY when the
    // binary is built `--features mssql-wire` AND EPISTEMIC_GRAPH_MSSQL_ADDR is set.
    // With the feature off, or on but unset, this is a no-op. Deploy-configurable
    // (CONCEPT:EG-OS.config.configurable-listeners): a bare enable token binds the safe localhost default
    // `127.0.0.1:1433`. Direct TDS is authenticated loopback-only; remote clients
    // terminate TLS/mTLS at an identity-binding gateway that forwards to loopback.
    #[cfg(feature = "mssql-wire")]
    if let Some(addr) = resolve_listener_addr(
        std::env::var(epistemic_graph::server::mssql_wire::MSSQL_ADDR_ENV)
            .ok()
            .as_deref(),
        "127.0.0.1:1433",
    ) {
        let mssql_auth_secret = state.read().await.auth_secret.clone();
        epistemic_graph::server::mssql_wire::validate_startup_policy(&addr, &mssql_auth_secret)?;
        let mssql_state = state.clone();
        info!("mssql-wire: enabling the configured authenticated loopback listener");
        tokio::spawn(async move {
            if let Err(e) = epistemic_graph::server::mssql_wire::serve(&addr, mssql_state).await {
                tracing::error!("mssql-wire server error: {}", e);
            }
        });
    }

    // ── AMQP 0.9.1 wire-protocol listener (CONCEPT:EG-KG.compute.message-broker-exchanges) ────────────────
    // Opt-in AND feature-gated, mirroring the SQL wires: the listener starts ONLY when
    // the binary is built `--features amqp-wire` AND EPISTEMIC_GRAPH_AMQP_ADDR is set.
    // With the feature off, or on but unset, this is a no-op. Deploy-configurable
    // (CONCEPT:EG-OS.config.configurable-listeners): a bare enable token binds the safe localhost default
    // `127.0.0.1:5672`. Direct AMQP is authenticated loopback-only; remote clients
    // terminate TLS/mTLS at an identity-binding gateway. Maps AMQP
    // exchange/queue/basic.* onto the `broker` primitives (KG-2.303 queue) via dispatch.
    #[cfg(feature = "amqp-wire")]
    if let Some(addr) = resolve_listener_addr(
        std::env::var(epistemic_graph::server::amqp_wire::AMQP_ADDR_ENV)
            .ok()
            .as_deref(),
        "127.0.0.1:5672",
    ) {
        let amqp_auth_secret = state.read().await.auth_secret.clone();
        epistemic_graph::server::amqp_wire::validate_startup_policy(&addr, &amqp_auth_secret)?;
        let amqp_state = state.clone();
        info!("amqp-wire: enabling the configured authenticated loopback listener");
        tokio::spawn(async move {
            if let Err(e) = epistemic_graph::server::amqp_wire::serve(&addr, amqp_state).await {
                tracing::error!("amqp-wire server error: {}", e);
            }
        });
    }

    // ── Neo4j Bolt wire-protocol listener (CONCEPT:EG-KG.query.bolt-wire-protocol) ─────────────────
    // Opt-in AND feature-gated, mirroring the SQL wires: the listener starts ONLY when
    // the binary is built `--features bolt-wire` AND EPISTEMIC_GRAPH_BOLT_ADDR is set.
    // With the feature off, or on but unset, this is a no-op. Deploy-configurable
    // (CONCEPT:EG-OS.config.configurable-listeners): a bare enable token binds the safe localhost default
    // `127.0.0.1:7687` (the Neo4j default); non-loopback requires the protected-ingress
    // policy. A native hand-rolled Bolt v4.4 server (PackStream v2 + chunked framing)
    // that routes RUN's Cypher straight to the eg-query cypher engine, so a Neo4j driver
    // runs Cypher over a graph directly.
    #[cfg(feature = "bolt-wire")]
    if let Some(addr) = resolve_listener_addr(
        std::env::var(epistemic_graph::server::bolt_wire::BOLT_ADDR_ENV)
            .ok()
            .as_deref(),
        "127.0.0.1:7687",
    ) {
        let bolt_auth_secret = state.read().await.auth_secret.clone();
        epistemic_graph::server::bolt_wire::validate_startup_policy(&addr, &bolt_auth_secret)?;
        let bolt_state = state.clone();
        info!("bolt-wire: enabling the configured loopback listener");
        tokio::spawn(async move {
            if let Err(e) = epistemic_graph::server::bolt_wire::serve(&addr, bolt_state).await {
                tracing::error!("bolt-wire server error: {}", e);
            }
        });
    }

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
    #[cfg(feature = "mqtt-wire")]
    if let Some(addr) = resolve_listener_addr(
        std::env::var(epistemic_graph::server::mqtt_wire::MQTT_ADDR_ENV)
            .ok()
            .as_deref(),
        "127.0.0.1:1883",
    ) {
        let mqtt_auth_secret = state.read().await.auth_secret.clone();
        epistemic_graph::server::mqtt_wire::validate_startup_policy(&addr, &mqtt_auth_secret)?;
        let mqtt_state = state.clone();
        info!("mqtt-wire: enabling the configured authenticated loopback listener");
        tokio::spawn(async move {
            if let Err(e) = epistemic_graph::server::mqtt_wire::serve(&addr, mqtt_state).await {
                tracing::error!("mqtt-wire server error: {}", e);
            }
        });
    }

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
    #[cfg(feature = "stomp-wire")]
    if let Some(addr) = resolve_listener_addr(
        std::env::var(epistemic_graph::server::stomp_wire::STOMP_ADDR_ENV)
            .ok()
            .as_deref(),
        "127.0.0.1:61613",
    ) {
        let stomp_auth_secret = state.read().await.auth_secret.clone();
        epistemic_graph::server::stomp_wire::validate_startup_policy(&addr, &stomp_auth_secret)?;
        let stomp_state = state.clone();
        info!("stomp-wire: enabling the configured authenticated loopback listener");
        tokio::spawn(async move {
            if let Err(e) = epistemic_graph::server::stomp_wire::serve(&addr, stomp_state).await {
                tracing::error!("stomp-wire server error: {}", e);
            }
        });
    }

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
        info!("s3-api: serving S3-compatible REST surface on {}", addr);
        tokio::spawn(async move {
            if let Err(e) = epistemic_graph::server::s3::serve(&addr, s3_state).await {
                tracing::error!("s3-api server error: {}", e);
            }
        });
    }

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

    // Recover the durable catalog first; graph material is paged in on demand.
    // This bounded current path is the only served startup mode.
    let persistence_for_load = { state.read().await.persistence.clone() };
    if let Some(p) = &persistence_for_load {
        match p.load_catalog(&state).await {
            Ok(n) => info!(
                "durable catalog recovered: {n} graph(s); material pages load on first access"
            ),
            Err(error) => {
                return Err(
                    format!("durable recovery failed; refusing availability: {error}").into(),
                )
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
                    Err(error) => {
                        return Err(format!(
                            "time-series startup reconciliation failed; refusing availability: {error}"
                        )
                        .into())
                    }
                }
            }
        }
    }
    #[cfg(feature = "epistemic-tms")]
    epistemic_graph::server::reasoning_projection::spawn(state.clone());
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
    // large graph (or REOPENS a persisted one with no rebuild) so the first query is
    // served by the index, or by an exact brute-force fallback while it warms — never
    // by an inline build. The built index is persisted so subsequent restarts reopen
    // it in milliseconds. Feature-gated: a non-`ann` build is byte-for-byte unchanged.
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
                use epistemic_graph::compute::semantic_ann::ANN_BUILD_THRESHOLD;
                let mut warmed = 0usize;
                for (name, core) in cores {
                    let store = core.semantic_store.read();
                    if store.len() < ANN_BUILD_THRESHOLD {
                        continue; // brute force is exact + fast below the threshold
                    }
                    let idx_dir = warm_dir
                        .as_ref()
                        .map(|d| epistemic_graph::persist::annidx_dir(d, &name));
                    // 1. Try the no-rebuild reopen of a persisted index.
                    if let Some(dir) = &idx_dir {
                        if dir.exists()
                            && store.load_index(dir).is_ok()
                            && store.index_matches_len()
                        {
                            info!(
                                "semantic ANN index reopened (no rebuild) for graph '{}' ({} vectors)",
                                name,
                                store.len()
                            );
                            warmed += 1;
                            continue;
                        }
                    }
                    // 2. One-time build off the query path (logs build_ms, span).
                    let t = std::time::Instant::now();
                    store.warm(&name);
                    if store.is_ready() {
                        warmed += 1;
                        info!(
                            "semantic ANN index warmed for graph '{}' ({} vectors) in {:.1}s",
                            name,
                            store.len(),
                            t.elapsed().as_secs_f64()
                        );
                        // 3. Persist so the next restart REOPENS it (never rebuilds).
                        if let Some(dir) = &idx_dir {
                            if let Err(e) = store.save_index(dir) {
                                tracing::warn!(
                                    "semantic ANN index persist failed for graph '{}': {}",
                                    name,
                                    e
                                );
                            }
                        }
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
                let n =
                    epistemic_graph::server::ann_warm::sweep_resident_graphs(&sweep_state).await;
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
    if args.decay_interval > 0 {
        let dk_state = state.clone();
        let interval = args.decay_interval;
        let half_life = args.decay_half_life;
        let floor = args.decay_floor;
        let prune = args.decay_floor > 0.0;
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

    // ── Per-graph memory cap (CONCEPT:EG-KG.storage.nonblocking-checkpoint) — degrade, don't OOM ─────────
    // The engine keeps a bounded resident projection over the durable backend, so a graph that
    // exceeds EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH is evicted (LRU) back down to it
    // — the backstop that makes a shard shed working set instead of OOM-killing
    // every tenant. The sweep is periodic so it never touches the write hot path.
    //
    // CONCEPT:AU-KG.backend.b-auto-size (Pi-OOM correctness): the DEFAULT now AUTO-SIZES from total RAM
    // instead of being unbounded. An unbounded projection OOM-kills a 1 GiB Pi; a
    // RAM-derived cap bounds a runaway graph's RESIDENT footprint with ZERO data loss
    // — evicted nodes still serve from the durable redb tier (read-through eviction,
    // CONCEPT:EG-KG.storage.read-through-seam-exercised). Any explicit override must
    // remain positive; the safety bound cannot be disabled.
    let max_nodes_per_graph = match std::env::var("EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH") {
        Ok(value) => match value.trim().parse::<usize>() {
            Ok(limit) if limit > 0 => limit,
            _ => {
                eprintln!("error: EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH must be positive");
                std::process::exit(2);
            }
        },
        Err(std::env::VarError::NotPresent) => host_capacity.node_cap(),
        Err(std::env::VarError::NotUnicode(_)) => {
            eprintln!("error: EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH is not valid Unicode");
            std::process::exit(2);
        }
    };
    let cap_state = state.clone();
    let cap_interval = match std::env::var("EPISTEMIC_GRAPH_MEMCAP_INTERVAL") {
        Ok(value) => match value.trim().parse::<u64>() {
            Ok(interval) if (1..=3_600).contains(&interval) => interval,
            _ => {
                eprintln!("error: EPISTEMIC_GRAPH_MEMCAP_INTERVAL must be between 1 and 3600");
                std::process::exit(2);
            }
        },
        Err(std::env::VarError::NotPresent) => 10,
        Err(std::env::VarError::NotUnicode(_)) => {
            eprintln!("error: EPISTEMIC_GRAPH_MEMCAP_INTERVAL is not valid Unicode");
            std::process::exit(2);
        }
    };
    info!(
        "Memory cap: per-graph max {} nodes, swept every {}s (LRU eviction)",
        max_nodes_per_graph, cap_interval
    );
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(cap_interval));
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let loop_started = std::time::Instant::now();
            let evicted =
                epistemic_graph::persist::evict_oversized_all(&cap_state, max_nodes_per_graph)
                    .await;
            if evicted > 0 {
                tracing::info!("Memory cap: evicted {} LRU node(s) over cap", evicted);
            }
            epistemic_graph::metrics::loop_tick(
                "memcap_sweep",
                loop_started.elapsed().as_secs_f64(),
            );
        }
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
        let budget_state = state.clone();
        info!(
            "Memory budget: global ceiling {} bytes, per-tenant {} bytes, swept every {}s \
                 (CONCEPT:EG-KG.compute.lane-v)",
            cost_config.global_ceiling_bytes,
            cost_config.per_tenant_budget_bytes,
            cost_config.interval_secs
        );
        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(std::time::Duration::from_secs(cost_config.interval_secs));
            ticker.tick().await; // consume the immediate first tick
            loop {
                ticker.tick().await;
                let __loop_tick_started = std::time::Instant::now();
                let (evicted, hibernated) =
                    epistemic_graph::cost::enforce_memory_budgets(&budget_state, cost_config).await;
                if evicted > 0 || hibernated > 0 {
                    tracing::info!(
                        "Memory budget: evicted {} node(s), hibernated {} graph(s) to keep \
                             tenants under budget",
                        evicted,
                        hibernated
                    );
                }
                epistemic_graph::metrics::loop_tick(
                    "budget_enforcer",
                    __loop_tick_started.elapsed().as_secs_f64(),
                );
            }
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
        let window_secs = std::env::var("EPISTEMIC_GRAPH_COLD_OFFLOAD_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);
        if window_secs > 0 {
            let cold_state = state.clone();
            let tracker = { cold_state.read().await.cold_tracker.clone() };
            let window = std::time::Duration::from_secs(window_secs);
            info!(
                "Cold-tenant offload: hibernate graphs idle > {}s, swept every {}s \
                 (CONCEPT:EG-KG.backend.r6-feature)",
                window_secs, window_secs
            );
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(window);
                ticker.tick().await; // consume the immediate first tick
                loop {
                    ticker.tick().await;
                    let __loop_tick_started = std::time::Instant::now();
                    let n =
                        epistemic_graph::server::persistence::cold_offload::offload_cold_tenants(
                            &cold_state,
                            &tracker,
                            window,
                        )
                        .await;
                    if n > 0 {
                        tracing::info!("Cold-tenant offload: hibernated {} idle graph(s)", n);
                    }
                    epistemic_graph::metrics::loop_tick(
                        "cold_offload",
                        __loop_tick_started.elapsed().as_secs_f64(),
                    );
                }
            });
        }
    }

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
    {
        let reaper_state = state.clone();
        let reap_interval = epistemic_graph::server::registry_reaper::reap_interval_secs();
        info!(
            "Server registry: reaping expired :Server leases every {}s (CONCEPT:EG-KG.sharding.server-registry)",
            reap_interval
        );
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(reap_interval));
            ticker.tick().await; // consume the immediate first tick
            loop {
                ticker.tick().await;
                let __loop_tick_started = std::time::Instant::now();
                let now_ms = epistemic_graph::server::txn::now_ms();
                let n = epistemic_graph::server::registry_reaper::reap_expired_servers(
                    &reaper_state,
                    now_ms,
                )
                .await;
                if n > 0 {
                    tracing::info!("Server registry: reaped {} expired :Server lease(s)", n);
                }
                epistemic_graph::metrics::loop_tick(
                    "server_registry_reap",
                    __loop_tick_started.elapsed().as_secs_f64(),
                );
            }
        });
    }

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
        let interval_secs = std::env::var("EPISTEMIC_GRAPH_PROVENANCE_ANCHOR_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);
        if interval_secs > 0 {
            let anchor_state = state.clone();
            info!(
                "Provenance anchoring: Merkle-anchoring :ToolCall/:RunTrace windows every {}s \
                 (CONCEPT:EG-KG.sharding.row-level-security)",
                interval_secs
            );
            tokio::spawn(async move {
                let mut ticker =
                    tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                ticker.tick().await; // consume the immediate first tick
                loop {
                    ticker.tick().await;
                    let __loop_tick_started = std::time::Instant::now();
                    let anchored = epistemic_graph::server::persistence::provenance_anchor::sweep(
                        &anchor_state,
                    )
                    .await;
                    if anchored > 0 {
                        tracing::info!(
                            "Provenance anchoring: anchored {} graph(s) this tick",
                            anchored
                        );
                    }
                    epistemic_graph::metrics::loop_tick(
                        "provenance_anchor",
                        __loop_tick_started.elapsed().as_secs_f64(),
                    );
                }
            });
        }
    }

    // ── OCC transaction TTL sweep (CONCEPT:EG-KG.txn.multi-op-occ-acid safety rail) ─────────
    // Auto-roll-back transactions idle past the TTL so an abandoned client never
    // leaks a staged transaction forever. An abandoned txn never committed, so it
    // applied nothing — reclaiming it just frees memory and never touches a graph
    // lock. Sweeps at most every 30s (or sooner for a short TTL).
    {
        let sweep_state = state.clone();
        let ttl = txn_ttl_secs;
        let sweep_interval = ttl.clamp(5, 30);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(sweep_interval));
            ticker.tick().await; // consume the immediate first tick
            loop {
                ticker.tick().await;
                let __loop_tick_started = std::time::Instant::now();
                let now = epistemic_graph::server::txn::now_ms();
                let reclaimed =
                    epistemic_graph::server::txn::sweep_expired_txns(&sweep_state, ttl, now);
                if reclaimed > 0 {
                    tracing::info!(
                        "Txn TTL sweep: rolled back {} idle transaction(s)",
                        reclaimed
                    );
                }
                epistemic_graph::metrics::loop_tick(
                    "txn_ttl_sweep",
                    __loop_tick_started.elapsed().as_secs_f64(),
                );
            }
        });
    }

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
                        // The MultiRaft manager owns the shared listener (runs for the
                        // process lifetime). Keep it alive by storing the handle; the
                        // routing handle goes into ServerState for the dispatch path.
                        state.write().await.raft = Some(started.handle);
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
                        // Hold the manager in ServerState (CONCEPT:EG-KG.storage.100m-tenant/2.226) so
                        // its listener task lives the process lifetime AND the
                        // user-facing cross-group surfaces (multi-graph Commit → 2PC,
                        // online reshard, tenant hibernation) can reach it. The Arc is
                        // kept alive by the field; no leak needed.
                        state.write().await.multi_raft = Some(started.multi.clone());
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
    match epistemic_graph::server::reload_matviews(&state).await {
        Ok(0) => {}
        Ok(n) => {
            info!("Reloaded {n} materialized view(s) from redb (CONCEPT:EG-KG.storage.feature)")
        }
        Err(e) => tracing::warn!("materialized-view reload skipped: {e}"),
    }

    // ── Graceful shutdown coordination (reference-counted) ────────────────
    // ONE coordinator shared by every accept loop, the SIGTERM/SIGINT handler,
    // and the optional idle watcher. When its signal fires, the accept loop(s)
    // BREAK and the main listener returns, so we fall through to the persistence
    // flush below. An acknowledged write is already committed; shutdown only
    // drains the writer's bounded in-flight tail.
    let shutdown = server::ShutdownCoordinator::new();

    // SIGTERM (a supervisor / `kill` / agent-utilities stopping the daemon) and
    // SIGINT (Ctrl-C) both fire the same graceful signal. On non-unix only Ctrl-C
    // is available.
    {
        let sig_coord = shutdown.clone();
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut term = match signal(SignalKind::terminate()) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!("failed to install SIGTERM handler: {e}");
                        return;
                    }
                };
                let mut int = match signal(SignalKind::interrupt()) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!("failed to install SIGINT handler: {e}");
                        return;
                    }
                };
                tokio::select! {
                    _ = term.recv() => info!("Received SIGTERM — graceful shutdown"),
                    _ = int.recv()  => info!("Received SIGINT — graceful shutdown"),
                }
            }
            #[cfg(not(unix))]
            {
                if tokio::signal::ctrl_c().await.is_ok() {
                    info!("Received Ctrl-C — graceful shutdown");
                }
            }
            sig_coord.trigger();
        });
    }

    // Optional reference-counted idle shutdown (CONCEPT:EG-KG.backend.tiny-shared). Only spawned
    // when --idle-shutdown-secs N (N>0); absent/0 ⇒ no watcher ⇒ the engine is
    // long-living/persistent and never self-terminates on idle.
    if args.idle_shutdown_secs > 0 {
        info!(
            "Idle shutdown ARMED: will self-terminate after {}s with zero active connections",
            args.idle_shutdown_secs
        );
        let idle_coord = shutdown.clone();
        let secs = args.idle_shutdown_secs;
        tokio::spawn(async move {
            server::run_idle_watcher(idle_coord, secs).await;
        });
    } else {
        info!("Idle shutdown disabled (persistent mode): engine stays up while idle");
    }

    // ── Transport ───────────────────────────────────────────────────────
    // UDS is the primary transport on unix; Windows has no Unix Domain Sockets,
    // so TCP is the main (and only) transport there.
    #[cfg(unix)]
    {
        // TCP listener (secondary) if configured.
        if let Some(ref tcp_addr) = args.tcp_addr {
            let tcp_state = state.clone();
            let tcp_shutdown = shutdown.clone();
            let addr = tcp_addr.clone();
            let tls = tcp_tls.clone();
            tokio::spawn(async move {
                if let Err(e) = server::serve_tcp(&addr, tcp_state, tcp_shutdown, tls).await {
                    tracing::error!("TCP server error ({:?})", e.kind());
                }
            });
        }

        // UDS listener (main loop). Returns when the shutdown signal fires.
        server::serve_uds(&socket_path, socket_mode, state.clone(), shutdown.clone()).await?;
    }

    #[cfg(not(unix))]
    {
        // Non-unix (Windows): Tokio has no UnixListener, so AF_UNIX is unavailable.
        // TCP loopback is the per-platform DEFAULT transport here — an explicit
        // --tcp-addr wins; otherwise the loopback-only platform default is used.
        // `socket_path`/`socket_mode` are still resolved+validated (above) for
        // config/lock parity & logging.
        let _ = &socket_path;
        let _ = socket_mode;
        let addr = args
            .tcp_addr
            .clone()
            .unwrap_or_else(|| "127.0.0.1:8765".to_string());
        info!("AF_UNIX unavailable; using the configured native TCP transport");
        server::serve_tcp(&addr, state.clone(), shutdown.clone(), tcp_tls).await?;
    }

    // Graceful shutdown: the accept loop has exited, so flush any bounded writer
    // work that had not yet crossed its acknowledgement barrier.
    info!("Accept loop stopped — flushing durable state");
    if let Some(p) = &persistence_shutdown {
        p.shutdown();
    }
    info!("Shutdown complete");
    Ok(())
}

#[cfg(test)]
mod listener_policy_tests {
    use super::resolve_listener_addr;

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
