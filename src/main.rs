#![allow(dead_code)]
#![deny(unsafe_code)]
// CONCEPT:KG-2.19 — Epistemic Graph Service Binary
//
// Entry point for the long-running Tokio service process.
// Parses CLI args, initializes the GraphRegistry, and starts
// the UDS/TCP listener.

use clap::Parser;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

use epistemic_graph::channels::ChannelManager;
use epistemic_graph::isolation::IsolationLayer;
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::{self, ServerState};

#[derive(Parser, Debug)]
#[command(name = "epistemic-graph-server")]
#[command(about = "Tokio-native epistemic graph service")]
struct Args {
    /// Unix Domain Socket path.
    /// Falls back to $GRAPH_SERVICE_SOCKET, then $XDG_RUNTIME_DIR/epistemic-graph.sock,
    /// then /tmp/epistemic-graph.sock.
    #[arg(long, env = "GRAPH_SERVICE_SOCKET")]
    socket_path: Option<String>,

    /// Optional TCP address (e.g., 0.0.0.0:9100). If set, TCP listener is started.
    #[arg(long)]
    tcp_addr: Option<String>,

    /// HMAC-SHA256 shared secret for authentication.
    #[arg(long, env = "GRAPH_SERVICE_AUTH_SECRET", default_value = "")]
    auth_secret: String,

    /// Allow starting WITHOUT an auth secret (insecure; development only).
    /// Also enabled by EPISTEMIC_GRAPH_ALLOW_INSECURE=1|true.
    #[arg(long)]
    allow_insecure: bool,

    /// Directory for checkpoint persistence.
    #[arg(long, env = "GRAPH_SERVICE_PERSIST_DIR")]
    persist_dir: Option<String>,

    /// Auto-checkpoint interval in seconds (0 = disabled).
    #[arg(long, default_value = "300")]
    checkpoint_interval: u64,

    /// Serialize graphs to disk on shutdown.
    #[arg(long, default_value = "true")]
    persist_on_shutdown: bool,

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

    /// GraphQL subscription SSE carrier listener address (e.g. 127.0.0.1:7879), feature
    /// `graphql` (CONCEPT:EG-064). Disabled when unset. Streams a `subscription { … }`
    /// as a live query — a `text/event-stream` frame per graph change. Separate from the
    /// RPC transports and the read/write GraphQL RPC surface (`Method::GraphQl`).
    #[arg(long, env = "EPISTEMIC_GRAPH_GRAPHQL_ADDR")]
    graphql_addr: Option<String>,

    /// Observability log-ingestion HTTP listener address (e.g. 127.0.0.1:5080),
    /// feature `obs` (CONCEPT:EG-160/161). Disabled when unset. Accepts OTLP/HTTP
    /// (`/v1/logs`), Elasticsearch `_bulk`/`_doc`, and JSON-lines log records, landing
    /// them in eg-tsdb series + eg-text full-text indices and rolling Parquet segments
    /// into the blob CAS. Separate from the RPC transports.
    #[arg(long, env = "EPISTEMIC_GRAPH_OBS_ADDR")]
    obs_addr: Option<String>,

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

/// The default TCP loopback endpoint used when AF_UNIX is unavailable for the
/// primary transport (Windows, or any non-unix target). Honors
/// `$GRAPH_SERVICE_TCP_FALLBACK_ADDR` so an operator can pin host:port without a
/// CLI flag; defaults to `127.0.0.1:8765` (loopback-only — never 0.0.0.0).
fn default_tcp_fallback_addr() -> String {
    std::env::var("GRAPH_SERVICE_TCP_FALLBACK_ADDR")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "127.0.0.1:8765".to_string())
}

/// Resolve an optional listener bind address from a deploy-supplied value
/// (CONCEPT:EG-022 — deploy-configurable listeners). The auxiliary HTTP listeners
/// (Prometheus metrics, SPARQL, pgwire) are all opt-in; this lets a deploy turn one
/// on WITHOUT a full `host:port` and WITHOUT a code change:
///   * `None` / empty / `0`|`off`|`false`|`no`|`disabled` ⇒ `None` (listener off).
///   * `1`|`on`|`true`|`yes`|`enabled` ⇒ the safe localhost default `default_addr`.
///   * a bare port (`9101`) ⇒ `127.0.0.1:9101` (loopback — never `0.0.0.0`).
///   * anything else ⇒ taken verbatim as the bind address (an operator pinning a
///     specific interface keeps full control, including binding non-loopback).
fn resolve_listener_addr(value: Option<&str>, default_addr: &str) -> Option<String> {
    let v = value.map(str::trim).filter(|s| !s.is_empty())?;
    match v.to_ascii_lowercase().as_str() {
        "0" | "off" | "false" | "no" | "disabled" => None,
        "1" | "on" | "true" | "yes" | "enabled" => Some(default_addr.to_string()),
        _ if v.chars().all(|c| c.is_ascii_digit()) => Some(format!("127.0.0.1:{v}")),
        _ => Some(v.to_string()),
    }
}

/// True if any legacy snapshot (`.mp`) or WAL (`.wal`) file exists in `dir` —
/// the trigger for the one-time redb-authoritative migration (CONCEPT:KG-2.187).
fn legacy_snapshots_present(dir: &str) -> bool {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok()).any(|e| {
                let p = e.path();
                matches!(
                    p.extension().and_then(|x| x.to_str()),
                    Some("mp") | Some("wal")
                )
            })
        })
        .unwrap_or(false)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Explicit, hardware-sized multi-thread runtime (CONCEPT:KG-2.8 — A4). The
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
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .max_blocking_threads(max_blocking)
        .enable_all()
        .build()?;
    runtime.block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // CONCEPT:EG-091 — install the tracing subscriber. This is the fmt-only
    // subscriber (INFO, no target) UNLESS built with `otel` AND
    // EPISTEMIC_GRAPH_OTLP_ENDPOINT is set, in which case an OTLP batch span
    // exporter is layered on top. Off/unset ⇒ byte-for-byte the prior behavior.
    epistemic_graph::otel::init_tracing();

    let args = Args::parse();
    let socket_path = resolve_socket_path(args.socket_path);

    // ── Security gate: an auth secret is mandatory ───────────────────────
    // An empty secret means every request is accepted unauthenticated. That is
    // never a silent default: the server refuses to start unless the operator
    // explicitly opts in via --allow-insecure or EPISTEMIC_GRAPH_ALLOW_INSECURE=1.
    let allow_insecure = args.allow_insecure
        || std::env::var("EPISTEMIC_GRAPH_ALLOW_INSECURE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
    if args.auth_secret.is_empty() && !allow_insecure {
        eprintln!(
            "error: no auth secret configured — refusing to start.\n\
             Set GRAPH_SERVICE_AUTH_SECRET (or pass --auth-secret) to enable \
             HMAC-SHA256 authentication.\n\
             To intentionally run unauthenticated (development only), pass \
             --allow-insecure or set EPISTEMIC_GRAPH_ALLOW_INSECURE=1."
        );
        std::process::exit(2);
    }

    info!("Starting epistemic-graph-server");
    info!("  UDS: {}", socket_path);

    // ── Hardware capacity auto-detection (CONCEPT:EG-028) ─────────────────
    // Size the concurrency / buffer / per-graph node-cap DEFAULTS from
    // (cpu_count, total_RAM) so the SAME binary is lean + OOM-safe on a Pi 3 and
    // exploits a big box. Detected ONCE; each env var below still overrides its
    // own default. Mirrors `available_parallelism()` (runtime sizing above) +
    // CoalescerConfig::auto + cost.rs's /proc/meminfo read.
    let host_capacity = epistemic_graph::autosize::detect_capacity();
    info!(
        "  Capacity: {} cpu(s), {} MiB RAM, tier {:?} (auto-sizing inflight/WAL/node-cap defaults)",
        host_capacity.cpus,
        host_capacity.total_ram_bytes / (1024 * 1024),
        host_capacity.tier
    );
    if let Some(ref tcp) = args.tcp_addr {
        info!("  TCP: {}", tcp);
    }
    info!(
        "  Auth: {}",
        if args.auth_secret.is_empty() {
            "disabled"
        } else {
            "enabled"
        }
    );
    if args.auth_secret.is_empty() {
        tracing::warn!(
            "SECURITY: running WITHOUT authentication (insecure opt-out is set). \
             Every connection to UDS {}{} is trusted unconditionally. \
             Do NOT expose this server beyond localhost.",
            socket_path,
            match &args.tcp_addr {
                Some(tcp) => format!(" and TCP {}", tcp),
                None => String::new(),
            }
        );
    }

    // ── Single-writer persist-dir guard (CONCEPT:KG-2.8 / OS-5.9, Phase B1) ──
    // Refuse to start if another engine already owns this persist dir; hold the
    // lock for the whole process lifetime so no second engine can clobber our
    // snapshots (the engine-level complement to the Python spawn guard). Kept in
    // `_persist_lock` until run() returns; the kernel releases it on exit/crash.
    let _persist_lock = match &args.persist_dir {
        Some(dir) => match epistemic_graph::persist_lock::acquire(dir) {
            Ok(lock) => {
                info!("Acquired single-writer lock on persist dir {}", dir);
                Some(lock)
            }
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        },
        None => None,
    };

    // Default auto-sizes from cpu count (CONCEPT:EG-028): a Pi sheds early, a big
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
    // Reserved READ-admission lane (CONCEPT:EG-044): a dedicated pool of in-flight
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
    // Per-graph write coalescer (CONCEPT:KG-2.182): batch size auto-sized from cpu
    // count; default ON, opt out with EPISTEMIC_GRAPH_WRITE_COALESCE=0.
    {
        let cfg = epistemic_graph::write_coalescer::CoalescerConfig::auto();
        info!(
            "Write coalescer: batch up to {} ops/lock (queue {}, linger {:?})",
            cfg.max_batch, cfg.queue_capacity, cfg.max_linger
        );
    }

    // ── Off-reactor WAL writer (CONCEPT:KG-2.8, Phase B3) ────────────────
    // When persisting, all WAL file I/O runs on one dedicated thread so durable
    // mutations never block a Tokio worker; fsync is group-committed per
    // EPISTEMIC_GRAPH_WAL_FSYNC (off | each | <ms> | interval, default 100ms).
    // The bounded channel (EPISTEMIC_GRAPH_WAL_QUEUE, default 8192) sheds — loudly
    // — rather than stalling the reactor under a saturated disk.
    // Build the durable persistence backend (CONCEPT:KG-2.177). `snapshot`
    // (default) = today's snapshot RDB + off-reactor WAL; `redb` = the
    // feature-gated write-through tier. Selection is one env read; both own their
    // off-reactor writer internally so the dispatch path only sees the trait.
    // THE FLIP (CONCEPT:KG-2.195): the engine is a SOURCE OF TRUTH out of the box.
    // The persist backend now DEFAULTS to "redb" (was "snapshot"); operators can
    // still force the old rebuildable-cache path with
    // EPISTEMIC_GRAPH_PERSIST_BACKEND=snapshot.
    let backend_env = std::env::var("EPISTEMIC_GRAPH_PERSIST_BACKEND")
        .ok()
        .map(|s| s.trim().to_ascii_lowercase());
    // Whether the operator NAMED a backend (vs. taking the new default). Used to
    // keep the fallback path quiet for the implicit default: a build without the
    // redb feature must boot clean even though "redb" is now the default name.
    let backend_explicit = backend_env.is_some();
    let backend_kind = backend_env.unwrap_or_else(|| "redb".to_string());
    // Is the redb backend actually USABLE in this build? Authoritative mode is only
    // real when the `redb` cargo feature is compiled in AND `redb` is the selected
    // backend (so `RedbBackend::open` is the live PersistenceBackend, not the
    // snapshot fallback). A `redb` request in a build without the feature silently
    // degrades to snapshot+WAL below, so it is NOT authoritative.
    let redb_feature = cfg!(feature = "redb");
    let redb_active = redb_feature && backend_kind == "redb";

    // redb-authoritative mode (CONCEPT:KG-2.187 / KG-2.195), read ONCE at startup.
    // EXPLICIT env (Option<bool>): when the operator sets EPISTEMIC_GRAPH_REDB_AUTHORITATIVE
    // we honor it verbatim; when UNSET it DEFAULTS to ON exactly when the redb
    // backend is active — so a stock redb-bearing build (full/node/cluster/pi) is a
    // durable source of truth by default, while a snapshot or no-redb build stays in
    // the byte-for-byte rebuildable-cache model.
    let redb_authoritative_explicit = std::env::var("EPISTEMIC_GRAPH_REDB_AUTHORITATIVE")
        .ok()
        .map(|s| {
            let s = s.trim().to_ascii_lowercase();
            s == "1" || s == "true" || s == "yes" || s == "on"
        });
    let redb_authoritative = redb_authoritative_explicit.unwrap_or(redb_active);
    // Warn ONLY when an operator EXPLICITLY asked for authoritative mode but the
    // redb backend is not active (snapshot selected, or redb feature not compiled) —
    // a genuine misconfig. The NEW default never trips this, so a snapshot / no-redb
    // build boots clean with no scary warning.
    if redb_authoritative_explicit == Some(true) && !redb_active {
        tracing::warn!(
            "EPISTEMIC_GRAPH_REDB_AUTHORITATIVE is set but the redb backend is not active \
             (EPISTEMIC_GRAPH_PERSIST_BACKEND='{}', redb feature compiled: {}); authoritative \
             mode is IGNORED — it only applies to the redb backend",
            backend_kind,
            redb_feature
        );
    }
    if redb_authoritative && redb_active {
        if args.persist_dir.is_some() {
            info!("redb AUTHORITATIVE mode ON (CONCEPT:KG-2.187): commit-before-ack, eviction gated, backpressure (no drop)");
        } else {
            // Authoritative is requested/defaulted but there is no persist dir, so
            // there is nowhere durable to write — the engine runs IN-MEMORY ONLY and
            // every durable-record path short-circuits. Be loud so an operator does
            // not mistake this for a durable source of truth.
            tracing::warn!(
                "redb authoritative is active but no persist dir is configured \
                 (GRAPH_SERVICE_PERSIST_DIR / --persist-dir) — running IN-MEMORY ONLY; \
                 writes are NOT durable. Set a persist dir to make the engine a source of truth."
            );
        }
    }
    let persistence: Option<
        Arc<dyn epistemic_graph::server::persistence::PersistenceBackend>,
    > = args.persist_dir.as_ref().map(|dir| {
        let policy = epistemic_graph::wal_service::FsyncPolicy::from_env(
            std::env::var("EPISTEMIC_GRAPH_WAL_FSYNC").ok().as_deref(),
        );
        // WAL channel depth: default auto-sizes from cpu count (CONCEPT:EG-028) so a
        // Pi holds little and a big box absorbs bursts. Env override (>0) still wins.
        // (`capacity` here is the queue depth; the host Capacity is `host_capacity`.)
        let capacity = std::env::var("EPISTEMIC_GRAPH_WAL_QUEUE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or_else(|| host_capacity.wal_queue());
        match backend_kind.as_str() {
            #[cfg(feature = "redb")]
            "redb" => {
                info!(
                    "Persistence: redb write-through tier (fsync {:?}, queue {})",
                    policy, capacity
                );
                let b: Arc<dyn epistemic_graph::server::persistence::PersistenceBackend> =
                    match epistemic_graph::server::persistence::redb_backend::RedbBackend::open(
                        dir.clone(),
                        policy,
                        capacity,
                    ) {
                        Ok(b) => Arc::new(b),
                        Err(e) => {
                            eprintln!("error: failed to open redb backend at {dir}: {e}");
                            std::process::exit(1);
                        }
                    };
                b
            }
            other => {
                // Warn about a fallback only when the operator EXPLICITLY named a
                // backend we can't honor (e.g. set redb but the feature isn't
                // compiled, or a typo). The NEW implicit default ("redb" on a build
                // without the feature) falls back to snapshot SILENTLY so a
                // bare/server-only build boots clean (CONCEPT:KG-2.195).
                if other != "snapshot" && backend_explicit {
                    tracing::warn!(
                        "EPISTEMIC_GRAPH_PERSIST_BACKEND='{}' not available in this build; \
                         falling back to snapshot+WAL",
                        other
                    );
                }
                info!(
                    "Persistence: snapshot+WAL (fsync {:?}, queue {})",
                    policy, capacity
                );
                let svc =
                    epistemic_graph::wal_service::WalService::spawn(dir.clone(), policy, capacity);
                let b: Arc<dyn epistemic_graph::server::persistence::PersistenceBackend> = Arc::new(
                    epistemic_graph::server::persistence::snapshot_wal::SnapshotWalBackend::new(
                        Some(svc),
                    ),
                );
                b
            }
        }
    });
    let persistence_shutdown = persistence.clone();

    // OCC ACID transaction limits (CONCEPT:KG-2.180).
    let (txn_ttl_secs, txn_max_per_graph, txn_max_per_agent) =
        epistemic_graph::server::txn_limits_from_env();

    // Native time-series store (CONCEPT:KG-2.210, feature `tsdb`). A durable
    // `series.redb` beside `graph.redb` when a persist dir is set; else a
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
                info!("Time-series store (tsdb): {}", path.display());
                Some(Arc::new(s))
            }
            Err(e) => {
                tracing::error!(
                    "failed to open time-series store at {}: {e}",
                    path.display()
                );
                std::process::exit(1);
            }
        }
    };

    // Opt-in lossless RDF quad table (CONCEPT:KG-2.217, feature `rdf-redb`). A
    // durable `rdf_quads.redb` beside `graph.redb` ONLY when a persist dir is set —
    // with no persist dir the property-graph mapping alone is used (the multi-valued
    // literal extras are reported by `LoadReport`, never silently lost), so there is
    // nothing to durably store. A store-open failure is fatal at boot.
    #[cfg(feature = "rdf-redb")]
    let rdf_quads: Option<Arc<eg_rdf::quads::QuadStore>> = match &args.persist_dir {
        Some(dir) => {
            let path = std::path::Path::new(dir).join("rdf_quads.redb");
            match eg_rdf::quads::QuadStore::open(&path) {
                Ok(s) => {
                    info!("RDF lossless quad store (rdf-redb): {}", path.display());
                    Some(Arc::new(s))
                }
                Err(e) => {
                    tracing::error!("failed to open RDF quad store at {}: {e}", path.display());
                    std::process::exit(1);
                }
            }
        }
        None => None,
    };

    // Streamed content-addressed BLOB substrate (CONCEPT:KG-2.206). The CAS lives
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
                            eprintln!("error: failed to open blob-s3 CAS at {dir}: {e}");
                            std::process::exit(1);
                        }
                    };
                #[cfg(not(feature = "blob-s3"))]
                let store: Arc<dyn epistemic_graph::server::blob::ChunkStore> =
                    match epistemic_graph::server::blob::RedbChunkStore::open(dir) {
                        Ok(s) => Arc::new(s),
                        Err(e) => {
                            eprintln!("error: failed to open blob CAS at {dir}: {e}");
                            std::process::exit(1);
                        }
                    };
                info!(
                    "Blob substrate: content-addressed CAS at {dir}/blob.redb (CONCEPT:KG-2.206)"
                );
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

    // ── Generic Key→Value store (CONCEPT:EG-022, feature `kv`) ───────────
    // A durable `{persist_dir}/kv.redb` when a persist dir is set, else an in-memory
    // scratch map. Built BEFORE `persist_dir` is moved into the struct. A store-open
    // failure is fatal at boot (loud + early), same discipline as the other stores.
    #[cfg(feature = "kv")]
    let kv: Option<Arc<epistemic_graph::server::kv::KvStore>> =
        match epistemic_graph::server::kv::KvStore::open(args.persist_dir.as_deref()) {
            Ok(s) => {
                if s.is_durable() {
                    info!("Key→Value store (kv): durable kv.redb (CONCEPT:EG-022)");
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

    let state = Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: args.auth_secret,
        persist_dir: args.persist_dir,
        persistence,
        // Only honor authoritative mode when the redb backend is actually active
        // (feature compiled AND selected — its `record_durable`/`read_node` are the
        // only real implementations); any other backend / a redb fallback stays in
        // safe write-through/no-op behavior.
        redb_authoritative: redb_authoritative && redb_active,
        max_in_flight: std::sync::Arc::new(tokio::sync::Semaphore::new(max_in_flight)),
        read_admission: std::sync::Arc::new(tokio::sync::Semaphore::new(read_reserved)),
        per_graph_inflight: std::sync::Arc::new(dashmap::DashMap::new()),
        per_graph_inflight_limit,
        write_coalescer: std::sync::Arc::new(
            epistemic_graph::write_coalescer::WriteCoalescerRegistry::from_env(),
        ),
        open_txns: std::sync::Arc::new(dashmap::DashMap::new()),
        txn_id_gen: std::sync::Arc::new(epistemic_graph::server::txn::TxnIdGen::default()),
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
        #[cfg(feature = "rdf-redb")]
        rdf_quads,
        // Change-Data-Capture hub (CONCEPT:KG-2.229/230). In-memory only (a bounded
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
    }));

    // ── Prometheus metrics endpoint (CONCEPT:KG-2.51) ────────────────────
    // Opt-in + deploy-configurable (CONCEPT:EG-022): bound only when
    // --metrics-addr / GRAPH_SERVICE_METRICS_ADDR is set. A bare enable token
    // (`1`/`on`/…) binds the safe localhost default `127.0.0.1:9101`; a bare port
    // binds loopback:port; a full addr is honored verbatim — so a deploy turns the
    // listener on without a code change, and shards still never collide by default.
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

    // ── W3C SPARQL 1.1 HTTP endpoint (CONCEPT:EG-017) ────────────────────
    // Opt-in AND feature-gated: the listener starts ONLY when built `--features
    // sparql-http` AND --sparql-addr / EPISTEMIC_GRAPH_SPARQL_ADDR is set. With the
    // feature off, or unset, this is a no-op and the engine runs exactly as before.
    // Deploy-configurable (CONCEPT:EG-022): a bare enable token binds the safe
    // localhost default `127.0.0.1:7878`; a bare port binds loopback:port; a full
    // addr is honored verbatim.
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

    // ── GraphQL subscription SSE carrier (CONCEPT:EG-064) ────────────────
    // Opt-in AND feature-gated: the listener starts ONLY when built `--features graphql`
    // AND --graphql-addr / EPISTEMIC_GRAPH_GRAPHQL_ADDR is set. With the feature off, or
    // unset, this is a no-op. Streams a GraphQL `subscription { … }` as a live query
    // (re-resolve-on-change) over `text/event-stream`. Deploy-configurable (EG-022): a
    // bare enable token binds the safe localhost default `127.0.0.1:7879`.
    let graphql_addr = resolve_listener_addr(args.graphql_addr.as_deref(), "127.0.0.1:7879");
    #[cfg(feature = "graphql")]
    if let Some(ref graphql_addr) = graphql_addr {
        let listener = tokio::net::TcpListener::bind(graphql_addr).await?;
        info!(
            "GraphQL: serving subscription SSE carrier on http://{}/graphql/subscribe",
            graphql_addr
        );
        let gql_state = state.clone();
        tokio::spawn(async move {
            epistemic_graph::server::graphql_sub::serve(listener, gql_state).await;
        });
    }
    #[cfg(not(feature = "graphql"))]
    if graphql_addr.is_some() {
        tracing::warn!("--graphql-addr ignored: binary built without the `graphql` feature");
    }

    // ── Observability log ingestion (CONCEPT:EG-160/161) ─────────────────
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
        use epistemic_graph::server::obs::{ObsState, DEFAULT_FLUSH_RECORDS, OBS_FLUSH_RECORDS_ENV};
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
                tokio::spawn(async move {
                    epistemic_graph::server::obs::serve(listener, obs_state).await;
                });
            }
            Err(e) => tracing::error!("--obs-addr {}: failed to open ingest state: {}", obs_addr, e),
        }
    }
    #[cfg(not(feature = "obs"))]
    if obs_addr.is_some() {
        tracing::warn!("--obs-addr ignored: binary built without the `obs` feature");
    }

    // ── Postgres wire-protocol shim (CONCEPT:KG-2.189) ───────────────────
    // Opt-in AND feature-gated: the listener starts ONLY when the binary is built
    // `--features pgwire` AND EPISTEMIC_GRAPH_PGWIRE_ADDR is set. With the feature
    // off, or on but unset, this is a no-op and the engine runs exactly as today.
    // Deploy-configurable (CONCEPT:EG-022): the addr is env-driven
    // (EPISTEMIC_GRAPH_PGWIRE_ADDR); a bare enable token binds the safe localhost
    // default `127.0.0.1:5433`, a bare port binds loopback:port, a full addr verbatim.
    // pgwire's OWN internals are untouched — only the bind addr is resolved here.
    #[cfg(feature = "pgwire")]
    if let Some(addr) = resolve_listener_addr(
        std::env::var(epistemic_graph::server::pgwire::PGWIRE_ADDR_ENV)
            .ok()
            .as_deref(),
        "127.0.0.1:5433",
    ) {
        let pg_state = state.clone();
        info!("pgwire: serving Postgres wire protocol on {}", addr);
        tokio::spawn(async move {
            if let Err(e) = epistemic_graph::server::pgwire::serve(&addr, pg_state).await {
                tracing::error!("pgwire server error: {}", e);
            }
        });
    }

    // ── MSSQL TDS wire-protocol listener (CONCEPT:EG-077) ─────────────────
    // Opt-in AND feature-gated, mirroring pgwire: the listener starts ONLY when the
    // binary is built `--features mssql-wire` AND EPISTEMIC_GRAPH_MSSQL_ADDR is set.
    // With the feature off, or on but unset, this is a no-op. Deploy-configurable
    // (CONCEPT:EG-022): a bare enable token binds the safe localhost default
    // `127.0.0.1:1433`, a bare port binds loopback:port, a full addr verbatim.
    #[cfg(feature = "mssql-wire")]
    if let Some(addr) = resolve_listener_addr(
        std::env::var(epistemic_graph::server::mssql_wire::MSSQL_ADDR_ENV)
            .ok()
            .as_deref(),
        "127.0.0.1:1433",
    ) {
        let mssql_state = state.clone();
        info!("mssql-wire: serving MSSQL TDS wire protocol on {}", addr);
        tokio::spawn(async move {
            if let Err(e) = epistemic_graph::server::mssql_wire::serve(&addr, mssql_state).await {
                tracing::error!("mssql-wire server error: {}", e);
            }
        });
    }

    // ── Snapshot persistence (CONCEPT:KG-2.8 / OS-5.9) ───────────────────
    // Load any prior checkpoint for a fast warm restart, then auto-checkpoint on
    // the configured interval. Both no-op when no persist dir is configured.
    // Boot-time recovery + periodic checkpoint route through the chosen backend
    // (CONCEPT:KG-2.177). Both no-op when no persist dir is configured.
    let persistence_for_load = { state.read().await.persistence.clone() };
    if let Some(p) = &persistence_for_load {
        // One-time legacy → redb migration (CONCEPT:KG-2.187). When authoritative and
        // the redb store is EMPTY but legacy snapshot/WAL files exist (an engine that
        // ran on snapshot+WAL before the flag flip), import them into redb FIRST so
        // the authoritative store is seeded loss-free, then proceed. Precedent:
        // persist.rs migrate_legacy_commons (AGENTS.md sanctions a one-time
        // read-old→write-new). The old files are LEFT in place as a backstop.
        let authoritative = { state.read().await.redb_authoritative };
        if authoritative {
            match p.load_all(&state).await {
                Ok(0) => {
                    // Bind `dir` to an OWNED String and DROP the read guard before the
                    // migration body. A `state.read().await` temporary in the `if let`
                    // scrutinee would otherwise live to the end of the `if let` block —
                    // and the body below calls `persist::load_all`/`checkpoint_all`, both
                    // of which take `state.write().await`. A read guard held across that
                    // write acquire is a permanent deadlock: the migration awaits a write
                    // lock that can never be granted, the task parks forever, and the UDS
                    // socket is never bound (CONCEPT:KG-2.200).
                    let migrate_dir = {
                        let s = state.read().await;
                        s.persist_dir.clone()
                    };
                    if let Some(dir) = migrate_dir {
                        if legacy_snapshots_present(&dir) {
                            info!(
                                "redb authoritative: redb store empty but legacy snapshot/WAL \
                                 present at {dir} — importing into redb (one-time migration)"
                            );
                            // Load the legacy snapshot+WAL into the live registry via the
                            // snapshot recovery path, then checkpoint the registry into redb.
                            // Both phases are O(graphs) and run BEFORE the socket binds, so
                            // each logs progress (CONCEPT:KG-2.200) — a silent multi-second
                            // boot on a many-graph homelab is indistinguishable from a hang.
                            let mig_start = std::time::Instant::now();
                            if let Err(e) = epistemic_graph::persist::load_all(&state, None).await {
                                tracing::warn!("legacy snapshot load failed: {e}");
                            }
                            let loaded = { state.read().await.registry.all_entries().len() };
                            info!(
                                "redb authoritative migration: legacy load complete \
                                 ({loaded} graph(s) in registry after {:.1}s) — writing them \
                                 into redb…",
                                mig_start.elapsed().as_secs_f64()
                            );
                            // checkpoint_all writes the WHOLE registry into redb in ONE atomic
                            // transaction: a crash mid-write leaves the txn uncommitted (redb
                            // stays empty), so the next boot re-detects "empty + legacy
                            // present" and re-runs the migration — idempotent + crash-safe, no
                            // half-populated-yet-considered-done store.
                            match p.checkpoint_all(&state).await {
                                Ok(n) => info!(
                                    "redb authoritative migration: imported {n} graph(s) from \
                                     legacy snapshot/WAL into redb in {:.1}s (old files left as \
                                     backstop)",
                                    mig_start.elapsed().as_secs_f64()
                                ),
                                Err(e) => tracing::warn!("redb migration checkpoint failed: {e}"),
                            }
                        }
                    }
                }
                Ok(n) => info!("redb authoritative: loaded {n} graph(s) from redb"),
                Err(e) => tracing::warn!("redb load failed (continuing fresh): {e}"),
            }
        } else if let Err(e) = p.load_all(&state).await {
            tracing::warn!("Snapshot load failed (continuing fresh): {}", e);
        }

        // Install the durable read-through (CONCEPT:KG-2.191) AFTER recovery, only
        // under redb-authoritative mode. This is the single wiring point that lets
        // the per-graph node cap resume EVICTING (memory bounded) without data loss:
        // an evicted node's properties are served back from redb on a RAM miss. It
        // attaches to every recovered graph and to every future one. In the default
        // (rebuildable-cache) model the factory is never installed, so reads and
        // eviction behave byte-for-byte as before.
        if state.read().await.redb_authoritative {
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
            info!(
                "redb authoritative: read-through-on-RAM-miss installed (CONCEPT:KG-2.191) — \
                 per-graph node cap now EVICTS durable nodes (memory bounded, no data loss)"
            );
        }
    }
    // CONCEPT:EG-013 — warm the semantic ANN index OFF the request path. The
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

    if args.checkpoint_interval > 0 {
        if let Some(backend) = { state.read().await.persistence.clone() } {
            let cp_state = state.clone();
            let interval = args.checkpoint_interval;
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval));
                ticker.tick().await; // consume the immediate first tick
                loop {
                    ticker.tick().await;
                    if let Err(e) = backend.checkpoint_all(&cp_state).await {
                        tracing::warn!("Auto-checkpoint failed: {}", e);
                    }
                }
            });
        }
    }
    // Periodic Ebbinghaus decay sweep (CONCEPT:KG-2.16) — opt-in. Confidence on
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
                let stats =
                    epistemic_graph::persist::decay_all(&dk_state, half_life, floor, prune).await;
                tracing::info!(
                    "Decay sweep: {} nodes / {} edges decayed, {} nodes / {} edges pruned",
                    stats.nodes_decayed,
                    stats.edges_decayed,
                    stats.nodes_pruned,
                    stats.edges_pruned
                );
            }
        });
    }

    // ── Per-graph memory cap (CONCEPT:KG-2.8) — degrade, don't OOM ─────────
    // The engine is a rebuildable cache over the durable backend, so a graph that
    // exceeds EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH is evicted (LRU) back down to it
    // — the backstop that makes a shard shed working set instead of OOM-killing
    // every tenant. The sweep is periodic so it never touches the write hot path.
    //
    // CONCEPT:EG-028 (Pi-OOM correctness): the DEFAULT now AUTO-SIZES from total RAM
    // instead of being 0/unbounded. An unbounded default OOM-kills a 1 GiB Pi; a
    // RAM-derived cap bounds a runaway graph's RESIDENT footprint with ZERO data loss
    // — evicted nodes still serve from the durable redb tier (read-through eviction,
    // CONCEPT:KG-2.191). A big box derives an effectively-unbounded cap, so it is not
    // constrained. Setting the env to `0` is the explicit opt-out for "truly
    // unbounded"; any explicit value still wins.
    let max_nodes_per_graph = match std::env::var("EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH") {
        Ok(v) => v
            .trim()
            .parse::<usize>()
            .unwrap_or_else(|_| host_capacity.node_cap()),
        Err(_) => host_capacity.node_cap(),
    };
    if max_nodes_per_graph > 0 {
        let cap_state = state.clone();
        let cap_interval = std::env::var("EPISTEMIC_GRAPH_MEMCAP_INTERVAL")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(10);
        info!(
            "Memory cap: per-graph max {} nodes, swept every {}s (LRU eviction)",
            max_nodes_per_graph, cap_interval
        );
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(cap_interval));
            ticker.tick().await; // consume the immediate first tick
            loop {
                ticker.tick().await;
                let evicted =
                    epistemic_graph::persist::evict_oversized_all(&cap_state, max_nodes_per_graph)
                        .await;
                if evicted > 0 {
                    tracing::info!("Memory cap: evicted {} LRU node(s) over cap", evicted);
                }
            }
        });
    }

    // ── Per-tenant memory budget enforcer (CONCEPT:KG-2.234, Lane V) ─────
    // Tracks an approximate resident-RAM estimate per TENANT (a tenant owns one or more
    // graphs) and evicts/hibernates a tenant's coldest graphs when it exceeds its byte
    // budget, with a global ceiling + fair per-tenant caps so one hot tenant can't starve
    // others. ONE knob (EPISTEMIC_GRAPH_MEMORY_BUDGET) turns it on; the default auto-sizes
    // to 70% of system RAM. Off when the ceiling resolves to 0. Reuses the durability-
    // gated eviction + hibernation ops, so it never loses data. Periodic — never on the
    // write hot path. Complements the per-GRAPH node cap above (this adds the per-TENANT
    // byte dimension on top).
    #[cfg(feature = "cost")]
    {
        let cost_config = epistemic_graph::cost::CostConfig::from_env();
        if cost_config.enabled() {
            let budget_state = state.clone();
            info!(
                "Memory budget: global ceiling {} bytes, per-tenant {} bytes, swept every {}s \
                 (CONCEPT:KG-2.234)",
                cost_config.global_ceiling_bytes,
                cost_config.per_tenant_budget_bytes,
                cost_config.interval_secs
            );
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(std::time::Duration::from_secs(
                    cost_config.interval_secs,
                ));
                ticker.tick().await; // consume the immediate first tick
                loop {
                    ticker.tick().await;
                    let (evicted, hibernated) =
                        epistemic_graph::cost::enforce_memory_budgets(&budget_state, cost_config)
                            .await;
                    if evicted > 0 || hibernated > 0 {
                        tracing::info!(
                            "Memory budget: evicted {} node(s), hibernated {} graph(s) to keep \
                             tenants under budget",
                            evicted,
                            hibernated
                        );
                    }
                }
            });
        }
    }

    // ── Cold-tenant idle offload sweep (CONCEPT:EG-040, R6) ─────────────
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
                 (CONCEPT:EG-040)",
                window_secs, window_secs
            );
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(window);
                ticker.tick().await; // consume the immediate first tick
                loop {
                    ticker.tick().await;
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
                }
            });
        }
    }

    // ── OCC transaction TTL sweep (CONCEPT:KG-2.180 safety rail) ─────────
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
                let now = epistemic_graph::server::txn::now_ms();
                let reclaimed =
                    epistemic_graph::server::txn::sweep_expired_txns(&sweep_state, ttl, now);
                if reclaimed > 0 {
                    tracing::info!(
                        "Txn TTL sweep: rolled back {} idle transaction(s)",
                        reclaimed
                    );
                }
            }
        });
    }

    // ── In-engine Raft replication (CONCEPT:KG-2.188) — cluster tier ──────
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
                    "Raft cluster mode: node {} of {} peers (CONCEPT:KG-2.188)",
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
                        // Cross-shard 2PC recovery (CONCEPT:KG-2.222): resolve any
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
                                        "Cross-shard 2PC recovery: resolved {n} in-doubt txn(s) (CONCEPT:KG-2.222)"
                                    ),
                                    Err(e) => {
                                        eprintln!("error: cross-shard 2PC recovery failed: {e}");
                                        std::process::exit(1);
                                    }
                                }
                            }
                        }
                        // Hold the manager in ServerState (CONCEPT:KG-2.224/2.226) so
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

    // ── Distributed-compute materialized-view reload (CONCEPT:KG-2.227) ───────
    // On every boot, reload any persisted matviews from the redb durable tier into
    // the in-RAM index so `GetMatView` serves them immediately. A no-op when no
    // matviews were ever created / no redb backend is configured.
    #[cfg(feature = "compute-dist")]
    match epistemic_graph::server::reload_matviews(&state).await {
        Ok(0) => {}
        Ok(n) => info!("Reloaded {n} materialized view(s) from redb (CONCEPT:KG-2.227)"),
        Err(e) => tracing::warn!("materialized-view reload skipped: {e}"),
    }

    // ── Graceful shutdown coordination (reference-counted) ────────────────
    // ONE coordinator shared by every accept loop, the SIGTERM/SIGINT handler,
    // and the optional idle watcher. When its signal fires, the accept loop(s)
    // BREAK and the main listener returns, so we fall through to the persistence
    // flush below (which was previously UNREACHABLE — the accept loop looped
    // forever). The durable/redb commit-before-ack semantics are untouched: an
    // already-acked write is already on disk; shutdown only flushes the writer's
    // buffered/in-flight tail and writes a final checkpoint.
    let shutdown = server::ShutdownCoordinator::new();

    // SIGTERM (a supervisor / `kill` / agent-utilities stopping the daemon) and
    // SIGINT (Ctrl-C) both fire the SAME graceful signal, so a supervised stop is
    // a clean checkpointed shutdown in BOTH the persistent and the idle-shutdown
    // modes. On non-unix only Ctrl-C is available.
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

    // Optional reference-counted idle shutdown (CONCEPT:KG-2.223). Only spawned
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
            tokio::spawn(async move {
                if let Err(e) = server::serve_tcp(&addr, tcp_state, tcp_shutdown).await {
                    tracing::error!("TCP server error: {}", e);
                }
            });
        }

        // UDS listener (main loop). Returns when the shutdown signal fires.
        server::serve_uds(&socket_path, state.clone(), shutdown.clone()).await?;
    }

    #[cfg(not(unix))]
    {
        // Non-unix (Windows): Tokio has no UnixListener, so AF_UNIX is unavailable.
        // TCP loopback is the per-platform DEFAULT transport here — an explicit
        // --tcp-addr wins, else GRAPH_SERVICE_TCP_FALLBACK_ADDR, else 127.0.0.1:8765.
        // `socket_path` is still resolved (above) for config/lock parity & logging.
        let _ = &socket_path;
        let addr = args
            .tcp_addr
            .clone()
            .unwrap_or_else(default_tcp_fallback_addr);
        info!(
            "AF_UNIX unavailable on this platform; default transport is TCP loopback: {}",
            addr
        );
        server::serve_tcp(&addr, state.clone(), shutdown.clone()).await?;
    }

    // Graceful shutdown: the accept loop has exited, so flush + fsync any buffered
    // durable writes and write a final checkpoint before exit. Reachable now that
    // the accept loop breaks on the shutdown signal (previously dead code).
    info!("Accept loop stopped — flushing durable state and checkpointing");
    if let Some(p) = &persistence_shutdown {
        if let Err(e) = p.checkpoint_all(&state).await {
            tracing::warn!("Final checkpoint failed: {}", e);
        }
        p.shutdown();
    }
    info!("Shutdown complete");
    Ok(())
}
