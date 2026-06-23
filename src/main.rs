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
use tracing::{info, Level};

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
}

fn resolve_socket_path(explicit: Option<String>) -> String {
    if let Some(p) = explicit {
        return p;
    }
    if let Ok(xdg) = std::env::var("XDG_RUNTIME_DIR") {
        let xdg_sock = format!("{}/epistemic-graph.sock", xdg);
        // Prefer XDG if the directory exists
        if std::path::Path::new(&xdg).exists() {
            return xdg_sock;
        }
    }
    "/tmp/epistemic-graph.sock".to_string()
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
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .init();

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

    let max_in_flight = std::env::var("EPISTEMIC_GRAPH_MAX_INFLIGHT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(1024);
    // Per-graph fairness cap (Phase C-D): default to a quarter of the global pool
    // so any one hot graph holds at most 25% of capacity and ~4 graphs can saturate
    // the server, instead of a single tenant monopolizing all in-flight slots.
    let per_graph_inflight_limit = std::env::var("EPISTEMIC_GRAPH_MAX_INFLIGHT_PER_GRAPH")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| (max_in_flight / 4).max(1));
    info!(
        "Backpressure: max in-flight = {} (per-graph cap = {})",
        max_in_flight, per_graph_inflight_limit
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
    let backend_kind = std::env::var("EPISTEMIC_GRAPH_PERSIST_BACKEND")
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "snapshot".to_string());
    // redb-authoritative mode (CONCEPT:KG-2.187), read ONCE at startup. Default OFF
    // → byte-for-byte today's behavior. Only meaningful with the redb backend
    // selected; warn (don't fail) if set without it so a misconfig is loud but the
    // engine still boots in safe write-through mode.
    let redb_authoritative = std::env::var("EPISTEMIC_GRAPH_REDB_AUTHORITATIVE")
        .ok()
        .map(|s| {
            let s = s.trim().to_ascii_lowercase();
            s == "1" || s == "true" || s == "yes" || s == "on"
        })
        .unwrap_or(false);
    if redb_authoritative && backend_kind != "redb" {
        tracing::warn!(
            "EPISTEMIC_GRAPH_REDB_AUTHORITATIVE is set but EPISTEMIC_GRAPH_PERSIST_BACKEND='{}' \
             (not 'redb'); authoritative mode is IGNORED — it only applies to the redb backend",
            backend_kind
        );
    }
    if redb_authoritative && backend_kind == "redb" {
        info!("redb AUTHORITATIVE mode ON (CONCEPT:KG-2.187): commit-before-ack, eviction gated, backpressure (no drop)");
    }
    let persistence: Option<
        Arc<dyn epistemic_graph::server::persistence::PersistenceBackend>,
    > = args.persist_dir.as_ref().map(|dir| {
        let policy = epistemic_graph::wal_service::FsyncPolicy::from_env(
            std::env::var("EPISTEMIC_GRAPH_WAL_FSYNC").ok().as_deref(),
        );
        let capacity = std::env::var("EPISTEMIC_GRAPH_WAL_QUEUE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(8192);
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
                if other != "snapshot" {
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

    let state = Arc::new(RwLock::new(ServerState {
        registry: GraphRegistry::new(),
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: args.auth_secret,
        persist_dir: args.persist_dir,
        persistence,
        // Only honor authoritative mode when the redb backend is actually selected
        // (its `record_durable`/`read_node` are the only real implementations); any
        // other backend stays in safe write-through/no-op behavior.
        redb_authoritative: redb_authoritative && backend_kind == "redb",
        max_in_flight: std::sync::Arc::new(tokio::sync::Semaphore::new(max_in_flight)),
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
    }));

    // ── Prometheus metrics endpoint (CONCEPT:KG-2.51) ────────────────────
    // Opt-in: bound only when --metrics-addr / GRAPH_SERVICE_METRICS_ADDR is
    // set, so shards never collide on a default port.
    if let Some(ref metrics_addr) = args.metrics_addr {
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
                    if let Some(dir) = { state.read().await.persist_dir.clone() } {
                        if legacy_snapshots_present(&dir) {
                            info!(
                                "redb authoritative: redb store empty but legacy snapshot/WAL \
                                 present at {dir} — importing into redb (one-time migration)"
                            );
                            // Load the legacy snapshot+WAL into the live registry via the
                            // snapshot recovery path, then checkpoint the registry into redb.
                            if let Err(e) = epistemic_graph::persist::load_all(&state, None).await {
                                tracing::warn!("legacy snapshot load failed: {e}");
                            }
                            match p.checkpoint_all(&state).await {
                                Ok(n) => info!(
                                    "redb authoritative migration: imported {n} graph(s) from \
                                     legacy snapshot/WAL into redb (old files left as backstop)"
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
    // every tenant. Off by default (0 = unbounded). The sweep is periodic so it
    // never touches the write hot path.
    let max_nodes_per_graph = std::env::var("EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
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

    // ── Transport ───────────────────────────────────────────────────────
    // UDS is the primary transport on unix; Windows has no Unix Domain Sockets,
    // so TCP is the main (and only) transport there.
    #[cfg(unix)]
    {
        // TCP listener (secondary) if configured.
        if let Some(ref tcp_addr) = args.tcp_addr {
            let tcp_state = state.clone();
            let addr = tcp_addr.clone();
            tokio::spawn(async move {
                if let Err(e) = server::serve_tcp(&addr, tcp_state).await {
                    tracing::error!("TCP server error: {}", e);
                }
            });
        }

        // UDS listener (main loop).
        server::serve_uds(&socket_path, state).await?;
    }

    #[cfg(not(unix))]
    {
        // Windows: no UDS — serve on TCP as the main loop.
        let _ = &socket_path; // computed for parity; UDS unavailable here
        let addr = args
            .tcp_addr
            .clone()
            .unwrap_or_else(|| "127.0.0.1:8765".to_string());
        info!("UDS unavailable on this platform; serving on TCP: {}", addr);
        server::serve_tcp(&addr, state).await?;
    }

    // Graceful shutdown: flush + fsync any buffered durable writes before exit.
    if let Some(p) = persistence_shutdown {
        p.shutdown();
    }
    Ok(())
}
