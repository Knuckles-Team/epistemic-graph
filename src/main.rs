#![allow(dead_code)]
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
#[cfg(feature = "kafka")]
use epistemic_graph::event_bus;
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    let max_in_flight = std::env::var("EPISTEMIC_GRAPH_MAX_INFLIGHT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(1024);
    info!("Backpressure: max in-flight requests = {}", max_in_flight);

    let state = Arc::new(RwLock::new(ServerState {
        registry: GraphRegistry::new(),
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: args.auth_secret,
        persist_dir: args.persist_dir,
        max_in_flight: std::sync::Arc::new(tokio::sync::Semaphore::new(max_in_flight)),
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
    if let Err(e) = epistemic_graph::persist::load_all(&state).await {
        tracing::warn!("Snapshot load failed (continuing fresh): {}", e);
    }
    if args.checkpoint_interval > 0 {
        let cp_state = state.clone();
        let interval = args.checkpoint_interval;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval));
            ticker.tick().await; // consume the immediate first tick
            loop {
                ticker.tick().await;
                if let Err(e) = epistemic_graph::persist::checkpoint_all(&cp_state).await {
                    tracing::warn!("Auto-checkpoint failed: {}", e);
                }
            }
        });
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

        // Kafka consumer — opt-in `kafka` feature (linux deployments only).
        #[cfg(feature = "kafka")]
        if let Ok(brokers) = std::env::var("KAFKA_BOOTSTRAP_SERVERS") {
            if !brokers.is_empty() {
                let kafka_state = state.clone();
                tokio::spawn(async move {
                    event_bus::start_kafka_consumer(
                        &brokers,
                        "epistemic-graph-consumer",
                        "kg.mutations",
                        kafka_state,
                    )
                    .await;
                });
            }
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

    Ok(())
}
