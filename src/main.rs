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
use epistemic_graph::isolation::IsolationLayer;
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::{self, ServerState};
#[cfg(feature = "kafka")]
use epistemic_graph::event_bus;

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

    /// Directory for checkpoint persistence.
    #[arg(long, env = "GRAPH_SERVICE_PERSIST_DIR")]
    persist_dir: Option<String>,

    /// Auto-checkpoint interval in seconds (0 = disabled).
    #[arg(long, default_value = "300")]
    checkpoint_interval: u64,

    /// Serialize graphs to disk on shutdown.
    #[arg(long, default_value = "true")]
    persist_on_shutdown: bool,
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

    // Start TCP listener if configured.
    if let Some(ref tcp_addr) = args.tcp_addr {
        let tcp_state = state.clone();
        let addr = tcp_addr.clone();
        tokio::spawn(async move {
            if let Err(e) = server::serve_tcp(&addr, tcp_state).await {
                tracing::error!("TCP server error: {}", e);
            }
        });
    }

    // Start Kafka consumer if configured. Opt-in `kafka` feature: rdkafka's
    // librdkafka does not build cleanly cross-platform (Windows/manylinux), so
    // it is excluded from the default wheel and enabled only where the event bus
    // is actually deployed (linux server, build with `--features kafka`).
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

    // Start UDS listener (main loop).
    server::serve_uds(&socket_path, state).await?;

    Ok(())
}
