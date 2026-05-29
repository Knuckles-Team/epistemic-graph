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

mod algorithms;
mod channels;
mod graph;
mod isolation;
mod protocol;
mod reasoning;
mod registry;
mod server;
pub mod types;
mod event_bus;

use channels::ChannelManager;
use isolation::IsolationLayer;
use registry::GraphRegistry;
use server::ServerState;

#[derive(Parser, Debug)]
#[command(name = "epistemic-graph-server")]
#[command(about = "Tokio-native epistemic graph service")]
struct Args {
    /// Unix Domain Socket path.
    #[arg(long, default_value = "/tmp/epistemic-graph.sock")]
    socket_path: String,

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .init();

    let args = Args::parse();

    info!("Starting epistemic-graph-server");
    info!("  UDS: {}", args.socket_path);
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

    let state = Arc::new(RwLock::new(ServerState {
        registry: GraphRegistry::new(),
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: args.auth_secret,
        persist_dir: args.persist_dir,
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

    // Start Kafka consumer if configured.
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
    server::serve_uds(&args.socket_path, state).await?;

    Ok(())
}
