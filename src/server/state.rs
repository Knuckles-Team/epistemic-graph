//! Shared server state + global limits (extracted from the server monolith).

use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::channels::ChannelManager;
use crate::isolation::IsolationLayer;
use crate::registry::GraphRegistry;

/// Upper bound on ids/edges accepted by a single batch read op. Oversize batches
/// are rejected (not truncated) so a runaway request can't allocate a multi-GB
/// response and OOM the shared process.
pub const MAX_BATCH_IDS: usize = 100_000;

/// Shared server state behind `Arc<RwLock<>>`.
pub struct ServerState {
    pub registry: GraphRegistry,
    pub isolation: IsolationLayer,
    pub channels: ChannelManager,
    pub auth_secret: String,
    pub persist_dir: Option<String>,
    /// Off-reactor WAL writer (Phase B3). `Some` when a persist dir is configured.
    /// Durable mutations enqueue their append here instead of doing blocking file
    /// I/O on a Tokio worker; the writer thread group-commits fsyncs.
    pub wal_service: Option<Arc<crate::wal_service::WalService>>,
    /// Global backpressure: caps concurrent in-flight requests across all
    /// connections. Exhaustion yields a `BUSY` response so clients retry with
    /// jitter instead of the server queueing unbounded work (Plan 01 Step 8).
    pub max_in_flight: Arc<Semaphore>,
    /// Per-graph backpressure (Phase C-D — multi-tenant fairness). A lazily
    /// created semaphore per graph caps how many of the GLOBAL in-flight slots
    /// any single graph may hold at once, so one hot tenant flooding requests
    /// cannot starve every other tenant. Lock-free on the hot path.
    pub per_graph_inflight: Arc<DashMap<String, Arc<Semaphore>>>,
    /// Max concurrent in-flight requests any single graph may hold.
    pub per_graph_inflight_limit: usize,
}
