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
    /// Pluggable durable persistence tier (CONCEPT:KG-2.177). `Some` when a
    /// persist dir is configured. The dispatch write-side-effect block calls
    /// `record` after every successful durable mutation; the chosen backend
    /// (snapshot+WAL by default, redb write-through when selected) owns its own
    /// off-reactor writer internally, so the WAL type no longer leaks here.
    pub persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
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
    /// Per-graph write coalescer (CONCEPT:KG-2.182). Lazily creates one batching
    /// writer per graph name (same lazy-keyed pattern as `per_graph_inflight`), so
    /// concurrent single-op writes to ONE hot graph (the `__commons__` ingestion
    /// firehose) collapse into one topology-lock acquisition per batch instead of
    /// serializing one-op-at-a-time. A new graph/connector gets a writer
    /// automatically. Default ON; opt out with `EPISTEMIC_GRAPH_WRITE_COALESCE=0`.
    pub write_coalescer: Arc<crate::write_coalescer::WriteCoalescerRegistry>,
}
