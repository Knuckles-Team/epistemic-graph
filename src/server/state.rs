//! Shared server state + global limits (extracted from the server monolith).

use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::channels::ChannelManager;
use crate::isolation::IsolationLayer;
use crate::registry::GraphRegistry;
use crate::server::txn::{GraphTxnState, TxnIdGen};
use parking_lot::Mutex;

/// Upper bound on ids/edges accepted by a single batch read op. Oversize batches
/// are rejected (not truncated) so a runaway request can't allocate a multi-GB
/// response and OOM the shared process.
pub const MAX_BATCH_IDS: usize = 100_000;

/// Read the OCC-transaction TTL + open-txn caps from the environment
/// (CONCEPT:KG-2.180), with the documented defaults. Centralized so every
/// `ServerState` construction site gets the same knobs without re-reading env.
/// Returns `(ttl_secs, max_per_graph, max_per_agent)`.
pub fn txn_limits_from_env() -> (u64, usize, usize) {
    fn env_usize(key: &str, default: usize) -> usize {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(default)
    }
    let ttl = std::env::var("EPISTEMIC_GRAPH_TXN_TTL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(300);
    (
        ttl,
        env_usize("EPISTEMIC_GRAPH_TXN_MAX_PER_GRAPH", 256),
        env_usize("EPISTEMIC_GRAPH_TXN_MAX_PER_AGENT", 256),
    )
}

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
    /// Open server-staged OCC transactions (CONCEPT:KG-2.180), keyed by the
    /// server-issued `txn_id`. A staged txn holds its write-set + read-set off the
    /// graph lock; the lock is taken only at commit. Each entry is behind a `Mutex`
    /// so the begin/stage/commit RPCs for one txn serialize without blocking other
    /// txns. Stateless requests thread `txn_id` in the body, so this is keyed by id
    /// (NOT by connection). A TTL sweep auto-rolls-back idle txns.
    pub open_txns: Arc<DashMap<String, Mutex<GraphTxnState>>>,
    /// Server-issued monotonic transaction-id source (no `rand`/`Date` dep).
    pub txn_id_gen: Arc<TxnIdGen>,
    /// Idle TTL (seconds) after which an open txn is auto-rolled-back by the sweep
    /// (`EPISTEMIC_GRAPH_TXN_TTL_SECS`, default 300).
    pub txn_ttl_secs: u64,
    /// Cap on concurrently-open txns per graph (`EPISTEMIC_GRAPH_TXN_MAX_PER_GRAPH`,
    /// default 256). `BeginTxn` over the cap is rejected — bounds memory the same
    /// way `per_graph_inflight` bounds request concurrency.
    pub txn_max_per_graph: usize,
    /// Cap on concurrently-open txns per agent (`EPISTEMIC_GRAPH_TXN_MAX_PER_AGENT`,
    /// default 256). Anonymous callers (`agent_id` absent) share the `""` bucket.
    pub txn_max_per_agent: usize,
}
