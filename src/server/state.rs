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

/// Default cap on the number of nodes a `GetNodes`-style FULL-graph dump may
/// return before the engine refuses to build the response (CONCEPT:KG-2.264 —
/// intelligent overload backstop). Tunable via
/// `EPISTEMIC_GRAPH_MAX_RESPONSE_NODES`. An unbounded `GetNodes` on a large
/// graph (e.g. `__commons__` with 166K+ nodes, each carrying a 1024-dim
/// embedding) serializes the WHOLE graph into ONE response frame — a
/// gigabyte-scale payload that overruns/resets the client connection (the
/// client sees `ConnectionReset`, which looks like a crash but the engine is
/// fine). Past this cap the handler returns a typed `RESULT_TOO_LARGE` error
/// telling the caller to use a bounded query (`get_nodes_by_label` /
/// pagination) INSTEAD of building the pathological frame.
pub const DEFAULT_MAX_RESPONSE_NODES: usize = 50_000;

/// Resolve the `GetNodes` full-dump node cap, read ONCE from
/// `EPISTEMIC_GRAPH_MAX_RESPONSE_NODES` (CONCEPT:KG-2.264). Cached in a
/// `OnceLock` so the env var is parsed a single time at first use, matching the
/// "read once at startup" discipline without threading a new field through every
/// `ServerState` construction site. A value of `0` disables the guard (no cap —
/// for an operator who knowingly wants the old unbounded behavior); any other
/// absent/non-parsable value falls back to [`DEFAULT_MAX_RESPONSE_NODES`].
pub fn max_response_nodes() -> usize {
    use std::sync::OnceLock;
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("EPISTEMIC_GRAPH_MAX_RESPONSE_NODES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_RESPONSE_NODES)
    })
}

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
    /// redb-authoritative mode (CONCEPT:KG-2.187), read ONCE at startup from
    /// `EPISTEMIC_GRAPH_REDB_AUTHORITATIVE` (default `false`). When `false` the
    /// engine behaves EXACTLY as before: redb (if selected) is an opt-in
    /// write-through cache tier and Postgres remains the system-of-record, with
    /// fire-and-forget `record()` durability. When `true` redb becomes the
    /// authoritative store: durable mutations are committed-before-ack (dispatch
    /// awaits `record_durable`), LRU eviction is gated so no node is dropped
    /// before it is durable + readable, and the writer applies backpressure rather
    /// than dropping. ONLY meaningful when the redb backend is selected; a
    /// non-redb backend treats it as a no-op (its `record_durable` default falls
    /// back to `record`).
    pub redb_authoritative: bool,
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
    /// Streamed content-addressed BLOB substrate (CONCEPT:KG-2.206), feature `blob`.
    /// Holds the CAS chunk store + the open upload/fetch cursors (DashMap keyed by a
    /// server-issued cursor id, with a TTL reaper like `open_txns`). `Some` when the
    /// engine is built `--features blob`; `None` otherwise (the Blob* variants then
    /// fall to the dispatch "not available" catch-all). The chunk bytes live in
    /// `{persist_dir}/blob.redb`, entirely off the inline node/edge KV.
    #[cfg(feature = "blob")]
    pub blob: Option<Arc<crate::server::blob::BlobCursors>>,
    /// Idle TTL (seconds) after which an open blob cursor is reaped
    /// (`EPISTEMIC_GRAPH_BLOB_CURSOR_TTL_SECS`, default 300).
    #[cfg(feature = "blob")]
    pub blob_cursor_ttl_secs: u64,
    /// In-engine Raft replication handle (CONCEPT:KG-2.188), cluster tier only.
    /// `None` ⇒ single-node: the dispatch write path is byte-for-byte unchanged.
    /// `Some` ⇒ durable mutations are routed through Raft consensus (the leader's
    /// `client_write`) BEFORE they are applied+acked — the replication barrier.
    /// Only ever populated when built `--features raft` AND configured at startup.
    #[cfg(feature = "raft")]
    pub raft: Option<crate::raft::RaftHandle>,
    /// The per-node [`MultiRaft`] manager (CONCEPT:KG-2.205/2.222/2.224), cluster tier
    /// only. `Some` when a cluster is active. Held here (rather than leaked) so the
    /// USER-FACING surfaces that need cross-group routing reach it: the multi-graph
    /// `Commit` path (CONCEPT:KG-2.226 — routes a cross-shard txn through the 2PC
    /// coordinator) and the online-reshard / tenant-hibernation ops (CONCEPT:KG-2.224).
    /// `None` ⇒ single-node: a multi-graph txn falls back to applying each graph's
    /// slice locally (no consensus), and the commit path is otherwise unchanged.
    #[cfg(feature = "raft")]
    pub multi_raft: Option<std::sync::Arc<crate::raft::multi::MultiRaft>>,
    /// Native time-series store (CONCEPT:KG-2.210, feature `tsdb`). `Some` when the
    /// engine booted with a series store: a durable `series.redb` beside `graph.redb`
    /// when a persist dir is set, else a process-temp file (in-memory deployments).
    /// The `Ts*` handlers append/scan/query through it; it is independent of the
    /// graph registry + the graph write-coalescer (series are keyed by `series_id`,
    /// not a graph). redb holds an exclusive per-process file lock, so this is its
    /// OWN file — NOT a second handle on `graph.redb`.
    #[cfg(feature = "tsdb")]
    pub tsdb_store: Option<Arc<eg_tsdb::store::SeriesStore>>,
    /// Opt-in lossless RDF quad table (CONCEPT:KG-2.217, feature `rdf-redb`). `Some`
    /// when a persist dir is set (a durable `rdf_quads.redb` beside `graph.redb`):
    /// the `AddTriples` handler routes multi-valued literal predicates here (the one
    /// lossy edge of the property-graph mapping), and `GetRdf` unions them back.
    /// `None` ⇒ the property-graph mapping alone (the extras are reported, not lost
    /// silently). Its OWN redb file — NOT a second handle on `graph.redb`.
    #[cfg(feature = "rdf-redb")]
    pub rdf_quads: Option<Arc<eg_rdf::quads::QuadStore>>,
    /// Change-Data-Capture hub (CONCEPT:KG-2.229/230, feature `streaming`). `Some` on
    /// any `streaming` build (constructed unconditionally — it needs no persist dir,
    /// the in-memory ring IS the cursor surface). The dispatch write-side-effect block
    /// emits a `CdcEvent` into it after every successful durable mutation; the
    /// streaming handler reads the feed / maintains continuous queries / serves watch
    /// + triggers off it. PURE in-memory (bounded ring + Notify) — no second redb file.
    #[cfg(feature = "streaming")]
    pub cdc: Option<Arc<crate::server::cdc::CdcHub>>,
    /// WASM-sandboxed UDF registry (CONCEPT:KG-2.228, feature `wasm-udf`). Holds the
    /// compiled, cached `UdfModule`s an agent pushed via `RegisterUdf`; `RunUdf` + the
    /// `Op::Udf` plan op look up + run them sandboxed (fuel + memory limits, NO host
    /// caps). Always present (empty) with the feature on — UDFs are process-global, not
    /// per-graph. Behind `Arc` so the off-lock compute path clones a handle cheaply.
    #[cfg(feature = "wasm-udf")]
    pub udf_registry: Arc<eg_wasm::UdfRegistry>,
    /// Materialized views of distributed-compute results (CONCEPT:KG-2.227, feature
    /// `compute-dist`). The in-RAM index of named, incrementally-maintained matviews;
    /// the durable copy lives in redb (the handler persists + reloads on boot). Mutex
    /// because a `CreateMatView`/`RefreshMatView` mutates it off the registry lock.
    #[cfg(feature = "compute-dist")]
    pub matviews: Arc<Mutex<crate::raft::pregel::MatViewStore>>,
    /// Registered FOREIGN sources for query federation (CONCEPT:KG-2.232, feature
    /// `federation`), keyed by name. `RegisterForeignSource` inserts a
    /// [`eg_types::wire::ForeignSourceSpec`] here so it can be reused by name; the
    /// inline-spec `Op::ForeignScan` path does not need it. Process-global (a foreign
    /// endpoint is not per-graph), lock-free on read. Always present (empty) with the
    /// feature on.
    #[cfg(feature = "federation")]
    pub foreign_sources: Arc<DashMap<String, eg_types::wire::ForeignSourceSpec>>,
    /// Generic namespaced Key→Value store (CONCEPT:EG-022, feature `kv`). `Some` on a
    /// `kv` build: a durable `{persist_dir}/kv.redb` when a persist dir is set, else an
    /// in-memory scratch map. The `Kv*` handlers get/put/delete/scan/cas through it; it
    /// is independent of the graph registry + the write-coalescer (KV pairs are keyed by
    /// `(namespace, key)`, not a graph). Its OWN redb file — NOT a second handle on
    /// `graph.redb`. `None` ⇒ the Kv* variants fall to the dispatch "not available"
    /// catch-all.
    #[cfg(feature = "kv")]
    pub kv: Option<Arc<crate::server::kv::KvStore>>,
}
