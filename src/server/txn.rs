//! Server-side staged OCC transactions (CONCEPT:EG-KG.txn.multi-op-occ-acid — multi-operation ACID).
//!
//! Design: **optimistic, snapshot-isolation, server-staged**. A transaction is
//! opened with `BeginTxn`, then `TxnAddNode`/`TxnRemoveNode`/`TxnAddEdge`/
//! `TxnRemoveEdge`/`TxnCas` STAGE durable mutations into an in-memory write-set —
//! none of which touch the graph or persistence. The topology write lock is taken
//! ONLY during `Commit`'s validate-and-apply step, so a transaction can stay open
//! across client think-time without ever holding `topo.write()` and the lock-free
//! read hot path is preserved.
//!
//! OCC conflict detection: each node a staged op targets has its current state
//! *fingerprinted* (its property blob, or an "absent" marker) the first time it is
//! referenced, recorded in the `read_set`. At commit the engine re-reads each
//! fingerprint under the held write guard; if any changed since it was captured the
//! commit is a conflict (`Bool(false)`) and NOTHING is applied or persisted — a
//! true rollback. The per-`GraphCore` write-version counter (`core.version()`) is
//! the cheap coarse guard: if it is unchanged since begin, no write landed and the
//! per-node re-check is skipped entirely.
//!
//! Isolation levels (CONCEPT:EG-KG.txn.serializable-zero-cost — M6b). `BeginTxn` carries an
//! `isolation: Option<String>` hint:
//!   * `None` / `"snapshot"` — the default. Validation is exactly the per-NODE
//!     fingerprint read-set above (catches write-write on touched nodes). This is
//!     UNCHANGED by M6b and pays NO extra cost.
//!   * `"serializable"` — additionally captures a PREDICATE/RANGE read-set the txn
//!     relied on (a label scan) and re-evaluates it at commit, failing the commit
//!     if the matched set changed (a phantom inserted/removed, or a matching node's
//!     property changed). Because the protocol has no in-txn read op (and M6b adds
//!     no wire change), the predicate is declared inline in the isolation hint:
//!     `"serializable:label=<L>"` watches the label scan `get_nodes_by_label(<L>)`.
//!     Plain `"serializable"` is accepted with an empty predicate read-set (it then
//!     behaves like snapshot — there is simply nothing extra to protect).
//!
//! Any other isolation value is REJECTED at `BeginTxn`.
//!
//! PITR hook (CONCEPT:EG-KG.txn.serializable-zero-cost): point-in-time recovery would replay the
//! append-only `ledger` table up to a target version/timestamp; the natural slot is
//! the commit path in `handlers/txn.rs` right after `core.mark_dirty()` (each
//! committed txn maps to a version bump that a PITR cursor can target). Not
//! implemented here — it requires a ledger replay API that lives outside the txn
//! subsystem.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;

use crate::graph::GraphCore;
use crate::protocol::Method;
use crate::server::ServerState;

#[cfg(feature = "raft")]
#[derive(Clone, PartialEq, Eq)]
struct ConsensusGraphFence {
    coordinator_id: String,
    participant_id: u64,
}

#[cfg(feature = "raft")]
fn consensus_graph_fences() -> &'static dashmap::DashMap<String, ConsensusGraphFence> {
    static FENCES: std::sync::OnceLock<dashmap::DashMap<String, ConsensusGraphFence>> =
        std::sync::OnceLock::new();
    FENCES.get_or_init(dashmap::DashMap::new)
}

#[cfg(feature = "raft")]
fn consensus_placement_fence_lock() -> &'static Arc<tokio::sync::Mutex<()>> {
    static LOCK: std::sync::OnceLock<Arc<tokio::sync::Mutex<()>>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| Arc::new(tokio::sync::Mutex::new(())))
}

/// Serialize placement cutovers with the prepare-fence check-and-persist interval.
#[cfg(feature = "raft")]
pub(crate) async fn consensus_placement_fence_guard() -> tokio::sync::OwnedMutexGuard<()> {
    Arc::clone(consensus_placement_fence_lock())
        .lock_owned()
        .await
}

/// Reserve a participant graph between replicated PREPARE and COMMIT/ABORT. The
/// fence is process-local serving state reconstructed by Raft history; its durable
/// authority remains the encrypted participant intent.
#[cfg(feature = "raft")]
pub(crate) fn acquire_consensus_graph_fence(
    graph: &str,
    coordinator_id: &str,
    participant_id: u64,
) -> Result<bool, String> {
    let expected = ConsensusGraphFence {
        coordinator_id: coordinator_id.to_string(),
        participant_id,
    };
    match consensus_graph_fences().entry(graph.to_string()) {
        dashmap::mapref::entry::Entry::Vacant(entry) => {
            entry.insert(expected);
            Ok(true)
        }
        dashmap::mapref::entry::Entry::Occupied(entry) if entry.get() == &expected => Ok(false),
        dashmap::mapref::entry::Entry::Occupied(_) => {
            Err("graph is reserved by another prepared transaction".to_string())
        }
    }
}

#[cfg(feature = "raft")]
pub(crate) fn release_consensus_graph_fence(
    graph: &str,
    coordinator_id: &str,
    participant_id: u64,
) {
    let should_remove = consensus_graph_fences()
        .get(graph)
        .map(|fence| {
            fence.coordinator_id == coordinator_id && fence.participant_id == participant_id
        })
        .unwrap_or(false);
    if should_remove {
        consensus_graph_fences().remove(graph);
    }
}

#[cfg(feature = "raft")]
pub(crate) fn consensus_graph_is_prepared(graph: &str) -> bool {
    consensus_graph_fences().contains_key(graph)
}

/// Reject control-plane operations that would move, reroute, or remove a graph
/// while a replicated participant intent owns its prepare fence.
#[cfg(feature = "raft")]
pub(crate) fn consensus_control_conflicts(method: &Method) -> bool {
    match method {
        Method::Reshard { graph, .. }
        | Method::CatalogAssign { graph, .. }
        | Method::CatalogReassign { graph, .. }
        | Method::CatalogRemove { graph }
        | Method::DeleteGraph { graph_name: graph } => consensus_graph_is_prepared(graph),
        Method::RebalanceExecute { .. } => !consensus_graph_fences().is_empty(),
        _ => false,
    }
}

/// Placement administration for a tenant cannot race any prepared graph in that
/// tenant's exact or partition-qualified keyspace.
#[cfg(feature = "raft")]
pub(crate) fn consensus_tenant_is_prepared(tenant: &str) -> bool {
    let partition_prefix = format!("{tenant}:");
    consensus_graph_fences()
        .iter()
        .any(|entry| entry.key() == tenant || entry.key().starts_with(&partition_prefix))
}

#[cfg(feature = "raft")]
pub(crate) fn consensus_has_prepared_graphs() -> bool {
    !consensus_graph_fences().is_empty()
}

/// Wall-clock milliseconds for the idle-TTL bookkeeping. Monotonicity is not
/// required — the TTL sweep tolerates clock skew (it auto-rolls-back only txns
/// idle *past* the TTL; a skewed clock at worst delays/advances a reclaim).
pub fn now_ms() -> u64 {
    #[cfg(feature = "raft")]
    if crate::server::dispatch::is_replicated_apply() {
        return crate::server::dispatch::authoritative_now_ms();
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Auto-rollback transactions idle past `ttl_secs` (CONCEPT:EG-KG.txn.multi-op-occ-acid safety rail).
/// Called by the background maintenance tick in `main.rs`. Returns the number of
/// expired transactions reclaimed. Holds only the `open_txns` DashMap per-entry
/// locks briefly; never the topology lock — an abandoned txn that never committed
/// has applied nothing, so reclaiming it just frees memory (a true rollback).
pub fn sweep_expired_txns(state: &Arc<RwLock<ServerState>>, ttl_secs: u64, now: u64) -> usize {
    // We only need `open_txns`; a try_read avoids contending with writers, and on
    // the rare miss we skip this tick (the next tick reclaims).
    let Ok(s) = state.try_read() else {
        return 0;
    };
    let ttl_ms = ttl_secs.saturating_mul(1000);
    let expired: Vec<String> = s
        .open_txns
        .iter()
        .filter_map(|e| {
            let t = e.value().lock();
            (now.saturating_sub(t.last_active_ms) >= ttl_ms).then(|| e.key().clone())
        })
        .collect();
    for id in &expired {
        s.open_txns.remove(id);
    }
    expired.len()
}

/// Fingerprint of a node's current state, captured when a staged op first
/// references it and re-checked at commit for OCC conflict detection.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum NodeFingerprint {
    /// Node absent at capture time.
    Absent,
    /// Node present; the hash of its property blob at capture time. Hashing (not
    /// storing the bytes) keeps the read-set small for a long-lived transaction.
    Present(u64),
}

impl NodeFingerprint {
    /// Capture the current fingerprint of `node_id` in `core` (an off-lock
    /// point-read; no topology lock is held while staging).
    fn capture(core: &GraphCore, node_id: &str) -> Self {
        match core.get_node_properties(node_id) {
            Some(bytes) => NodeFingerprint::Present(hash_bytes(&bytes)),
            None => NodeFingerprint::Absent,
        }
    }
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = ahash::AHasher::default();
    bytes.hash(&mut h);
    h.finish()
}

/// Transaction isolation level (CONCEPT:EG-KG.txn.serializable-zero-cost). `Snapshot` is today's behavior
/// (per-node OCC read-set only); `Serializable` additionally re-evaluates a captured
/// predicate read-set at commit to reject phantom/range anomalies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum IsolationLevel {
    Snapshot,
    Serializable,
}

/// Parse the `BeginTxn` `isolation` hint into a level plus an optional predicate to
/// watch. Returns `Err(msg)` for an unknown value so `BeginTxn` can reject it.
///
/// Accepted forms (case-insensitive on the level keyword):
///   * `None` / `""` / `"snapshot"`        → (Snapshot, None)
///   * `"serializable"`                     → (Serializable, None)
///   * `"serializable:label=<L>"`           → (Serializable, Some(Label(<L>)))
pub(crate) fn parse_isolation(
    hint: Option<&str>,
) -> Result<(IsolationLevel, Option<PredicateRead>), String> {
    let raw = hint.unwrap_or("").trim();
    if raw.is_empty() {
        return Ok((IsolationLevel::Snapshot, None));
    }
    // Split off an optional `:<predicate>` suffix from the level keyword.
    let (level_kw, predicate) = match raw.split_once(':') {
        Some((lvl, pred)) => (lvl.trim(), Some(pred.trim())),
        None => (raw, None),
    };
    let level = match level_kw.to_ascii_lowercase().as_str() {
        "snapshot" => IsolationLevel::Snapshot,
        "serializable" => IsolationLevel::Serializable,
        other => return Err(format!("unknown isolation level '{other}'")),
    };
    let predicate = match (level, predicate) {
        // A predicate is only meaningful under serializable; snapshot has no
        // predicate read-set, so a `snapshot:…` suffix is a malformed hint.
        (IsolationLevel::Snapshot, Some(p)) if !p.is_empty() => {
            return Err(format!(
                "isolation predicate '{p}' is only valid with serializable"
            ));
        }
        (IsolationLevel::Serializable, Some(p)) if !p.is_empty() => Some(PredicateRead::parse(p)?),
        _ => None,
    };
    Ok((level, predicate))
}

/// A predicate/range read the txn relied on, captured at begin and re-evaluated at
/// commit (CONCEPT:EG-KG.txn.serializable-zero-cost serializable). Currently the one supported predicate is
/// a label scan — the minimal staged-read primitive the txn subsystem can express
/// without a protocol change.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) enum PredicateRead {
    /// `get_nodes_by_label(label)` — the set of nodes carrying `label`.
    Label(String),
}

impl PredicateRead {
    /// Parse a predicate clause from the isolation hint suffix, e.g. `label=Doc`.
    fn parse(clause: &str) -> Result<Self, String> {
        match clause.split_once('=') {
            Some(("label", lbl)) if !lbl.trim().is_empty() => {
                Ok(PredicateRead::Label(lbl.trim().to_string()))
            }
            _ => Err(format!(
                "unsupported isolation predicate '{clause}' (expected label=<L>)"
            )),
        }
    }

    /// Fingerprint the predicate's current result set: an order-independent hash over
    /// each matching `(node_id, property-blob)` pair. A phantom inserted/removed
    /// changes the id membership; a matching node's property change alters its blob —
    /// either flips the fingerprint, which is exactly the serializable anomaly set.
    fn fingerprint(&self, core: &GraphCore) -> u64 {
        match self {
            PredicateRead::Label(label) => {
                // `0` = uncapped: serializability needs the FULL matched set, not a page.
                let mut rows = core.get_nodes_by_label(label, 0);
                // Sort by id so the fold is order-independent (the label index does
                // not guarantee a stable order across rebuilds).
                rows.sort_by(|a, b| a.0.cmp(&b.0));
                let mut acc: u64 = 0xcbf2_9ce4_8422_2325; // FNV offset basis, as a seed.
                for (id, props) in &rows {
                    acc ^= hash_bytes(id.as_bytes());
                    acc = acc.rotate_left(13);
                    acc ^= hash_bytes(props);
                    acc = acc.rotate_left(13);
                }
                acc
            }
        }
    }
}

/// One open server-staged transaction (CONCEPT:EG-KG.txn.multi-op-occ-acid). Holds the target graph
/// name, the OCC begin-version, the staged durable-mutation write-set, and the
/// per-node read-set fingerprints for conflict detection. Stored behind a `Mutex`
/// in `ServerState::open_txns` keyed by a server-issued `txn_id`.
#[derive(Clone)]
pub struct GraphTxnState {
    /// Target graph (resolved at begin; all staged ops apply here).
    pub(crate) graph: String,
    /// Opaque tenant authority derived from the verified request context. This is
    /// the sole durable tenant scope for every batch produced by the transaction.
    pub(crate) tenant_scope: String,
    /// `core.version()` at begin — the coarse OCC guard.
    pub(crate) begin_version: u64,
    /// Staged durable mutations, applied in order at commit. Restricted to the
    /// durable-mutation set (AddNode/RemoveNode/AddEdge/RemoveEdge/CompareAndSet).
    pub(crate) write_set: Vec<Method>,
    /// Per-node fingerprints captured when first referenced — the OCC read-set.
    pub(crate) read_set: HashMap<String, NodeFingerprint>,
    /// Isolation level (CONCEPT:EG-KG.txn.serializable-zero-cost). `Snapshot` validates only `read_set`;
    /// `Serializable` additionally re-checks `predicate_reads`.
    pub(crate) isolation: IsolationLevel,
    /// Predicate/range reads captured at begin and re-evaluated at commit under
    /// serializable — each entry is `(predicate, fingerprint-at-begin)`. Empty under
    /// snapshot (so snapshot pays no extra cost).
    pub(crate) predicate_reads: Vec<(PredicateRead, u64)>,
    /// Owning verified tenant+actor scope (for authorization and the per-owner
    /// open-txn cap).
    pub(crate) agent: String,
    /// Monotonic ms timestamp of the last staged/begun activity, for TTL idle
    /// expiry. Updated on every stage so an actively-used txn is never swept.
    pub(crate) last_active_ms: u64,
    /// Multi-graph staged write-set (CONCEPT:EG-KG.txn.routes-cross-shard-txn — Lane N). A `Txn*` op naming a
    /// graph OTHER than the default `graph` accumulates here, keyed by graph name, in
    /// stage order. EMPTY for an ordinary single-graph txn (the fast path pays
    /// nothing). At commit, if these touch graphs in a DIFFERENT Raft group than the
    /// default graph, the txn is CROSS-SHARD and routes through the 2PC coordinator;
    /// otherwise the spanned graphs collapse onto one group and stay single-group.
    pub(crate) extra_writes: HashMap<String, Vec<Method>>,
    /// Staged VECTOR upserts `(node_id, embedding)` for the DEFAULT graph (CONCEPT:
    /// KG-2.225 — cross-modal ACID). EMPTY for a graph-only txn. When non-empty, the
    /// commit lands graph + vectors (+ blob-refs) in ONE redb `WriteTransaction` so a
    /// node and its embedding are atomic — never one without the other.
    pub(crate) vectors: Vec<(String, Vec<f32>)>,
    /// Staged BLOB REFERENCES `(node_id, digest)` for the DEFAULT graph (CONCEPT:
    /// KG-2.225). Each records a durable graph-side `__blob__` link to an already-
    /// stored content-addressed blob; lands in the SAME cross-modal commit txn.
    pub(crate) blob_refs: Vec<(String, String)>,
    /// Staged TIME-SERIES measurement batches for the DEFAULT graph (CONCEPT:EG-KG.backend.cross-modal-atomic-commit —
    /// extended cross-modal staging). EMPTY for a txn with no measurements. When
    /// non-empty, the commit lands each batch into the graph's SERIES tables inside the
    /// SAME redb `WriteTransaction` as the graph/vector/blob writes, so a node and the
    /// measurements annotating it are atomic. Exposed readably (crate-visible) so Lane A's
    /// `run_unified_overlaid` can overlay these staged, uncommitted points into an in-txn
    /// `TsScan` (read-your-own-writes on the tsdb modality).
    pub(crate) measurements: Vec<StagedMeasurement>,
    /// Staged OWL AXIOM writes, already LOWERED to graph-native `AddNode`/`AddEdge`
    /// methods at stage time (CONCEPT:EG-KG.txn.extended-cross-modal). Kept separate from `write_set` for
    /// provenance, but applied through the SAME cross-modal wtx (`apply_method_rows`
    /// durably + `apply_staged` in-memory) so the committed OWL axioms are visible to the
    /// reasoner consistently with the txn's other modalities. EMPTY for a non-axiom txn.
    pub(crate) axioms: Vec<Method>,
    /// Staged SPARQL CONSTRUCT results, already LOWERED to graph-native `AddNode`/
    /// `AddEdge` methods at stage time (CONCEPT:EG-KG.txn.construct-evaluated — the CONSTRUCT is evaluated
    /// against the committed snapshot when staged). Applied through the SAME cross-modal
    /// wtx as `axioms`. EMPTY for a non-construct txn.
    pub(crate) constructs: Vec<Method>,
    /// Staged PLANNER WRITEBACK methods (CONCEPT:EG-KG.query.plan-dag, D7 — the planner-writeback
    /// ACID seam), already LOWERED to graph-native `AddNode`/`AddEdge` methods at stage
    /// time from a `eg_plan::Plan`'s (or `PlanDag`'s) executed `RowSet` result — e.g.
    /// materializing a `Reason`/`Traverse`-inferred edge set. Kept separate from
    /// `write_set` for provenance, but applied through the SAME cross-modal wtx
    /// (`apply_method_rows` durably + `apply_staged` in-memory) as `axioms`/`constructs`,
    /// so the committed planner-inferred edges are atomic with the txn's other staged
    /// modalities. EMPTY for a txn with no plan writeback.
    pub(crate) plan_writeback: Vec<Method>,
}

/// One staged time-series measurement batch (CONCEPT:EG-KG.backend.cross-modal-atomic-commit). Carries the decoded points
/// plus the schema fields the durable SERIES tables need on FIRST write of a series
/// (`n_fields`/`bucket_ns`/`field_names`); for an already-existing series the stored meta
/// is authoritative and these are ignored. Decoded once at stage time so the commit path
/// is a pure durable write.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct StagedMeasurement {
    pub(crate) series: String,
    pub(crate) n_fields: usize,
    pub(crate) bucket_ns: u64,
    pub(crate) field_names: Vec<String>,
    /// `(ts_nanos, field_values)` points — the SAME shape `TsAppend` carries on the wire.
    pub(crate) points: Vec<(i64, Vec<f64>)>,
}

/// Canonical private recovery body for a prepared transaction coordinator.  It is
/// never written to a coordinator batch, outbox, log message, or trace: callers
/// serialize this deterministic shape, encrypt it with the environment-managed
/// data key, and atomically attach only the ciphertext to the parent receipt.
///
/// `agent` and wall-clock activity are deliberately absent.  Retry authorization
/// is bound by the parent's principal fingerprint, avoiding durable raw identity;
/// idle bookkeeping is reconstructed in memory.
#[derive(serde::Serialize, serde::Deserialize)]
struct DurableTxnPlan {
    schema_version: u16,
    graph: String,
    tenant_scope: String,
    begin_version: u64,
    write_set: Vec<Method>,
    read_set: BTreeMap<String, NodeFingerprint>,
    isolation: IsolationLevel,
    predicate_reads: Vec<(PredicateRead, u64)>,
    extra_writes: BTreeMap<String, Vec<Method>>,
    vectors: Vec<(String, Vec<f32>)>,
    blob_refs: Vec<(String, String)>,
    measurements: Vec<StagedMeasurement>,
    axioms: Vec<Method>,
    constructs: Vec<Method>,
    plan_writeback: Vec<Method>,
}

const DURABLE_TXN_PLAN_VERSION: u16 = 2;
const MAX_DURABLE_TXN_PLAN_BYTES: usize = 64 * 1024 * 1024;
const MAX_DURABLE_TXN_PLAN_ITEMS: usize = 1_000_000;

impl StagedMeasurement {
    /// Flatten into the persistence-layer [`crate::MeasurementBatch`] tuple threaded
    /// through `commit_crossmodal`.
    pub(crate) fn to_batch(&self) -> crate::MeasurementBatch {
        (
            self.series.clone(),
            self.n_fields,
            self.bucket_ns,
            self.field_names.clone(),
            self.points.clone(),
        )
    }
}

impl GraphTxnState {
    /// Serialize the complete staged transaction into a stable canonical ordering.
    /// The returned bytes are plaintext only in process memory and MUST be sealed
    /// before persistence.
    pub(crate) fn encode_recovery_plan(&self) -> Result<Vec<u8>, String> {
        let plan = DurableTxnPlan {
            schema_version: DURABLE_TXN_PLAN_VERSION,
            graph: self.graph.clone(),
            tenant_scope: self.tenant_scope.clone(),
            begin_version: self.begin_version,
            write_set: self.write_set.clone(),
            read_set: self
                .read_set
                .iter()
                .map(|(node, fingerprint)| (node.clone(), fingerprint.clone()))
                .collect(),
            isolation: self.isolation,
            predicate_reads: self.predicate_reads.clone(),
            extra_writes: self
                .extra_writes
                .iter()
                .map(|(graph, methods)| (graph.clone(), methods.clone()))
                .collect(),
            vectors: self.vectors.clone(),
            blob_refs: self.blob_refs.clone(),
            measurements: self.measurements.clone(),
            axioms: self.axioms.clone(),
            constructs: self.constructs.clone(),
            plan_writeback: self.plan_writeback.clone(),
        };
        let bytes = rmp_serde::to_vec_named(&plan)
            .map_err(|_| "transaction recovery plan encode failed".to_string())?;
        eg_types::msgpack::validate_single_value(
            &bytes,
            eg_types::msgpack::MsgpackLimits::new(
                MAX_DURABLE_TXN_PLAN_BYTES,
                MAX_DURABLE_TXN_PLAN_ITEMS,
                64,
            ),
        )
        .map_err(|_| "transaction recovery plan exceeds limits".to_string())?;
        Ok(bytes)
    }

    /// Reconstruct an ephemeral staged transaction from authenticated private
    /// recovery bytes.  The retrying caller is held only in RAM; its durable scope
    /// was already verified against the parent receipt before this method is called.
    pub(crate) fn decode_recovery_plan(bytes: &[u8], agent: String) -> Result<Self, String> {
        let plan: DurableTxnPlan = eg_types::msgpack::decode_bounded(
            bytes,
            eg_types::msgpack::MsgpackLimits::new(
                MAX_DURABLE_TXN_PLAN_BYTES,
                MAX_DURABLE_TXN_PLAN_ITEMS,
                64,
            ),
        )
        .map_err(|_| "transaction recovery plan is corrupt".to_string())?;
        if plan.schema_version != DURABLE_TXN_PLAN_VERSION {
            return Err(format!(
                "unsupported transaction recovery plan version {}",
                plan.schema_version
            ));
        }
        if plan.graph.is_empty() || plan.tenant_scope.is_empty() {
            return Err("transaction recovery plan has incomplete authority".to_string());
        }
        Ok(GraphTxnState {
            graph: plan.graph,
            tenant_scope: plan.tenant_scope,
            begin_version: plan.begin_version,
            write_set: plan.write_set,
            read_set: plan.read_set.into_iter().collect(),
            isolation: plan.isolation,
            predicate_reads: plan.predicate_reads,
            agent,
            last_active_ms: now_ms(),
            extra_writes: plan.extra_writes.into_iter().collect(),
            vectors: plan.vectors,
            blob_refs: plan.blob_refs,
            measurements: plan.measurements,
            axioms: plan.axioms,
            constructs: plan.constructs,
            plan_writeback: plan.plan_writeback,
        })
    }

    /// Open a fresh staged transaction. Under `Serializable` with a declared
    /// `predicate`, capture its result-set fingerprint NOW (the snapshot the txn
    /// reads against) so commit can detect a phantom/range change. `core` is the
    /// txn's target graph core (an off-lock read; nothing is held while staging).
    pub(crate) fn new(
        core: &GraphCore,
        graph: String,
        tenant_scope: String,
        begin_version: u64,
        isolation: IsolationLevel,
        predicate: Option<PredicateRead>,
        agent: String,
        now_ms: u64,
    ) -> Self {
        let predicate_reads = match (isolation, predicate) {
            (IsolationLevel::Serializable, Some(p)) => {
                let fp = p.fingerprint(core);
                vec![(p, fp)]
            }
            _ => Vec::new(),
        };
        GraphTxnState {
            graph,
            tenant_scope,
            begin_version,
            write_set: Vec::new(),
            read_set: HashMap::new(),
            isolation,
            predicate_reads,
            agent,
            last_active_ms: now_ms,
            extra_writes: HashMap::new(),
            vectors: Vec::new(),
            blob_refs: Vec::new(),
            measurements: Vec::new(),
            axioms: Vec::new(),
            constructs: Vec::new(),
            plan_writeback: Vec::new(),
        }
    }

    /// Stage a VECTOR upsert into the cross-modal write-set (CONCEPT:EG-KG.txn.reader-never-sees-node). The
    /// node it targets is captured into the OCC read-set (so a concurrent change to
    /// that node still conflicts), then the embedding is queued to land atomically at
    /// commit. Only the DEFAULT graph's vectors participate in the one-txn barrier.
    pub(crate) fn stage_vector(
        &mut self,
        core: &GraphCore,
        node_id: String,
        embedding: Vec<f32>,
        now_ms: u64,
    ) {
        self.observe(core, &node_id);
        self.vectors.push((node_id, embedding));
        self.last_active_ms = now_ms;
    }

    /// Stage a BLOB REFERENCE into the cross-modal write-set (CONCEPT:EG-KG.txn.reader-never-sees-node). The
    /// node is captured into the OCC read-set; the `(node_id, digest)` ref lands
    /// atomically with the node/vector/property at commit.
    pub(crate) fn stage_blob_ref(
        &mut self,
        core: &GraphCore,
        node_id: String,
        digest: String,
        now_ms: u64,
    ) {
        self.observe(core, &node_id);
        self.blob_refs.push((node_id, digest));
        self.last_active_ms = now_ms;
    }

    /// Stage a TIME-SERIES measurement batch into the cross-modal write-set
    /// (CONCEPT:EG-KG.backend.cross-modal-atomic-commit). Measurements are not graph nodes, so nothing is added to the OCC
    /// node read-set; the batch is queued to land atomically with the txn's other
    /// modalities at commit.
    pub(crate) fn stage_measurement(&mut self, measurement: StagedMeasurement, now_ms: u64) {
        self.measurements.push(measurement);
        self.last_active_ms = now_ms;
    }

    /// Stage OWL AXIOM writes, pre-lowered to `AddNode`/`AddEdge` methods (CONCEPT:EG-KG.txn.extended-cross-modal).
    /// Each method's referenced nodes are captured into the OCC read-set (so a concurrent
    /// change to a touched node still conflicts), then the methods are queued to land in
    /// the SAME cross-modal commit.
    pub(crate) fn stage_axiom(&mut self, core: &GraphCore, methods: Vec<Method>, now_ms: u64) {
        for m in &methods {
            self.observe_method(core, m);
        }
        self.axioms.extend(methods);
        self.last_active_ms = now_ms;
    }

    /// Stage SPARQL CONSTRUCT results, pre-lowered to `AddNode`/`AddEdge` methods
    /// (CONCEPT:EG-KG.txn.construct-evaluated). Same OCC read-set capture + atomic-commit semantics as
    /// [`Self::stage_axiom`].
    pub(crate) fn stage_construct(&mut self, core: &GraphCore, methods: Vec<Method>, now_ms: u64) {
        for m in &methods {
            self.observe_method(core, m);
        }
        self.constructs.extend(methods);
        self.last_active_ms = now_ms;
    }

    /// Stage PLANNER WRITEBACK methods, pre-lowered to `AddNode`/`AddEdge` methods
    /// (CONCEPT:EG-KG.query.plan-dag, D7 — the planner-writeback ACID seam). Same OCC
    /// read-set capture + atomic-commit semantics as [`Self::stage_axiom`] /
    /// [`Self::stage_construct`] — copied verbatim, this is the well-precedented shape
    /// every staged-and-lowered modality shares.
    pub(crate) fn stage_plan_writeback(
        &mut self,
        core: &GraphCore,
        methods: Vec<Method>,
        now_ms: u64,
    ) {
        for m in &methods {
            self.observe_method(core, m);
        }
        self.plan_writeback.extend(methods);
        self.last_active_ms = now_ms;
    }

    /// True when this txn staged any NON-graph-topology modality — a vector, blob-ref,
    /// measurement, OWL axiom, CONSTRUCT, or planner writeback — i.e. it is a CROSS-MODAL
    /// txn whose commit must land all modalities in ONE redb `WriteTransaction`.
    pub(crate) fn is_cross_modal(&self) -> bool {
        !self.vectors.is_empty()
            || !self.blob_refs.is_empty()
            || !self.measurements.is_empty()
            || !self.axioms.is_empty()
            || !self.constructs.is_empty()
            || !self.plan_writeback.is_empty()
    }

    /// Stage one durable mutation against a NAMED graph that may differ from the
    /// txn's default (CONCEPT:EG-KG.txn.routes-cross-shard-txn — multi-graph). When `graph` equals the
    /// default, this routes to [`Self::stage`] (the single-graph read-set/OCC path,
    /// unchanged). Otherwise the op accumulates in `extra_writes` for that graph; the
    /// op is NOT applied (the cross-shard coordinator applies the multi-graph slices
    /// atomically at commit, with its own per-participant OCC validation).
    pub(crate) fn stage_in(
        &mut self,
        default_core: &GraphCore,
        target_graph: &str,
        method: Method,
        now_ms: u64,
    ) {
        if target_graph == self.graph {
            self.stage(default_core, method, now_ms);
        } else {
            self.extra_writes
                .entry(target_graph.to_string())
                .or_default()
                .push(method);
            self.last_active_ms = now_ms;
        }
    }

    /// Every graph this txn touches: the default plus any extra-graph targets
    /// (CONCEPT:EG-KG.txn.routes-cross-shard-txn). Used at commit to detect a cross-shard span.
    pub(crate) fn touched_graphs(&self) -> Vec<String> {
        let mut out = vec![self.graph.clone()];
        out.extend(self.extra_writes.keys().cloned());
        out
    }

    /// True when this txn staged ops against a graph other than its default
    /// (CONCEPT:EG-KG.txn.routes-cross-shard-txn) — i.e. it is a multi-graph txn whose span the commit path
    /// must evaluate against the router.
    pub(crate) fn is_multi_graph(&self) -> bool {
        !self.extra_writes.is_empty()
    }

    /// Record (once) the current fingerprint of a node this txn touches, capturing
    /// the snapshot it read-modified. Idempotent per node — the FIRST observed
    /// value is the OCC baseline; later stages against the same node keep it.
    fn observe(&mut self, core: &GraphCore, node_id: &str) {
        self.read_set
            .entry(node_id.to_string())
            .or_insert_with(|| NodeFingerprint::capture(core, node_id));
    }

    /// Capture the OCC read-set fingerprint(s) of every node a staged method references
    /// (without pushing it to any write-set). Shared by [`Self::stage`] and the lowered
    /// axiom/CONSTRUCT stagers so all paths protect the nodes they touch identically.
    fn observe_method(&mut self, core: &GraphCore, method: &Method) {
        match method {
            Method::AddNode { node_id, .. } | Method::RemoveNode { node_id } => {
                self.observe(core, node_id);
            }
            Method::CompareAndSetNodeFields { node_id, .. } => {
                self.observe(core, node_id);
            }
            Method::AddEdge {
                source_id,
                target_id,
                ..
            }
            | Method::RemoveEdge {
                source_id,
                target_id,
            } => {
                self.observe(core, source_id);
                self.observe(core, target_id);
            }
            _ => {}
        }
    }

    /// Stage one durable mutation, capturing the read-set fingerprint(s) of every
    /// node it references. The op is NOT applied to the graph here.
    pub(crate) fn stage(&mut self, core: &GraphCore, method: Method, now_ms: u64) {
        self.observe_method(core, &method);
        self.write_set.push(method);
        self.last_active_ms = now_ms;
    }

    /// Validate the OCC read-set against `core` under the held commit guard. Cheap
    /// path: if `core.version()` is unchanged since begin, no write landed and BOTH
    /// the per-node and the serializable predicate re-checks are skipped. Otherwise
    /// re-fingerprint each read-set node and require equality; under `Serializable`
    /// additionally re-evaluate each captured predicate read-set and require its
    /// result-set fingerprint to be unchanged (rejects phantoms/range anomalies).
    /// Returns `true` when the transaction may commit.
    pub(crate) fn validate(&self, core: &GraphCore) -> bool {
        // Coarse guard: no write landed at all since begin → nothing to re-check
        // (CONCEPT:EG-KG.txn.serializable-zero-cost — serializable pays nothing when the graph is untouched).
        if core.version() == self.begin_version {
            return true;
        }
        // Per-node read-set (snapshot isolation; always enforced).
        let nodes_ok = self
            .read_set
            .iter()
            .all(|(node_id, fp)| &NodeFingerprint::capture(core, node_id) == fp);
        if !nodes_ok {
            return false;
        }
        // Serializable: re-evaluate the predicate read-set. A changed result-set
        // fingerprint = a phantom inserted/removed or a matching node's relevant
        // property changed → not serializable → conflict.
        if self.isolation == IsolationLevel::Serializable {
            for (predicate, fp_at_begin) in &self.predicate_reads {
                if predicate.fingerprint(core) != *fp_at_begin {
                    return false;
                }
            }
        }
        true
    }
}

/// Server-issued opaque transaction-id source (CONCEPT:EG-KG.txn.multi-op-occ-acid).
/// IDs must now remain unique across process restarts because a prepared/terminal
/// coordinator receipt outlives `open_txns`; a per-process counter would collide
/// with durable parent/child idempotency keys after every restart.
#[derive(Debug, Default)]
pub struct TxnIdGen;

impl TxnIdGen {
    /// Next UUID-v4 transaction id.  It is exposed to the client only as an opaque
    /// capability and is one-way hashed before entering coordinator metadata.
    pub fn next(&self) -> String {
        format!("txn-{}", uuid::Uuid::new_v4().simple())
    }
}

#[cfg(test)]
mod recovery_plan_tests {
    use super::*;

    #[test]
    fn recovery_plan_is_canonical_and_omits_raw_agent_identity() {
        let core = GraphCore::new();
        let mut first = GraphTxnState::new(
            &core,
            "logical-graph".to_string(),
            "opaque-tenant-scope".to_string(),
            7,
            IsolationLevel::Snapshot,
            None,
            "raw-personal-identity".to_string(),
            10,
        );
        first.write_set.push(Method::RemoveNode {
            node_id: "node-a".to_string(),
        });
        first
            .read_set
            .insert("node-b".to_string(), NodeFingerprint::Absent);
        first
            .read_set
            .insert("node-a".to_string(), NodeFingerprint::Present(42));

        let mut second = first.clone();
        second.read_set.clear();
        second
            .read_set
            .insert("node-a".to_string(), NodeFingerprint::Present(42));
        second
            .read_set
            .insert("node-b".to_string(), NodeFingerprint::Absent);

        let encoded = first.encode_recovery_plan().unwrap();
        assert_eq!(encoded, second.encode_recovery_plan().unwrap());
        assert!(!encoded
            .windows(b"raw-personal-identity".len())
            .any(|window| window == b"raw-personal-identity"));

        let recovered = GraphTxnState::decode_recovery_plan(
            &encoded,
            "retry-identity-held-only-in-ram".to_string(),
        )
        .unwrap();
        assert_eq!(recovered.graph, "logical-graph");
        assert_eq!(recovered.tenant_scope, "opaque-tenant-scope");
        assert_eq!(recovered.begin_version, 7);
        assert_eq!(recovered.agent, "retry-identity-held-only-in-ram");
        assert_eq!(recovered.read_set, first.read_set);
    }
}
