//! Server-side staged OCC transactions (CONCEPT:KG-2.180 — multi-operation ACID).
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
//! Isolation levels (CONCEPT:KG-2.183 — M6b). `BeginTxn` carries an
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
//! PITR hook (CONCEPT:KG-2.183): point-in-time recovery would replay the
//! append-only `ledger` table up to a target version/timestamp; the natural slot is
//! the commit path in `handlers/txn.rs` right after `core.mark_dirty()` (each
//! committed txn maps to a version bump that a PITR cursor can target). Not
//! implemented here — it requires a ledger replay API that lives outside the txn
//! subsystem.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;

use crate::graph::GraphCore;
use crate::protocol::Method;
use crate::server::ServerState;

/// Wall-clock milliseconds for the idle-TTL bookkeeping. Monotonicity is not
/// required — the TTL sweep tolerates clock skew (it auto-rolls-back only txns
/// idle *past* the TTL; a skewed clock at worst delays/advances a reclaim).
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Auto-rollback transactions idle past `ttl_secs` (CONCEPT:KG-2.180 safety rail).
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
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Transaction isolation level (CONCEPT:KG-2.183). `Snapshot` is today's behavior
/// (per-node OCC read-set only); `Serializable` additionally re-evaluates a captured
/// predicate read-set at commit to reject phantom/range anomalies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
/// commit (CONCEPT:KG-2.183 serializable). Currently the one supported predicate is
/// a label scan — the minimal staged-read primitive the txn subsystem can express
/// without a protocol change.
#[derive(Debug, Clone)]
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

/// One open server-staged transaction (CONCEPT:KG-2.180). Holds the target graph
/// name, the OCC begin-version, the staged durable-mutation write-set, and the
/// per-node read-set fingerprints for conflict detection. Stored behind a `Mutex`
/// in `ServerState::open_txns` keyed by a server-issued `txn_id`.
pub struct GraphTxnState {
    /// Target graph (resolved at begin; all staged ops apply here).
    pub(crate) graph: String,
    /// `core.version()` at begin — the coarse OCC guard.
    pub(crate) begin_version: u64,
    /// Staged durable mutations, applied in order at commit. Restricted to the
    /// durable-mutation set (AddNode/RemoveNode/AddEdge/RemoveEdge/CompareAndSet).
    pub(crate) write_set: Vec<Method>,
    /// Per-node fingerprints captured when first referenced — the OCC read-set.
    pub(crate) read_set: HashMap<String, NodeFingerprint>,
    /// Isolation level (CONCEPT:KG-2.183). `Snapshot` validates only `read_set`;
    /// `Serializable` additionally re-checks `predicate_reads`.
    pub(crate) isolation: IsolationLevel,
    /// Predicate/range reads captured at begin and re-evaluated at commit under
    /// serializable — each entry is `(predicate, fingerprint-at-begin)`. Empty under
    /// snapshot (so snapshot pays no extra cost).
    pub(crate) predicate_reads: Vec<(PredicateRead, u64)>,
    /// Owning agent (for the per-agent open-txn cap), or `""` when anonymous.
    pub(crate) agent: String,
    /// Monotonic ms timestamp of the last staged/begun activity, for TTL idle
    /// expiry. Updated on every stage so an actively-used txn is never swept.
    pub(crate) last_active_ms: u64,
    /// Multi-graph staged write-set (CONCEPT:KG-2.226 — Lane N). A `Txn*` op naming a
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
}

impl GraphTxnState {
    /// Open a fresh staged transaction. Under `Serializable` with a declared
    /// `predicate`, capture its result-set fingerprint NOW (the snapshot the txn
    /// reads against) so commit can detect a phantom/range change. `core` is the
    /// txn's target graph core (an off-lock read; nothing is held while staging).
    pub(crate) fn new(
        core: &GraphCore,
        graph: String,
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
        }
    }

    /// Stage a VECTOR upsert into the cross-modal write-set (CONCEPT:KG-2.225). The
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

    /// Stage a BLOB REFERENCE into the cross-modal write-set (CONCEPT:KG-2.225). The
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

    /// True when this txn staged a vector or blob-ref — i.e. it is a CROSS-MODAL txn
    /// whose commit must land all modalities in ONE redb `WriteTransaction`.
    pub(crate) fn is_cross_modal(&self) -> bool {
        !self.vectors.is_empty() || !self.blob_refs.is_empty()
    }

    /// Stage one durable mutation against a NAMED graph that may differ from the
    /// txn's default (CONCEPT:KG-2.226 — multi-graph). When `graph` equals the
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
    /// (CONCEPT:KG-2.226). Used at commit to detect a cross-shard span.
    pub(crate) fn touched_graphs(&self) -> Vec<String> {
        let mut out = vec![self.graph.clone()];
        out.extend(self.extra_writes.keys().cloned());
        out
    }

    /// True when this txn staged ops against a graph other than its default
    /// (CONCEPT:KG-2.226) — i.e. it is a multi-graph txn whose span the commit path
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

    /// Stage one durable mutation, capturing the read-set fingerprint(s) of every
    /// node it references. The op is NOT applied to the graph here.
    pub(crate) fn stage(&mut self, core: &GraphCore, method: Method, now_ms: u64) {
        match &method {
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
        // (CONCEPT:KG-2.183 — serializable pays nothing when the graph is untouched).
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

/// Server-issued monotonic transaction-id source (CONCEPT:KG-2.180). A plain
/// `AtomicU64` counter — no `rand`/`Date` dependency — rendered as a hex string so
/// the client can thread it back opaquely. Unique within a server process, which is
/// all the keying of `ServerState::open_txns` requires.
#[derive(Debug, Default)]
pub struct TxnIdGen(AtomicU64);

impl TxnIdGen {
    /// Next unique transaction id, e.g. `"txn-0000000000000001"`.
    pub fn next(&self) -> String {
        let n = self.0.fetch_add(1, Ordering::Relaxed) + 1;
        format!("txn-{n:016x}")
    }
}
