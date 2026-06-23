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
    /// Owning agent (for the per-agent open-txn cap), or `""` when anonymous.
    pub(crate) agent: String,
    /// Monotonic ms timestamp of the last staged/begun activity, for TTL idle
    /// expiry. Updated on every stage so an actively-used txn is never swept.
    pub(crate) last_active_ms: u64,
}

impl GraphTxnState {
    pub(crate) fn new(graph: String, begin_version: u64, agent: String, now_ms: u64) -> Self {
        GraphTxnState {
            graph,
            begin_version,
            write_set: Vec::new(),
            read_set: HashMap::new(),
            agent,
            last_active_ms: now_ms,
        }
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
    /// path: if `core.version()` is unchanged since begin, no write landed and the
    /// per-node re-check is skipped. Otherwise re-fingerprint each read-set node and
    /// require equality. Returns `true` when the transaction may commit.
    pub(crate) fn validate(&self, core: &GraphCore) -> bool {
        if core.version() == self.begin_version {
            return true;
        }
        self.read_set
            .iter()
            .all(|(node_id, fp)| &NodeFingerprint::capture(core, node_id) == fp)
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
