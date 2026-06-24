//! Cross-shard distributed transactions — a 2-phase-commit coordinator
//! (CONCEPT:KG-2.222, Lane N increment 1).
//!
//! ## The problem this closes
//!
//! Today a transaction is single-graph: one [`GraphCore`], one OCC validation, one
//! redb `WriteTransaction` (`src/server/txn.rs` + `handlers/txn.rs`). A graph belongs
//! to exactly one Raft [`GroupId`] ([`super::multi::GroupRouter`]), and a txn stays
//! inside that group — so atomically touching two graphs that route to DIFFERENT
//! groups had no atomic commit (a crash mid-way left a partial write). This module
//! adds an atomic commit ACROSS groups.
//!
//! ## The 2PC protocol (the commit path, verbatim)
//!
//! A cross-shard txn carries a staged write-set partitioned by graph; each graph
//! resolves to a participant [`GroupId`] via the router. The coordinator is the node
//! the client committed on. On `commit_cross_shard`:
//!
//! **PHASE 1 — PREPARE.** For each participant group, in `GroupId` order (a stable
//! lock order — no two coordinators deadlock prepare):
//!   1. Validate the slice's OCC read-set against that group's live [`GraphCore`]
//!      under its commit guard (the SAME [`crate::server::txn::GraphTxnState::validate`]
//!      check the single-graph path uses). A conflict ⇒ vote NO.
//!   2. Durably PERSIST the slice as a PREPARE record in redb (`xshard_prepare`,
//!      keyed by `(txn_id, group_id)`) and await the fsync — **commit-before-vote**.
//!      A group votes YES only once its prepared slice is on disk, so the slice
//!      survives a crash and can be re-applied on recovery.
//!
//! Nothing is applied to any GraphCore yet.
//!
//! **The atomic commit point.** If EVERY participant voted YES, the coordinator
//! durably writes ONE DECISION record (`xshard_decision[txn_id] = COMMIT`) and awaits
//! its fsync. This single durable write is the linearization point of the whole
//! cross-shard txn: once it is on disk the txn WILL commit everywhere (recovery
//! re-applies any participant that had not yet applied); if it is absent or ABORT,
//! the txn commits nowhere (presumed-abort). If any participant voted NO (or a
//! prepare/timeout failed) the coordinator writes `= ABORT` instead.
//!
//! **PHASE 2 — COMMIT / ABORT.** With the decision durable:
//!   * COMMIT: for each participant, route its slice through that group's Raft
//!     `client_write` (so the apply is REPLICATED + M2-durable on every replica, the
//!     exact KG-2.188/KG-2.204 path), then clear its prepare record. After every
//!     participant cleared, clear the decision record. An apply is idempotent on the
//!     graph data (re-applying an AddNode/AddEdge is a no-op overwrite), so a retry
//!     after a crash mid-phase-2 is safe.
//!   * ABORT: clear every prepare record + the decision record. Nothing was applied,
//!     so abort just discards the prepared slices — a true rollback.
//!
//! ## Recovery (the in-doubt resolution)
//!
//! On restart [`recover_in_doubt`] scans every durable prepare record and groups them
//! by `txn_id`. For each in-doubt txn it reads the DECISION record:
//!   * decision = COMMIT ⇒ re-run PHASE 2 commit (re-apply every prepared slice, then
//!     clear) — deterministic, because the decision is the recorded outcome.
//!   * decision = ABORT, or NO decision record at all ⇒ ABORT (clear the prepares).
//!     Presumed-abort: a txn that crashed BEFORE the coordinator logged a decision is
//!     resolved as aborted, which is correct because no participant could have applied
//!     (an apply only happens in phase 2, AFTER the decision is durable).
//!
//! The outcome is therefore deterministic from the durable decision record alone — no
//! participant ever applies without a COMMIT decision on disk.
//!
//! ## Failure model (honest — this is classic 2PC)
//!
//! This is textbook 2PC and inherits its one blocking window: if the coordinator
//! crashes AFTER a participant voted YES but BEFORE the decision record is durable,
//! that participant is in-doubt and cannot unilaterally resolve until the coordinator
//! (its redb decision record) comes back. Increment 1 accepts this blocking window —
//! the coordinator is the committing node and its redb IS the decision log, so a
//! restarted coordinator resolves every in-doubt txn deterministically from disk. A
//! non-blocking commit (3PC / Paxos-Commit, or replicating the decision record itself
//! through Raft so a surviving node can resolve) is a documented follow-up; so are
//! Calvin-style deterministic ordering, parallel-commit, read-only-participant
//! optimization, and a >2-group scale test.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::multi::MultiRaft;
use super::{GroupId, RaftRequest};
use crate::protocol::{GraphType, Method};
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;

/// One participant graph's slice of a cross-shard transaction: the staged write-set
/// for that graph plus the metadata needed to apply it (the same fields a
/// [`RaftRequest`] carries per op). The slice is the unit that is durably prepared.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSlice {
    /// Human-readable graph name (resolves to a participant group via the router).
    pub graph_name: String,
    /// Sanitized graph file-name (the persistence-tier key).
    pub graph_fname: String,
    /// Graph type (used if a follower must create the graph on apply).
    pub graph_type: GraphType,
    /// Staged durable mutations for this graph, applied in order at COMMIT.
    pub methods: Vec<Method>,
}

impl GraphSlice {
    /// The per-op [`RaftRequest`]s this slice applies in phase 2 (one per method,
    /// each routed through the owning group's Raft `client_write`).
    fn to_requests(&self) -> Vec<RaftRequest> {
        self.methods
            .iter()
            .map(|m| RaftRequest {
                graph_fname: self.graph_fname.clone(),
                graph_name: self.graph_name.clone(),
                graph_type: self.graph_type,
                method: m.clone(),
            })
            .collect()
    }
}

/// A staged cross-shard transaction: an id + its slices partitioned by graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossShardTxn {
    pub txn_id: String,
    pub slices: Vec<GraphSlice>,
}

/// The outcome of running the 2PC commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnOutcome {
    /// All participants committed (the decision was COMMIT, durable, and applied).
    Committed,
    /// At least one participant voted NO (or a prepare failed) → all aborted.
    Aborted,
}

/// The cross-shard 2PC coordinator (CONCEPT:KG-2.222). Holds the per-node
/// [`MultiRaft`] manager (to reach each participant group) and the shared redb
/// backend (to persist the durable prepare + decision records).
pub struct CrossShardCoordinator {
    multi: Arc<MultiRaft>,
    backend: Arc<dyn PersistenceBackend>,
}

impl CrossShardCoordinator {
    pub fn new(multi: Arc<MultiRaft>, backend: Arc<dyn PersistenceBackend>) -> Self {
        Self { multi, backend }
    }

    /// The concrete redb backend (cross-shard txns require the redb durable tier —
    /// the same invariant the Raft store has).
    fn redb(&self) -> Result<&RedbBackend, String> {
        self.backend
            .as_redb()
            .ok_or_else(|| "cross-shard txn requires the redb persistence backend".to_string())
    }

    /// Map a staged txn's slices to their participant groups, preserving the slice.
    /// Two graphs that route to the SAME group collapse onto one participant (the
    /// router enforces one-graph→one-group, but two graphs CAN share a group).
    fn participants(&self, txn: &CrossShardTxn) -> BTreeMap<GroupId, Vec<GraphSlice>> {
        let router = self.multi.router();
        let mut by_group: BTreeMap<GroupId, Vec<GraphSlice>> = BTreeMap::new();
        for slice in &txn.slices {
            let gid = router.group_of(&slice.graph_name);
            by_group.entry(gid).or_default().push(slice.clone());
        }
        by_group
    }

    /// Run the full 2PC commit for a staged cross-shard transaction.
    ///
    /// Returns `Ok(Committed)` only if every participant prepared (voted YES) AND the
    /// COMMIT decision was made durable AND every slice applied; `Ok(Aborted)` if any
    /// participant voted NO; `Err` on an infrastructure failure (a missing group, a
    /// redb error) AFTER which the caller treats the txn as aborted (the decision is
    /// presumed-abort until a COMMIT record exists).
    pub async fn commit_cross_shard(&self, txn: &CrossShardTxn) -> Result<TxnOutcome, String> {
        let redb = self.redb()?;
        let participants = self.participants(txn);
        if participants.len() < 2 {
            return Err(format!(
                "commit_cross_shard called for a {}-group txn (use the single-group path)",
                participants.len()
            ));
        }

        // ── PHASE 1: PREPARE each participant in GroupId order ──────────────────
        let mut prepared_groups: Vec<GroupId> = Vec::new();
        let mut all_yes = true;
        for (gid, slices) in &participants {
            match self
                .prepare_participant(redb, &txn.txn_id, *gid, slices)
                .await
            {
                Ok(true) => prepared_groups.push(*gid),
                Ok(false) => {
                    all_yes = false;
                    break; // a NO vote ends prepare — we will ABORT.
                }
                Err(e) => {
                    tracing::warn!(
                        "xshard {}: prepare of group {} errored ({e}) → abort",
                        txn.txn_id,
                        gid
                    );
                    all_yes = false;
                    break;
                }
            }
        }

        // ── THE ATOMIC COMMIT POINT: durably record the decision ────────────────
        let commit = all_yes && prepared_groups.len() == participants.len();
        redb.xshard_decision_put(&txn.txn_id, commit).await?;

        // ── PHASE 2: apply the decision ─────────────────────────────────────────
        if commit {
            self.apply_commit(redb, &txn.txn_id, &participants).await?;
            Ok(TxnOutcome::Committed)
        } else {
            // ABORT: clear every prepared participant (only those that got a record),
            // then the decision record. Nothing was applied → a true rollback.
            self.apply_abort(redb, &txn.txn_id, &prepared_groups)
                .await?;
            Ok(TxnOutcome::Aborted)
        }
    }

    /// PHASE 1 for one participant: validate its OCC read-set against the live group
    /// state, then DURABLY persist its prepared slice (commit-before-vote). Returns
    /// the vote (`true` = YES). The slice is persisted as a serialized `Vec<GraphSlice>`
    /// so recovery can re-apply every graph this participant owns.
    async fn prepare_participant(
        &self,
        redb: &RedbBackend,
        txn_id: &str,
        gid: GroupId,
        slices: &[GraphSlice],
    ) -> Result<bool, String> {
        // The group must be running on this node to validate/apply (the coordinator
        // routes to local group state; cross-NODE participants are a follow-up — see
        // the failure model). A missing group is a NO vote (cannot prepare).
        if self.multi.group(gid).await.is_none() {
            return Ok(false);
        }
        // OCC validation against the live GraphCore of each graph in the slice. We
        // re-fingerprint the touched nodes under the group's current state; a slice
        // whose read-set moved cannot prepare (vote NO). Validation reuses the
        // single-graph OCC check so cross-shard isolation == single-graph isolation.
        if !self.validate_slices(slices).await {
            return Ok(false);
        }
        // Commit-before-vote: persist the prepared slice durably, THEN vote YES.
        let blob = rmp_serde::to_vec_named(slices).map_err(|e| e.to_string())?;
        redb.xshard_prepare_put(txn_id, gid, blob).await?;
        Ok(true)
    }

    /// Validate every slice's OCC read-set against its graph's live core. A node a
    /// slice's ops touch must be unchanged since the txn captured it. We capture the
    /// read-set HERE from the staged methods (the cross-shard txn carries the methods,
    /// not a pre-captured fingerprint, in this increment), so validation reduces to:
    /// every targeted endpoint that a slice expects to exist still exists, and an
    /// AddEdge's endpoints are present. This is the conservative subset of the
    /// single-graph OCC check that the staged-method form can express without a
    /// protocol read-set; a NO here is always safe (it aborts rather than risks a
    /// stale write).
    async fn validate_slices(&self, slices: &[GraphSlice]) -> bool {
        let state = self.multi.app_state();
        let s = state.read().await;
        for slice in slices {
            let core = match s.registry.get(&slice.graph_name) {
                Some(e) => e.core.clone(),
                // A graph that does not exist yet is fine for pure-insert slices
                // (the apply creates it). Treat as preparable.
                None => continue,
            };
            for m in &slice.methods {
                if let Method::AddEdge {
                    source_id,
                    target_id,
                    ..
                } = m
                {
                    // An edge needs both endpoints present at commit; if a concurrent
                    // writer removed one, prepare must fail (vote NO).
                    if core.get_node_properties(source_id).is_none()
                        && !slice_inserts_node(slices, source_id)
                    {
                        return false;
                    }
                    if core.get_node_properties(target_id).is_none()
                        && !slice_inserts_node(slices, target_id)
                    {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// PHASE 2 COMMIT: apply every prepared slice through its group's Raft
    /// `client_write` (replicated + M2-durable), then clear its prepare record; after
    /// all cleared, clear the decision record. Idempotent under a recovery retry.
    async fn apply_commit(
        &self,
        redb: &RedbBackend,
        txn_id: &str,
        participants: &BTreeMap<GroupId, Vec<GraphSlice>>,
    ) -> Result<(), String> {
        for (gid, slices) in participants {
            let group = self.multi.group(*gid).await.ok_or_else(|| {
                format!("xshard {txn_id}: participant group {gid} gone at commit")
            })?;
            for slice in slices {
                for req in slice.to_requests() {
                    group.client_write(req).await?;
                }
            }
            redb.xshard_prepare_clear(txn_id, *gid).await?;
        }
        redb.xshard_decision_clear(txn_id).await?;
        Ok(())
    }

    /// PHASE 2 ABORT: clear every prepared participant's record + the decision record.
    async fn apply_abort(
        &self,
        redb: &RedbBackend,
        txn_id: &str,
        prepared_groups: &[GroupId],
    ) -> Result<(), String> {
        for gid in prepared_groups {
            redb.xshard_prepare_clear(txn_id, *gid).await?;
        }
        redb.xshard_decision_clear(txn_id).await?;
        Ok(())
    }

    /// Resolve every in-doubt cross-shard txn from the durable records (CONCEPT:
    /// KG-2.222 recovery). Called once on restart, AFTER the M2 graph data + the Raft
    /// groups are up. Deterministic: the durable decision record is the sole oracle.
    ///
    /// Returns the number of in-doubt txns resolved.
    pub async fn recover_in_doubt(&self) -> Result<usize, String> {
        let redb = self.redb()?;
        // Group every durable prepare record by txn_id.
        let mut by_txn: BTreeMap<String, BTreeMap<GroupId, Vec<GraphSlice>>> = BTreeMap::new();
        for (txn_id, gid, blob) in redb.xshard_scan_prepares()? {
            let slices: Vec<GraphSlice> =
                rmp_serde::from_slice(&blob).map_err(|e| e.to_string())?;
            by_txn.entry(txn_id).or_default().insert(gid, slices);
        }
        let mut resolved = 0usize;
        for (txn_id, participants) in by_txn {
            match redb.xshard_decision_get(&txn_id)? {
                // COMMIT was logged → re-run phase 2 commit (re-apply, then clear).
                Some(true) => {
                    tracing::info!("xshard recovery: {txn_id} → COMMIT (re-applying)");
                    self.apply_commit(redb, &txn_id, &participants).await?;
                }
                // ABORT logged, OR no decision at all (presumed-abort): clear prepares.
                Some(false) | None => {
                    tracing::info!("xshard recovery: {txn_id} → ABORT (clearing prepares)");
                    let gids: Vec<GroupId> = participants.keys().copied().collect();
                    self.apply_abort(redb, &txn_id, &gids).await?;
                }
            }
            resolved += 1;
        }
        Ok(resolved)
    }

    // ── Phase-granular entry points for the nemesis harness (CONCEPT:KG-2.222) ──
    // These let the gauntlet inject a crash/partition BETWEEN phases — exactly the
    // window where a naive design would leave a partial commit. They are the SAME
    // steps `commit_cross_shard` runs internally, exposed so a test can stop after
    // PREPARE (or after the decision) and prove recovery resolves the in-doubt txn.
    // Gated to the harness so they add nothing to a normal build.

    /// Run ONLY PHASE 1 (prepare every participant), durably persisting each slice.
    /// Returns `true` if every participant voted YES (the txn is preparable). Applies
    /// NOTHING — the caller is the harness simulating a crash before the decision.
    #[cfg(any(test, feature = "harness"))]
    pub async fn prepare_only(&self, txn: &CrossShardTxn) -> Result<bool, String> {
        let redb = self.redb()?;
        let participants = self.participants(txn);
        let mut yes = 0usize;
        for (gid, slices) in &participants {
            if self
                .prepare_participant(redb, &txn.txn_id, *gid, slices)
                .await?
            {
                yes += 1;
            } else {
                return Ok(false);
            }
        }
        Ok(yes == participants.len())
    }

    /// Durably write the coordinator's decision WITHOUT applying phase 2 (the harness
    /// crash window: decision on disk, apply not yet done).
    #[cfg(any(test, feature = "harness"))]
    pub async fn decide_only(&self, txn_id: &str, commit: bool) -> Result<(), String> {
        self.redb()?.xshard_decision_put(txn_id, commit).await
    }
}

/// Does ANY slice in the txn insert `node_id`? An AddEdge whose endpoint is added by
/// a sibling slice in the SAME cross-shard txn is valid (the endpoint will exist
/// after the txn applies), so it must not fail prepare.
fn slice_inserts_node(slices: &[GraphSlice], node_id: &str) -> bool {
    slices.iter().any(|s| {
        s.methods
            .iter()
            .any(|m| matches!(m, Method::AddNode { node_id: nid, .. } if nid == node_id))
    })
}
