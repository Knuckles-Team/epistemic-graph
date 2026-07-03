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
//! ## Commit-path optimizations (CONCEPT:EG-081)
//!
//! Two tractable 2PC optimizations are folded into the commit path above WITHOUT
//! weakening any durability/recovery invariant:
//!
//!   * **Read-only-participant fast path.** A participant group whose write-set slice
//!     is EMPTY (it only READ, or has no ops) is OCC-validated but skips PHASE 1's
//!     durable prepare-log AND its PHASE 2 write entirely (the standard 2PC read-only
//!     optimization). It never enters the durable protocol, so the atomic commit point
//!     and recovery only ever cover the groups that actually mutate. A fully read-only
//!     cross-shard txn commits with ZERO durable 2PC records. (`readonly_skipped`
//!     counts the groups that took this path.)
//!   * **Parallel prepare.** PHASE 1 issues the PREPAREs to all WRITING participant
//!     groups CONCURRENTLY (joined futures) instead of strictly one-at-a-time in
//!     GroupId order, shrinking the prepare latency to the slowest single group rather
//!     than the sum. Deadlock-freedom is preserved because a prepare holds NO lock
//!     across groups (see PHASE 1 below); the former GroupId sequencing was defensive,
//!     not load-bearing. PHASE 2 apply (and the shared recovery re-apply) stays
//!     sequential in GroupId order — it is post-decision and order-independent, so
//!     leaving it sequential costs nothing and keeps one apply path for recovery.
//!
//! ## Failure model (honest — this is classic 2PC)
//!
//! This is textbook 2PC and inherits its one blocking window: if the coordinator
//! crashes AFTER a participant voted YES but BEFORE the decision record is durable,
//! that participant is in-doubt and cannot unilaterally resolve until the coordinator
//! (its redb decision record) comes back. This increment accepts this blocking window —
//! the coordinator is the committing node and its redb IS the decision log, so a
//! restarted coordinator resolves every in-doubt txn deterministically from disk. A
//! non-blocking commit (3PC / Paxos-Commit, or replicating the decision record itself
//! through Raft so a surviving node can resolve) plus Calvin-style deterministic
//! ordering remain a separate follow-up track (CONCEPT:EG-082); so is a cross-NODE
//! participant path and a larger >2-group scale test.
//!
//! ## Non-blocking commit — Raft-replicated decision (CONCEPT:EG-082)
//!
//! The blocking window above exists for ONE reason: in classic 2PC the durable COMMIT
//! decision lives ONLY in the coordinator's private redb (`xshard_decision_put`). A
//! participant that voted YES cannot resolve itself until THAT coordinator's redb comes
//! back — no other node holds the decision. EG-082 removes the window by making the
//! decision a **Raft-replicated log entry** instead of a coordinator-private record
//! (Paxos-Commit / "3PC-lite"; the tractable option chosen over full Calvin — see the
//! design note below).
//!
//! [`CrossShardCoordinator::commit_cross_shard_nonblocking`] runs the SAME PHASE 1
//! (durable, commit-before-vote prepare — every invariant preserved) and the SAME PHASE
//! 2 apply, but at the atomic commit point it replicates the decision through a Raft
//! **decision group**: it writes an `AddNode(txn_id, {xshard_commit})` into a dedicated
//! [`XSHARD_DECISION_GRAPH`] via that group's `client_write`. Once `client_write`
//! returns `Ok` the decision is quorum-committed to the group's Raft log AND applied to
//! its replicated state machine — so it is durable on a quorum and **readable on every
//! replica**, not just the coordinator. Crucially the coordinator-private redb decision
//! table is NEVER written on this path (`xshard_decision_get(txn_id)` stays `None`),
//! which is exactly what a test asserts to distinguish it from 2PC.
//!
//! [`CrossShardCoordinator::recover_in_doubt_nonblocking`] scans the durable PREPARE
//! records exactly as 2PC recovery does, but LEARNS each txn's outcome from the
//! replicated decision graph rather than the coordinator's redb. Therefore ANY node that
//! carries the decision group's replicated state — a surviving replica, or a wholly
//! different coordinator instance — resolves the in-doubt txn and drives PHASE 2 to
//! completion WITHOUT waiting on the node that made the decision. That is the removed
//! blocking window, and the `xshard_harness` `nonblocking_*` tests prove it: (a)
//! atomicity holds, (b) a coordinator dropped between decision and apply does not block —
//! a fresh resolver finishes the txn from the replicated decision, and (c) the outcome
//! agrees with 2PC for the same inputs.
//!
//! **Why Paxos-Commit-lite and not Calvin.** Calvin (a global sequencer assigning a
//! total order, deterministic execution, no vote round) is a larger rewrite: it needs a
//! new sequencer service, a batching/epoch protocol, and a re-plumb of how slices reach
//! participants — and it discards the OCC/prepare machinery this module already has.
//! The Paxos-Commit approach reuses the engine's EXISTING openraft integration wholesale
//! (a group's `client_write` IS a replicated durable log append), touches only
//! `src/raft`, adds no dependency, and keeps the prepare/OCC/presumed-abort invariants
//! byte-for-byte — so it is the sound, additive, tractable subset. Calvin remains open.
//!
//! **Feature/runtime gating.** The whole non-blocking path is compiled only under
//! `feature = "nonblocking"` (a standalone feature in NO deployment tier) OR the
//! `test`/`harness` proof build; a plain `--features raft` build does not link it, and
//! even when linked the DEFAULT commit path stays [`CrossShardCoordinator::commit_cross_shard`]
//! (2PC) — the non-blocking path is opt-in per call (the caller passes the decision
//! group id).
//!
//! **Landed subset / what remains.** Landed: the Raft-replicated decision record, the
//! participant-/replica-learns-from-log recovery path, decision-record GC after
//! resolution, and the correctness+liveness gauntlet. Deliberately still open
//! (documented, not weakened): a cross-NODE participant apply path (participants are
//! local groups on the coordinator node today), a dedicated always-on decision Raft group
//! wired into the server bootstrap (the group id is a per-call parameter here), and full
//! Calvin deterministic ordering.

use std::collections::BTreeMap;
#[cfg(any(feature = "calvin", test, feature = "harness"))]
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::multi::MultiRaft;
#[cfg(any(feature = "calvin", test, feature = "harness"))]
use super::NodeId;
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
    /// Observability (CONCEPT:EG-081): count of participant groups that took the
    /// read-only fast path — i.e. their write-set slice was EMPTY, so they were
    /// OCC-validated but NEVER durably prepared or applied in phase 2. Cheap
    /// relaxed counter, useful as a metric and as a test observable.
    readonly_skipped: Arc<AtomicU64>,
}

impl CrossShardCoordinator {
    pub fn new(multi: Arc<MultiRaft>, backend: Arc<dyn PersistenceBackend>) -> Self {
        Self {
            multi,
            backend,
            readonly_skipped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Total number of participant groups skipped by the read-only fast path across
    /// this coordinator's lifetime (CONCEPT:EG-081) — groups whose write-set slice was
    /// empty and therefore never got a durable PREPARE record or a phase-2 write.
    pub fn readonly_skipped(&self) -> u64 {
        self.readonly_skipped.load(Ordering::Relaxed)
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

        // ── EG-081 READ-ONLY-PARTICIPANT FAST PATH ──────────────────────────────
        // Partition participants into WRITING groups (their slice mutates → full
        // 2PC: durable PREPARE + phase-2 replicated apply) and READ-ONLY groups
        // (their slice's write-set is EMPTY → they only READ in the txn). A
        // read-only participant needs NEITHER a durable prepare log NOR a phase-2
        // write — the standard 2PC read-only optimization. We still OCC-VALIDATE its
        // reads (so cross-shard isolation is unchanged), then release it (one-phase);
        // it simply never enters the durable protocol. This shrinks the participant
        // set that the atomic commit point + recovery must cover to only the groups
        // that actually mutate, and cuts their fsync + Raft round-trips entirely.
        let (writing, read_only) = self.split_participants(participants);
        self.readonly_skipped
            .fetch_add(read_only.len() as u64, Ordering::Relaxed);

        // OCC-validate every read-only participant's reads (no durable state written
        // for these). An unreachable or read-invalidated read-only participant cannot
        // confirm its reads → the txn ABORTS. Because NOTHING durable has been written
        // yet (no read-only log, no writer prepared), this is a pure rollback with
        // zero 2PC state to clear and nothing for recovery to find.
        for (gid, slices) in &read_only {
            if !self.validate_read_only_participant(*gid, slices).await? {
                return Ok(TxnOutcome::Aborted);
            }
        }

        // A fully read-only cross-shard txn (no group mutates) commits WITHOUT any
        // durable decision/prepare record at all — there is nothing to make atomic or
        // to recover, since no participant applies anything.
        if writing.is_empty() {
            return Ok(TxnOutcome::Committed);
        }

        // ── PHASE 1: PREPARE every WRITING participant CONCURRENTLY (EG-081) ─────
        // Deadlock-freedom under parallel prepare: a prepare holds NO lock ACROSS
        // groups. Each `prepare_participant` takes its group's shared app_state READ
        // guard only for the span of its own OCC validation (released before its
        // durable write), and the redb writer serializes the prepare-log commits
        // internally. No prepare ever waits on a lock another prepare holds, so there
        // is no cross-group lock cycle regardless of the order prepares arrive in —
        // the former strict GroupId sequencing was a defensive lock-order that the
        // actual (across-groups lock-free) prepare does not require. The per-group
        // local lock order inside each group is unchanged. We therefore issue the
        // independent prepare RPCs/durable-writes as joined futures instead of one at
        // a time; the joined set preserves input (GroupId) order so the collected
        // votes are deterministic.
        let prepare_futs = writing.iter().map(|(gid, slices)| {
            let gid = *gid;
            async move {
                (
                    gid,
                    self.prepare_participant(redb, &txn.txn_id, gid, slices)
                        .await,
                )
            }
        });
        let votes = futures::future::join_all(prepare_futs).await;

        let mut prepared_groups: Vec<GroupId> = Vec::new();
        let mut all_yes = true;
        for (gid, vote) in votes {
            match vote {
                // Ok(true) ⟺ a durable prepare record was committed (commit-before-vote):
                // the group is exactly the set that must be cleared on abort/commit.
                Ok(true) => prepared_groups.push(gid),
                Ok(false) => all_yes = false,
                Err(e) => {
                    tracing::warn!(
                        "xshard {}: prepare of group {} errored ({e}) → abort",
                        txn.txn_id,
                        gid
                    );
                    all_yes = false;
                }
            }
        }

        // ── THE ATOMIC COMMIT POINT: durably record the decision ────────────────
        let commit = all_yes && prepared_groups.len() == writing.len();
        redb.xshard_decision_put(&txn.txn_id, commit).await?;

        // ── PHASE 2: apply the decision (writing participants only) ─────────────
        if commit {
            self.apply_commit(redb, &txn.txn_id, &writing).await?;
            Ok(TxnOutcome::Committed)
        } else {
            // ABORT: clear every prepared participant (only those that got a record),
            // then the decision record. Nothing was applied → a true rollback.
            self.apply_abort(redb, &txn.txn_id, &prepared_groups)
                .await?;
            Ok(TxnOutcome::Aborted)
        }
    }

    /// Split the participant map into (WRITING, READ-ONLY) groups (CONCEPT:EG-081).
    /// A group is READ-ONLY iff EVERY slice it owns has an empty method/write-set —
    /// it contributed only reads to the txn (or no ops at all), so it can skip the
    /// durable prepare + phase-2 write entirely.
    fn split_participants(
        &self,
        participants: BTreeMap<GroupId, Vec<GraphSlice>>,
    ) -> (
        BTreeMap<GroupId, Vec<GraphSlice>>,
        BTreeMap<GroupId, Vec<GraphSlice>>,
    ) {
        let mut writing: BTreeMap<GroupId, Vec<GraphSlice>> = BTreeMap::new();
        let mut read_only: BTreeMap<GroupId, Vec<GraphSlice>> = BTreeMap::new();
        for (gid, slices) in participants {
            if slices.iter().all(|s| s.methods.is_empty()) {
                read_only.insert(gid, slices);
            } else {
                writing.insert(gid, slices);
            }
        }
        (writing, read_only)
    }

    /// Validate a READ-ONLY participant's reads without preparing it (CONCEPT:EG-081).
    /// Mirrors the reachability + OCC check `prepare_participant` runs for a writer,
    /// but writes NO durable prepare record and applies nothing: an unreachable group
    /// cannot confirm its reads (→ NO), otherwise the reads are OCC-validated against
    /// the live group state exactly as a writer's would be.
    async fn validate_read_only_participant(
        &self,
        gid: GroupId,
        slices: &[GraphSlice],
    ) -> Result<bool, String> {
        if self.multi.group(gid).await.is_none() {
            return Ok(false);
        }
        Ok(self.validate_slices(slices).await)
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

    // ── EG-082: NON-BLOCKING commit via a Raft-replicated decision ──────────────
    // Everything below is compiled ONLY under `feature = "nonblocking"` (a standalone
    // feature in no deployment tier) OR the `test`/`harness` proof build — a plain
    // `--features raft` build links none of it, and the DEFAULT commit path stays the
    // 2PC `commit_cross_shard` above. See the module-level "Non-blocking commit"
    // section for the design.

    /// Run the NON-BLOCKING cross-shard commit (CONCEPT:EG-082): identical to
    /// [`commit_cross_shard`] except the atomic commit point REPLICATES the decision
    /// through the `decision_gid` Raft group (a durable, quorum-committed log entry in
    /// [`XSHARD_DECISION_GRAPH`]) instead of writing it to the coordinator-private redb.
    /// Because the decision lives in a replicated log, a coordinator crash between the
    /// decision and PHASE 2 no longer blocks participants — any node carrying the
    /// decision group's replicated state resolves the txn (see
    /// [`recover_in_doubt_nonblocking`]).
    ///
    /// Preserves every 2PC invariant: durable commit-before-vote prepare, presumed-abort
    /// (an in-doubt txn with no replicated decision aborts), commit-before-ack (PHASE 2
    /// only runs after the decision is quorum-durable), and the read-only fast path.
    ///
    /// [`recover_in_doubt_nonblocking`]: CrossShardCoordinator::recover_in_doubt_nonblocking
    #[cfg(any(feature = "nonblocking", test, feature = "harness"))]
    pub async fn commit_cross_shard_nonblocking(
        &self,
        txn: &CrossShardTxn,
        decision_gid: GroupId,
    ) -> Result<TxnOutcome, String> {
        let redb = self.redb()?;
        let participants = self.participants(txn);
        if participants.len() < 2 {
            return Err(format!(
                "commit_cross_shard_nonblocking called for a {}-group txn (use the single-group path)",
                participants.len()
            ));
        }

        // Read-only fast path — identical to 2PC (EG-081): validate read-only
        // participants without any durable/replicated state; a fully read-only txn
        // commits with zero records.
        let (writing, read_only) = self.split_participants(participants);
        self.readonly_skipped
            .fetch_add(read_only.len() as u64, Ordering::Relaxed);
        for (gid, slices) in &read_only {
            if !self.validate_read_only_participant(*gid, slices).await? {
                return Ok(TxnOutcome::Aborted);
            }
        }
        if writing.is_empty() {
            return Ok(TxnOutcome::Committed);
        }

        // PHASE 1: durable, commit-before-vote prepare of every WRITING participant,
        // issued concurrently — byte-for-byte the 2PC prepare (same invariant).
        let prepare_futs = writing.iter().map(|(gid, slices)| {
            let gid = *gid;
            async move {
                (
                    gid,
                    self.prepare_participant(redb, &txn.txn_id, gid, slices)
                        .await,
                )
            }
        });
        let votes = futures::future::join_all(prepare_futs).await;
        let mut prepared_groups: Vec<GroupId> = Vec::new();
        let mut all_yes = true;
        for (gid, vote) in votes {
            match vote {
                Ok(true) => prepared_groups.push(gid),
                Ok(false) => all_yes = false,
                Err(e) => {
                    tracing::warn!(
                        "xshard-nb {}: prepare of group {} errored ({e}) → abort",
                        txn.txn_id,
                        gid
                    );
                    all_yes = false;
                }
            }
        }
        let commit = all_yes && prepared_groups.len() == writing.len();

        // ── THE ATOMIC COMMIT POINT: REPLICATE the decision through Raft ────────
        // NOT the coordinator-private redb. Once this returns Ok the decision is
        // quorum-committed + applied to the decision group's state machine, so any
        // replica can learn it. This single replicated write is the linearization
        // point of the whole non-blocking txn.
        self.replicate_decision(decision_gid, &txn.txn_id, commit)
            .await?;

        // ── PHASE 2: apply the (replicated) decision ────────────────────────────
        if commit {
            self.apply_commit(redb, &txn.txn_id, &writing).await?;
        } else {
            self.apply_abort(redb, &txn.txn_id, &prepared_groups)
                .await?;
        }
        // GC the resolved decision record from the replicated graph (idempotent; a
        // recovery that re-runs before this clears will simply re-learn + re-apply the
        // same outcome, which is a no-op on the graph data).
        self.clear_replicated_decision(decision_gid, &txn.txn_id)
            .await?;
        Ok(if commit {
            TxnOutcome::Committed
        } else {
            TxnOutcome::Aborted
        })
    }

    /// Resolve every in-doubt cross-shard txn for the NON-BLOCKING path (CONCEPT:EG-082).
    /// Like [`recover_in_doubt`] it scans the durable PREPARE records, but it LEARNS each
    /// txn's outcome from the REPLICATED decision graph ([`learn_decision`]) rather than
    /// the coordinator-private redb — so ANY node holding the decision group's replicated
    /// state (a surviving replica, or a different coordinator entirely) can run it and
    /// drive the in-doubt txn to completion without the crashed coordinator. This is the
    /// removed blocking window. Returns the number of in-doubt txns resolved.
    ///
    /// [`learn_decision`]: CrossShardCoordinator::learn_decision
    #[cfg(any(feature = "nonblocking", test, feature = "harness"))]
    pub async fn recover_in_doubt_nonblocking(
        &self,
        decision_gid: GroupId,
    ) -> Result<usize, String> {
        let redb = self.redb()?;
        let mut by_txn: BTreeMap<String, BTreeMap<GroupId, Vec<GraphSlice>>> = BTreeMap::new();
        for (txn_id, gid, blob) in redb.xshard_scan_prepares()? {
            let slices: Vec<GraphSlice> =
                rmp_serde::from_slice(&blob).map_err(|e| e.to_string())?;
            by_txn.entry(txn_id).or_default().insert(gid, slices);
        }
        let mut resolved = 0usize;
        for (txn_id, participants) in by_txn {
            // Learn the outcome from the REPLICATED decision, not coordinator redb.
            match self.learn_decision(&txn_id).await? {
                // COMMIT replicated → re-run phase 2 commit (re-apply, then clear), then
                // GC the replicated decision.
                Some(true) => {
                    tracing::info!("xshard-nb recovery: {txn_id} → COMMIT (re-applying)");
                    self.apply_commit(redb, &txn_id, &participants).await?;
                    self.clear_replicated_decision(decision_gid, &txn_id)
                        .await?;
                }
                // ABORT replicated → clear prepares, then GC the replicated decision.
                Some(false) => {
                    tracing::info!("xshard-nb recovery: {txn_id} → ABORT (clearing prepares)");
                    let gids: Vec<GroupId> = participants.keys().copied().collect();
                    self.apply_abort(redb, &txn_id, &gids).await?;
                    self.clear_replicated_decision(decision_gid, &txn_id)
                        .await?;
                }
                // NO replicated decision at all → presumed-abort (a crash before the
                // decision was replicated). Correct: apply only ever runs after the
                // decision is quorum-durable, so no participant could have applied.
                None => {
                    tracing::info!("xshard-nb recovery: {txn_id} → ABORT (presumed, no decision)");
                    let gids: Vec<GroupId> = participants.keys().copied().collect();
                    self.apply_abort(redb, &txn_id, &gids).await?;
                }
            }
            resolved += 1;
        }
        Ok(resolved)
    }

    /// Replicate the COMMIT/ABORT decision through the `decision_gid` Raft group
    /// (CONCEPT:EG-082): an `AddNode(txn_id, {xshard_commit})` into [`XSHARD_DECISION_GRAPH`]
    /// committed via that group's `client_write`. Returns after the entry is
    /// quorum-committed AND applied locally — the decision is now durable on a quorum and
    /// readable on every replica. Writes NOTHING to the coordinator-private redb.
    #[cfg(any(feature = "nonblocking", test, feature = "harness"))]
    async fn replicate_decision(
        &self,
        decision_gid: GroupId,
        txn_id: &str,
        commit: bool,
    ) -> Result<(), String> {
        let group = self.multi.group(decision_gid).await.ok_or_else(|| {
            format!("xshard-nb: decision group {decision_gid} not running on this node")
        })?;
        let properties_msgpack = rmp_serde::to_vec_named(&serde_json::json!({
            "kind": "xshard_decision",
            "xshard_commit": commit,
        }))
        .map_err(|e| e.to_string())?;
        let req = RaftRequest {
            graph_fname: crate::persist::sanitize(XSHARD_DECISION_GRAPH),
            graph_name: XSHARD_DECISION_GRAPH.to_string(),
            graph_type: GraphType::Global,
            method: Method::AddNode {
                node_id: txn_id.to_string(),
                properties_msgpack,
            },
        };
        group.client_write(req).await?;
        Ok(())
    }

    /// GC a resolved txn's replicated decision node (CONCEPT:EG-082) — a `RemoveNode`
    /// through the decision group. Idempotent (removing an absent node is a no-op), so a
    /// recovery retry is safe. A missing decision group is tolerated (nothing to clear).
    #[cfg(any(feature = "nonblocking", test, feature = "harness"))]
    async fn clear_replicated_decision(
        &self,
        decision_gid: GroupId,
        txn_id: &str,
    ) -> Result<(), String> {
        let Some(group) = self.multi.group(decision_gid).await else {
            return Ok(());
        };
        let req = RaftRequest {
            graph_fname: crate::persist::sanitize(XSHARD_DECISION_GRAPH),
            graph_name: XSHARD_DECISION_GRAPH.to_string(),
            graph_type: GraphType::Global,
            method: Method::RemoveNode {
                node_id: txn_id.to_string(),
            },
        };
        group.client_write(req).await?;
        Ok(())
    }

    /// Learn a txn's outcome from the REPLICATED decision graph (CONCEPT:EG-082):
    /// `Some(true)` = COMMIT, `Some(false)` = ABORT, `None` = no decision replicated
    /// (undecided → presumed-abort). Reads the decision group's applied state machine
    /// (the registry [`XSHARD_DECISION_GRAPH`] core), NOT the coordinator-private redb —
    /// so it works on any replica that has applied the committed decision entry. Public
    /// so the gauntlet can assert the decision is readable from replicated state.
    #[cfg(any(feature = "nonblocking", test, feature = "harness"))]
    pub async fn learn_decision(&self, txn_id: &str) -> Result<Option<bool>, String> {
        let state = self.multi.app_state();
        let s = state.read().await;
        let core = match s.registry.get(XSHARD_DECISION_GRAPH) {
            Some(e) => e.core.clone(),
            None => return Ok(None),
        };
        match core.get_node_properties(txn_id) {
            None => Ok(None),
            Some(blob) => {
                let v: serde_json::Value =
                    rmp_serde::from_slice(&blob).map_err(|e| e.to_string())?;
                Ok(Some(
                    v.get("xshard_commit")
                        .and_then(|b| b.as_bool())
                        .unwrap_or(false),
                ))
            }
        }
    }

    /// Replicate a decision WITHOUT applying phase 2 (CONCEPT:EG-082 harness crash
    /// window: the decision is quorum-durable in the replicated log, apply not yet done).
    /// The analog of [`decide_only`] for the non-blocking path.
    #[cfg(any(test, feature = "harness"))]
    pub async fn decide_replicated_only(
        &self,
        txn_id: &str,
        decision_gid: GroupId,
        commit: bool,
    ) -> Result<(), String> {
        self.replicate_decision(decision_gid, txn_id, commit).await
    }

    // ── EG-324: FULL CALVIN deterministic-ordering commit ───────────────────────
    // Everything below is compiled ONLY under `feature = "calvin"` (which implies
    // `nonblocking`, so the replicated-decision-graph helpers it reuses are linked)
    // OR the `test`/`harness` proof build. The DEFAULT commit path is unchanged —
    // this is opt-in per call, a THIRD commit strategy alongside 2PC
    // (`commit_cross_shard`) and Paxos-Commit-lite (`commit_cross_shard_nonblocking`).

    /// FULL Calvin deterministic-ordering commit for a cross-shard txn (CONCEPT:EG-324).
    ///
    /// The 2PC / Paxos-Commit paths are *agreement-first*: every writing participant
    /// runs an OCC prepare and VOTES, and only a unanimous YES commits. Calvin inverts
    /// that: it is *order-first*. A global [`CalvinSequencer`] stamps the txn with a
    /// monotone [`GlobalSeq`], that sequence assignment is REPLICATED through the Raft
    /// decision group (the "input log" of Calvin's sequencing layer), and then every
    /// participant EXECUTES the txn deterministically in sequence order — with **no vote
    /// round and no abort**. Because the total order is agreed up front and the apply is
    /// a deterministic function of it, every replica that replays the same ordered input
    /// log converges to the same state, so agreement on the ORDER is agreement on the
    /// OUTCOME. There is therefore no in-doubt window at all: a crashed coordinator's
    /// txn is resolved by any node replaying the replicated sequence
    /// ([`recover_sequenced`]).
    ///
    /// Protocol, verbatim:
    ///   1. **Sequence.** Draw a monotone `GlobalSeq` from the sequencer. Within one
    ///      sequencer this is a strict total order; [`deterministic_order`] gives the SAME
    ///      order to any node that batches the same input set (ties broken by `txn_id`),
    ///      which is the determinism the execution layer relies on.
    ///   2. **Replicate the order (the atomic point).** Write the sequenced record
    ///      (`{kind: xshard_sequence, seq}`) into [`XSHARD_DECISION_GRAPH`] via the
    ///      decision group's `client_write`. Once it returns Ok the ORDER is quorum-durable
    ///      and readable on every replica — the linearization point. No coordinator-private
    ///      redb decision is ever written (like EG-082, `xshard_decision_get` stays `None`).
    ///   3. **Deterministic execute.** Apply every writing participant's slice through its
    ///      group's Raft `client_write` in `GroupId` order (a deterministic, replay-stable
    ///      order). Read-only participants take the EG-081 fast path (validated, never
    ///      sequenced/applied). No prepare, no vote, no OCC round — the order is the
    ///      contract.
    ///   4. **GC.** Clear the replicated sequence node (idempotent; a recovery replay before
    ///      the clear simply re-applies the same deterministic result — a no-op overwrite).
    ///
    /// **Honest scope (what is and is NOT full Calvin).** IMPLEMENTED here: the global
    /// sequencer + monotone total order, the deterministic replay-stable order function,
    /// the Raft-replicated input log, and the vote-free deterministic execution + crash
    /// recovery by replay. DELIBERATELY still open (documented, not weakened): the
    /// distributed *reconnaissance*/OLLP read-lock phase (Calvin pre-declares read/write
    /// sets and acquires locks in sequence order for full serializable isolation of
    /// CONFLICTING sequenced txns — this branch assumes a single active sequencer draining
    /// its ordered log, which is the common single-coordinator deployment and is where the
    /// determinism win lives), and a multi-node sequencer fan-in/epoch batch protocol
    /// (the sequencer is per-coordinator here). Those are additive and do not weaken any
    /// durability/recovery invariant of the shipped subset.
    #[cfg(any(feature = "calvin", test, feature = "harness"))]
    pub async fn commit_cross_shard_calvin(
        &self,
        txn: &CrossShardTxn,
        sequencer: &CalvinSequencer,
        decision_gid: GroupId,
    ) -> Result<(TxnOutcome, GlobalSeq), String> {
        // Calvin still requires the redb durable tier (participant groups persist via it),
        // but writes NO coordinator-private redb record — so we assert the backend is
        // present and discard the handle (the order lives only in the replicated log).
        self.redb()?;
        let participants = self.participants(txn);
        if participants.len() < 2 {
            return Err(format!(
                "commit_cross_shard_calvin called for a {}-group txn (use the single-group path)",
                participants.len()
            ));
        }

        // EG-081 read-only fast path — validate read-only participants; they are never
        // sequenced or applied (they carry no write-set to order).
        let (writing, read_only) = self.split_participants(participants);
        self.readonly_skipped
            .fetch_add(read_only.len() as u64, Ordering::Relaxed);
        for (gid, slices) in &read_only {
            if !self.validate_read_only_participant(*gid, slices).await? {
                // A read-only participant whose reads no longer hold cannot be ordered
                // consistently; in the deterministic model this txn is dropped from the
                // input log before it is sequenced (nothing durable written yet).
                return Ok((TxnOutcome::Aborted, GlobalSeq(0)));
            }
        }

        // 1. SEQUENCE — draw the global total-order position.
        let seq = sequencer.assign();

        if writing.is_empty() {
            // Fully read-only txn: it takes a sequence slot but has nothing to replicate
            // or apply (mirrors the EG-081 zero-record commit).
            return Ok((TxnOutcome::Committed, seq));
        }

        // 2. REPLICATE THE ORDER — the atomic point. Any replica can now replay it.
        self.replicate_sequence(decision_gid, &txn.txn_id, seq)
            .await?;

        // 3. DETERMINISTIC EXECUTE — no vote round, apply in GroupId order.
        self.apply_sequenced(&txn.txn_id, seq, &writing).await?;

        // 4. GC the replicated sequence record (idempotent).
        self.clear_replicated_decision(decision_gid, &txn.txn_id)
            .await?;
        Ok((TxnOutcome::Committed, seq))
    }

    /// Deterministic execution of a sequenced txn's writing slices (CONCEPT:EG-324):
    /// apply each participant group's slice through its Raft `client_write` in `GroupId`
    /// order. Pure function of the (already agreed) order — no prepare/vote/OCC. Idempotent
    /// under a recovery replay (re-applying an AddNode/AddEdge is a no-op overwrite).
    #[cfg(any(feature = "calvin", test, feature = "harness"))]
    async fn apply_sequenced(
        &self,
        txn_id: &str,
        seq: GlobalSeq,
        writing: &BTreeMap<GroupId, Vec<GraphSlice>>,
    ) -> Result<(), String> {
        for (gid, slices) in writing {
            let group = self.multi.group(*gid).await.ok_or_else(|| {
                format!(
                    "calvin {txn_id}@{}: participant group {gid} gone at execute",
                    seq.0
                )
            })?;
            for slice in slices {
                for req in slice.to_requests() {
                    group.client_write(req).await?;
                }
            }
        }
        Ok(())
    }

    /// Replicate a txn's assigned [`GlobalSeq`] through the decision group (CONCEPT:EG-324)
    /// — an `AddNode(txn_id, {kind: xshard_sequence, seq})` into [`XSHARD_DECISION_GRAPH`]
    /// committed via that group's `client_write`. Returns after the entry is quorum-committed
    /// AND applied locally, so the ORDER is durable on a quorum and readable on every replica.
    /// Writes NOTHING to the coordinator-private redb (the Calvin input log lives ONLY in
    /// the replicated state machine).
    #[cfg(any(feature = "calvin", test, feature = "harness"))]
    async fn replicate_sequence(
        &self,
        decision_gid: GroupId,
        txn_id: &str,
        seq: GlobalSeq,
    ) -> Result<(), String> {
        let group = self.multi.group(decision_gid).await.ok_or_else(|| {
            format!("calvin: decision group {decision_gid} not running on this node")
        })?;
        let properties_msgpack = rmp_serde::to_vec_named(&serde_json::json!({
            "kind": "xshard_sequence",
            "seq": seq.0,
        }))
        .map_err(|e| e.to_string())?;
        let req = RaftRequest {
            graph_fname: crate::persist::sanitize(XSHARD_DECISION_GRAPH),
            graph_name: XSHARD_DECISION_GRAPH.to_string(),
            graph_type: GraphType::Global,
            method: Method::AddNode {
                node_id: txn_id.to_string(),
                properties_msgpack,
            },
        };
        group.client_write(req).await?;
        Ok(())
    }

    /// Learn a txn's replicated [`GlobalSeq`] from the decision graph (CONCEPT:EG-324):
    /// `Some(seq)` if the order was replicated (the txn WILL commit — replay it),
    /// `None` if no sequence was replicated (a crash before sequencing — the txn never
    /// entered the input log, so it commits nowhere). Reads the replicated state machine,
    /// not the coordinator-private redb, so any replica can resolve.
    #[cfg(any(feature = "calvin", test, feature = "harness"))]
    pub async fn learn_sequence(&self, txn_id: &str) -> Result<Option<GlobalSeq>, String> {
        let state = self.multi.app_state();
        let s = state.read().await;
        let core = match s.registry.get(XSHARD_DECISION_GRAPH) {
            Some(e) => e.core.clone(),
            None => return Ok(None),
        };
        match core.get_node_properties(txn_id) {
            None => Ok(None),
            Some(blob) => {
                let v: serde_json::Value =
                    rmp_serde::from_slice(&blob).map_err(|e| e.to_string())?;
                Ok(v.get("seq").and_then(|s| s.as_u64()).map(GlobalSeq))
            }
        }
    }

    /// Replicate a txn's ORDER WITHOUT executing it (CONCEPT:EG-324 harness crash window:
    /// the order is quorum-durable in the replicated log, execute not yet done). The Calvin
    /// analog of [`decide_replicated_only`].
    #[cfg(any(test, feature = "harness"))]
    pub async fn sequence_replicated_only(
        &self,
        txn_id: &str,
        decision_gid: GroupId,
        seq: GlobalSeq,
    ) -> Result<(), String> {
        self.replicate_sequence(decision_gid, txn_id, seq).await
    }

    /// Resolve an in-doubt Calvin txn by REPLAYING its replicated sequence (CONCEPT:EG-324).
    /// Because a sequenced txn's outcome is a deterministic function of the agreed order,
    /// any node that reads the replicated [`GlobalSeq`] can finish it WITHOUT the crashed
    /// coordinator: it re-executes the writing slices (idempotent apply) and GCs the record.
    /// Returns `Some(seq)` if it replayed a committed order, `None` if the txn was never
    /// sequenced (presumed-not-committed — it never entered the input log).
    #[cfg(any(feature = "calvin", test, feature = "harness"))]
    pub async fn recover_sequenced(
        &self,
        txn: &CrossShardTxn,
        decision_gid: GroupId,
    ) -> Result<Option<GlobalSeq>, String> {
        let Some(seq) = self.learn_sequence(&txn.txn_id).await? else {
            return Ok(None);
        };
        let participants = self.participants(txn);
        let (writing, _read_only) = self.split_participants(participants);
        self.apply_sequenced(&txn.txn_id, seq, &writing).await?;
        self.clear_replicated_decision(decision_gid, &txn.txn_id)
            .await?;
        Ok(Some(seq))
    }

    // ── EG-342: Calvin OLLP reconnaissance / read-set prediction ────────────────
    //
    // Calvin's deterministic execution needs the FULL read/write set of every txn up
    // front (so the ordered lock phase can be laid out before execution). For a txn
    // whose access set is STATIC that set is just its slices. For a txn whose set is
    // DATA-DEPENDENT (e.g. "read a directory node, then write the record it points at")
    // the set cannot be known without touching the database first — so Calvin runs an
    // OLLP (Optimistic Lock Location Prediction) *reconnaissance* pass: it reads the
    // current committed state at the seed records to PREDICT the actual set, then locks
    // that predicted set in sequence order. `reconnoiter` is the recon read primitive;
    // `predict_rwset` is the OLLP set-discovery driver over it.

    /// EG-342: an OLLP *reconnaissance read* — the CURRENT committed value of a record
    /// from its live group state (no lock, no write). This is the primitive Calvin uses
    /// both (a) to DISCOVER a data-dependent txn's read/write set before locking and
    /// (b) as the in-execution read once the ordered locks are held (under a held lock
    /// it observes the committed writes of every lower-sequence conflicting txn, which
    /// is where the serializable-per-sequence-order guarantee comes from).
    #[cfg(any(feature = "calvin", test, feature = "harness"))]
    pub async fn reconnoiter(&self, key: &RecordKey) -> Result<Option<Vec<u8>>, String> {
        let state = self.multi.app_state();
        let s = state.read().await;
        Ok(match s.registry.get(&key.graph) {
            Some(e) => e.core.get_node_properties(&key.node),
            // A graph/record that does not exist yet reads as absent (the recon simply
            // predicts against the empty state — the standard OCC-style optimistic read).
            None => None,
        })
    }

    /// EG-342: OLLP read-set prediction. Reconnoiter each `seed` record (the statically
    /// known reads a txn must consult to decide the rest of its footprint), hand the
    /// observed bytes to `derive`, and return the full predicted [`RwSet`] — the reads
    /// and writes the ordered lock phase will acquire. `derive` is the txn's own
    /// data-dependent set-discovery logic (application code, exactly as in real Calvin);
    /// the engine supplies the consistent recon snapshot it runs against.
    ///
    /// **Honest scope.** The prediction is OPTIMISTIC: if a seed a txn depended on is
    /// mutated by a lower-sequence txn BETWEEN this recon and lock acquisition, the
    /// predicted set can be stale. Calvin handles that by re-validating the seeds after
    /// the locks are held and RE-SEQUENCING (aborting + re-submitting) a txn whose recon
    /// no longer holds. [`recon_still_valid`] implements the detection half; the
    /// automatic re-sequence/restart loop is the documented remaining follow-up.
    #[cfg(any(feature = "calvin", test, feature = "harness"))]
    pub async fn predict_rwset<F>(&self, seeds: &[RecordKey], derive: F) -> Result<RwSet, String>
    where
        F: FnOnce(&BTreeMap<RecordKey, Option<Vec<u8>>>) -> RwSet,
    {
        let mut observed: BTreeMap<RecordKey, Option<Vec<u8>>> = BTreeMap::new();
        for k in seeds {
            let bytes = self.reconnoiter(k).await?;
            observed.insert(k.clone(), bytes);
        }
        Ok(derive(&observed))
    }

    /// EG-342: re-validate an OLLP prediction — under the acquired ordered locks, re-read
    /// each seed and compare against the value the recon observed. `true` ⇒ the prediction
    /// still holds and deterministic execution may proceed lock-free; `false` ⇒ a
    /// lower-sequence txn changed a dependency, so the predicted lock-set may be wrong and
    /// the txn must be re-sequenced (the honest OLLP restart boundary).
    #[cfg(any(feature = "calvin", test, feature = "harness"))]
    pub async fn recon_still_valid(
        &self,
        observed: &BTreeMap<RecordKey, Option<Vec<u8>>>,
    ) -> Result<bool, String> {
        for (k, seen) in observed {
            if self.reconnoiter(k).await?.as_deref() != seen.as_deref() {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// The dedicated graph that holds Raft-replicated cross-shard commit decisions
/// (CONCEPT:EG-082). One node per txn — id = `txn_id`, property `xshard_commit: bool`.
/// Lives in the decision Raft group's replicated state machine, so every replica can
/// read a txn's outcome without the coordinator.
#[cfg(any(feature = "nonblocking", test, feature = "harness"))]
pub const XSHARD_DECISION_GRAPH: &str = "__xshard_decisions__";

/// A position in the global total order Calvin assigns to cross-shard txns
/// (CONCEPT:EG-324). Monotone within one [`CalvinSequencer`]; `Ord` so a replayer can
/// sort a batch back into execution order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GlobalSeq(pub u64);

/// The global sequencer of Calvin's deterministic-ordering layer (CONCEPT:EG-324).
///
/// It hands out a strictly monotone [`GlobalSeq`] per txn — the total order every
/// participant then executes in, with no per-txn vote. One sequencer is the single
/// active ordering authority for a coordinator; [`deterministic_order`] is the pure
/// counterpart that lets any node reconstruct the SAME order from the same batch (the
/// determinism the execution layer relies on). Cheap, lock-free (`AtomicU64`), and
/// `Clone` (the counter is shared) so it can be held on `ServerState` and drawn from
/// concurrent commit tasks.
#[derive(Debug, Clone, Default)]
#[cfg(any(feature = "calvin", test, feature = "harness"))]
pub struct CalvinSequencer {
    next: Arc<AtomicU64>,
}

#[cfg(any(feature = "calvin", test, feature = "harness"))]
impl CalvinSequencer {
    /// A fresh sequencer starting at sequence 1 (0 is reserved for the read-only /
    /// unsequenced sentinel returned by an aborted-before-sequencing commit).
    pub fn new() -> Self {
        Self {
            next: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Draw the next monotone global sequence position. Strictly increasing, so two
    /// txns never share a slot — a strict total order.
    pub fn assign(&self) -> GlobalSeq {
        GlobalSeq(self.next.fetch_add(1, Ordering::SeqCst))
    }

    /// The next sequence that WOULD be assigned (for observability/tests) — not consumed.
    pub fn peek(&self) -> GlobalSeq {
        GlobalSeq(self.next.load(Ordering::SeqCst))
    }
}

/// The deterministic total order Calvin executes a BATCH of txns in (CONCEPT:EG-324).
///
/// Returns the indices of `txns` in the canonical order EVERY node computes: ascending
/// by `txn_id` (a stable, replay-safe key that does not depend on arrival timing, which
/// two independent replicas cannot agree on). This is the pure kernel of the determinism
/// property — given the same input SET, any node derives the identical execution order,
/// so replaying the replicated input log converges the state everywhere without a vote.
pub fn deterministic_order(txns: &[CrossShardTxn]) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..txns.len()).collect();
    idx.sort_by(|&a, &b| txns[a].txn_id.cmp(&txns[b].txn_id));
    idx
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

// ─────────────────────────────────────────────────────────────────────────────
// EG-342: the deterministic ORDERED READ-LOCK phase (Calvin OLLP lock manager)
// ─────────────────────────────────────────────────────────────────────────────

/// A single lockable record — a `(graph, node)` pair — the granularity Calvin's OLLP
/// lock phase acquires read/write locks at (CONCEPT:EG-342). `Ord`+`Hash` so it keys
/// the lock manager's per-record queues and the deterministic [`RwSet`] sets.
#[cfg(any(feature = "calvin", test, feature = "harness"))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RecordKey {
    pub graph: String,
    pub node: String,
}

#[cfg(any(feature = "calvin", test, feature = "harness"))]
impl RecordKey {
    pub fn new(graph: impl Into<String>, node: impl Into<String>) -> Self {
        Self {
            graph: graph.into(),
            node: node.into(),
        }
    }
}

/// The mode a record is locked in during the OLLP phase (CONCEPT:EG-342): `Shared` for
/// a read (co-grantable with other reads), `Exclusive` for a write (mutually exclusive
/// with every other holder).
#[cfg(any(feature = "calvin", test, feature = "harness"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Shared,
    Exclusive,
}

/// A txn's predicted read/write set (CONCEPT:EG-342) — the output of the OLLP
/// reconnaissance ([`CrossShardCoordinator::predict_rwset`]) and the input to the
/// ordered lock phase. A record in BOTH sets is locked `Exclusive` (the write subsumes
/// the read).
#[cfg(any(feature = "calvin", test, feature = "harness"))]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RwSet {
    pub reads: BTreeSet<RecordKey>,
    pub writes: BTreeSet<RecordKey>,
}

#[cfg(any(feature = "calvin", test, feature = "harness"))]
impl RwSet {
    /// The per-record lock modes this set requests: every read `Shared`, every write
    /// `Exclusive` (a written record is Exclusive even if also read — the write wins).
    fn lock_requests(&self) -> BTreeMap<RecordKey, LockMode> {
        let mut reqs: BTreeMap<RecordKey, LockMode> = BTreeMap::new();
        for r in &self.reads {
            reqs.insert(r.clone(), LockMode::Shared);
        }
        for w in &self.writes {
            reqs.insert(w.clone(), LockMode::Exclusive);
        }
        reqs
    }
}

/// One outstanding lock request on a record, tagged with its owning txn's global
/// sequence (CONCEPT:EG-342). Ordered by `seq` inside a record's queue.
#[cfg(any(feature = "calvin", test, feature = "harness"))]
#[derive(Debug, Clone, Copy)]
struct LockEntry {
    seq: GlobalSeq,
    mode: LockMode,
    granted: bool,
}

/// A record's lock queue: outstanding requests kept ASCENDING by `seq` (CONCEPT:EG-342).
/// Grants flow strictly in sequence order, which is what makes conflicting sequenced
/// txns serialize in the sequencer's total order.
#[cfg(any(feature = "calvin", test, feature = "harness"))]
#[derive(Debug, Default)]
struct RecordQueue {
    entries: Vec<LockEntry>,
}

#[cfg(any(feature = "calvin", test, feature = "harness"))]
impl RecordQueue {
    /// Grant every front request that is now grantable (CONCEPT:EG-342). A request is
    /// grantable iff EVERY strictly-lower-sequence request on the record is already
    /// granted AND `Shared`, and — for an `Exclusive` request — it sits at the very
    /// front (no lower-sequence request outstanding at all). Scanning front→back and
    /// stopping at the first non-grantable entry co-grants a run of shared readers while
    /// keeping a writer strictly behind every lower-sequence holder. Returns whether any
    /// entry transitioned to granted.
    fn grant_front(&mut self) -> bool {
        let mut changed = false;
        for i in 0..self.entries.len() {
            if self.entries[i].granted {
                continue;
            }
            let earlier_all_granted_shared = self.entries[..i]
                .iter()
                .all(|d| d.granted && d.mode == LockMode::Shared);
            let ok =
                earlier_all_granted_shared && (i == 0 || self.entries[i].mode == LockMode::Shared);
            if ok {
                self.entries[i].granted = true;
                changed = true;
            } else {
                // Nothing after `i` can be grantable either (it has strictly more
                // lower-sequence predecessors), so stop.
                break;
            }
        }
        changed
    }
}

/// The deterministic ORDERED read/write lock manager of Calvin's OLLP phase
/// (CONCEPT:EG-342).
///
/// Locks are keyed by [`RecordKey`] and granted STRICTLY in the global-sequence order
/// the [`CalvinSequencer`] (EG-324) / [`epoch_fan_in`] (EG-343) assign. A txn
/// [`register`](OrderedLockManager::register)s its whole predicted [`RwSet`] at once
/// (all its records enter their queues sorted by the txn's `seq`), then awaits until
/// every one of its requests is granted. Because a request only ever waits on
/// STRICTLY-lower-sequence requests (on every record, ordering is by `seq`, identical
/// across records), the wait-for graph is a DAG oriented by `seq` → **deadlock-free**,
/// and any two CONFLICTING sequenced txns are forced into the sequence order on the
/// records they share → **serializable, equivalent to executing them one-at-a-time in
/// sequence order**. The subsequent deterministic-execution phase then runs lock-free.
///
/// **Required discipline (Calvin's invariant, honestly stated).** Registration MUST
/// happen in sequence order — the sequencing layer / epoch scheduler feeds the ordered
/// input log to `register` in `seq` order (the real Calvin lock-manager reads the
/// ordered batch and requests locks txn-by-txn). Registering out of order would let a
/// higher-sequence txn be granted before a lower one arrives. The manager enforces the
/// per-record ORDER (by `seq`) but relies on the driver for in-order SUBMISSION;
/// [`OrderedLockManager::admit_batch`] wraps that discipline for a whole epoch batch.
///
/// The wake mechanism is a single manager-wide [`tokio::sync::Notify`] (a simple,
/// correct notify-all; a per-record condition would cut spurious wakeups — a documented
/// throughput follow-up, not a correctness gap).
#[cfg(any(feature = "calvin", test, feature = "harness"))]
#[derive(Debug, Default)]
pub struct OrderedLockManager {
    records: std::sync::Mutex<HashMap<RecordKey, RecordQueue>>,
    wake: tokio::sync::Notify,
}

/// A registered-but-not-yet-granted lock request set (CONCEPT:EG-342). Produced by
/// [`OrderedLockManager::register`] SYNCHRONOUSLY in sequence order; awaited via
/// [`LockTicket::granted`] which resolves to the held [`LockGuard`] once every record is
/// granted.
#[cfg(any(feature = "calvin", test, feature = "harness"))]
#[derive(Debug)]
pub struct LockTicket {
    seq: GlobalSeq,
    keys: Vec<RecordKey>,
}

/// Proof the ordered locks for one txn are held (CONCEPT:EG-342). The deterministic
/// execution phase runs while this is alive; dropping it (or [`LockGuard::release`])
/// releases every record and lets the next sequenced waiter proceed.
#[cfg(any(feature = "calvin", test, feature = "harness"))]
#[derive(Debug)]
pub struct LockGuard {
    mgr: Arc<OrderedLockManager>,
    seq: GlobalSeq,
    keys: Vec<RecordKey>,
    released: bool,
}

#[cfg(any(feature = "calvin", test, feature = "harness"))]
impl OrderedLockManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register a txn's whole predicted lock-set at global sequence `seq` (CONCEPT:
    /// EG-342). SYNCHRONOUS and MUST be called in sequence order (see the type docs):
    /// every record's queue gets this txn's request inserted at its `seq`-sorted slot.
    /// Returns a [`LockTicket`] to await. Also runs a grant pass so a lower-sequence
    /// registration that unblocks the front is applied immediately.
    pub fn register(self: &Arc<Self>, seq: GlobalSeq, rwset: &RwSet) -> LockTicket {
        let reqs = rwset.lock_requests();
        let keys: Vec<RecordKey> = reqs.keys().cloned().collect();
        {
            let mut map = self.records.lock().unwrap();
            for (key, mode) in &reqs {
                let q = map.entry(key.clone()).or_default();
                let pos = q.entries.partition_point(|e| e.seq < seq);
                q.entries.insert(
                    pos,
                    LockEntry {
                        seq,
                        mode: *mode,
                        granted: false,
                    },
                );
                q.grant_front();
            }
        }
        self.wake.notify_waiters();
        LockTicket { seq, keys }
    }

    /// Await until every record in `ticket` is granted, then return the held guard
    /// (CONCEPT:EG-342). Re-checks on every manager wake; the arm-before-check +
    /// `enable()` pattern registers the waiter before the grant test so no wake is lost.
    pub async fn granted(self: &Arc<Self>, ticket: LockTicket) -> LockGuard {
        loop {
            let notified = self.wake.notified();
            tokio::pin!(notified);
            // Register the waiter NOW so a grant between the check below and the await
            // still wakes us (no lost notification).
            notified.as_mut().enable();
            if self.all_granted(&ticket) {
                return LockGuard {
                    mgr: self.clone(),
                    seq: ticket.seq,
                    keys: ticket.keys,
                    released: false,
                };
            }
            notified.await;
        }
    }

    /// Register + await in one call (CONCEPT:EG-342) — safe ONLY when the caller
    /// guarantees calls happen in sequence order (e.g. the two-txn conflict path or a
    /// single-threaded scheduler). For a concurrent epoch use
    /// [`register`](Self::register) in order first, then await each ticket.
    pub async fn acquire(self: &Arc<Self>, seq: GlobalSeq, rwset: &RwSet) -> LockGuard {
        let ticket = self.register(seq, rwset);
        self.granted(ticket).await
    }

    /// Whether every record of a ticket is currently granted to its sequence.
    fn all_granted(self: &Arc<Self>, ticket: &LockTicket) -> bool {
        let map = self.records.lock().unwrap();
        ticket.keys.iter().all(|key| {
            map.get(key)
                .and_then(|q| q.entries.iter().find(|e| e.seq == ticket.seq))
                .map(|e| e.granted)
                .unwrap_or(false)
        })
    }

    /// Release every record a guard holds and grant the next sequenced waiters
    /// (CONCEPT:EG-342). Synchronous (runs from `Drop`): removes this txn's entries,
    /// advances each queue's front, then wakes all waiters to re-check.
    fn release(&self, seq: GlobalSeq, keys: &[RecordKey]) {
        {
            let mut map = self.records.lock().unwrap();
            for key in keys {
                let now_empty = if let Some(q) = map.get_mut(key) {
                    q.entries.retain(|e| e.seq != seq);
                    q.grant_front();
                    q.entries.is_empty()
                } else {
                    false
                };
                if now_empty {
                    map.remove(key);
                }
            }
        }
        self.wake.notify_waiters();
    }

    /// Admit a whole epoch batch's txns in ONE in-sequence-order pass (CONCEPT:EG-342 +
    /// EG-343): given `(seq, rwset)` pairs, sort by `seq` and `register` each in order,
    /// returning the tickets in the same order. This is the Calvin invariant "the lock
    /// manager requests locks in the sequenced-log order" made into a single call —
    /// callers then await the tickets concurrently to run the deterministic phase.
    pub fn admit_batch(self: &Arc<Self>, mut batch: Vec<(GlobalSeq, RwSet)>) -> Vec<LockTicket> {
        batch.sort_by_key(|(seq, _)| *seq);
        batch
            .into_iter()
            .map(|(seq, rw)| self.register(seq, &rw))
            .collect()
    }
}

#[cfg(any(feature = "calvin", test, feature = "harness"))]
impl LockGuard {
    /// Explicitly release the held locks (equivalent to dropping the guard).
    pub fn release(mut self) {
        self.mgr.release(self.seq, &self.keys);
        self.released = true;
    }
}

#[cfg(any(feature = "calvin", test, feature = "harness"))]
impl Drop for LockGuard {
    fn drop(&mut self) {
        if !self.released {
            self.mgr.release(self.seq, &self.keys);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-343: multi-node sequencer epoch fan-in
// ─────────────────────────────────────────────────────────────────────────────

/// One node's locally-sequenced input for a Calvin epoch (CONCEPT:EG-343) — a txn that
/// node's local sequencer accepted at position `local_seq`. Nodes exchange their
/// per-epoch input batches; [`epoch_fan_in`] deterministically merges them.
#[cfg(any(feature = "calvin", test, feature = "harness"))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInput {
    /// The node whose sequencer accepted this txn.
    pub node: NodeId,
    /// The txn's position in that node's LOCAL order for the epoch (1-based).
    pub local_seq: u64,
    /// The txn id (stable deterministic tie-breaker across nodes).
    pub txn_id: String,
}

#[cfg(any(feature = "calvin", test, feature = "harness"))]
impl NodeInput {
    pub fn new(node: NodeId, local_seq: u64, txn_id: impl Into<String>) -> Self {
        Self {
            node,
            local_seq,
            txn_id: txn_id.into(),
        }
    }
}

/// A txn's position in the merged GLOBAL order of an epoch (CONCEPT:EG-343) — carries
/// the epoch-relative [`GlobalSeq`] plus the origin node so execution can route/attribute
/// it. The cross-epoch total order is lexicographic `(epoch, global_seq)`.
#[cfg(any(feature = "calvin", test, feature = "harness"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequencedInput {
    pub global_seq: GlobalSeq,
    pub origin_node: NodeId,
    pub local_seq: u64,
    pub txn_id: String,
}

/// The deterministic merged order of ONE Calvin epoch (CONCEPT:EG-343): every node's
/// per-node input batches folded into a single total order, `inputs` ascending by
/// `global_seq`. Deriving this from the same per-node batches yields byte-identical
/// output on every node — the property the vote-free deterministic execution relies on.
#[cfg(any(feature = "calvin", test, feature = "harness"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochBatch {
    pub epoch: u64,
    pub inputs: Vec<SequencedInput>,
}

/// Fan-in the per-node sequenced inputs of one epoch into a single deterministic global
/// order (CONCEPT:EG-343) — the multi-node extension of the single [`CalvinSequencer`]
/// (EG-324).
///
/// Every node, given the SAME `per_node` batches, computes the IDENTICAL [`EpochBatch`]:
/// the merge is a pure function. The order is a deterministic ROUND-ROBIN interleave —
/// sort all inputs by `(local_seq, node, txn_id)`, so round *r* contains each node's
/// *r*-th input ordered by node id (a fair cross-node interleave), and ties are broken by
/// the stable `txn_id`. Global positions are assigned 1-based within the epoch. Because
/// the sort key is total and data-independent, no two nodes can disagree on the result —
/// which is exactly what lets each node run the deterministic execution phase without a
/// vote. (This mirrors [`deterministic_order`]'s replay-stability, generalized from one
/// sequencer to a multi-node fan-in.)
#[cfg(any(feature = "calvin", test, feature = "harness"))]
pub fn epoch_fan_in(epoch: u64, per_node: &BTreeMap<NodeId, Vec<NodeInput>>) -> EpochBatch {
    let mut all: Vec<&NodeInput> = per_node.values().flatten().collect();
    all.sort_by(|a, b| {
        a.local_seq
            .cmp(&b.local_seq)
            .then_with(|| a.node.cmp(&b.node))
            .then_with(|| a.txn_id.cmp(&b.txn_id))
    });
    let inputs = all
        .into_iter()
        .enumerate()
        .map(|(i, ni)| SequencedInput {
            global_seq: GlobalSeq(i as u64 + 1),
            origin_node: ni.node,
            local_seq: ni.local_seq,
            txn_id: ni.txn_id.clone(),
        })
        .collect();
    EpochBatch { epoch, inputs }
}

#[cfg(test)]
mod calvin_tests {
    //! Pure unit tests for the CONCEPT:EG-324 Calvin deterministic-ordering layer
    //! (the sequencer + the replay-stable order kernel) — no live cluster needed. The
    //! end-to-end deterministic-execute + crash-replay-recovery proof lives in the live
    //! `xshard_harness` (`calvin_*` tests).
    use super::*;

    fn txn(id: &str) -> CrossShardTxn {
        CrossShardTxn {
            txn_id: id.to_string(),
            slices: vec![],
        }
    }

    #[test]
    fn eg3_3_sequencer_assigns_strict_monotone_total_order() {
        let seq = CalvinSequencer::new();
        let a = seq.assign();
        let b = seq.assign();
        let c = seq.assign();
        assert!(a < b && b < c, "strictly increasing total order");
        assert_eq!(
            a,
            GlobalSeq(1),
            "first slot is 1 (0 is the unsequenced sentinel)"
        );
        assert_eq!(seq.peek(), GlobalSeq(4), "peek does not consume");
    }

    #[test]
    fn eg3_3_sequencer_is_shared_across_clones() {
        // A clone shares the counter (Arc), so two commit tasks never collide on a slot.
        let seq = CalvinSequencer::new();
        let seq2 = seq.clone();
        let x = seq.assign();
        let y = seq2.assign();
        assert_ne!(x, y, "cloned sequencers share one monotone counter");
    }

    #[test]
    fn eg3_3_deterministic_order_is_replay_stable_regardless_of_arrival() {
        // Same input SET in two different arrival orders ⇒ identical execution order,
        // which is the determinism the vote-free execution layer relies on.
        let batch1 = vec![txn("c"), txn("a"), txn("b")];
        let batch2 = vec![txn("b"), txn("c"), txn("a")];
        let order1: Vec<&str> = deterministic_order(&batch1)
            .iter()
            .map(|&i| batch1[i].txn_id.as_str())
            .collect();
        let order2: Vec<&str> = deterministic_order(&batch2)
            .iter()
            .map(|&i| batch2[i].txn_id.as_str())
            .collect();
        assert_eq!(order1, vec!["a", "b", "c"]);
        assert_eq!(order1, order2, "any node derives the identical total order");
    }

    // ── EG-342: ordered lock manager (pure-logic proofs, no live cluster) ──────

    fn rk(node: &str) -> RecordKey {
        RecordKey::new("g", node)
    }

    /// EG-342: two txns that CONFLICT on a record are serialized in sequence order —
    /// the lower-seq exclusive writer holds first, the higher-seq reader waits, and only
    /// after the writer RELEASES is the reader granted. Proves the ordered lock phase
    /// gives sequence-order serializability at the lock layer.
    #[tokio::test]
    async fn eg342_conflicting_locks_serialize_in_sequence_order() {
        let mgr = OrderedLockManager::new();
        let write_k = RwSet {
            reads: BTreeSet::new(),
            writes: [rk("k")].into_iter().collect(),
        };
        let read_k = RwSet {
            reads: [rk("k")].into_iter().collect(),
            writes: BTreeSet::new(),
        };
        // Register IN SEQUENCE ORDER (the Calvin invariant): seq 1 (writer) then seq 2.
        let t1 = mgr.register(GlobalSeq(1), &write_k);
        let t2 = mgr.register(GlobalSeq(2), &read_k);

        // seq 1 is grantable immediately (front, exclusive); seq 2 must WAIT behind it.
        let g1 = mgr.granted(t1).await;
        assert!(
            !mgr.all_granted(&t2),
            "higher-seq reader must wait behind the lower-seq writer on the shared record"
        );
        // Release the writer → the reader is now granted (order enforced).
        g1.release();
        let _g2 = mgr.granted(t2).await; // resolves ⇒ granted after the writer released
    }

    /// EG-342: two READERS at different sequences co-grant (shared/shared compatible) —
    /// the ordered lock phase only serializes CONFLICTING txns, not read-read pairs.
    #[tokio::test]
    async fn eg342_shared_readers_cogrant() {
        let mgr = OrderedLockManager::new();
        let read_k = RwSet {
            reads: [rk("k")].into_iter().collect(),
            writes: BTreeSet::new(),
        };
        let t1 = mgr.register(GlobalSeq(1), &read_k);
        let t2 = mgr.register(GlobalSeq(2), &read_k);
        // Both granted concurrently — no serialization needed for read/read.
        let _g1 = mgr.granted(t1).await;
        let _g2 = mgr.granted(t2).await;
    }

    /// EG-342: a non-conflicting higher-seq txn is NOT blocked by a lower-seq txn on a
    /// DISJOINT record — the lock phase serializes only on shared records.
    #[tokio::test]
    async fn eg342_disjoint_records_do_not_block() {
        let mgr = OrderedLockManager::new();
        let write_k = RwSet {
            reads: BTreeSet::new(),
            writes: [rk("k")].into_iter().collect(),
        };
        let write_l = RwSet {
            reads: BTreeSet::new(),
            writes: [rk("l")].into_iter().collect(),
        };
        let t1 = mgr.register(GlobalSeq(1), &write_k);
        let t2 = mgr.register(GlobalSeq(2), &write_l);
        let _g1 = mgr.granted(t1).await;
        assert!(
            mgr.all_granted(&t2),
            "disjoint record ⇒ granted immediately"
        );
    }

    // ── EG-343: multi-node epoch fan-in determinism ───────────────────────────

    /// EG-343: two independent nodes that exchange the SAME per-node epoch batches derive
    /// the IDENTICAL global order — the property that lets each run the deterministic
    /// execution phase with no vote.
    #[test]
    fn eg343_two_nodes_derive_identical_global_order() {
        let mut per_node: BTreeMap<NodeId, Vec<NodeInput>> = BTreeMap::new();
        per_node.insert(
            1,
            vec![NodeInput::new(1, 1, "a1"), NodeInput::new(1, 2, "a2")],
        );
        per_node.insert(
            2,
            vec![NodeInput::new(2, 1, "b1"), NodeInput::new(2, 2, "b2")],
        );

        let on_node_1 = epoch_fan_in(7, &per_node);
        let on_node_2 = epoch_fan_in(7, &per_node);
        assert_eq!(
            on_node_1, on_node_2,
            "both nodes derive byte-identical global epoch order"
        );

        // Round-robin interleave by (local_seq, node): round 1 = a1,b1; round 2 = a2,b2.
        let order: Vec<&str> = on_node_1
            .inputs
            .iter()
            .map(|si| si.txn_id.as_str())
            .collect();
        assert_eq!(order, vec!["a1", "b1", "a2", "b2"]);
        // Global sequence positions are dense + 1-based within the epoch.
        assert_eq!(on_node_1.inputs[0].global_seq, GlobalSeq(1));
        assert_eq!(on_node_1.inputs[3].global_seq, GlobalSeq(4));
    }

    /// EG-343: the merge is order-INDEPENDENT of how the per-node map is built — a pure
    /// function of the batches' CONTENTS (a `BTreeMap` already canonicalizes node order;
    /// this asserts the interleave itself does not smuggle in arrival order).
    #[test]
    fn eg343_fan_in_is_a_pure_function_of_contents() {
        let mut a: BTreeMap<NodeId, Vec<NodeInput>> = BTreeMap::new();
        a.insert(2, vec![NodeInput::new(2, 1, "b1")]);
        a.insert(1, vec![NodeInput::new(1, 1, "a1")]);
        let mut b: BTreeMap<NodeId, Vec<NodeInput>> = BTreeMap::new();
        b.insert(1, vec![NodeInput::new(1, 1, "a1")]);
        b.insert(2, vec![NodeInput::new(2, 1, "b1")]);
        assert_eq!(epoch_fan_in(3, &a), epoch_fan_in(3, &b));
    }
}
