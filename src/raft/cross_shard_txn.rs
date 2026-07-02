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
use std::sync::atomic::{AtomicU64, Ordering};
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
}

/// The dedicated graph that holds Raft-replicated cross-shard commit decisions
/// (CONCEPT:EG-082). One node per txn — id = `txn_id`, property `xshard_commit: bool`.
/// Lives in the decision Raft group's replicated state machine, so every replica can
/// read a txn's outcome without the coordinator.
#[cfg(any(feature = "nonblocking", test, feature = "harness"))]
pub const XSHARD_DECISION_GRAPH: &str = "__xshard_decisions__";

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
