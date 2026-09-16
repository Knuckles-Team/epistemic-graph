//! Helpers for [`super::CrossShardCoordinator::commit_cross_shard_inner`] and
//! [`super::CrossShardCoordinator::validate_slices`], split out of `mod.rs`
//! (CCCC burn-down lane L-raft-a, D-CX-cross-shard-txn-split).
//!
//! `cross_shard_txn/mod.rs` was already well over the KISS whole-file
//! `lines_per_file`/`functions_per_file`/`methods_per_class` thresholds before
//! this split (pre-existing debt, out of this lane's scope); the
//! `kiss-changed-rust` gate only fails a commit that WORSENS an
//! already-crossed whole-file count relative to `HEAD`. Fixing
//! `commit_cross_shard_inner`'s and `validate_slices`'s cognitive complexity
//! requires new named helpers, and adding them as further items/methods in
//! `mod.rs` would have worsened those counts. Putting the new helpers in this
//! sibling submodule instead keeps `mod.rs`'s own counts flat-or-improved
//! (code moved OUT, not added) while this file starts fresh, well under every
//! threshold. `CrossShardCoordinator`'s two methods here are `pub(super)`:
//! implementation detail visible only to `commit_cross_shard_inner` in the
//! parent module, not part of the crate's public surface.

use std::collections::BTreeMap;

use super::{CrossShardCoordinator, GraphSlice, GroupId, Method, RedbBackend, TxnOutcome};
use crate::graph::GraphCore;

impl CrossShardCoordinator {
    /// Validate every READ-ONLY participant's reads; on the first failure, record
    /// the (optional) recoverable decision as ABORT. `Ok(None)` means every
    /// read-only participant validated and the caller should continue into the
    /// writing-participant protocol; `Ok(Some(outcome))` is the terminal outcome
    /// the caller must return without doing anything else.
    pub(super) async fn abort_on_invalid_read_only(
        &self,
        redb: &RedbBackend,
        txn_id: &str,
        read_only: &BTreeMap<GroupId, Vec<GraphSlice>>,
        retain_decision: bool,
    ) -> Result<Option<TxnOutcome>, String> {
        for (gid, slices) in read_only {
            if !self.validate_read_only_participant(*gid, slices).await? {
                if retain_decision {
                    redb.xshard_recoverable_decision_put(txn_id, false).await?;
                }
                return Ok(Some(TxnOutcome::Aborted));
            }
        }
        Ok(None)
    }

    /// PHASE 1: PREPARE every WRITING participant CONCURRENTLY (EG-081) and tally
    /// the votes. Returns `(all_yes, prepared_groups)`.
    ///
    /// Deadlock-freedom under parallel prepare: a prepare holds NO lock ACROSS
    /// groups. Each `prepare_participant` takes its group's shared app_state READ
    /// guard only for the span of its own OCC validation (released before its
    /// durable write), and the redb writer serializes the prepare-log commits
    /// internally. No prepare ever waits on a lock another prepare holds, so there
    /// is no cross-group lock cycle regardless of the order prepares arrive in —
    /// the former strict GroupId sequencing was a defensive lock-order that the
    /// actual (across-groups lock-free) prepare does not require. The per-group
    /// local lock order inside each group is unchanged. We therefore issue the
    /// independent prepare RPCs/durable-writes as joined futures instead of one at
    /// a time; the joined set preserves input (GroupId) order so the collected
    /// votes are deterministic.
    pub(super) async fn run_prepare_phase(
        &self,
        redb: &RedbBackend,
        txn_id: &str,
        writing: &BTreeMap<GroupId, Vec<GraphSlice>>,
    ) -> (bool, Vec<GroupId>) {
        let prepare_futs = writing.iter().map(|(gid, slices)| {
            let gid = *gid;
            async move {
                (
                    gid,
                    self.prepare_participant(redb, txn_id, gid, slices).await,
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
                        txn_id,
                        gid
                    );
                    all_yes = false;
                }
            }
        }
        (all_yes, prepared_groups)
    }
}

/// Every `AddEdge` method in `slice` needs both endpoints present at commit —
/// either already live in `core`, or inserted by some slice in the same
/// prepared set (`slices`). If a concurrent writer removed one, prepare must
/// fail (vote NO). See [`super::CrossShardCoordinator::validate_slices`].
pub(super) fn slice_edge_endpoints_are_live(
    core: &GraphCore,
    slice: &GraphSlice,
    slices: &[GraphSlice],
) -> bool {
    for m in &slice.methods {
        let Method::AddEdge {
            source_id,
            target_id,
            ..
        } = m
        else {
            continue;
        };
        if core.get_node_properties(source_id).is_none() && !slice_inserts_node(slices, source_id) {
            return false;
        }
        if core.get_node_properties(target_id).is_none() && !slice_inserts_node(slices, target_id) {
            return false;
        }
    }
    true
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
