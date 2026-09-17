//! Two-part commit of one placement plan: the replicated epoch reservation,
//! then the placement row methods.
//!
//! Split out of `multi.rs` (CCCC burn-down lane L-raft-b). `multi.rs` was
//! already over the KISS whole-file thresholds, so
//! `commit_placement_plan` and the one commit step its four copies shared live
//! here and the parent's own counts only go down.

use super::{placement, MultiRaft, PlacementCommitOutcome};
use crate::protocol::{Method, ResultPayload};

impl MultiRaft {
    /// Complete a [`placement::PendingWrite`] in two fenced parts: first reserve
    /// an epoch through the replicated counter CAS, then apply the placement row
    /// CAS/add/remove methods. CAS outcomes come from the exact state-machine
    /// apply result; the plan child identity prevents distinct identical plans
    /// from sharing a durable replay record.
    pub(super) async fn commit_placement_plan(
        &self,
        plan: &placement::PendingWrite<'_>,
    ) -> Result<PlacementCommitOutcome, String> {
        let mut ordinal = 0u64;
        if let Some(allocation) = plan.epoch_allocation {
            if !reserve_plan_epoch(self, &plan.operation_id, allocation, &mut ordinal).await? {
                return Ok(PlacementCommitOutcome::EpochConflict);
            }
        }
        for method in &plan.methods {
            let accepted = commit_plan_step(self, &plan.operation_id, &mut ordinal, method).await?;
            if plan.require_success && !accepted {
                return Err("placement row CAS fence rejected the stale proposal".to_string());
            }
        }
        Ok(PlacementCommitOutcome::Committed)
    }
}

/// Commit one plan child at `ordinal` (advancing it), surface a native apply
/// error, and report whether the state machine answered `true`.
async fn commit_plan_step(
    multi: &MultiRaft,
    operation_id: &str,
    ordinal: &mut u64,
    method: &Method,
) -> Result<bool, String> {
    let response = multi
        .commit_placement_plan_method(operation_id, *ordinal, method)
        .await?;
    *ordinal = ordinal.saturating_add(1);
    if let Some(error) = response.native_error {
        return Err(error);
    }
    Ok(matches!(
        response.native_result,
        Some(ResultPayload::Bool(true))
    ))
}

/// Seed the durable counter when absent, reconcile it up to the legacy floor
/// when it lags, then CAS it to the allocated epoch. `Ok(false)` is a lost
/// race: the caller must re-plan from the durable catalog.
async fn reserve_plan_epoch(
    multi: &MultiRaft,
    operation_id: &str,
    allocation: placement::EpochAllocation,
    ordinal: &mut u64,
) -> Result<bool, String> {
    let mut steps = Vec::with_capacity(3);
    if allocation.seed_if_absent {
        steps.push(placement::PlacementCatalog::epoch_seed_method(
            allocation.floor,
        ));
    }
    if allocation.expected != allocation.floor {
        steps.push(placement::PlacementCatalog::epoch_reconcile_method(
            allocation.expected,
            allocation.floor,
        ));
    }
    steps.push(placement::PlacementCatalog::epoch_cas_method(
        allocation.floor,
        allocation.allocated,
    ));
    for method in &steps {
        if !commit_plan_step(multi, operation_id, ordinal, method).await? {
            return Ok(false);
        }
    }
    Ok(true)
}
