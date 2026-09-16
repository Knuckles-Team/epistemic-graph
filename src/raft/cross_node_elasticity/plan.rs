//! Per-placement admission for [`super::CrossNodeElasticityPlanner::plan`]
//! (CCCC burn-down lane L-raft-a, D-CX-cross-node-elasticity-split).
//!
//! `cross_node_elasticity/mod.rs` was already over KISS's whole-file
//! `lines_per_file` threshold before this change (pre-existing debt, out of
//! this lane's scope); decomposing `plan`'s cognitive complexity needs new
//! named helpers, and adding them as further items/methods in `mod.rs` would
//! have both worsened that pre-existing count AND newly crossed
//! `methods_per_class` on `CrossNodeElasticityPlanner` (8 methods at HEAD,
//! comfortably under the 13 cap; +7 new ones would have made it 15). Putting
//! the new helpers in this sibling submodule instead keeps `mod.rs`'s counts
//! at or below HEAD's (code moved OUT, not added) while this file starts
//! fresh. `MoveBudget` and the two entry points `plan()` calls directly
//! (`continue_in_flight_move`, `plan_new_move`) are `pub(super)`:
//! implementation detail visible only to `plan()` in the parent module, not
//! part of the crate's public surface.

use super::*;

/// Running per-node load/spend accumulated by proposals already emitted in this
/// planning pass. Every planned move nudges the additions/removals maps so the
/// next placement in the same pass is evaluated against a self-consistent
/// projection, not just the point-in-time observed load.
#[derive(Default)]
pub(super) struct MoveBudget {
    planned_additions: std::collections::BTreeMap<NodeId, ResourceVector>,
    planned_removals: std::collections::BTreeMap<NodeId, ResourceVector>,
    network_budget_used: u64,
    total_budget_used: u64,
}

impl CrossNodeElasticityPlanner {
    /// Resume/re-evaluate a placement that already has an in-flight checkpoint:
    /// re-validate the checkpoint against the current topology, then run the same
    /// capacity/SLO/budget admission a fresh move would (see
    /// [`Self::accept_in_flight_move`]).
    pub(super) fn continue_in_flight_move(
        input: &PlannerInput,
        placement: &ShardPlacement,
        checkpoint: &MoveCheckpoint,
        budget: &mut MoveBudget,
        proposals_so_far: usize,
    ) -> Result<MoveProposal, PlanAbortReason> {
        let target = Self::resolve_in_flight_target(input, placement, checkpoint)?;
        let (source_load, projected_target, cost) = Self::accept_in_flight_move(
            input,
            placement,
            checkpoint,
            target,
            budget,
            proposals_so_far,
        )?;
        let proposal = Self::proposal(
            input,
            placement,
            checkpoint.kind,
            checkpoint.clone(),
            source_load,
            projected_target,
            cost,
        );
        budget.network_budget_used = budget
            .network_budget_used
            .saturating_add(cost.network_bytes);
        budget.total_budget_used = budget.total_budget_used.saturating_add(cost.budget_units);
        Self::record_load_delta(
            &mut budget.planned_additions,
            &mut budget.planned_removals,
            checkpoint.source_node,
            checkpoint.target_node,
            placement.load,
            checkpoint.kind,
        );
        Ok(proposal)
    }

    /// The checkpoint/topology gates that must hold before an in-flight move is
    /// even considered for capacity admission: the checkpoint itself is valid, it
    /// still matches this placement's current epoch/source, it targets a node
    /// other than the (still current) primary, and that target is a resolvable,
    /// eligible node.
    fn resolve_in_flight_target<'a>(
        input: &'a PlannerInput,
        placement: &ShardPlacement,
        checkpoint: &MoveCheckpoint,
    ) -> Result<&'a NodeCapacity, PlanAbortReason> {
        if !checkpoint.validate() {
            return Err(PlanAbortReason::InvalidCheckpoint);
        }
        if checkpoint.placement_epoch != placement.placement_epoch
            || checkpoint.source_node != placement.primary_node
        {
            return Err(PlanAbortReason::StaleTopology);
        }
        if checkpoint.target_node == placement.primary_node {
            return Err(PlanAbortReason::DuplicateInFlight);
        }
        let target = input
            .nodes
            .iter()
            .find(|node| node.node_id == checkpoint.target_node)
            .ok_or(PlanAbortReason::NoSafeTarget)?;
        if target.availability != NodeAvailability::Eligible {
            return Err(PlanAbortReason::NoSafeTarget);
        }
        Ok(target)
    }

    /// Proposal-limit, capacity, SLO, and budget admission for an already
    /// topology-valid in-flight move (see [`Self::resolve_in_flight_target`]).
    fn accept_in_flight_move(
        input: &PlannerInput,
        placement: &ShardPlacement,
        checkpoint: &MoveCheckpoint,
        target: &NodeCapacity,
        budget: &MoveBudget,
        proposals_so_far: usize,
    ) -> Result<(ResourceVector, ResourceVector, MovementCost), PlanAbortReason> {
        if proposals_so_far >= input.policy.max_proposals {
            return Err(PlanAbortReason::ProposalLimit);
        }
        let (source_load, target_load) = Self::projected_loads(
            input,
            placement,
            checkpoint.target_node,
            &budget.planned_additions,
            &budget.planned_removals,
        )
        .ok_or(PlanAbortReason::SourceLoadMismatch)?;
        let projected_target = target_load.saturating_add(placement.load);
        let cost = MovementCost::estimate(
            placement,
            checkpoint.kind,
            target.network_bytes_per_sec,
            input.policy.delta_window_seconds,
        );
        if !projected_target.fits_in(target.limits) {
            return Err(PlanAbortReason::InsufficientCapacity);
        }
        if Self::violates_slo(projected_target, &input.policy) {
            return Err(PlanAbortReason::SloRisk);
        }
        if !Self::within_budget(
            cost,
            budget.network_budget_used,
            budget.total_budget_used,
            &input.policy,
        ) {
            return Err(PlanAbortReason::BudgetExceeded);
        }
        Ok((source_load, projected_target, cost))
    }

    /// Plan a brand-new move for a placement with no in-flight checkpoint:
    /// classify what kind of move (if any) its current state permits, then admit
    /// it through the same cooldown/proposal-limit/target-selection gates.
    pub(super) fn plan_new_move(
        input: &PlannerInput,
        placement: &ShardPlacement,
        trigger_bp: u64,
        budget: &mut MoveBudget,
        proposals_so_far: usize,
    ) -> Result<MoveProposal, PlanAbortReason> {
        let (source, source_load) = Self::resolve_new_move_source(input, placement, budget)?;
        let source_pressure = source_load.pressure_bp(source.limits);
        let kind = Self::classify_move_kind(input, placement, source, source_pressure, trigger_bp)?;
        let (target, projected_target, cost) =
            Self::admit_new_move(input, placement, kind, budget, proposals_so_far)?;
        let checkpoint = MoveCheckpoint::new(
            placement,
            placement.primary_node,
            target.node_id,
            kind,
            if kind == MoveKind::Hydrate {
                MovePhase::Hydrate
            } else {
                MovePhase::Snapshot
            },
            input.now_tick,
        );
        let proposal = Self::proposal(
            input,
            placement,
            kind,
            checkpoint,
            source_load,
            projected_target,
            cost,
        );
        budget.network_budget_used = budget
            .network_budget_used
            .saturating_add(cost.network_bytes);
        budget.total_budget_used = budget.total_budget_used.saturating_add(cost.budget_units);
        Self::record_load_delta(
            &mut budget.planned_additions,
            &mut budget.planned_removals,
            placement.primary_node,
            target.node_id,
            placement.load,
            kind,
        );
        Ok(proposal)
    }

    fn resolve_new_move_source<'a>(
        input: &'a PlannerInput,
        placement: &ShardPlacement,
        budget: &MoveBudget,
    ) -> Result<(&'a NodeCapacity, ResourceVector), PlanAbortReason> {
        let source = input
            .nodes
            .iter()
            .find(|node| node.node_id == placement.primary_node)
            .ok_or(PlanAbortReason::NoSafeTarget)?;
        let (source_load, _) = Self::projected_loads(
            input,
            placement,
            placement.primary_node,
            &budget.planned_additions,
            &budget.planned_removals,
        )
        .ok_or(PlanAbortReason::SourceLoadMismatch)?;
        Ok((source, source_load))
    }

    /// Decide what kind of move (if any) `placement`'s CURRENT state permits.
    /// This is an exhaustive match over [`PlacementState`] — every variant is
    /// named, so adding a state is a compile error at this dispatch site; it is
    /// deliberately never turned into a lookup table (see the module's terms of
    /// acceptance for cyclomatic-vs-cognitive complexity on exhaustive Rust
    /// matches).
    fn classify_move_kind(
        input: &PlannerInput,
        placement: &ShardPlacement,
        source: &NodeCapacity,
        source_pressure: u64,
        trigger_bp: u64,
    ) -> Result<MoveKind, PlanAbortReason> {
        match placement.state {
            PlacementState::Cold => {
                if !input.policy.allow_hydration
                    || placement.load.read_ops_per_sec < input.policy.hydrate_read_ops_per_sec
                {
                    return Err(PlanAbortReason::HysteresisNotCrossed {
                        pressure_bp: source_pressure,
                        trigger_bp,
                    });
                }
                Ok(MoveKind::Hydrate)
            }
            PlacementState::Hydrating
            | PlacementState::Snapshotting
            | PlacementState::DeltaCatchUp
            | PlacementState::FencedCutover => Err(PlanAbortReason::DuplicateInFlight),
            PlacementState::Quarantined => Err(PlanAbortReason::NoSafeTarget),
            PlacementState::Resident | PlacementState::Follower | PlacementState::Draining => {
                if source.availability != NodeAvailability::Draining && source_pressure < trigger_bp
                {
                    return Err(PlanAbortReason::HysteresisNotCrossed {
                        pressure_bp: source_pressure,
                        trigger_bp,
                    });
                }
                Ok(MoveKind::MovePrimary)
            }
        }
    }

    /// Cooldown + proposal-limit + target-selection admission shared by every new
    /// move, regardless of `kind`.
    fn admit_new_move<'a>(
        input: &'a PlannerInput,
        placement: &ShardPlacement,
        kind: MoveKind,
        budget: &MoveBudget,
        proposals_so_far: usize,
    ) -> Result<(&'a NodeCapacity, ResourceVector, MovementCost), PlanAbortReason> {
        if input.now_tick
            < placement
                .last_transition_tick
                .saturating_add(input.policy.cooldown_ticks)
        {
            return Err(PlanAbortReason::CooldownActive {
                until_tick: placement
                    .last_transition_tick
                    .saturating_add(input.policy.cooldown_ticks),
            });
        }
        if proposals_so_far >= input.policy.max_proposals {
            return Err(PlanAbortReason::ProposalLimit);
        }
        Self::select_target(
            input,
            placement,
            kind,
            &budget.planned_additions,
            &budget.planned_removals,
            budget.network_budget_used,
            budget.total_budget_used,
        )
    }
}
