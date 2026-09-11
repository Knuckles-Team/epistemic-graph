use super::super::DurableCrypto;
use super::identity::{hold_encode, hold_load, lane_intent_matches_hold};
use super::lifecycle_shared::{
    current_policy, hold_correlations_match, terminal_source_correlations_match,
};
use super::links::lane_intent_value;
use super::quota::{
    apply_counter_delta, branch_key, hold_charge, load_global_policy, load_policy,
    load_scope_counters,
};
use super::reserve_support::{branch_index_id, work_item_index_id, work_item_kind_name};
use super::results::{finish_result, get_index, get_lane_index};
use super::types::LaneDecision;
use super::work_item::load_work_item;
use super::DurableLaneHold;
use crate::epistemic_operations::{
    DevelopmentLaneFinishRequest, DevelopmentLaneFinishRequestTerminalState, DevelopmentLaneHold,
    DevelopmentLaneHoldState,
};
use crate::protocol::DevelopmentLaneWorkItemKind;
use eg_storage::ScopedOwnerTableMut;

pub(super) fn finish_state_matches(
    terminal_state: DevelopmentLaneFinishRequestTerminalState,
    status: &str,
) -> bool {
    matches!(
        (terminal_state, status),
        (
            DevelopmentLaneFinishRequestTerminalState::Succeeded,
            "succeeded"
        ) | (DevelopmentLaneFinishRequestTerminalState::Failed, "failed")
            | (
                DevelopmentLaneFinishRequestTerminalState::Cancelled,
                "cancelled"
            )
            | (
                DevelopmentLaneFinishRequestTerminalState::DeadLetter,
                "dead_letter"
            )
    )
}

pub(super) fn finish_state_name(state: DevelopmentLaneFinishRequestTerminalState) -> &'static str {
    match state {
        DevelopmentLaneFinishRequestTerminalState::Succeeded => "succeeded",
        DevelopmentLaneFinishRequestTerminalState::Failed => "failed",
        DevelopmentLaneFinishRequestTerminalState::Cancelled => "cancelled",
        DevelopmentLaneFinishRequestTerminalState::DeadLetter => "dead_letter",
    }
}

/// A live hold must be tombstone-free and in one of the pre-terminal states.
pub(super) fn hold_not_live(hold: &DevelopmentLaneHold) -> bool {
    hold.tombstone
        || !matches!(
            hold.state,
            DevelopmentLaneHoldState::Allocating
                | DevelopmentLaneHoldState::Active
                | DevelopmentLaneHoldState::Submitted
        )
}

/// Is the WorkItem pre-image not a leased/running lifecycle row for this
/// hold's tenant?
pub(super) fn work_item_identity_mismatch(
    pre_props: &serde_json::Map<String, serde_json::Value>,
    hold: &DevelopmentLaneHold,
) -> bool {
    super::super::property_string(pre_props, "node_type") != "WorkItem"
        || super::super::property_string(pre_props, "kind")
            != work_item_kind_name(DevelopmentLaneWorkItemKind::Lifecycle)
        || super::super::property_string(pre_props, "tenant") != hold.tenant_ref
        || !matches!(
            super::super::property_string(pre_props, "status"),
            "leased" | "running"
        )
}

/// Does the WorkItem pre-image carry a different identity/lease/fence tuple
/// than the hold it is linked to?
pub(super) fn work_item_fence_mismatch(
    pre_props: &serde_json::Map<String, serde_json::Value>,
    hold: &DevelopmentLaneHold,
    work_item_id: &str,
    attempt: u64,
) -> bool {
    hold.work_item_id != work_item_id
        || hold.attempt != attempt
        || hold.lease_epoch != super::super::property_u64(pre_props, "lease_epoch")
        || hold.fencing_token != super::super::property_u64(pre_props, "fencing_token")
        || hold.work_item_fence != super::super::property_string(pre_props, "work_item_fence")
        || super::super::property_string(pre_props, "lease_owner") != hold.owner_id
}

/// A cancel advances the WorkItem lease epoch / fencing token by exactly one;
/// every other terminal outcome keeps the pre-image's counter.
pub(super) fn expected_terminal_counter(
    value: u64,
    advance: bool,
    overflow: &str,
) -> Result<u64, String> {
    if !advance {
        return Ok(value);
    }
    value.checked_add(1).ok_or_else(|| overflow.to_string())
}

/// Is the caller's post-terminal WorkItem tuple anything other than the exact
/// expected successor of the pre-image?
#[allow(clippy::too_many_arguments)]
pub(super) fn terminal_tuple_invalid(
    pre_props: &serde_json::Map<String, serde_json::Value>,
    attempt: u64,
    expected_lease_epoch: u64,
    expected_fencing_token: u64,
    next_attempt: u64,
    next_lease_epoch: u64,
    next_fencing_token: u64,
    next_work_item_fence: &str,
) -> bool {
    next_attempt == 0
        || next_attempt != attempt
        || next_lease_epoch == 0
        || next_lease_epoch != expected_lease_epoch
        || next_fencing_token == 0
        || next_fencing_token != expected_fencing_token
        || next_work_item_fence.is_empty()
        || next_work_item_fence != super::super::property_string(pre_props, "work_item_fence")
}

pub(super) const ACTIVE_HOLD_REQUIRES_TERMINAL_WORK_ITEM: &str =
    "development lane WorkItem terminalization requires a non-retryable outcome while a hold is active";

/// Apply the lane side of a terminal WorkItem transition inside the caller's
/// already-open redb transaction.  The WorkItem caller has already passed its
/// own CAS; this seam re-reads the linked hold through the maintained
/// WorkItem/attempt index and proves the *pre-terminal* tuple before changing
/// either authority.  A cancel advances the WorkItem epoch/fence, so the hold
/// follows that explicit next tuple while retaining the source tuple for
/// acknowledgement-loss repair in `apply_finish`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn transition_work_item_terminal_hold(
    graph: &str,
    pre_props: &serde_json::Map<String, serde_json::Value>,
    work_item_id: &str,
    next_status: &str,
    cancel_fence_evolution: bool,
    next_attempt: u64,
    next_lease_epoch: u64,
    next_fencing_token: u64,
    next_work_item_fence: &str,
    holds: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    work_item_index: &ScopedOwnerTableMut<(&str, &str, u64), &str>,
    counters: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<bool, String> {
    let attempt = super::super::property_u64(pre_props, "attempt");
    if attempt == 0 {
        return Ok(false);
    }
    let Some(hold_id) = work_item_index_id(work_item_index, graph, work_item_id, attempt)? else {
        return Ok(false);
    };
    let Some(mut row) = hold_load(holds, graph, &hold_id, crypto)? else {
        return Err("development lane WorkItem index points to a missing hold".to_string());
    };

    // A hold that has already released its active charge is a retained
    // terminal authority.  The generic WorkItem row is still checked by the
    // caller's final lane-link validator; there is no second transition here.
    if !row.hold.active_count_charged {
        return Ok(false);
    }
    if hold_not_live(&row.hold) {
        return Err("development lane active hold has an invalid live state".to_string());
    }

    // The linked row must be the exact pre-terminal lifecycle image.  A
    // caller cannot turn a stale/foreign WorkItem mutation into a lane finish,
    // and a ready/submitted image with an active hold is rejected rather than
    // silently auto-finished.
    if work_item_identity_mismatch(pre_props, &row.hold)
        || work_item_fence_mismatch(pre_props, &row.hold, work_item_id, attempt)
    {
        return Err("development lane WorkItem/hold pre-terminal fence mismatch".to_string());
    }
    let intent = lane_intent_value(pre_props)
        .map_err(|_| "development lane WorkItem intent is missing".to_string())?;
    if !lane_intent_matches_hold(
        Some(&intent),
        &row.hold,
        row.ttl_ms,
        &row.resource_reservation_id,
    ) {
        return Err("development lane WorkItem/hold intent mismatch".to_string());
    }

    let terminal = matches!(
        next_status,
        "succeeded" | "failed" | "cancelled" | "dead_letter"
    );
    if !terminal {
        return Err(ACTIVE_HOLD_REQUIRES_TERMINAL_WORK_ITEM.to_string());
    }
    let pre_lease_epoch = super::super::property_u64(pre_props, "lease_epoch");
    let pre_fencing_token = super::super::property_u64(pre_props, "fencing_token");
    let expected_lease_epoch = expected_terminal_counter(
        pre_lease_epoch,
        cancel_fence_evolution,
        "development lane cancel lease epoch overflow",
    )?;
    let expected_fencing_token = expected_terminal_counter(
        pre_fencing_token,
        cancel_fence_evolution,
        "development lane cancel fencing token overflow",
    )?;
    if terminal_tuple_invalid(
        pre_props,
        attempt,
        expected_lease_epoch,
        expected_fencing_token,
        next_attempt,
        next_lease_epoch,
        next_fencing_token,
        next_work_item_fence,
    ) {
        return Err("development lane terminal WorkItem tuple is invalid".to_string());
    }
    let policy = load_policy(policies, graph, &row.hold.tenant_ref, crypto)?
        .ok_or_else(|| "development lane terminal transition has no tenant policy".to_string())?;
    let global_policy = load_global_policy(policies, graph, crypto)?
        .ok_or_else(|| "development lane terminal transition has no global policy".to_string())?;
    transition_terminal_hold(
        graph,
        &mut row,
        next_status,
        next_attempt,
        next_lease_epoch,
        next_fencing_token,
        next_work_item_fence,
        Some((
            attempt,
            super::super::property_u64(pre_props, "lease_epoch"),
            super::super::property_u64(pre_props, "fencing_token"),
            super::super::property_string(pre_props, "work_item_fence").to_string(),
        )),
        counters,
        pressure_index,
        policy.policy_revision,
        global_policy.policy_revision,
        crypto,
    )?;
    hold_encode(holds, graph, &row, crypto)?;
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn transition_terminal_hold(
    graph: &str,
    row: &mut DurableLaneHold,
    terminal_state: &str,
    terminal_attempt: u64,
    terminal_lease_epoch: u64,
    terminal_fencing_token: u64,
    terminal_work_item_fence: &str,
    source_tuple: Option<(u64, u64, u64, String)>,
    counters: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
    policy_revision: u64,
    global_policy_revision: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    if !matches!(
        terminal_state,
        "succeeded" | "failed" | "cancelled" | "dead_letter"
    ) {
        return Err("development lane terminal state is invalid".to_string());
    }
    let retained = row
        .hold
        .predicted_disk_bytes
        .max(row.hold.observed_disk_bytes);
    let loaded = load_scope_counters(
        counters,
        graph,
        &row.hold,
        policy_revision,
        global_policy_revision,
        crypto,
    )?;
    apply_counter_delta(
        counters,
        pressure_index,
        graph,
        &row.hold.tenant_ref,
        loaded,
        policy_revision,
        Some(false),
        Some((false, row.hold.predicted_disk_bytes)),
        Some((false, row.hold.observed_disk_bytes)),
        Some((true, retained)),
        global_policy_revision,
        crypto,
    )?;
    let expected_hold_revision = row.hold.hold_revision;
    row.hold.attempt = terminal_attempt;
    row.hold.lease_epoch = terminal_lease_epoch;
    row.hold.fencing_token = terminal_fencing_token;
    row.hold.work_item_fence = terminal_work_item_fence.to_string();
    row.hold.active_count_charged = false;
    row.hold.retained_disk_bytes = retained;
    row.hold.state = DevelopmentLaneHoldState::CleanupPending;
    row.hold.tombstone = true;
    row.terminal_state = Some(terminal_state.to_string());
    row.terminal_expected_hold_revision = Some(expected_hold_revision);
    row.terminal_source_attempt = source_tuple.as_ref().map(|value| value.0);
    row.terminal_source_lease_epoch = source_tuple.as_ref().map(|value| value.1);
    row.terminal_source_fencing_token = source_tuple.as_ref().map(|value| value.2);
    row.terminal_source_work_item_fence = source_tuple.map(|value| value.3);
    row.hold.hold_revision = row
        .hold
        .hold_revision
        .checked_add(1)
        .ok_or_else(|| "development lane hold revision overflow".to_string())?;
    row.hold.lifecycle_revision = row
        .hold
        .lifecycle_revision
        .checked_add(1)
        .ok_or_else(|| "development lane lifecycle revision overflow".to_string())?;
    row.hold.quota_charge = hold_charge(&row.hold, row.hold.hold_revision, policy_revision);
    Ok(())
}

/// Is any field the finish request must carry missing?
pub(super) fn finish_request_incomplete(request: &DevelopmentLaneFinishRequest) -> bool {
    request.tenant_ref.is_empty()
        || request.work_item_id.is_empty()
        || request.owner_id.is_empty()
        || request.hold_id.is_empty()
        || request.idempotency_key.is_empty()
}

/// Does the request reproduce the pre-terminal tuple an earlier finish
/// retained, together with the caller identity?  A lost acknowledgement
/// replays through this tuple rather than borrowing a new fence.
pub(super) fn finish_source_tuple_matches(
    row: &DurableLaneHold,
    request: &DevelopmentLaneFinishRequest,
) -> bool {
    terminal_source_correlations_match(row, request)
        && row.hold.tenant_ref == request.tenant_ref
        && row.hold.work_item_id == request.work_item_id
        && row.hold.owner_id == request.owner_id
}

#[derive(Debug, Clone, Copy)]
pub(super) struct FinishReplayMatches {
    pub(super) source_tuple: bool,
    pub(super) current_tuple: bool,
}

/// The hold has already released its active charge.  A fresh invocation
/// against that terminal tombstone still proves the current lifecycle WorkItem
/// and its typed intent; only the exact invocation key may bypass this (the
/// replay lookup happens before `apply_finish`), so knowing a hold id and an
/// old fence is not enough to manufacture a terminal outcome.
#[allow(clippy::too_many_arguments)]
pub(super) fn finish_retained_replay(
    graph: &str,
    request: &DevelopmentLaneFinishRequest,
    nodes: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    row: &DurableLaneHold,
    matches: FinishReplayMatches,
    policy_revision: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    // A fresh invocation against a terminal tombstone still proves the
    // current lifecycle WorkItem and its typed intent.  Only the exact
    // invocation key may bypass this check (the replay lookup happens
    // before this function); knowing a hold id and old fence is not enough
    // to manufacture a terminal outcome.
    let (work_item_attempt, work_item_lease_epoch, work_item_fencing_token, work_item_fence) =
        if matches.source_tuple {
            (
                row.hold.attempt,
                row.hold.lease_epoch,
                row.hold.fencing_token,
                row.hold.work_item_fence.as_str(),
            )
        } else {
            (
                request.attempt,
                request.lease_epoch,
                request.fencing_token,
                request.work_item_fence.as_str(),
            )
        };
    let work_item = match load_work_item(
        nodes,
        graph,
        &request.work_item_id,
        &request.tenant_ref,
        Some(&request.owner_id),
        work_item_attempt,
        work_item_lease_epoch,
        work_item_fencing_token,
        work_item_fence,
        DevelopmentLaneWorkItemKind::Lifecycle,
        true,
        request.now_ms,
        crypto,
        None,
        None,
    ) {
        Ok(value) => value,
        Err(decision) => return Ok((finish_result(decision, Some(row), policy_revision)?, false)),
    };
    if !finish_state_matches(request.terminal_state, &work_item.status)
        || !lane_intent_matches_hold(
            work_item.lane_intent.as_ref(),
            &row.hold,
            row.ttl_ms,
            &row.resource_reservation_id,
        )
    {
        return Ok((
            finish_result(LaneDecision::InputConflict, Some(row), policy_revision)?,
            false,
        ));
    }
    let requested = finish_state_name(request.terminal_state);
    let terminal_revision_matches = row.terminal_expected_hold_revision
        == Some(request.expected_hold_revision)
        || (row.terminal_source_attempt.is_some()
            && matches.current_tuple
            && row.hold.hold_revision == request.expected_hold_revision);
    let decision = row
        .terminal_state
        .as_deref()
        .filter(|stored| *stored == requested)
        .filter(|_| terminal_revision_matches)
        .map_or(LaneDecision::InputConflict, |_| LaneDecision::Idempotent);
    Ok((finish_result(decision, Some(row), policy_revision)?, false))
}

/// The finish tail for a still-charged hold: prove the current lifecycle
/// WorkItem, its terminal outcome and its intent, then release the active
/// charge into the retained one.
#[allow(clippy::too_many_arguments)]
pub(super) fn finish_live_hold(
    graph: &str,
    request: &DevelopmentLaneFinishRequest,
    nodes: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    holds: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    counters: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
    mut row: DurableLaneHold,
    policy_revision: u64,
    global_policy_revision: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    if row.hold.hold_revision != request.expected_hold_revision {
        return Ok((
            finish_result(LaneDecision::Stale, Some(&row), policy_revision)?,
            false,
        ));
    }
    let work_item = match load_work_item(
        nodes,
        graph,
        &request.work_item_id,
        &request.tenant_ref,
        Some(&request.owner_id),
        request.attempt,
        request.lease_epoch,
        request.fencing_token,
        &request.work_item_fence,
        DevelopmentLaneWorkItemKind::Lifecycle,
        true,
        request.now_ms,
        crypto,
        None,
        None,
    ) {
        Ok(value) => value,
        Err(decision) => return Ok((finish_result(decision, Some(&row), policy_revision)?, false)),
    };
    if !finish_state_matches(request.terminal_state, &work_item.status) {
        return Ok((
            finish_result(LaneDecision::InputConflict, Some(&row), policy_revision)?,
            false,
        ));
    }
    if !lane_intent_matches_hold(
        work_item.lane_intent.as_ref(),
        &row.hold,
        row.ttl_ms,
        &row.resource_reservation_id,
    ) {
        return Ok((
            finish_result(LaneDecision::InputConflict, Some(&row), policy_revision)?,
            false,
        ));
    }
    transition_terminal_hold(
        graph,
        &mut row,
        finish_state_name(request.terminal_state),
        request.attempt,
        request.lease_epoch,
        request.fencing_token,
        &request.work_item_fence,
        None,
        counters,
        pressure_index,
        policy_revision,
        global_policy_revision,
        crypto,
    )?;
    hold_encode(holds, graph, &row, crypto)?;
    Ok((
        finish_result(LaneDecision::Accepted, Some(&row), policy_revision)?,
        true,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_finish(
    graph: &str,
    request: &DevelopmentLaneFinishRequest,
    nodes: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    holds: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    counters: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    if finish_request_incomplete(request) {
        return Ok((finish_result(LaneDecision::Invalid, None, 0)?, false));
    }
    let policy = current_policy(policies, graph, &request.tenant_ref, crypto)?;
    let Some(policy) = policy else {
        return Ok((finish_result(LaneDecision::Policy, None, 0)?, false));
    };
    let policy_revision = policy.policy_revision;
    let Some(global_policy) = load_global_policy(policies, graph, crypto)? else {
        return Ok((
            finish_result(LaneDecision::Policy, None, policy_revision)?,
            false,
        ));
    };
    let global_policy_revision = global_policy.policy_revision;
    let Some(row) = hold_load(holds, graph, &request.hold_id, crypto)? else {
        return Ok((
            finish_result(LaneDecision::NotFound, None, policy_revision)?,
            false,
        ));
    };
    let current_correlations = hold_correlations_match(
        &row.hold,
        &request.tenant_ref,
        &request.work_item_id,
        &request.owner_id,
        request.attempt,
        request.lease_epoch,
        request.fencing_token,
        &request.work_item_fence,
    );
    let current_tuple_matches = current_correlations.is_ok();
    let source_tuple_matches = finish_source_tuple_matches(&row, request);
    if !current_tuple_matches && !source_tuple_matches {
        let decision = current_correlations
            .expect_err("lane finish correlation predicate changed unexpectedly");
        return Ok((finish_result(decision, Some(&row), policy_revision)?, false));
    }
    if !row.hold.active_count_charged {
        return finish_retained_replay(
            graph,
            request,
            nodes,
            &row,
            FinishReplayMatches {
                source_tuple: source_tuple_matches,
                current_tuple: current_tuple_matches,
            },
            policy_revision,
            crypto,
        );
    }
    finish_live_hold(
        graph,
        request,
        nodes,
        holds,
        counters,
        pressure_index,
        row,
        policy_revision,
        global_policy_revision,
        crypto,
    )
}

pub(super) fn remove_index(
    table: &mut ScopedOwnerTableMut<(&str, &str), &str>,
    graph: &str,
    key: &str,
    hold_id: &str,
) -> Result<(), String> {
    let existing = get_index(table, graph, key)?
        .ok_or_else(|| "development lane exclusivity index is missing".to_string())?;
    if existing != hold_id {
        return Err("development lane exclusivity index points to another hold".to_string());
    }
    table.remove((graph, key))?;
    Ok(())
}

pub(super) fn remove_lane_index(
    table: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    graph: &str,
    tenant: &str,
    lane_id: &str,
    hold_id: &str,
) -> Result<(), String> {
    let existing = get_lane_index(table, graph, tenant, lane_id)?
        .ok_or_else(|| "development lane exclusivity index is missing".to_string())?;
    if existing != hold_id {
        return Err("development lane exclusivity index points to another hold".to_string());
    }
    table.remove((graph, tenant, lane_id))?;
    Ok(())
}

pub(super) fn remove_work_item_index(
    table: &mut ScopedOwnerTableMut<(&str, &str, u64), &str>,
    graph: &str,
    hold: &DevelopmentLaneHold,
) -> Result<(), String> {
    let existing = work_item_index_id(table, graph, &hold.work_item_id, hold.attempt)?
        .ok_or_else(|| "development lane WorkItem index is missing".to_string())?;
    if existing != hold.hold_id {
        return Err("development lane WorkItem index points to another hold".to_string());
    }
    table.remove((graph, hold.work_item_id.as_str(), hold.attempt))?;
    Ok(())
}

pub(super) fn remove_branch_index_checked(
    table: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    graph: &str,
    hold: &DevelopmentLaneHold,
) -> Result<(), String> {
    let key = branch_key(hold);
    let existing = branch_index_id(table, graph, hold)?
        .ok_or_else(|| "development lane branch index is missing".to_string())?;
    if existing != hold.hold_id {
        return Err("development lane branch index points to another hold".to_string());
    }
    table.remove((graph, hold.tenant_ref.as_str(), key.as_str()))?;
    Ok(())
}
