use super::super::DurableCrypto;
use super::identity::{hold_encode, hold_load, lane_intent_matches_hold};
use super::lifecycle_shared::{
    current_policy, expire_active_hold, hold_correlations_match, hold_no_longer_active,
    observation_fresh, observation_ordering_refusal, observe_request_incomplete,
    renew_request_incomplete, ttl_outside_policy,
};
use super::quota::{apply_counter_delta, hold_charge, load_global_policy, load_scope_counters};
use super::results::{observe_result, renew_result};
use super::types::LaneDecision;
use super::work_item::load_work_item;
use super::DurableLaneHold;
use crate::epistemic_operations::{
    DevelopmentLaneHoldState, DevelopmentLaneObserveRequest, DevelopmentLaneQuotaPolicy,
    DevelopmentLaneRenewRequest,
};
use crate::protocol::DevelopmentLaneWorkItemKind;
use eg_storage::ScopedOwnerTableMut;

/// The renew tail: prove the lifecycle WorkItem and observation freshness,
/// then extend the live hold in place.  `row` is the already-gated hold.
#[allow(clippy::too_many_arguments)]
pub(super) fn renew_live_hold(
    graph: &str,
    request: &DevelopmentLaneRenewRequest,
    nodes: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    holds: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    mut row: DurableLaneHold,
    global_policy: &DevelopmentLaneQuotaPolicy,
    policy_revision: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
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
        false,
        request.now_ms,
        crypto,
        None,
        None,
    ) {
        Ok(value) => value,
        Err(decision) => return Ok((renew_result(decision, Some(&row), policy_revision)?, false)),
    };
    if !lane_intent_matches_hold(
        work_item.lane_intent.as_ref(),
        &row.hold,
        row.ttl_ms,
        &row.resource_reservation_id,
    ) {
        return Ok((
            renew_result(LaneDecision::InputConflict, Some(&row), policy_revision)?,
            false,
        ));
    }
    if !observation_fresh(&row, global_policy, request.now_ms) {
        return Ok((
            renew_result(LaneDecision::Stale, Some(&row), policy_revision)?,
            false,
        ));
    }
    let expires_at_ms = request
        .now_ms
        .checked_add(request.ttl_ms)
        .ok_or_else(|| "development lane renewal expiry overflow".to_string())?;
    row.hold.state = DevelopmentLaneHoldState::Active;
    row.hold.expires_at_ms = expires_at_ms;
    row.hold.last_renewed_at_ms = request.now_ms;
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
    hold_encode(holds, graph, &row, crypto)?;
    Ok((
        renew_result(LaneDecision::Accepted, Some(&row), policy_revision)?,
        true,
    ))
}

/// The observe tail: prove the lifecycle WorkItem, apply the monotonic
/// observation, and charge only the checked positive delta.  `row` is the
/// already-gated hold.
#[allow(clippy::too_many_arguments)]
pub(super) fn observe_live_hold(
    graph: &str,
    request: &DevelopmentLaneObserveRequest,
    nodes: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    holds: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    counters: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
    mut row: DurableLaneHold,
    policy_revision: u64,
    global_policy_revision: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
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
        false,
        request.now_ms,
        crypto,
        None,
        None,
    ) {
        Ok(value) => value,
        Err(decision) => {
            return Ok((
                observe_result(decision, Some(&row), policy_revision)?,
                false,
            ))
        }
    };
    if !lane_intent_matches_hold(
        work_item.lane_intent.as_ref(),
        &row.hold,
        row.ttl_ms,
        &row.resource_reservation_id,
    ) {
        return Ok((
            observe_result(LaneDecision::InputConflict, Some(&row), policy_revision)?,
            false,
        ));
    }
    if let Some(decision) = observation_ordering_refusal(request, &row) {
        return Ok((
            observe_result(decision, Some(&row), policy_revision)?,
            false,
        ));
    }
    let previous_observed = row.hold.observed_disk_bytes;
    let delta = request
        .observed_disk_bytes
        .checked_sub(previous_observed)
        .ok_or_else(|| "development lane observation regressed".to_string())?;
    row.observation_revision = request.observation_revision;
    row.hold.observed_disk_bytes = request.observed_disk_bytes;
    row.last_observed_at_ms = Some(request.now_ms);
    row.hold.hold_revision = row
        .hold
        .hold_revision
        .checked_add(1)
        .ok_or_else(|| "development lane hold revision overflow".to_string())?;
    row.hold.quota_charge = hold_charge(&row.hold, row.hold.hold_revision, policy_revision);
    // `row.hold.observed_disk_bytes` is the new monotonic value. Apply only
    // the checked positive delta to each maintained scope counter.
    if delta > 0 {
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
            &request.tenant_ref,
            loaded,
            policy_revision,
            None,
            None,
            Some((true, delta)),
            None,
            global_policy_revision,
            crypto,
        )?;
    }
    hold_encode(holds, graph, &row, crypto)?;
    Ok((
        observe_result(LaneDecision::Accepted, Some(&row), policy_revision)?,
        true,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_renew(
    graph: &str,
    request: &DevelopmentLaneRenewRequest,
    nodes: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    holds: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    counters: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    if renew_request_incomplete(request) {
        return Ok((renew_result(LaneDecision::Invalid, None, 0)?, false));
    }
    let policy = current_policy(policies, graph, &request.tenant_ref, crypto)?;
    let policy_revision = policy.as_ref().map_or(0, |value| value.policy_revision);
    let Some(_policy) = policy else {
        return Ok((renew_result(LaneDecision::Policy, None, 0)?, false));
    };
    let Some(global_policy) = load_global_policy(policies, graph, crypto)? else {
        return Ok((
            renew_result(LaneDecision::Policy, None, policy_revision)?,
            false,
        ));
    };
    let global_policy_revision = global_policy.policy_revision;
    // TTL and freshness are graph-global controls.  Existing holds may renew
    // while a drain is active, but they must observe the current global
    // policy rather than a tenant row that still references an older global
    // revision.
    if ttl_outside_policy(request.ttl_ms, &global_policy.policy) {
        return Ok((
            renew_result(LaneDecision::Policy, None, policy_revision)?,
            false,
        ));
    }
    let Some(mut row) = hold_load(holds, graph, &request.hold_id, crypto)? else {
        return Ok((
            renew_result(LaneDecision::NotFound, None, policy_revision)?,
            false,
        ));
    };
    if let Err(decision) = hold_correlations_match(
        &row.hold,
        &request.tenant_ref,
        &request.work_item_id,
        &request.owner_id,
        request.attempt,
        request.lease_epoch,
        request.fencing_token,
        &request.work_item_fence,
    ) {
        return Ok((renew_result(decision, Some(&row), policy_revision)?, false));
    }
    if hold_no_longer_active(&row.hold) {
        return Ok((
            renew_result(LaneDecision::Terminal, Some(&row), policy_revision)?,
            false,
        ));
    }
    if row.hold.hold_revision != request.expected_hold_revision {
        return Ok((
            renew_result(LaneDecision::Stale, Some(&row), policy_revision)?,
            false,
        ));
    }
    if request.now_ms >= row.hold.expires_at_ms {
        expire_active_hold(
            graph,
            &mut row,
            counters,
            pressure_index,
            policy_revision,
            global_policy_revision,
            crypto,
        )?;
        hold_encode(holds, graph, &row, crypto)?;
        return Ok((
            renew_result(LaneDecision::Expired, Some(&row), policy_revision)?,
            true,
        ));
    }
    renew_live_hold(
        graph,
        request,
        nodes,
        holds,
        row,
        &global_policy.policy,
        policy_revision,
        crypto,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_observe(
    graph: &str,
    request: &DevelopmentLaneObserveRequest,
    nodes: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    holds: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    counters: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    if observe_request_incomplete(request) {
        return Ok((observe_result(LaneDecision::Invalid, None, 0)?, false));
    }
    let policy = current_policy(policies, graph, &request.tenant_ref, crypto)?;
    let policy_revision = policy.as_ref().map_or(0, |value| value.policy_revision);
    let Some(policy) = policy else {
        return Ok((observe_result(LaneDecision::Policy, None, 0)?, false));
    };
    let Some(global_policy) = load_global_policy(policies, graph, crypto)? else {
        return Ok((
            observe_result(LaneDecision::Policy, None, policy.policy_revision)?,
            false,
        ));
    };
    let global_policy_revision = global_policy.policy_revision;
    let Some(mut row) = hold_load(holds, graph, &request.hold_id, crypto)? else {
        return Ok((
            observe_result(LaneDecision::NotFound, None, policy_revision)?,
            false,
        ));
    };
    if let Err(decision) = hold_correlations_match(
        &row.hold,
        &request.tenant_ref,
        &request.work_item_id,
        &request.owner_id,
        request.attempt,
        request.lease_epoch,
        request.fencing_token,
        &request.work_item_fence,
    ) {
        return Ok((
            observe_result(decision, Some(&row), policy_revision)?,
            false,
        ));
    }
    if hold_no_longer_active(&row.hold) {
        return Ok((
            observe_result(LaneDecision::CleanupRequired, Some(&row), policy_revision)?,
            false,
        ));
    }
    if row.hold.hold_revision != request.expected_hold_revision {
        return Ok((
            observe_result(LaneDecision::Stale, Some(&row), policy_revision)?,
            false,
        ));
    }
    if request.now_ms >= row.hold.expires_at_ms {
        expire_active_hold(
            graph,
            &mut row,
            counters,
            pressure_index,
            policy_revision,
            global_policy_revision,
            crypto,
        )?;
        hold_encode(holds, graph, &row, crypto)?;
        return Ok((
            observe_result(LaneDecision::Expired, Some(&row), policy_revision)?,
            true,
        ));
    }
    observe_live_hold(
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
