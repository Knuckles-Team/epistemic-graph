use super::super::{resource_decode, DurableCrypto};
use super::identity::{hold_encode, hold_immutable_equal, hold_load};
use super::quota::{
    apply_counter_delta, empty_charge, global_policy_equal, hold_charge, hold_id,
    load_global_policy, load_policy, load_scope_counters, worktree_key,
};
use super::reserve_support::{
    branch_index_id, exclusive_pair, hold_target_kind, index_hold_id, put_branch_index,
    put_work_item_index, reserve_counter_check, reserve_policy_mismatch, reserve_request_decision,
    work_item_index_id, ReservePolicyGate,
};
use super::results::{
    get_index, get_lane_index, put_index, put_lane_index, put_tenant_index, reserve_result,
};
use super::types::{LaneDecision, ScopeCounter};
use super::work_item::load_work_item;
use super::{DurableLaneHold, DurableLanePolicy};
use crate::epistemic_operations::{
    DevelopmentLaneHold, DevelopmentLaneHoldState, DevelopmentLaneIntent,
    DevelopmentLaneIntentHostTargetKind, DevelopmentLaneQuotaPolicy, DevelopmentLaneReserveRequest,
    ResourceReservationRecordState,
};
use crate::protocol::DevelopmentLaneWorkItemKind;
use eg_storage::ScopedOwnerTableMut;

fn reserve_policy_refusal(
    policy: &DurableLanePolicy,
    global_policy: &DurableLanePolicy,
    request: &DevelopmentLaneReserveRequest,
    global_policy_revision: u64,
) -> Option<LaneDecision> {
    [
        (global_policy.policy.drain_only, LaneDecision::Drained),
        (
            policy.global_policy_revision != global_policy_revision,
            LaneDecision::Conflict,
        ),
        (
            !global_policy_equal(&policy.policy, &global_policy.policy),
            LaneDecision::Conflict,
        ),
        (
            reserve_policy_mismatch(&policy.policy, &request.intent),
            LaneDecision::Policy,
        ),
        (policy.policy.drain_only, LaneDecision::Drained),
    ]
    .into_iter()
    .find_map(|(mismatch, decision)| mismatch.then_some(decision))
}

/// Load the tenant and graph-global policies and prove they agree with each
/// other and with the request's intent.
pub(super) fn reserve_policy_gate(
    policies: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    graph: &str,
    request: &DevelopmentLaneReserveRequest,
    crypto: DurableCrypto<'_>,
) -> Result<ReservePolicyGate, String> {
    let policy = load_policy(policies, graph, &request.tenant_ref, crypto)?;
    let policy_revision = policy.as_ref().map_or(0, |value| value.policy_revision);
    let global_policy = load_global_policy(policies, graph, crypto)?;
    let global_policy_revision = global_policy
        .as_ref()
        .map_or(0, |value| value.policy_revision);
    let decision = match (policy, global_policy) {
        (None, _) => ReservePolicyGate::Refused(LaneDecision::Policy, 0),
        (_, None) => ReservePolicyGate::Refused(LaneDecision::Policy, policy_revision),
        (Some(policy), Some(global_policy)) => {
            reserve_policy_refusal(&policy, &global_policy, request, global_policy_revision)
                .map_or_else(
                    || ReservePolicyGate::Ready {
                        policy: Box::new(policy),
                        policy_revision,
                        global_policy_revision,
                    },
                    |decision| ReservePolicyGate::Refused(decision, policy_revision),
                )
        }
    };
    Ok(decision)
}

/// The derived hold identity is either already present -- an exact replay or
/// an input conflict -- or already claimed for this WorkItem attempt.  `None`
/// means the reserve may proceed to allocate.
pub(super) fn reserve_identity_gate(
    graph: &str,
    request: &DevelopmentLaneReserveRequest,
    derived_hold_id: &str,
    holds: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    work_item_index: &ScopedOwnerTableMut<(&str, &str, u64), &str>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<(LaneDecision, Option<DurableLaneHold>)>, String> {
    if let Some(existing) = hold_load(holds, graph, derived_hold_id, crypto)? {
        if hold_immutable_equal(&existing, request) {
            return Ok(Some((LaneDecision::Idempotent, Some(existing))));
        }
        return Ok(Some((LaneDecision::InputConflict, None)));
    }
    let Some(existing) = work_item_index_id(
        work_item_index,
        graph,
        &request.work_item_id,
        request.attempt,
    )?
    else {
        return Ok(None);
    };
    let existing = index_hold_id(holds, graph, &existing, crypto)
        .map_err(|_| "development lane WorkItem index is orphaned".to_string())?;
    if existing != derived_hold_id {
        return Ok(Some((LaneDecision::InputConflict, None)));
    }
    Err("development lane WorkItem index disagrees with hold identity".to_string())
}

/// Does the linked resource reservation carry a different state/lease/fence
/// tuple than this reserve request?
pub(super) fn resource_fence_mismatch(
    resource: &crate::epistemic_operations::ResourceReservationRecord,
    request: &DevelopmentLaneReserveRequest,
) -> bool {
    resource.state != ResourceReservationRecordState::Reserved
        || resource.expires_at_ms <= request.now_ms
        || resource.tenant_ref != request.tenant_ref
        || resource.work_item_id != request.work_item_id
        || resource.owner_id != request.owner_id
        || resource.attempt != request.attempt
        || resource.lease_epoch != request.lease_epoch
        || resource.fencing_token != request.fencing_token
        || resource.fence != request.work_item_fence
}

/// Does the reservation target the exact host placement the intent asks for?
pub(super) fn resource_target_matches(
    resource: &crate::epistemic_operations::ResourceReservationRecord,
    intent: &DevelopmentLaneIntent,
) -> bool {
    match intent.host_target_kind {
        DevelopmentLaneIntentHostTargetKind::Local => {
            resource.target_kind
                == crate::epistemic_operations::ResourceReservationRecordTargetKind::Local
                && resource.target_alias.is_none()
        }
        DevelopmentLaneIntentHostTargetKind::InventoryAlias => {
            resource.target_kind
                == crate::epistemic_operations::ResourceReservationRecordTargetKind::InventoryAlias
                && resource.target_alias == intent.host_target_alias
        }
    }
}

/// Does the reservation describe a different placement or content than the
/// intent plus the host the proven WorkItem carries?
pub(super) fn resource_placement_mismatch(
    resource: &crate::epistemic_operations::ResourceReservationRecord,
    intent: &DevelopmentLaneIntent,
    host_ref: &str,
) -> bool {
    resource.host_ref != host_ref
        || resource.input_fingerprint != intent.input_fingerprint
        || resource.repository_id != intent.repository_id
        || resource.branch != intent.branch
        || !resource_target_matches(resource, intent)
}

/// The whole reservation cross-check.  Every mismatch is one `WrongFence`
/// decision, so the two halves may be evaluated in either order.
pub(super) fn resource_reservation_mismatch(
    resource: &crate::epistemic_operations::ResourceReservationRecord,
    request: &DevelopmentLaneReserveRequest,
    host_ref: &str,
) -> bool {
    resource_fence_mismatch(resource, request)
        || resource_placement_mismatch(resource, &request.intent, host_ref)
}

/// Exclusivity plus quota admission for a candidate hold.  `Some(decision)`
/// refuses it; `None` means the caller may commit, and the returned scope
/// counters are the ones the commit charges against.
#[allow(clippy::too_many_arguments)]
pub(super) fn reserve_admission_gate(
    graph: &str,
    hold: &DevelopmentLaneHold,
    worktree_key: &str,
    holds: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    lane_index: &ScopedOwnerTableMut<(&str, &str, &str), &str>,
    branch_index: &ScopedOwnerTableMut<(&str, &str, &str), &str>,
    worktree_index: &ScopedOwnerTableMut<(&str, &str), &str>,
    counters: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    policy: &DevelopmentLaneQuotaPolicy,
    policy_revision: u64,
    global_policy_revision: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(Option<LaneDecision>, Vec<ScopeCounter>), String> {
    let lane_existing = get_lane_index(
        lane_index,
        graph,
        hold.tenant_ref.as_str(),
        hold.lane_id.as_str(),
    )?;
    if let Err(decision) = exclusive_pair(lane_existing, &hold.hold_id, holds, graph, crypto) {
        return Ok((Some(decision), Vec::new()));
    }
    let branch_existing = branch_index_id(branch_index, graph, hold)?;
    if let Err(decision) = exclusive_pair(branch_existing, &hold.hold_id, holds, graph, crypto) {
        return Ok((Some(decision), Vec::new()));
    }
    let worktree_existing = get_index(worktree_index, graph, worktree_key)?;
    if let Err(decision) = exclusive_pair(worktree_existing, &hold.hold_id, holds, graph, crypto) {
        return Ok((Some(decision), Vec::new()));
    }
    let loaded = load_scope_counters(
        counters,
        graph,
        hold,
        policy_revision,
        global_policy_revision,
        crypto,
    )?;
    if let Err(decision) = reserve_counter_check(&loaded, policy, hold.predicted_disk_bytes) {
        return Ok((Some(decision), Vec::new()));
    }
    Ok((None, loaded))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_reserve(
    graph: &str,
    request: &DevelopmentLaneReserveRequest,
    nodes: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    holds: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    tenant_index: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    lane_index: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    branch_index: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    worktree_index: &mut ScopedOwnerTableMut<(&str, &str), &str>,
    work_item_index: &mut ScopedOwnerTableMut<(&str, &str, u64), &str>,
    counters: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    resource_reservations: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    if let Err(decision) = reserve_request_decision(request) {
        return Ok((reserve_result(decision, None, 0)?, false));
    }
    let (policy, policy_revision, global_policy_revision) =
        match reserve_policy_gate(policies, graph, request, crypto)? {
            ReservePolicyGate::Refused(decision, revision) => {
                return Ok((reserve_result(decision, None, revision)?, false));
            }
            ReservePolicyGate::Ready {
                policy,
                policy_revision,
                global_policy_revision,
            } => (*policy, policy_revision, global_policy_revision),
        };

    let derived_hold_id = hold_id(&request.intent);
    if let Some((decision, existing)) = reserve_identity_gate(
        graph,
        request,
        &derived_hold_id,
        holds,
        work_item_index,
        crypto,
    )? {
        return Ok((
            reserve_result(decision, existing.as_ref(), policy_revision)?,
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
        false,
        request.now_ms,
        crypto,
        Some(&request.intent),
        None,
    ) {
        Ok(value) => value,
        Err(decision) => return Ok((reserve_result(decision, None, policy_revision)?, false)),
    };

    let resource_row = resource_reservations
        .get((graph, work_item.resource_reservation_id.as_str()))?
        .map(|value| {
            resource_decode::<super::super::DurableResourceReservation>(value.value(), crypto)
        })
        .transpose()?;
    let Some(resource_row) = resource_row else {
        return Ok((
            reserve_result(LaneDecision::NotFound, None, policy_revision)?,
            false,
        ));
    };
    if resource_reservation_mismatch(&resource_row.record, request, &work_item.host_ref) {
        return Ok((
            reserve_result(LaneDecision::WrongFence, None, policy_revision)?,
            false,
        ));
    }

    let expires_at_ms = request
        .now_ms
        .checked_add(request.intent.ttl_ms)
        .ok_or_else(|| "development lane expiry overflow".to_string())?;
    let target_kind = hold_target_kind(request.intent.host_target_kind);
    let mut hold = DevelopmentLaneHold {
        schema_version: crate::epistemic_operations::DevelopmentLaneHoldSchemaVersion::V1,
        hold_id: derived_hold_id,
        lane_id: request.intent.lane_id.clone(),
        tenant_ref: request.tenant_ref.clone(),
        request_id: request.intent.request_id.clone(),
        work_item_id: request.work_item_id.clone(),
        owner_id: request.owner_id.clone(),
        session_id: request.intent.session_id.clone(),
        fairness_group: request.intent.fairness_group.clone(),
        workspace_ref: request.intent.workspace_ref.clone(),
        repository_id: request.intent.repository_id.clone(),
        base_ref: request.intent.base_ref.clone(),
        base_sha: request.intent.base_sha.clone(),
        branch: request.intent.branch.clone(),
        worktree_locator: request.intent.worktree_locator.clone(),
        host_target_kind: target_kind,
        host_target_alias: request.intent.host_target_alias.clone(),
        host_ref: request.intent.host_ref.clone(),
        quota_policy_name: request.intent.quota_policy_name.clone(),
        quota_policy_version: request.intent.quota_policy_version.clone(),
        input_fingerprint: request.intent.input_fingerprint.clone(),
        predicted_disk_bytes: request.intent.predicted_disk_bytes,
        observed_disk_bytes: 0,
        retained_disk_bytes: 0,
        active_count_charged: true,
        quota_charge: empty_charge(policy_revision),
        // No filesystem effect is performed by this checkpoint.  Persisting
        // `Allocating` without a native activate/abort/reconcile transition
        // would strand an authority row after a crash, so reserve commits the
        // database-side hold directly as Active.  RMDD-09's guarded effect
        // adapter will add the later two-phase activation protocol.
        state: DevelopmentLaneHoldState::Active,
        attempt: request.attempt,
        lease_epoch: request.lease_epoch,
        fencing_token: request.fencing_token,
        work_item_fence: request.work_item_fence.clone(),
        hold_revision: 1,
        lifecycle_revision: 1,
        allocation_revision: 1,
        cleanup_revision: 0,
        expires_at_ms,
        last_renewed_at_ms: request.now_ms,
        cleanup_work_item_id: None,
        cleanup_work_item_fence: None,
        cleanup_attempt: None,
        cleanup_lease_epoch: None,
        cleanup_fencing_token: None,
        tombstone: false,
    };
    hold.quota_charge = hold_charge(&hold, hold.hold_revision, policy_revision);

    let worktree_key = worktree_key(&hold);
    let (refusal, loaded) = reserve_admission_gate(
        graph,
        &hold,
        &worktree_key,
        holds,
        lane_index,
        branch_index,
        worktree_index,
        counters,
        &policy.policy,
        policy_revision,
        global_policy_revision,
        crypto,
    )?;
    if let Some(decision) = refusal {
        return Ok((reserve_result(decision, None, policy_revision)?, false));
    }
    apply_counter_delta(
        counters,
        pressure_index,
        graph,
        &request.tenant_ref,
        loaded,
        policy_revision,
        Some(true),
        Some((true, hold.predicted_disk_bytes)),
        None,
        None,
        global_policy_revision,
        crypto,
    )?;
    put_lane_index(
        lane_index,
        graph,
        &hold.tenant_ref,
        &hold.lane_id,
        &hold.hold_id,
    )?;
    put_branch_index(branch_index, graph, &hold)?;
    put_index(worktree_index, graph, &worktree_key, &hold.hold_id)?;
    put_work_item_index(work_item_index, graph, &hold)?;
    put_tenant_index(tenant_index, graph, &hold.tenant_ref, &hold.hold_id)?;
    let row = DurableLaneHold {
        hold: hold.clone(),
        observation_revision: 0,
        last_observed_at_ms: None,
        terminal_state: None,
        terminal_expected_hold_revision: None,
        cleanup_removal_proof_ref: None,
        cleanup_expected_hold_revision: None,
        terminal_source_attempt: None,
        terminal_source_lease_epoch: None,
        terminal_source_fencing_token: None,
        terminal_source_work_item_fence: None,
        resource_reservation_id: request.intent.resource_reservation_id.clone(),
        ttl_ms: request.intent.ttl_ms,
    };
    hold_encode(holds, graph, &row, crypto)?;
    Ok((
        reserve_result(LaneDecision::Accepted, Some(&row), policy_revision)?,
        true,
    ))
}
