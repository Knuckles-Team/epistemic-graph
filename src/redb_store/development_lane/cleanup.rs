use super::super::{resource_encode, DurableCrypto};
use super::finish::{
    remove_branch_index_checked, remove_index, remove_lane_index, remove_work_item_index,
};
use super::identity::{hold_encode, hold_load, lane_intent_matches_hold};
use super::lifecycle_shared::{current_policy, hold_correlations_match};
use super::links::text;
use super::quota::{
    apply_counter_delta, empty_charge, global_policy_equal, hold_charge, load_global_policy,
    load_policy, load_scope_counters, snapshot_charge, worktree_key,
};
use super::reserve_support::{indexed_policy_pressure, policy_pressure};
use super::results::{cleanup_result, get_tenant_index, quota_result};
use super::types::{LaneDecision, Scope, ScopeCounter};
use super::validation::decision_name;
use super::work_item::load_work_item;
use super::{DurableLaneHold, DurableLanePolicy, GLOBAL_POLICY_KEY};
use crate::epistemic_operations::{
    DevelopmentLaneCleanupCompleteRequest, DevelopmentLaneHold, DevelopmentLaneHoldHostTargetKind,
    DevelopmentLaneHoldState, DevelopmentLaneQuotaCharge, DevelopmentLaneQuotaUpdateRequest,
};
use crate::protocol::DevelopmentLaneWorkItemKind;
use eg_storage::ScopedOwnerTableMut;

/// Load the bounded tenant/global policy snapshot.  The sorted pressure index
/// supplies exact maxima for the non-tenant scope families; policy CAS never
/// scans those families or the hold index.
pub(super) fn policy_scope_rows(
    counters: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    graph: &str,
    tenant: &str,
    policy_revision: u64,
    global_policy_revision: u64,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<ScopeCounter>, String> {
    let probe = DevelopmentLaneHold {
        schema_version: crate::epistemic_operations::DevelopmentLaneHoldSchemaVersion::V1,
        hold_id: "snapshot".to_string(),
        lane_id: "snapshot".to_string(),
        tenant_ref: tenant.to_string(),
        request_id: "snapshot".to_string(),
        work_item_id: "snapshot".to_string(),
        owner_id: "snapshot".to_string(),
        session_id: "snapshot".to_string(),
        fairness_group: "snapshot".to_string(),
        workspace_ref: "snapshot".to_string(),
        repository_id: "snapshot".to_string(),
        base_ref: "refs/heads/main".to_string(),
        base_sha: "0123456789012345678901234567890123456789".to_string(),
        branch: "snapshot".to_string(),
        worktree_locator: "snapshot".to_string(),
        host_target_kind: DevelopmentLaneHoldHostTargetKind::Local,
        host_target_alias: None,
        host_ref: "snapshot-host".to_string(),
        quota_policy_name: "snapshot".to_string(),
        quota_policy_version: "1".to_string(),
        input_fingerprint: format!("v1:{}", "0".repeat(64)),
        predicted_disk_bytes: 0,
        observed_disk_bytes: 0,
        retained_disk_bytes: 0,
        active_count_charged: false,
        quota_charge: empty_charge(policy_revision),
        state: DevelopmentLaneHoldState::Absent,
        attempt: 1,
        lease_epoch: 1,
        fencing_token: 1,
        work_item_fence: "snapshot".to_string(),
        hold_revision: 0,
        lifecycle_revision: 0,
        allocation_revision: 0,
        cleanup_revision: 0,
        expires_at_ms: 0,
        last_renewed_at_ms: 0,
        cleanup_work_item_id: None,
        cleanup_work_item_fence: None,
        cleanup_attempt: None,
        cleanup_lease_epoch: None,
        cleanup_fencing_token: None,
        tombstone: false,
    };
    load_scope_counters(
        counters,
        graph,
        &probe,
        policy_revision,
        global_policy_revision,
        crypto,
    )
}

pub(super) fn policy_counter_snapshot(
    counters: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    graph: &str,
    tenant: &str,
    policy_revision: u64,
    global_policy_revision: u64,
    crypto: DurableCrypto<'_>,
) -> Result<DevelopmentLaneQuotaCharge, String> {
    let mut probe = DevelopmentLaneHold {
        schema_version: crate::epistemic_operations::DevelopmentLaneHoldSchemaVersion::V1,
        hold_id: "snapshot".to_string(),
        lane_id: "snapshot".to_string(),
        tenant_ref: tenant.to_string(),
        request_id: "snapshot".to_string(),
        work_item_id: "snapshot".to_string(),
        owner_id: "snapshot".to_string(),
        session_id: "snapshot".to_string(),
        fairness_group: "snapshot".to_string(),
        workspace_ref: "snapshot".to_string(),
        repository_id: "snapshot".to_string(),
        base_ref: "snapshot".to_string(),
        base_sha: "0123456789012345678901234567890123456789".to_string(),
        branch: "snapshot".to_string(),
        worktree_locator: "snapshot".to_string(),
        host_target_kind: DevelopmentLaneHoldHostTargetKind::Local,
        host_target_alias: None,
        host_ref: "snapshot-host".to_string(),
        quota_policy_name: "snapshot".to_string(),
        quota_policy_version: "1".to_string(),
        input_fingerprint: format!("v1:{}", "0".repeat(64)),
        predicted_disk_bytes: 0,
        observed_disk_bytes: 0,
        retained_disk_bytes: 0,
        active_count_charged: false,
        quota_charge: empty_charge(policy_revision),
        state: DevelopmentLaneHoldState::Absent,
        attempt: 1,
        lease_epoch: 1,
        fencing_token: 1,
        work_item_fence: "snapshot".to_string(),
        hold_revision: 0,
        lifecycle_revision: 0,
        allocation_revision: 0,
        cleanup_revision: 0,
        expires_at_ms: 0,
        last_renewed_at_ms: 0,
        cleanup_work_item_id: None,
        cleanup_work_item_fence: None,
        cleanup_attempt: None,
        cleanup_lease_epoch: None,
        cleanup_fencing_token: None,
        tombstone: false,
    };
    let loaded = load_scope_counters(
        counters,
        graph,
        &probe,
        policy_revision,
        global_policy_revision,
        crypto,
    )?;
    let tenant_value = loaded
        .iter()
        .find(|row| row.scope == Scope::Tenant)
        .map(|row| &row.value)
        .ok_or_else(|| "tenant counter missing from scope set".to_string())?;
    let global_value = loaded
        .iter()
        .find(|row| row.scope == Scope::Global)
        .map(|row| &row.value)
        .ok_or_else(|| "global counter missing from scope set".to_string())?;
    let result = snapshot_charge(tenant_value, global_value, policy_revision);
    // Keep this helper's construction obviously local and avoid accidentally
    // exposing a mutable probe if the generated hold grows new fields.
    probe.tombstone = result.revision == u64::MAX;
    let _ = probe;
    Ok(result)
}

/// Is any field the cleanup-complete request must carry missing, or is its
/// cleanup WorkItem the same node as the lifecycle one?
pub(super) fn cleanup_request_incomplete(request: &DevelopmentLaneCleanupCompleteRequest) -> bool {
    request.tenant_ref.is_empty()
        || request.work_item_id.is_empty()
        || request.owner_id.is_empty()
        || request.hold_id.is_empty()
        || request.cleanup_work_item_id.is_empty()
        || request.cleanup_work_item_id == request.work_item_id
        || request.idempotency_key.is_empty()
        || request.removal_proof_ref.is_empty()
}

/// Prove both WorkItem authorities a cleanup needs: the current lifecycle
/// terminal fence with its typed intent, and the distinct cleanup WorkItem
/// with its typed correlation.  `Some(decision)` refuses the request.
pub(super) fn cleanup_work_item_decision(
    graph: &str,
    request: &DevelopmentLaneCleanupCompleteRequest,
    nodes: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    row: &DurableLaneHold,
    crypto: DurableCrypto<'_>,
) -> Result<Option<LaneDecision>, String> {
    let lifecycle_work_item = match load_work_item(
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
        Err(decision) => return Ok(Some(decision)),
    };
    if !lane_intent_matches_hold(
        lifecycle_work_item.lane_intent.as_ref(),
        &row.hold,
        row.ttl_ms,
        &row.resource_reservation_id,
    ) {
        return Ok(Some(LaneDecision::InputConflict));
    }
    if request.cleanup_attempt == 0
        || request.cleanup_lease_epoch == 0
        || request.cleanup_fencing_token == 0
    {
        return Ok(Some(LaneDecision::Invalid));
    }
    if let Err(decision) = load_work_item(
        nodes,
        graph,
        &request.cleanup_work_item_id,
        &request.tenant_ref,
        None,
        request.cleanup_attempt,
        request.cleanup_lease_epoch,
        request.cleanup_fencing_token,
        &request.cleanup_work_item_fence,
        DevelopmentLaneWorkItemKind::Cleanup,
        false,
        request.now_ms,
        crypto,
        None,
        Some((
            &row.hold.hold_id,
            &row.hold.lane_id,
            request.expected_hold_revision,
        )),
    ) {
        return Ok(Some(decision));
    }
    Ok(None)
}

/// Does the tombstone already record exactly this cleanup?
pub(super) fn cleanup_replay_matches(
    row: &DurableLaneHold,
    request: &DevelopmentLaneCleanupCompleteRequest,
) -> bool {
    row.cleanup_expected_hold_revision == Some(request.expected_hold_revision)
        && row.cleanup_removal_proof_ref.as_deref() == Some(request.removal_proof_ref.as_str())
        && row.hold.cleanup_work_item_id.as_deref() == Some(request.cleanup_work_item_id.as_str())
        && row.hold.cleanup_work_item_fence.as_deref()
            == Some(request.cleanup_work_item_fence.as_str())
        && row.hold.cleanup_attempt == Some(request.cleanup_attempt)
        && row.hold.cleanup_lease_epoch == Some(request.cleanup_lease_epoch)
        && row.hold.cleanup_fencing_token == Some(request.cleanup_fencing_token)
}

/// A fresh invocation against an already-cleaned tombstone must still prove
/// both WorkItem authorities.  The stored replay tuple is necessary for exact
/// idempotency, but it is not a substitute for the current lifecycle terminal
/// fence or the typed cleanup correlation.
pub(super) fn cleanup_tombstone_replay(
    graph: &str,
    request: &DevelopmentLaneCleanupCompleteRequest,
    nodes: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    row: &DurableLaneHold,
    policy_revision: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    if let Some(decision) = cleanup_work_item_decision(graph, request, nodes, row, crypto)? {
        return Ok((cleanup_result(decision, Some(row), policy_revision)?, false));
    }
    if !cleanup_replay_matches(row, request) {
        return Ok((
            cleanup_result(LaneDecision::InputConflict, Some(row), policy_revision)?,
            false,
        ));
    }
    Ok((
        cleanup_result(LaneDecision::Idempotent, Some(row), policy_revision)?,
        false,
    ))
}

/// The cleanup tail for a retained hold: prove both WorkItems, release the
/// retained charge and every exclusivity index, and tombstone the hold as
/// Cleaned while keeping its tenant keyset row discoverable.
#[allow(clippy::too_many_arguments)]
pub(super) fn cleanup_live_hold(
    graph: &str,
    request: &DevelopmentLaneCleanupCompleteRequest,
    nodes: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    holds: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    tenant_index: &ScopedOwnerTableMut<(&str, &str, &str), &str>,
    lane_index: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    branch_index: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    worktree_index: &mut ScopedOwnerTableMut<(&str, &str), &str>,
    work_item_index: &mut ScopedOwnerTableMut<(&str, &str, u64), &str>,
    counters: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
    mut row: DurableLaneHold,
    policy_revision: u64,
    global_policy_revision: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    if let Some(decision) = cleanup_work_item_decision(graph, request, nodes, &row, crypto)? {
        return Ok((
            cleanup_result(decision, Some(&row), policy_revision)?,
            false,
        ));
    }
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
        None,
        Some((false, row.hold.retained_disk_bytes)),
        global_policy_revision,
        crypto,
    )?;
    remove_lane_index(
        lane_index,
        graph,
        &row.hold.tenant_ref,
        &row.hold.lane_id,
        &row.hold.hold_id,
    )?;
    remove_branch_index_checked(branch_index, graph, &row.hold)?;
    remove_index(
        worktree_index,
        graph,
        &worktree_key(&row.hold),
        &row.hold.hold_id,
    )?;
    remove_work_item_index(work_item_index, graph, &row.hold)?;
    if get_tenant_index(tenant_index, graph, &row.hold.tenant_ref, &row.hold.hold_id)?.is_none() {
        return Err("development lane tenant tombstone index is missing".to_string());
    }
    row.hold.retained_disk_bytes = 0;
    row.hold.state = DevelopmentLaneHoldState::Cleaned;
    row.hold.cleanup_work_item_id = Some(request.cleanup_work_item_id.clone());
    row.hold.cleanup_work_item_fence = Some(request.cleanup_work_item_fence.clone());
    row.hold.cleanup_attempt = Some(request.cleanup_attempt);
    row.hold.cleanup_lease_epoch = Some(request.cleanup_lease_epoch);
    row.hold.cleanup_fencing_token = Some(request.cleanup_fencing_token);
    row.cleanup_removal_proof_ref = Some(request.removal_proof_ref.clone());
    row.cleanup_expected_hold_revision = Some(request.expected_hold_revision);
    row.hold.cleanup_revision = row
        .hold
        .cleanup_revision
        .checked_add(1)
        .ok_or_else(|| "development lane cleanup revision overflow".to_string())?;
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
        cleanup_result(LaneDecision::Accepted, Some(&row), policy_revision)?,
        true,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_cleanup(
    graph: &str,
    request: &DevelopmentLaneCleanupCompleteRequest,
    nodes: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    holds: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    tenant_index: &ScopedOwnerTableMut<(&str, &str, &str), &str>,
    lane_index: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    branch_index: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    worktree_index: &mut ScopedOwnerTableMut<(&str, &str), &str>,
    work_item_index: &mut ScopedOwnerTableMut<(&str, &str, u64), &str>,
    counters: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    if cleanup_request_incomplete(request) {
        return Ok((cleanup_result(LaneDecision::Invalid, None, 0)?, false));
    }
    text(&request.removal_proof_ref, "removal proof")
        .map_err(|decision| format!("cleanup proof: {}", decision_name(decision)))?;
    let policy = current_policy(policies, graph, &request.tenant_ref, crypto)?;
    let policy_revision = policy.as_ref().map_or(0, |value| value.policy_revision);
    let Some(_policy) = policy else {
        return Ok((cleanup_result(LaneDecision::Policy, None, 0)?, false));
    };
    let Some(global_policy) = load_global_policy(policies, graph, crypto)? else {
        return Ok((
            cleanup_result(LaneDecision::Policy, None, policy_revision)?,
            false,
        ));
    };
    let global_policy_revision = global_policy.policy_revision;
    let Some(row) = hold_load(holds, graph, &request.hold_id, crypto)? else {
        return Ok((
            cleanup_result(LaneDecision::NotFound, None, policy_revision)?,
            false,
        ));
    };
    if row.hold.tenant_ref != request.tenant_ref {
        return Ok((
            cleanup_result(LaneDecision::NotFound, None, policy_revision)?,
            false,
        ));
    }
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
            cleanup_result(decision, Some(&row), policy_revision)?,
            false,
        ));
    }
    if row.hold.state == DevelopmentLaneHoldState::Cleaned {
        return cleanup_tombstone_replay(graph, request, nodes, &row, policy_revision, crypto);
    }
    if !matches!(
        row.hold.state,
        DevelopmentLaneHoldState::CleanupPending
            | DevelopmentLaneHoldState::Released
            | DevelopmentLaneHoldState::Expired
    ) {
        return Ok((
            cleanup_result(LaneDecision::CleanupRequired, Some(&row), policy_revision)?,
            false,
        ));
    }
    if row.hold.hold_revision != request.expected_hold_revision {
        return Ok((
            cleanup_result(LaneDecision::Stale, Some(&row), policy_revision)?,
            false,
        ));
    }
    cleanup_live_hold(
        graph,
        request,
        nodes,
        holds,
        tenant_index,
        lane_index,
        branch_index,
        worktree_index,
        work_item_index,
        counters,
        pressure_index,
        row,
        policy_revision,
        global_policy_revision,
        crypto,
    )
}

/// The graph-global quota-policy CAS, reached only through the frozen
/// sentinel tenant.  It is the one route that may enter drain while the
/// global counter is already over pressure.
pub(super) fn apply_global_quota_update(
    graph: &str,
    request: &DevelopmentLaneQuotaUpdateRequest,
    counters: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    policies: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    let current = load_global_policy(policies, graph, crypto)?;
    let current_revision = current.as_ref().map_or(0, |value| value.policy_revision);
    let current_charge = policy_counter_snapshot(
        counters,
        graph,
        GLOBAL_POLICY_KEY,
        0,
        current_revision,
        crypto,
    )?;
    if request.expected_policy_revision != current_revision {
        return Ok((
            quota_result(
                LaneDecision::Stale,
                current.as_ref().map(|value| value.policy.clone()),
                current_charge,
                current_revision,
            )?,
            false,
        ));
    }
    if request
        .expected_policy_version
        .as_deref()
        .is_some_and(|expected| {
            current
                .as_ref()
                .is_none_or(|value| value.policy.policy_version != expected)
        })
    {
        return Ok((
            quota_result(
                LaneDecision::Conflict,
                current.as_ref().map(|value| value.policy.clone()),
                current_charge,
                current_revision,
            )?,
            false,
        ));
    }
    let pressure = current_charge.global_count > request.policy.global_count_limit
        || current_charge.global_predicted_disk_bytes > request.policy.global_predicted_disk_bytes
        || current_charge.global_observed_disk_bytes > request.policy.global_observed_disk_bytes
        || current_charge.global_retained_disk_bytes > request.policy.global_retained_disk_bytes;
    if pressure && !request.policy.drain_only {
        return Ok((
            quota_result(
                LaneDecision::Quota,
                current.as_ref().map(|value| value.policy.clone()),
                current_charge,
                current_revision,
            )?,
            false,
        ));
    }
    let next_revision = current_revision
        .checked_add(1)
        .ok_or_else(|| "development lane global policy revision overflow".to_string())?;
    let row = DurableLanePolicy {
        policy: request.policy.clone(),
        policy_revision: next_revision,
        global_policy_revision: next_revision,
    };
    let bytes = resource_encode(&row, crypto)?;
    policies.insert((graph, GLOBAL_POLICY_KEY), bytes.as_slice())?;
    let charge =
        policy_counter_snapshot(counters, graph, GLOBAL_POLICY_KEY, 0, next_revision, crypto)?;
    Ok((
        quota_result(
            LaneDecision::Accepted,
            Some(request.policy.clone()),
            charge,
            next_revision,
        )?,
        true,
    ))
}

/// The ordinary tenant quota-policy CAS.  A tenant may tune its local
/// dimensions but cannot change the shared global controls, and cannot use
/// its drain flag to undercut a live owner/session/workspace/repository/host
/// charge.
pub(super) fn apply_tenant_quota_update(
    graph: &str,
    request: &DevelopmentLaneQuotaUpdateRequest,
    counters: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    let current = load_policy(policies, graph, &request.tenant_ref, crypto)?;
    let current_revision = current.as_ref().map_or(0, |value| value.policy_revision);
    let global = load_global_policy(policies, graph, crypto)?;
    let global_policy_revision = global.as_ref().map_or(0, |value| value.policy_revision);
    let current_charge = policy_counter_snapshot(
        counters,
        graph,
        &request.tenant_ref,
        current_revision,
        global_policy_revision,
        crypto,
    )?;
    if request.expected_policy_revision != current_revision {
        return Ok((
            quota_result(
                LaneDecision::Stale,
                current.as_ref().map(|value| value.policy.clone()),
                current_charge,
                current_revision,
            )?,
            false,
        ));
    }
    if request
        .expected_policy_version
        .as_deref()
        .is_some_and(|expected| {
            current
                .as_ref()
                .is_none_or(|value| value.policy.policy_version != expected)
        })
    {
        return Ok((
            quota_result(
                LaneDecision::Conflict,
                current.as_ref().map(|value| value.policy.clone()),
                current_charge,
                current_revision,
            )?,
            false,
        ));
    }
    // There is one graph-global policy authority.  A tenant may tune its local
    // dimensions, but cannot silently change the shared counter's
    // limits/freshness/drain semantics.
    if global
        .as_ref()
        .is_some_and(|global| !global_policy_equal(&request.policy, &global.policy))
    {
        return Ok((
            quota_result(
                LaneDecision::Conflict,
                current.as_ref().map(|value| value.policy.clone()),
                current_charge,
                current_revision,
            )?,
            false,
        ));
    }
    let next_revision = current_revision
        .checked_add(1)
        .ok_or_else(|| "development lane policy revision overflow".to_string())?;
    let scope_rows = policy_scope_rows(
        counters,
        graph,
        &request.tenant_ref,
        current_revision,
        global_policy_revision,
        crypto,
    )?;
    let pressure = policy_pressure(&scope_rows, &request.policy)
        || indexed_policy_pressure(pressure_index, graph, &request.tenant_ref, &request.policy)?;
    // A tenant CAS cannot use its drain flag to undercut a live owner,
    // session, workspace, repository, or host charge.  Only the explicit
    // graph-global sentinel route above may enter drain while over pressure.
    if pressure {
        return Ok((
            quota_result(
                LaneDecision::Quota,
                current.as_ref().map(|value| value.policy.clone()),
                current_charge,
                current_revision,
            )?,
            false,
        ));
    }
    // The first tenant policy in a graph also seeds the graph-global row, so
    // both rows start at revision 1 together.
    let effective_global_revision = if global.is_none() {
        1
    } else {
        global_policy_revision
    };
    let row = DurableLanePolicy {
        policy: request.policy.clone(),
        policy_revision: next_revision,
        global_policy_revision: effective_global_revision,
    };
    let bytes = resource_encode(&row, crypto)?;
    policies.insert((graph, request.tenant_ref.as_str()), bytes.as_slice())?;
    if global.is_none() {
        let global_row = DurableLanePolicy {
            policy: request.policy.clone(),
            policy_revision: 1,
            global_policy_revision: 1,
        };
        let global_bytes = resource_encode(&global_row, crypto)?;
        policies.insert((graph, GLOBAL_POLICY_KEY), global_bytes.as_slice())?;
    }
    let charge = policy_counter_snapshot(
        counters,
        graph,
        &request.tenant_ref,
        next_revision,
        effective_global_revision,
        crypto,
    )?;
    Ok((
        quota_result(
            LaneDecision::Accepted,
            Some(request.policy.clone()),
            charge,
            next_revision,
        )?,
        true,
    ))
}
