use super::super::{resource_decode, resource_encode, DurableCrypto};
use super::links::durable_hold_bounds;
use super::quota::{
    hold_lane_identity_equal, hold_placement_equal, hold_quota_identity_equal,
    hold_work_item_identity_equal,
};
use super::reserve_support::hold_target_kind;
use super::rows::LaneRows;
use super::DurableLaneHold;
use crate::epistemic_operations::{
    DevelopmentLaneHold, DevelopmentLaneIntent, DevelopmentLaneReserveRequest,
};
use crate::protocol::Method;
use eg_storage::ScopedOwnerTableMut;

pub(super) fn hold_immutable_equal(
    row: &DurableLaneHold,
    request: &DevelopmentLaneReserveRequest,
) -> bool {
    hold_work_item_identity_equal(row, request)
        && hold_lane_identity_equal(row, request)
        && hold_placement_equal(row, request)
        && hold_quota_identity_equal(row, request)
}

/// Immutable lane/repository identity carried by the WorkItem intent.
pub(super) fn lane_intent_identity_matches(
    intent: &DevelopmentLaneIntent,
    hold: &DevelopmentLaneHold,
) -> bool {
    intent.tenant_ref == hold.tenant_ref
        && intent.request_id == hold.request_id
        && intent.lane_id == hold.lane_id
        && intent.repository_id == hold.repository_id
        && intent.base_ref == hold.base_ref
        && intent.base_sha == hold.base_sha
        && intent.branch == hold.branch
}

/// Host/workspace/worktree placement carried by the WorkItem intent.
pub(super) fn lane_intent_placement_matches(
    intent: &DevelopmentLaneIntent,
    hold: &DevelopmentLaneHold,
) -> bool {
    hold_target_kind(intent.host_target_kind) == hold.host_target_kind
        && intent.host_target_alias == hold.host_target_alias
        && intent.host_ref == hold.host_ref
        && intent.workspace_ref == hold.workspace_ref
        && intent.worktree_locator == hold.worktree_locator
}

/// Ownership, fairness, quota and reservation inputs.
pub(super) fn lane_intent_quota_matches(
    intent: &DevelopmentLaneIntent,
    hold: &DevelopmentLaneHold,
    ttl_ms: u64,
    resource_reservation_id: &str,
) -> bool {
    intent.owner_id == hold.owner_id
        && intent.session_id == hold.session_id
        && intent.fairness_group == hold.fairness_group
        && intent.quota_policy_name == hold.quota_policy_name
        && intent.quota_policy_version == hold.quota_policy_version
        && intent.predicted_disk_bytes == hold.predicted_disk_bytes
        && intent.ttl_ms == ttl_ms
        && intent.input_fingerprint == hold.input_fingerprint
        && intent.resource_reservation_id == resource_reservation_id
}

pub(super) fn lane_intent_matches_hold(
    intent: Option<&DevelopmentLaneIntent>,
    hold: &DevelopmentLaneHold,
    ttl_ms: u64,
    resource_reservation_id: &str,
) -> bool {
    let Some(intent) = intent else {
        return false;
    };
    lane_intent_identity_matches(intent, hold)
        && lane_intent_placement_matches(intent, hold)
        && lane_intent_quota_matches(intent, hold, ttl_ms, resource_reservation_id)
}

pub(super) fn hold_encode(
    holds: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    graph: &str,
    row: &DurableLaneHold,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    durable_hold_bounds(row)?;
    let bytes = resource_encode(row, crypto)?;
    holds.insert((graph, row.hold.hold_id.as_str()), bytes.as_slice())?;
    Ok(())
}

pub(super) fn hold_load<T>(
    holds: &T,
    graph: &str,
    hold_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableLaneHold>, String>
where
    T: LaneRows<(&'static str, &'static str), &'static [u8]>,
{
    holds
        .row((graph, hold_id))?
        .map(|row| {
            let decoded: DurableLaneHold = resource_decode(row.value(), crypto)?;
            durable_hold_bounds(&decoded)?;
            Ok(decoded)
        })
        .transpose()
}

pub(super) fn idempotency_key(method: &Method) -> Option<(&str, &str)> {
    match method {
        Method::ReserveDevelopmentLane { request } => {
            Some((&request.tenant_ref, &request.idempotency_key))
        }
        Method::RenewDevelopmentLane { request } => {
            Some((&request.tenant_ref, &request.idempotency_key))
        }
        Method::ObserveDevelopmentLane { request } => {
            Some((&request.tenant_ref, &request.idempotency_key))
        }
        Method::FinishDevelopmentLane { request } => {
            Some((&request.tenant_ref, &request.idempotency_key))
        }
        Method::CleanupDevelopmentLane { request } => {
            Some((&request.tenant_ref, &request.idempotency_key))
        }
        Method::UpdateDevelopmentLaneQuota { request } => {
            Some((&request.tenant_ref, &request.idempotency_key))
        }
        _ => None,
    }
}

pub(super) fn method_name(method: &Method) -> &'static str {
    match method {
        Method::ReserveDevelopmentLane { .. } => "reserve",
        Method::RenewDevelopmentLane { .. } => "renew",
        Method::ObserveDevelopmentLane { .. } => "observe",
        Method::FinishDevelopmentLane { .. } => "finish",
        Method::CleanupDevelopmentLane { .. } => "cleanup-complete",
        Method::UpdateDevelopmentLaneQuota { .. } => "quota-policy-update",
        Method::QueryDevelopmentLane { .. } => "exact-query",
        Method::DevelopmentLaneStatus { .. } => "status",
        _ => "unknown",
    }
}

pub(super) fn normalize_now(method: &Method, now_ms: u64) -> Option<Method> {
    let mut method = method.clone();
    match &mut method {
        Method::ReserveDevelopmentLane { request } => request.now_ms = now_ms,
        Method::RenewDevelopmentLane { request } => request.now_ms = now_ms,
        Method::ObserveDevelopmentLane { request } => request.now_ms = now_ms,
        Method::FinishDevelopmentLane { request } => request.now_ms = now_ms,
        Method::CleanupDevelopmentLane { request } => request.now_ms = now_ms,
        Method::QueryDevelopmentLane { request } => request.now_ms = now_ms,
        Method::DevelopmentLaneStatus { request } => request.now_ms = now_ms,
        Method::UpdateDevelopmentLaneQuota { request } => request.now_ms = now_ms,
        _ => return None,
    }
    Some(method)
}
