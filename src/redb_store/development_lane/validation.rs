use super::identity::normalize_now;
use super::links::{intent_validate, text};
use super::reserve_support::policy_validate;
use super::types::LaneDecision;
use super::{MAX_DISK_BYTES, MAX_STATUS_LIMIT, MAX_TTL_MS};
use crate::epistemic_operations::{
    DevelopmentLaneCleanupCompleteRequest, DevelopmentLaneFinishRequest,
    DevelopmentLaneObserveRequest, DevelopmentLaneQueryRequest, DevelopmentLaneQuotaUpdateRequest,
    DevelopmentLaneRenewRequest, DevelopmentLaneReserveRequest, DevelopmentLaneStatusRequest,
};
use crate::protocol::Method;
use sha2::{Digest, Sha256};

pub(super) fn bounded_texts(values: &[(&str, &str)]) -> Result<(), LaneDecision> {
    for (value, name) in values {
        text(value, name)?;
    }
    Ok(())
}

/// Validate every caller-controlled key before opening a native table or
/// consulting an index.  The individual transaction functions repeat the
/// checks needed for their typed decision, but this early gate prevents an
/// oversized opaque key/fence from reaching redb at all.
/// Bounds for the reserve request, including its lane intent.
pub(super) fn validate_reserve_bounds(
    request: &DevelopmentLaneReserveRequest,
) -> Result<(), LaneDecision> {
    intent_validate(&request.intent)?;
    bounded_texts(&[
        (&request.tenant_ref, "reserve tenant"),
        (&request.work_item_id, "reserve WorkItem"),
        (&request.owner_id, "reserve owner"),
        (&request.work_item_fence, "reserve fence"),
        (&request.idempotency_key, "reserve invocation"),
    ])?;
    if request.attempt == 0 || request.lease_epoch == 0 || request.fencing_token == 0 {
        return Err(LaneDecision::Invalid);
    }
    Ok(())
}

/// Bounds for the renew request, including its replacement TTL.
pub(super) fn validate_renew_bounds(
    request: &DevelopmentLaneRenewRequest,
) -> Result<(), LaneDecision> {
    bounded_texts(&[
        (&request.tenant_ref, "renew tenant"),
        (&request.work_item_id, "renew WorkItem"),
        (&request.owner_id, "renew owner"),
        (&request.work_item_fence, "renew fence"),
        (&request.hold_id, "renew hold"),
        (&request.idempotency_key, "renew invocation"),
    ])?;
    if request.attempt == 0
        || request.lease_epoch == 0
        || request.fencing_token == 0
        || request.ttl_ms == 0
        || request.ttl_ms > MAX_TTL_MS
    {
        return Err(LaneDecision::Invalid);
    }
    Ok(())
}

/// Bounds for the observe request, including the replacement footprint.
pub(super) fn validate_observe_bounds(
    request: &DevelopmentLaneObserveRequest,
) -> Result<(), LaneDecision> {
    bounded_texts(&[
        (&request.tenant_ref, "observe tenant"),
        (&request.work_item_id, "observe WorkItem"),
        (&request.owner_id, "observe owner"),
        (&request.work_item_fence, "observe fence"),
        (&request.hold_id, "observe hold"),
        (&request.idempotency_key, "observe invocation"),
    ])?;
    if request.attempt == 0
        || request.lease_epoch == 0
        || request.fencing_token == 0
        || request.observed_disk_bytes > MAX_DISK_BYTES
    {
        return Err(LaneDecision::Invalid);
    }
    Ok(())
}

/// Bounds for the finish request.
pub(super) fn validate_finish_bounds(
    request: &DevelopmentLaneFinishRequest,
) -> Result<(), LaneDecision> {
    bounded_texts(&[
        (&request.tenant_ref, "finish tenant"),
        (&request.work_item_id, "finish WorkItem"),
        (&request.owner_id, "finish owner"),
        (&request.work_item_fence, "finish fence"),
        (&request.hold_id, "finish hold"),
        (&request.idempotency_key, "finish invocation"),
    ])?;
    if request.attempt == 0 || request.lease_epoch == 0 || request.fencing_token == 0 {
        return Err(LaneDecision::Invalid);
    }
    Ok(())
}

/// Bounds for the cleanup-complete request, which carries a second (cleanup)
/// WorkItem fence tuple beside the lifecycle one.
pub(super) fn validate_cleanup_bounds(
    request: &DevelopmentLaneCleanupCompleteRequest,
) -> Result<(), LaneDecision> {
    bounded_texts(&[
        (&request.tenant_ref, "cleanup tenant"),
        (&request.work_item_id, "cleanup lifecycle WorkItem"),
        (&request.owner_id, "cleanup owner"),
        (&request.work_item_fence, "cleanup lifecycle fence"),
        (&request.cleanup_work_item_id, "cleanup WorkItem"),
        (&request.cleanup_work_item_fence, "cleanup fence"),
        (&request.hold_id, "cleanup hold"),
        (&request.removal_proof_ref, "cleanup proof"),
        (&request.idempotency_key, "cleanup invocation"),
    ])?;
    if request.attempt == 0
        || request.lease_epoch == 0
        || request.fencing_token == 0
        || request.cleanup_attempt == 0
        || request.cleanup_lease_epoch == 0
        || request.cleanup_fencing_token == 0
    {
        return Err(LaneDecision::Invalid);
    }
    Ok(())
}

/// Bounds for the quota-policy update request.
pub(super) fn validate_quota_update_bounds(
    request: &DevelopmentLaneQuotaUpdateRequest,
) -> Result<(), LaneDecision> {
    text(&request.tenant_ref, "quota tenant")?;
    text(&request.idempotency_key, "quota invocation")?;
    if let Some(version) = request.expected_policy_version.as_deref() {
        text(version, "quota expected policy version")?;
    }
    policy_validate(&request.policy)?;
    Ok(())
}

/// Bounds for the exact single-hold read.
pub(super) fn validate_query_bounds(
    request: &DevelopmentLaneQueryRequest,
) -> Result<(), LaneDecision> {
    bounded_texts(&[
        (&request.tenant_ref, "query tenant"),
        (&request.hold_id, "query hold"),
    ])?;
    Ok(())
}

/// Bounds for the bounded status page and its optional filters/cursor.
pub(super) fn validate_status_bounds(
    request: &DevelopmentLaneStatusRequest,
) -> Result<(), LaneDecision> {
    text(&request.tenant_ref, "status tenant")?;
    if !(1..=MAX_STATUS_LIMIT).contains(&request.limit) {
        return Err(LaneDecision::Invalid);
    }
    if let Some(value) = request.hold_id.as_deref() {
        text(value, "status hold")?;
    }
    if let Some(value) = request.lane_id.as_deref() {
        text(value, "status lane")?;
    }
    if let Some(value) = request.work_item_id.as_deref() {
        text(value, "status WorkItem")?;
    }
    if let Some(value) = request.cursor.as_deref() {
        text(value, "status cursor")?;
    }
    Ok(())
}

/// Cleanup, quota update, and the two reads.  Split out of
/// `validate_method_bounds` so neither dispatch outgrows the complexity cap.
pub(super) fn validate_lane_tail_bounds(method: &Method) -> Result<(), LaneDecision> {
    match method {
        Method::CleanupDevelopmentLane { request } => validate_cleanup_bounds(request),
        Method::UpdateDevelopmentLaneQuota { request } => validate_quota_update_bounds(request),
        Method::QueryDevelopmentLane { request } => validate_query_bounds(request),
        Method::DevelopmentLaneStatus { request } => validate_status_bounds(request),
        _ => Ok(()),
    }
}

pub(super) fn validate_method_bounds(graph: &str, method: &Method) -> Result<(), LaneDecision> {
    text(graph, "lane graph")?;
    match method {
        Method::ReserveDevelopmentLane { request } => validate_reserve_bounds(request),
        Method::RenewDevelopmentLane { request } => validate_renew_bounds(request),
        Method::ObserveDevelopmentLane { request } => validate_observe_bounds(request),
        Method::FinishDevelopmentLane { request } => validate_finish_bounds(request),
        other => validate_lane_tail_bounds(other),
    }
}

pub(super) fn request_digest(method: &Method) -> Result<String, String> {
    let normalized = normalize_now(method, 0).ok_or_else(|| "not a lane method".to_string())?;
    let bytes = rmp_serde::to_vec_named(&normalized).map_err(|e| e.to_string())?;
    Ok(format!("v1:{}", hex::encode(Sha256::digest(bytes))))
}

/// The wire name of every decision.  Split across three functions purely to
/// stay under the per-function complexity cap; `decision_name_lifecycle` keeps
/// the exhaustive arm list, so adding a `LaneDecision` variant still fails to
/// compile until it is named here.
pub(super) fn decision_name(decision: LaneDecision) -> &'static str {
    match decision {
        LaneDecision::Accepted => "accepted",
        LaneDecision::Idempotent => "idempotent",
        LaneDecision::Stale => "stale",
        LaneDecision::Conflict => "conflict",
        LaneDecision::InputConflict => "input_conflict",
        LaneDecision::Quota => "quota",
        LaneDecision::Policy => "policy",
        other => decision_name_mismatch(other),
    }
}

/// Fence/identity mismatch decisions.
pub(super) fn decision_name_mismatch(decision: LaneDecision) -> &'static str {
    match decision {
        LaneDecision::Drained => "drained",
        LaneDecision::NotFound => "not_found",
        LaneDecision::WrongKind => "wrong_kind",
        LaneDecision::WrongTenant => "wrong_tenant",
        LaneDecision::WrongOwner => "wrong_owner",
        LaneDecision::WrongAttempt => "wrong_attempt",
        LaneDecision::WrongLeaseEpoch => "wrong_lease_epoch",
        other => decision_name_lifecycle(other),
    }
}

/// Lifecycle decisions, plus the exhaustive tail: every variant named by
/// `decision_name`/`decision_name_mismatch` is listed so the match stays
/// exhaustive without a wildcard arm.
pub(super) fn decision_name_lifecycle(decision: LaneDecision) -> &'static str {
    match decision {
        LaneDecision::WrongFence => "wrong_fence",
        LaneDecision::Expired => "expired",
        LaneDecision::Terminal => "terminal",
        LaneDecision::CleanupRequired => "cleanup_required",
        LaneDecision::Exclusivity => "exclusivity",
        LaneDecision::Invalid
        | LaneDecision::Accepted
        | LaneDecision::Idempotent
        | LaneDecision::Stale
        | LaneDecision::Conflict
        | LaneDecision::InputConflict
        | LaneDecision::Quota
        | LaneDecision::Policy
        | LaneDecision::Drained
        | LaneDecision::NotFound
        | LaneDecision::WrongKind
        | LaneDecision::WrongTenant
        | LaneDecision::WrongOwner
        | LaneDecision::WrongAttempt
        | LaneDecision::WrongLeaseEpoch => "invalid",
    }
}
