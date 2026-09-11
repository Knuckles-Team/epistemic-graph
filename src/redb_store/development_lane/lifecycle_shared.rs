use super::super::DurableCrypto;
use super::quota::{apply_counter_delta, hold_charge, load_policy, load_scope_counters};
use super::types::LaneDecision;
use super::{DurableLaneHold, DurableLanePolicy};
use crate::epistemic_operations::{
    DevelopmentLaneFinishRequest, DevelopmentLaneHold, DevelopmentLaneHoldState,
    DevelopmentLaneObserveRequest, DevelopmentLaneQuotaPolicy, DevelopmentLaneRenewRequest,
};
use eg_storage::ScopedOwnerTableMut;

// Private redb-transaction-scoped helper; see `load_work_item`'s justification
// above -- each identity/fencing field must be checked against `hold`
// independently.
#[allow(clippy::too_many_arguments)]
pub(super) fn hold_correlations_match(
    hold: &DevelopmentLaneHold,
    tenant: &str,
    work_item_id: &str,
    owner_id: &str,
    attempt: u64,
    lease_epoch: u64,
    fencing_token: u64,
    work_item_fence: &str,
) -> Result<(), LaneDecision> {
    let decision = [
        (hold.tenant_ref != tenant, LaneDecision::WrongTenant),
        (hold.work_item_id != work_item_id, LaneDecision::Conflict),
        (hold.owner_id != owner_id, LaneDecision::WrongOwner),
        (hold.attempt != attempt, LaneDecision::WrongAttempt),
        (
            hold.lease_epoch != lease_epoch,
            LaneDecision::WrongLeaseEpoch,
        ),
        (
            hold.fencing_token != fencing_token || hold.work_item_fence != work_item_fence,
            LaneDecision::WrongFence,
        ),
    ]
    .into_iter()
    .find_map(|(mismatch, decision)| mismatch.then_some(decision));
    decision.map_or(Ok(()), Err)
}

pub(super) fn terminal_source_correlations_match(
    row: &DurableLaneHold,
    request: &DevelopmentLaneFinishRequest,
) -> bool {
    !row.hold.active_count_charged
        && row.terminal_source_attempt == Some(request.attempt)
        && row.terminal_source_lease_epoch == Some(request.lease_epoch)
        && row.terminal_source_fencing_token == Some(request.fencing_token)
        && row.terminal_source_work_item_fence.as_deref() == Some(request.work_item_fence.as_str())
}

pub(super) fn observation_fresh(
    row: &DurableLaneHold,
    policy: &DevelopmentLaneQuotaPolicy,
    now_ms: u64,
) -> bool {
    row.last_observed_at_ms.is_some_and(|observed_at| {
        observed_at <= now_ms
            && now_ms.saturating_sub(observed_at) <= policy.max_observation_staleness_ms
    })
}

pub(super) fn expire_active_hold(
    graph: &str,
    row: &mut DurableLaneHold,
    counters: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
    policy_revision: u64,
    global_policy_revision: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
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
    row.hold.active_count_charged = false;
    row.hold.retained_disk_bytes = retained;
    row.hold.state = DevelopmentLaneHoldState::Expired;
    row.hold.tombstone = true;
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

pub(super) fn hold_policy_revision(
    row: &DurableLaneHold,
    policy: Option<&DurableLanePolicy>,
) -> u64 {
    policy.map_or(row.hold.quota_charge.policy_revision, |value| {
        value.policy_revision
    })
}

pub(super) fn current_policy(
    policies: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    graph: &str,
    tenant: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableLanePolicy>, String> {
    load_policy(policies, graph, tenant, crypto)
}

/// Is any field the renew request must carry missing or zero?
pub(super) fn renew_request_incomplete(request: &DevelopmentLaneRenewRequest) -> bool {
    request.tenant_ref.is_empty()
        || request.work_item_id.is_empty()
        || request.owner_id.is_empty()
        || request.hold_id.is_empty()
        || request.idempotency_key.is_empty()
        || request.ttl_ms == 0
}

/// Is any field the observe request must carry missing or zero?
pub(super) fn observe_request_incomplete(request: &DevelopmentLaneObserveRequest) -> bool {
    request.tenant_ref.is_empty()
        || request.work_item_id.is_empty()
        || request.owner_id.is_empty()
        || request.hold_id.is_empty()
        || request.idempotency_key.is_empty()
        || request.observation_revision == 0
}

/// A tombstoned or no-longer-charged hold cannot be renewed or observed in
/// place; it needs its fenced cleanup first.
pub(super) fn hold_no_longer_active(hold: &DevelopmentLaneHold) -> bool {
    hold.tombstone || !hold.active_count_charged
}

/// Is the requested TTL outside the policy's window?
pub(super) fn ttl_outside_policy(ttl_ms: u64, policy: &DevelopmentLaneQuotaPolicy) -> bool {
    ttl_ms < policy.min_ttl_ms || ttl_ms > policy.max_ttl_ms
}

/// Monotonic observation ordering.  An older revision or a shrinking footprint
/// is stale; the exact same revision replays only when the footprint is
/// identical, and is otherwise stale.  `None` means the observation advances.
pub(super) fn observation_ordering_refusal(
    request: &DevelopmentLaneObserveRequest,
    row: &DurableLaneHold,
) -> Option<LaneDecision> {
    if request.observation_revision < row.observation_revision
        || request.observed_disk_bytes < row.hold.observed_disk_bytes
    {
        return Some(LaneDecision::Stale);
    }
    if request.observation_revision != row.observation_revision {
        return None;
    }
    if request.observed_disk_bytes == row.hold.observed_disk_bytes {
        return Some(LaneDecision::Idempotent);
    }
    Some(LaneDecision::Stale)
}
