use super::super::{resource_decode, DurableCrypto};
use super::links::{durable_hold_bounds, fingerprint, intent_validate, text};
use super::quota::{
    branch_key, policy_count_limits, policy_limit, policy_observed_limits, policy_predicted_limits,
    policy_retained_limits, pressure_metric_name, pressure_scope_name, Metric,
};
use super::rows::LaneRows;
use super::types::{LaneDecision, Scope, ScopeCounter};
use super::validation::decision_name;
use super::{DurableLaneHold, DurableLanePolicy, MAX_COUNT, MAX_DISK_BYTES, MAX_TTL_MS};
use crate::epistemic_operations::{
    DevelopmentLaneHold, DevelopmentLaneHoldHostTargetKind, DevelopmentLaneIntent,
    DevelopmentLaneIntentHostTargetKind, DevelopmentLaneQuotaPolicy, DevelopmentLaneReserveRequest,
};
use crate::protocol::DevelopmentLaneWorkItemKind;
use eg_storage::ScopedOwnerTableMut;

pub(super) fn work_item_kind_name(kind: DevelopmentLaneWorkItemKind) -> &'static str {
    match kind {
        DevelopmentLaneWorkItemKind::Lifecycle => "lane.lifecycle",
        DevelopmentLaneWorkItemKind::Cleanup => "lane.cleanup",
    }
}

pub(super) fn hold_target_kind(
    kind: DevelopmentLaneIntentHostTargetKind,
) -> DevelopmentLaneHoldHostTargetKind {
    match kind {
        DevelopmentLaneIntentHostTargetKind::Local => DevelopmentLaneHoldHostTargetKind::Local,
        DevelopmentLaneIntentHostTargetKind::InventoryAlias => {
            DevelopmentLaneHoldHostTargetKind::InventoryAlias
        }
    }
}

/// The policy's TTL window, validated apart from the per-scope limit tables.
pub(super) fn policy_ttl_bounded(policy: &DevelopmentLaneQuotaPolicy) -> bool {
    policy.min_ttl_ms != 0
        && policy.max_ttl_ms >= policy.min_ttl_ms
        && policy.max_ttl_ms <= MAX_TTL_MS
        && policy.max_observation_staleness_ms <= MAX_TTL_MS
}

pub(super) fn policy_validate(policy: &DevelopmentLaneQuotaPolicy) -> Result<(), LaneDecision> {
    text(&policy.policy_name, "policy name")?;
    text(&policy.policy_version, "policy version")?;
    let counts_bounded = policy_count_limits(policy)
        .into_iter()
        .all(|value| (1..=MAX_COUNT).contains(&value));
    let disk_bounded = policy_predicted_limits(policy)
        .into_iter()
        .chain(policy_observed_limits(policy))
        .chain(policy_retained_limits(policy))
        .all(|value| (1..=MAX_DISK_BYTES).contains(&value));
    if !policy_ttl_bounded(policy) || !counts_bounded || !disk_bounded {
        return Err(LaneDecision::Invalid);
    }
    Ok(())
}

pub(super) fn reserve_counter_check(
    counters: &[ScopeCounter],
    policy: &DevelopmentLaneQuotaPolicy,
    predicted_disk_bytes: u64,
) -> Result<(), LaneDecision> {
    for row in counters {
        let next_count = row
            .value
            .active_count
            .checked_add(1)
            .ok_or(LaneDecision::Quota)?;
        let next_predicted = row
            .value
            .predicted_disk_bytes
            .checked_add(predicted_disk_bytes)
            .ok_or(LaneDecision::Quota)?;
        if next_count > policy_limit(policy, row.scope, Metric::Count)
            || next_predicted > policy_limit(policy, row.scope, Metric::Predicted)
            || row.value.observed_disk_bytes > policy_limit(policy, row.scope, Metric::Observed)
            || row.value.retained_disk_bytes > policy_limit(policy, row.scope, Metric::Retained)
        {
            return Err(LaneDecision::Quota);
        }
    }
    Ok(())
}

pub(super) fn policy_pressure(
    counters: &[ScopeCounter],
    policy: &DevelopmentLaneQuotaPolicy,
) -> bool {
    counters.iter().any(|row| {
        row.value.active_count > policy_limit(policy, row.scope, Metric::Count)
            || row.value.predicted_disk_bytes > policy_limit(policy, row.scope, Metric::Predicted)
            || row.value.observed_disk_bytes > policy_limit(policy, row.scope, Metric::Observed)
            || row.value.retained_disk_bytes > policy_limit(policy, row.scope, Metric::Retained)
    })
}

pub(super) fn pressure_max<T>(
    pressure_index: &T,
    graph: &str,
    tenant: &str,
    scope: Scope,
    metric: Metric,
) -> Result<u64, String>
where
    T: LaneRows<
        (
            &'static str,
            &'static str,
            &'static str,
            &'static str,
            u64,
            &'static str,
        ),
        u8,
    >,
{
    let Some(scope_name) = pressure_scope_name(scope) else {
        return Ok(0);
    };
    let metric_name = pressure_metric_name(metric);
    let mut range = pressure_index.row_range(
        (graph, tenant, scope_name, metric_name, 0u64, ""),
        (
            graph,
            tenant,
            scope_name,
            metric_name,
            u64::MAX,
            "\u{10ffff}",
        ),
    )?;
    let Some((key, _)) = range.next_back().transpose().map_err(|e| e.to_string())? else {
        return Ok(0);
    };
    Ok(key.value().4)
}

pub(super) fn indexed_policy_pressure<T>(
    pressure_index: &T,
    graph: &str,
    tenant: &str,
    policy: &DevelopmentLaneQuotaPolicy,
) -> Result<bool, String>
where
    T: LaneRows<
        (
            &'static str,
            &'static str,
            &'static str,
            &'static str,
            u64,
            &'static str,
        ),
        u8,
    >,
{
    for scope in [
        Scope::Owner,
        Scope::Session,
        Scope::Workspace,
        Scope::Repository,
        Scope::Host,
    ] {
        for metric in [
            Metric::Count,
            Metric::Predicted,
            Metric::Observed,
            Metric::Retained,
        ] {
            if pressure_max(pressure_index, graph, tenant, scope, metric)?
                > policy_limit(policy, scope, metric)
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(super) fn index_hold_id(
    holds: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    graph: &str,
    hold_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<String, String> {
    let Some(row) = holds.get((graph, hold_id))? else {
        return Err("development lane index points to a missing hold".to_string());
    };
    let decoded: DurableLaneHold = resource_decode(row.value(), crypto)?;
    durable_hold_bounds(&decoded)?;
    Ok(hold_id.to_string())
}

pub(super) fn exclusive_pair(
    existing: Option<String>,
    expected_hold_id: &str,
    holds: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(), LaneDecision> {
    let Some(existing) = existing else {
        return Ok(());
    };
    if existing == expected_hold_id {
        return Ok(());
    }
    index_hold_id(holds, graph, &existing, crypto).map_err(|_| LaneDecision::Invalid)?;
    Err(LaneDecision::Exclusivity)
}

pub(super) fn put_branch_index(
    table: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    graph: &str,
    hold: &DevelopmentLaneHold,
) -> Result<(), String> {
    let key = branch_key(hold);
    fingerprint(&hold.hold_id)
        .map_err(|decision| format!("lane branch index hold: {}", decision_name(decision)))?;
    table.insert(
        (graph, hold.tenant_ref.as_str(), key.as_str()),
        hold.hold_id.as_str(),
    )?;
    Ok(())
}

pub(super) fn branch_index_id<T>(
    table: &T,
    graph: &str,
    hold: &DevelopmentLaneHold,
) -> Result<Option<String>, String>
where
    T: LaneRows<(&'static str, &'static str, &'static str), &'static str>,
{
    let key = branch_key(hold);
    let Some(value) = table.row((graph, hold.tenant_ref.as_str(), key.as_str()))? else {
        return Ok(None);
    };
    let value = value.value().to_string();
    fingerprint(&value)
        .map_err(|decision| format!("lane branch index hold: {}", decision_name(decision)))?;
    Ok(Some(value))
}

pub(super) fn remove_branch_index(
    table: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    graph: &str,
    hold: &DevelopmentLaneHold,
) -> Result<(), String> {
    let key = branch_key(hold);
    table.remove((graph, hold.tenant_ref.as_str(), key.as_str()))?;
    Ok(())
}

pub(super) fn put_work_item_index(
    table: &mut ScopedOwnerTableMut<(&str, &str, u64), &str>,
    graph: &str,
    hold: &DevelopmentLaneHold,
) -> Result<(), String> {
    fingerprint(&hold.hold_id)
        .map_err(|decision| format!("lane WorkItem index hold: {}", decision_name(decision)))?;
    table.insert(
        (graph, hold.work_item_id.as_str(), hold.attempt),
        hold.hold_id.as_str(),
    )?;
    Ok(())
}

pub(super) fn work_item_index_id<T>(
    table: &T,
    graph: &str,
    work_item_id: &str,
    attempt: u64,
) -> Result<Option<String>, String>
where
    T: LaneRows<(&'static str, &'static str, u64), &'static str>,
{
    let Some(value) = table.row((graph, work_item_id, attempt))? else {
        return Ok(None);
    };
    let value = value.value().to_string();
    fingerprint(&value)
        .map_err(|decision| format!("lane WorkItem index hold: {}", decision_name(decision)))?;
    Ok(Some(value))
}

/// Do the reserve request's own fields disagree with its intent, or is a
/// required identity/fence field missing?
pub(super) fn reserve_request_inconsistent(request: &DevelopmentLaneReserveRequest) -> bool {
    request.tenant_ref != request.intent.tenant_ref
        || request.owner_id != request.intent.owner_id
        || request.work_item_id.is_empty()
        || request.attempt == 0
        || request.lease_epoch == 0
        || request.fencing_token == 0
        || request.work_item_fence.is_empty()
        || request.idempotency_key.is_empty()
}

/// The reserve request's own shape, before any policy, hold or WorkItem is
/// loaded.  Every failure here is reported with revision 0.
pub(super) fn reserve_request_decision(
    request: &DevelopmentLaneReserveRequest,
) -> Result<(), LaneDecision> {
    intent_validate(&request.intent)?;
    if reserve_request_inconsistent(request) {
        return Err(LaneDecision::Invalid);
    }
    for (value, name) in [
        (&request.tenant_ref, "reserve tenant"),
        (&request.work_item_id, "reserve WorkItem"),
        (&request.owner_id, "reserve owner"),
        (&request.work_item_fence, "reserve fence"),
        (&request.idempotency_key, "reserve invocation"),
    ] {
        if text(value, name).is_err() {
            return Err(LaneDecision::Invalid);
        }
    }
    Ok(())
}

/// Does the intent name a different policy than the tenant's current one, or
/// ask for a TTL outside its window?
pub(super) fn reserve_policy_mismatch(
    policy: &DevelopmentLaneQuotaPolicy,
    intent: &DevelopmentLaneIntent,
) -> bool {
    policy.policy_name != intent.quota_policy_name
        || policy.policy_version != intent.quota_policy_version
        || intent.ttl_ms < policy.min_ttl_ms
        || intent.ttl_ms > policy.max_ttl_ms
}

/// The outcome of the reserve policy gate: a refusal with the revision it must
/// be reported against, or the policy the reserve is admitted under.
pub(super) enum ReservePolicyGate {
    Refused(LaneDecision, u64),
    Ready {
        policy: Box<DurableLanePolicy>,
        policy_revision: u64,
        global_policy_revision: u64,
    },
}
