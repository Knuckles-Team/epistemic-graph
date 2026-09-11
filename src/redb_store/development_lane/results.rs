use super::super::{resource_decode, resource_encode, DurableCrypto};
use super::identity::method_name;
use super::invocation::prune_invocations;
use super::links::fingerprint;
use super::quota::{empty_charge, hold_charge};
use super::rows::LaneRows;
use super::types::LaneDecision;
use super::validation::{bounded_texts, decision_name, request_digest};
use super::{DurableLaneHold, DurableLaneInvocation};
use crate::epistemic_operations::{
    DevelopmentLaneCleanupCompleteResult, DevelopmentLaneCleanupCompleteResultSchemaVersion,
    DevelopmentLaneFinishResult, DevelopmentLaneFinishResultSchemaVersion, DevelopmentLaneHold,
    DevelopmentLaneObserveResult, DevelopmentLaneObserveResultSchemaVersion,
    DevelopmentLaneQueryResult, DevelopmentLaneQueryResultSchemaVersion,
    DevelopmentLaneQuotaCharge, DevelopmentLaneQuotaPolicy, DevelopmentLaneQuotaUpdateResult,
    DevelopmentLaneQuotaUpdateResultSchemaVersion, DevelopmentLaneRenewResult,
    DevelopmentLaneResult, DevelopmentLaneResultSchemaVersion,
};
use crate::protocol::Method;
use eg_storage::ScopedOwnerTableMut;

pub(super) const REDACTED_PRIVATE_ID: &str = "redacted";

/// `DevelopmentLaneHold` is also the encrypted native record, so its private
/// identity fields remain available to the authority internally.  Every
/// public result/status projection passes through this copy and replaces the
/// managed locator, opaque host identity, and inventory alias with a bounded
/// redaction; callers can observe lifecycle/quota state without learning local
/// filesystem or host-placement details.
pub(super) fn public_hold(hold: &DevelopmentLaneHold) -> DevelopmentLaneHold {
    let mut projected = hold.clone();
    projected.worktree_locator = REDACTED_PRIVATE_ID.to_string();
    projected.host_ref = REDACTED_PRIVATE_ID.to_string();
    projected.host_target_alias = None;
    projected
}

pub(super) fn typed_decision<T: serde::de::DeserializeOwned>(
    decision: LaneDecision,
) -> Result<T, String> {
    serde_json::from_value(serde_json::Value::String(
        decision_name(decision).to_string(),
    ))
    .map_err(|e| e.to_string())
}

pub(super) fn reserve_result(
    decision: LaneDecision,
    row: Option<&DurableLaneHold>,
    policy_revision: u64,
) -> Result<Vec<u8>, String> {
    let hold = row.map(|value| public_hold(&value.hold));
    let hold_revision = hold.as_ref().map_or(0, |value| value.hold_revision);
    let lifecycle_revision = hold.as_ref().map_or(0, |value| value.lifecycle_revision);
    let tombstone = hold.as_ref().is_some_and(|value| value.tombstone);
    let quota_charge = hold
        .as_ref()
        .map(|value| hold_charge(value, hold_revision, policy_revision));
    let result = DevelopmentLaneResult {
        schema_version: DevelopmentLaneResultSchemaVersion::V1,
        decision: typed_decision(decision)?,
        hold,
        hold_revision,
        lifecycle_revision,
        tombstone,
        changed_work_item_ids: Vec::new(),
        quota_charge,
    };
    rmp_serde::to_vec_named(&result).map_err(|e| e.to_string())
}

pub(super) fn renew_result(
    decision: LaneDecision,
    row: Option<&DurableLaneHold>,
    policy_revision: u64,
) -> Result<Vec<u8>, String> {
    let hold = row.map(|value| public_hold(&value.hold));
    let result = DevelopmentLaneRenewResult {
        schema_version: crate::epistemic_operations::DevelopmentLaneRenewResultSchemaVersion::V1,
        decision: typed_decision(decision)?,
        hold_revision: hold.as_ref().map_or(0, |value| value.hold_revision),
        lifecycle_revision: hold.as_ref().map_or(0, |value| value.lifecycle_revision),
        tombstone: hold.as_ref().is_some_and(|value| value.tombstone),
        changed_work_item_ids: Vec::new(),
        quota_charge: hold
            .as_ref()
            .map(|value| hold_charge(value, value.hold_revision, policy_revision)),
        hold,
    };
    rmp_serde::to_vec_named(&result).map_err(|e| e.to_string())
}

pub(super) fn observe_result(
    decision: LaneDecision,
    row: Option<&DurableLaneHold>,
    policy_revision: u64,
) -> Result<Vec<u8>, String> {
    let hold = row.map(|value| public_hold(&value.hold));
    let result = DevelopmentLaneObserveResult {
        schema_version: DevelopmentLaneObserveResultSchemaVersion::V1,
        decision: typed_decision(decision)?,
        hold_revision: hold.as_ref().map_or(0, |value| value.hold_revision),
        lifecycle_revision: hold.as_ref().map_or(0, |value| value.lifecycle_revision),
        tombstone: hold.as_ref().is_some_and(|value| value.tombstone),
        changed_work_item_ids: Vec::new(),
        quota_charge: hold
            .as_ref()
            .map(|value| hold_charge(value, value.hold_revision, policy_revision)),
        hold,
    };
    rmp_serde::to_vec_named(&result).map_err(|e| e.to_string())
}

pub(super) fn finish_result(
    decision: LaneDecision,
    row: Option<&DurableLaneHold>,
    policy_revision: u64,
) -> Result<Vec<u8>, String> {
    let hold = row.map(|value| public_hold(&value.hold));
    let result = DevelopmentLaneFinishResult {
        schema_version: DevelopmentLaneFinishResultSchemaVersion::V1,
        decision: typed_decision(decision)?,
        hold_revision: hold.as_ref().map_or(0, |value| value.hold_revision),
        lifecycle_revision: hold.as_ref().map_or(0, |value| value.lifecycle_revision),
        tombstone: hold.as_ref().is_some_and(|value| value.tombstone),
        changed_work_item_ids: Vec::new(),
        quota_charge: hold
            .as_ref()
            .map(|value| hold_charge(value, value.hold_revision, policy_revision)),
        hold,
    };
    rmp_serde::to_vec_named(&result).map_err(|e| e.to_string())
}

pub(super) fn cleanup_result(
    decision: LaneDecision,
    row: Option<&DurableLaneHold>,
    policy_revision: u64,
) -> Result<Vec<u8>, String> {
    let hold = row.map(|value| public_hold(&value.hold));
    let result = DevelopmentLaneCleanupCompleteResult {
        schema_version: DevelopmentLaneCleanupCompleteResultSchemaVersion::V1,
        decision: typed_decision(decision)?,
        hold_revision: hold.as_ref().map_or(0, |value| value.hold_revision),
        lifecycle_revision: hold.as_ref().map_or(0, |value| value.lifecycle_revision),
        tombstone: hold.as_ref().is_some_and(|value| value.tombstone),
        changed_work_item_ids: Vec::new(),
        quota_charge: hold
            .as_ref()
            .map(|value| hold_charge(value, value.hold_revision, policy_revision)),
        hold,
    };
    rmp_serde::to_vec_named(&result).map_err(|e| e.to_string())
}

pub(super) fn quota_result(
    decision: LaneDecision,
    policy: Option<DevelopmentLaneQuotaPolicy>,
    counters: DevelopmentLaneQuotaCharge,
    policy_revision: u64,
) -> Result<Vec<u8>, String> {
    let result = DevelopmentLaneQuotaUpdateResult {
        schema_version: DevelopmentLaneQuotaUpdateResultSchemaVersion::V1,
        decision: typed_decision(decision)?,
        policy,
        counters,
        policy_revision,
    };
    rmp_serde::to_vec_named(&result).map_err(|e| e.to_string())
}

pub(super) fn query_result(
    decision: LaneDecision,
    row: Option<&DurableLaneHold>,
) -> Result<DevelopmentLaneQueryResult, String> {
    Ok(DevelopmentLaneQueryResult {
        schema_version: DevelopmentLaneQueryResultSchemaVersion::V1,
        decision: typed_decision(decision)?,
        hold: row.map(|value| public_hold(&value.hold)),
        hold_revision: row.map_or(0, |value| value.hold.hold_revision),
        lifecycle_revision: row.map_or(0, |value| value.hold.lifecycle_revision),
        tombstone: row.is_some_and(|value| value.hold.tombstone),
    })
}

pub(super) fn load_invocation<T>(
    invocations: &T,
    graph: &str,
    tenant: &str,
    key: &str,
    method: &Method,
    crypto: DurableCrypto<'_>,
) -> Result<Option<(bool, Vec<u8>)>, String>
where
    T: LaneRows<(&'static str, &'static str, &'static str), &'static [u8]>,
{
    if key.is_empty() {
        return Ok(None);
    }
    bounded_texts(&[
        (graph, "lane invocation graph"),
        (tenant, "lane invocation tenant"),
        (key, "lane invocation key"),
    ])
    .map_err(|decision| format!("lane invocation: {}", decision_name(decision)))?;
    let Some(row) = invocations.row((graph, tenant, key))? else {
        return Ok(None);
    };
    let stored: DurableLaneInvocation = resource_decode(row.value(), crypto)?;
    durable_invocation_bounds(&stored)?;
    let digest = request_digest(method)?;
    if stored.method == method_name(method) && stored.request_digest == digest {
        Ok(Some((true, stored.result)))
    } else {
        Ok(Some((false, stored.result)))
    }
}

pub(super) fn store_invocation(
    invocations: &mut ScopedOwnerTableMut<(&str, &str, &str), &[u8]>,
    graph: &str,
    tenant: &str,
    key: &str,
    method: &Method,
    result: &[u8],
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    if key.is_empty() {
        return Ok(());
    }
    bounded_texts(&[
        (graph, "lane invocation graph"),
        (tenant, "lane invocation tenant"),
        (key, "lane invocation key"),
    ])
    .map_err(|decision| format!("lane invocation: {}", decision_name(decision)))?;
    let row = DurableLaneInvocation {
        method: method_name(method).to_string(),
        request_digest: request_digest(method)?,
        result: result.to_vec(),
    };
    durable_invocation_bounds(&row)?;
    let bytes = resource_encode(&row, crypto)?;
    invocations.insert((graph, tenant, key), bytes.as_slice())?;
    prune_invocations(invocations, graph, tenant, key)
}

pub(super) fn durable_invocation_bounds(row: &DurableLaneInvocation) -> Result<(), String> {
    if !matches!(
        row.method.as_str(),
        "reserve" | "renew" | "observe" | "finish" | "cleanup-complete" | "quota-policy-update"
    ) {
        return Err("stored lane invocation method is invalid".to_string());
    }
    fingerprint(&row.request_digest)
        .map_err(|decision| format!("stored lane invocation: {}", decision_name(decision)))?;
    if row.result.len() > 64 * 1024 {
        return Err("stored lane invocation result exceeds native bound".to_string());
    }
    Ok(())
}

pub(super) fn put_index(
    table: &mut ScopedOwnerTableMut<(&str, &str), &str>,
    graph: &str,
    key: &str,
    hold_id: &str,
) -> Result<(), String> {
    fingerprint(hold_id)
        .map_err(|decision| format!("lane index hold: {}", decision_name(decision)))?;
    table.insert((graph, key), hold_id)?;
    Ok(())
}

pub(super) fn put_lane_index(
    table: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    graph: &str,
    tenant: &str,
    lane_id: &str,
    hold_id: &str,
) -> Result<(), String> {
    fingerprint(hold_id)
        .map_err(|decision| format!("lane index hold: {}", decision_name(decision)))?;
    table.insert((graph, tenant, lane_id), hold_id)?;
    Ok(())
}

pub(super) fn put_tenant_index(
    table: &mut ScopedOwnerTableMut<(&str, &str, &str), &str>,
    graph: &str,
    tenant: &str,
    hold_id: &str,
) -> Result<(), String> {
    fingerprint(hold_id)
        .map_err(|decision| format!("lane tenant index hold: {}", decision_name(decision)))?;
    table.insert((graph, tenant, hold_id), hold_id)?;
    Ok(())
}

pub(super) fn get_index<T>(table: &T, graph: &str, key: &str) -> Result<Option<String>, String>
where
    T: LaneRows<(&'static str, &'static str), &'static str>,
{
    let Some(value) = table.row((graph, key))? else {
        return Ok(None);
    };
    let value = value.value().to_string();
    fingerprint(&value)
        .map_err(|decision| format!("lane index hold: {}", decision_name(decision)))?;
    Ok(Some(value))
}

pub(super) fn get_lane_index<T>(
    table: &T,
    graph: &str,
    tenant: &str,
    lane_id: &str,
) -> Result<Option<String>, String>
where
    T: LaneRows<(&'static str, &'static str, &'static str), &'static str>,
{
    let Some(value) = table.row((graph, tenant, lane_id))? else {
        return Ok(None);
    };
    let value = value.value().to_string();
    fingerprint(&value)
        .map_err(|decision| format!("lane index hold: {}", decision_name(decision)))?;
    Ok(Some(value))
}

pub(super) fn get_tenant_index<T>(
    table: &T,
    graph: &str,
    tenant: &str,
    hold_id: &str,
) -> Result<Option<String>, String>
where
    T: LaneRows<(&'static str, &'static str, &'static str), &'static str>,
{
    let Some(value) = table.row((graph, tenant, hold_id))? else {
        return Ok(None);
    };
    let value = value.value().to_string();
    fingerprint(&value)
        .map_err(|decision| format!("lane tenant index hold: {}", decision_name(decision)))?;
    Ok(Some(value))
}

pub(super) fn empty_input_conflict(method: &Method) -> Result<Vec<u8>, String> {
    match method {
        Method::ReserveDevelopmentLane { .. } => {
            reserve_result(LaneDecision::InputConflict, None, 0)
        }
        Method::RenewDevelopmentLane { .. } => renew_result(LaneDecision::InputConflict, None, 0),
        Method::ObserveDevelopmentLane { .. } => {
            observe_result(LaneDecision::InputConflict, None, 0)
        }
        Method::FinishDevelopmentLane { .. } => finish_result(LaneDecision::InputConflict, None, 0),
        Method::CleanupDevelopmentLane { .. } => {
            cleanup_result(LaneDecision::InputConflict, None, 0)
        }
        Method::UpdateDevelopmentLaneQuota { .. } => {
            quota_result(LaneDecision::Conflict, None, empty_charge(0), 0)
        }
        _ => Err("method is not an idempotent lane mutation".to_string()),
    }
}
