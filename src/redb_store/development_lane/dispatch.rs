use super::super::{DurableCrypto, NODES};
use super::cleanup::{apply_cleanup, apply_global_quota_update, apply_tenant_quota_update};
use super::finish::apply_finish;
use super::identity::{hold_load, idempotency_key, method_name, normalize_now};
use super::links::text;
use super::quota::empty_charge;
use super::renew_observe::{apply_observe, apply_renew};
use super::reserve::apply_reserve;
use super::reserve_support::policy_validate;
use super::results::{
    empty_input_conflict, load_invocation, query_result, quota_result, store_invocation,
};
use super::rows::lane_op_id;
use super::types::LaneDecision;
use super::validation::{decision_name, validate_method_bounds};
use super::{
    COUNTERS, GLOBAL_POLICY_KEY, HOLDS, INVOCATIONS, LANE_INDEX, POLICIES, PRESSURE_INDEX,
    REPOSITORY_BRANCH_INDEX, TENANT_INDEX, WORKTREE_INDEX, WORK_ITEM_INDEX,
};
use crate::epistemic_operations::{
    DevelopmentLaneQueryRequest, DevelopmentLaneQueryResult, DevelopmentLaneQuotaUpdateRequest,
};
use crate::protocol::Method;
use crate::redb_store::shard::{Shard, ShardWrite};
use eg_storage::{GraphShardOwner, ScopedOwnerTableMut, ScopedRead};

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_quota_update(
    graph: &str,
    request: &DevelopmentLaneQuotaUpdateRequest,
    counters: &ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    if request.tenant_ref.is_empty() || request.idempotency_key.is_empty() {
        return Ok((
            quota_result(LaneDecision::Invalid, None, empty_charge(0), 0)?,
            false,
        ));
    }
    if let Err(decision) = policy_validate(&request.policy) {
        return Ok((quota_result(decision, None, empty_charge(0), 0)?, false));
    }
    if request.tenant_ref == GLOBAL_POLICY_KEY {
        return apply_global_quota_update(graph, request, counters, policies, crypto);
    }
    apply_tenant_quota_update(graph, request, counters, pressure_index, policies, crypto)
}

/// Replay one already-committed invocation, or report an input conflict when
/// the same idempotency key arrives with different request bytes.  `None` means
/// the mutation has not been seen and must be applied.
pub(super) fn replay_lane_invocation(
    invocations: &ScopedOwnerTableMut<(&str, &str, &str), &[u8]>,
    graph: &str,
    method: &Method,
    crypto: DurableCrypto<'_>,
) -> Result<Option<(Vec<u8>, bool)>, String> {
    let Some((tenant, key)) = idempotency_key(method) else {
        return Ok(None);
    };
    text(tenant, "lane invocation tenant")
        .map_err(|decision| decision_name(decision).to_string())?;
    text(key, "lane invocation key").map_err(|decision| decision_name(decision).to_string())?;
    let Some((exact, result)) = load_invocation(invocations, graph, tenant, key, method, crypto)?
    else {
        return Ok(None);
    };
    if exact {
        return Ok(Some((result, false)));
    }
    Ok(Some((empty_input_conflict(method)?, false)))
}

/// Persist this mutation's outcome under its idempotency key and report
/// whether the transaction must commit.  Refusal results are also invocation
/// outcomes: persisting them makes acknowledgement loss deterministic while a
/// fresh idempotency key can retry after policy/capacity changes -- so a
/// stored refusal still commits even though nothing else changed.
pub(super) fn record_lane_invocation(
    invocations: &mut ScopedOwnerTableMut<(&str, &str, &str), &[u8]>,
    graph: &str,
    method: &Method,
    result: &[u8],
    operation_changed: bool,
    crypto: DurableCrypto<'_>,
) -> Result<bool, String> {
    let Some((tenant, key)) = idempotency_key(method) else {
        return Ok(operation_changed);
    };
    store_invocation(invocations, graph, tenant, key, method, result, crypto)?;
    Ok(true)
}

/// Apply one lane mutation to this graph's rows inside an already-admitted
/// group.  Answers the generated result bytes and whether any row changed.
pub(super) fn apply_lane_mutation(
    write: &ShardWrite<'_>,
    graph: &str,
    method: &Method,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    validate_method_bounds(graph, method)
        .map_err(|decision| format!("development lane request: {}", decision_name(decision)))?;
    let member = write.graph(graph)?;
    let mut holds = member.open_scoped_table(HOLDS)?;
    let mut tenant_index = member.open_scoped_table(TENANT_INDEX)?;
    let mut lane_index = member.open_scoped_table(LANE_INDEX)?;
    let mut branch_index = member.open_scoped_table(REPOSITORY_BRANCH_INDEX)?;
    let mut worktree_index = member.open_scoped_table(WORKTREE_INDEX)?;
    let mut work_item_index = member.open_scoped_table(WORK_ITEM_INDEX)?;
    let mut counters = member.open_scoped_table(COUNTERS)?;
    let mut pressure_index = member.open_scoped_table(PRESSURE_INDEX)?;
    let mut policies = member.open_scoped_table(POLICIES)?;
    let mut invocations = member.open_scoped_table(INVOCATIONS)?;
    let mut resource_reservations =
        member.open_scoped_table(super::super::RESOURCE_RESERVATIONS)?;
    let nodes = member.open_scoped_table(NODES)?;

    if let Some(replayed) = replay_lane_invocation(&invocations, graph, method, crypto)? {
        return Ok(replayed);
    }

    let (result, operation_changed) = match method {
        Method::ReserveDevelopmentLane { request } => apply_reserve(
            graph,
            request,
            &nodes,
            &mut holds,
            &mut tenant_index,
            &mut lane_index,
            &mut branch_index,
            &mut worktree_index,
            &mut work_item_index,
            &mut counters,
            &mut pressure_index,
            &policies,
            &mut resource_reservations,
            crypto,
        )?,
        Method::RenewDevelopmentLane { request } => apply_renew(
            graph,
            request,
            &nodes,
            &mut holds,
            &mut counters,
            &mut pressure_index,
            &policies,
            crypto,
        )?,
        Method::ObserveDevelopmentLane { request } => apply_observe(
            graph,
            request,
            &nodes,
            &mut holds,
            &mut counters,
            &mut pressure_index,
            &policies,
            crypto,
        )?,
        Method::FinishDevelopmentLane { request } => apply_finish(
            graph,
            request,
            &nodes,
            &mut holds,
            &mut counters,
            &mut pressure_index,
            &policies,
            crypto,
        )?,
        Method::CleanupDevelopmentLane { request } => apply_cleanup(
            graph,
            request,
            &nodes,
            &mut holds,
            &tenant_index,
            &mut lane_index,
            &mut branch_index,
            &mut worktree_index,
            &mut work_item_index,
            &mut counters,
            &mut pressure_index,
            &policies,
            crypto,
        )?,
        Method::UpdateDevelopmentLaneQuota { request } => apply_quota_update(
            graph,
            request,
            &counters,
            &pressure_index,
            &mut policies,
            crypto,
        )?,
        _ => return Err("method is not a development-lane mutation".to_string()),
    };
    let changed = record_lane_invocation(
        &mut invocations,
        graph,
        method,
        &result,
        operation_changed,
        crypto,
    )?;
    Ok((result, changed))
}

/// Commit one lane mutation atomically in redb and return its generated typed
/// result bytes.  `authoritative_now_ms` is supplied by the eventual dispatch
/// seam; the caller's serialized `now_ms` is overwritten before validation,
/// idempotency hashing, or persistence.
pub(crate) fn commit_development_lane(
    shard: &Shard,
    graph: &str,
    method: &Method,
    authoritative_now_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<u8>, String> {
    let method = normalize_now(method, authoritative_now_ms)
        .ok_or_else(|| "method is not a development-lane operation".to_string())?;
    if !matches!(
        method,
        Method::ReserveDevelopmentLane { .. }
            | Method::RenewDevelopmentLane { .. }
            | Method::ObserveDevelopmentLane { .. }
            | Method::FinishDevelopmentLane { .. }
            | Method::CleanupDevelopmentLane { .. }
            | Method::UpdateDevelopmentLaneQuota { .. }
    ) {
        return Err("method is not a development-lane mutation".to_string());
    }
    validate_method_bounds(graph, &method)
        .map_err(|decision| format!("development lane request: {}", decision_name(decision)))?;
    let members = shard.graph_members(&[graph])?;
    let op_id = lane_op_id(graph, method_name(&method), authoritative_now_ms);
    let (group, batches) = shard.admit_maintenance(&members, &op_id)?;
    let write = ShardWrite::open(shard, &group, &members, &batches)?;
    let applied = apply_lane_mutation(&write, graph, &method, crypto);
    write.finish()?;
    match applied {
        // A replay, or a refusal that wrote no row, must not bump the graph's
        // version: the admitted group is aborted rather than committed, which
        // is what the raw path expressed by dropping its write transaction.
        Ok((result, false)) => {
            shard.mutations().abort_group(group)?;
            Ok(result)
        }
        Ok((result, true)) => {
            shard.commit_drain(group, &batches, authoritative_now_ms)?;
            Ok(result)
        }
        Err(error) => {
            shard.mutations().abort_group(group)?;
            Err(error)
        }
    }
}

pub(super) fn read_lane_query(
    read: &ScopedRead<'_, GraphShardOwner>,
    graph: &str,
    request: &DevelopmentLaneQueryRequest,
    crypto: DurableCrypto<'_>,
) -> Result<DevelopmentLaneQueryResult, String> {
    text(&request.tenant_ref, "lane query tenant")
        .map_err(|decision| decision_name(decision).to_string())?;
    text(&request.hold_id, "lane query hold")
        .map_err(|decision| decision_name(decision).to_string())?;
    let holds = read.scoped_owner_table(HOLDS)?;
    let Some(row) = hold_load(&holds, graph, &request.hold_id, crypto)? else {
        return query_result(LaneDecision::NotFound, None);
    };
    if row.hold.tenant_ref != request.tenant_ref {
        return query_result(LaneDecision::NotFound, None);
    }
    query_result(LaneDecision::Accepted, Some(&row))
}

/// Exact authenticated lane-hold/tombstone read.  The read is an MVCC snapshot;
/// clustered ReadIndex/leader routing is intentionally added by the later server
/// seam, not hidden in this redb-only checkpoint.
pub(crate) fn read_development_lane(
    shard: &Shard,
    graph: &str,
    request: &DevelopmentLaneQueryRequest,
    authoritative_now_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<DevelopmentLaneQueryResult, String> {
    let mut request = request.clone();
    request.now_ms = authoritative_now_ms;
    validate_method_bounds(
        graph,
        &Method::QueryDevelopmentLane {
            request: request.clone(),
        },
    )
    .map_err(|decision| format!("development lane query: {}", decision_name(decision)))?;
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    read_lane_query(&read, graph, &request, crypto)
}
