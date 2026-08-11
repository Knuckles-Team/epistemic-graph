//! Native development-lane hold and quota authority (RMDD-28).
//!
//! This module deliberately stops at the redb transaction boundary.  It owns
//! the durable lane hold, identity indexes, quota policy/counters, tombstones,
//! and invocation replay rows.  Server dispatch, capability checks, audit/CDC,
//! ReadIndex/Raft routing, and the guarded filesystem effect are separate
//! follow-up seams.  In particular, `worktree_locator` is validated as a
//! managed relative locator but this module never touches the filesystem.

use super::{decode_durable, resource_decode, resource_encode, DurableCrypto, NODES};
use crate::epistemic_operations::{
    DevelopmentLaneCleanupCompleteRequest, DevelopmentLaneCleanupCompleteResult,
    DevelopmentLaneCleanupCompleteResultDecision,
    DevelopmentLaneCleanupCompleteResultSchemaVersion, DevelopmentLaneCleanupIntent,
    DevelopmentLaneCleanupIntentSchemaVersion, DevelopmentLaneFinishRequest,
    DevelopmentLaneFinishRequestTerminalState, DevelopmentLaneFinishResult,
    DevelopmentLaneFinishResultDecision, DevelopmentLaneFinishResultSchemaVersion,
    DevelopmentLaneHold, DevelopmentLaneHoldHostTargetKind, DevelopmentLaneHoldState,
    DevelopmentLaneIntent, DevelopmentLaneIntentHostTargetKind, DevelopmentLaneObserveRequest,
    DevelopmentLaneObserveResult, DevelopmentLaneObserveResultDecision,
    DevelopmentLaneObserveResultSchemaVersion, DevelopmentLaneQueryRequest,
    DevelopmentLaneQueryResult, DevelopmentLaneQueryResultDecision,
    DevelopmentLaneQueryResultSchemaVersion, DevelopmentLaneQuotaCharge,
    DevelopmentLaneQuotaChargeSchemaVersion, DevelopmentLaneQuotaPolicy,
    DevelopmentLaneQuotaUpdateRequest, DevelopmentLaneQuotaUpdateResult,
    DevelopmentLaneQuotaUpdateResultDecision, DevelopmentLaneQuotaUpdateResultSchemaVersion,
    DevelopmentLaneRenewRequest, DevelopmentLaneRenewResult, DevelopmentLaneRenewResultDecision,
    DevelopmentLaneReserveRequest, DevelopmentLaneResult, DevelopmentLaneResultDecision,
    DevelopmentLaneResultSchemaVersion, DevelopmentLaneStatusRequest, DevelopmentLaneStatusResult,
    DevelopmentLaneStatusResultSchemaVersion, ResourceReservationRecordState,
};
use crate::protocol::{DevelopmentLaneWorkItemKind, Method};
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_TEXT: usize = 512;
const MAX_FINGERPRINT: usize = 67;
const MAX_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const MAX_DISK_BYTES: u64 = 1 << 50;
const MAX_COUNT: u64 = 1 << 32;
const MAX_STATUS_LIMIT: u64 = 100;
const MAX_STATUS_SCAN: usize = 512;
const MAX_INVOCATIONS_PER_TENANT: usize = 256;
const MAX_INVOCATION_REPAIR_SCAN: usize = 4_096;
const GLOBAL_POLICY_KEY: &str = "*";
const WORK_ITEM_METADATA_KEY: &str = "metadata";
const REPOSITORY_WORK_ITEM_EXTENSION_KEY: &str = "repository_work_item";
const LANE_INTENT_EXTENSION_KEY: &str = "development_lane_intent";
const LANE_CLEANUP_EXTENSION_KEY: &str = "development_lane_cleanup";

// The only global-policy route is the typed quota-update request with this
// frozen sentinel tenant. Server capability auth must authorize it as an
// administrator; ordinary tenant updates cannot mutate graph-wide controls.

/// Durable hold identity, keyed `(graph, hold_id)`.
pub(crate) const HOLDS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("development_lane_holds");
/// Tenant keyset used only for bounded status pagination.  It remains after
/// cleanup so a terminal hold is still discoverable without a full-table scan.
pub(crate) const TENANT_INDEX: TableDefinition<(&str, &str, &str), &str> =
    TableDefinition::new("development_lane_tenant_index");
/// Immutable lane identity `(graph, tenant, lane_id) -> hold_id`.
///
/// The tenant is part of the key, not merely a value checked after lookup:
/// two tenants may intentionally use the same opaque lane id without
/// contending for one another's hold.
pub(crate) const LANE_INDEX: TableDefinition<(&str, &str, &str), &str> =
    TableDefinition::new("development_lane_lane_index");
/// Repository/branch exclusivity `(tenant, repository, branch) -> hold_id`.
pub(crate) const REPOSITORY_BRANCH_INDEX: TableDefinition<(&str, &str, &str), &str> =
    TableDefinition::new("development_lane_repository_branch_index");
/// Managed worktree locator exclusivity `(host, workspace, locator) -> hold_id`.
/// The key is graph-wide so two tenants cannot claim the same host path.
pub(crate) const WORKTREE_INDEX: TableDefinition<(&str, &str), &str> =
    TableDefinition::new("development_lane_worktree_index");
/// One allocation winner per WorkItem attempt.
pub(crate) const WORK_ITEM_INDEX: TableDefinition<(&str, &str, u64), &str> =
    TableDefinition::new("development_lane_work_item_index");
/// Exact maintained count/predicted/retained counters by scope.
pub(crate) const COUNTERS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("development_lane_counters");
/// Sorted pressure index `(graph, tenant, scope, metric, value, counter_key)`.
/// The final counter key makes equal values unique; reading the last row for a
/// scope/metric is an O(1) exact maximum, so quota-policy CAS never scans live
/// holds or counter rows.
pub(crate) const PRESSURE_INDEX: TableDefinition<(&str, &str, &str, &str, u64, &str), u8> =
    TableDefinition::new("development_lane_pressure_index");
/// Server-owned policy and its monotonic numeric revision, keyed by tenant.
pub(crate) const POLICIES: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("development_lane_policies");
/// Mutation invocation replay/input-conflict rows `(graph, tenant, key)`.
pub(crate) const INVOCATIONS: TableDefinition<(&str, &str, &str), &[u8]> =
    TableDefinition::new("development_lane_invocations");

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableLaneHold {
    hold: DevelopmentLaneHold,
    /// Monotonic observation revision is kept beside the generated public row;
    /// the v1 DTO intentionally exposes the resulting footprint, not internal
    /// observation bookkeeping.
    observation_revision: u64,
    last_observed_at_ms: Option<u64>,
    terminal_state: Option<String>,
    terminal_expected_hold_revision: Option<u64>,
    cleanup_removal_proof_ref: Option<String>,
    cleanup_expected_hold_revision: Option<u64>,
    /// The WorkItem tuple that authorized the terminal transition.  A cancel
    /// transition deliberately advances the WorkItem lease epoch/fencing
    /// token; retaining this pre-terminal tuple lets a lost acknowledgement
    /// retry the already-atomic lane finish without borrowing a new fence.
    #[serde(default)]
    terminal_source_attempt: Option<u64>,
    #[serde(default)]
    terminal_source_lease_epoch: Option<u64>,
    #[serde(default)]
    terminal_source_fencing_token: Option<u64>,
    #[serde(default)]
    terminal_source_work_item_fence: Option<String>,
    resource_reservation_id: String,
    ttl_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableLanePolicy {
    policy: DevelopmentLaneQuotaPolicy,
    policy_revision: u64,
    global_policy_revision: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct DurableLaneCounter {
    active_count: u64,
    predicted_disk_bytes: u64,
    observed_disk_bytes: u64,
    retained_disk_bytes: u64,
    revision: u64,
    policy_revision: u64,
    /// Global counters are shared by every tenant. Their CAS revision is
    /// separate from tenant policy revisions.
    global_policy_revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableLaneInvocation {
    method: String,
    request_digest: String,
    result: Vec<u8>,
}

/// Invocation replay is a bounded acknowledgement-loss cache, not an event
/// log.  Lexical retention is deliberately deterministic because the native
/// redb transaction has no server-side wall clock in its replay key.  The
/// current key is always retained for the duration of the commit so an
/// uncertain acknowledgement can replay the exact result bytes.
fn prune_invocations(
    invocations: &mut redb::Table<(&str, &str, &str), &[u8]>,
    graph: &str,
    tenant: &str,
    current_key: &str,
) -> Result<(), String> {
    let mut keys = Vec::new();
    for row in invocations
        .range((graph, tenant, "")..)
        .map_err(|e| e.to_string())?
    {
        let (key, _) = row.map_err(|e| e.to_string())?;
        let (row_graph, row_tenant, invocation_key) = key.value();
        if row_graph != graph || row_tenant != tenant {
            break;
        }
        text(invocation_key, "stored lane invocation key")
            .map_err(|decision| format!("stored lane invocation: {}", decision_name(decision)))?;
        keys.push(invocation_key.to_string());
        if keys.len() > MAX_INVOCATION_REPAIR_SCAN {
            return Err("lane invocation table exceeds bounded repair capacity".to_string());
        }
    }
    if keys.len() <= MAX_INVOCATIONS_PER_TENANT {
        return Ok(());
    }
    if !keys.iter().any(|key| key == current_key) {
        return Err("lane invocation repair could not find current key".to_string());
    }

    // Redb orders this key range lexically.  Retain the newest lexical keys as
    // the deterministic bounded history, then force the just-written key into
    // the retained set even when it sorts below that window.  Remove every
    // other key from this exact graph/tenant prefix in the same transaction.
    let mut retained = std::collections::BTreeSet::new();
    for key in keys.iter().rev().take(MAX_INVOCATIONS_PER_TENANT) {
        retained.insert(key.clone());
    }
    retained.insert(current_key.to_string());
    while retained.len() > MAX_INVOCATIONS_PER_TENANT {
        let victim = retained
            .iter()
            .find(|key| key.as_str() != current_key)
            .cloned()
            .ok_or_else(|| "lane invocation retention cannot evict current key".to_string())?;
        retained.remove(&victim);
    }
    for key in keys {
        if !retained.contains(&key) {
            invocations
                .remove((graph, tenant, key.as_str()))
                .map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaneDecision {
    Accepted,
    Idempotent,
    Stale,
    Conflict,
    InputConflict,
    Quota,
    Policy,
    Drained,
    NotFound,
    WrongKind,
    WrongTenant,
    WrongOwner,
    WrongAttempt,
    WrongLeaseEpoch,
    WrongFence,
    Expired,
    Terminal,
    CleanupRequired,
    Exclusivity,
    Invalid,
}

#[derive(Debug, Clone)]
struct LaneWorkItem {
    status: String,
    terminal: bool,
    lease_expires_at_ms: u64,
    host_ref: String,
    resource_reservation_id: String,
    cleanup_hold_id: Option<String>,
    cleanup_lane_id: Option<String>,
    cleanup_expected_hold_revision: Option<u64>,
    lane_intent: Option<DevelopmentLaneIntent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scope {
    Tenant,
    Owner,
    Session,
    Workspace,
    Repository,
    Host,
    Global,
}

#[derive(Debug, Clone)]
struct ScopeCounter {
    key: String,
    scope: Scope,
    value: DurableLaneCounter,
}

/// Create every lane-owned table in the canonical bootstrap transaction.
pub(crate) fn initialize_tables(wtx: &WriteTransaction) -> Result<(), String> {
    wtx.open_table(HOLDS).map_err(|e| e.to_string())?;
    wtx.open_table(TENANT_INDEX).map_err(|e| e.to_string())?;
    wtx.open_table(LANE_INDEX).map_err(|e| e.to_string())?;
    wtx.open_table(REPOSITORY_BRANCH_INDEX)
        .map_err(|e| e.to_string())?;
    wtx.open_table(WORKTREE_INDEX).map_err(|e| e.to_string())?;
    wtx.open_table(WORK_ITEM_INDEX).map_err(|e| e.to_string())?;
    wtx.open_table(COUNTERS).map_err(|e| e.to_string())?;
    wtx.open_table(PRESSURE_INDEX).map_err(|e| e.to_string())?;
    wtx.open_table(POLICIES).map_err(|e| e.to_string())?;
    wtx.open_table(INVOCATIONS).map_err(|e| e.to_string())?;
    Ok(())
}

/// Clear every native lane row for a graph as part of the graph lifecycle
/// transaction.  A live or retained-unpruned hold is an authority, not cache
/// data, so ClearGraph/DeleteGraph must fail closed until its fenced cleanup is
/// complete.  Once all holds are terminally cleaned (or an explicitly aborted
/// tombstone), every lane table/index/counter/policy/replay row is removed in
/// the same write transaction; a same-name recreation cannot inherit it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn clear_native_graph_rows(
    graph: &str,
    holds: &mut redb::Table<(&str, &str), &[u8]>,
    tenant_index: &mut redb::Table<(&str, &str, &str), &str>,
    lane_index: &mut redb::Table<(&str, &str, &str), &str>,
    branch_index: &mut redb::Table<(&str, &str, &str), &str>,
    worktree_index: &mut redb::Table<(&str, &str), &str>,
    work_item_index: &mut redb::Table<(&str, &str, u64), &str>,
    counters: &mut redb::Table<(&str, &str), &[u8]>,
    pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &mut redb::Table<(&str, &str), &[u8]>,
    invocations: &mut redb::Table<(&str, &str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    text(graph, "lane graph").map_err(|_| "development lane graph key is invalid".to_string())?;
    let mut hold_keys = Vec::new();
    for row in holds.range((graph, "")..).map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        let (row_graph, hold_id) = key.value();
        if row_graph != graph {
            break;
        }
        let row: DurableLaneHold = resource_decode(value.value(), crypto)?;
        durable_hold_bounds(&row)?;
        if matches!(
            row.hold.state,
            DevelopmentLaneHoldState::Allocating
                | DevelopmentLaneHoldState::Active
                | DevelopmentLaneHoldState::Submitted
                | DevelopmentLaneHoldState::Released
                | DevelopmentLaneHoldState::Expired
                | DevelopmentLaneHoldState::CleanupPending
        ) || row.hold.active_count_charged
            || row.hold.retained_disk_bytes != 0
        {
            return Err("development lane graph lifecycle requires drained holds".to_string());
        }
        hold_keys.push(hold_id.to_string());
    }
    for hold_id in hold_keys {
        holds
            .remove((graph, hold_id.as_str()))
            .map_err(|e| e.to_string())?;
    }

    let tenant_keys: Vec<(String, String)> = tenant_index
        .range((graph, "", "")..)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, tenant, hold_id) = key.value();
            Ok::<_, String>((row_graph == graph, tenant.to_string(), hold_id.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .take_while(|(same_graph, _, _)| *same_graph)
        .map(|(_, tenant, hold_id)| (tenant, hold_id))
        .collect();
    for (tenant, hold_id) in tenant_keys {
        tenant_index
            .remove((graph, tenant.as_str(), hold_id.as_str()))
            .map_err(|e| e.to_string())?;
    }
    let lane_keys: Vec<(String, String)> = lane_index
        .range((graph, "", "")..)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, tenant, lane_id) = key.value();
            Ok::<_, String>((row_graph == graph, tenant.to_string(), lane_id.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .take_while(|(same_graph, _, _)| *same_graph)
        .map(|(_, tenant, lane_id)| (tenant, lane_id))
        .collect();
    for (tenant, lane_id) in lane_keys {
        lane_index
            .remove((graph, tenant.as_str(), lane_id.as_str()))
            .map_err(|e| e.to_string())?;
    }
    let branch_keys: Vec<(String, String)> = branch_index
        .range((graph, "", "")..)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, tenant, branch) = key.value();
            Ok::<_, String>((row_graph == graph, tenant.to_string(), branch.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .take_while(|(same_graph, _, _)| *same_graph)
        .map(|(_, tenant, branch)| (tenant, branch))
        .collect();
    for (tenant, branch) in branch_keys {
        branch_index
            .remove((graph, tenant.as_str(), branch.as_str()))
            .map_err(|e| e.to_string())?;
    }
    let worktree_keys: Vec<String> = worktree_index
        .range((graph, "")..)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, worktree) = key.value();
            Ok::<_, String>((row_graph == graph, worktree.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .take_while(|(same_graph, _)| *same_graph)
        .map(|(_, worktree)| worktree)
        .collect();
    for key in worktree_keys {
        worktree_index
            .remove((graph, key.as_str()))
            .map_err(|e| e.to_string())?;
    }
    let work_item_keys: Vec<(String, u64)> = work_item_index
        .range((graph, "", 0u64)..)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, work_item, attempt) = key.value();
            Ok::<_, String>((row_graph == graph, work_item.to_string(), attempt))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .take_while(|(same_graph, _, _)| *same_graph)
        .map(|(_, work_item, attempt)| (work_item, attempt))
        .collect();
    for (work_item_id, attempt) in work_item_keys {
        work_item_index
            .remove((graph, work_item_id.as_str(), attempt))
            .map_err(|e| e.to_string())?;
    }
    let counter_keys: Vec<String> = counters
        .range((graph, "")..)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, counter_key) = key.value();
            Ok::<_, String>((row_graph == graph, counter_key.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .take_while(|(same_graph, _)| *same_graph)
        .map(|(_, counter_key)| counter_key)
        .collect();
    for key in counter_keys {
        counters
            .remove((graph, key.as_str()))
            .map_err(|e| e.to_string())?;
    }
    let pressure_keys: Vec<(String, String, String, u64, String)> = pressure_index
        .range((graph, "", "", "", 0u64, "")..)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (_, tenant, scope, metric, value, counter_key) = key.value();
            Ok::<_, String>((
                key.value().0 == graph,
                tenant.to_string(),
                scope.to_string(),
                metric.to_string(),
                value,
                counter_key.to_string(),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .take_while(|(same_graph, _, _, _, _, _)| *same_graph)
        .map(|(_, tenant, scope, metric, value, counter_key)| {
            (tenant, scope, metric, value, counter_key)
        })
        .collect();
    for (tenant, scope, metric, value, counter_key) in pressure_keys {
        pressure_index
            .remove((
                graph,
                tenant.as_str(),
                scope.as_str(),
                metric.as_str(),
                value,
                counter_key.as_str(),
            ))
            .map_err(|e| e.to_string())?;
    }
    let policy_keys: Vec<String> = policies
        .range((graph, "")..)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, tenant) = key.value();
            Ok::<_, String>((row_graph == graph, tenant.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .take_while(|(same_graph, _)| *same_graph)
        .map(|(_, tenant)| tenant)
        .collect();
    for key in policy_keys {
        policies
            .remove((graph, key.as_str()))
            .map_err(|e| e.to_string())?;
    }
    let invocation_keys: Vec<(String, String)> = invocations
        .range((graph, "", "")..)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, tenant, invocation_key) = key.value();
            Ok::<_, String>((
                row_graph == graph,
                tenant.to_string(),
                invocation_key.to_string(),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .take_while(|(same_graph, _, _)| *same_graph)
        .map(|(_, tenant, invocation_key)| (tenant, invocation_key))
        .collect();
    for (tenant, key) in invocation_keys {
        invocations
            .remove((graph, tenant.as_str(), key.as_str()))
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Write-transaction adapter used by graph Clear/Delete/checkpoint paths.  It
/// deliberately opens the complete lane table family here so callers cannot
/// clear the ordinary graph/resource rows and forget one lane index.
pub(crate) fn clear_native_graph_rows_in_wtx(
    wtx: &WriteTransaction,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut holds = wtx.open_table(HOLDS).map_err(|e| e.to_string())?;
    let mut tenant_index = wtx.open_table(TENANT_INDEX).map_err(|e| e.to_string())?;
    let mut lane_index = wtx.open_table(LANE_INDEX).map_err(|e| e.to_string())?;
    let mut branch_index = wtx
        .open_table(REPOSITORY_BRANCH_INDEX)
        .map_err(|e| e.to_string())?;
    let mut worktree_index = wtx.open_table(WORKTREE_INDEX).map_err(|e| e.to_string())?;
    let mut work_item_index = wtx.open_table(WORK_ITEM_INDEX).map_err(|e| e.to_string())?;
    let mut counters = wtx.open_table(COUNTERS).map_err(|e| e.to_string())?;
    let mut pressure_index = wtx.open_table(PRESSURE_INDEX).map_err(|e| e.to_string())?;
    let mut policies = wtx.open_table(POLICIES).map_err(|e| e.to_string())?;
    let mut invocations = wtx.open_table(INVOCATIONS).map_err(|e| e.to_string())?;
    clear_native_graph_rows(
        graph,
        &mut holds,
        &mut tenant_index,
        &mut lane_index,
        &mut branch_index,
        &mut worktree_index,
        &mut work_item_index,
        &mut counters,
        &mut pressure_index,
        &mut policies,
        &mut invocations,
        crypto,
    )
}

/// Variant for the compact MutationBatch path, which already owns the lane
/// hold/index/counter tables while applying the batch.  redb does not permit
/// opening the same table twice in one write transaction, so this adapter
/// opens only the remaining lane tables and reuses the existing guards.
#[allow(clippy::too_many_arguments)]
pub(crate) fn clear_native_graph_rows_in_wtx_with_lane_tables(
    wtx: &WriteTransaction,
    graph: &str,
    holds: &mut redb::Table<(&str, &str), &[u8]>,
    work_item_index: &mut redb::Table<(&str, &str, u64), &str>,
    counters: &mut redb::Table<(&str, &str), &[u8]>,
    pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut tenant_index = wtx.open_table(TENANT_INDEX).map_err(|e| e.to_string())?;
    let mut lane_index = wtx.open_table(LANE_INDEX).map_err(|e| e.to_string())?;
    let mut branch_index = wtx
        .open_table(REPOSITORY_BRANCH_INDEX)
        .map_err(|e| e.to_string())?;
    let mut worktree_index = wtx.open_table(WORKTREE_INDEX).map_err(|e| e.to_string())?;
    let mut invocations = wtx.open_table(INVOCATIONS).map_err(|e| e.to_string())?;
    clear_native_graph_rows(
        graph,
        holds,
        &mut tenant_index,
        &mut lane_index,
        &mut branch_index,
        &mut worktree_index,
        work_item_index,
        counters,
        pressure_index,
        policies,
        &mut invocations,
        crypto,
    )
}

/// A lifecycle WorkItem's owner is authoritative while it is live and is
/// retained in `last_lease_owner` after terminalization.  Keep the terminal
/// shape strict as well: a terminal row must not retain a live lease owner that
/// could be mistaken for a fresh claim.
fn lifecycle_work_item_owner_matches(
    props: &serde_json::Map<String, serde_json::Value>,
    status: &str,
    expected_owner: &str,
) -> bool {
    match status {
        "leased" | "running" => super::property_string(props, "lease_owner") == expected_owner,
        "succeeded" | "failed" | "cancelled" | "dead_letter" => {
            super::property_string(props, "lease_owner").is_empty()
                && super::property_string(props, "last_lease_owner") == expected_owner
        }
        _ => false,
    }
}

/// Ordinary checkpoints do not carry lane tables in `GraphDump`; the native
/// rows therefore remain in place and the replacement image must prove every
/// retained hold still has its exact immutable/fenced lifecycle WorkItem (and,
/// after cleanup, its distinct cleanup WorkItem correlation).
/// This is the lane equivalent of RMDD-27's resource-link validation and
/// prevents restore from either discarding a live authority or preserving one
/// whose WorkItem vanished from the incoming image.
pub(crate) fn validate_checkpoint_lane_links<T>(
    graph: &str,
    incoming_nodes: &[(String, Vec<u8>)],
    holds: &T,
    crypto: DurableCrypto<'_>,
) -> Result<(), String>
where
    T: ReadableTable<(&'static str, &'static str), &'static [u8]>,
{
    text(graph, "lane graph").map_err(|_| "development lane graph key is invalid".to_string())?;
    // `incoming_nodes` is the checkpoint's FULL node set for `graph` — every node
    // the graph carries, not just development-lane WorkItems (e.g. `__commons__`
    // also carries broker exchange/binding/message nodes whose ids intentionally
    // use a `\u{1}` control-byte delimiter — see `broker::binding_node_id` — which
    // is a legal graph node id but not `text()`-bounded "WorkItem id" text).
    // Bound-checking every incoming id against the WorkItem id format here would
    // reject an entire checkpoint over an unrelated node's id shape. The actual
    // WorkItem ids this function cares about (`row.hold.work_item_id` /
    // `cleanup_work_item_id`) are already bound-checked at the point they matter —
    // `durable_hold_bounds` below validates the STORED hold's own `work_item_id`
    // field before it is ever used as a lookup key into `incoming`. Mirrors
    // `work_item_capability::validate_snapshot_nodes`'s same content-shape-scoped
    // (not id-format-universal) convention for this same checkpoint path.
    let mut incoming = std::collections::HashMap::with_capacity(incoming_nodes.len());
    for (id, bytes) in incoming_nodes {
        if incoming.insert(id.as_str(), bytes.as_slice()).is_some() {
            return Err("checkpoint contains duplicate WorkItem node".to_string());
        }
    }
    for row in holds.range((graph, "")..).map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        let (row_graph, _) = key.value();
        if row_graph != graph {
            break;
        }
        let row: DurableLaneHold = resource_decode(value.value(), crypto)?;
        durable_hold_bounds(&row)?;
        if matches!(
            row.hold.state,
            DevelopmentLaneHoldState::Absent | DevelopmentLaneHoldState::Aborted
        ) {
            continue;
        }
        let bytes = incoming
            .get(row.hold.work_item_id.as_str())
            .ok_or_else(|| "checkpoint would orphan a development lane hold".to_string())?;
        let props: serde_json::Map<String, serde_json::Value> =
            decode_durable(bytes).map_err(|_| "checkpoint WorkItem decode failed".to_string())?;
        let status = super::property_string(&props, "status");
        if super::property_string(&props, "node_type") != "WorkItem"
            || super::property_string(&props, "tenant") != row.hold.tenant_ref
            || super::property_string(&props, "kind")
                != work_item_kind_name(DevelopmentLaneWorkItemKind::Lifecycle)
            || super::property_u64(&props, "attempt") != row.hold.attempt
            || super::property_u64(&props, "lease_epoch") != row.hold.lease_epoch
            || super::property_u64(&props, "fencing_token") != row.hold.fencing_token
            || super::property_string(&props, "work_item_fence") != row.hold.work_item_fence
            || !lifecycle_work_item_owner_matches(&props, status, &row.hold.owner_id)
        {
            return Err("checkpoint development lane WorkItem fence mismatch".to_string());
        }
        if !checkpoint_lifecycle_status_matches(&row, status) {
            return Err("checkpoint development lane WorkItem status/state mismatch".to_string());
        }
        let intent = lane_intent_value(&props)
            .map_err(|_| "checkpoint development lane intent missing".to_string())?;
        if !lane_intent_matches_hold(
            Some(&intent),
            &row.hold,
            row.ttl_ms,
            &row.resource_reservation_id,
        ) {
            return Err("checkpoint development lane intent mismatch".to_string());
        }
        if row.hold.state == DevelopmentLaneHoldState::Cleaned {
            let cleanup_id =
                row.hold.cleanup_work_item_id.as_deref().ok_or_else(|| {
                    "checkpoint cleaned lane cleanup WorkItem missing".to_string()
                })?;
            let cleanup_fence = row
                .hold
                .cleanup_work_item_fence
                .as_deref()
                .ok_or_else(|| "checkpoint cleaned lane cleanup fence missing".to_string())?;
            let cleanup_attempt = row
                .hold
                .cleanup_attempt
                .ok_or_else(|| "checkpoint cleaned lane cleanup attempt missing".to_string())?;
            let cleanup_lease_epoch = row
                .hold
                .cleanup_lease_epoch
                .ok_or_else(|| "checkpoint cleaned lane cleanup lease epoch missing".to_string())?;
            let cleanup_fencing_token = row.hold.cleanup_fencing_token.ok_or_else(|| {
                "checkpoint cleaned lane cleanup fencing token missing".to_string()
            })?;
            let expected_revision = row
                .cleanup_expected_hold_revision
                .ok_or_else(|| "checkpoint cleaned lane cleanup revision missing".to_string())?;
            let cleanup_bytes = incoming.get(cleanup_id).ok_or_else(|| {
                "checkpoint would orphan a cleaned lane cleanup WorkItem".to_string()
            })?;
            let cleanup_props: serde_json::Map<String, serde_json::Value> =
                decode_durable(cleanup_bytes)
                    .map_err(|_| "checkpoint cleanup WorkItem decode failed".to_string())?;
            let cleanup_status = super::property_string(&cleanup_props, "status");
            let cleanup_terminal = matches!(
                cleanup_status,
                "succeeded" | "failed" | "cancelled" | "dead_letter"
            );
            if super::property_string(&cleanup_props, "node_type") != "WorkItem"
                || super::property_string(&cleanup_props, "tenant") != row.hold.tenant_ref
                || super::property_string(&cleanup_props, "kind")
                    != work_item_kind_name(DevelopmentLaneWorkItemKind::Cleanup)
                || super::property_u64(&cleanup_props, "attempt") != cleanup_attempt
                || super::property_u64(&cleanup_props, "lease_epoch") != cleanup_lease_epoch
                || super::property_u64(&cleanup_props, "fencing_token") != cleanup_fencing_token
                || super::property_string(&cleanup_props, "work_item_fence") != cleanup_fence
                || (!cleanup_terminal && !matches!(cleanup_status, "leased" | "running"))
            {
                return Err("checkpoint cleaned lane cleanup WorkItem fence mismatch".to_string());
            }
            let cleanup = lane_cleanup_value(&cleanup_props)
                .map_err(|_| "checkpoint cleaned lane cleanup intent missing".to_string())?;
            if cleanup.hold_id != row.hold.hold_id
                || cleanup.lane_id != row.hold.lane_id
                || cleanup.expected_hold_revision != expected_revision
            {
                return Err("checkpoint cleaned lane cleanup intent mismatch".to_string());
            }
        }
    }
    Ok(())
}

/// Validate a replacement image against the lane rows already staged in the
/// caller's write transaction.  Snapshot/row-delta commits use this seam before
/// their transaction can commit, so a WorkItem replacement cannot orphan a live
/// or retained lane authority.
pub(crate) fn validate_lane_links_in_wtx(
    wtx: &WriteTransaction,
    graph: &str,
    incoming_nodes: &[(String, Vec<u8>)],
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let holds = wtx.open_table(HOLDS).map_err(|e| e.to_string())?;
    validate_checkpoint_lane_links(graph, incoming_nodes, &holds, crypto)
}

/// Validate the current post-delta WorkItem image from the same write
/// transaction.  Node values are unsealed before they are passed to the
/// checkpoint validator, preserving the exact WorkItem extension checks while
/// keeping the lane and graph replacement atomic.
pub(crate) fn validate_current_lane_links_in_wtx(
    wtx: &WriteTransaction,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let incoming_nodes = {
        let nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;
        let mut incoming_nodes = Vec::new();
        for row in nodes.range((graph, "")..).map_err(|e| e.to_string())? {
            let (key, value) = row.map_err(|e| e.to_string())?;
            let (row_graph, node_id) = key.value();
            if row_graph != graph {
                break;
            }
            incoming_nodes.push((node_id.to_string(), crypto.unseal(value.value())?));
        }
        incoming_nodes
    };
    validate_lane_links_in_wtx(wtx, graph, &incoming_nodes, crypto)
}

fn checkpoint_lifecycle_status_matches(row: &DurableLaneHold, status: &str) -> bool {
    let terminal = matches!(status, "succeeded" | "failed" | "cancelled" | "dead_letter");
    let terminal_state_matches = row
        .terminal_state
        .as_deref()
        .is_some_and(|expected| expected == status);
    match row.hold.state {
        // A live hold must still have the current lease claim and no terminal
        // replay tuple.  Ready/pending rows are not authoritative claims.
        DevelopmentLaneHoldState::Allocating
        | DevelopmentLaneHoldState::Active
        | DevelopmentLaneHoldState::Submitted => {
            matches!(status, "leased" | "running")
                && !terminal
                && row.terminal_state.is_none()
                && row.terminal_expected_hold_revision.is_none()
        }
        // Finish records the exact terminal outcome and the pre-finish hold
        // revision; cleanup can reconcile this retained charge later.
        DevelopmentLaneHoldState::CleanupPending => {
            terminal
                && terminal_state_matches
                && row.terminal_expected_hold_revision.is_some()
                && row.hold.tombstone
                && !row.hold.active_count_charged
                && row.hold.retained_disk_bytes != 0
        }
        // Expiry can race the WorkItem terminal transition.  Both a still-live
        // leased/running claim and an exact terminal outcome remain cleanable,
        // but neither may carry a finish terminal replay tuple.
        DevelopmentLaneHoldState::Expired => {
            (matches!(status, "leased" | "running") || terminal)
                && row.terminal_state.is_none()
                && row.terminal_expected_hold_revision.is_none()
                && row.hold.tombstone
                && !row.hold.active_count_charged
        }
        // Released is a terminal retained state in the durable vocabulary; it
        // must carry the same exact outcome mapping as CleanupPending.
        DevelopmentLaneHoldState::Released => {
            terminal
                && terminal_state_matches
                && row.terminal_expected_hold_revision.is_some()
                && row.hold.tombstone
                && !row.hold.active_count_charged
                && row.hold.retained_disk_bytes != 0
        }
        // Cleanup has released the retained charge, but the lifecycle terminal
        // outcome and replay revision remain bound to the tombstone.
        DevelopmentLaneHoldState::Cleaned => {
            terminal
                && terminal_state_matches
                && row.terminal_expected_hold_revision.is_some()
                && row.hold.tombstone
                && !row.hold.active_count_charged
                && row.hold.retained_disk_bytes == 0
        }
        DevelopmentLaneHoldState::Aborted | DevelopmentLaneHoldState::Absent => true,
    }
}

/// Validate persisted hold identifiers before they can be used as a table
/// lookup key or scope component.  Native rows are encrypted, but encryption
/// authenticates bytes; it does not make a corrupt/old row safe to feed into
/// redb or a pressure index.  Reconciliation and graph lifecycle therefore
/// fail closed on the same bounded vocabulary as fresh requests.
fn durable_hold_bounds(row: &DurableLaneHold) -> Result<(), String> {
    bounded_texts(&[
        (&row.hold.hold_id, "stored hold"),
        (&row.hold.lane_id, "stored lane"),
        (&row.hold.tenant_ref, "stored tenant"),
        (&row.hold.request_id, "stored request"),
        (&row.hold.work_item_id, "stored WorkItem"),
        (&row.hold.owner_id, "stored owner"),
        (&row.hold.session_id, "stored session"),
        (&row.hold.fairness_group, "stored fairness group"),
        (&row.hold.workspace_ref, "stored workspace"),
        (&row.hold.repository_id, "stored repository"),
        (&row.hold.base_ref, "stored base ref"),
        (&row.hold.branch, "stored branch"),
        (&row.hold.host_ref, "stored host"),
        (&row.resource_reservation_id, "stored resource reservation"),
        (&row.hold.work_item_fence, "stored WorkItem fence"),
        (&row.hold.quota_policy_name, "stored policy name"),
        (&row.hold.quota_policy_version, "stored policy version"),
    ])
    .map_err(|decision| format!("stored development lane hold: {}", decision_name(decision)))?;
    fingerprint(&row.hold.hold_id)
        .map_err(|decision| format!("stored development lane hold: {}", decision_name(decision)))?;
    base_sha(&row.hold.base_sha)
        .map_err(|decision| format!("stored development lane hold: {}", decision_name(decision)))?;
    relative_locator(&row.hold.worktree_locator)
        .map_err(|decision| format!("stored development lane hold: {}", decision_name(decision)))?;
    fingerprint(&row.hold.input_fingerprint)
        .map_err(|decision| format!("stored development lane hold: {}", decision_name(decision)))?;
    if let Some(alias) = row.hold.host_target_alias.as_deref() {
        text(alias, "stored host alias").map_err(|decision| {
            format!("stored development lane hold: {}", decision_name(decision))
        })?;
    }
    if matches!(
        (
            row.hold.host_target_kind,
            row.hold.host_target_alias.is_some()
        ),
        (DevelopmentLaneHoldHostTargetKind::Local, true)
            | (DevelopmentLaneHoldHostTargetKind::InventoryAlias, false)
    ) {
        return Err("stored lane host target does not match its alias".to_string());
    }
    for (value, name) in [
        (row.hold.predicted_disk_bytes, "stored predicted disk"),
        (row.hold.observed_disk_bytes, "stored observed disk"),
        (row.hold.retained_disk_bytes, "stored retained disk"),
    ] {
        if value > MAX_DISK_BYTES {
            return Err(format!("{name} exceeds native bound"));
        }
    }
    if [
        row.hold.quota_charge.tenant_count,
        row.hold.quota_charge.owner_count,
        row.hold.quota_charge.session_count,
        row.hold.quota_charge.workspace_count,
        row.hold.quota_charge.repository_count,
        row.hold.quota_charge.host_count,
        row.hold.quota_charge.global_count,
    ]
    .into_iter()
    .any(|value| value > MAX_COUNT)
    {
        return Err("stored lane quota count exceeds native bound".to_string());
    }
    if [
        row.hold.quota_charge.tenant_predicted_disk_bytes,
        row.hold.quota_charge.owner_predicted_disk_bytes,
        row.hold.quota_charge.session_predicted_disk_bytes,
        row.hold.quota_charge.workspace_predicted_disk_bytes,
        row.hold.quota_charge.repository_predicted_disk_bytes,
        row.hold.quota_charge.host_predicted_disk_bytes,
        row.hold.quota_charge.global_predicted_disk_bytes,
        row.hold.quota_charge.tenant_observed_disk_bytes,
        row.hold.quota_charge.owner_observed_disk_bytes,
        row.hold.quota_charge.session_observed_disk_bytes,
        row.hold.quota_charge.workspace_observed_disk_bytes,
        row.hold.quota_charge.repository_observed_disk_bytes,
        row.hold.quota_charge.host_observed_disk_bytes,
        row.hold.quota_charge.global_observed_disk_bytes,
        row.hold.quota_charge.tenant_retained_disk_bytes,
        row.hold.quota_charge.owner_retained_disk_bytes,
        row.hold.quota_charge.session_retained_disk_bytes,
        row.hold.quota_charge.workspace_retained_disk_bytes,
        row.hold.quota_charge.repository_retained_disk_bytes,
        row.hold.quota_charge.host_retained_disk_bytes,
        row.hold.quota_charge.global_retained_disk_bytes,
    ]
    .into_iter()
    .any(|value| value > MAX_DISK_BYTES)
    {
        return Err("stored lane quota disk charge exceeds native bound".to_string());
    }
    if row.hold.quota_charge.revision > MAX_COUNT
        || row.hold.quota_charge.policy_revision > MAX_COUNT
    {
        return Err("stored lane quota revision exceeds native bound".to_string());
    }
    if row.ttl_ms == 0 || row.ttl_ms > MAX_TTL_MS {
        return Err("stored lane TTL exceeds native bound".to_string());
    }
    if row.observation_revision > MAX_COUNT
        || row.hold.attempt == 0
        || row.hold.attempt > MAX_COUNT
        || row.hold.lease_epoch == 0
        || row.hold.lease_epoch > MAX_COUNT
        || row.hold.fencing_token == 0
        || row.hold.fencing_token > MAX_COUNT
        || row.hold.hold_revision > MAX_COUNT
        || row.hold.lifecycle_revision > MAX_COUNT
        || row.hold.allocation_revision > MAX_COUNT
        || row.hold.cleanup_revision > MAX_COUNT
    {
        return Err("stored lane fence is invalid".to_string());
    }
    if row.hold.last_renewed_at_ms > row.hold.expires_at_ms
        || row
            .last_observed_at_ms
            .is_some_and(|observed_at| observed_at > row.hold.expires_at_ms)
    {
        return Err("stored lane observation timestamp is invalid".to_string());
    }
    if let Some(value) = row.terminal_state.as_deref() {
        text(value, "stored terminal state").map_err(|decision| {
            format!("stored development lane hold: {}", decision_name(decision))
        })?;
        if !matches!(value, "succeeded" | "failed" | "cancelled" | "dead_letter") {
            return Err("stored development lane terminal state is invalid".to_string());
        }
    }
    for (value, name) in [
        (
            row.hold.cleanup_work_item_id.as_deref(),
            "stored cleanup WorkItem",
        ),
        (
            row.hold.cleanup_work_item_fence.as_deref(),
            "stored cleanup fence",
        ),
    ] {
        if let Some(value) = value {
            text(value, name).map_err(|decision| {
                format!("stored development lane hold: {}", decision_name(decision))
            })?;
        }
    }
    for (value, name) in [
        (row.hold.cleanup_attempt, "stored cleanup attempt"),
        (row.hold.cleanup_lease_epoch, "stored cleanup lease epoch"),
        (
            row.hold.cleanup_fencing_token,
            "stored cleanup fencing token",
        ),
        (
            row.terminal_expected_hold_revision,
            "stored terminal hold revision",
        ),
        (
            row.cleanup_expected_hold_revision,
            "stored cleanup hold revision",
        ),
        (
            row.terminal_source_attempt,
            "stored terminal source attempt",
        ),
        (
            row.terminal_source_lease_epoch,
            "stored terminal source lease epoch",
        ),
        (
            row.terminal_source_fencing_token,
            "stored terminal source fencing token",
        ),
    ] {
        if let Some(value) = value {
            if value == 0 || value > MAX_COUNT {
                return Err(format!("{name} exceeds native bound"));
            }
        }
    }
    if let Some(value) = row.cleanup_removal_proof_ref.as_deref() {
        text(value, "stored cleanup removal proof").map_err(|decision| {
            format!("stored development lane hold: {}", decision_name(decision))
        })?;
    }
    if let Some(value) = row.terminal_source_work_item_fence.as_deref() {
        text(value, "stored terminal source WorkItem fence").map_err(|decision| {
            format!("stored development lane hold: {}", decision_name(decision))
        })?;
    }
    let terminal_source_fields = [
        row.terminal_source_attempt.is_some(),
        row.terminal_source_lease_epoch.is_some(),
        row.terminal_source_fencing_token.is_some(),
        row.terminal_source_work_item_fence.is_some(),
    ];
    if terminal_source_fields.iter().any(|present| *present)
        && terminal_source_fields.iter().any(|present| !*present)
    {
        return Err("stored terminal source tuple is incomplete".to_string());
    }
    Ok(())
}

fn text(value: &str, name: &str) -> Result<(), LaneDecision> {
    if value.is_empty()
        || value.len() > MAX_TEXT
        || value
            .as_bytes()
            .iter()
            .any(|byte| *byte == 0 || *byte < 0x20)
    {
        return Err(LaneDecision::Invalid);
    }
    let _ = name;
    Ok(())
}

fn fingerprint(value: &str) -> Result<(), LaneDecision> {
    if value.len() != MAX_FINGERPRINT
        || !value.starts_with("v1:")
        || !value.as_bytes()[3..].iter().all(u8::is_ascii_hexdigit)
        || value.as_bytes()[3..].iter().any(u8::is_ascii_uppercase)
    {
        return Err(LaneDecision::Invalid);
    }
    Ok(())
}

fn base_sha(value: &str) -> Result<(), LaneDecision> {
    if !matches!(value.len(), 40 | 64)
        || !value.as_bytes().iter().all(u8::is_ascii_hexdigit)
        || value.as_bytes().iter().any(u8::is_ascii_uppercase)
    {
        return Err(LaneDecision::Invalid);
    }
    Ok(())
}

fn relative_locator(value: &str) -> Result<(), LaneDecision> {
    text(value, "worktree_locator")?;
    if value.starts_with('/')
        || value.starts_with('\\')
        || value.contains('\\')
        || value
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(LaneDecision::Invalid);
    }
    Ok(())
}

fn intent_validate(intent: &DevelopmentLaneIntent) -> Result<(), LaneDecision> {
    text(&intent.tenant_ref, "intent tenant")?;
    text(&intent.request_id, "intent request")?;
    text(&intent.lane_id, "intent lane")?;
    text(&intent.repository_id, "intent repository")?;
    text(&intent.base_ref, "intent base_ref")?;
    base_sha(&intent.base_sha)?;
    text(&intent.branch, "intent branch")?;
    text(&intent.workspace_ref, "intent workspace")?;
    relative_locator(&intent.worktree_locator)?;
    text(&intent.owner_id, "intent owner")?;
    text(&intent.session_id, "intent session")?;
    text(&intent.fairness_group, "intent fairness")?;
    text(&intent.quota_policy_name, "intent policy")?;
    text(&intent.quota_policy_version, "intent policy version")?;
    fingerprint(&intent.input_fingerprint)?;
    if intent.predicted_disk_bytes == 0
        || intent.predicted_disk_bytes > MAX_DISK_BYTES
        || intent.ttl_ms == 0
        || intent.ttl_ms > MAX_TTL_MS
    {
        return Err(LaneDecision::Invalid);
    }
    match intent.host_target_kind {
        DevelopmentLaneIntentHostTargetKind::Local if intent.host_target_alias.is_some() => {
            return Err(LaneDecision::Invalid);
        }
        DevelopmentLaneIntentHostTargetKind::InventoryAlias
            if intent.host_target_alias.is_none() =>
        {
            return Err(LaneDecision::Invalid)
        }
        _ => {}
    }
    if let Some(alias) = intent.host_target_alias.as_deref() {
        text(alias, "intent host alias")?;
    }
    text(&intent.host_ref, "intent host ref")?;
    text(
        &intent.resource_reservation_id,
        "intent resource reservation id",
    )?;
    Ok(())
}

fn repository_work_item_extension<'a>(
    props: &'a serde_json::Map<String, serde_json::Value>,
) -> Result<&'a serde_json::Map<String, serde_json::Value>, LaneDecision> {
    props
        .get(WORK_ITEM_METADATA_KEY)
        .and_then(serde_json::Value::as_object)
        .and_then(|metadata| metadata.get(REPOSITORY_WORK_ITEM_EXTENSION_KEY))
        .and_then(serde_json::Value::as_object)
        .ok_or(LaneDecision::InputConflict)
}

fn lane_intent_value(
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<DevelopmentLaneIntent, LaneDecision> {
    let extension = repository_work_item_extension(props)?;
    let value = extension
        .get(LANE_INTENT_EXTENSION_KEY)
        .cloned()
        .ok_or(LaneDecision::InputConflict)?;
    serde_json::from_value(value).map_err(|_| LaneDecision::InputConflict)
}

fn lane_cleanup_value(
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<DevelopmentLaneCleanupIntent, LaneDecision> {
    let extension = repository_work_item_extension(props)?;
    let value = extension
        .get(LANE_CLEANUP_EXTENSION_KEY)
        .cloned()
        .ok_or(LaneDecision::InputConflict)?;
    let correlation: DevelopmentLaneCleanupIntent =
        serde_json::from_value(value).map_err(|_| LaneDecision::InputConflict)?;
    if !matches!(
        correlation.schema_version,
        DevelopmentLaneCleanupIntentSchemaVersion::V1
    ) || correlation.hold_id.is_empty()
        || correlation.lane_id.is_empty()
        || correlation.expected_hold_revision == 0
    {
        return Err(LaneDecision::InputConflict);
    }
    fingerprint(&correlation.hold_id)?;
    text(&correlation.lane_id, "cleanup lane id")?;
    Ok(correlation)
}

fn load_work_item(
    nodes: &redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    work_item_id: &str,
    tenant: &str,
    owner: Option<&str>,
    attempt: u64,
    lease_epoch: u64,
    fencing_token: u64,
    work_item_fence: &str,
    expected_kind: DevelopmentLaneWorkItemKind,
    require_terminal: bool,
    now_ms: u64,
    crypto: DurableCrypto<'_>,
    expected_intent: Option<&DevelopmentLaneIntent>,
    expected_cleanup: Option<(&str, &str, u64)>,
) -> Result<LaneWorkItem, LaneDecision> {
    let bytes = nodes
        .get((graph, work_item_id))
        .map_err(|_| LaneDecision::Invalid)?
        .map(|value| {
            crypto
                .unseal(value.value())
                .map_err(|_| LaneDecision::Invalid)
        })
        .transpose()?
        .ok_or(LaneDecision::NotFound)?;
    let props: serde_json::Map<String, serde_json::Value> =
        decode_durable(&bytes).map_err(|_| LaneDecision::Invalid)?;
    if super::property_string(&props, "node_type") != "WorkItem" {
        return Err(LaneDecision::NotFound);
    }
    if super::property_string(&props, "tenant") != tenant {
        return Err(LaneDecision::WrongTenant);
    }
    // `kind` and `work_item_fence` are the only frozen WorkItem projection
    // fields.  Do not search generic aliases or nested metadata: an echoed
    // `work_item_kind`/`fence` must never become an authority claim.
    if super::property_string(&props, "kind") != work_item_kind_name(expected_kind) {
        return Err(LaneDecision::WrongKind);
    }
    if attempt == 0 || lease_epoch == 0 || fencing_token == 0 || work_item_fence.is_empty() {
        return Err(LaneDecision::Invalid);
    }
    let current_attempt = super::property_u64(&props, "attempt");
    if current_attempt != attempt {
        return Err(LaneDecision::WrongAttempt);
    }
    if super::property_u64(&props, "lease_epoch") != lease_epoch {
        return Err(LaneDecision::WrongLeaseEpoch);
    }
    if super::property_u64(&props, "fencing_token") != fencing_token {
        return Err(LaneDecision::WrongFence);
    }
    if super::property_string(&props, "work_item_fence") != work_item_fence {
        return Err(LaneDecision::WrongFence);
    }
    let status = super::property_string(&props, "status").to_string();
    let terminal = matches!(
        status.as_str(),
        "succeeded" | "failed" | "cancelled" | "dead_letter"
    );
    if require_terminal != terminal {
        return Err(if require_terminal {
            LaneDecision::Terminal
        } else {
            LaneDecision::Terminal
        });
    }
    if !terminal && !matches!(status.as_str(), "leased" | "running") {
        // A future lease timestamp on a ready/pending row is not a claim.  A
        // lane lifecycle or cleanup action must be tied to the WorkItem's
        // current live claim, not merely to an echoed fence tuple.
        return Err(LaneDecision::WrongOwner);
    }
    let lease_expires_at_ms = (super::property_f64(&props, "lease_expires_at") * 1_000.0)
        .max(0.0)
        .min(u64::MAX as f64) as u64;
    if !terminal && lease_expires_at_ms <= now_ms {
        return Err(LaneDecision::Expired);
    }
    if let Some(owner) = owner {
        let current_owner = if terminal || !matches!(status.as_str(), "leased" | "running") {
            super::property_string(&props, "last_lease_owner")
        } else {
            super::property_string(&props, "lease_owner")
        };
        if current_owner != owner {
            return Err(LaneDecision::WrongOwner);
        }
    }
    let (host_ref, resource_reservation_id, cleanup, lane_intent) = match expected_kind {
        DevelopmentLaneWorkItemKind::Lifecycle => {
            let stored = lane_intent_value(&props)?;
            if stored.tenant_ref != tenant {
                return Err(LaneDecision::WrongTenant);
            }
            if owner.is_some_and(|expected| stored.owner_id != expected) {
                return Err(LaneDecision::WrongOwner);
            }
            if let Some(expected_intent) = expected_intent {
                if stored != *expected_intent {
                    return Err(LaneDecision::InputConflict);
                }
            }
            (
                stored.host_ref.clone(),
                stored.resource_reservation_id.clone(),
                None,
                Some(stored),
            )
        }
        DevelopmentLaneWorkItemKind::Cleanup => {
            let correlation = lane_cleanup_value(&props)?;
            if let Some((hold_id, lane_id, expected_revision)) = expected_cleanup {
                if correlation.hold_id != hold_id
                    || correlation.lane_id != lane_id
                    || correlation.expected_hold_revision != expected_revision
                {
                    return Err(LaneDecision::InputConflict);
                }
            }
            (String::new(), String::new(), Some(correlation), None)
        }
    };
    Ok(LaneWorkItem {
        status,
        terminal,
        lease_expires_at_ms,
        host_ref,
        resource_reservation_id,
        cleanup_hold_id: cleanup.as_ref().map(|value| value.hold_id.clone()),
        cleanup_lane_id: cleanup.as_ref().map(|value| value.lane_id.clone()),
        cleanup_expected_hold_revision: cleanup.as_ref().map(|value| value.expected_hold_revision),
        lane_intent,
    })
}

fn scope_key(scope: Scope, hold: &DevelopmentLaneHold) -> String {
    let (name, value) = match scope {
        Scope::Tenant => ("tenant", hold.tenant_ref.as_str()),
        Scope::Owner => ("owner", hold.owner_id.as_str()),
        Scope::Session => ("session", hold.session_id.as_str()),
        Scope::Workspace => ("workspace", hold.workspace_ref.as_str()),
        Scope::Repository => ("repository", hold.repository_id.as_str()),
        Scope::Host => ("host", hold.host_ref.as_str()),
        // This is intentionally the one graph-wide key. Tenant is not part
        // of the global namespace; its policy revision is tracked separately.
        Scope::Global => return "global\0*".to_string(),
    };
    format!("{name}\0{}\0{value}", hold.tenant_ref)
}

fn scopes(hold: &DevelopmentLaneHold) -> Vec<(Scope, String)> {
    [
        Scope::Tenant,
        Scope::Owner,
        Scope::Session,
        Scope::Workspace,
        Scope::Repository,
        Scope::Host,
        Scope::Global,
    ]
    .into_iter()
    .map(|scope| (scope, scope_key(scope, hold)))
    .collect()
}

fn policy_limit(policy: &DevelopmentLaneQuotaPolicy, scope: Scope, metric: Metric) -> u64 {
    match (scope, metric) {
        (Scope::Tenant, Metric::Count) => policy.tenant_count_limit,
        (Scope::Owner, Metric::Count) => policy.owner_count_limit,
        (Scope::Session, Metric::Count) => policy.session_count_limit,
        (Scope::Workspace, Metric::Count) => policy.workspace_count_limit,
        (Scope::Repository, Metric::Count) => policy.repository_count_limit,
        (Scope::Host, Metric::Count) => policy.host_count_limit,
        (Scope::Global, Metric::Count) => policy.global_count_limit,
        (Scope::Tenant, Metric::Predicted) => policy.tenant_predicted_disk_bytes,
        (Scope::Owner, Metric::Predicted) => policy.owner_predicted_disk_bytes,
        (Scope::Session, Metric::Predicted) => policy.session_predicted_disk_bytes,
        (Scope::Workspace, Metric::Predicted) => policy.workspace_predicted_disk_bytes,
        (Scope::Repository, Metric::Predicted) => policy.repository_predicted_disk_bytes,
        (Scope::Host, Metric::Predicted) => policy.host_predicted_disk_bytes,
        (Scope::Global, Metric::Predicted) => policy.global_predicted_disk_bytes,
        (Scope::Tenant, Metric::Observed) => policy.tenant_observed_disk_bytes,
        (Scope::Owner, Metric::Observed) => policy.owner_observed_disk_bytes,
        (Scope::Session, Metric::Observed) => policy.session_observed_disk_bytes,
        (Scope::Workspace, Metric::Observed) => policy.workspace_observed_disk_bytes,
        (Scope::Repository, Metric::Observed) => policy.repository_observed_disk_bytes,
        (Scope::Host, Metric::Observed) => policy.host_observed_disk_bytes,
        (Scope::Global, Metric::Observed) => policy.global_observed_disk_bytes,
        (Scope::Tenant, Metric::Retained) => policy.tenant_retained_disk_bytes,
        (Scope::Owner, Metric::Retained) => policy.owner_retained_disk_bytes,
        (Scope::Session, Metric::Retained) => policy.session_retained_disk_bytes,
        (Scope::Workspace, Metric::Retained) => policy.workspace_retained_disk_bytes,
        (Scope::Repository, Metric::Retained) => policy.repository_retained_disk_bytes,
        (Scope::Host, Metric::Retained) => policy.host_retained_disk_bytes,
        (Scope::Global, Metric::Retained) => policy.global_retained_disk_bytes,
    }
}

#[derive(Debug, Clone, Copy)]
enum Metric {
    Count,
    Predicted,
    Observed,
    Retained,
}

fn durable_policy_bounds(row: &DurableLanePolicy) -> Result<(), String> {
    policy_validate(&row.policy).map_err(|decision| {
        format!(
            "stored development lane policy: {}",
            decision_name(decision)
        )
    })?;
    if row.policy_revision == 0
        || row.policy_revision > MAX_COUNT
        || row.global_policy_revision == 0
        || row.global_policy_revision > MAX_COUNT
    {
        return Err("stored development lane policy revision is invalid".to_string());
    }
    Ok(())
}

fn durable_counter_bounds(value: &DurableLaneCounter) -> Result<(), String> {
    if value.active_count > MAX_COUNT
        || value.predicted_disk_bytes > MAX_DISK_BYTES
        || value.observed_disk_bytes > MAX_DISK_BYTES
        || value.retained_disk_bytes > MAX_DISK_BYTES
        || value.revision > MAX_COUNT
        || value.policy_revision > MAX_COUNT
        || value.global_policy_revision > MAX_COUNT
    {
        return Err("stored development lane counter exceeds native bounds".to_string());
    }
    Ok(())
}

fn load_policy<T>(
    policies: &T,
    graph: &str,
    tenant: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableLanePolicy>, String>
where
    T: ReadableTable<(&'static str, &'static str), &'static [u8]>,
{
    policies
        .get((graph, tenant))
        .map_err(|e| e.to_string())?
        .map(|row| {
            let decoded: DurableLanePolicy = resource_decode(row.value(), crypto)?;
            durable_policy_bounds(&decoded)?;
            Ok::<DurableLanePolicy, String>(decoded)
        })
        .transpose()
}

fn load_global_policy<T>(
    policies: &T,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableLanePolicy>, String>
where
    T: ReadableTable<(&'static str, &'static str), &'static [u8]>,
{
    load_policy(policies, graph, GLOBAL_POLICY_KEY, crypto)
}

/// The global counter is a single graph-wide authority. Tenant policies may
/// differ in owner/session/workspace/repository/host limits, but every policy
/// must agree on the dimensions that govern the shared counter and freshness
/// gate. A typed quota update using `GLOBAL_POLICY_KEY` is the administrator
/// CAS route for changing those graph-wide controls.
fn global_policy_equal(
    left: &DevelopmentLaneQuotaPolicy,
    right: &DevelopmentLaneQuotaPolicy,
) -> bool {
    left.global_count_limit == right.global_count_limit
        && left.global_predicted_disk_bytes == right.global_predicted_disk_bytes
        && left.global_observed_disk_bytes == right.global_observed_disk_bytes
        && left.global_retained_disk_bytes == right.global_retained_disk_bytes
        && left.min_ttl_ms == right.min_ttl_ms
        && left.max_ttl_ms == right.max_ttl_ms
        && left.max_observation_staleness_ms == right.max_observation_staleness_ms
        && left.drain_only == right.drain_only
}

fn load_counter<T>(
    counters: &T,
    graph: &str,
    key: &str,
    scope: Scope,
    policy_revision: u64,
    global_policy_revision: u64,
    crypto: DurableCrypto<'_>,
) -> Result<DurableLaneCounter, String>
where
    T: ReadableTable<(&'static str, &'static str), &'static [u8]>,
{
    let value: DurableLaneCounter = counters
        .get((graph, key))
        .map_err(|e| e.to_string())?
        .map(|row| {
            let decoded: DurableLaneCounter = resource_decode(row.value(), crypto)?;
            durable_counter_bounds(&decoded)?;
            Ok::<DurableLaneCounter, String>(decoded)
        })
        .transpose()?
        .unwrap_or_default();
    if scope == Scope::Global {
        if value.global_policy_revision > global_policy_revision {
            return Err(
                "development lane global counter has a future global policy revision".to_string(),
            );
        }
    } else if value.policy_revision > policy_revision {
        return Err("development lane counter has a future policy revision".to_string());
    }
    Ok(value)
}

fn put_counter(
    counters: &mut redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    key: &str,
    value: &DurableLaneCounter,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let bytes = resource_encode(value, crypto)?;
    counters
        .insert((graph, key), bytes.as_slice())
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn load_scope_counters<T>(
    counters: &T,
    graph: &str,
    hold: &DevelopmentLaneHold,
    policy_revision: u64,
    global_policy_revision: u64,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<ScopeCounter>, String>
where
    T: ReadableTable<(&'static str, &'static str), &'static [u8]>,
{
    scopes(hold)
        .into_iter()
        .map(|(scope, key)| {
            Ok(ScopeCounter {
                value: load_counter(
                    counters,
                    graph,
                    &key,
                    scope,
                    policy_revision,
                    global_policy_revision,
                    crypto,
                )?,
                key,
                scope,
            })
        })
        .collect()
}

fn adjust(value: u64, amount: u64, increase: bool, name: &str) -> Result<u64, String> {
    if increase {
        value
            .checked_add(amount)
            .ok_or_else(|| format!("development lane {name} counter overflow"))
    } else {
        value
            .checked_sub(amount)
            .ok_or_else(|| format!("development lane {name} counter underflow"))
    }
}

fn pressure_scope_name(scope: Scope) -> Option<&'static str> {
    match scope {
        Scope::Owner => Some("owner"),
        Scope::Session => Some("session"),
        Scope::Workspace => Some("workspace"),
        Scope::Repository => Some("repository"),
        Scope::Host => Some("host"),
        Scope::Tenant | Scope::Global => None,
    }
}

fn pressure_metric_name(metric: Metric) -> &'static str {
    match metric {
        Metric::Count => "count",
        Metric::Predicted => "predicted",
        Metric::Observed => "observed",
        Metric::Retained => "retained",
    }
}

fn pressure_replace(
    pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    graph: &str,
    tenant: &str,
    scope: Scope,
    metric: Metric,
    old: u64,
    new: u64,
    counter_key: &str,
) -> Result<(), String> {
    let Some(scope_name) = pressure_scope_name(scope) else {
        return Ok(());
    };
    let metric_name = pressure_metric_name(metric);
    if old > 0 {
        pressure_index
            .remove((graph, tenant, scope_name, metric_name, old, counter_key))
            .map_err(|e| e.to_string())?;
    }
    if new > 0 {
        pressure_index
            .insert(
                (graph, tenant, scope_name, metric_name, new, counter_key),
                0,
            )
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn apply_counter_delta(
    counters: &mut redb::Table<(&str, &str), &[u8]>,
    pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    graph: &str,
    tenant: &str,
    mut loaded: Vec<ScopeCounter>,
    policy_revision: u64,
    active_add: Option<bool>,
    predicted: Option<(bool, u64)>,
    observed: Option<(bool, u64)>,
    retained: Option<(bool, u64)>,
    global_policy_revision: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for row in &mut loaded {
        let old_active = row.value.active_count;
        let old_predicted = row.value.predicted_disk_bytes;
        let old_observed = row.value.observed_disk_bytes;
        let old_retained = row.value.retained_disk_bytes;
        if let Some(increase) = active_add {
            row.value.active_count = adjust(row.value.active_count, 1, increase, "active_count")?;
        }
        if let Some((increase, amount)) = predicted {
            row.value.predicted_disk_bytes = adjust(
                row.value.predicted_disk_bytes,
                amount,
                increase,
                "predicted_disk_bytes",
            )?;
        }
        if let Some((increase, amount)) = observed {
            row.value.observed_disk_bytes = adjust(
                row.value.observed_disk_bytes,
                amount,
                increase,
                "observed_disk_bytes",
            )?;
        }
        if let Some((increase, amount)) = retained {
            row.value.retained_disk_bytes = adjust(
                row.value.retained_disk_bytes,
                amount,
                increase,
                "retained_disk_bytes",
            )?;
        }
        row.value.revision = row
            .value
            .revision
            .checked_add(1)
            .ok_or_else(|| "development lane counter revision overflow".to_string())?;
        if row.scope == Scope::Global {
            row.value.global_policy_revision = global_policy_revision;
        } else {
            row.value.policy_revision = policy_revision;
        }
        pressure_replace(
            pressure_index,
            graph,
            tenant,
            row.scope,
            Metric::Count,
            old_active,
            row.value.active_count,
            &row.key,
        )?;
        pressure_replace(
            pressure_index,
            graph,
            tenant,
            row.scope,
            Metric::Predicted,
            old_predicted,
            row.value.predicted_disk_bytes,
            &row.key,
        )?;
        pressure_replace(
            pressure_index,
            graph,
            tenant,
            row.scope,
            Metric::Observed,
            old_observed,
            row.value.observed_disk_bytes,
            &row.key,
        )?;
        pressure_replace(
            pressure_index,
            graph,
            tenant,
            row.scope,
            Metric::Retained,
            old_retained,
            row.value.retained_disk_bytes,
            &row.key,
        )?;
    }
    for row in &loaded {
        put_counter(counters, graph, &row.key, &row.value, crypto)?;
    }
    Ok(())
}

fn hold_charge(
    hold: &DevelopmentLaneHold,
    revision: u64,
    policy_revision: u64,
) -> DevelopmentLaneQuotaCharge {
    DevelopmentLaneQuotaCharge {
        schema_version: DevelopmentLaneQuotaChargeSchemaVersion::V1,
        tenant_count: u64::from(hold.active_count_charged),
        owner_count: u64::from(hold.active_count_charged),
        session_count: u64::from(hold.active_count_charged),
        workspace_count: u64::from(hold.active_count_charged),
        repository_count: u64::from(hold.active_count_charged),
        host_count: u64::from(hold.active_count_charged),
        global_count: u64::from(hold.active_count_charged),
        tenant_predicted_disk_bytes: if hold.active_count_charged {
            hold.predicted_disk_bytes
        } else {
            0
        },
        owner_predicted_disk_bytes: if hold.active_count_charged {
            hold.predicted_disk_bytes
        } else {
            0
        },
        session_predicted_disk_bytes: if hold.active_count_charged {
            hold.predicted_disk_bytes
        } else {
            0
        },
        workspace_predicted_disk_bytes: if hold.active_count_charged {
            hold.predicted_disk_bytes
        } else {
            0
        },
        repository_predicted_disk_bytes: if hold.active_count_charged {
            hold.predicted_disk_bytes
        } else {
            0
        },
        host_predicted_disk_bytes: if hold.active_count_charged {
            hold.predicted_disk_bytes
        } else {
            0
        },
        global_predicted_disk_bytes: if hold.active_count_charged {
            hold.predicted_disk_bytes
        } else {
            0
        },
        tenant_observed_disk_bytes: if hold.active_count_charged {
            hold.observed_disk_bytes
        } else {
            0
        },
        owner_observed_disk_bytes: if hold.active_count_charged {
            hold.observed_disk_bytes
        } else {
            0
        },
        session_observed_disk_bytes: if hold.active_count_charged {
            hold.observed_disk_bytes
        } else {
            0
        },
        workspace_observed_disk_bytes: if hold.active_count_charged {
            hold.observed_disk_bytes
        } else {
            0
        },
        repository_observed_disk_bytes: if hold.active_count_charged {
            hold.observed_disk_bytes
        } else {
            0
        },
        host_observed_disk_bytes: if hold.active_count_charged {
            hold.observed_disk_bytes
        } else {
            0
        },
        global_observed_disk_bytes: if hold.active_count_charged {
            hold.observed_disk_bytes
        } else {
            0
        },
        tenant_retained_disk_bytes: hold.retained_disk_bytes,
        owner_retained_disk_bytes: hold.retained_disk_bytes,
        session_retained_disk_bytes: hold.retained_disk_bytes,
        workspace_retained_disk_bytes: hold.retained_disk_bytes,
        repository_retained_disk_bytes: hold.retained_disk_bytes,
        host_retained_disk_bytes: hold.retained_disk_bytes,
        global_retained_disk_bytes: hold.retained_disk_bytes,
        revision,
        policy_revision,
    }
}

fn empty_charge(policy_revision: u64) -> DevelopmentLaneQuotaCharge {
    DevelopmentLaneQuotaCharge {
        schema_version: DevelopmentLaneQuotaChargeSchemaVersion::V1,
        tenant_count: 0,
        owner_count: 0,
        session_count: 0,
        workspace_count: 0,
        repository_count: 0,
        host_count: 0,
        global_count: 0,
        tenant_predicted_disk_bytes: 0,
        owner_predicted_disk_bytes: 0,
        session_predicted_disk_bytes: 0,
        workspace_predicted_disk_bytes: 0,
        repository_predicted_disk_bytes: 0,
        host_predicted_disk_bytes: 0,
        global_predicted_disk_bytes: 0,
        tenant_observed_disk_bytes: 0,
        owner_observed_disk_bytes: 0,
        session_observed_disk_bytes: 0,
        workspace_observed_disk_bytes: 0,
        repository_observed_disk_bytes: 0,
        host_observed_disk_bytes: 0,
        global_observed_disk_bytes: 0,
        tenant_retained_disk_bytes: 0,
        owner_retained_disk_bytes: 0,
        session_retained_disk_bytes: 0,
        workspace_retained_disk_bytes: 0,
        repository_retained_disk_bytes: 0,
        host_retained_disk_bytes: 0,
        global_retained_disk_bytes: 0,
        revision: 0,
        policy_revision,
    }
}

fn snapshot_charge(
    tenant: &DurableLaneCounter,
    global: &DurableLaneCounter,
    policy_revision: u64,
) -> DevelopmentLaneQuotaCharge {
    DevelopmentLaneQuotaCharge {
        schema_version: DevelopmentLaneQuotaChargeSchemaVersion::V1,
        tenant_count: tenant.active_count,
        owner_count: 0,
        session_count: 0,
        workspace_count: 0,
        repository_count: 0,
        host_count: 0,
        global_count: global.active_count,
        tenant_predicted_disk_bytes: tenant.predicted_disk_bytes,
        owner_predicted_disk_bytes: 0,
        session_predicted_disk_bytes: 0,
        workspace_predicted_disk_bytes: 0,
        repository_predicted_disk_bytes: 0,
        host_predicted_disk_bytes: 0,
        global_predicted_disk_bytes: global.predicted_disk_bytes,
        tenant_observed_disk_bytes: tenant.observed_disk_bytes,
        owner_observed_disk_bytes: 0,
        session_observed_disk_bytes: 0,
        workspace_observed_disk_bytes: 0,
        repository_observed_disk_bytes: 0,
        host_observed_disk_bytes: 0,
        global_observed_disk_bytes: global.observed_disk_bytes,
        tenant_retained_disk_bytes: tenant.retained_disk_bytes,
        owner_retained_disk_bytes: 0,
        session_retained_disk_bytes: 0,
        workspace_retained_disk_bytes: 0,
        repository_retained_disk_bytes: 0,
        host_retained_disk_bytes: 0,
        global_retained_disk_bytes: global.retained_disk_bytes,
        revision: tenant.revision.max(global.revision),
        policy_revision,
    }
}

fn branch_key(hold: &DevelopmentLaneHold) -> String {
    format!("{}\0{}", hold.repository_id, hold.branch)
}

fn worktree_key(hold: &DevelopmentLaneHold) -> String {
    format!(
        "{}\0{}\0{}",
        hold.host_ref, hold.workspace_ref, hold.worktree_locator
    )
}

fn hold_id(intent: &DevelopmentLaneIntent) -> String {
    let mut hasher = Sha256::new();
    // Request identity, not a reusable lane name, is the durable tombstone
    // identity.  This lets a cleaned lane be allocated by a new request while
    // ensuring that reusing one request ID with changed immutable input hits
    // the retained row and returns input_conflict.
    hasher.update(b"development-lane-hold-v1\0");
    hasher.update(intent.tenant_ref.as_bytes());
    hasher.update([0]);
    hasher.update(intent.request_id.as_bytes());
    format!("v1:{}", hex::encode(hasher.finalize()))
}

fn hold_immutable_equal(row: &DurableLaneHold, request: &DevelopmentLaneReserveRequest) -> bool {
    let hold = &row.hold;
    hold.tenant_ref == request.tenant_ref
        && hold.work_item_id == request.work_item_id
        && hold.owner_id == request.owner_id
        && hold.attempt == request.attempt
        && hold.lease_epoch == request.lease_epoch
        && hold.fencing_token == request.fencing_token
        && hold.work_item_fence == request.work_item_fence
        && hold.lane_id == request.intent.lane_id
        && hold.request_id == request.intent.request_id
        && hold.repository_id == request.intent.repository_id
        && hold.base_ref == request.intent.base_ref
        && hold.base_sha == request.intent.base_sha
        && hold.branch == request.intent.branch
        && hold.workspace_ref == request.intent.workspace_ref
        && hold.worktree_locator == request.intent.worktree_locator
        && hold.host_ref == request.intent.host_ref
        && hold.host_target_kind == hold_target_kind(request.intent.host_target_kind)
        && hold.host_target_alias == request.intent.host_target_alias
        && row.resource_reservation_id == request.intent.resource_reservation_id
        && row.ttl_ms == request.intent.ttl_ms
        && hold.session_id == request.intent.session_id
        && hold.fairness_group == request.intent.fairness_group
        && hold.quota_policy_name == request.intent.quota_policy_name
        && hold.quota_policy_version == request.intent.quota_policy_version
        && hold.predicted_disk_bytes == request.intent.predicted_disk_bytes
        && hold.input_fingerprint == request.intent.input_fingerprint
}

fn lane_intent_matches_hold(
    intent: Option<&DevelopmentLaneIntent>,
    hold: &DevelopmentLaneHold,
    ttl_ms: u64,
    resource_reservation_id: &str,
) -> bool {
    let Some(intent) = intent else {
        return false;
    };
    intent.tenant_ref == hold.tenant_ref
        && intent.request_id == hold.request_id
        && intent.lane_id == hold.lane_id
        && intent.repository_id == hold.repository_id
        && intent.base_ref == hold.base_ref
        && intent.base_sha == hold.base_sha
        && intent.branch == hold.branch
        && hold_target_kind(intent.host_target_kind) == hold.host_target_kind
        && intent.host_target_alias == hold.host_target_alias
        && intent.host_ref == hold.host_ref
        && intent.workspace_ref == hold.workspace_ref
        && intent.worktree_locator == hold.worktree_locator
        && intent.owner_id == hold.owner_id
        && intent.session_id == hold.session_id
        && intent.fairness_group == hold.fairness_group
        && intent.quota_policy_name == hold.quota_policy_name
        && intent.quota_policy_version == hold.quota_policy_version
        && intent.predicted_disk_bytes == hold.predicted_disk_bytes
        && intent.ttl_ms == ttl_ms
        && intent.input_fingerprint == hold.input_fingerprint
        && intent.resource_reservation_id == resource_reservation_id
}

fn hold_encode(
    holds: &mut redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    row: &DurableLaneHold,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    durable_hold_bounds(row)?;
    let bytes = resource_encode(row, crypto)?;
    holds
        .insert((graph, row.hold.hold_id.as_str()), bytes.as_slice())
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn hold_load<T>(
    holds: &T,
    graph: &str,
    hold_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableLaneHold>, String>
where
    T: ReadableTable<(&'static str, &'static str), &'static [u8]>,
{
    holds
        .get((graph, hold_id))
        .map_err(|e| e.to_string())?
        .map(|row| {
            let decoded: DurableLaneHold = resource_decode(row.value(), crypto)?;
            durable_hold_bounds(&decoded)?;
            Ok(decoded)
        })
        .transpose()
}

fn idempotency_key(method: &Method) -> Option<(&str, &str)> {
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

fn method_name(method: &Method) -> &'static str {
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

fn normalize_now(method: &Method, now_ms: u64) -> Option<Method> {
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

fn bounded_texts(values: &[(&str, &str)]) -> Result<(), LaneDecision> {
    for (value, name) in values {
        text(value, name)?;
    }
    Ok(())
}

/// Validate every caller-controlled key before opening a native table or
/// consulting an index.  The individual transaction functions repeat the
/// checks needed for their typed decision, but this early gate prevents an
/// oversized opaque key/fence from reaching redb at all.
fn validate_method_bounds(graph: &str, method: &Method) -> Result<(), LaneDecision> {
    text(graph, "lane graph")?;
    match method {
        Method::ReserveDevelopmentLane { request } => {
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
        }
        Method::RenewDevelopmentLane { request } => {
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
        }
        Method::ObserveDevelopmentLane { request } => {
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
        }
        Method::FinishDevelopmentLane { request } => {
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
        }
        Method::CleanupDevelopmentLane { request } => {
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
        }
        Method::UpdateDevelopmentLaneQuota { request } => {
            text(&request.tenant_ref, "quota tenant")?;
            text(&request.idempotency_key, "quota invocation")?;
            if let Some(version) = request.expected_policy_version.as_deref() {
                text(version, "quota expected policy version")?;
            }
            policy_validate(&request.policy)?;
        }
        Method::QueryDevelopmentLane { request } => {
            bounded_texts(&[
                (&request.tenant_ref, "query tenant"),
                (&request.hold_id, "query hold"),
            ])?;
        }
        Method::DevelopmentLaneStatus { request } => {
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
        }
        _ => {}
    }
    Ok(())
}

fn request_digest(method: &Method) -> Result<String, String> {
    let normalized = normalize_now(method, 0).ok_or_else(|| "not a lane method".to_string())?;
    let bytes = rmp_serde::to_vec_named(&normalized).map_err(|e| e.to_string())?;
    Ok(format!("v1:{}", hex::encode(Sha256::digest(bytes))))
}

fn decision_name(decision: LaneDecision) -> &'static str {
    match decision {
        LaneDecision::Accepted => "accepted",
        LaneDecision::Idempotent => "idempotent",
        LaneDecision::Stale => "stale",
        LaneDecision::Conflict => "conflict",
        LaneDecision::InputConflict => "input_conflict",
        LaneDecision::Quota => "quota",
        LaneDecision::Policy => "policy",
        LaneDecision::Drained => "drained",
        LaneDecision::NotFound => "not_found",
        LaneDecision::WrongKind => "wrong_kind",
        LaneDecision::WrongTenant => "wrong_tenant",
        LaneDecision::WrongOwner => "wrong_owner",
        LaneDecision::WrongAttempt => "wrong_attempt",
        LaneDecision::WrongLeaseEpoch => "wrong_lease_epoch",
        LaneDecision::WrongFence => "wrong_fence",
        LaneDecision::Expired => "expired",
        LaneDecision::Terminal => "terminal",
        LaneDecision::CleanupRequired => "cleanup_required",
        LaneDecision::Exclusivity => "exclusivity",
        LaneDecision::Invalid => "invalid",
    }
}

const REDACTED_PRIVATE_ID: &str = "redacted";

/// `DevelopmentLaneHold` is also the encrypted native record, so its private
/// identity fields remain available to the authority internally.  Every
/// public result/status projection passes through this copy and replaces the
/// managed locator, opaque host identity, and inventory alias with a bounded
/// redaction; callers can observe lifecycle/quota state without learning local
/// filesystem or host-placement details.
fn public_hold(hold: &DevelopmentLaneHold) -> DevelopmentLaneHold {
    let mut projected = hold.clone();
    projected.worktree_locator = REDACTED_PRIVATE_ID.to_string();
    projected.host_ref = REDACTED_PRIVATE_ID.to_string();
    projected.host_target_alias = None;
    projected
}

fn typed_decision<T: serde::de::DeserializeOwned>(decision: LaneDecision) -> Result<T, String> {
    serde_json::from_value(serde_json::Value::String(
        decision_name(decision).to_string(),
    ))
    .map_err(|e| e.to_string())
}

fn reserve_result(
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

fn renew_result(
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

fn observe_result(
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

fn finish_result(
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

fn cleanup_result(
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

fn quota_result(
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

fn query_result(
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

fn load_invocation<T>(
    invocations: &T,
    graph: &str,
    tenant: &str,
    key: &str,
    method: &Method,
    crypto: DurableCrypto<'_>,
) -> Result<Option<(bool, Vec<u8>)>, String>
where
    T: ReadableTable<(&'static str, &'static str, &'static str), &'static [u8]>,
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
    let Some(row) = invocations
        .get((graph, tenant, key))
        .map_err(|e| e.to_string())?
    else {
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

fn store_invocation(
    invocations: &mut redb::Table<(&str, &str, &str), &[u8]>,
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
    invocations
        .insert((graph, tenant, key), bytes.as_slice())
        .map_err(|e| e.to_string())?;
    prune_invocations(invocations, graph, tenant, key)
}

fn durable_invocation_bounds(row: &DurableLaneInvocation) -> Result<(), String> {
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

fn put_index(
    table: &mut redb::Table<(&str, &str), &str>,
    graph: &str,
    key: &str,
    hold_id: &str,
) -> Result<(), String> {
    fingerprint(hold_id)
        .map_err(|decision| format!("lane index hold: {}", decision_name(decision)))?;
    table
        .insert((graph, key), hold_id)
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn put_lane_index(
    table: &mut redb::Table<(&str, &str, &str), &str>,
    graph: &str,
    tenant: &str,
    lane_id: &str,
    hold_id: &str,
) -> Result<(), String> {
    fingerprint(hold_id)
        .map_err(|decision| format!("lane index hold: {}", decision_name(decision)))?;
    table
        .insert((graph, tenant, lane_id), hold_id)
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn put_tenant_index(
    table: &mut redb::Table<(&str, &str, &str), &str>,
    graph: &str,
    tenant: &str,
    hold_id: &str,
) -> Result<(), String> {
    fingerprint(hold_id)
        .map_err(|decision| format!("lane tenant index hold: {}", decision_name(decision)))?;
    table
        .insert((graph, tenant, hold_id), hold_id)
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn get_index<T>(table: &T, graph: &str, key: &str) -> Result<Option<String>, String>
where
    T: ReadableTable<(&'static str, &'static str), &'static str>,
{
    let Some(value) = table.get((graph, key)).map_err(|e| e.to_string())? else {
        return Ok(None);
    };
    let value = value.value().to_string();
    fingerprint(&value)
        .map_err(|decision| format!("lane index hold: {}", decision_name(decision)))?;
    Ok(Some(value))
}

fn get_lane_index<T>(
    table: &T,
    graph: &str,
    tenant: &str,
    lane_id: &str,
) -> Result<Option<String>, String>
where
    T: ReadableTable<(&'static str, &'static str, &'static str), &'static str>,
{
    let Some(value) = table
        .get((graph, tenant, lane_id))
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    let value = value.value().to_string();
    fingerprint(&value)
        .map_err(|decision| format!("lane index hold: {}", decision_name(decision)))?;
    Ok(Some(value))
}

fn get_tenant_index<T>(
    table: &T,
    graph: &str,
    tenant: &str,
    hold_id: &str,
) -> Result<Option<String>, String>
where
    T: ReadableTable<(&'static str, &'static str, &'static str), &'static str>,
{
    let Some(value) = table
        .get((graph, tenant, hold_id))
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    let value = value.value().to_string();
    fingerprint(&value)
        .map_err(|decision| format!("lane tenant index hold: {}", decision_name(decision)))?;
    Ok(Some(value))
}

fn empty_input_conflict(method: &Method) -> Result<Vec<u8>, String> {
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

fn work_item_kind_name(kind: DevelopmentLaneWorkItemKind) -> &'static str {
    match kind {
        DevelopmentLaneWorkItemKind::Lifecycle => "lane.lifecycle",
        DevelopmentLaneWorkItemKind::Cleanup => "lane.cleanup",
    }
}

fn hold_target_kind(
    kind: DevelopmentLaneIntentHostTargetKind,
) -> DevelopmentLaneHoldHostTargetKind {
    match kind {
        DevelopmentLaneIntentHostTargetKind::Local => DevelopmentLaneHoldHostTargetKind::Local,
        DevelopmentLaneIntentHostTargetKind::InventoryAlias => {
            DevelopmentLaneHoldHostTargetKind::InventoryAlias
        }
    }
}

fn policy_validate(policy: &DevelopmentLaneQuotaPolicy) -> Result<(), LaneDecision> {
    text(&policy.policy_name, "policy name")?;
    text(&policy.policy_version, "policy version")?;
    if policy.min_ttl_ms == 0
        || policy.max_ttl_ms < policy.min_ttl_ms
        || policy.max_ttl_ms > MAX_TTL_MS
        || policy.max_observation_staleness_ms > MAX_TTL_MS
        || policy.tenant_count_limit == 0
        || policy.owner_count_limit == 0
        || policy.session_count_limit == 0
        || policy.workspace_count_limit == 0
        || policy.repository_count_limit == 0
        || policy.host_count_limit == 0
        || policy.global_count_limit == 0
        || policy.tenant_predicted_disk_bytes == 0
        || policy.owner_predicted_disk_bytes == 0
        || policy.session_predicted_disk_bytes == 0
        || policy.workspace_predicted_disk_bytes == 0
        || policy.repository_predicted_disk_bytes == 0
        || policy.host_predicted_disk_bytes == 0
        || policy.global_predicted_disk_bytes == 0
        || policy.tenant_observed_disk_bytes == 0
        || policy.owner_observed_disk_bytes == 0
        || policy.session_observed_disk_bytes == 0
        || policy.workspace_observed_disk_bytes == 0
        || policy.repository_observed_disk_bytes == 0
        || policy.host_observed_disk_bytes == 0
        || policy.global_observed_disk_bytes == 0
        || policy.tenant_retained_disk_bytes == 0
        || policy.owner_retained_disk_bytes == 0
        || policy.session_retained_disk_bytes == 0
        || policy.workspace_retained_disk_bytes == 0
        || policy.repository_retained_disk_bytes == 0
        || policy.host_retained_disk_bytes == 0
        || policy.global_retained_disk_bytes == 0
        || [
            policy.tenant_count_limit,
            policy.owner_count_limit,
            policy.session_count_limit,
            policy.workspace_count_limit,
            policy.repository_count_limit,
            policy.host_count_limit,
            policy.global_count_limit,
        ]
        .into_iter()
        .any(|value| value > MAX_COUNT)
        || [
            policy.tenant_predicted_disk_bytes,
            policy.owner_predicted_disk_bytes,
            policy.session_predicted_disk_bytes,
            policy.workspace_predicted_disk_bytes,
            policy.repository_predicted_disk_bytes,
            policy.host_predicted_disk_bytes,
            policy.global_predicted_disk_bytes,
            policy.tenant_observed_disk_bytes,
            policy.owner_observed_disk_bytes,
            policy.session_observed_disk_bytes,
            policy.workspace_observed_disk_bytes,
            policy.repository_observed_disk_bytes,
            policy.host_observed_disk_bytes,
            policy.global_observed_disk_bytes,
            policy.tenant_retained_disk_bytes,
            policy.owner_retained_disk_bytes,
            policy.session_retained_disk_bytes,
            policy.workspace_retained_disk_bytes,
            policy.repository_retained_disk_bytes,
            policy.host_retained_disk_bytes,
            policy.global_retained_disk_bytes,
        ]
        .into_iter()
        .any(|value| value > MAX_DISK_BYTES)
    {
        return Err(LaneDecision::Invalid);
    }
    Ok(())
}

fn reserve_counter_check(
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

fn policy_pressure(counters: &[ScopeCounter], policy: &DevelopmentLaneQuotaPolicy) -> bool {
    counters.iter().any(|row| {
        row.value.active_count > policy_limit(policy, row.scope, Metric::Count)
            || row.value.predicted_disk_bytes > policy_limit(policy, row.scope, Metric::Predicted)
            || row.value.observed_disk_bytes > policy_limit(policy, row.scope, Metric::Observed)
            || row.value.retained_disk_bytes > policy_limit(policy, row.scope, Metric::Retained)
    })
}

fn pressure_max<T>(
    pressure_index: &T,
    graph: &str,
    tenant: &str,
    scope: Scope,
    metric: Metric,
) -> Result<u64, String>
where
    T: ReadableTable<
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
    let mut range = pressure_index
        .range(
            (graph, tenant, scope_name, metric_name, 0u64, "")
                ..=(
                    graph,
                    tenant,
                    scope_name,
                    metric_name,
                    u64::MAX,
                    "\u{10ffff}",
                ),
        )
        .map_err(|e| e.to_string())?;
    let Some((key, _)) = range.next_back().transpose().map_err(|e| e.to_string())? else {
        return Ok(0);
    };
    Ok(key.value().4)
}

fn indexed_policy_pressure<T>(
    pressure_index: &T,
    graph: &str,
    tenant: &str,
    policy: &DevelopmentLaneQuotaPolicy,
) -> Result<bool, String>
where
    T: ReadableTable<
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

fn index_hold_id(
    holds: &redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    hold_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<String, String> {
    let Some(row) = holds.get((graph, hold_id)).map_err(|e| e.to_string())? else {
        return Err("development lane index points to a missing hold".to_string());
    };
    let decoded: DurableLaneHold = resource_decode(row.value(), crypto)?;
    durable_hold_bounds(&decoded)?;
    Ok(hold_id.to_string())
}

fn exclusive_pair(
    existing: Option<String>,
    expected_hold_id: &str,
    holds: &redb::Table<(&str, &str), &[u8]>,
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

fn put_branch_index(
    table: &mut redb::Table<(&str, &str, &str), &str>,
    graph: &str,
    hold: &DevelopmentLaneHold,
) -> Result<(), String> {
    let key = branch_key(hold);
    fingerprint(&hold.hold_id)
        .map_err(|decision| format!("lane branch index hold: {}", decision_name(decision)))?;
    table
        .insert(
            (graph, hold.tenant_ref.as_str(), key.as_str()),
            hold.hold_id.as_str(),
        )
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn branch_index_id<T>(
    table: &T,
    graph: &str,
    hold: &DevelopmentLaneHold,
) -> Result<Option<String>, String>
where
    T: ReadableTable<(&'static str, &'static str, &'static str), &'static str>,
{
    let key = branch_key(hold);
    let Some(value) = table
        .get((graph, hold.tenant_ref.as_str(), key.as_str()))
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    let value = value.value().to_string();
    fingerprint(&value)
        .map_err(|decision| format!("lane branch index hold: {}", decision_name(decision)))?;
    Ok(Some(value))
}

fn remove_branch_index(
    table: &mut redb::Table<(&str, &str, &str), &str>,
    graph: &str,
    hold: &DevelopmentLaneHold,
) -> Result<(), String> {
    let key = branch_key(hold);
    table
        .remove((graph, hold.tenant_ref.as_str(), key.as_str()))
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn put_work_item_index(
    table: &mut redb::Table<(&str, &str, u64), &str>,
    graph: &str,
    hold: &DevelopmentLaneHold,
) -> Result<(), String> {
    fingerprint(&hold.hold_id)
        .map_err(|decision| format!("lane WorkItem index hold: {}", decision_name(decision)))?;
    table
        .insert(
            (graph, hold.work_item_id.as_str(), hold.attempt),
            hold.hold_id.as_str(),
        )
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn work_item_index_id<T>(
    table: &T,
    graph: &str,
    work_item_id: &str,
    attempt: u64,
) -> Result<Option<String>, String>
where
    T: ReadableTable<(&'static str, &'static str, u64), &'static str>,
{
    let Some(value) = table
        .get((graph, work_item_id, attempt))
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    let value = value.value().to_string();
    fingerprint(&value)
        .map_err(|decision| format!("lane WorkItem index hold: {}", decision_name(decision)))?;
    Ok(Some(value))
}

#[allow(clippy::too_many_arguments)]
fn apply_reserve(
    graph: &str,
    request: &DevelopmentLaneReserveRequest,
    nodes: &redb::Table<(&str, &str), &[u8]>,
    holds: &mut redb::Table<(&str, &str), &[u8]>,
    tenant_index: &mut redb::Table<(&str, &str, &str), &str>,
    lane_index: &mut redb::Table<(&str, &str, &str), &str>,
    branch_index: &mut redb::Table<(&str, &str, &str), &str>,
    worktree_index: &mut redb::Table<(&str, &str), &str>,
    work_item_index: &mut redb::Table<(&str, &str, u64), &str>,
    counters: &mut redb::Table<(&str, &str), &[u8]>,
    pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &redb::Table<(&str, &str), &[u8]>,
    resource_reservations: &mut redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    if let Err(decision) = intent_validate(&request.intent) {
        return Ok((reserve_result(decision, None, 0)?, false));
    }
    if request.tenant_ref != request.intent.tenant_ref
        || request.owner_id != request.intent.owner_id
        || request.work_item_id.is_empty()
        || request.attempt == 0
        || request.lease_epoch == 0
        || request.fencing_token == 0
        || request.work_item_fence.is_empty()
        || request.idempotency_key.is_empty()
    {
        return Ok((reserve_result(LaneDecision::Invalid, None, 0)?, false));
    }
    for (value, name) in [
        (&request.tenant_ref, "reserve tenant"),
        (&request.work_item_id, "reserve WorkItem"),
        (&request.owner_id, "reserve owner"),
        (&request.work_item_fence, "reserve fence"),
        (&request.idempotency_key, "reserve invocation"),
    ] {
        if text(value, name).is_err() {
            return Ok((reserve_result(LaneDecision::Invalid, None, 0)?, false));
        }
    }
    let policy = load_policy(policies, graph, &request.tenant_ref, crypto)?;
    let policy_revision = policy.as_ref().map_or(0, |value| value.policy_revision);
    let global_policy = load_global_policy(policies, graph, crypto)?;
    let global_policy_revision = global_policy
        .as_ref()
        .map_or(0, |value| value.policy_revision);
    let Some(policy) = policy else {
        return Ok((reserve_result(LaneDecision::Policy, None, 0)?, false));
    };
    let Some(global_policy) = global_policy else {
        return Ok((
            reserve_result(LaneDecision::Policy, None, policy_revision)?,
            false,
        ));
    };
    if global_policy.policy.drain_only {
        return Ok((
            reserve_result(LaneDecision::Drained, None, policy_revision)?,
            false,
        ));
    }
    if policy.global_policy_revision != global_policy_revision {
        return Ok((
            reserve_result(LaneDecision::Conflict, None, policy_revision)?,
            false,
        ));
    }
    if !global_policy_equal(&policy.policy, &global_policy.policy) {
        return Ok((
            reserve_result(LaneDecision::Conflict, None, policy_revision)?,
            false,
        ));
    }
    if policy.policy.policy_name != request.intent.quota_policy_name
        || policy.policy.policy_version != request.intent.quota_policy_version
        || request.intent.ttl_ms < policy.policy.min_ttl_ms
        || request.intent.ttl_ms > policy.policy.max_ttl_ms
    {
        return Ok((
            reserve_result(LaneDecision::Policy, None, policy_revision)?,
            false,
        ));
    }
    if policy.policy.drain_only {
        return Ok((
            reserve_result(LaneDecision::Drained, None, policy_revision)?,
            false,
        ));
    }

    let derived_hold_id = hold_id(&request.intent);
    if let Some(existing) = hold_load(holds, graph, &derived_hold_id, crypto)? {
        if hold_immutable_equal(&existing, request) {
            return Ok((
                reserve_result(LaneDecision::Idempotent, Some(&existing), policy_revision)?,
                false,
            ));
        }
        return Ok((
            reserve_result(LaneDecision::InputConflict, None, policy_revision)?,
            false,
        ));
    }
    if let Some(existing) = work_item_index_id(
        work_item_index,
        graph,
        &request.work_item_id,
        request.attempt,
    )? {
        let existing = index_hold_id(holds, graph, &existing, crypto)
            .map_err(|_| "development lane WorkItem index is orphaned".to_string())?;
        if existing != derived_hold_id {
            return Ok((
                reserve_result(LaneDecision::InputConflict, None, policy_revision)?,
                false,
            ));
        }
        return Err("development lane WorkItem index disagrees with hold identity".to_string());
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
        .get((graph, work_item.resource_reservation_id.as_str()))
        .map_err(|error| error.to_string())?
        .map(|value| resource_decode::<super::DurableResourceReservation>(value.value(), crypto))
        .transpose()?;
    let Some(resource_row) = resource_row else {
        return Ok((
            reserve_result(LaneDecision::NotFound, None, policy_revision)?,
            false,
        ));
    };
    let resource = &resource_row.record;
    let resource_target_matches =
        match request.intent.host_target_kind {
            DevelopmentLaneIntentHostTargetKind::Local => {
                resource.target_kind
                    == crate::epistemic_operations::ResourceReservationRecordTargetKind::Local
                    && resource.target_alias.is_none()
            }
            DevelopmentLaneIntentHostTargetKind::InventoryAlias => resource.target_kind
                == crate::epistemic_operations::ResourceReservationRecordTargetKind::InventoryAlias
                && resource.target_alias == request.intent.host_target_alias,
        };
    if resource_row.record.state != ResourceReservationRecordState::Reserved
        || resource.expires_at_ms <= request.now_ms
        || resource.tenant_ref != request.tenant_ref
        || resource.work_item_id != request.work_item_id
        || resource.owner_id != request.owner_id
        || resource.attempt != request.attempt
        || resource.lease_epoch != request.lease_epoch
        || resource.fencing_token != request.fencing_token
        || resource.fence != request.work_item_fence
        || resource.host_ref != work_item.host_ref
        || resource.input_fingerprint != request.intent.input_fingerprint
        || resource.repository_id != request.intent.repository_id
        || resource.branch != request.intent.branch
        || !resource_target_matches
    {
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

    let lane_existing = get_lane_index(
        lane_index,
        graph,
        hold.tenant_ref.as_str(),
        hold.lane_id.as_str(),
    )?;
    if let Err(decision) = exclusive_pair(lane_existing, &hold.hold_id, holds, graph, crypto) {
        return Ok((reserve_result(decision, None, policy_revision)?, false));
    }
    let branch_existing = branch_index_id(branch_index, graph, &hold)?;
    if let Err(decision) = exclusive_pair(branch_existing, &hold.hold_id, holds, graph, crypto) {
        return Ok((reserve_result(decision, None, policy_revision)?, false));
    }
    let worktree_key = worktree_key(&hold);
    let worktree_existing = get_index(worktree_index, graph, &worktree_key)?;
    if let Err(decision) = exclusive_pair(worktree_existing, &hold.hold_id, holds, graph, crypto) {
        return Ok((reserve_result(decision, None, policy_revision)?, false));
    }

    let loaded = load_scope_counters(
        counters,
        graph,
        &hold,
        policy_revision,
        global_policy_revision,
        crypto,
    )?;
    if let Err(decision) = reserve_counter_check(&loaded, &policy.policy, hold.predicted_disk_bytes)
    {
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

fn hold_correlations_match(
    hold: &DevelopmentLaneHold,
    tenant: &str,
    work_item_id: &str,
    owner_id: &str,
    attempt: u64,
    lease_epoch: u64,
    fencing_token: u64,
    work_item_fence: &str,
) -> Result<(), LaneDecision> {
    if hold.tenant_ref != tenant {
        return Err(LaneDecision::WrongTenant);
    }
    if hold.work_item_id != work_item_id {
        return Err(LaneDecision::Conflict);
    }
    if hold.owner_id != owner_id {
        return Err(LaneDecision::WrongOwner);
    }
    if hold.attempt != attempt {
        return Err(LaneDecision::WrongAttempt);
    }
    if hold.lease_epoch != lease_epoch {
        return Err(LaneDecision::WrongLeaseEpoch);
    }
    if hold.fencing_token != fencing_token || hold.work_item_fence != work_item_fence {
        return Err(LaneDecision::WrongFence);
    }
    Ok(())
}

fn terminal_source_correlations_match(
    row: &DurableLaneHold,
    request: &DevelopmentLaneFinishRequest,
) -> bool {
    !row.hold.active_count_charged
        && row.terminal_source_attempt == Some(request.attempt)
        && row.terminal_source_lease_epoch == Some(request.lease_epoch)
        && row.terminal_source_fencing_token == Some(request.fencing_token)
        && row.terminal_source_work_item_fence.as_deref() == Some(request.work_item_fence.as_str())
}

fn observation_fresh(
    row: &DurableLaneHold,
    policy: &DevelopmentLaneQuotaPolicy,
    now_ms: u64,
) -> bool {
    row.last_observed_at_ms.is_some_and(|observed_at| {
        observed_at <= now_ms
            && now_ms.saturating_sub(observed_at) <= policy.max_observation_staleness_ms
    })
}

fn expire_active_hold(
    graph: &str,
    row: &mut DurableLaneHold,
    counters: &mut redb::Table<(&str, &str), &[u8]>,
    pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
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

fn hold_policy_revision(row: &DurableLaneHold, policy: Option<&DurableLanePolicy>) -> u64 {
    policy.map_or(row.hold.quota_charge.policy_revision, |value| {
        value.policy_revision
    })
}

fn current_policy(
    policies: &redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    tenant: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableLanePolicy>, String> {
    load_policy(policies, graph, tenant, crypto)
}

#[allow(clippy::too_many_arguments)]
fn apply_renew(
    graph: &str,
    request: &DevelopmentLaneRenewRequest,
    nodes: &redb::Table<(&str, &str), &[u8]>,
    holds: &mut redb::Table<(&str, &str), &[u8]>,
    counters: &mut redb::Table<(&str, &str), &[u8]>,
    pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    if request.tenant_ref.is_empty()
        || request.work_item_id.is_empty()
        || request.owner_id.is_empty()
        || request.hold_id.is_empty()
        || request.idempotency_key.is_empty()
        || request.ttl_ms == 0
    {
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
    if request.ttl_ms < global_policy.policy.min_ttl_ms
        || request.ttl_ms > global_policy.policy.max_ttl_ms
    {
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
    if row.hold.tombstone || !row.hold.active_count_charged {
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
    if !observation_fresh(&row, &global_policy.policy, request.now_ms) {
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

#[allow(clippy::too_many_arguments)]
fn apply_observe(
    graph: &str,
    request: &DevelopmentLaneObserveRequest,
    nodes: &redb::Table<(&str, &str), &[u8]>,
    holds: &mut redb::Table<(&str, &str), &[u8]>,
    counters: &mut redb::Table<(&str, &str), &[u8]>,
    pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    if request.tenant_ref.is_empty()
        || request.work_item_id.is_empty()
        || request.owner_id.is_empty()
        || request.hold_id.is_empty()
        || request.idempotency_key.is_empty()
        || request.observation_revision == 0
    {
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
    if row.hold.tombstone || !row.hold.active_count_charged {
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
    if request.observation_revision < row.observation_revision
        || request.observed_disk_bytes < row.hold.observed_disk_bytes
    {
        return Ok((
            observe_result(LaneDecision::Stale, Some(&row), policy_revision)?,
            false,
        ));
    }
    if request.observation_revision == row.observation_revision {
        if request.observed_disk_bytes == row.hold.observed_disk_bytes {
            return Ok((
                observe_result(LaneDecision::Idempotent, Some(&row), policy_revision)?,
                false,
            ));
        }
        return Ok((
            observe_result(LaneDecision::Stale, Some(&row), policy_revision)?,
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

fn finish_state_matches(
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

fn finish_state_name(state: DevelopmentLaneFinishRequestTerminalState) -> &'static str {
    match state {
        DevelopmentLaneFinishRequestTerminalState::Succeeded => "succeeded",
        DevelopmentLaneFinishRequestTerminalState::Failed => "failed",
        DevelopmentLaneFinishRequestTerminalState::Cancelled => "cancelled",
        DevelopmentLaneFinishRequestTerminalState::DeadLetter => "dead_letter",
    }
}

const ACTIVE_HOLD_REQUIRES_TERMINAL_WORK_ITEM: &str =
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
    holds: &mut redb::Table<(&str, &str), &[u8]>,
    work_item_index: &redb::Table<(&str, &str, u64), &str>,
    counters: &mut redb::Table<(&str, &str), &[u8]>,
    pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<bool, String> {
    let attempt = super::property_u64(pre_props, "attempt");
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
    if row.hold.tombstone
        || !matches!(
            row.hold.state,
            DevelopmentLaneHoldState::Allocating
                | DevelopmentLaneHoldState::Active
                | DevelopmentLaneHoldState::Submitted
        )
    {
        return Err("development lane active hold has an invalid live state".to_string());
    }

    // The linked row must be the exact pre-terminal lifecycle image.  A
    // caller cannot turn a stale/foreign WorkItem mutation into a lane finish,
    // and a ready/submitted image with an active hold is rejected rather than
    // silently auto-finished.
    if row.hold.work_item_id != work_item_id
        || super::property_string(pre_props, "node_type") != "WorkItem"
        || super::property_string(pre_props, "kind")
            != work_item_kind_name(DevelopmentLaneWorkItemKind::Lifecycle)
        || super::property_string(pre_props, "tenant") != row.hold.tenant_ref
        || !matches!(
            super::property_string(pre_props, "status"),
            "leased" | "running"
        )
        || row.hold.attempt != attempt
        || row.hold.lease_epoch != super::property_u64(pre_props, "lease_epoch")
        || row.hold.fencing_token != super::property_u64(pre_props, "fencing_token")
        || row.hold.work_item_fence != super::property_string(pre_props, "work_item_fence")
        || super::property_string(pre_props, "lease_owner") != row.hold.owner_id
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
    let pre_lease_epoch = super::property_u64(pre_props, "lease_epoch");
    let pre_fencing_token = super::property_u64(pre_props, "fencing_token");
    let expected_lease_epoch = if cancel_fence_evolution {
        pre_lease_epoch
            .checked_add(1)
            .ok_or_else(|| "development lane cancel lease epoch overflow".to_string())?
    } else {
        pre_lease_epoch
    };
    let expected_fencing_token = if cancel_fence_evolution {
        pre_fencing_token
            .checked_add(1)
            .ok_or_else(|| "development lane cancel fencing token overflow".to_string())?
    } else {
        pre_fencing_token
    };
    if next_attempt == 0
        || next_attempt != attempt
        || next_lease_epoch == 0
        || next_lease_epoch != expected_lease_epoch
        || next_fencing_token == 0
        || next_fencing_token != expected_fencing_token
        || next_work_item_fence.is_empty()
        || next_work_item_fence != super::property_string(pre_props, "work_item_fence")
    {
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
            super::property_u64(pre_props, "lease_epoch"),
            super::property_u64(pre_props, "fencing_token"),
            super::property_string(pre_props, "work_item_fence").to_string(),
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
fn transition_terminal_hold(
    graph: &str,
    row: &mut DurableLaneHold,
    terminal_state: &str,
    terminal_attempt: u64,
    terminal_lease_epoch: u64,
    terminal_fencing_token: u64,
    terminal_work_item_fence: &str,
    source_tuple: Option<(u64, u64, u64, String)>,
    counters: &mut redb::Table<(&str, &str), &[u8]>,
    pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
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

#[allow(clippy::too_many_arguments)]
fn apply_finish(
    graph: &str,
    request: &DevelopmentLaneFinishRequest,
    nodes: &redb::Table<(&str, &str), &[u8]>,
    holds: &mut redb::Table<(&str, &str), &[u8]>,
    counters: &mut redb::Table<(&str, &str), &[u8]>,
    pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    if request.tenant_ref.is_empty()
        || request.work_item_id.is_empty()
        || request.owner_id.is_empty()
        || request.hold_id.is_empty()
        || request.idempotency_key.is_empty()
    {
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
    let Some(mut row) = hold_load(holds, graph, &request.hold_id, crypto)? else {
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
    let source_tuple_matches = terminal_source_correlations_match(&row, request)
        && row.hold.tenant_ref == request.tenant_ref
        && row.hold.work_item_id == request.work_item_id
        && row.hold.owner_id == request.owner_id;
    if !current_tuple_matches && !source_tuple_matches {
        let decision = current_correlations
            .expect_err("lane finish correlation predicate changed unexpectedly");
        return Ok((finish_result(decision, Some(&row), policy_revision)?, false));
    }
    if !row.hold.active_count_charged {
        // A fresh invocation against a terminal tombstone still proves the
        // current lifecycle WorkItem and its typed intent.  Only the exact
        // invocation key may bypass this check (the replay lookup happens
        // before this function); knowing a hold id and old fence is not enough
        // to manufacture a terminal outcome.
        let (work_item_attempt, work_item_lease_epoch, work_item_fencing_token, work_item_fence) =
            if source_tuple_matches {
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
            Err(decision) => {
                return Ok((finish_result(decision, Some(&row), policy_revision)?, false))
            }
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
                finish_result(LaneDecision::InputConflict, Some(&row), policy_revision)?,
                false,
            ));
        }
        let requested = finish_state_name(request.terminal_state);
        let terminal_revision_matches = row.terminal_expected_hold_revision
            == Some(request.expected_hold_revision)
            || (row.terminal_source_attempt.is_some()
                && current_tuple_matches
                && row.hold.hold_revision == request.expected_hold_revision);
        let decision = row
            .terminal_state
            .as_deref()
            .filter(|stored| *stored == requested)
            .filter(|_| terminal_revision_matches)
            .map_or(LaneDecision::InputConflict, |_| LaneDecision::Idempotent);
        return Ok((finish_result(decision, Some(&row), policy_revision)?, false));
    }
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

fn remove_index(
    table: &mut redb::Table<(&str, &str), &str>,
    graph: &str,
    key: &str,
    hold_id: &str,
) -> Result<(), String> {
    let existing = get_index(table, graph, key)?
        .ok_or_else(|| "development lane exclusivity index is missing".to_string())?;
    if existing != hold_id {
        return Err("development lane exclusivity index points to another hold".to_string());
    }
    table.remove((graph, key)).map_err(|e| e.to_string())?;
    Ok(())
}

fn remove_lane_index(
    table: &mut redb::Table<(&str, &str, &str), &str>,
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
    table
        .remove((graph, tenant, lane_id))
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn remove_work_item_index(
    table: &mut redb::Table<(&str, &str, u64), &str>,
    graph: &str,
    hold: &DevelopmentLaneHold,
) -> Result<(), String> {
    let existing = work_item_index_id(table, graph, &hold.work_item_id, hold.attempt)?
        .ok_or_else(|| "development lane WorkItem index is missing".to_string())?;
    if existing != hold.hold_id {
        return Err("development lane WorkItem index points to another hold".to_string());
    }
    table
        .remove((graph, hold.work_item_id.as_str(), hold.attempt))
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn remove_branch_index_checked(
    table: &mut redb::Table<(&str, &str, &str), &str>,
    graph: &str,
    hold: &DevelopmentLaneHold,
) -> Result<(), String> {
    let key = branch_key(hold);
    let existing = branch_index_id(table, graph, hold)?
        .ok_or_else(|| "development lane branch index is missing".to_string())?;
    if existing != hold.hold_id {
        return Err("development lane branch index points to another hold".to_string());
    }
    table
        .remove((graph, hold.tenant_ref.as_str(), key.as_str()))
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Load the bounded tenant/global policy snapshot.  The sorted pressure index
/// supplies exact maxima for the non-tenant scope families; policy CAS never
/// scans those families or the hold index.
fn policy_scope_rows(
    counters: &redb::Table<(&str, &str), &[u8]>,
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

fn policy_counter_snapshot(
    counters: &redb::Table<(&str, &str), &[u8]>,
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

#[allow(clippy::too_many_arguments)]
fn apply_cleanup(
    graph: &str,
    request: &DevelopmentLaneCleanupCompleteRequest,
    nodes: &redb::Table<(&str, &str), &[u8]>,
    holds: &mut redb::Table<(&str, &str), &[u8]>,
    tenant_index: &redb::Table<(&str, &str, &str), &str>,
    lane_index: &mut redb::Table<(&str, &str, &str), &str>,
    branch_index: &mut redb::Table<(&str, &str, &str), &str>,
    worktree_index: &mut redb::Table<(&str, &str), &str>,
    work_item_index: &mut redb::Table<(&str, &str, u64), &str>,
    counters: &mut redb::Table<(&str, &str), &[u8]>,
    pressure_index: &mut redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    if request.tenant_ref.is_empty()
        || request.work_item_id.is_empty()
        || request.owner_id.is_empty()
        || request.hold_id.is_empty()
        || request.cleanup_work_item_id.is_empty()
        || request.cleanup_work_item_id == request.work_item_id
        || request.idempotency_key.is_empty()
        || request.removal_proof_ref.is_empty()
    {
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
    let Some(mut row) = hold_load(holds, graph, &request.hold_id, crypto)? else {
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
        // A fresh invocation against a tombstone must still prove both
        // WorkItem authorities.  The stored replay tuple is necessary for
        // exact idempotency, but it is not a substitute for the current
        // lifecycle terminal fence or the typed cleanup correlation.
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
            Err(decision) => {
                return Ok((
                    cleanup_result(decision, Some(&row), policy_revision)?,
                    false,
                ))
            }
        };
        if !lane_intent_matches_hold(
            lifecycle_work_item.lane_intent.as_ref(),
            &row.hold,
            row.ttl_ms,
            &row.resource_reservation_id,
        ) {
            return Ok((
                cleanup_result(LaneDecision::InputConflict, Some(&row), policy_revision)?,
                false,
            ));
        }
        if request.cleanup_attempt == 0
            || request.cleanup_lease_epoch == 0
            || request.cleanup_fencing_token == 0
        {
            return Ok((
                cleanup_result(LaneDecision::Invalid, Some(&row), policy_revision)?,
                false,
            ));
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
            return Ok((
                cleanup_result(decision, Some(&row), policy_revision)?,
                false,
            ));
        }
        let replay_matches = row.cleanup_expected_hold_revision
            == Some(request.expected_hold_revision)
            && row.cleanup_removal_proof_ref.as_deref() == Some(request.removal_proof_ref.as_str())
            && row.hold.cleanup_work_item_id.as_deref()
                == Some(request.cleanup_work_item_id.as_str())
            && row.hold.cleanup_work_item_fence.as_deref()
                == Some(request.cleanup_work_item_fence.as_str())
            && row.hold.cleanup_attempt == Some(request.cleanup_attempt)
            && row.hold.cleanup_lease_epoch == Some(request.cleanup_lease_epoch)
            && row.hold.cleanup_fencing_token == Some(request.cleanup_fencing_token);
        if !replay_matches {
            return Ok((
                cleanup_result(LaneDecision::InputConflict, Some(&row), policy_revision)?,
                false,
            ));
        }
        return Ok((
            cleanup_result(LaneDecision::Idempotent, Some(&row), policy_revision)?,
            false,
        ));
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
        Err(decision) => {
            return Ok((
                cleanup_result(decision, Some(&row), policy_revision)?,
                false,
            ))
        }
    };
    if !lane_intent_matches_hold(
        lifecycle_work_item.lane_intent.as_ref(),
        &row.hold,
        row.ttl_ms,
        &row.resource_reservation_id,
    ) {
        return Ok((
            cleanup_result(LaneDecision::InputConflict, Some(&row), policy_revision)?,
            false,
        ));
    }
    if request.cleanup_attempt == 0
        || request.cleanup_lease_epoch == 0
        || request.cleanup_fencing_token == 0
    {
        return Ok((
            cleanup_result(LaneDecision::Invalid, Some(&row), policy_revision)?,
            false,
        ));
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
fn apply_quota_update(
    graph: &str,
    request: &DevelopmentLaneQuotaUpdateRequest,
    counters: &redb::Table<(&str, &str), &[u8]>,
    pressure_index: &redb::Table<(&str, &str, &str, &str, u64, &str), u8>,
    policies: &mut redb::Table<(&str, &str), &[u8]>,
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
            || current_charge.global_predicted_disk_bytes
                > request.policy.global_predicted_disk_bytes
            || current_charge.global_observed_disk_bytes
                > request.policy.global_observed_disk_bytes
            || current_charge.global_retained_disk_bytes
                > request.policy.global_retained_disk_bytes;
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
        policies
            .insert((graph, GLOBAL_POLICY_KEY), bytes.as_slice())
            .map_err(|e| e.to_string())?;
        let charge =
            policy_counter_snapshot(counters, graph, GLOBAL_POLICY_KEY, 0, next_revision, crypto)?;
        return Ok((
            quota_result(
                LaneDecision::Accepted,
                Some(request.policy.clone()),
                charge,
                next_revision,
            )?,
            true,
        ));
    }
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
                .map_or(true, |value| value.policy.policy_version != expected)
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
    if let Some(global) = global.as_ref() {
        if !global_policy_equal(&request.policy, &global.policy) {
            // There is one graph-global policy authority.  A tenant may tune
            // its local dimensions, but cannot silently change the shared
            // counter's limits/freshness/drain semantics.
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
    let row = DurableLanePolicy {
        policy: request.policy.clone(),
        policy_revision: next_revision,
        global_policy_revision: if global.is_none() {
            1
        } else {
            global_policy_revision
        },
    };
    let bytes = resource_encode(&row, crypto)?;
    policies
        .insert((graph, request.tenant_ref.as_str()), bytes.as_slice())
        .map_err(|e| e.to_string())?;
    if global.is_none() {
        let global_row = DurableLanePolicy {
            policy: request.policy.clone(),
            policy_revision: 1,
            global_policy_revision: 1,
        };
        let global_bytes = resource_encode(&global_row, crypto)?;
        policies
            .insert((graph, GLOBAL_POLICY_KEY), global_bytes.as_slice())
            .map_err(|e| e.to_string())?;
    }
    let global_policy_revision = if global.is_none() {
        1
    } else {
        global_policy_revision
    };
    let charge = policy_counter_snapshot(
        counters,
        graph,
        &request.tenant_ref,
        next_revision,
        global_policy_revision,
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

#[allow(clippy::too_many_arguments)]
fn apply_mutation_in_wtx(
    wtx: &WriteTransaction,
    graph: &str,
    method: &Method,
    crypto: DurableCrypto<'_>,
) -> Result<(Vec<u8>, bool), String> {
    validate_method_bounds(graph, method)
        .map_err(|decision| format!("development lane request: {}", decision_name(decision)))?;
    let mut holds = wtx.open_table(HOLDS).map_err(|e| e.to_string())?;
    let mut tenant_index = wtx.open_table(TENANT_INDEX).map_err(|e| e.to_string())?;
    let mut lane_index = wtx.open_table(LANE_INDEX).map_err(|e| e.to_string())?;
    let mut branch_index = wtx
        .open_table(REPOSITORY_BRANCH_INDEX)
        .map_err(|e| e.to_string())?;
    let mut worktree_index = wtx.open_table(WORKTREE_INDEX).map_err(|e| e.to_string())?;
    let mut work_item_index = wtx.open_table(WORK_ITEM_INDEX).map_err(|e| e.to_string())?;
    let mut counters = wtx.open_table(COUNTERS).map_err(|e| e.to_string())?;
    let mut pressure_index = wtx.open_table(PRESSURE_INDEX).map_err(|e| e.to_string())?;
    let mut policies = wtx.open_table(POLICIES).map_err(|e| e.to_string())?;
    let mut invocations = wtx.open_table(INVOCATIONS).map_err(|e| e.to_string())?;
    let mut resource_reservations = wtx
        .open_table(super::RESOURCE_RESERVATIONS)
        .map_err(|e| e.to_string())?;
    let nodes = wtx.open_table(NODES).map_err(|e| e.to_string())?;

    if let Some((tenant, key)) = idempotency_key(method) {
        text(tenant, "lane invocation tenant")
            .map_err(|decision| decision_name(decision).to_string())?;
        text(key, "lane invocation key").map_err(|decision| decision_name(decision).to_string())?;
        if let Some((exact, result)) =
            load_invocation(&invocations, graph, tenant, key, method, crypto)?
        {
            if exact {
                return Ok((result, false));
            }
            return Ok((empty_input_conflict(method)?, false));
        }
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
    if operation_changed {
        if let Some((tenant, key)) = idempotency_key(method) {
            store_invocation(
                &mut invocations,
                graph,
                tenant,
                key,
                method,
                &result,
                crypto,
            )?;
        }
        Ok((result, true))
    } else {
        // Refusal results are also invocation outcomes.  Persisting them makes
        // acknowledgement loss deterministic while a fresh idempotency key can
        // retry after policy/capacity changes.
        if let Some((tenant, key)) = idempotency_key(method) {
            store_invocation(
                &mut invocations,
                graph,
                tenant,
                key,
                method,
                &result,
                crypto,
            )?;
            return Ok((result, true));
        }
        Ok((result, false))
    }
}

/// Commit one lane mutation atomically in redb and return its generated typed
/// result bytes.  `authoritative_now_ms` is supplied by the eventual dispatch
/// seam; the caller's serialized `now_ms` is overwritten before validation,
/// idempotency hashing, or persistence.
pub(crate) fn commit_development_lane(
    db: &Database,
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
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    let (result, changed) = apply_mutation_in_wtx(&wtx, graph, &method, crypto)?;
    if changed {
        wtx.commit().map_err(|e| e.to_string())?;
    }
    Ok(result)
}

fn read_query_in_rtx(
    rtx: &redb::ReadTransaction,
    graph: &str,
    request: &DevelopmentLaneQueryRequest,
    crypto: DurableCrypto<'_>,
) -> Result<DevelopmentLaneQueryResult, String> {
    text(&request.tenant_ref, "lane query tenant")
        .map_err(|decision| decision_name(decision).to_string())?;
    text(&request.hold_id, "lane query hold")
        .map_err(|decision| decision_name(decision).to_string())?;
    let holds = rtx.open_table(HOLDS).map_err(|e| e.to_string())?;
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
    db: &Database,
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
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    read_query_in_rtx(&rtx, graph, &request, crypto)
}

fn read_status_in_rtx(
    rtx: &redb::ReadTransaction,
    graph: &str,
    request: &DevelopmentLaneStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<DevelopmentLaneStatusResult, String> {
    text(&request.tenant_ref, "lane status tenant")
        .map_err(|decision| decision_name(decision).to_string())?;
    if !(1..=MAX_STATUS_LIMIT).contains(&request.limit) {
        return Err("development lane status limit is outside the native bound".to_string());
    }
    if let Some(cursor) = request.cursor.as_deref() {
        text(cursor, "lane status cursor")
            .map_err(|decision| decision_name(decision).to_string())?;
    }
    let cursor = request.cursor.as_deref().unwrap_or("");
    let holds = rtx.open_table(HOLDS).map_err(|e| e.to_string())?;
    let tenant_index = rtx.open_table(TENANT_INDEX).map_err(|e| e.to_string())?;
    let policies = rtx.open_table(POLICIES).map_err(|e| e.to_string())?;
    let counters = rtx.open_table(COUNTERS).map_err(|e| e.to_string())?;
    let policy_revision = load_policy(&policies, graph, &request.tenant_ref, crypto)?
        .map_or(0, |value| value.policy_revision);
    let mut rows = Vec::new();
    let mut scanned = 0usize;
    let mut has_more = false;
    let mut last = None;
    for row in tenant_index
        .range((graph, request.tenant_ref.as_str(), cursor)..)
        .map_err(|e| e.to_string())?
    {
        scanned = scanned
            .checked_add(1)
            .ok_or_else(|| "development lane status scan overflow".to_string())?;
        if scanned > MAX_STATUS_SCAN {
            return Err("development lane status scan exceeds native bound".to_string());
        }
        let (key, value) = row.map_err(|e| e.to_string())?;
        let (row_graph, tenant, hold_id) = key.value();
        if row_graph != graph || tenant != request.tenant_ref {
            break;
        }
        if hold_id == cursor || value.value() != hold_id {
            continue;
        }
        let Some(row) = hold_load(&holds, graph, hold_id, crypto)? else {
            return Err("development lane status index points to a missing hold".to_string());
        };
        if request
            .hold_id
            .as_deref()
            .is_some_and(|filter| filter != row.hold.hold_id)
            || request
                .lane_id
                .as_deref()
                .is_some_and(|filter| filter != row.hold.lane_id)
            || request
                .work_item_id
                .as_deref()
                .is_some_and(|filter| filter != row.hold.work_item_id)
        {
            continue;
        }
        rows.push(public_hold(&row.hold));
        if rows.len() > request.limit as usize {
            // Read one row beyond the requested page before declaring a next
            // page.  Exactly `limit` rows therefore produce a complete page;
            // the extra row is only a bounded existence probe.
            rows.pop();
            last = rows.last().map(|value| value.hold_id.clone());
            has_more = true;
            break;
        }
        last = Some(hold_id.to_string());
    }
    let probe = DevelopmentLaneHold {
        schema_version: crate::epistemic_operations::DevelopmentLaneHoldSchemaVersion::V1,
        hold_id: "snapshot".to_string(),
        lane_id: "snapshot".to_string(),
        tenant_ref: request.tenant_ref.clone(),
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
    let global_policy_revision =
        load_global_policy(&policies, graph, crypto)?.map_or(0, |value| value.policy_revision);
    let scope_rows = load_scope_counters(
        &counters,
        graph,
        &probe,
        policy_revision,
        global_policy_revision,
        crypto,
    )?;
    let tenant_counter = scope_rows
        .iter()
        .find(|row| row.scope == Scope::Tenant)
        .map(|row| &row.value)
        .ok_or_else(|| "tenant counter missing from scope set".to_string())?;
    let global_counter = scope_rows
        .iter()
        .find(|row| row.scope == Scope::Global)
        .map(|row| &row.value)
        .ok_or_else(|| "global counter missing from scope set".to_string())?;
    Ok(DevelopmentLaneStatusResult {
        schema_version: DevelopmentLaneStatusResultSchemaVersion::V1,
        complete: !has_more,
        next_cursor: has_more.then_some(last).flatten(),
        holds: rows,
        counters: snapshot_charge(tenant_counter, global_counter, policy_revision),
        tenant_active_count: tenant_counter.active_count,
        tenant_retained_disk_bytes: tenant_counter.retained_disk_bytes,
        tombstone: false,
    })
}

/// Return a bounded tenant status page from maintained indexes/counters.
pub(crate) fn read_development_lane_status(
    db: &Database,
    graph: &str,
    request: &DevelopmentLaneStatusRequest,
    authoritative_now_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<DevelopmentLaneStatusResult, String> {
    let mut request = request.clone();
    request.now_ms = authoritative_now_ms;
    validate_method_bounds(
        graph,
        &Method::DevelopmentLaneStatus {
            request: request.clone(),
        },
    )
    .map_err(|decision| format!("development lane status: {}", decision_name(decision)))?;
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    read_status_in_rtx(&rtx, graph, &request, crypto)
}

#[cfg(test)]
mod tests {
    use super::super::{property_string, read_one_node};
    use super::*;
    use crate::epistemic_operations::{
        DevelopmentLaneCleanupCompleteRequestSchemaVersion, DevelopmentLaneCleanupIntent,
        DevelopmentLaneCleanupIntentSchemaVersion, DevelopmentLaneFinishRequestSchemaVersion,
        DevelopmentLaneIntentSchemaVersion, DevelopmentLaneObserveRequestSchemaVersion,
        DevelopmentLaneRenewRequestSchemaVersion, DevelopmentLaneReserveRequestSchemaVersion,
        ResourceCapacitySnapshot, ResourceRequirement, ResourceReservationRecord,
        ResourceReservationRecordTargetKind, ResourceTargetSnapshot, ResourceTargetSnapshotKind,
    };
    use crate::mutation_batch::{
        MutationBatch, MutationBatchCommit, MutationDomain, MutationOperation,
        MutationOutboxIntent, MutationRequestContext, MutationSurface, MUTATION_BATCH_VERSION,
    };
    use std::path::PathBuf;
    use std::sync::{Arc, Barrier};

    fn hold(tenant: &str, host: &str) -> DevelopmentLaneHold {
        DevelopmentLaneHold {
            schema_version: crate::epistemic_operations::DevelopmentLaneHoldSchemaVersion::V1,
            hold_id: format!("v1:{}", "a".repeat(64)),
            lane_id: "lane:test".into(),
            tenant_ref: tenant.into(),
            request_id: "request:test".into(),
            work_item_id: "work:test".into(),
            owner_id: "owner:test".into(),
            session_id: "session:test".into(),
            fairness_group: "fairness:test".into(),
            workspace_ref: "workspace:test".into(),
            repository_id: "repository:test".into(),
            base_ref: "refs/heads/main".into(),
            base_sha: "a".repeat(40),
            branch: "branch:test".into(),
            worktree_locator: "lanes/test".into(),
            host_target_kind: DevelopmentLaneHoldHostTargetKind::Local,
            host_target_alias: None,
            host_ref: host.into(),
            quota_policy_name: "default".into(),
            quota_policy_version: "1".into(),
            input_fingerprint: format!("v1:{}", "b".repeat(64)),
            predicted_disk_bytes: 10,
            observed_disk_bytes: 0,
            retained_disk_bytes: 0,
            active_count_charged: true,
            quota_charge: empty_charge(1),
            state: DevelopmentLaneHoldState::Active,
            attempt: 1,
            lease_epoch: 1,
            fencing_token: 1,
            work_item_fence: "fence:test".into(),
            hold_revision: 1,
            lifecycle_revision: 1,
            allocation_revision: 1,
            cleanup_revision: 0,
            expires_at_ms: 10_000,
            last_renewed_at_ms: 1_000,
            cleanup_work_item_id: None,
            cleanup_work_item_fence: None,
            cleanup_attempt: None,
            cleanup_lease_epoch: None,
            cleanup_fencing_token: None,
            tombstone: false,
        }
    }

    fn policy() -> DevelopmentLaneQuotaPolicy {
        DevelopmentLaneQuotaPolicy {
            schema_version:
                crate::epistemic_operations::DevelopmentLaneQuotaPolicySchemaVersion::V1,
            policy_name: "default".into(),
            policy_version: "1".into(),
            tenant_count_limit: 10,
            owner_count_limit: 10,
            session_count_limit: 10,
            workspace_count_limit: 10,
            repository_count_limit: 10,
            host_count_limit: 10,
            global_count_limit: 10,
            tenant_predicted_disk_bytes: 100,
            owner_predicted_disk_bytes: 100,
            session_predicted_disk_bytes: 100,
            workspace_predicted_disk_bytes: 100,
            repository_predicted_disk_bytes: 100,
            host_predicted_disk_bytes: 100,
            global_predicted_disk_bytes: 100,
            tenant_observed_disk_bytes: 100,
            owner_observed_disk_bytes: 100,
            session_observed_disk_bytes: 100,
            workspace_observed_disk_bytes: 100,
            repository_observed_disk_bytes: 100,
            host_observed_disk_bytes: 100,
            global_observed_disk_bytes: 100,
            tenant_retained_disk_bytes: 100,
            owner_retained_disk_bytes: 100,
            session_retained_disk_bytes: 100,
            workspace_retained_disk_bytes: 100,
            repository_retained_disk_bytes: 100,
            host_retained_disk_bytes: 100,
            global_retained_disk_bytes: 100,
            min_ttl_ms: 1,
            max_ttl_ms: 10_000,
            max_observation_staleness_ms: 100,
            drain_only: false,
        }
    }

    fn row(scope: Scope, value: DurableLaneCounter) -> ScopeCounter {
        ScopeCounter {
            key: format!("scope:{scope:?}"),
            scope,
            value,
        }
    }

    const TEST_GRAPH: &str = "graph-a";
    const TEST_NOW: u64 = 100;

    fn test_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "eg-development-lane-{label}-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ))
    }

    fn test_fingerprint(seed: char) -> String {
        format!("v1:{}", seed.to_string().repeat(64))
    }

    fn test_intent(
        tenant: &str,
        suffix: &str,
        branch: &str,
        worktree_locator: &str,
    ) -> DevelopmentLaneIntent {
        DevelopmentLaneIntent {
            schema_version: DevelopmentLaneIntentSchemaVersion::V1,
            tenant_ref: tenant.to_string(),
            request_id: format!("request:{suffix}"),
            lane_id: format!("lane:{suffix}"),
            repository_id: "repository:test".into(),
            base_ref: "refs/heads/main".into(),
            base_sha: "a".repeat(40),
            branch: branch.into(),
            host_target_kind: DevelopmentLaneIntentHostTargetKind::Local,
            host_target_alias: None,
            host_ref: "host:test".into(),
            resource_reservation_id: format!("resource:{suffix}"),
            workspace_ref: "workspace:test".into(),
            worktree_locator: worktree_locator.into(),
            owner_id: format!("owner:{suffix}"),
            session_id: format!("session:{suffix}"),
            fairness_group: "fairness:test".into(),
            quota_policy_name: "default".into(),
            quota_policy_version: "1".into(),
            predicted_disk_bytes: 10,
            ttl_ms: 1_000,
            input_fingerprint: test_fingerprint('b'),
        }
    }

    fn test_reserve_request(intent: DevelopmentLaneIntent) -> DevelopmentLaneReserveRequest {
        DevelopmentLaneReserveRequest {
            schema_version: DevelopmentLaneReserveRequestSchemaVersion::V1,
            tenant_ref: intent.tenant_ref.clone(),
            work_item_id: format!("work:{}", intent.request_id.trim_start_matches("request:")),
            owner_id: intent.owner_id.clone(),
            attempt: 1,
            lease_epoch: 1,
            fencing_token: 1,
            work_item_fence: format!("fence:{}", intent.request_id),
            intent,
            idempotency_key: "reserve:initial".into(),
            now_ms: TEST_NOW,
        }
    }

    fn seed_lane_work_item(
        db: &Database,
        request: &DevelopmentLaneReserveRequest,
    ) -> Result<(), String> {
        let wtx = db.begin_write().map_err(|error| error.to_string())?;
        let intent = &request.intent;
        let props = serde_json::json!({
            "node_type": "WorkItem",
            "kind": "lane.lifecycle",
            "tenant": request.tenant_ref,
            "status": "running",
            "lease_owner": request.owner_id,
            "last_lease_owner": request.owner_id,
            "attempt": request.attempt,
            // Keep the native retry regression below on the pre-DLQ path.
            "max_attempts": 2,
            "lease_epoch": request.lease_epoch,
            "fencing_token": request.fencing_token,
            "work_item_fence": request.work_item_fence,
            "lease_expires_at": 1000.0,
            "metadata": {
                "repository_work_item": {
                    "development_lane_intent": serde_json::to_value(intent)
                        .map_err(|error| error.to_string())?
                }
            }
        })
        .as_object()
        .ok_or_else(|| "lane test WorkItem properties are not an object".to_string())?
        .clone();
        let node_bytes = rmp_serde::to_vec_named(&props).map_err(|error| error.to_string())?;
        let requirement = ResourceRequirement {
            cpu_weight: 1,
            memory_mib: 1,
            disk_mib: intent.predicted_disk_bytes,
            process_slots: 1,
        };
        let record = ResourceReservationRecord {
            reservation_id: intent.resource_reservation_id.clone(),
            tenant_ref: request.tenant_ref.clone(),
            owner_id: request.owner_id.clone(),
            work_item_id: request.work_item_id.clone(),
            fence: request.work_item_fence.clone(),
            attempt: request.attempt,
            lease_epoch: request.lease_epoch,
            fencing_token: request.fencing_token,
            input_fingerprint: intent.input_fingerprint.clone(),
            host_ref: intent.host_ref.clone(),
            profile_name: "lane-profile".into(),
            profile_version: "1".into(),
            requirement: requirement.clone(),
            capacity_snapshot: ResourceCapacitySnapshot {
                cpu_weight: 10,
                memory_mib: 10,
                disk_mib: 10_000,
                process_slots: 10,
                host_revision: 1,
            },
            selected_target: ResourceTargetSnapshot {
                kind: ResourceTargetSnapshotKind::Local,
                alias: None,
                capability_labels: Vec::new(),
            },
            target_kind: ResourceReservationRecordTargetKind::Local,
            target_alias: None,
            repository_id: intent.repository_id.clone(),
            branch: intent.branch.clone(),
            concurrency_key: "lane".into(),
            concurrency_limit: None,
            repository_exclusive: false,
            branch_exclusive: false,
            required_labels: Vec::new(),
            anti_affinity: Vec::new(),
            fairness_group: intent.fairness_group.clone(),
            fairness_cost: 1,
            disk_low_watermark_mib: None,
            disk_high_watermark_mib: None,
            disk_policy_key: "lane".into(),
            reserved_at_ms: 1,
            expires_at_ms: 100_000,
            expected_host_revision: Some(1),
            expected_lifecycle_revision: None,
            state: ResourceReservationRecordState::Reserved,
            revision: 1,
            lifecycle_revision: 1,
            tombstone: false,
        };
        let resource = super::super::DurableResourceReservation {
            record,
            held_cpu_weight: requirement.cpu_weight,
            held_memory_mib: requirement.memory_mib,
            held_disk_mib: requirement.disk_mib,
            held_process_slots: requirement.process_slots,
            fairness_debt: 0,
        };
        let resource_bytes = resource_encode(&resource, DurableCrypto::none())?;
        {
            let mut nodes = wtx.open_table(NODES).map_err(|error| error.to_string())?;
            nodes
                .insert(
                    (TEST_GRAPH, request.work_item_id.as_str()),
                    node_bytes.as_slice(),
                )
                .map_err(|error| error.to_string())?;
        }
        {
            let mut resources = wtx
                .open_table(super::super::RESOURCE_RESERVATIONS)
                .map_err(|error| error.to_string())?;
            resources
                .insert(
                    (TEST_GRAPH, intent.resource_reservation_id.as_str()),
                    resource_bytes.as_slice(),
                )
                .map_err(|error| error.to_string())?;
        }
        wtx.commit().map_err(|error| error.to_string())
    }

    struct NativeLaneFixture {
        path: PathBuf,
        db: Database,
        reserve: DevelopmentLaneReserveRequest,
        remove_file_on_drop: bool,
    }

    impl Drop for NativeLaneFixture {
        fn drop(&mut self) {
            if self.remove_file_on_drop {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }

    impl NativeLaneFixture {
        fn new(policy: DevelopmentLaneQuotaPolicy) -> Self {
            let path = test_path("fixture");
            let db = Database::create(&path).expect("create lane database");
            {
                let wtx = db.begin_write().expect("begin lane schema transaction");
                super::super::initialize_canonical_tables(&wtx).expect("initialize lane tables");
                wtx.commit().expect("commit lane schema");
            }
            let reserve = test_reserve_request(test_intent(
                "tenant:a",
                "initial",
                "branch:initial",
                "lanes/initial",
            ));
            seed_lane_work_item(&db, &reserve).expect("seed linked resource reservation");
            let fixture = Self {
                path,
                db,
                reserve,
                remove_file_on_drop: true,
            };
            let result: DevelopmentLaneQuotaUpdateResult = fixture.decode(
                &fixture.commit(
                    Method::UpdateDevelopmentLaneQuota {
                        request: DevelopmentLaneQuotaUpdateRequest {
                            schema_version:
                                crate::epistemic_operations::DevelopmentLaneQuotaUpdateRequestSchemaVersion::V1,
                            tenant_ref: "tenant:a".into(),
                            policy,
                            expected_policy_revision: 0,
                            expected_policy_version: None,
                            idempotency_key: "policy:initial".into(),
                            now_ms: TEST_NOW,
                        },
                    },
                    TEST_NOW,
                ),
            );
            assert_eq!(
                result.decision,
                crate::epistemic_operations::DevelopmentLaneQuotaUpdateResultDecision::Accepted
            );
            fixture
        }

        fn close_for_reopen(mut self) -> PathBuf {
            self.remove_file_on_drop = false;
            let path = self.path.clone();
            drop(self);
            path
        }

        fn commit(&self, method: Method, now_ms: u64) -> Vec<u8> {
            commit_development_lane(&self.db, TEST_GRAPH, &method, now_ms, DurableCrypto::none())
                .expect("lane transaction")
        }

        fn decode<T: serde::de::DeserializeOwned>(&self, bytes: &[u8]) -> T {
            rmp_serde::from_slice(bytes).expect("typed lane result")
        }

        fn commit_work_item(
            &self,
            method: Method,
            batch_suffix: &str,
            committed_at_ms: u64,
        ) -> Result<MutationBatchCommit, String> {
            self.commit_work_item_with_crash(method, batch_suffix, committed_at_ms, None)
        }

        fn commit_work_item_with_crash(
            &self,
            method: Method,
            batch_suffix: &str,
            committed_at_ms: u64,
            crashpoint: Option<super::super::MutationBatchCrashpoint>,
        ) -> Result<MutationBatchCommit, String> {
            let batch = MutationBatch {
                schema_version: MUTATION_BATCH_VERSION,
                batch_id: format!("native-work-item:{batch_suffix}"),
                context: MutationRequestContext {
                    request_id: committed_at_ms,
                    principal: format!("principal:sha256:{}", "a".repeat(64)),
                    purpose: Some("native-lane-test".into()),
                    policy_fingerprint: None,
                    trace_id: None,
                },
                tenant: self.reserve.tenant_ref.clone(),
                graph: TEST_GRAPH.into(),
                placement_epoch: 0,
                idempotency_key: format!("native-work-item-idem:{batch_suffix}"),
                expected_graph_version: None,
                fencing_token: None,
                authoritative_state: None,
                operations: vec![MutationOperation {
                    ordinal: 0,
                    surface: MutationSurface::Job,
                    domain: MutationDomain::ControlPlane,
                    method,
                }],
                outbox: vec![MutationOutboxIntent {
                    topic: "native-lane.test".into(),
                    key: format!("native-work-item:{batch_suffix}"),
                    payload: Vec::new(),
                    headers: Default::default(),
                }],
                created_at_ms: committed_at_ms,
            };
            #[cfg(feature = "security")]
            let mut audit = super::super::AuditTailCache::new();
            super::super::commit_mutation_batch_inner(
                &self.db,
                TEST_GRAPH,
                &batch,
                None,
                None,
                None,
                None,
                committed_at_ms,
                DurableCrypto::none(),
                #[cfg(feature = "security")]
                &mut audit,
                true,
                crashpoint,
            )
        }

        fn compare_and_set_work_item_owner(
            &self,
            expected_owner: &str,
            updated_owner: &str,
            batch_suffix: &str,
            committed_at_ms: u64,
        ) -> Result<MutationBatchCommit, String> {
            let expected_graph_version =
                super::super::read_mutation_graph_version(&self.db, TEST_GRAPH)?.unwrap_or(0);
            let conditions_msgpack =
                rmp_serde::to_vec_named(&serde_json::json!({"lease_owner": expected_owner}))
                    .map_err(|error| error.to_string())?;
            let updates_msgpack =
                rmp_serde::to_vec_named(&serde_json::json!({"lease_owner": updated_owner}))
                    .map_err(|error| error.to_string())?;
            let batch = MutationBatch {
                schema_version: MUTATION_BATCH_VERSION,
                batch_id: format!("native-owner-cas:{batch_suffix}"),
                context: MutationRequestContext {
                    request_id: committed_at_ms,
                    principal: format!("principal:sha256:{}", "a".repeat(64)),
                    purpose: Some("native-lane-owner-test".into()),
                    policy_fingerprint: None,
                    trace_id: None,
                },
                tenant: self.reserve.tenant_ref.clone(),
                graph: TEST_GRAPH.into(),
                placement_epoch: 0,
                idempotency_key: format!("native-owner-cas-idem:{batch_suffix}"),
                expected_graph_version: Some(expected_graph_version),
                fencing_token: None,
                authoritative_state: None,
                operations: vec![MutationOperation {
                    ordinal: 0,
                    surface: MutationSurface::Graph,
                    domain: MutationDomain::GraphRows,
                    method: Method::CompareAndSetNodeFields {
                        node_id: self.reserve.work_item_id.clone(),
                        conditions_msgpack,
                        updates_msgpack,
                    },
                }],
                outbox: vec![MutationOutboxIntent {
                    topic: "native-lane.test".into(),
                    key: format!("native-owner-cas:{batch_suffix}"),
                    payload: Vec::new(),
                    headers: Default::default(),
                }],
                created_at_ms: committed_at_ms,
            };
            #[cfg(feature = "security")]
            let mut audit = super::super::AuditTailCache::new();
            super::super::commit_mutation_batch_inner(
                &self.db,
                TEST_GRAPH,
                &batch,
                None,
                None,
                None,
                None,
                committed_at_ms,
                DurableCrypto::none(),
                #[cfg(feature = "security")]
                &mut audit,
                true,
                None,
            )
        }

        fn commit_work_item_result(
            &self,
            outcome: &str,
            retryable: bool,
            batch_suffix: &str,
            committed_at_ms: u64,
        ) -> Result<MutationBatchCommit, String> {
            self.commit_work_item_result_with_tuple(
                self.reserve.lease_epoch,
                self.reserve.fencing_token,
                outcome,
                retryable,
                batch_suffix,
                committed_at_ms,
            )
        }

        fn commit_work_item_result_with_tuple(
            &self,
            lease_epoch: u64,
            fencing_token: u64,
            outcome: &str,
            retryable: bool,
            batch_suffix: &str,
            committed_at_ms: u64,
        ) -> Result<MutationBatchCommit, String> {
            self.commit_work_item(
                Method::CommitWorkItemResult {
                    tenant: self.reserve.tenant_ref.clone(),
                    work_item_id: self.reserve.work_item_id.clone(),
                    worker_id: self.reserve.owner_id.clone(),
                    lease_epoch,
                    fencing_token,
                    idempotency_key: format!("work-item:{batch_suffix}"),
                    outcome: outcome.into(),
                    result_ref: None,
                    error_ref: None,
                    retryable,
                    now_ms: committed_at_ms,
                },
                batch_suffix,
                committed_at_ms,
            )
        }

        fn cancel_work_item(
            &self,
            batch_suffix: &str,
            committed_at_ms: u64,
        ) -> Result<MutationBatchCommit, String> {
            self.cancel_work_item_with_crash(batch_suffix, committed_at_ms, None)
        }

        fn cancel_work_item_with_crash(
            &self,
            batch_suffix: &str,
            committed_at_ms: u64,
            crashpoint: Option<super::super::MutationBatchCrashpoint>,
        ) -> Result<MutationBatchCommit, String> {
            self.commit_work_item_with_crash(
                Method::CancelWorkItem {
                    tenant: self.reserve.tenant_ref.clone(),
                    work_item_id: self.reserve.work_item_id.clone(),
                    idempotency_key: format!("cancel-item:{batch_suffix}"),
                    reason_ref: Some("reason:test".into()),
                    now_ms: committed_at_ms,
                },
                batch_suffix,
                committed_at_ms,
                crashpoint,
            )
        }

        fn reserve_method(&self, key: &str) -> Method {
            let mut request = self.reserve.clone();
            request.idempotency_key = key.into();
            Method::ReserveDevelopmentLane { request }
        }

        fn candidate(
            &self,
            suffix: &str,
            branch: &str,
            worktree_locator: &str,
        ) -> DevelopmentLaneReserveRequest {
            self.candidate_for_tenant("tenant:a", suffix, branch, worktree_locator)
        }

        fn candidate_for_tenant(
            &self,
            tenant: &str,
            suffix: &str,
            branch: &str,
            worktree_locator: &str,
        ) -> DevelopmentLaneReserveRequest {
            let request =
                test_reserve_request(test_intent(tenant, suffix, branch, worktree_locator));
            seed_lane_work_item(&self.db, &request).expect("seed lane candidate");
            request
        }

        fn update_policy(
            &self,
            tenant: &str,
            policy: DevelopmentLaneQuotaPolicy,
            expected_policy_revision: u64,
            idempotency_key: &str,
            now_ms: u64,
        ) -> DevelopmentLaneQuotaUpdateResult {
            self.decode(&self.commit(
                Method::UpdateDevelopmentLaneQuota {
                    request: DevelopmentLaneQuotaUpdateRequest {
                        schema_version:
                            crate::epistemic_operations::DevelopmentLaneQuotaUpdateRequestSchemaVersion::V1,
                        tenant_ref: tenant.into(),
                        policy,
                        expected_policy_revision,
                        expected_policy_version: None,
                        idempotency_key: idempotency_key.into(),
                        now_ms,
                    },
                },
                now_ms,
            ))
        }

        fn mutate_work_item<F>(&self, work_item_id: &str, mutate: F)
        where
            F: FnOnce(&mut serde_json::Map<String, serde_json::Value>),
        {
            let wtx = self.db.begin_write().expect("begin WorkItem mutation");
            {
                let mut nodes = wtx.open_table(NODES).expect("open WorkItem table");
                let mut props: serde_json::Map<String, serde_json::Value> = {
                    let bytes = nodes
                        .get((TEST_GRAPH, work_item_id))
                        .expect("read WorkItem")
                        .expect("WorkItem exists");
                    decode_durable(bytes.value()).expect("decode WorkItem")
                };
                mutate(&mut props);
                let encoded = rmp_serde::to_vec_named(&props).expect("encode WorkItem");
                nodes
                    .insert((TEST_GRAPH, work_item_id), encoded.as_slice())
                    .expect("write WorkItem");
            }
            wtx.commit().expect("commit WorkItem mutation");
        }

        fn seed_cleanup_work_item(
            &self,
            hold: &DevelopmentLaneHold,
            cleanup_work_item_id: &str,
            cleanup_work_item_fence: &str,
        ) {
            let wtx = self.db.begin_write().expect("begin cleanup WorkItem seed");
            let correlation = DevelopmentLaneCleanupIntent {
                schema_version: DevelopmentLaneCleanupIntentSchemaVersion::V1,
                hold_id: hold.hold_id.clone(),
                lane_id: hold.lane_id.clone(),
                expected_hold_revision: hold.hold_revision,
            };
            let props = serde_json::json!({
                "node_type": "WorkItem",
                "kind": "lane.cleanup",
                "tenant": hold.tenant_ref,
                "status": "running",
                "lease_owner": "cleanup-controller",
                "last_lease_owner": "cleanup-controller",
                "attempt": 1,
                "lease_epoch": 1,
                "fencing_token": 1,
                "work_item_fence": cleanup_work_item_fence,
                "lease_expires_at": 1000.0,
                "metadata": {
                    "repository_work_item": {
                        "development_lane_cleanup": serde_json::to_value(correlation)
                            .expect("encode cleanup correlation")
                    }
                }
            });
            let encoded = rmp_serde::to_vec_named(&props).expect("encode cleanup WorkItem");
            {
                let mut nodes = wtx.open_table(NODES).expect("open WorkItem table");
                nodes
                    .insert((TEST_GRAPH, cleanup_work_item_id), encoded.as_slice())
                    .expect("write cleanup WorkItem");
            }
            wtx.commit().expect("commit cleanup WorkItem");
        }
    }

    #[test]
    fn global_scope_is_graph_wide_and_worktree_is_host_scoped() {
        let a = hold("tenant:a", "host:a");
        let mut b = a.clone();
        b.tenant_ref = "tenant:b".into();
        b.host_ref = "host:b".into();
        assert_eq!(scope_key(Scope::Global, &a), "global\0*");
        assert_eq!(scope_key(Scope::Global, &a), scope_key(Scope::Global, &b));
        assert_ne!(scope_key(Scope::Tenant, &a), scope_key(Scope::Tenant, &b));
        assert_ne!(worktree_key(&a), worktree_key(&b));
    }

    #[test]
    fn lane_index_is_tenant_scoped_for_reused_lane_ids() {
        let fixture = NativeLaneFixture::new(policy());
        let tenant_b_policy =
            fixture.update_policy("tenant:b", policy(), 0, "policy:tenant-b", TEST_NOW);
        assert_eq!(
            tenant_b_policy.decision,
            crate::epistemic_operations::DevelopmentLaneQuotaUpdateResultDecision::Accepted
        );

        let mut first = test_reserve_request(test_intent(
            "tenant:a",
            "shared-a",
            "branch:shared-a",
            "lanes/shared-a",
        ));
        first.intent.lane_id = "lane:shared".into();
        seed_lane_work_item(&fixture.db, &first).expect("seed tenant-a shared lane");
        let mut second = test_reserve_request(test_intent(
            "tenant:b",
            "shared-b",
            "branch:shared-b",
            "lanes/shared-b",
        ));
        second.intent.lane_id = "lane:shared".into();
        seed_lane_work_item(&fixture.db, &second).expect("seed tenant-b shared lane");

        let first_result: DevelopmentLaneResult = fixture.decode(&fixture.commit(
            Method::ReserveDevelopmentLane {
                request: first.clone(),
            },
            TEST_NOW,
        ));
        let second_result: DevelopmentLaneResult = fixture.decode(&fixture.commit(
            Method::ReserveDevelopmentLane {
                request: second.clone(),
            },
            TEST_NOW,
        ));
        assert_eq!(
            first_result.decision,
            DevelopmentLaneResultDecision::Accepted
        );
        assert_eq!(
            second_result.decision,
            DevelopmentLaneResultDecision::Accepted
        );

        let first_hold = first_result.hold.expect("tenant-a shared hold");
        let second_hold = second_result.hold.expect("tenant-b shared hold");
        let rtx = fixture.db.begin_read().expect("read tenant-scoped lanes");
        let lane_index = rtx.open_table(LANE_INDEX).expect("open lane index");
        assert_eq!(
            lane_index
                .get((TEST_GRAPH, "tenant:a", "lane:shared"))
                .expect("read tenant-a lane")
                .map(|value| value.value().to_string()),
            Some(first_hold.hold_id)
        );
        assert_eq!(
            lane_index
                .get((TEST_GRAPH, "tenant:b", "lane:shared"))
                .expect("read tenant-b lane")
                .map(|value| value.value().to_string()),
            Some(second_hold.hold_id)
        );
    }

    #[test]
    fn observation_freshness_is_per_hold_and_boundary_exact() {
        let mut row = DurableLaneHold {
            hold: hold("tenant:a", "host:a"),
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
            resource_reservation_id: "reservation:test".into(),
            ttl_ms: 1_000,
        };
        let policy = policy();
        assert!(!observation_fresh(&row, &policy, 1_000));
        row.last_observed_at_ms = Some(900);
        assert!(observation_fresh(&row, &policy, 1_000));
        assert!(!observation_fresh(&row, &policy, 1_001));
        row.last_observed_at_ms = Some(1_001);
        assert!(!observation_fresh(&row, &policy, 1_000));
        let mut fresh = row;
        fresh.last_observed_at_ms = Some(1_000);
        let stale = DurableLaneHold {
            hold: hold("tenant:a", "host:a"),
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
            resource_reservation_id: "reservation:stale".into(),
            ttl_ms: 1_000,
        };
        assert!(observation_fresh(&fresh, &policy, 1_000));
        assert!(!observation_fresh(&stale, &policy, 1_000));
    }

    #[test]
    fn observed_and_retained_pressure_are_separate_from_prediction() {
        let mut policy = policy();
        let mut observed = DurableLaneCounter {
            observed_disk_bytes: 101,
            ..DurableLaneCounter::default()
        };
        let counters = vec![row(Scope::Tenant, observed.clone())];
        assert_eq!(
            reserve_counter_check(&counters, &policy, 1),
            Err(LaneDecision::Quota)
        );
        observed.observed_disk_bytes = 0;
        observed.retained_disk_bytes = 101;
        assert_eq!(
            reserve_counter_check(&[row(Scope::Tenant, observed)], &policy, 1),
            Err(LaneDecision::Quota)
        );
        policy.tenant_observed_disk_bytes = 200;
        assert!(reserve_counter_check(
            &[row(Scope::Tenant, DurableLaneCounter::default())],
            &policy,
            1
        )
        .is_ok());
        let overflowing = DurableLaneCounter {
            active_count: u64::MAX,
            ..DurableLaneCounter::default()
        };
        assert_eq!(
            reserve_counter_check(&[row(Scope::Tenant, overflowing)], &policy, 1),
            Err(LaneDecision::Quota)
        );
    }

    #[test]
    fn checked_counter_arithmetic_refuses_overflow_and_underflow() {
        assert_eq!(
            adjust(u64::MAX, 1, true, "test"),
            Err("development lane test counter overflow".into())
        );
        assert_eq!(
            adjust(0, 1, false, "test"),
            Err("development lane test counter underflow".into())
        );
    }

    #[test]
    fn checkpoint_restore_status_mapping_is_exact_for_retained_states() {
        let active = DurableLaneHold {
            hold: hold("tenant:a", "host:a"),
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
            resource_reservation_id: "reservation:active".into(),
            ttl_ms: 1_000,
        };
        assert!(checkpoint_lifecycle_status_matches(&active, "running"));
        assert!(!checkpoint_lifecycle_status_matches(&active, "ready"));
        assert!(!checkpoint_lifecycle_status_matches(&active, "succeeded"));

        let mut cleanup_pending = active.clone();
        cleanup_pending.hold.state = DevelopmentLaneHoldState::CleanupPending;
        cleanup_pending.hold.tombstone = true;
        cleanup_pending.hold.active_count_charged = false;
        cleanup_pending.hold.retained_disk_bytes = 10;
        cleanup_pending.terminal_state = Some("succeeded".into());
        cleanup_pending.terminal_expected_hold_revision = Some(1);
        assert!(checkpoint_lifecycle_status_matches(
            &cleanup_pending,
            "succeeded"
        ));
        assert!(!checkpoint_lifecycle_status_matches(
            &cleanup_pending,
            "failed"
        ));
        assert!(!checkpoint_lifecycle_status_matches(
            &cleanup_pending,
            "pending"
        ));

        let mut expired = active.clone();
        expired.hold.state = DevelopmentLaneHoldState::Expired;
        expired.hold.tombstone = true;
        expired.hold.active_count_charged = false;
        expired.hold.retained_disk_bytes = 10;
        assert!(checkpoint_lifecycle_status_matches(&expired, "running"));
        assert!(checkpoint_lifecycle_status_matches(&expired, "succeeded"));
        expired.terminal_state = Some("succeeded".into());
        assert!(!checkpoint_lifecycle_status_matches(&expired, "succeeded"));

        let mut released = cleanup_pending.clone();
        released.hold.state = DevelopmentLaneHoldState::Released;
        assert!(checkpoint_lifecycle_status_matches(&released, "succeeded"));
        released.terminal_state = Some("failed".into());
        assert!(!checkpoint_lifecycle_status_matches(&released, "succeeded"));

        let mut cleaned = released.clone();
        cleaned.hold.state = DevelopmentLaneHoldState::Cleaned;
        cleaned.hold.retained_disk_bytes = 0;
        cleaned.terminal_state = Some("succeeded".into());
        assert!(checkpoint_lifecycle_status_matches(&cleaned, "succeeded"));
        assert!(!checkpoint_lifecycle_status_matches(&cleaned, "running"));
    }

    #[test]
    fn corrupt_durable_rows_fail_closed_before_native_use() {
        let fixture = NativeLaneFixture::new(policy());
        {
            let wtx = fixture.db.begin_write().expect("begin corrupt row seed");
            let mut policies = wtx.open_table(POLICIES).expect("open corrupt policies");
            let mut invalid_policy = policy();
            invalid_policy.tenant_count_limit = 0;
            let invalid_policy_row = DurableLanePolicy {
                policy: invalid_policy,
                policy_revision: 1,
                global_policy_revision: 1,
            };
            let policy_bytes = resource_encode(&invalid_policy_row, DurableCrypto::none())
                .expect("encode corrupt policy");
            policies
                .insert((TEST_GRAPH, "tenant:a"), policy_bytes.as_slice())
                .expect("write corrupt policy");
            drop(policies);

            let mut counters = wtx.open_table(COUNTERS).expect("open corrupt counters");
            let invalid_counter = DurableLaneCounter {
                observed_disk_bytes: MAX_DISK_BYTES + 1,
                ..DurableLaneCounter::default()
            };
            let counter_bytes = resource_encode(&invalid_counter, DurableCrypto::none())
                .expect("encode corrupt counter");
            counters
                .insert((TEST_GRAPH, "corrupt-counter"), counter_bytes.as_slice())
                .expect("write corrupt counter");
            drop(counters);

            let mut invocations = wtx
                .open_table(INVOCATIONS)
                .expect("open corrupt invocations");
            let invocation_method = fixture.reserve_method("corrupt-invocation");
            let invalid_invocation = DurableLaneInvocation {
                method: method_name(&invocation_method).to_string(),
                request_digest: request_digest(&invocation_method)
                    .expect("digest corrupt invocation"),
                result: vec![0; 64 * 1024 + 1],
            };
            let invocation_bytes = resource_encode(&invalid_invocation, DurableCrypto::none())
                .expect("encode corrupt invocation");
            invocations
                .insert(
                    (TEST_GRAPH, "tenant:a", "corrupt-invocation"),
                    invocation_bytes.as_slice(),
                )
                .expect("write corrupt invocation");
            drop(invocations);

            let mut holds = wtx.open_table(HOLDS).expect("open corrupt holds");
            let mut invalid_hold = DurableLaneHold {
                hold: hold("tenant:a", "host:a"),
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
                resource_reservation_id: "reservation:corrupt".into(),
                ttl_ms: 1_000,
            };
            invalid_hold.hold.worktree_locator = "../escape".into();
            let hold_bytes =
                resource_encode(&invalid_hold, DurableCrypto::none()).expect("encode corrupt hold");
            holds
                .insert(
                    (TEST_GRAPH, invalid_hold.hold.hold_id.as_str()),
                    hold_bytes.as_slice(),
                )
                .expect("write corrupt hold");
            drop(holds);
            wtx.commit().expect("commit corrupt rows");
        }

        let rtx = fixture.db.begin_read().expect("read corrupt rows");
        let policies = rtx.open_table(POLICIES).expect("read corrupt policies");
        assert!(load_policy(&policies, TEST_GRAPH, "tenant:a", DurableCrypto::none()).is_err());
        drop(policies);
        let counters = rtx.open_table(COUNTERS).expect("read corrupt counters");
        assert!(load_counter(
            &counters,
            TEST_GRAPH,
            "corrupt-counter",
            Scope::Tenant,
            1,
            1,
            DurableCrypto::none()
        )
        .is_err());
        drop(counters);
        let invocations = rtx
            .open_table(INVOCATIONS)
            .expect("read corrupt invocations");
        let invocation_method = fixture.reserve_method("corrupt-invocation");
        assert!(load_invocation(
            &invocations,
            TEST_GRAPH,
            "tenant:a",
            "corrupt-invocation",
            &invocation_method,
            DurableCrypto::none()
        )
        .is_err());
        drop(invocations);
        let holds = rtx.open_table(HOLDS).expect("read corrupt holds");
        let invalid_hold_id = hold("tenant:a", "host:a").hold_id;
        assert!(hold_load(&holds, TEST_GRAPH, &invalid_hold_id, DurableCrypto::none()).is_err());
        drop(holds);
    }

    #[test]
    fn invocation_replay_retention_is_bounded_and_keeps_the_current_key() {
        let fixture = NativeLaneFixture::new(policy());
        {
            let wtx = fixture
                .db
                .begin_write()
                .expect("begin invocation retention");
            let mut invocations = wtx.open_table(INVOCATIONS).expect("open invocations");
            // Simulate a pre-existing overfull/corrupt replay range.  The
            // repair must inspect the full bounded tenant prefix, not only a
            // MAX+2 prefix, and must retain the current key even though it
            // sorts before the newest lexical window.
            for index in 0..1_000u64 {
                let key = format!("invocation:{index:04}");
                let method = fixture.reserve_method(&key);
                let row = DurableLaneInvocation {
                    method: method_name(&method).to_string(),
                    request_digest: request_digest(&method).expect("digest pre-existing replay"),
                    result: b"bounded-result".to_vec(),
                };
                let bytes = resource_encode(&row, DurableCrypto::none())
                    .expect("encode pre-existing replay");
                invocations
                    .insert((TEST_GRAPH, "tenant:a", key.as_str()), bytes.as_slice())
                    .expect("seed pre-existing replay");
            }
            let other_method = fixture.reserve_method("tenant-b-current");
            let other_row = DurableLaneInvocation {
                method: method_name(&other_method).to_string(),
                request_digest: request_digest(&other_method).expect("digest other replay"),
                result: b"other-result".to_vec(),
            };
            let other_bytes = resource_encode(&other_row, DurableCrypto::none())
                .expect("encode other tenant replay");
            invocations
                .insert(
                    (TEST_GRAPH, "tenant:b", "tenant-b-current"),
                    other_bytes.as_slice(),
                )
                .expect("seed other tenant replay");
            let current_key = "aaa-current";
            let current_method = fixture.reserve_method(current_key);
            store_invocation(
                &mut invocations,
                TEST_GRAPH,
                "tenant:a",
                current_key,
                &current_method,
                b"current-result",
                DurableCrypto::none(),
            )
            .expect("repair bounded invocation range");
            drop(invocations);
            wtx.commit().expect("commit invocation retention");
        }
        let rtx = fixture.db.begin_read().expect("read invocation retention");
        let invocations = rtx
            .open_table(INVOCATIONS)
            .expect("open retained invocations");
        let mut retained = 0usize;
        for row in invocations
            .range((TEST_GRAPH, "tenant:a", "")..)
            .expect("scan retained invocations")
        {
            let (key, _) = row.expect("read retained invocation");
            let (graph, tenant, _) = key.value();
            if graph != TEST_GRAPH || tenant != "tenant:a" {
                break;
            }
            retained += 1;
        }
        assert_eq!(retained, MAX_INVOCATIONS_PER_TENANT);
        assert!(invocations
            .get((TEST_GRAPH, "tenant:a", "aaa-current"))
            .expect("lookup current replay key")
            .is_some());
        let current_method = fixture.reserve_method("aaa-current");
        assert_eq!(
            load_invocation(
                &invocations,
                TEST_GRAPH,
                "tenant:a",
                "aaa-current",
                &current_method,
                DurableCrypto::none(),
            )
            .expect("load current replay"),
            Some((true, b"current-result".to_vec()))
        );
        assert!(invocations
            .get((TEST_GRAPH, "tenant:b", "tenant-b-current"))
            .expect("lookup other tenant replay")
            .is_some());
        drop(invocations);
    }

    #[test]
    fn global_policy_allows_local_limits_but_freezes_shared_controls() {
        let first = policy();
        let mut local = first.clone();
        local.owner_count_limit = 2;
        assert!(global_policy_equal(&first, &local));
        local.global_count_limit = 2;
        assert!(!global_policy_equal(&first, &local));
        local = first.clone();
        local.drain_only = true;
        assert!(!global_policy_equal(&first, &local));
    }

    #[test]
    fn policy_reduction_checks_every_maintained_scope() {
        let mut limits = policy();
        limits.owner_count_limit = 1;
        let owner_pressure = row(
            Scope::Owner,
            DurableLaneCounter {
                active_count: 2,
                ..DurableLaneCounter::default()
            },
        );
        assert!(policy_pressure(&[owner_pressure], &limits));
        limits.owner_count_limit = 3;
        assert!(!policy_pressure(
            &[row(
                Scope::Owner,
                DurableLaneCounter {
                    active_count: 2,
                    ..DurableLaneCounter::default()
                }
            )],
            &limits
        ));
        let host_pressure = row(
            Scope::Host,
            DurableLaneCounter {
                retained_disk_bytes: 101,
                ..DurableLaneCounter::default()
            },
        );
        assert!(policy_pressure(&[host_pressure], &policy()));
    }

    #[test]
    fn native_reserve_links_live_resource_and_replays_or_refuses_input_atomically() {
        let fixture = NativeLaneFixture::new(policy());
        let accepted: DevelopmentLaneResult =
            fixture.decode(&fixture.commit(fixture.reserve_method("reserve:one"), TEST_NOW));
        assert_eq!(accepted.decision, DevelopmentLaneResultDecision::Accepted);
        let hold = accepted.hold.clone().expect("accepted hold");
        assert_eq!(hold.state, DevelopmentLaneHoldState::Active);
        assert_eq!(hold.host_ref, REDACTED_PRIVATE_ID);
        assert_eq!(hold.worktree_locator, REDACTED_PRIVATE_ID);
        assert!(hold.host_target_alias.is_none());

        let replay: DevelopmentLaneResult =
            fixture.decode(&fixture.commit(fixture.reserve_method("reserve:one"), TEST_NOW));
        assert_eq!(replay, accepted);

        let mut conflict_request = fixture.reserve.clone();
        conflict_request.idempotency_key = "reserve:one".into();
        conflict_request.intent.branch = "branch:conflict".into();
        let conflict: DevelopmentLaneResult = fixture.decode(&fixture.commit(
            Method::ReserveDevelopmentLane {
                request: conflict_request,
            },
            TEST_NOW,
        ));
        assert_eq!(
            conflict.decision,
            DevelopmentLaneResultDecision::InputConflict
        );
        let query = read_development_lane(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneQueryRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneQueryRequestSchemaVersion::V1,
                tenant_ref: "tenant:a".into(),
                hold_id: hold.hold_id,
                now_ms: 0,
            },
            TEST_NOW,
            DurableCrypto::none(),
        )
        .expect("query accepted hold");
        assert_eq!(query.decision, DevelopmentLaneQueryResultDecision::Accepted);
    }

    #[test]
    fn native_work_item_authority_rejects_generic_kind_and_fence_aliases() {
        let fixture = NativeLaneFixture::new(policy());
        let work_item_id = fixture.reserve.work_item_id.clone();
        fixture.mutate_work_item(&work_item_id, |props| {
            props.remove("kind");
            props.insert("work_item_kind".into(), serde_json::json!("lane.lifecycle"));
        });
        let mut kind_request = fixture.reserve.clone();
        kind_request.idempotency_key = "reserve:generic-kind".into();
        let kind_refusal: DevelopmentLaneResult = fixture.decode(&fixture.commit(
            Method::ReserveDevelopmentLane {
                request: kind_request,
            },
            TEST_NOW,
        ));
        assert_eq!(
            kind_refusal.decision,
            DevelopmentLaneResultDecision::WrongKind
        );

        let fixture = NativeLaneFixture::new(policy());
        let work_item_id = fixture.reserve.work_item_id.clone();
        fixture.mutate_work_item(&work_item_id, |props| {
            props.remove("work_item_fence");
            props.insert("fence".into(), serde_json::json!("fence:request:initial"));
        });
        let mut fence_request = fixture.reserve.clone();
        fence_request.idempotency_key = "reserve:generic-fence".into();
        let fence_refusal: DevelopmentLaneResult = fixture.decode(&fixture.commit(
            Method::ReserveDevelopmentLane {
                request: fence_request,
            },
            TEST_NOW,
        ));
        assert_eq!(
            fence_refusal.decision,
            DevelopmentLaneResultDecision::WrongFence
        );

        let fixture = NativeLaneFixture::new(policy());
        let work_item_id = fixture.reserve.work_item_id.clone();
        let forged_intent = serde_json::to_value(&fixture.reserve.intent)
            .expect("encode forged generic lane intent");
        fixture.mutate_work_item(&work_item_id, |props| {
            if let Some(metadata) = props
                .get_mut("metadata")
                .and_then(|value| value.as_object_mut())
            {
                metadata.remove("repository_work_item");
            }
            props.insert("development_lane_intent".into(), forged_intent);
        });
        let mut intent_request = fixture.reserve.clone();
        intent_request.idempotency_key = "reserve:generic-intent".into();
        let intent_refusal: DevelopmentLaneResult = fixture.decode(&fixture.commit(
            Method::ReserveDevelopmentLane {
                request: intent_request,
            },
            TEST_NOW,
        ));
        assert_eq!(
            intent_refusal.decision,
            DevelopmentLaneResultDecision::InputConflict
        );
    }

    #[test]
    fn graph_lifecycle_clear_fails_closed_while_lane_hold_is_live() {
        let fixture = NativeLaneFixture::new(policy());
        let accepted: DevelopmentLaneResult =
            fixture.decode(&fixture.commit(fixture.reserve_method("reserve:clear-live"), TEST_NOW));
        assert_eq!(accepted.decision, DevelopmentLaneResultDecision::Accepted);
        let wtx = fixture.db.begin_write().expect("begin graph clear guard");
        let refusal = clear_native_graph_rows_in_wtx(&wtx, TEST_GRAPH, DurableCrypto::none());
        assert!(
            refusal.is_err(),
            "live lane authority must block graph clear"
        );
        drop(wtx);

        let hold = accepted
            .hold
            .expect("live hold remains after refused clear");
        let query = read_development_lane(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneQueryRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneQueryRequestSchemaVersion::V1,
                tenant_ref: hold.tenant_ref,
                hold_id: hold.hold_id,
                now_ms: 0,
            },
            TEST_NOW,
            DurableCrypto::none(),
        )
        .expect("query live hold after refused clear");
        assert_eq!(query.decision, DevelopmentLaneQueryResultDecision::Accepted);
    }

    #[test]
    fn authoritative_snapshot_and_row_delta_paths_refuse_orphaning_lane_work_item() {
        use crate::mutation_batch::{
            MutationDomain, MutationOperation, MutationOutboxIntent, MutationRequestContext,
            MutationStateDescriptor, MutationSurface, MUTATION_BATCH_VERSION,
        };
        use sha2::{Digest, Sha256};

        let fixture = NativeLaneFixture::new(policy());
        let accepted: DevelopmentLaneResult =
            fixture.decode(&fixture.commit(fixture.reserve_method("reserve:state-path"), TEST_NOW));
        let hold = accepted.hold.expect("state-path hold");
        let mut linked = crate::graph::GraphCore::new().snapshot();
        {
            let rtx = fixture.db.begin_read().expect("read linked WorkItem");
            let nodes = rtx.open_table(NODES).expect("open linked WorkItem table");
            let bytes = nodes
                .get((TEST_GRAPH, hold.work_item_id.as_str()))
                .expect("lookup linked WorkItem")
                .expect("linked WorkItem exists")
                .value()
                .to_vec();
            linked
                .nodes
                .push((hold.work_item_id.clone(), std::sync::Arc::new(bytes)));
        }

        let make_batch =
            |batch_id: &str, key: &str, algorithm: &str, state: &[u8], source_version: u64| {
                crate::mutation_batch::MutationBatch {
                    schema_version: MUTATION_BATCH_VERSION,
                    batch_id: batch_id.into(),
                    context: MutationRequestContext {
                        request_id: 700,
                        principal: format!("principal:sha256:{}", "a".repeat(64)),
                        purpose: None,
                        policy_fingerprint: None,
                        trace_id: None,
                    },
                    tenant: "tenant:a".into(),
                    graph: TEST_GRAPH.into(),
                    placement_epoch: 1,
                    idempotency_key: key.into(),
                    expected_graph_version: Some(source_version),
                    fencing_token: Some(1),
                    authoritative_state: Some(MutationStateDescriptor {
                        algorithm: algorithm.into(),
                        digest: hex::encode(Sha256::digest(state)),
                        source_graph_version: source_version,
                        target_graph_version: source_version + 1,
                    }),
                    operations: vec![MutationOperation {
                        ordinal: 0,
                        surface: MutationSurface::Query,
                        domain: MutationDomain::GraphSnapshot,
                        method: Method::ApplyMutation {
                            event_type: "authoritative_state_operation".into(),
                            query: "sha256:state-path".into(),
                        },
                    }],
                    outbox: vec![MutationOutboxIntent {
                        topic: "state-path.test".into(),
                        key: batch_id.into(),
                        payload: Vec::new(),
                        headers: std::collections::BTreeMap::new(),
                    }],
                    created_at_ms: TEST_NOW,
                }
            };

        let mut orphaned = linked.clone();
        orphaned.nodes.clear();
        let orphaned_state = orphaned.to_msgpack().expect("encode orphaned snapshot");
        let orphaned_batch = make_batch(
            "state-path-orphaned",
            "state-path-orphaned",
            "sha256",
            &orphaned_state,
            0,
        );
        #[cfg(feature = "security")]
        let mut orphaned_audit = super::super::AuditTailCache::new();
        assert!(super::super::commit_mutation_batch_state(
            &fixture.db,
            super::super::StateCommitInput {
                graph_fname: TEST_GRAPH,
                batch: &orphaned_batch,
                authoritative_state_msgpack: &orphaned_state,
                result_msgpack: None,
                committed_at_ms: TEST_NOW,
                audited: true,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut orphaned_audit,
        )
        .is_err());

        // RMDD-29's native WorkItem-authority migration
        // (`work_item_capability::validate_snapshot_nodes`, invoked from
        // `commit_mutation_batch_state` for every `AuthoritativeGraphState::Snapshot`) now
        // unconditionally refuses any state-snapshot commit containing a WorkItem-shaped node
        // whose `status` is not `submitted`/`ready` -- by design, a snapshot commit always
        // purges native claim state, so it can never carry forward an ACTIVE lease. `linked`
        // carries this fixture's live (`status: "running"`) lane WorkItem, so this "exact
        // linked WorkItem" commit -- which predates that migration and originally expected
        // success -- is now ALSO refused, for a broader (not lane-specific) reason than
        // `orphaned` above. The invariant this test exists to prove -- a state-snapshot commit
        // can never orphan OR silently carry forward a live lane authority -- holds a fortiori:
        // NEITHER commit lands, and the query below proves the original hold is completely
        // untouched.
        let linked_state = linked.to_msgpack().expect("encode linked snapshot");
        let linked_batch = make_batch(
            "state-path-linked",
            "state-path-linked",
            "sha256",
            &linked_state,
            0,
        );
        #[cfg(feature = "security")]
        let mut linked_audit = super::super::AuditTailCache::new();
        assert!(super::super::commit_mutation_batch_state(
            &fixture.db,
            super::super::StateCommitInput {
                graph_fname: TEST_GRAPH,
                batch: &linked_batch,
                authoritative_state_msgpack: &linked_state,
                result_msgpack: None,
                committed_at_ms: TEST_NOW,
                audited: true,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut linked_audit,
        )
        .is_err());

        // Neither `orphaned` nor `linked` above ever committed, so the durable graph version
        // is still 0 (unchanged from `commit_ops` never being called on this fixture's graph).
        let after = crate::graph::GraphCore::new().snapshot();
        let delta = crate::graph_delta::GraphRowDelta::between(&linked, &after)
            .expect("build orphaning row delta");
        let delta_state = delta.to_msgpack().expect("encode orphaning row delta");
        let delta_batch = make_batch(
            "state-path-delta",
            "state-path-delta",
            crate::graph_delta::ROW_DELTA_ALGORITHM,
            &delta_state,
            0,
        );
        #[cfg(feature = "security")]
        let mut delta_audit = super::super::AuditTailCache::new();
        assert!(super::super::commit_mutation_batch_state(
            &fixture.db,
            super::super::StateCommitInput {
                graph_fname: TEST_GRAPH,
                batch: &delta_batch,
                authoritative_state_msgpack: &delta_state,
                result_msgpack: None,
                committed_at_ms: TEST_NOW,
                audited: true,
            },
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut delta_audit,
        )
        .is_err());

        let query = read_development_lane(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneQueryRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneQueryRequestSchemaVersion::V1,
                tenant_ref: hold.tenant_ref,
                hold_id: hold.hold_id,
                now_ms: 0,
            },
            TEST_NOW,
            DurableCrypto::none(),
        )
        .expect("query state-path hold after all three refused restores");
        assert_eq!(query.decision, DevelopmentLaneQueryResultDecision::Accepted);
    }

    #[test]
    fn every_low_level_graph_row_path_refuses_orphaning_a_lane_work_item() {
        let fixture = NativeLaneFixture::new(policy());
        let accepted: DevelopmentLaneResult =
            fixture.decode(&fixture.commit(fixture.reserve_method("reserve:row-path"), TEST_NOW));
        let hold = accepted.hold.expect("row-path hold");
        let remove = Method::RemoveNode {
            node_id: hold.work_item_id.clone(),
        };

        let mut ops = vec![(TEST_GRAPH.to_string(), remove.clone())];
        let mut raft_log = Vec::new();
        #[cfg(feature = "security")]
        let mut audit_tail = super::super::AuditTailCache::new();
        assert!(super::super::commit_ops(
            &fixture.db,
            &mut ops,
            &mut raft_log,
            Durability::Immediate,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit_tail,
        )
        .is_err());

        #[cfg(feature = "security")]
        let mut crossmodal_audit = super::super::AuditTailCache::new();
        assert!(super::super::commit_crossmodal(
            &fixture.db,
            TEST_GRAPH,
            std::slice::from_ref(&remove),
            &[],
            &[],
            &[],
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut crossmodal_audit,
        )
        .is_err());

        let query = read_development_lane(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneQueryRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneQueryRequestSchemaVersion::V1,
                tenant_ref: hold.tenant_ref,
                hold_id: hold.hold_id,
                now_ms: 0,
            },
            TEST_NOW,
            DurableCrypto::none(),
        )
        .expect("query row-path hold after refused writes");
        assert_eq!(query.decision, DevelopmentLaneQueryResultDecision::Accepted);
    }

    #[test]
    fn checkpoint_restore_requires_the_exact_linked_lane_work_item() {
        let fixture = NativeLaneFixture::new(policy());
        let accepted: DevelopmentLaneResult =
            fixture.decode(&fixture.commit(fixture.reserve_method("reserve:checkpoint"), TEST_NOW));
        let hold = accepted.hold.expect("checkpoint hold");
        let rtx = fixture
            .db
            .begin_read()
            .expect("begin checkpoint validation");
        let nodes = rtx.open_table(NODES).expect("open checkpoint nodes");
        let holds = rtx.open_table(HOLDS).expect("open checkpoint holds");
        assert!(
            validate_checkpoint_lane_links(TEST_GRAPH, &[], &holds, DurableCrypto::none()).is_err()
        );
        let node = nodes
            .get((TEST_GRAPH, hold.work_item_id.as_str()))
            .expect("read linked checkpoint WorkItem")
            .expect("linked checkpoint WorkItem exists");
        let incoming = vec![(hold.work_item_id.clone(), node.value().to_vec())];
        validate_checkpoint_lane_links(TEST_GRAPH, &incoming, &holds, DurableCrypto::none())
            .expect("exact linked WorkItem preserves lane authority");
        let mut stale_props: serde_json::Map<String, serde_json::Value> =
            decode_durable(node.value()).expect("decode checkpoint WorkItem");
        stale_props.insert("status".into(), serde_json::json!("succeeded"));
        let stale_bytes = rmp_serde::to_vec_named(&stale_props).expect("encode stale WorkItem");
        assert!(validate_checkpoint_lane_links(
            TEST_GRAPH,
            &[(hold.work_item_id, stale_bytes)],
            &holds,
            DurableCrypto::none()
        )
        .is_err());
    }

    #[test]
    fn native_same_branch_and_worktree_races_leave_one_winner() {
        let fixture = NativeLaneFixture::new(policy());
        let first = fixture.candidate("race-a", "branch:race", "lanes/race-a");
        let second = fixture.candidate("race-b", "branch:race", "lanes/race-b");
        let first_for_indexes = first.clone();
        let second_for_indexes = second.clone();
        let db = &fixture.db;
        let start = Arc::new(Barrier::new(3));
        let (first_decision, second_decision) = std::thread::scope(|scope| {
            let first_start = Arc::clone(&start);
            let first_thread = scope.spawn({
                let db = db;
                move || {
                    first_start.wait();
                    let mut request = first;
                    request.idempotency_key = "reserve:race-a".into();
                    let bytes = commit_development_lane(
                        db,
                        TEST_GRAPH,
                        &Method::ReserveDevelopmentLane { request },
                        TEST_NOW,
                        DurableCrypto::none(),
                    )
                    .expect("branch race transaction");
                    rmp_serde::from_slice::<DevelopmentLaneResult>(&bytes)
                        .expect("decode branch race result")
                        .decision
                }
            });
            let second_start = Arc::clone(&start);
            let second_thread = scope.spawn({
                let db = db;
                move || {
                    second_start.wait();
                    let mut request = second;
                    request.idempotency_key = "reserve:race-b".into();
                    let bytes = commit_development_lane(
                        db,
                        TEST_GRAPH,
                        &Method::ReserveDevelopmentLane { request },
                        TEST_NOW,
                        DurableCrypto::none(),
                    )
                    .expect("branch race transaction");
                    rmp_serde::from_slice::<DevelopmentLaneResult>(&bytes)
                        .expect("decode branch race result")
                        .decision
                }
            });
            start.wait();
            (
                first_thread.join().expect("first branch race thread"),
                second_thread.join().expect("second branch race thread"),
            )
        });
        let accepted_request = if first_decision == DevelopmentLaneResultDecision::Accepted {
            &first_for_indexes
        } else {
            &second_for_indexes
        };
        let refused_request = if first_decision == DevelopmentLaneResultDecision::Accepted {
            &second_for_indexes
        } else {
            &first_for_indexes
        };
        let status = read_development_lane_status(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneStatusRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                tenant_ref: "tenant:a".into(),
                hold_id: None,
                lane_id: None,
                work_item_id: None,
                limit: 10,
                cursor: None,
                now_ms: 0,
            },
            TEST_NOW,
            DurableCrypto::none(),
        )
        .expect("read branch race status");
        assert_eq!(status.holds.len(), 1);
        assert_eq!(status.tenant_active_count, 1);
        assert_eq!(status.counters.global_count, 1);
        let expected_hold_id = hold_id(&accepted_request.intent);
        let branch_key = format!(
            "{}\0{}",
            accepted_request.intent.repository_id, accepted_request.intent.branch
        );
        let accepted_worktree_key = format!(
            "{}\0{}\0{}",
            accepted_request.intent.host_ref,
            accepted_request.intent.workspace_ref,
            accepted_request.intent.worktree_locator
        );
        let refused_worktree_key = format!(
            "{}\0{}\0{}",
            refused_request.intent.host_ref,
            refused_request.intent.workspace_ref,
            refused_request.intent.worktree_locator
        );
        let rtx = fixture.db.begin_read().expect("read branch race indexes");
        let branch_index = rtx
            .open_table(REPOSITORY_BRANCH_INDEX)
            .expect("open branch race index");
        let worktree_index = rtx
            .open_table(WORKTREE_INDEX)
            .expect("open worktree race index");
        let lane_index = rtx.open_table(LANE_INDEX).expect("open lane race index");
        let work_item_index = rtx
            .open_table(WORK_ITEM_INDEX)
            .expect("open WorkItem race index");
        let counters = rtx.open_table(COUNTERS).expect("open race counters");
        let pressure_index = rtx
            .open_table(PRESSURE_INDEX)
            .expect("open race pressure index");
        assert_eq!(
            branch_index
                .get((TEST_GRAPH, "tenant:a", branch_key.as_str()))
                .expect("read branch winner")
                .map(|value| value.value().to_string()),
            Some(expected_hold_id.clone())
        );
        assert_eq!(
            worktree_index
                .get((TEST_GRAPH, accepted_worktree_key.as_str()))
                .expect("read worktree winner")
                .map(|value| value.value().to_string()),
            Some(expected_hold_id.clone())
        );
        assert!(worktree_index
            .get((TEST_GRAPH, refused_worktree_key.as_str()))
            .expect("read worktree refusal")
            .is_none());
        assert_eq!(
            lane_index
                .get((
                    TEST_GRAPH,
                    accepted_request.intent.tenant_ref.as_str(),
                    accepted_request.intent.lane_id.as_str(),
                ))
                .expect("read lane winner")
                .map(|value| value.value().to_string()),
            Some(expected_hold_id.clone())
        );
        assert!(lane_index
            .get((
                TEST_GRAPH,
                refused_request.intent.tenant_ref.as_str(),
                refused_request.intent.lane_id.as_str(),
            ))
            .expect("read refused lane index")
            .is_none());
        assert!(work_item_index
            .get((
                TEST_GRAPH,
                refused_request.work_item_id.as_str(),
                refused_request.attempt
            ))
            .expect("read refused WorkItem index")
            .is_none());
        for key in [
            format!("tenant\0tenant:a\0tenant:a"),
            format!("owner\0tenant:a\0{}", accepted_request.intent.owner_id),
            format!("session\0tenant:a\0{}", accepted_request.intent.session_id),
            format!(
                "workspace\0tenant:a\0{}",
                accepted_request.intent.workspace_ref
            ),
            format!(
                "repository\0tenant:a\0{}",
                accepted_request.intent.repository_id
            ),
            format!("host\0tenant:a\0{}", accepted_request.intent.host_ref),
            "global\0*".to_string(),
        ] {
            let counter = counters
                .get((TEST_GRAPH, key.as_str()))
                .expect("read exact race counter")
                .map(|value| {
                    resource_decode::<DurableLaneCounter>(value.value(), DurableCrypto::none())
                })
                .transpose()
                .expect("decode exact race counter")
                .expect("race counter exists");
            assert_eq!(counter.active_count, 1, "counter {key}");
            assert_eq!(counter.predicted_disk_bytes, 10, "counter {key}");
            assert_eq!(counter.observed_disk_bytes, 0, "counter {key}");
            assert_eq!(counter.retained_disk_bytes, 0, "counter {key}");
        }
        assert_eq!(
            pressure_max(
                &pressure_index,
                TEST_GRAPH,
                "tenant:a",
                Scope::Workspace,
                Metric::Count
            )
            .expect("read workspace pressure"),
            1
        );
        assert_eq!(
            pressure_max(
                &pressure_index,
                TEST_GRAPH,
                "tenant:a",
                Scope::Workspace,
                Metric::Predicted
            )
            .expect("read workspace predicted pressure"),
            10
        );
        let accepted = [first_decision, second_decision]
            .into_iter()
            .filter(|decision| *decision == DevelopmentLaneResultDecision::Accepted)
            .count();
        let exclusive = [first_decision, second_decision]
            .into_iter()
            .filter(|decision| *decision == DevelopmentLaneResultDecision::Exclusivity)
            .count();
        assert_eq!(accepted, 1);
        assert_eq!(exclusive, 1);

        let fixture = NativeLaneFixture::new(policy());
        let first = fixture.candidate("tree-a", "branch:tree-a", "lanes/same");
        let second = fixture.candidate("tree-b", "branch:tree-b", "lanes/same");
        let first_for_indexes = first.clone();
        let second_for_indexes = second.clone();
        let db = &fixture.db;
        let start = Arc::new(Barrier::new(3));
        let (first_decision, second_decision) = std::thread::scope(|scope| {
            let first_start = Arc::clone(&start);
            let first_thread = scope.spawn({
                let db = db;
                move || {
                    first_start.wait();
                    let mut request = first;
                    request.idempotency_key = "reserve:tree-a".into();
                    let bytes = commit_development_lane(
                        db,
                        TEST_GRAPH,
                        &Method::ReserveDevelopmentLane { request },
                        TEST_NOW,
                        DurableCrypto::none(),
                    )
                    .expect("worktree race transaction");
                    rmp_serde::from_slice::<DevelopmentLaneResult>(&bytes)
                        .expect("decode worktree race result")
                        .decision
                }
            });
            let second_start = Arc::clone(&start);
            let second_thread = scope.spawn({
                let db = db;
                move || {
                    second_start.wait();
                    let mut request = second;
                    request.idempotency_key = "reserve:tree-b".into();
                    let bytes = commit_development_lane(
                        db,
                        TEST_GRAPH,
                        &Method::ReserveDevelopmentLane { request },
                        TEST_NOW,
                        DurableCrypto::none(),
                    )
                    .expect("worktree race transaction");
                    rmp_serde::from_slice::<DevelopmentLaneResult>(&bytes)
                        .expect("decode worktree race result")
                        .decision
                }
            });
            start.wait();
            (
                first_thread.join().expect("first worktree race thread"),
                second_thread.join().expect("second worktree race thread"),
            )
        });
        let accepted_request = if first_decision == DevelopmentLaneResultDecision::Accepted {
            &first_for_indexes
        } else {
            &second_for_indexes
        };
        let expected_hold_id = hold_id(&accepted_request.intent);
        let worktree_key = format!(
            "{}\0{}\0{}",
            accepted_request.intent.host_ref,
            accepted_request.intent.workspace_ref,
            accepted_request.intent.worktree_locator
        );
        let rtx = fixture.db.begin_read().expect("read worktree race index");
        let worktree_index = rtx
            .open_table(WORKTREE_INDEX)
            .expect("open worktree race index");
        assert_eq!(
            worktree_index
                .get((TEST_GRAPH, worktree_key.as_str()))
                .expect("read worktree race winner")
                .map(|value| value.value().to_string()),
            Some(expected_hold_id)
        );
        assert_eq!(
            [first_decision, second_decision]
                .into_iter()
                .filter(|decision| *decision == DevelopmentLaneResultDecision::Accepted)
                .count(),
            1
        );
        assert_eq!(
            [first_decision, second_decision]
                .into_iter()
                .filter(|decision| *decision == DevelopmentLaneResultDecision::Exclusivity)
                .count(),
            1
        );
    }

    #[test]
    fn native_status_limit_uses_an_extra_row_probe() {
        let fixture = NativeLaneFixture::new(policy());
        let initial: DevelopmentLaneResult =
            fixture.decode(&fixture.commit(fixture.reserve_method("reserve:initial"), TEST_NOW));
        assert_eq!(initial.decision, DevelopmentLaneResultDecision::Accepted);
        let status = |limit: u64, cursor: Option<String>| {
            read_development_lane_status(
                &fixture.db,
                TEST_GRAPH,
                &DevelopmentLaneStatusRequest {
                    schema_version:
                        crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                    tenant_ref: "tenant:a".into(),
                    hold_id: None,
                    lane_id: None,
                    work_item_id: None,
                    limit,
                    cursor,
                    now_ms: 0,
                },
                TEST_NOW,
                DurableCrypto::none(),
            )
            .expect("read bounded status page")
        };
        let exact = status(1, None);
        assert_eq!(exact.holds.len(), 1);
        assert_eq!(exact.tenant_active_count, 1);
        assert_eq!(exact.holds[0].worktree_locator, REDACTED_PRIVATE_ID);
        assert_eq!(exact.holds[0].host_ref, REDACTED_PRIVATE_ID);
        assert!(exact.holds[0].host_target_alias.is_none());
        assert!(exact.complete);
        assert!(exact.next_cursor.is_none());

        let candidate = fixture.candidate(
            "status-second",
            "branch:status-second",
            "lanes/status-second",
        );
        let accepted: DevelopmentLaneResult = fixture.decode(&fixture.commit(
            Method::ReserveDevelopmentLane {
                request: DevelopmentLaneReserveRequest {
                    idempotency_key: "reserve:status-second".into(),
                    ..candidate
                },
            },
            TEST_NOW,
        ));
        assert_eq!(accepted.decision, DevelopmentLaneResultDecision::Accepted);
        let first_page = status(1, None);
        assert_eq!(first_page.holds.len(), 1);
        assert_eq!(first_page.tenant_active_count, 2);
        assert!(!first_page.complete);
        let cursor = first_page.next_cursor.clone().expect("next status cursor");
        let second_page = status(1, Some(cursor));
        assert_eq!(second_page.holds.len(), 1);
        assert_eq!(second_page.tenant_active_count, 2);
        assert!(second_page.complete);
        assert!(second_page.next_cursor.is_none());
    }

    #[test]
    fn native_last_quota_unit_refuses_without_partial_indexes_or_counters() {
        let mut limited = policy();
        limited.tenant_count_limit = 1;
        limited.global_count_limit = 1;
        let fixture = NativeLaneFixture::new(limited);
        let first: DevelopmentLaneResult =
            fixture.decode(&fixture.commit(fixture.reserve_method("reserve:limited-a"), TEST_NOW));
        assert_eq!(first.decision, DevelopmentLaneResultDecision::Accepted);
        let second = fixture.candidate("limited-b", "branch:limited-b", "lanes/limited-b");
        let second: DevelopmentLaneResult = fixture.decode(&fixture.commit(
            Method::ReserveDevelopmentLane {
                request: DevelopmentLaneReserveRequest {
                    idempotency_key: "reserve:limited-b".into(),
                    ..second
                },
            },
            TEST_NOW,
        ));
        assert_eq!(second.decision, DevelopmentLaneResultDecision::Quota);
        let status = read_development_lane_status(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneStatusRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                tenant_ref: "tenant:a".into(),
                hold_id: None,
                lane_id: None,
                work_item_id: None,
                limit: 10,
                cursor: None,
                now_ms: 0,
            },
            TEST_NOW,
            DurableCrypto::none(),
        )
        .expect("read bounded status");
        assert_eq!(status.holds.len(), 1);
        assert_eq!(status.tenant_active_count, 1);
        assert_eq!(status.counters.global_count, 1);
    }

    #[test]
    fn native_policy_cas_sees_non_tenant_scope_totals_without_a_scan() {
        let fixture = NativeLaneFixture::new(policy());
        let first: DevelopmentLaneResult =
            fixture.decode(&fixture.commit(fixture.reserve_method("reserve:scope-a"), TEST_NOW));
        assert_eq!(first.decision, DevelopmentLaneResultDecision::Accepted);
        let mut second = test_reserve_request(test_intent(
            "tenant:a",
            "scope-b",
            "branch:scope-b",
            "lanes/scope-b",
        ));
        second.owner_id = fixture.reserve.owner_id.clone();
        second.intent.owner_id = fixture.reserve.intent.owner_id.clone();
        second.intent.session_id = fixture.reserve.intent.session_id.clone();
        seed_lane_work_item(&fixture.db, &second).expect("seed shared-scope candidate");
        let second: DevelopmentLaneResult = fixture.decode(&fixture.commit(
            Method::ReserveDevelopmentLane {
                request: DevelopmentLaneReserveRequest {
                    idempotency_key: "reserve:scope-b".into(),
                    ..second
                },
            },
            TEST_NOW,
        ));
        assert_eq!(second.decision, DevelopmentLaneResultDecision::Accepted);

        {
            let rtx = fixture.db.begin_read().expect("read maintained pressure");
            let pressure_index = rtx
                .open_table(PRESSURE_INDEX)
                .expect("open maintained pressure index");
            for scope in [
                Scope::Owner,
                Scope::Session,
                Scope::Workspace,
                Scope::Repository,
                Scope::Host,
            ] {
                assert_eq!(
                    pressure_max(
                        &pressure_index,
                        TEST_GRAPH,
                        "tenant:a",
                        scope,
                        Metric::Count
                    )
                    .expect("read maintained count pressure"),
                    2,
                    "count pressure for {scope:?}"
                );
                assert_eq!(
                    pressure_max(
                        &pressure_index,
                        TEST_GRAPH,
                        "tenant:a",
                        scope,
                        Metric::Predicted
                    )
                    .expect("read maintained predicted pressure"),
                    20,
                    "predicted pressure for {scope:?}"
                );
                assert_eq!(
                    pressure_max(
                        &pressure_index,
                        TEST_GRAPH,
                        "tenant:a",
                        scope,
                        Metric::Observed
                    )
                    .expect("read maintained observed pressure"),
                    0,
                    "observed pressure for {scope:?}"
                );
                assert_eq!(
                    pressure_max(
                        &pressure_index,
                        TEST_GRAPH,
                        "tenant:a",
                        scope,
                        Metric::Retained
                    )
                    .expect("read maintained retained pressure"),
                    0,
                    "retained pressure for {scope:?}"
                );
            }
        }

        let mut drain_policy = policy();
        drain_policy.drain_only = true;
        let global: DevelopmentLaneQuotaUpdateResult = fixture.decode(&fixture.commit(
            Method::UpdateDevelopmentLaneQuota {
                request: DevelopmentLaneQuotaUpdateRequest {
                    schema_version:
                        crate::epistemic_operations::DevelopmentLaneQuotaUpdateRequestSchemaVersion::V1,
                    tenant_ref: GLOBAL_POLICY_KEY.into(),
                    policy: drain_policy.clone(),
                    expected_policy_revision: 1,
                    expected_policy_version: Some("1".into()),
                    idempotency_key: "policy:scope-global-drain".into(),
                    now_ms: 0,
                },
            },
            150,
        ));
        assert_eq!(
            global.decision,
            DevelopmentLaneQuotaUpdateResultDecision::Accepted
        );

        let reductions: [(&str, fn(&mut DevelopmentLaneQuotaPolicy)); 10] = [
            ("owner-count", |value| value.owner_count_limit = 1),
            ("session-count", |value| value.session_count_limit = 1),
            ("workspace-count", |value| value.workspace_count_limit = 1),
            ("repository-count", |value| value.repository_count_limit = 1),
            ("host-count", |value| value.host_count_limit = 1),
            ("owner-predicted", |value| {
                value.owner_predicted_disk_bytes = 10
            }),
            ("session-predicted", |value| {
                value.session_predicted_disk_bytes = 10
            }),
            ("workspace-predicted", |value| {
                value.workspace_predicted_disk_bytes = 10
            }),
            ("repository-predicted", |value| {
                value.repository_predicted_disk_bytes = 10
            }),
            ("host-predicted", |value| {
                value.host_predicted_disk_bytes = 10
            }),
        ];
        for (name, reduce) in reductions {
            let mut reduced = drain_policy.clone();
            reduce(&mut reduced);
            let refused: DevelopmentLaneQuotaUpdateResult = fixture.decode(&fixture.commit(
                Method::UpdateDevelopmentLaneQuota {
                    request: DevelopmentLaneQuotaUpdateRequest {
                        schema_version:
                            crate::epistemic_operations::DevelopmentLaneQuotaUpdateRequestSchemaVersion::V1,
                        tenant_ref: "tenant:a".into(),
                        policy: reduced,
                        expected_policy_revision: 1,
                        expected_policy_version: Some("1".into()),
                        idempotency_key: format!("policy:scope-reduction:{name}"),
                        now_ms: 0,
                    },
                },
                151,
            ));
            assert_eq!(
                refused.decision,
                DevelopmentLaneQuotaUpdateResultDecision::Quota,
                "reduction {name} must observe maintained pressure"
            );
        }
    }

    #[test]
    fn native_observe_replaces_exact_delta_and_renew_checks_each_hold_freshness() {
        let fixture = NativeLaneFixture::new(policy());
        let accepted: DevelopmentLaneResult =
            fixture.decode(&fixture.commit(fixture.reserve_method("reserve:observe"), TEST_NOW));
        let hold = accepted.hold.expect("accepted observation hold");
        let observe = |hold: &DevelopmentLaneHold,
                       observed_disk_bytes: u64,
                       observation_revision: u64,
                       expected_hold_revision: u64,
                       key: &str,
                       now_ms: u64|
         -> DevelopmentLaneObserveResult {
            let request = DevelopmentLaneObserveRequest {
                schema_version: DevelopmentLaneObserveRequestSchemaVersion::V1,
                tenant_ref: hold.tenant_ref.clone(),
                work_item_id: hold.work_item_id.clone(),
                owner_id: hold.owner_id.clone(),
                attempt: hold.attempt,
                lease_epoch: hold.lease_epoch,
                fencing_token: hold.fencing_token,
                work_item_fence: hold.work_item_fence.clone(),
                hold_id: hold.hold_id.clone(),
                expected_hold_revision,
                observed_disk_bytes,
                observation_revision,
                idempotency_key: key.into(),
                now_ms,
            };
            fixture.decode(&fixture.commit(Method::ObserveDevelopmentLane { request }, now_ms))
        };
        let first = observe(&hold, 20, 1, hold.hold_revision, "observe:20", 200);
        assert_eq!(
            first.decision,
            DevelopmentLaneObserveResultDecision::Accepted
        );
        let hold = first.hold.expect("first observation hold");
        assert_eq!(hold.observed_disk_bytes, 20);
        assert_eq!(hold.hold_revision, 2);
        let stale = observe(&hold, 10, 2, hold.hold_revision, "observe:lower", 201);
        assert_eq!(stale.decision, DevelopmentLaneObserveResultDecision::Stale);
        let replacement = observe(&hold, 30, 3, hold.hold_revision, "observe:30", 200);
        assert_eq!(
            replacement.decision,
            DevelopmentLaneObserveResultDecision::Accepted
        );
        let hold = replacement.hold.expect("replacement observation hold");
        assert_eq!(hold.observed_disk_bytes, 30);
        let status = read_development_lane_status(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneStatusRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                tenant_ref: "tenant:a".into(),
                hold_id: None,
                lane_id: None,
                work_item_id: None,
                limit: 10,
                cursor: None,
                now_ms: 0,
            },
            200,
            DurableCrypto::none(),
        )
        .expect("status after observation");
        assert_eq!(status.counters.tenant_observed_disk_bytes, 30);
        {
            let rtx = fixture
                .db
                .begin_read()
                .expect("read observed pressure index");
            let pressure_index = rtx
                .open_table(PRESSURE_INDEX)
                .expect("open observed pressure index");
            for scope in [
                Scope::Owner,
                Scope::Session,
                Scope::Workspace,
                Scope::Repository,
                Scope::Host,
            ] {
                assert_eq!(
                    pressure_max(
                        &pressure_index,
                        TEST_GRAPH,
                        "tenant:a",
                        scope,
                        Metric::Observed
                    )
                    .expect("read observed scope pressure"),
                    30,
                    "observed pressure for {scope:?}"
                );
            }
        }

        let second_request = fixture.candidate("never-observed", "branch:never", "lanes/never");
        let second: DevelopmentLaneResult = fixture.decode(&fixture.commit(
            Method::ReserveDevelopmentLane {
                request: DevelopmentLaneReserveRequest {
                    idempotency_key: "reserve:never-observed".into(),
                    ..second_request
                },
            },
            TEST_NOW,
        ));
        assert_eq!(second.decision, DevelopmentLaneResultDecision::Accepted);
        let second_hold = second.hold.expect("never-observed hold");

        let renewed: DevelopmentLaneRenewResult = fixture.decode(&fixture.commit(
            Method::RenewDevelopmentLane {
                request: DevelopmentLaneRenewRequest {
                    schema_version: DevelopmentLaneRenewRequestSchemaVersion::V1,
                    tenant_ref: hold.tenant_ref.clone(),
                    work_item_id: hold.work_item_id.clone(),
                    owner_id: hold.owner_id.clone(),
                    attempt: hold.attempt,
                    lease_epoch: hold.lease_epoch,
                    fencing_token: hold.fencing_token,
                    work_item_fence: hold.work_item_fence.clone(),
                    hold_id: hold.hold_id.clone(),
                    expected_hold_revision: hold.hold_revision,
                    ttl_ms: 1_000,
                    idempotency_key: "renew:fresh".into(),
                    now_ms: 0,
                },
            },
            200,
        ));
        assert_eq!(
            renewed.decision,
            DevelopmentLaneRenewResultDecision::Accepted
        );
        let stale_never_observed: DevelopmentLaneRenewResult = fixture.decode(&fixture.commit(
            Method::RenewDevelopmentLane {
                request: DevelopmentLaneRenewRequest {
                    schema_version: DevelopmentLaneRenewRequestSchemaVersion::V1,
                    tenant_ref: second_hold.tenant_ref.clone(),
                    work_item_id: second_hold.work_item_id.clone(),
                    owner_id: second_hold.owner_id.clone(),
                    attempt: second_hold.attempt,
                    lease_epoch: second_hold.lease_epoch,
                    fencing_token: second_hold.fencing_token,
                    work_item_fence: second_hold.work_item_fence.clone(),
                    hold_id: second_hold.hold_id.clone(),
                    expected_hold_revision: second_hold.hold_revision,
                    ttl_ms: 1_000,
                    idempotency_key: "renew:never-observed".into(),
                    now_ms: 0,
                },
            },
            200,
        ));
        assert_eq!(
            stale_never_observed.decision,
            DevelopmentLaneRenewResultDecision::Stale
        );
    }

    #[test]
    fn native_retryable_failure_refuses_before_leaving_an_active_hold() {
        let fixture = NativeLaneFixture::new(policy());
        let accepted: DevelopmentLaneResult =
            fixture.decode(&fixture.commit(fixture.reserve_method("reserve:retry"), TEST_NOW));
        let hold = accepted.hold.expect("accepted retry hold");

        let fenced = fixture
            .commit_work_item_result_with_tuple(
                hold.lease_epoch,
                hold.fencing_token + 1,
                "succeeded",
                false,
                "wrong-work-item-fence",
                200,
            )
            .expect("wrong WorkItem fence response");
        let fenced_result: serde_json::Value = rmp_serde::from_slice(
            fenced
                .record
                .result_msgpack
                .as_deref()
                .expect("fenced WorkItem result bytes"),
        )
        .expect("decode fenced WorkItem result");
        assert_eq!(fenced_result["status"], "fenced");

        let error = fixture
            .commit_work_item_result("failed", true, "retryable-failure", 200)
            .expect_err("retryable failure must not orphan an active lane hold");
        assert_eq!(error, ACTIVE_HOLD_REQUIRES_TERMINAL_WORK_ITEM);

        let status = read_development_lane_status(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneStatusRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                tenant_ref: hold.tenant_ref.clone(),
                hold_id: Some(hold.hold_id.clone()),
                lane_id: None,
                work_item_id: Some(hold.work_item_id.clone()),
                limit: 10,
                cursor: None,
                now_ms: 0,
            },
            200,
            DurableCrypto::none(),
        )
        .expect("read unchanged lane after refused retry");
        assert_eq!(status.tenant_active_count, 1);
        assert_eq!(status.tenant_retained_disk_bytes, 0);
        assert_eq!(status.holds[0].state, DevelopmentLaneHoldState::Active);

        // The transaction rolled back the local ready/epoch mutation, so the
        // exact original worker tuple can still terminalize the WorkItem and
        // release the linked hold atomically.
        let committed = fixture
            .commit_work_item_result("succeeded", false, "retryable-recovery", 200)
            .expect("terminal recovery after refused retry");
        assert!(!committed.replayed);
        let status = read_development_lane_status(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneStatusRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                tenant_ref: hold.tenant_ref,
                hold_id: Some(hold.hold_id),
                lane_id: None,
                work_item_id: None,
                limit: 10,
                cursor: None,
                now_ms: 0,
            },
            200,
            DurableCrypto::none(),
        )
        .expect("read terminalized lane after recovery");
        assert_eq!(status.tenant_active_count, 0);
        assert_eq!(status.tenant_retained_disk_bytes, 10);
        assert_eq!(
            status.holds[0].state,
            DevelopmentLaneHoldState::CleanupPending
        );
    }

    #[test]
    fn native_public_owner_cas_drift_rolls_back_before_terminal_lane_transition() {
        let fixture = NativeLaneFixture::new(policy());
        let accepted: DevelopmentLaneResult =
            fixture.decode(&fixture.commit(fixture.reserve_method("reserve:owner-cas"), TEST_NOW));
        let hold = accepted.hold.expect("accepted owner binding hold");
        let status_before = read_development_lane_status(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneStatusRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                tenant_ref: hold.tenant_ref.clone(),
                hold_id: Some(hold.hold_id.clone()),
                lane_id: None,
                work_item_id: Some(hold.work_item_id.clone()),
                limit: 10,
                cursor: None,
                now_ms: 0,
            },
            TEST_NOW,
            DurableCrypto::none(),
        )
        .expect("read owner binding baseline");

        // This is the public compact MutationBatch CAS path, not a direct NODES
        // edit.  A lane-linked WorkItem cannot change its authoritative owner
        // while preserving the lifecycle tuple and intent.
        //
        // RMDD-29's native WorkItem-authority migration
        // (`work_item_capability::validate_generic_method`'s `CompareAndSetNodeFields` arm)
        // now refuses any generic CAS that touches a protected authority field
        // (`lease_owner` is one of `WORK_ITEM_AUTHORITY_KEYS`) on an existing WorkItem node
        // BEFORE the lane's own fence-mismatch check in `apply_mutation_in_wtx` ever runs --
        // a broader, earlier-firing refusal than the lane-specific one this assertion
        // originally named. The invariant this test proves (a foreign owner drift never lands)
        // holds a fortiori.
        let drift = fixture.compare_and_set_work_item_owner(
            &hold.owner_id,
            "owner:foreign",
            "owner-drift",
            200,
        );
        assert_eq!(
            drift.unwrap_err(),
            "native WorkItem authority required for protected field 'lease_owner'"
        );

        let stored = read_one_node(
            &fixture.db,
            TEST_GRAPH,
            &hold.work_item_id,
            DurableCrypto::none(),
        )
        .expect("read rolled-back WorkItem")
        .expect("linked WorkItem remains present");
        let stored: serde_json::Map<String, serde_json::Value> =
            decode_durable(&stored).expect("decode rolled-back WorkItem");
        assert_eq!(property_string(&stored, "status"), "running");
        assert_eq!(
            property_string(&stored, "lease_owner"),
            hold.owner_id.as_str()
        );

        let status_after_drift = read_development_lane_status(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneStatusRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                tenant_ref: hold.tenant_ref.clone(),
                hold_id: Some(hold.hold_id.clone()),
                lane_id: None,
                work_item_id: Some(hold.work_item_id.clone()),
                limit: 10,
                cursor: None,
                now_ms: 0,
            },
            TEST_NOW,
            DurableCrypto::none(),
        )
        .expect("read unchanged lane after owner drift");
        assert_eq!(status_after_drift.counters, status_before.counters);
        assert_eq!(status_after_drift.holds, status_before.holds);
        assert_eq!(status_after_drift.tenant_active_count, 1);
        assert_eq!(status_after_drift.tenant_retained_disk_bytes, 0);

        let finish_request = |tenant_ref: &str, owner_id: &str, idempotency_key: &str| {
            DevelopmentLaneFinishRequest {
                schema_version: DevelopmentLaneFinishRequestSchemaVersion::V1,
                tenant_ref: tenant_ref.into(),
                work_item_id: hold.work_item_id.clone(),
                owner_id: owner_id.into(),
                attempt: hold.attempt,
                lease_epoch: hold.lease_epoch,
                fencing_token: hold.fencing_token,
                work_item_fence: hold.work_item_fence.clone(),
                hold_id: hold.hold_id.clone(),
                expected_hold_revision: hold.hold_revision,
                terminal_state: DevelopmentLaneFinishRequestTerminalState::Succeeded,
                idempotency_key: idempotency_key.into(),
                now_ms: 0,
            }
        };
        let foreign_policy =
            fixture.update_policy("tenant:foreign", policy(), 0, "policy:foreign", 200);
        assert_eq!(
            foreign_policy.decision,
            DevelopmentLaneQuotaUpdateResultDecision::Accepted
        );
        let wrong_tenant: DevelopmentLaneFinishResult = fixture.decode(&fixture.commit(
            Method::FinishDevelopmentLane {
                request: finish_request("tenant:foreign", &hold.owner_id, "finish:wrong-tenant"),
            },
            200,
        ));
        assert_eq!(
            wrong_tenant.decision,
            DevelopmentLaneFinishResultDecision::WrongTenant
        );
        let wrong_owner: DevelopmentLaneFinishResult = fixture.decode(&fixture.commit(
            Method::FinishDevelopmentLane {
                request: finish_request("tenant:a", "owner:foreign", "finish:wrong-owner"),
            },
            200,
        ));
        assert_eq!(
            wrong_owner.decision,
            DevelopmentLaneFinishResultDecision::WrongOwner
        );

        // A foreign worker cannot terminalize the hold even after the failed
        // CAS attempt; the WorkItem CAS and lane state remain untouched.
        let foreign_commit = fixture
            .commit_work_item(
                Method::CommitWorkItemResult {
                    tenant: hold.tenant_ref.clone(),
                    work_item_id: hold.work_item_id.clone(),
                    worker_id: "owner:foreign".into(),
                    lease_epoch: hold.lease_epoch,
                    fencing_token: hold.fencing_token,
                    idempotency_key: "work-item:foreign-owner".into(),
                    outcome: "succeeded".into(),
                    result_ref: None,
                    error_ref: None,
                    retryable: false,
                    now_ms: 200,
                },
                "foreign-owner",
                200,
            )
            .expect("foreign owner result is durably fenced");
        let foreign_result: serde_json::Value = fixture.decode(
            foreign_commit
                .record
                .result_msgpack
                .as_deref()
                .expect("foreign owner result bytes"),
        );
        assert_eq!(foreign_result["status"], "fenced");
        let status_after_foreign = read_development_lane_status(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneStatusRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                tenant_ref: hold.tenant_ref.clone(),
                hold_id: Some(hold.hold_id.clone()),
                lane_id: None,
                work_item_id: Some(hold.work_item_id.clone()),
                limit: 10,
                cursor: None,
                now_ms: 0,
            },
            200,
            DurableCrypto::none(),
        )
        .expect("read unchanged lane after foreign result");
        assert_eq!(status_after_foreign.counters, status_before.counters);
        assert_eq!(status_after_foreign.holds, status_before.holds);
        let stored = read_one_node(
            &fixture.db,
            TEST_GRAPH,
            &hold.work_item_id,
            DurableCrypto::none(),
        )
        .expect("read non-terminal WorkItem")
        .expect("WorkItem remains present");
        let stored: serde_json::Map<String, serde_json::Value> =
            decode_durable(&stored).expect("decode non-terminal WorkItem");
        assert_eq!(property_string(&stored, "status"), "running");
        assert_eq!(
            property_string(&stored, "lease_owner"),
            hold.owner_id.as_str()
        );

        // The original owner still has the exact authority, and its replay is
        // byte-identical after the terminal hold transition.
        let committed = fixture
            .commit_work_item_result("succeeded", false, "owner-correct", 200)
            .expect("owner-correct terminal result");
        assert!(!committed.replayed);
        let replay = fixture
            .commit_work_item_result("succeeded", false, "owner-correct", 200)
            .expect("owner-correct terminal replay");
        assert!(replay.replayed);
        let final_status = read_development_lane_status(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneStatusRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                tenant_ref: hold.tenant_ref.clone(),
                hold_id: Some(hold.hold_id.clone()),
                lane_id: None,
                work_item_id: None,
                limit: 10,
                cursor: None,
                now_ms: 0,
            },
            200,
            DurableCrypto::none(),
        )
        .expect("read terminal lane");
        assert_eq!(final_status.tenant_active_count, 0);
        assert_eq!(final_status.tenant_retained_disk_bytes, 10);
        assert_eq!(
            final_status.holds[0].state,
            DevelopmentLaneHoldState::CleanupPending
        );
        let stored = read_one_node(
            &fixture.db,
            TEST_GRAPH,
            &hold.work_item_id,
            DurableCrypto::none(),
        )
        .expect("read terminal WorkItem")
        .expect("terminal WorkItem remains present");
        let stored: serde_json::Map<String, serde_json::Value> =
            decode_durable(&stored).expect("decode terminal WorkItem");
        assert_eq!(property_string(&stored, "status"), "succeeded");
        assert!(property_string(&stored, "lease_owner").is_empty());
        assert_eq!(
            property_string(&stored, "last_lease_owner"),
            "owner:initial"
        );
    }

    #[test]
    fn native_cancel_advances_fence_and_old_finish_replays_after_reopen() {
        let fixture = NativeLaneFixture::new(policy());
        let accepted: DevelopmentLaneResult =
            fixture.decode(&fixture.commit(fixture.reserve_method("reserve:cancel"), TEST_NOW));
        let hold = accepted.hold.expect("accepted cancel hold");
        let finish_request = DevelopmentLaneFinishRequest {
            schema_version: DevelopmentLaneFinishRequestSchemaVersion::V1,
            tenant_ref: hold.tenant_ref.clone(),
            work_item_id: hold.work_item_id.clone(),
            owner_id: hold.owner_id.clone(),
            attempt: hold.attempt,
            lease_epoch: hold.lease_epoch,
            fencing_token: hold.fencing_token,
            work_item_fence: hold.work_item_fence.clone(),
            hold_id: hold.hold_id.clone(),
            expected_hold_revision: hold.hold_revision,
            terminal_state: DevelopmentLaneFinishRequestTerminalState::Cancelled,
            idempotency_key: "finish:cancel-old-tuple".into(),
            now_ms: 0,
        };

        let committed = fixture
            .cancel_work_item("cancel-terminal", 2_000_000)
            .expect("expired WorkItem cancellation");
        assert!(!committed.replayed);
        let status = read_development_lane_status(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneStatusRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                tenant_ref: hold.tenant_ref.clone(),
                hold_id: Some(hold.hold_id.clone()),
                lane_id: None,
                work_item_id: Some(hold.work_item_id.clone()),
                limit: 10,
                cursor: None,
                now_ms: 0,
            },
            2_000_000,
            DurableCrypto::none(),
        )
        .expect("read atomically cancelled lane");
        let cancelled_hold = status.holds[0].clone();
        assert_eq!(
            cancelled_hold.state,
            DevelopmentLaneHoldState::CleanupPending
        );
        assert!(!cancelled_hold.active_count_charged);
        assert_eq!(cancelled_hold.retained_disk_bytes, 10);
        assert_eq!(cancelled_hold.lease_epoch, hold.lease_epoch + 1);
        assert_eq!(cancelled_hold.fencing_token, hold.fencing_token + 1);
        assert_eq!(status.tenant_active_count, 0);
        assert_eq!(status.tenant_retained_disk_bytes, 10);

        let path = fixture.close_for_reopen();
        let db = Database::open(&path).expect("reopen cancelled lane database");
        let finished: DevelopmentLaneFinishResult = decode_durable(
            &commit_development_lane(
                &db,
                TEST_GRAPH,
                &Method::FinishDevelopmentLane {
                    request: finish_request.clone(),
                },
                2_000_000,
                DurableCrypto::none(),
            )
            .expect("old tuple finish must repair cancelled lane"),
        )
        .expect("decode replayed finish result");
        assert_eq!(
            finished.decision,
            DevelopmentLaneFinishResultDecision::Idempotent
        );
        assert_eq!(finished.hold, Some(cancelled_hold.clone()));

        // A fresh caller may use the retained, post-cancel WorkItem tuple; it
        // must observe the same terminal authority without changing counters.
        let mut current_tuple = finish_request.clone();
        current_tuple.idempotency_key = "finish:cancel-current-tuple".into();
        current_tuple.attempt = cancelled_hold.attempt;
        current_tuple.lease_epoch = cancelled_hold.lease_epoch;
        current_tuple.fencing_token = cancelled_hold.fencing_token;
        current_tuple.work_item_fence = cancelled_hold.work_item_fence.clone();
        current_tuple.expected_hold_revision = cancelled_hold.hold_revision;
        let current: DevelopmentLaneFinishResult = decode_durable(
            &commit_development_lane(
                &db,
                TEST_GRAPH,
                &Method::FinishDevelopmentLane {
                    request: current_tuple,
                },
                2_000_000,
                DurableCrypto::none(),
            )
            .expect("current tuple finish replay"),
        )
        .expect("decode current tuple finish result");
        assert_eq!(
            current.decision,
            DevelopmentLaneFinishResultDecision::Idempotent
        );

        let mut wrong_fence = finish_request;
        wrong_fence.idempotency_key = "finish:cancel-wrong-fence".into();
        wrong_fence.lease_epoch = cancelled_hold.lease_epoch;
        wrong_fence.fencing_token = cancelled_hold.fencing_token;
        wrong_fence.work_item_fence = "fence:wrong".into();
        let refused: DevelopmentLaneFinishResult = decode_durable(
            &commit_development_lane(
                &db,
                TEST_GRAPH,
                &Method::FinishDevelopmentLane {
                    request: wrong_fence,
                },
                2_000_000,
                DurableCrypto::none(),
            )
            .expect("wrong fence finish response"),
        )
        .expect("decode wrong fence finish result");
        assert_eq!(
            refused.decision,
            DevelopmentLaneFinishResultDecision::WrongFence
        );
        drop(db);
        std::fs::remove_file(path).expect("remove reopened cancellation database");
    }

    #[test]
    fn native_cancel_lost_ack_is_replay_safe_across_crash_and_reopen() {
        let fixture = NativeLaneFixture::new(policy());
        let accepted: DevelopmentLaneResult = fixture
            .decode(&fixture.commit(fixture.reserve_method("reserve:cancel-crash"), TEST_NOW));
        let hold = accepted.hold.expect("accepted crash hold");
        let finish_request = DevelopmentLaneFinishRequest {
            schema_version: DevelopmentLaneFinishRequestSchemaVersion::V1,
            tenant_ref: hold.tenant_ref.clone(),
            work_item_id: hold.work_item_id.clone(),
            owner_id: hold.owner_id.clone(),
            attempt: hold.attempt,
            lease_epoch: hold.lease_epoch,
            fencing_token: hold.fencing_token,
            work_item_fence: hold.work_item_fence.clone(),
            hold_id: hold.hold_id.clone(),
            expected_hold_revision: hold.hold_revision,
            terminal_state: DevelopmentLaneFinishRequestTerminalState::Cancelled,
            idempotency_key: "finish:cancel-crash-repair".into(),
            now_ms: 0,
        };
        let crash = fixture.cancel_work_item_with_crash(
            "cancel-crash",
            2_000_000,
            Some(super::super::MutationBatchCrashpoint::AfterCommitBeforeAck),
        );
        assert_eq!(
            crash.unwrap_err(),
            "injected crash after mutation commit before acknowledgement"
        );
        let path = fixture.close_for_reopen();
        let db = Database::open(&path).expect("reopen crash database");
        let status = read_development_lane_status(
            &db,
            TEST_GRAPH,
            &DevelopmentLaneStatusRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                tenant_ref: hold.tenant_ref,
                hold_id: Some(hold.hold_id),
                lane_id: None,
                work_item_id: None,
                limit: 10,
                cursor: None,
                now_ms: 0,
            },
            2_000_000,
            DurableCrypto::none(),
        )
        .expect("read committed cancellation after lost ack");
        assert_eq!(status.tenant_active_count, 0);
        assert_eq!(status.tenant_retained_disk_bytes, 10);
        let finished: DevelopmentLaneFinishResult = decode_durable(
            &commit_development_lane(
                &db,
                TEST_GRAPH,
                &Method::FinishDevelopmentLane {
                    request: finish_request,
                },
                2_000_000,
                DurableCrypto::none(),
            )
            .expect("repair lost cancellation acknowledgement"),
        )
        .expect("decode crash repair result");
        assert_eq!(
            finished.decision,
            DevelopmentLaneFinishResultDecision::Idempotent
        );
        drop(db);
        std::fs::remove_file(path).expect("remove reopened crash database");
    }

    #[test]
    fn native_finish_retains_charge_and_cleanup_has_a_separate_fence_and_replay_tuple() {
        let fixture = NativeLaneFixture::new(policy());
        let accepted: DevelopmentLaneResult =
            fixture.decode(&fixture.commit(fixture.reserve_method("reserve:finish"), TEST_NOW));
        let hold = accepted.hold.expect("accepted finish hold");
        let committed = fixture
            .commit_work_item_result("succeeded", false, "finish-terminal", 200)
            .expect("generic terminal WorkItem commit");
        assert!(!committed.replayed);
        let finish_request = DevelopmentLaneFinishRequest {
            schema_version: DevelopmentLaneFinishRequestSchemaVersion::V1,
            tenant_ref: hold.tenant_ref.clone(),
            work_item_id: hold.work_item_id.clone(),
            owner_id: hold.owner_id.clone(),
            attempt: hold.attempt,
            lease_epoch: hold.lease_epoch,
            fencing_token: hold.fencing_token,
            work_item_fence: hold.work_item_fence.clone(),
            hold_id: hold.hold_id.clone(),
            expected_hold_revision: hold.hold_revision,
            terminal_state: DevelopmentLaneFinishRequestTerminalState::Succeeded,
            idempotency_key: "finish:one".into(),
            now_ms: 0,
        };
        let finished: DevelopmentLaneFinishResult = fixture.decode(&fixture.commit(
            Method::FinishDevelopmentLane {
                request: finish_request.clone(),
            },
            200,
        ));
        assert_eq!(
            finished.decision,
            DevelopmentLaneFinishResultDecision::Idempotent
        );
        let finished_hold = finished.hold.clone().expect("finished hold");
        assert_eq!(
            finished_hold.state,
            DevelopmentLaneHoldState::CleanupPending
        );
        assert_eq!(finished_hold.active_count_charged, false);
        assert_eq!(finished_hold.retained_disk_bytes, 10);
        {
            let rtx = fixture
                .db
                .begin_read()
                .expect("read retained pressure index");
            let pressure_index = rtx
                .open_table(PRESSURE_INDEX)
                .expect("open retained pressure index");
            for scope in [
                Scope::Owner,
                Scope::Session,
                Scope::Workspace,
                Scope::Repository,
                Scope::Host,
            ] {
                assert_eq!(
                    pressure_max(
                        &pressure_index,
                        TEST_GRAPH,
                        "tenant:a",
                        scope,
                        Metric::Retained
                    )
                    .expect("read retained scope pressure"),
                    10,
                    "retained pressure for {scope:?}"
                );
            }
        }
        let replay: DevelopmentLaneFinishResult = fixture.decode(&fixture.commit(
            Method::FinishDevelopmentLane {
                request: finish_request.clone(),
            },
            200,
        ));
        assert_eq!(replay, finished);
        let mut wrong_terminal = finish_request.clone();
        wrong_terminal.idempotency_key = "finish:wrong-terminal".into();
        wrong_terminal.terminal_state = DevelopmentLaneFinishRequestTerminalState::Failed;
        let wrong_terminal: DevelopmentLaneFinishResult = fixture.decode(&fixture.commit(
            Method::FinishDevelopmentLane {
                request: wrong_terminal,
            },
            200,
        ));
        assert_eq!(
            wrong_terminal.decision,
            DevelopmentLaneFinishResultDecision::InputConflict
        );

        fixture.seed_cleanup_work_item(&finished_hold, "cleanup:one", "cleanup-fence:one");
        let cleanup_request = DevelopmentLaneCleanupCompleteRequest {
            schema_version: DevelopmentLaneCleanupCompleteRequestSchemaVersion::V1,
            tenant_ref: finished_hold.tenant_ref.clone(),
            work_item_id: finished_hold.work_item_id.clone(),
            owner_id: finished_hold.owner_id.clone(),
            attempt: finished_hold.attempt,
            lease_epoch: finished_hold.lease_epoch,
            fencing_token: finished_hold.fencing_token,
            work_item_fence: finished_hold.work_item_fence.clone(),
            cleanup_work_item_id: "cleanup:one".into(),
            cleanup_work_item_fence: "cleanup-fence:one".into(),
            cleanup_attempt: 1,
            cleanup_lease_epoch: 1,
            cleanup_fencing_token: 1,
            hold_id: finished_hold.hold_id.clone(),
            expected_hold_revision: finished_hold.hold_revision,
            removal_proof_ref: "proof:one".into(),
            idempotency_key: "cleanup:one".into(),
            now_ms: 0,
        };
        let cleaned_bytes = fixture.commit(
            Method::CleanupDevelopmentLane {
                request: cleanup_request.clone(),
            },
            200,
        );
        let cleaned: DevelopmentLaneCleanupCompleteResult = fixture.decode(&cleaned_bytes);
        assert_eq!(
            cleaned.decision,
            DevelopmentLaneCleanupCompleteResultDecision::Accepted
        );
        assert_eq!(
            cleaned.hold.as_ref().map(|value| value.state),
            Some(DevelopmentLaneHoldState::Cleaned)
        );
        let clean_replay_bytes = fixture.commit(
            Method::CleanupDevelopmentLane {
                request: cleanup_request.clone(),
            },
            200,
        );
        assert_eq!(clean_replay_bytes, cleaned_bytes);
        let clean_replay: DevelopmentLaneCleanupCompleteResult =
            fixture.decode(&clean_replay_bytes);
        assert_eq!(
            clean_replay.decision,
            DevelopmentLaneCleanupCompleteResultDecision::Accepted
        );

        let status_before_fresh = read_development_lane_status(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneStatusRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                tenant_ref: "tenant:a".into(),
                hold_id: None,
                lane_id: None,
                work_item_id: None,
                limit: 10,
                cursor: None,
                now_ms: 0,
            },
            200,
            DurableCrypto::none(),
        )
        .expect("status before fresh cleanup replay");
        let mut fresh_replay_request = cleanup_request.clone();
        fresh_replay_request.idempotency_key = "cleanup:fresh-replay".into();
        let fresh_replay: DevelopmentLaneCleanupCompleteResult = fixture.decode(&fixture.commit(
            Method::CleanupDevelopmentLane {
                request: fresh_replay_request,
            },
            200,
        ));
        assert_eq!(
            fresh_replay.decision,
            DevelopmentLaneCleanupCompleteResultDecision::Idempotent
        );
        let status_after_fresh = read_development_lane_status(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneStatusRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                tenant_ref: "tenant:a".into(),
                hold_id: None,
                lane_id: None,
                work_item_id: None,
                limit: 10,
                cursor: None,
                now_ms: 0,
            },
            200,
            DurableCrypto::none(),
        )
        .expect("status after fresh cleanup replay");
        assert_eq!(status_after_fresh.counters, status_before_fresh.counters);
        assert_eq!(status_after_fresh.holds, status_before_fresh.holds);

        let mut wrong_revision = cleanup_request.clone();
        wrong_revision.idempotency_key = "cleanup:wrong-revision".into();
        wrong_revision.expected_hold_revision = wrong_revision
            .expected_hold_revision
            .checked_sub(1)
            .expect("finished hold revision is nonzero");
        let revision_conflict: DevelopmentLaneCleanupCompleteResult =
            fixture.decode(&fixture.commit(
                Method::CleanupDevelopmentLane {
                    request: wrong_revision,
                },
                200,
            ));
        assert_eq!(
            revision_conflict.decision,
            DevelopmentLaneCleanupCompleteResultDecision::InputConflict
        );

        let mut wrong_proof = cleanup_request.clone();
        wrong_proof.idempotency_key = "cleanup:wrong-proof".into();
        wrong_proof.removal_proof_ref = "proof:forged".into();
        let conflict: DevelopmentLaneCleanupCompleteResult = fixture.decode(&fixture.commit(
            Method::CleanupDevelopmentLane {
                request: wrong_proof,
            },
            200,
        ));
        assert_eq!(
            conflict.decision,
            DevelopmentLaneCleanupCompleteResultDecision::InputConflict
        );

        let mut wrong_fence = cleanup_request;
        wrong_fence.idempotency_key = "cleanup:wrong-fence".into();
        wrong_fence.cleanup_work_item_fence = "cleanup-fence:forged".into();
        let fence_refusal: DevelopmentLaneCleanupCompleteResult = fixture.decode(&fixture.commit(
            Method::CleanupDevelopmentLane {
                request: wrong_fence,
            },
            200,
        ));
        assert_eq!(
            fence_refusal.decision,
            DevelopmentLaneCleanupCompleteResultDecision::WrongFence
        );
        let status = read_development_lane_status(
            &fixture.db,
            TEST_GRAPH,
            &DevelopmentLaneStatusRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                tenant_ref: "tenant:a".into(),
                hold_id: None,
                lane_id: None,
                work_item_id: None,
                limit: 10,
                cursor: None,
                now_ms: 0,
            },
            200,
            DurableCrypto::none(),
        )
        .expect("status after cleanup");
        assert_eq!(status.holds.len(), 1);
        assert_eq!(status.tenant_active_count, 0);
        assert_eq!(status.tenant_retained_disk_bytes, 0);

        // A cleaned tombstone remains queryable for bounded replay, so a
        // checkpoint must carry both its terminal lifecycle WorkItem and the
        // exact cleanup WorkItem correlation rather than silently preserving a
        // native row after either node was dropped.
        let cleaned_hold = cleaned.hold.as_ref().expect("cleaned hold link");
        let rtx = fixture
            .db
            .begin_read()
            .expect("begin cleaned checkpoint validation");
        let nodes = rtx
            .open_table(NODES)
            .expect("open cleaned checkpoint nodes");
        let holds = rtx
            .open_table(HOLDS)
            .expect("open cleaned checkpoint holds");
        let lifecycle = nodes
            .get((TEST_GRAPH, cleaned_hold.work_item_id.as_str()))
            .expect("read cleaned lifecycle WorkItem")
            .expect("cleaned lifecycle WorkItem exists");
        let cleanup = nodes
            .get((TEST_GRAPH, "cleanup:one"))
            .expect("read cleaned cleanup WorkItem")
            .expect("cleaned cleanup WorkItem exists");
        let incoming = vec![
            (
                cleaned_hold.work_item_id.clone(),
                lifecycle.value().to_vec(),
            ),
            ("cleanup:one".to_string(), cleanup.value().to_vec()),
        ];
        validate_checkpoint_lane_links(TEST_GRAPH, &incoming, &holds, DurableCrypto::none())
            .expect("cleaned tombstone has exact linked WorkItems");
        assert!(validate_checkpoint_lane_links(
            TEST_GRAPH,
            &[(
                cleaned_hold.work_item_id.clone(),
                lifecycle.value().to_vec()
            )],
            &holds,
            DurableCrypto::none(),
        )
        .is_err());
    }

    #[test]
    fn native_global_and_tenant_drain_use_monotonic_cas_but_allow_lifecycle_cleanup() {
        let fixture = NativeLaneFixture::new(policy());
        let accepted_bytes =
            fixture.commit(fixture.reserve_method("reserve:drain-existing"), TEST_NOW);
        let accepted: DevelopmentLaneResult = fixture.decode(&accepted_bytes);
        let hold = accepted.hold.expect("drain test hold");
        let observed: DevelopmentLaneObserveResult = fixture.decode(&fixture.commit(
            Method::ObserveDevelopmentLane {
                request: DevelopmentLaneObserveRequest {
                    schema_version: DevelopmentLaneObserveRequestSchemaVersion::V1,
                    tenant_ref: hold.tenant_ref.clone(),
                    work_item_id: hold.work_item_id.clone(),
                    owner_id: hold.owner_id.clone(),
                    attempt: hold.attempt,
                    lease_epoch: hold.lease_epoch,
                    fencing_token: hold.fencing_token,
                    work_item_fence: hold.work_item_fence.clone(),
                    hold_id: hold.hold_id.clone(),
                    expected_hold_revision: hold.hold_revision,
                    observed_disk_bytes: 10,
                    observation_revision: 1,
                    idempotency_key: "observe:drain".into(),
                    now_ms: 0,
                },
            },
            150,
        ));
        assert_eq!(
            observed.decision,
            DevelopmentLaneObserveResultDecision::Accepted
        );
        let hold = observed.hold.expect("observed drain hold");
        let mut drain_policy = policy();
        drain_policy.drain_only = true;
        let global_drain: DevelopmentLaneQuotaUpdateResult = fixture.decode(&fixture.commit(
            Method::UpdateDevelopmentLaneQuota {
                request: DevelopmentLaneQuotaUpdateRequest {
                    schema_version:
                        crate::epistemic_operations::DevelopmentLaneQuotaUpdateRequestSchemaVersion::V1,
                    tenant_ref: GLOBAL_POLICY_KEY.into(),
                    policy: drain_policy.clone(),
                    expected_policy_revision: 1,
                    expected_policy_version: Some("1".into()),
                    idempotency_key: "policy:global-drain".into(),
                    now_ms: 0,
                },
            },
            160,
        ));
        assert_eq!(
            global_drain.decision,
            DevelopmentLaneQuotaUpdateResultDecision::Accepted
        );

        // Replay is checked before policy/drain/exclusivity evaluation.  An
        // acknowledgement retry therefore returns the original Accepted
        // bytes, even after the graph has entered drain-only mode.
        assert_eq!(
            fixture.commit(fixture.reserve_method("reserve:drain-existing"), 999),
            accepted_bytes
        );

        let candidate = fixture.candidate("drain-new", "branch:drain-new", "lanes/drain-new");
        let refused: DevelopmentLaneResult = fixture.decode(&fixture.commit(
            Method::ReserveDevelopmentLane {
                request: DevelopmentLaneReserveRequest {
                    idempotency_key: "reserve:drain-new".into(),
                    ..candidate
                },
            },
            170,
        ));
        assert_eq!(refused.decision, DevelopmentLaneResultDecision::Drained);

        let tenant_drain: DevelopmentLaneQuotaUpdateResult = fixture.decode(&fixture.commit(
            Method::UpdateDevelopmentLaneQuota {
                request: DevelopmentLaneQuotaUpdateRequest {
                    schema_version:
                        crate::epistemic_operations::DevelopmentLaneQuotaUpdateRequestSchemaVersion::V1,
                    tenant_ref: "tenant:a".into(),
                    policy: drain_policy,
                    expected_policy_revision: 1,
                    expected_policy_version: Some("1".into()),
                    idempotency_key: "policy:tenant-drain".into(),
                    now_ms: 0,
                },
            },
            171,
        ));
        assert_eq!(
            tenant_drain.decision,
            DevelopmentLaneQuotaUpdateResultDecision::Accepted
        );
        let stale_tenant: DevelopmentLaneQuotaUpdateResult = fixture.decode(&fixture.commit(
            Method::UpdateDevelopmentLaneQuota {
                request: DevelopmentLaneQuotaUpdateRequest {
                    schema_version:
                        crate::epistemic_operations::DevelopmentLaneQuotaUpdateRequestSchemaVersion::V1,
                    tenant_ref: "tenant:a".into(),
                    policy: policy(),
                    expected_policy_revision: 1,
                    expected_policy_version: Some("1".into()),
                    idempotency_key: "policy:tenant-stale".into(),
                    now_ms: 0,
                },
            },
            172,
        ));
        assert_eq!(
            stale_tenant.decision,
            DevelopmentLaneQuotaUpdateResultDecision::Stale
        );

        let renewed: DevelopmentLaneRenewResult = fixture.decode(&fixture.commit(
            Method::RenewDevelopmentLane {
                request: DevelopmentLaneRenewRequest {
                    schema_version: DevelopmentLaneRenewRequestSchemaVersion::V1,
                    tenant_ref: hold.tenant_ref.clone(),
                    work_item_id: hold.work_item_id.clone(),
                    owner_id: hold.owner_id.clone(),
                    attempt: hold.attempt,
                    lease_epoch: hold.lease_epoch,
                    fencing_token: hold.fencing_token,
                    work_item_fence: hold.work_item_fence.clone(),
                    hold_id: hold.hold_id.clone(),
                    expected_hold_revision: hold.hold_revision,
                    ttl_ms: 1_000,
                    idempotency_key: "renew:drain".into(),
                    now_ms: 0,
                },
            },
            170,
        ));
        assert_eq!(
            renewed.decision,
            DevelopmentLaneRenewResultDecision::Accepted
        );
        let hold = renewed.hold.expect("renewed drain hold");
        fixture
            .commit_work_item_result("succeeded", false, "finish-drain-terminal", 180)
            .expect("generic terminal WorkItem commit");
        let finished: DevelopmentLaneFinishResult = fixture.decode(&fixture.commit(
            Method::FinishDevelopmentLane {
                request: DevelopmentLaneFinishRequest {
                    schema_version: DevelopmentLaneFinishRequestSchemaVersion::V1,
                    tenant_ref: hold.tenant_ref.clone(),
                    work_item_id: hold.work_item_id.clone(),
                    owner_id: hold.owner_id.clone(),
                    attempt: hold.attempt,
                    lease_epoch: hold.lease_epoch,
                    fencing_token: hold.fencing_token,
                    work_item_fence: hold.work_item_fence.clone(),
                    hold_id: hold.hold_id.clone(),
                    expected_hold_revision: hold.hold_revision,
                    terminal_state: DevelopmentLaneFinishRequestTerminalState::Succeeded,
                    idempotency_key: "finish:drain".into(),
                    now_ms: 0,
                },
            },
            180,
        ));
        assert_eq!(
            finished.decision,
            DevelopmentLaneFinishResultDecision::Idempotent
        );
        let hold = finished.hold.expect("finished drain hold");
        fixture.seed_cleanup_work_item(&hold, "cleanup:drain", "cleanup-fence:drain");
        let cleaned: DevelopmentLaneCleanupCompleteResult = fixture.decode(&fixture.commit(
            Method::CleanupDevelopmentLane {
                request: DevelopmentLaneCleanupCompleteRequest {
                    schema_version: DevelopmentLaneCleanupCompleteRequestSchemaVersion::V1,
                    tenant_ref: hold.tenant_ref.clone(),
                    work_item_id: hold.work_item_id.clone(),
                    owner_id: hold.owner_id.clone(),
                    attempt: hold.attempt,
                    lease_epoch: hold.lease_epoch,
                    fencing_token: hold.fencing_token,
                    work_item_fence: hold.work_item_fence.clone(),
                    cleanup_work_item_id: "cleanup:drain".into(),
                    cleanup_work_item_fence: "cleanup-fence:drain".into(),
                    cleanup_attempt: 1,
                    cleanup_lease_epoch: 1,
                    cleanup_fencing_token: 1,
                    hold_id: hold.hold_id.clone(),
                    expected_hold_revision: hold.hold_revision,
                    removal_proof_ref: "proof:drain".into(),
                    idempotency_key: "cleanup:drain".into(),
                    now_ms: 0,
                },
            },
            190,
        ));
        assert_eq!(
            cleaned.decision,
            DevelopmentLaneCleanupCompleteResultDecision::Accepted
        );
    }

    #[test]
    fn native_reopen_preserves_exact_hold_and_status_is_tenant_bounded() {
        let fixture = NativeLaneFixture::new(policy());
        let accepted: DevelopmentLaneResult =
            fixture.decode(&fixture.commit(fixture.reserve_method("reserve:reopen"), TEST_NOW));
        let hold = accepted.hold.expect("reopen hold");
        let path = fixture.close_for_reopen();
        let reopened = Database::open(&path).expect("reopen lane database");
        let query = read_development_lane(
            &reopened,
            TEST_GRAPH,
            &DevelopmentLaneQueryRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneQueryRequestSchemaVersion::V1,
                tenant_ref: "tenant:a".into(),
                hold_id: hold.hold_id.clone(),
                now_ms: 0,
            },
            TEST_NOW,
            DurableCrypto::none(),
        )
        .expect("query after reopen");
        assert_eq!(query.decision, DevelopmentLaneQueryResultDecision::Accepted);
        assert_eq!(query.hold.expect("reopened hold"), hold);
        let isolated = read_development_lane_status(
            &reopened,
            TEST_GRAPH,
            &DevelopmentLaneStatusRequest {
                schema_version:
                    crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
                tenant_ref: "tenant:other".into(),
                hold_id: None,
                lane_id: None,
                work_item_id: None,
                limit: 10,
                cursor: None,
                now_ms: 0,
            },
            TEST_NOW,
            DurableCrypto::none(),
        )
        .expect("tenant-isolated status");
        assert!(isolated.holds.is_empty());
        assert_eq!(isolated.tenant_active_count, 0);
        drop(reopened);
        std::fs::remove_file(path).expect("remove reopened lane database");
    }
}
