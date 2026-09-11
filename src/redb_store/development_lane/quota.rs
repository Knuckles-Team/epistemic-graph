use super::super::{resource_decode, resource_encode, DurableCrypto};
use super::reserve_support::{hold_target_kind, policy_validate};
use super::rows::LaneRows;
use super::types::{Scope, ScopeCounter};
use super::validation::decision_name;
use super::{
    DurableLaneCounter, DurableLaneHold, DurableLanePolicy, GLOBAL_POLICY_KEY, MAX_COUNT,
    MAX_DISK_BYTES,
};
use crate::epistemic_operations::{
    DevelopmentLaneHold, DevelopmentLaneIntent, DevelopmentLaneQuotaCharge,
    DevelopmentLaneQuotaChargeSchemaVersion, DevelopmentLaneQuotaPolicy,
    DevelopmentLaneReserveRequest,
};
use eg_storage::ScopedOwnerTableMut;
use sha2::{Digest, Sha256};

pub(super) fn scope_key(scope: Scope, hold: &DevelopmentLaneHold) -> String {
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

/// The canonical scope order.  It is the index order of every per-scope limit
/// table (`policy_count_limits` and friends) as well as the iteration order of
/// `scopes`, so the two can never disagree.
pub(super) const SCOPES: [Scope; 7] = [
    Scope::Tenant,
    Scope::Owner,
    Scope::Session,
    Scope::Workspace,
    Scope::Repository,
    Scope::Host,
    Scope::Global,
];

/// This must stay the position of `scope` within `SCOPES`.
pub(super) fn scope_index(scope: Scope) -> usize {
    match scope {
        Scope::Tenant => 0,
        Scope::Owner => 1,
        Scope::Session => 2,
        Scope::Workspace => 3,
        Scope::Repository => 4,
        Scope::Host => 5,
        Scope::Global => 6,
    }
}

pub(super) fn scopes(hold: &DevelopmentLaneHold) -> Vec<(Scope, String)> {
    SCOPES
        .into_iter()
        .map(|scope| (scope, scope_key(scope, hold)))
        .collect()
}

/// Every per-scope limit table below is ordered exactly like `SCOPES`, so a
/// scope resolves to one index shared by all four metrics.  Keeping the tables
/// in that one order is what lets `policy_limit` be an index instead of a
/// twenty-eight arm `(scope, metric)` match.
pub(super) fn policy_count_limits(policy: &DevelopmentLaneQuotaPolicy) -> [u64; 7] {
    [
        policy.tenant_count_limit,
        policy.owner_count_limit,
        policy.session_count_limit,
        policy.workspace_count_limit,
        policy.repository_count_limit,
        policy.host_count_limit,
        policy.global_count_limit,
    ]
}

pub(super) fn policy_predicted_limits(policy: &DevelopmentLaneQuotaPolicy) -> [u64; 7] {
    [
        policy.tenant_predicted_disk_bytes,
        policy.owner_predicted_disk_bytes,
        policy.session_predicted_disk_bytes,
        policy.workspace_predicted_disk_bytes,
        policy.repository_predicted_disk_bytes,
        policy.host_predicted_disk_bytes,
        policy.global_predicted_disk_bytes,
    ]
}

pub(super) fn policy_observed_limits(policy: &DevelopmentLaneQuotaPolicy) -> [u64; 7] {
    [
        policy.tenant_observed_disk_bytes,
        policy.owner_observed_disk_bytes,
        policy.session_observed_disk_bytes,
        policy.workspace_observed_disk_bytes,
        policy.repository_observed_disk_bytes,
        policy.host_observed_disk_bytes,
        policy.global_observed_disk_bytes,
    ]
}

pub(super) fn policy_retained_limits(policy: &DevelopmentLaneQuotaPolicy) -> [u64; 7] {
    [
        policy.tenant_retained_disk_bytes,
        policy.owner_retained_disk_bytes,
        policy.session_retained_disk_bytes,
        policy.workspace_retained_disk_bytes,
        policy.repository_retained_disk_bytes,
        policy.host_retained_disk_bytes,
        policy.global_retained_disk_bytes,
    ]
}

pub(super) fn policy_limit(
    policy: &DevelopmentLaneQuotaPolicy,
    scope: Scope,
    metric: Metric,
) -> u64 {
    let index = scope_index(scope);
    match metric {
        Metric::Count => policy_count_limits(policy)[index],
        Metric::Predicted => policy_predicted_limits(policy)[index],
        Metric::Observed => policy_observed_limits(policy)[index],
        Metric::Retained => policy_retained_limits(policy)[index],
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum Metric {
    Count,
    Predicted,
    Observed,
    Retained,
}

pub(super) fn durable_policy_bounds(row: &DurableLanePolicy) -> Result<(), String> {
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

pub(super) fn durable_counter_bounds(value: &DurableLaneCounter) -> Result<(), String> {
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

pub(super) fn load_policy<T>(
    policies: &T,
    graph: &str,
    tenant: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableLanePolicy>, String>
where
    T: LaneRows<(&'static str, &'static str), &'static [u8]>,
{
    policies
        .row((graph, tenant))?
        .map(|row| {
            let decoded: DurableLanePolicy = resource_decode(row.value(), crypto)?;
            durable_policy_bounds(&decoded)?;
            Ok::<DurableLanePolicy, String>(decoded)
        })
        .transpose()
}

pub(super) fn load_global_policy<T>(
    policies: &T,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableLanePolicy>, String>
where
    T: LaneRows<(&'static str, &'static str), &'static [u8]>,
{
    load_policy(policies, graph, GLOBAL_POLICY_KEY, crypto)
}

/// The global counter is a single graph-wide authority. Tenant policies may
/// differ in owner/session/workspace/repository/host limits, but every policy
/// must agree on the dimensions that govern the shared counter and freshness
/// gate. A typed quota update using `GLOBAL_POLICY_KEY` is the administrator
/// CAS route for changing those graph-wide controls.
pub(super) fn global_policy_equal(
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

pub(super) fn load_counter<T>(
    counters: &T,
    graph: &str,
    key: &str,
    scope: Scope,
    policy_revision: u64,
    global_policy_revision: u64,
    crypto: DurableCrypto<'_>,
) -> Result<DurableLaneCounter, String>
where
    T: LaneRows<(&'static str, &'static str), &'static [u8]>,
{
    let value: DurableLaneCounter = counters
        .row((graph, key))?
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

pub(super) fn put_counter(
    counters: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    graph: &str,
    key: &str,
    value: &DurableLaneCounter,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let bytes = resource_encode(value, crypto)?;
    counters.insert((graph, key), bytes.as_slice())?;
    Ok(())
}

pub(super) fn load_scope_counters<T>(
    counters: &T,
    graph: &str,
    hold: &DevelopmentLaneHold,
    policy_revision: u64,
    global_policy_revision: u64,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<ScopeCounter>, String>
where
    T: LaneRows<(&'static str, &'static str), &'static [u8]>,
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

pub(super) fn adjust(value: u64, amount: u64, increase: bool, name: &str) -> Result<u64, String> {
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

pub(super) fn pressure_scope_name(scope: Scope) -> Option<&'static str> {
    match scope {
        Scope::Owner => Some("owner"),
        Scope::Session => Some("session"),
        Scope::Workspace => Some("workspace"),
        Scope::Repository => Some("repository"),
        Scope::Host => Some("host"),
        Scope::Tenant | Scope::Global => None,
    }
}

pub(super) fn pressure_metric_name(metric: Metric) -> &'static str {
    match metric {
        Metric::Count => "count",
        Metric::Predicted => "predicted",
        Metric::Observed => "observed",
        Metric::Retained => "retained",
    }
}

// Private redb-transaction-scoped helper; see `load_work_item`'s justification
// above -- the borrowed table handle plus the composite-key fields it indexes
// on are each required independently.
#[allow(clippy::too_many_arguments)]
pub(super) fn pressure_replace(
    pressure_index: &mut ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
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
        pressure_index.remove((graph, tenant, scope_name, metric_name, old, counter_key))?;
    }
    if new > 0 {
        pressure_index.insert(
            (graph, tenant, scope_name, metric_name, new, counter_key),
            0,
        )?;
    }
    Ok(())
}

// Private redb-transaction-scoped helper; see `load_work_item`'s justification
// above -- two borrowed table handles plus the independent per-scope delta
// inputs it applies atomically.
#[allow(clippy::too_many_arguments)]
pub(super) fn apply_counter_delta(
    counters: &mut ScopedOwnerTableMut<(&str, &str), &[u8]>,
    pressure_index: &mut ScopedOwnerTableMut<(&str, &str, &str, &str, u64, &str), u8>,
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

pub(super) fn hold_charge(
    hold: &DevelopmentLaneHold,
    revision: u64,
    policy_revision: u64,
) -> DevelopmentLaneQuotaCharge {
    // An uncharged hold contributes nothing to the count/predicted/observed
    // scopes.  Retained disk is charged either way: only cleanup releases it.
    let count = u64::from(hold.active_count_charged);
    let predicted = if hold.active_count_charged {
        hold.predicted_disk_bytes
    } else {
        0
    };
    let observed = if hold.active_count_charged {
        hold.observed_disk_bytes
    } else {
        0
    };
    let retained = hold.retained_disk_bytes;
    DevelopmentLaneQuotaCharge {
        schema_version: DevelopmentLaneQuotaChargeSchemaVersion::V1,
        tenant_count: count,
        owner_count: count,
        session_count: count,
        workspace_count: count,
        repository_count: count,
        host_count: count,
        global_count: count,
        tenant_predicted_disk_bytes: predicted,
        owner_predicted_disk_bytes: predicted,
        session_predicted_disk_bytes: predicted,
        workspace_predicted_disk_bytes: predicted,
        repository_predicted_disk_bytes: predicted,
        host_predicted_disk_bytes: predicted,
        global_predicted_disk_bytes: predicted,
        tenant_observed_disk_bytes: observed,
        owner_observed_disk_bytes: observed,
        session_observed_disk_bytes: observed,
        workspace_observed_disk_bytes: observed,
        repository_observed_disk_bytes: observed,
        host_observed_disk_bytes: observed,
        global_observed_disk_bytes: observed,
        tenant_retained_disk_bytes: retained,
        owner_retained_disk_bytes: retained,
        session_retained_disk_bytes: retained,
        workspace_retained_disk_bytes: retained,
        repository_retained_disk_bytes: retained,
        host_retained_disk_bytes: retained,
        global_retained_disk_bytes: retained,
        revision,
        policy_revision,
    }
}

pub(super) fn empty_charge(policy_revision: u64) -> DevelopmentLaneQuotaCharge {
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

pub(super) fn snapshot_charge(
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

pub(super) fn branch_key(hold: &DevelopmentLaneHold) -> String {
    format!("{}\0{}", hold.repository_id, hold.branch)
}

pub(super) fn worktree_key(hold: &DevelopmentLaneHold) -> String {
    format!(
        "{}\0{}\0{}",
        hold.host_ref, hold.workspace_ref, hold.worktree_locator
    )
}

pub(super) fn hold_id(intent: &DevelopmentLaneIntent) -> String {
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

/// The WorkItem fence tuple the reserve request must reproduce exactly.
pub(super) fn hold_work_item_identity_equal(
    row: &DurableLaneHold,
    request: &DevelopmentLaneReserveRequest,
) -> bool {
    let hold = &row.hold;
    hold.tenant_ref == request.tenant_ref
        && hold.work_item_id == request.work_item_id
        && hold.owner_id == request.owner_id
        && hold.attempt == request.attempt
        && hold.lease_epoch == request.lease_epoch
        && hold.fencing_token == request.fencing_token
        && hold.work_item_fence == request.work_item_fence
}

/// The immutable lane/repository identity of the hold.
pub(super) fn hold_lane_identity_equal(
    row: &DurableLaneHold,
    request: &DevelopmentLaneReserveRequest,
) -> bool {
    let hold = &row.hold;
    hold.lane_id == request.intent.lane_id
        && hold.request_id == request.intent.request_id
        && hold.repository_id == request.intent.repository_id
        && hold.base_ref == request.intent.base_ref
        && hold.base_sha == request.intent.base_sha
        && hold.branch == request.intent.branch
}

/// Workspace/worktree/host placement, which exclusivity indexes key on.
pub(super) fn hold_placement_equal(
    row: &DurableLaneHold,
    request: &DevelopmentLaneReserveRequest,
) -> bool {
    let hold = &row.hold;
    hold.workspace_ref == request.intent.workspace_ref
        && hold.worktree_locator == request.intent.worktree_locator
        && hold.host_ref == request.intent.host_ref
        && hold.host_target_kind == hold_target_kind(request.intent.host_target_kind)
        && hold.host_target_alias == request.intent.host_target_alias
}

/// Reservation, TTL, fairness and quota inputs.
pub(super) fn hold_quota_identity_equal(
    row: &DurableLaneHold,
    request: &DevelopmentLaneReserveRequest,
) -> bool {
    let hold = &row.hold;
    row.resource_reservation_id == request.intent.resource_reservation_id
        && row.ttl_ms == request.intent.ttl_ms
        && hold.session_id == request.intent.session_id
        && hold.fairness_group == request.intent.fairness_group
        && hold.quota_policy_name == request.intent.quota_policy_name
        && hold.quota_policy_version == request.intent.quota_policy_version
        && hold.predicted_disk_bytes == request.intent.predicted_disk_bytes
        && hold.input_fingerprint == request.intent.input_fingerprint
}
