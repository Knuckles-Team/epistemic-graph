use super::super::DurableCrypto;
use super::identity::hold_load;
use super::links::text;
use super::quota::{
    empty_charge, load_global_policy, load_policy, load_scope_counters, snapshot_charge,
};
use super::results::public_hold;
use super::rows::LaneRows;
use super::types::Scope;
use super::validation::{decision_name, validate_method_bounds};
use super::{COUNTERS, HOLDS, MAX_STATUS_LIMIT, MAX_STATUS_SCAN, POLICIES, TENANT_INDEX};
use crate::epistemic_operations::{
    DevelopmentLaneHold, DevelopmentLaneHoldHostTargetKind, DevelopmentLaneHoldState,
    DevelopmentLaneStatusRequest, DevelopmentLaneStatusResult,
    DevelopmentLaneStatusResultSchemaVersion,
};
use crate::protocol::Method;
use crate::redb_store::shard::Shard;
use eg_storage::{GraphShardOwner, ScopedRead};

/// One bounded page of the tenant status scan.
pub(super) struct StatusPage {
    rows: Vec<DevelopmentLaneHold>,
    has_more: bool,
    last: Option<String>,
}

/// Advance the bounded scan counter, failing closed on overflow or on
/// exceeding the native scan bound.
pub(super) fn next_status_scan(scanned: usize) -> Result<usize, String> {
    let scanned = scanned
        .checked_add(1)
        .ok_or_else(|| "development lane status scan overflow".to_string())?;
    if scanned > MAX_STATUS_SCAN {
        return Err("development lane status scan exceeds native bound".to_string());
    }
    Ok(scanned)
}

/// Does this hold fail any of the caller's optional exact filters?
pub(super) fn status_filters_reject(
    request: &DevelopmentLaneStatusRequest,
    hold: &DevelopmentLaneHold,
) -> bool {
    crate::redb_store::any_optional_text_filter_mismatch([
        (request.hold_id.as_deref(), hold.hold_id.as_str()),
        (request.lane_id.as_deref(), hold.lane_id.as_str()),
        (request.work_item_id.as_deref(), hold.work_item_id.as_str()),
    ])
}

/// Walk the tenant keyset from `cursor` and project one bounded page of
/// redacted holds.  The scan is bounded by `MAX_STATUS_SCAN` regardless of how
/// many rows the filters reject.
pub(super) fn scan_status_page<H, I>(
    graph: &str,
    request: &DevelopmentLaneStatusRequest,
    cursor: &str,
    holds: &H,
    tenant_index: &I,
    crypto: DurableCrypto<'_>,
) -> Result<StatusPage, String>
where
    H: LaneRows<(&'static str, &'static str), &'static [u8]>,
    I: LaneRows<(&'static str, &'static str, &'static str), &'static str>,
{
    let mut page = StatusPage {
        rows: Vec::new(),
        has_more: false,
        last: None,
    };
    let mut scanned = 0usize;
    tenant_index.visit_scope_rows(&mut |key: (&str, &str, &str), value: &str| {
        let (_, tenant, hold_id) = key;
        // The walk starts at this graph's first keyset row rather than at the
        // caller's cursor: a scope-bounded table has no cursor-positioned range
        // for a key whose non-leading components are `&str`.  Rows at or below
        // the cursor are skipped WITHOUT counting, so `MAX_STATUS_SCAN` still
        // bounds exactly the work one page does past its own cursor.
        if tenant < request.tenant_ref.as_str() {
            return Ok(true);
        }
        if tenant > request.tenant_ref.as_str() {
            return Ok(false);
        }
        if hold_id <= cursor {
            return Ok(true);
        }
        scanned = next_status_scan(scanned)?;
        if value != hold_id {
            return Ok(true);
        }
        let Some(row) = hold_load(holds, graph, hold_id, crypto)? else {
            return Err("development lane status index points to a missing hold".to_string());
        };
        if status_filters_reject(request, &row.hold) {
            return Ok(true);
        }
        page.rows.push(public_hold(&row.hold));
        if page.rows.len() > request.limit as usize {
            // Read one row beyond the requested page before declaring a next
            // page.  Exactly `limit` rows therefore produce a complete page;
            // the extra row is only a bounded existence probe.
            page.rows.pop();
            page.last = page.rows.last().map(|value| value.hold_id.clone());
            page.has_more = true;
            return Ok(false);
        }
        page.last = Some(hold_id.to_string());
        Ok(true)
    })?;
    Ok(page)
}

pub(super) fn read_lane_status(
    read: &ScopedRead<'_, GraphShardOwner>,
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
    let holds = read.scoped_owner_table(HOLDS)?;
    let tenant_index = read.scoped_owner_table(TENANT_INDEX)?;
    let policies = read.scoped_owner_table(POLICIES)?;
    let counters = read.scoped_owner_table(COUNTERS)?;
    let policy_revision = load_policy(&policies, graph, &request.tenant_ref, crypto)?
        .map_or(0, |value| value.policy_revision);
    let StatusPage {
        rows,
        has_more,
        last,
    } = scan_status_page(graph, request, cursor, &holds, &tenant_index, crypto)?;
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
    shard: &Shard,
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
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    read_lane_status(&read, graph, &request, crypto)
}
