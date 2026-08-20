//! Authoritative redb capacity-cell/lease ledger (GOC-21-W04/W05).
//!
//! The pure [`eg_types::capacity_lease::CapacityLedger`] is useful for policy
//! tests, but it is deliberately not an authority.  This module owns the
//! graph-scoped durable cells, aggregate usage, lease fences, and tenant/key
//! replay rows.  Every write below runs in one immediate redb transaction;
//! all validation happens before the first insert/update so a denial cannot
//! leave a partially charged dimension.

use std::collections::BTreeSet;

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use eg_types::capacity_lease::{CapacityCell, CapacityLease, LeaseState};
use eg_types::native_control::{
    CapacityAcquireRequest, CapacityAcquireResult, CapacityAvailability,
    CapacityCellUpdateRequest, CapacityCellUpdateResult, CapacityDecision, CapacityDemand,
    CapacityLeaseMutationRequest, CapacityMutationResult, CapacityReclaimRequest,
    CapacityReclaimResult, CapacityStatusRequest, CapacityStatusResult,
    NativeControlSchemaVersion, MAX_CAPACITY_AMOUNT, MAX_CAPACITY_BUDGET,
    MAX_CAPACITY_DEMANDS, MAX_CAPACITY_ID_BYTES, MAX_CAPACITY_MUTATION_BATCH,
    MAX_CAPACITY_RECLAIM_BATCH, MAX_CAPACITY_STATUS_ROWS, MAX_CAPACITY_TTL_MS,
};

use super::{decode_durable, DurableCrypto};
use crate::protocol::Method;

pub(crate) const CELLS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("capacity_cells");
pub(crate) const LEASES: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("capacity_leases");
pub(crate) const USAGE: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("capacity_usage");
pub(crate) const IDEMPOTENCY: TableDefinition<(&str, &str, &str), &[u8]> =
    TableDefinition::new("capacity_idempotency");

const MAX_SCAN: usize = 4096;
const DEFAULT_TTL_MS: u64 = 60_000;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableUsage {
    leased_amount: u64,
    next_fence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableReplay {
    digest: String,
    operation: String,
    result: Vec<u8>,
}

pub(crate) fn initialize_tables(wtx: &WriteTransaction) -> Result<(), String> {
    wtx.open_table(CELLS).map_err(|e| e.to_string())?;
    wtx.open_table(LEASES).map_err(|e| e.to_string())?;
    wtx.open_table(USAGE).map_err(|e| e.to_string())?;
    wtx.open_table(IDEMPOTENCY).map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) fn clear_graph_rows(wtx: &WriteTransaction, graph: &str) -> Result<(), String> {
    let mut cells = wtx.open_table(CELLS).map_err(|e| e.to_string())?;
    let cell_keys = cells
        .range((graph, "")..)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, cell_id) = key.value();
            if row_graph != graph {
                return Ok::<_, String>(None);
            }
            Ok(Some(cell_id.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for cell in cell_keys.into_iter().flatten() {
        cells.remove((graph, cell.as_str())).map_err(|e| e.to_string())?;
    }
    drop(cells);
    let mut leases = wtx.open_table(LEASES).map_err(|e| e.to_string())?;
    let lease_keys = leases
        .range((graph, "")..)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, lease_id) = key.value();
            if row_graph != graph {
                return Ok::<_, String>(None);
            }
            Ok(Some(lease_id.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for lease in lease_keys.into_iter().flatten() {
        leases.remove((graph, lease.as_str())).map_err(|e| e.to_string())?;
    }
    drop(leases);
    let mut usage = wtx.open_table(USAGE).map_err(|e| e.to_string())?;
    let usage_keys = usage
        .range((graph, "")..)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, cell_id) = key.value();
            if row_graph != graph {
                return Ok::<_, String>(None);
            }
            Ok(Some(cell_id.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for cell in usage_keys.into_iter().flatten() {
        usage.remove((graph, cell.as_str())).map_err(|e| e.to_string())?;
    }
    drop(usage);
    let mut idem = wtx.open_table(IDEMPOTENCY).map_err(|e| e.to_string())?;
    let idem_keys = idem
        .range((graph, "", "")..)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, tenant, idem_key) = key.value();
            if row_graph == graph {
                Ok::<_, String>(Some((tenant.to_string(), idem_key.to_string())))
            } else {
                Ok(None)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    for key in idem_keys.into_iter().flatten() {
        idem.remove((graph, key.0.as_str(), key.1.as_str()))
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

pub(crate) fn commit(
    db: &Database,
    graph: &str,
    method: &Method,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut super::AuditTailCache,
) -> Result<Vec<u8>, String> {
    let mut wtx = db.begin_write().map_err(|e| e.to_string())?;
    wtx.set_durability(Durability::Immediate)
        .map_err(|e| e.to_string())?;
    let bytes = apply_in_wtx(&wtx, graph, method, crypto)?;
    #[cfg(feature = "security")]
    let mut staged_audit_tail = audit_tail.clone();
    #[cfg(feature = "security")]
    if !result_is_replay(method, &bytes)? {
        let mut audit = wtx
            .open_table(super::AUDIT)
            .map_err(|e| e.to_string())?;
        super::append_audit_entry(&mut audit, &mut staged_audit_tail, graph, method)?;
    }
    wtx.commit().map_err(|e| e.to_string())?;
    #[cfg(feature = "security")]
    {
        *audit_tail = staged_audit_tail;
    }
    Ok(bytes)
}

#[cfg(feature = "security")]
fn result_is_replay(method: &Method, bytes: &[u8]) -> Result<bool, String> {
    match method {
        Method::AcquireCapacity { .. } => {
            Ok(decode_durable::<CapacityAcquireResult>(bytes)?.decision == CapacityDecision::Replayed)
        }
        Method::RenewCapacity { .. } | Method::ReleaseCapacity { .. } => Ok(
            decode_durable::<CapacityMutationResult>(bytes)?.decision
                == CapacityDecision::Replayed,
        ),
        _ => Ok(false),
    }
}

pub(crate) fn read(
    db: &Database,
    graph: &str,
    request: &CapacityStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<CapacityStatusResult, String> {
    validate_status_request(request)?;
    let rtx = db.begin_read().map_err(|e| e.to_string())?;
    let cells = rtx.open_table(CELLS).map_err(|e| e.to_string())?;
    let leases = rtx.open_table(LEASES).map_err(|e| e.to_string())?;
    let mut cell_rows = Vec::new();
    let mut scanned_cells = 0usize;
    for row in cells.range((graph, "")..).map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        let (row_graph, cell_id) = key.value();
        if row_graph != graph {
            break;
        }
        scanned_cells += 1;
        if scanned_cells > MAX_SCAN {
            return Err("capacity status cell scan exceeds native bound".to_string());
        }
        let cell: CapacityCell = decode_durable(&crypto.unseal(value.value())?)?;
        validate_cell_bounds(&cell)?;
        if request.cell_id.as_deref().is_none_or(|wanted| wanted == cell_id) {
            cell_rows.push(cell);
        }
        if cell_rows.len() > MAX_CAPACITY_STATUS_ROWS {
            return Err("capacity status cell page exceeds native bound".to_string());
        }
    }

    let mut lease_rows = Vec::new();
    let mut scanned_leases = 0usize;
    let cursor = request.cursor.as_deref().unwrap_or("");
    for row in leases.range((graph, "")..).map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        let (row_graph, lease_id) = key.value();
        if row_graph != graph {
            break;
        }
        scanned_leases += 1;
        if scanned_leases > MAX_SCAN {
            return Err("capacity status lease scan exceeds native bound".to_string());
        }
        if !cursor.is_empty() && lease_id <= cursor {
            continue;
        }
        let lease: CapacityLease = decode_durable(&crypto.unseal(value.value())?)?;
        if lease.tenant_ref != request.tenant_ref
            || request
                .cell_id
                .as_deref()
                .is_some_and(|wanted| wanted != lease.cell_id)
            || request
                .lease_id
                .as_deref()
                .is_some_and(|wanted| wanted != lease.lease_id)
        {
            continue;
        }
        lease_rows.push(lease);
        if lease_rows.len() >= request.max_count as usize {
            break;
        }
        if lease_rows.len() > MAX_CAPACITY_STATUS_ROWS {
            return Err("capacity status lease page exceeds native bound".to_string());
        }
    }
    let next_cursor = lease_rows.last().map(|lease| lease.lease_id.clone());
    Ok(CapacityStatusResult {
        schema_version: NativeControlSchemaVersion::V1,
        cells: cell_rows,
        leases: lease_rows,
        next_cursor,
    })
}

fn apply_in_wtx(
    wtx: &WriteTransaction,
    graph: &str,
    method: &Method,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<u8>, String> {
    match method {
        Method::AcquireCapacity { request } => {
            let result = acquire(wtx, graph, request, crypto)?;
            encode(&result)
        }
        Method::RenewCapacity { request } => {
            let result = mutate_leases(wtx, graph, request, true, crypto)?;
            encode(&result)
        }
        Method::ReleaseCapacity { request } => {
            let result = mutate_leases(wtx, graph, request, false, crypto)?;
            encode(&result)
        }
        Method::ReclaimExpiredCapacity { request } => {
            let result = reclaim(wtx, graph, request, crypto)?;
            encode(&result)
        }
        Method::UpdateCapacityCell { request } => {
            let result = update_cell(wtx, graph, request, crypto)?;
            encode(&result)
        }
        _ => Err("capacity ledger received an unsupported method".to_string()),
    }
}

fn acquire(
    wtx: &WriteTransaction,
    graph: &str,
    request: &CapacityAcquireRequest,
    crypto: DurableCrypto<'_>,
) -> Result<CapacityAcquireResult, String> {
    validate_acquire_request(request)?;
    let mut digest_request = request.clone();
    digest_request.now_ms = 0;
    let digest = request_digest(&digest_request)?;
    if let Some(replay) = read_replay(wtx, graph, &request.tenant_ref, &request.idempotency_key, crypto)? {
        if replay.digest != digest || replay.operation != "acquire" {
            return Err("IDEMPOTENCY_CONFLICT: capacity acquire key has a different request".to_string());
        }
        let mut result: CapacityAcquireResult =
            decode_durable(&crypto.unseal(&replay.result)?)?;
        result.decision = CapacityDecision::Replayed;
        return Ok(result);
    }

    // Expired capacity is reclaimed before the CAS check, in the same writer
    // transaction.  The scan is bounded; a pathological backlog fails closed
    // and asks the controller to drain it explicitly.
    reclaim_expired_inner(
        wtx,
        graph,
        request.now_ms,
        None,
        None,
        None,
        MAX_CAPACITY_RECLAIM_BATCH,
        crypto,
    )?;

    let cells = wtx.open_table(CELLS).map_err(|e| e.to_string())?;
    let mut usage = wtx.open_table(USAGE).map_err(|e| e.to_string())?;
    let mut leases = wtx.open_table(LEASES).map_err(|e| e.to_string())?;
    let mut demands = request.demands.clone();
    demands.sort_by(|left, right| (&left.cell_id, &left.resource_class, left.amount).cmp(&(&right.cell_id, &right.resource_class, right.amount)));
    let mut seen = BTreeSet::new();
    let mut availability = Vec::with_capacity(demands.len());
    let mut cell_rows = Vec::with_capacity(demands.len());
    let mut usage_rows = Vec::with_capacity(demands.len());
    for demand in &demands {
        if !seen.insert((demand.cell_id.clone(), demand.resource_class)) {
            return Err("capacity acquire demands must name each cell/resource dimension once".to_string());
        }
        let cell = cells
            .get((graph, demand.cell_id.as_str()))
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("capacity cell '{}' was not found", demand.cell_id))
            .and_then(|value| decode_durable::<CapacityCell>(&crypto.unseal(value.value())?))?;
        validate_cell_bounds(&cell)?;
        if cell.resource_class != demand.resource_class {
            return Err(format!("capacity cell '{}' resource dimension mismatch", demand.cell_id));
        }
        let row = usage
            .get((graph, demand.cell_id.as_str()))
            .map_err(|e| e.to_string())?
            .map(|value| decode_durable::<DurableUsage>(&crypto.unseal(value.value())?))
            .transpose()?
            .unwrap_or_default();
        let available = cell.available_for(request.priority, row.leased_amount);
        availability.push(CapacityAvailability {
            cell_id: demand.cell_id.clone(),
            resource_class: demand.resource_class,
            available,
            requested: demand.amount,
        });
        if demand.amount == 0 || demand.amount > MAX_CAPACITY_AMOUNT || available < demand.amount {
            return Ok(CapacityAcquireResult {
                schema_version: NativeControlSchemaVersion::V1,
                decision: CapacityDecision::Exhausted,
                leases: Vec::new(),
                available: availability,
                message: Some("capacity admission denied by native cell quota".to_string()),
            });
        }
        cell_rows.push(cell);
        usage_rows.push(row);
    }

    let mut out = Vec::with_capacity(demands.len());
    for (index, ((demand, cell), mut row)) in demands
        .iter()
        .zip(cell_rows)
        .zip(usage_rows)
        .enumerate()
    {
        row.next_fence = row
            .next_fence
            .checked_add(1)
            .ok_or_else(|| "capacity fence exhausted".to_string())?;
        let lease_id = lease_id(request, demand, index, &digest)?;
        if leases
            .get((graph, lease_id.as_str()))
            .map_err(|e| e.to_string())?
            .is_some()
        {
            return Err("capacity lease id is already in use".to_string());
        }
        let lease = CapacityLease {
            schema_version: 1,
            lease_id,
            work_item_id: request.work_item_id.clone(),
            tenant_ref: request.tenant_ref.clone(),
            actor_digest: request.owner_digest.clone(),
            cell_id: demand.cell_id.clone(),
            resource_class: demand.resource_class,
            amount: demand.amount,
            priority: request.priority,
            fence_token: row.next_fence,
            lease_epoch: cell.epoch,
            issued_at_ms: request.now_ms,
            expires_at_ms: request.now_ms.saturating_add(request.ttl_ms),
            renewed_count: 0,
            cost_budget_micros: request.cost_budget_micros,
            token_budget: request.token_budget,
            idempotency_key: request.idempotency_key.clone(),
            state: LeaseState::Active,
        };
        lease.validate().map_err(|error| format!("invalid capacity lease: {error:?}"))?;
        row.leased_amount = row
            .leased_amount
            .checked_add(demand.amount)
            .ok_or_else(|| "capacity usage overflow".to_string())?;
        let sealed_lease_bytes = rmp_serde::to_vec_named(&lease).map_err(|e| e.to_string())?;
        let sealed_lease = crypto.seal(&sealed_lease_bytes);
        leases
            .insert((graph, lease.lease_id.as_str()), sealed_lease.as_ref())
            .map_err(|e| e.to_string())?;
        let sealed_usage_bytes = rmp_serde::to_vec_named(&row).map_err(|e| e.to_string())?;
        let sealed_usage = crypto.seal(&sealed_usage_bytes);
        usage
            .insert((graph, demand.cell_id.as_str()), sealed_usage.as_ref())
            .map_err(|e| e.to_string())?;
        out.push(lease);
    }
    let result = CapacityAcquireResult {
        schema_version: NativeControlSchemaVersion::V1,
        decision: CapacityDecision::Accepted,
        leases: out,
        available: availability,
        message: None,
    };
    write_replay(wtx, graph, &request.tenant_ref, &request.idempotency_key, "acquire", &digest, &result, crypto)?;
    Ok(result)
}

fn mutate_leases(
    wtx: &WriteTransaction,
    graph: &str,
    request: &CapacityLeaseMutationRequest,
    renew: bool,
    crypto: DurableCrypto<'_>,
) -> Result<CapacityMutationResult, String> {
    validate_mutation_request(request, renew)?;
    let key = request.idempotency_key.as_deref();
    let mut digest_request = request.clone();
    digest_request.now_ms = 0;
    let digest = request_digest(&digest_request)?;
    if let Some(key) = key {
        if let Some(replay) = read_replay(wtx, graph, &request.tenant_ref, key, crypto)? {
            if replay.digest != digest || replay.operation != if renew { "renew" } else { "release" } {
                return Err("IDEMPOTENCY_CONFLICT: capacity mutation key has a different request".to_string());
            }
            let mut result: CapacityMutationResult =
                decode_durable(&crypto.unseal(&replay.result)?)?;
            result.decision = CapacityDecision::Replayed;
            return Ok(result);
        }
    }
    let leases_table = wtx.open_table(LEASES).map_err(|e| e.to_string())?;
    let cells_table = wtx.open_table(CELLS).map_err(|e| e.to_string())?;
    let mut snapshots = Vec::with_capacity(request.leases.len());
    for fence in &request.leases {
        let current = leases_table
            .get((graph, fence.lease_id.as_str()))
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("capacity lease '{}' was not found", fence.lease_id))
            .and_then(|value| decode_durable::<CapacityLease>(&crypto.unseal(value.value())?))?;
        if current.tenant_ref != request.tenant_ref || current.actor_digest != request.owner_digest {
            return Ok(CapacityMutationResult { schema_version: NativeControlSchemaVersion::V1, decision: CapacityDecision::StaleFence, leases: Vec::new(), message: Some("capacity lease owner mismatch".to_string()) });
        }
        if current.lease_epoch != fence.lease_epoch {
            return Ok(CapacityMutationResult { schema_version: NativeControlSchemaVersion::V1, decision: CapacityDecision::StaleEpoch, leases: Vec::new(), message: Some("capacity lease epoch is stale".to_string()) });
        }
        let cell = cells_table
            .get((graph, current.cell_id.as_str()))
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("capacity cell '{}' was not found", current.cell_id))
            .and_then(|value| decode_durable::<CapacityCell>(&crypto.unseal(value.value())?))?;
        validate_cell_bounds(&cell)?;
        if cell.epoch != current.lease_epoch {
            return Ok(CapacityMutationResult { schema_version: NativeControlSchemaVersion::V1, decision: CapacityDecision::StaleEpoch, leases: vec![current], message: Some("capacity cell epoch has advanced".to_string()) });
        }
        if current.fence_token != fence.fence_token {
            return Ok(CapacityMutationResult { schema_version: NativeControlSchemaVersion::V1, decision: CapacityDecision::StaleFence, leases: Vec::new(), message: Some("capacity lease fence is stale".to_string()) });
        }
        if matches!(current.state, LeaseState::Released | LeaseState::Expired | LeaseState::Reclaimed) {
            return Ok(CapacityMutationResult { schema_version: NativeControlSchemaVersion::V1, decision: CapacityDecision::Expired, leases: vec![current], message: Some("capacity lease is no longer active".to_string()) });
        }
        if request.now_ms >= current.expires_at_ms {
            return Ok(CapacityMutationResult { schema_version: NativeControlSchemaVersion::V1, decision: CapacityDecision::Expired, leases: vec![current], message: Some("capacity lease has expired".to_string()) });
        }
        snapshots.push(current);
    }
    drop(leases_table);
    drop(cells_table);
    let mut leases_table = wtx.open_table(LEASES).map_err(|e| e.to_string())?;
    let mut usage = wtx.open_table(USAGE).map_err(|e| e.to_string())?;
    let mut output = Vec::with_capacity(snapshots.len());
    for mut lease in snapshots {
        if renew {
            let ttl_ms = request.ttl_ms.unwrap_or(DEFAULT_TTL_MS);
            lease.expires_at_ms = request.now_ms.saturating_add(ttl_ms);
            lease.renewed_count = lease.renewed_count.saturating_add(1);
            lease.state = LeaseState::Renewed;
        } else {
            lease.state = LeaseState::Released;
            let mut row = usage
                .get((graph, lease.cell_id.as_str()))
                .map_err(|e| e.to_string())?
                .map(|value| decode_durable::<DurableUsage>(&crypto.unseal(value.value())?))
                .transpose()?
                .unwrap_or_default();
            row.leased_amount = row
                .leased_amount
                .checked_sub(lease.amount)
                .ok_or_else(|| "capacity usage underflow; ledger requires reconciliation".to_string())?;
            let sealed_usage_bytes = rmp_serde::to_vec_named(&row).map_err(|e| e.to_string())?;
            let sealed_usage = crypto.seal(&sealed_usage_bytes);
            usage
                .insert((graph, lease.cell_id.as_str()), sealed_usage.as_ref())
                .map_err(|e| e.to_string())?;
        }
        lease.validate().map_err(|error| format!("invalid capacity lease: {error:?}"))?;
        let sealed_bytes = rmp_serde::to_vec_named(&lease).map_err(|e| e.to_string())?;
        let sealed = crypto.seal(&sealed_bytes);
        leases_table
            .insert((graph, lease.lease_id.as_str()), sealed.as_ref())
            .map_err(|e| e.to_string())?;
        output.push(lease);
    }
    let result = CapacityMutationResult {
        schema_version: NativeControlSchemaVersion::V1,
        decision: if renew { CapacityDecision::Renewed } else { CapacityDecision::Released },
        leases: output,
        message: None,
    };
    if let Some(key) = key {
        write_replay(wtx, graph, &request.tenant_ref, key, if renew { "renew" } else { "release" }, &digest, &result, crypto)?;
    }
    Ok(result)
}

fn reclaim(
    wtx: &WriteTransaction,
    graph: &str,
    request: &CapacityReclaimRequest,
    crypto: DurableCrypto<'_>,
) -> Result<CapacityReclaimResult, String> {
    validate_reclaim_request(request)?;
    let reclaimed = reclaim_expired_inner(
        wtx,
        graph,
        request.now_ms,
        Some(request.tenant_ref.as_str()),
        request.cell_id.as_deref(),
        request.cursor.as_deref(),
        request.max_count as usize,
        crypto,
    )?;
    let next_cursor = reclaimed.last().cloned();
    Ok(CapacityReclaimResult {
        schema_version: NativeControlSchemaVersion::V1,
        decision: if reclaimed.is_empty() { CapacityDecision::Accepted } else { CapacityDecision::Reclaimed },
        reclaimed_lease_ids: reclaimed,
        next_cursor,
    })
}

fn reclaim_expired_inner(
    wtx: &WriteTransaction,
    graph: &str,
    now_ms: u64,
    tenant: Option<&str>,
    cell_id: Option<&str>,
    cursor: Option<&str>,
    max_count: usize,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<String>, String> {
    let mut leases = wtx.open_table(LEASES).map_err(|e| e.to_string())?;
    let mut usage = wtx.open_table(USAGE).map_err(|e| e.to_string())?;
    let mut expired = Vec::new();
    let mut candidates = Vec::new();
    let mut scanned = 0usize;
    for row in leases.range((graph, "")..).map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        let (row_graph, lease_id) = key.value();
        if row_graph != graph {
            break;
        }
        scanned += 1;
        if scanned > MAX_SCAN {
            return Err("capacity expiry scan exceeds native bound".to_string());
        }
        if cursor.is_some_and(|after| lease_id <= after) {
            continue;
        }
        let lease: CapacityLease = decode_durable(&crypto.unseal(value.value())?)?;
        if !matches!(lease.state, LeaseState::Active | LeaseState::Renewed)
            || lease.expires_at_ms > now_ms
            || tenant.is_some_and(|wanted| wanted != lease.tenant_ref)
            || cell_id.is_some_and(|wanted| wanted != lease.cell_id)
        {
            continue;
        }
        candidates.push((lease_id.to_string(), lease));
        if candidates.len() >= max_count {
            break;
        }
    }
    for (lease_id, mut lease) in candidates {
        lease.state = LeaseState::Reclaimed;
        let mut row = usage
            .get((graph, lease.cell_id.as_str()))
            .map_err(|e| e.to_string())?
            .map(|value| decode_durable::<DurableUsage>(&crypto.unseal(value.value())?))
            .transpose()?
            .unwrap_or_default();
        row.leased_amount = row
            .leased_amount
            .checked_sub(lease.amount)
            .ok_or_else(|| "capacity usage underflow; ledger requires reconciliation".to_string())?;
        let sealed_usage_bytes = rmp_serde::to_vec_named(&row).map_err(|e| e.to_string())?;
        let sealed_usage = crypto.seal(&sealed_usage_bytes);
        usage
            .insert((graph, lease.cell_id.as_str()), sealed_usage.as_ref())
            .map_err(|e| e.to_string())?;
        let sealed_lease_bytes = rmp_serde::to_vec_named(&lease).map_err(|e| e.to_string())?;
        let sealed_lease = crypto.seal(&sealed_lease_bytes);
        leases
            .insert((graph, lease_id.as_str()), sealed_lease.as_ref())
            .map_err(|e| e.to_string())?;
        expired.push(lease_id);
    }
    Ok(expired)
}

fn update_cell(
    wtx: &WriteTransaction,
    graph: &str,
    request: &CapacityCellUpdateRequest,
    crypto: DurableCrypto<'_>,
) -> Result<CapacityCellUpdateResult, String> {
    let mut next_cell = request.cell.clone();
    // `updated_at_ms` is authority-owned just like lease expiry.  The request
    // carries the leader/state-machine timestamp, while the caller's embedded
    // cell image is only the proposed dimension/policy shape.
    next_cell.updated_at_ms = request.now_ms;
    validate_cell_bounds(&next_cell)?;
    if next_cell.cell_id.len() > MAX_CAPACITY_ID_BYTES || next_cell.epoch == 0 {
        return Err("capacity cell id/epoch is outside native bounds".to_string());
    }
    let mut cells = wtx.open_table(CELLS).map_err(|e| e.to_string())?;
    let current = cells
        .get((graph, next_cell.cell_id.as_str()))
        .map_err(|e| e.to_string())?
        .map(|value| decode_durable::<CapacityCell>(&crypto.unseal(value.value())?))
        .transpose()?;
    if request.expected_epoch != current.as_ref().map(|cell| cell.epoch) {
        let cell = current.ok_or_else(|| "capacity cell was not found".to_string())?;
        return Ok(CapacityCellUpdateResult { schema_version: NativeControlSchemaVersion::V1, decision: CapacityDecision::StaleEpoch, cell, message: Some("capacity cell epoch CAS failed".to_string()) });
    }
    if current.as_ref().is_some_and(|cell| next_cell.epoch <= cell.epoch) {
        return Err("capacity cell epoch must advance monotonically".to_string());
    }
    let usage = wtx.open_table(USAGE).map_err(|e| e.to_string())?;
    let leased = usage
        .get((graph, next_cell.cell_id.as_str()))
        .map_err(|e| e.to_string())?
        .map(|value| decode_durable::<DurableUsage>(&crypto.unseal(value.value())?))
        .transpose()?
        .map(|row| row.leased_amount)
        .unwrap_or(0);
    if leased > next_cell.capacity {
        return Err("capacity cell update would place capacity below active usage".to_string());
    }
    let sealed_bytes = rmp_serde::to_vec_named(&next_cell).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&sealed_bytes);
    cells
        .insert((graph, next_cell.cell_id.as_str()), sealed.as_ref())
        .map_err(|e| e.to_string())?;
    Ok(CapacityCellUpdateResult { schema_version: NativeControlSchemaVersion::V1, decision: CapacityDecision::Accepted, cell: next_cell, message: None })
}

fn validate_id(value: &str, field: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > MAX_CAPACITY_ID_BYTES {
        return Err(format!("{field} must be non-empty and at most {MAX_CAPACITY_ID_BYTES} bytes"));
    }
    Ok(())
}

fn validate_cell_bounds(cell: &CapacityCell) -> Result<(), String> {
    cell.validate()
        .map_err(|error| format!("invalid capacity cell: {error:?}"))?;
    if cell.cell_id.len() > MAX_CAPACITY_ID_BYTES
        || cell.parent_id.as_deref().is_some_and(|id| id.len() > MAX_CAPACITY_ID_BYTES)
        || cell.policy_digest.len() > MAX_CAPACITY_ID_BYTES
        || cell.capacity > MAX_CAPACITY_AMOUNT
        || cell.reserved_floor > MAX_CAPACITY_AMOUNT
    {
        return Err("capacity cell fields exceed native bounds".to_string());
    }
    Ok(())
}

fn validate_acquire_request(request: &CapacityAcquireRequest) -> Result<(), String> {
    if request.schema_version != NativeControlSchemaVersion::V1 {
        return Err("capacity acquire schema_version must be 1".to_string());
    }
    validate_id(&request.tenant_ref, "tenant_ref")?;
    validate_id(&request.work_item_id, "work_item_id")?;
    validate_id(&request.owner_digest, "owner_digest")?;
    validate_id(&request.idempotency_key, "idempotency_key")?;
    if request.demands.is_empty() || request.demands.len() > MAX_CAPACITY_DEMANDS {
        return Err("capacity acquire demand count is outside native bounds".to_string());
    }
    if request.ttl_ms == 0 || request.ttl_ms > MAX_CAPACITY_TTL_MS {
        return Err("capacity acquire ttl_ms is outside native bounds".to_string());
    }
    if request.cost_budget_micros.is_some_and(|value| value > MAX_CAPACITY_BUDGET)
        || request.token_budget.is_some_and(|value| value > MAX_CAPACITY_BUDGET)
    {
        return Err("capacity budget is outside native bounds".to_string());
    }
    if request.lease_id.as_deref().is_some_and(|id| id.len() > MAX_CAPACITY_ID_BYTES) {
        return Err("capacity lease_id exceeds native bounds".to_string());
    }
    if request.lease_id.is_some() && request.demands.len() != 1 {
        return Err("capacity lease_id is only valid for a single demand".to_string());
    }
    for demand in &request.demands {
        validate_id(&demand.cell_id, "capacity demand cell_id")?;
        if demand.amount == 0 || demand.amount > MAX_CAPACITY_AMOUNT {
            return Err("capacity demand amount is outside native bounds".to_string());
        }
    }
    Ok(())
}

fn validate_mutation_request(request: &CapacityLeaseMutationRequest, renew: bool) -> Result<(), String> {
    if request.schema_version != NativeControlSchemaVersion::V1 {
        return Err("capacity lease mutation schema_version must be 1".to_string());
    }
    validate_id(&request.tenant_ref, "tenant_ref")?;
    validate_id(&request.owner_digest, "owner_digest")?;
    if request.leases.is_empty() || request.leases.len() > MAX_CAPACITY_MUTATION_BATCH {
        return Err("capacity lease mutation batch is outside native bounds".to_string());
    }
    if renew && request.ttl_ms.is_some_and(|ttl| ttl == 0 || ttl > MAX_CAPACITY_TTL_MS) {
        return Err("capacity renewal ttl_ms is outside native bounds".to_string());
    }
    if request.idempotency_key.as_deref().is_some_and(|key| key.trim().is_empty() || key.len() > MAX_CAPACITY_ID_BYTES) {
        return Err("capacity idempotency_key is outside native bounds".to_string());
    }
    let mut ids = BTreeSet::new();
    for lease in &request.leases {
        validate_id(&lease.lease_id, "capacity lease_id")?;
        if lease.lease_epoch == 0 || lease.fence_token == 0 || !ids.insert(lease.lease_id.clone()) {
            return Err("capacity lease fence is invalid or duplicated".to_string());
        }
    }
    Ok(())
}

fn validate_reclaim_request(request: &CapacityReclaimRequest) -> Result<(), String> {
    if request.schema_version != NativeControlSchemaVersion::V1 {
        return Err("capacity reclaim schema is invalid".to_string());
    }
    validate_id(&request.tenant_ref, "capacity reclaim tenant_ref")?;
    if request.max_count == 0 || request.max_count as usize > MAX_CAPACITY_RECLAIM_BATCH {
        return Err("capacity reclaim max_count is outside native bounds".to_string());
    }
    if request.cell_id.as_deref().is_some_and(|id| id.len() > MAX_CAPACITY_ID_BYTES) {
        return Err("capacity reclaim cell_id exceeds native bounds".to_string());
    }
    if request.cursor.as_deref().is_some_and(|id| id.len() > MAX_CAPACITY_ID_BYTES) {
        return Err("capacity reclaim cursor exceeds native bounds".to_string());
    }
    Ok(())
}

fn validate_status_request(request: &CapacityStatusRequest) -> Result<(), String> {
    if request.schema_version != NativeControlSchemaVersion::V1 {
        return Err("capacity status schema is invalid".to_string());
    }
    validate_id(&request.tenant_ref, "capacity status tenant_ref")?;
    if request.max_count == 0 || request.max_count as usize > MAX_CAPACITY_STATUS_ROWS {
        return Err("capacity status max_count is outside native bounds".to_string());
    }
    if request.cell_id.as_deref().is_some_and(|id| id.len() > MAX_CAPACITY_ID_BYTES)
        || request.lease_id.as_deref().is_some_and(|id| id.len() > MAX_CAPACITY_ID_BYTES)
        || request.cursor.as_deref().is_some_and(|id| id.len() > MAX_CAPACITY_ID_BYTES)
    {
        return Err("capacity status selector exceeds native bounds".to_string());
    }
    Ok(())
}

fn request_digest<T: Serialize>(request: &T) -> Result<String, String> {
    let encoded = rmp_serde::to_vec_named(request).map_err(|e| e.to_string())?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

fn lease_id(
    request: &CapacityAcquireRequest,
    demand: &CapacityDemand,
    index: usize,
    digest: &str,
) -> Result<String, String> {
    if request.demands.len() == 1 {
        if let Some(id) = request.lease_id.as_deref() {
            validate_id(id, "lease_id")?;
            return Ok(id.to_string());
        }
    }
    let mut hasher = Sha256::new();
    hasher.update(digest.as_bytes());
    hasher.update([0]);
    hasher.update(demand.cell_id.as_bytes());
    hasher.update([0]);
    hasher.update((index as u64).to_be_bytes());
    Ok(format!("capacity:{}", hex::encode(hasher.finalize())))
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(value).map_err(|e| e.to_string())
}

fn read_replay(
    wtx: &WriteTransaction,
    graph: &str,
    tenant: &str,
    key: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableReplay>, String> {
    let table = wtx.open_table(IDEMPOTENCY).map_err(|e| e.to_string())?;
    let found = table.get((graph, tenant, key)).map_err(|e| e.to_string())?;
    found
        .map(|value| decode_durable(&crypto.unseal(value.value())?))
        .transpose()
}

fn write_replay<T: Serialize>(
    wtx: &WriteTransaction,
    graph: &str,
    tenant: &str,
    key: &str,
    operation: &str,
    digest: &str,
    result: &T,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let result = encode(result)?;
    let replay = DurableReplay {
        digest: digest.to_string(),
        operation: operation.to_string(),
        result,
    };
    let sealed_bytes = rmp_serde::to_vec_named(&replay).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&sealed_bytes);
    let mut table = wtx.open_table(IDEMPOTENCY).map_err(|e| e.to_string())?;
    table
        .insert((graph, tenant, key), sealed.as_ref())
        .map_err(|e| e.to_string())?;
    Ok(())
}
