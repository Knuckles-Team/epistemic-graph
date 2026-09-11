//! Authoritative capacity-cell/lease ledger on the graph shard (GOC-21-W04/W05).
//!
//! The pure [`eg_types::capacity_lease::CapacityLedger`] is useful for policy
//! tests, but it is deliberately not an authority.  This module owns the
//! graph-scoped durable cells, aggregate usage, lease fences, and tenant/key
//! replay rows.  All four of its tables are scope-prefixed shard tables, so
//! every row access here goes through a capability bound to ONE graph: the
//! write side through [`ShardWrite::graph`], the read side through
//! [`Shard::read`].  Every write below runs in one admitted group -- one
//! physical transaction, one fsync -- and all validation happens before the
//! first insert/update so a denial cannot leave a partially charged dimension.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use rand::RngCore;
use redb::TableDefinition;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use eg_storage::{GraphShardOwner, PhysicalWriteCapability, ScopedOwnerTable, ScopedOwnerTableMut};
use eg_types::capacity_lease::{CapacityCell, CapacityLease, LeaseState};
use eg_types::native_control::{
    CapacityAcquireRequest, CapacityAcquireResult, CapacityAvailability, CapacityCellUpdateRequest,
    CapacityCellUpdateResult, CapacityDecision, CapacityDemand, CapacityLeaseMutationRequest,
    CapacityMutationResult, CapacityReclaimRequest, CapacityReclaimResult, CapacityStatusRequest,
    CapacityStatusResult, NativeControlSchemaVersion, MAX_CAPACITY_AMOUNT, MAX_CAPACITY_BUDGET,
    MAX_CAPACITY_DEMANDS, MAX_CAPACITY_ID_BYTES, MAX_CAPACITY_MUTATION_BATCH,
    MAX_CAPACITY_RECLAIM_BATCH, MAX_CAPACITY_STATUS_ROWS, MAX_CAPACITY_TTL_MS,
};

use super::shard::{Shard, ShardWrite};
use super::{decode_durable, DurableCrypto};
use crate::protocol::Method;

/// One capacity table opened for writing on one graph's scope.
type CapacityRows<'a> = ScopedOwnerTableMut<'a, (&'static str, &'static str), &'static [u8]>;

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

/// Drop every capacity row of one graph inside an admitted group.
///
/// The delete-time half of a graph's lifecycle (ClearGraph, DeleteGraph, a
/// checkpoint replacement). It is the same four-table sweep
/// [`retire_graph_rows`] performs, through the same kernel primitive; the two
/// differ only in the capability the caller holds -- a group member's
/// owner-row write here, the scope's own physical write capability there.
pub(crate) fn clear_graph_rows(write: &ShardWrite<'_>, graph: &str) -> Result<(), String> {
    let rows = write.graph(graph)?;
    rows.open_scoped_table(CELLS)?.purge_scope_rows()?;
    rows.open_scoped_table(LEASES)?.purge_scope_rows()?;
    rows.open_scoped_table(USAGE)?.purge_scope_rows()?;
    rows.open_scoped_table(IDEMPOTENCY)?.purge_scope_rows()
}

/// Retire this module's rows for the capability's own graph scope.
///
/// The payload half of a scope retirement (`OwnerPayloadRetirement`). Every
/// table this module declares is listed -- missing one would hand the retired
/// generation's rows to the next binding of the same graph name.
///
/// The sweep itself is `ScopedOwnerTableMut::purge_scope_rows`, whose scope
/// comes from the capability that opened the table rather than from an
/// argument; see its doc for why the ledger sweep
/// (`PhysicalWriteCapability::purge_scoped_rows`) cannot serve an owner table.
pub(crate) fn retire_graph_rows(
    write: &PhysicalWriteCapability<'_, GraphShardOwner>,
) -> Result<(), String> {
    write.scoped_owner_table_mut(CELLS)?.purge_scope_rows()?;
    write.scoped_owner_table_mut(LEASES)?.purge_scope_rows()?;
    write.scoped_owner_table_mut(USAGE)?.purge_scope_rows()?;
    write
        .scoped_owner_table_mut(IDEMPOTENCY)?
        .purge_scope_rows()
}

/// One admitted attempt's operation id.
///
/// Unique per ATTEMPT, deliberately, exactly as `Shard`'s own `drain_batch`
/// requires: a retried capacity method is a fresh admission, and the replay
/// identity these operations actually have is this module's own
/// `capacity_idempotency` row, not the batch id. The process nonce keeps two
/// runs of the same counter value apart across a restart, so a replayed
/// counter can never collide with a durable batch id from an earlier process.
fn attempt_id() -> String {
    static PROCESS: OnceLock<String> = OnceLock::new();
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let process = PROCESS.get_or_init(|| {
        let mut nonce = [0_u8; 16];
        rand::rngs::OsRng.fill_bytes(&mut nonce);
        hex::encode(nonce)
    });
    format!(
        "capacity/{process}:{}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// The authority-owned instant one capacity method is applied at.
///
/// Every capacity request carries the leader/state-machine timestamp already --
/// it is what dates a lease's expiry and a cell's `updated_at_ms` -- so the
/// commit is dated from the same clock the rows are, rather than from a second
/// one read at the transaction boundary.
fn method_now_ms(method: &Method) -> Result<u64, String> {
    match method {
        Method::AcquireCapacity { request } => Ok(request.now_ms),
        Method::RenewCapacity { request } | Method::ReleaseCapacity { request } => {
            Ok(request.now_ms)
        }
        Method::ReclaimExpiredCapacity { request } => Ok(request.now_ms),
        Method::UpdateCapacityCell { request } => Ok(request.now_ms),
        _ => Err("capacity ledger received an unsupported method".to_string()),
    }
}

/// Apply one capacity method to its graph in ONE admitted group.
///
/// The five-step shard write: bind the graph, admit the group at the version
/// resolved inside its transaction, open the owner-row writes, write the rows,
/// finish every member, commit. The class is `maintenance` because none of
/// these writes carries a caller operation identity into the ledger -- the
/// synthesized batch has an empty capability set and the store's own serving
/// principal, and the caller's replay identity lives in this module's
/// `capacity_idempotency` row instead, which is exactly what puts the write
/// outside operation-replay conflict semantics (RF-RULING-005).
pub(crate) fn commit(
    shard: &Shard,
    graph: &str,
    method: &Method,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut super::AuditTailCache,
) -> Result<Vec<u8>, String> {
    let committed_at_ms = method_now_ms(method)?;
    let op_id = attempt_id();
    let members = shard.graph_members(&[graph])?;
    let (group, batches) = shard.admit_maintenance(&members, &op_id)?;
    let write = ShardWrite::open(shard, &group, &members, &batches)?;
    let bytes = apply(&write, graph, method, crypto)?;
    // Scoped so the audit table handle is dropped before `write.finish()`
    // consumes the owner-row admission it borrows.
    #[cfg(feature = "security")]
    let staged_audit_tail = {
        let mut staged = audit_tail.clone();
        if !result_is_replay(method, &bytes)? {
            let mut audit = write.graph(graph)?.open_scoped_table(super::AUDIT)?;
            super::append_audit_entry(&mut audit, &mut staged, graph, method)?;
        }
        staged
    };
    write.finish()?;
    shard.commit_drain(group, &batches, committed_at_ms)?;
    #[cfg(feature = "security")]
    {
        *audit_tail = staged_audit_tail;
    }
    Ok(bytes)
}

#[cfg(feature = "security")]
fn result_is_replay(method: &Method, bytes: &[u8]) -> Result<bool, String> {
    match method {
        Method::AcquireCapacity { .. } => Ok(decode_durable::<CapacityAcquireResult>(bytes)?
            .decision
            == CapacityDecision::Replayed),
        Method::RenewCapacity { .. } | Method::ReleaseCapacity { .. } => {
            Ok(decode_durable::<CapacityMutationResult>(bytes)?.decision
                == CapacityDecision::Replayed)
        }
        _ => Ok(false),
    }
}

/// Page one graph's cells and leases from a kernel-issued scoped read.
pub(crate) fn read(
    shard: &Shard,
    graph: &str,
    request: &CapacityStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<CapacityStatusResult, String> {
    validate_status_request(request)?;
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let cells = read.scoped_owner_table(CELLS)?;
    let leases = read.scoped_owner_table(LEASES)?;
    let cell_rows = scan_status_cells(&cells, request, crypto)?;
    let lease_rows = scan_status_leases(&leases, request, crypto)?;
    let next_cursor = lease_rows.last().map(|lease| lease.lease_id.clone());
    Ok(CapacityStatusResult {
        schema_version: NativeControlSchemaVersion::V1,
        cells: cell_rows,
        leases: lease_rows,
        next_cursor,
    })
}

fn scan_status_cells(
    cells: &ScopedOwnerTable<(&'static str, &'static str), &'static [u8]>,
    request: &CapacityStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<CapacityCell>, String> {
    let mut cell_rows = Vec::new();
    let mut scanned_cells = 0usize;
    // `scope_rows` is already bounded to this read's own graph, so the scan
    // needs no starting key and no "did we leave the graph" break.
    for row in cells.scope_rows()? {
        let (key, value) = row?;
        let (_, cell_id) = key.value();
        scanned_cells += 1;
        if scanned_cells > MAX_SCAN {
            return Err("capacity status cell scan exceeds native bound".to_string());
        }
        let cell: CapacityCell = decode_durable(&crypto.unseal(value.value())?)?;
        validate_cell_bounds(&cell)?;
        if request
            .cell_id
            .as_deref()
            .is_none_or(|wanted| wanted == cell_id)
        {
            cell_rows.push(cell);
        }
        if cell_rows.len() > MAX_CAPACITY_STATUS_ROWS {
            return Err("capacity status cell page exceeds native bound".to_string());
        }
    }
    Ok(cell_rows)
}

fn scan_status_leases(
    leases: &ScopedOwnerTable<(&'static str, &'static str), &'static [u8]>,
    request: &CapacityStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<CapacityLease>, String> {
    let mut lease_rows = Vec::new();
    let mut scanned_leases = 0usize;
    let cursor = request.cursor.as_deref().unwrap_or("");
    for row in leases.scope_rows()? {
        let (key, value) = row?;
        let (_, lease_id) = key.value();
        scanned_leases += 1;
        if scanned_leases > MAX_SCAN {
            return Err("capacity status lease scan exceeds native bound".to_string());
        }
        if !cursor.is_empty() && lease_id <= cursor {
            continue;
        }
        let lease: CapacityLease = decode_durable(&crypto.unseal(value.value())?)?;
        if !lease_matches_status_request(&lease, request) {
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
    Ok(lease_rows)
}

/// Whether a scanned lease row belongs to the requester's tenant and matches
/// the optional cell/lease id filters.
fn lease_matches_status_request(lease: &CapacityLease, request: &CapacityStatusRequest) -> bool {
    lease.tenant_ref == request.tenant_ref
        && request
            .cell_id
            .as_deref()
            .is_none_or(|wanted| wanted == lease.cell_id)
        && request
            .lease_id
            .as_deref()
            .is_none_or(|wanted| wanted == lease.lease_id)
}

fn apply(
    write: &ShardWrite<'_>,
    graph: &str,
    method: &Method,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<u8>, String> {
    match method {
        Method::AcquireCapacity { request } => {
            let result = acquire(write, graph, request, crypto)?;
            encode(&result)
        }
        Method::RenewCapacity { request } => {
            let result = mutate_leases(write, graph, request, true, crypto)?;
            encode(&result)
        }
        Method::ReleaseCapacity { request } => {
            let result = mutate_leases(write, graph, request, false, crypto)?;
            encode(&result)
        }
        Method::ReclaimExpiredCapacity { request } => {
            let result = reclaim(write, graph, request, crypto)?;
            encode(&result)
        }
        Method::UpdateCapacityCell { request } => {
            let result = update_cell(write, graph, request, crypto)?;
            encode(&result)
        }
        _ => Err("capacity ledger received an unsupported method".to_string()),
    }
}

fn acquire(
    write: &ShardWrite<'_>,
    graph: &str,
    request: &CapacityAcquireRequest,
    crypto: DurableCrypto<'_>,
) -> Result<CapacityAcquireResult, String> {
    validate_acquire_request(request)?;
    let mut digest_request = request.clone();
    digest_request.now_ms = 0;
    let digest = request_digest(&digest_request)?;
    if let Some(replay) = read_replay(
        write,
        graph,
        &request.tenant_ref,
        &request.idempotency_key,
        crypto,
    )? {
        if replay.digest != digest || replay.operation != "acquire" {
            return Err(
                "IDEMPOTENCY_CONFLICT: capacity acquire key has a different request".to_string(),
            );
        }
        let mut result: CapacityAcquireResult = decode_durable(&crypto.unseal(&replay.result)?)?;
        result.decision = CapacityDecision::Replayed;
        return Ok(result);
    }

    // Expired capacity is reclaimed before the CAS check, in the same admitted
    // group.  The scan is bounded; a pathological backlog fails closed and asks
    // the controller to drain it explicitly.  It runs BEFORE the three table
    // handles below are opened, because `redb` refuses a second open of a table
    // whose first handle is still alive and it opens LEASES and USAGE itself.
    reclaim_expired_inner(
        write,
        graph,
        request.now_ms,
        ReclaimScope {
            tenant: None,
            cell_id: None,
            cursor: None,
            max_count: MAX_CAPACITY_RECLAIM_BATCH,
        },
        crypto,
    )?;

    let rows = write.graph(graph)?;
    let cells = rows.open_scoped_table(CELLS)?;
    let mut usage = rows.open_scoped_table(USAGE)?;
    let mut leases = rows.open_scoped_table(LEASES)?;
    let mut demands = request.demands.clone();
    demands.sort_by(|left, right| {
        (&left.cell_id, &left.resource_class, left.amount).cmp(&(
            &right.cell_id,
            &right.resource_class,
            right.amount,
        ))
    });
    let mut seen = BTreeSet::new();
    let mut availability = Vec::with_capacity(demands.len());
    let mut cell_rows = Vec::with_capacity(demands.len());
    let mut usage_rows = Vec::with_capacity(demands.len());
    for demand in &demands {
        if !seen.insert((demand.cell_id.clone(), demand.resource_class)) {
            return Err(
                "capacity acquire demands must name each cell/resource dimension once".to_string(),
            );
        }
        let (cell, row, priced) =
            price_demand(&cells, &usage, graph, request.priority, demand, crypto)?;
        let available = priced.available;
        availability.push(priced);
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
    for (index, ((demand, cell), row)) in demands
        .iter()
        .zip(cell_rows.iter())
        .zip(usage_rows)
        .enumerate()
    {
        let lease = issue_acquired_lease(
            &mut leases,
            &mut usage,
            graph,
            request,
            demand,
            cell,
            row,
            index,
            &digest,
            crypto,
        )?;
        out.push(lease);
    }
    let result = CapacityAcquireResult {
        schema_version: NativeControlSchemaVersion::V1,
        decision: CapacityDecision::Accepted,
        leases: out,
        available: availability,
        message: None,
    };
    write_replay(
        write,
        graph,
        ReplayKey {
            tenant: &request.tenant_ref,
            key: &request.idempotency_key,
            operation: "acquire",
            digest: &digest,
        },
        &result,
        crypto,
    )?;
    Ok(result)
}

/// Look up one demand's cell + current usage row and price its availability
/// against the requested priority. Does not decide admission — the caller
/// compares `CapacityAvailability::available` against the demand's requested
/// amount, since an exhausted demand still needs its priced entry recorded.
fn price_demand(
    cells: &CapacityRows<'_>,
    usage: &CapacityRows<'_>,
    graph: &str,
    priority: eg_types::capacity_lease::LeasePriority,
    demand: &CapacityDemand,
    crypto: DurableCrypto<'_>,
) -> Result<(CapacityCell, DurableUsage, CapacityAvailability), String> {
    let cell = cells
        .get((graph, demand.cell_id.as_str()))?
        .ok_or_else(|| format!("capacity cell '{}' was not found", demand.cell_id))
        .and_then(|value| decode_durable::<CapacityCell>(&crypto.unseal(value.value())?))?;
    validate_cell_bounds(&cell)?;
    if cell.resource_class != demand.resource_class {
        return Err(format!(
            "capacity cell '{}' resource dimension mismatch",
            demand.cell_id
        ));
    }
    let row = usage
        .get((graph, demand.cell_id.as_str()))?
        .map(|value| decode_durable::<DurableUsage>(&crypto.unseal(value.value())?))
        .transpose()?
        .unwrap_or_default();
    let available = cell.available_for(priority, row.leased_amount);
    let availability = CapacityAvailability {
        cell_id: demand.cell_id.clone(),
        resource_class: demand.resource_class,
        available,
        requested: demand.amount,
    };
    Ok((cell, row, availability))
}

/// Assign the next fence token, mint the lease, and durably record both the
/// lease row and the cell's updated usage row for one already-priced demand.
#[allow(clippy::too_many_arguments)]
fn issue_acquired_lease(
    leases: &mut CapacityRows<'_>,
    usage: &mut CapacityRows<'_>,
    graph: &str,
    request: &CapacityAcquireRequest,
    demand: &CapacityDemand,
    cell: &CapacityCell,
    mut row: DurableUsage,
    index: usize,
    digest: &str,
    crypto: DurableCrypto<'_>,
) -> Result<CapacityLease, String> {
    row.next_fence = row
        .next_fence
        .checked_add(1)
        .ok_or_else(|| "capacity fence exhausted".to_string())?;
    let lease_id = lease_id(request, demand, index, digest)?;
    if leases.get((graph, lease_id.as_str()))?.is_some() {
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
    lease
        .validate()
        .map_err(|error| format!("invalid capacity lease: {error:?}"))?;
    row.leased_amount = row
        .leased_amount
        .checked_add(demand.amount)
        .ok_or_else(|| "capacity usage overflow".to_string())?;
    let sealed_lease_bytes = rmp_serde::to_vec_named(&lease).map_err(|e| e.to_string())?;
    let sealed_lease = crypto.seal(&sealed_lease_bytes);
    leases.insert((graph, lease.lease_id.as_str()), sealed_lease.as_ref())?;
    let sealed_usage_bytes = rmp_serde::to_vec_named(&row).map_err(|e| e.to_string())?;
    let sealed_usage = crypto.seal(&sealed_usage_bytes);
    usage.insert((graph, demand.cell_id.as_str()), sealed_usage.as_ref())?;
    Ok(lease)
}

/// If the mutation's idempotency key already has a recorded replay row,
/// validate the stored digest/operation match and return the replayed
/// result; otherwise `None` so the caller proceeds with a fresh mutation.
fn check_mutation_replay(
    write: &ShardWrite<'_>,
    graph: &str,
    request: &CapacityLeaseMutationRequest,
    renew: bool,
    digest: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<CapacityMutationResult>, String> {
    let Some(key) = request.idempotency_key.as_deref() else {
        return Ok(None);
    };
    let Some(replay) = read_replay(write, graph, &request.tenant_ref, key, crypto)? else {
        return Ok(None);
    };
    if replay.digest != digest || replay.operation != if renew { "renew" } else { "release" } {
        return Err(
            "IDEMPOTENCY_CONFLICT: capacity mutation key has a different request".to_string(),
        );
    }
    let mut result: CapacityMutationResult = decode_durable(&crypto.unseal(&replay.result)?)?;
    result.decision = CapacityDecision::Replayed;
    Ok(Some(result))
}

fn mutate_leases(
    write: &ShardWrite<'_>,
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
    if let Some(result) = check_mutation_replay(write, graph, request, renew, &digest, crypto)? {
        return Ok(result);
    }
    let rows = write.graph(graph)?;
    let leases_table = rows.open_scoped_table(LEASES)?;
    let cells_table = rows.open_scoped_table(CELLS)?;
    let mut snapshots = Vec::with_capacity(request.leases.len());
    for fence in &request.leases {
        match check_lease_fence(&leases_table, &cells_table, graph, request, fence, crypto)? {
            FenceCheck::Snapshot(lease) => snapshots.push(*lease),
            FenceCheck::Rejected(result) => return Ok(*result),
        }
    }
    drop(leases_table);
    drop(cells_table);
    let mut leases_table = rows.open_scoped_table(LEASES)?;
    let mut usage = rows.open_scoped_table(USAGE)?;
    let mut output = Vec::with_capacity(snapshots.len());
    for lease in snapshots {
        let lease = apply_lease_mutation(
            &mut leases_table,
            &mut usage,
            graph,
            request,
            renew,
            lease,
            crypto,
        )?;
        output.push(lease);
    }
    let result = CapacityMutationResult {
        schema_version: NativeControlSchemaVersion::V1,
        decision: if renew {
            CapacityDecision::Renewed
        } else {
            CapacityDecision::Released
        },
        leases: output,
        message: None,
    };
    if let Some(key) = key {
        write_replay(
            write,
            graph,
            ReplayKey {
                tenant: &request.tenant_ref,
                key,
                operation: if renew { "renew" } else { "release" },
                digest: &digest,
            },
            &result,
            crypto,
        )?;
    }
    Ok(result)
}

/// Outcome of checking one lease-mutation request's fence against the
/// currently durable lease/cell rows: either the lease snapshot to carry into
/// the apply pass, or the terminal (non-error) `CapacityMutationResult` the
/// caller should return as-is. `Box`ed because `CapacityMutationResult` is
/// large relative to the `CapacityLease` alternative (clippy::large_enum_variant).
enum FenceCheck {
    Snapshot(Box<CapacityLease>),
    Rejected(Box<CapacityMutationResult>),
}

/// Validate one `CapacityLeaseFence` against its durable lease and cell rows:
/// ownership, lease epoch, cell epoch (CAS), fence token, and active/expiry
/// state, in that order — mirrors the original inline guard-clause sequence.
fn check_lease_fence(
    leases_table: &CapacityRows<'_>,
    cells_table: &CapacityRows<'_>,
    graph: &str,
    request: &CapacityLeaseMutationRequest,
    fence: &eg_types::native_control::CapacityLeaseFence,
    crypto: DurableCrypto<'_>,
) -> Result<FenceCheck, String> {
    let current = leases_table
        .get((graph, fence.lease_id.as_str()))?
        .ok_or_else(|| format!("capacity lease '{}' was not found", fence.lease_id))
        .and_then(|value| decode_durable::<CapacityLease>(&crypto.unseal(value.value())?))?;
    if current.tenant_ref != request.tenant_ref || current.actor_digest != request.owner_digest {
        return Ok(FenceCheck::Rejected(Box::new(CapacityMutationResult {
            schema_version: NativeControlSchemaVersion::V1,
            decision: CapacityDecision::StaleFence,
            leases: Vec::new(),
            message: Some("capacity lease owner mismatch".to_string()),
        })));
    }
    if current.lease_epoch != fence.lease_epoch {
        return Ok(FenceCheck::Rejected(Box::new(CapacityMutationResult {
            schema_version: NativeControlSchemaVersion::V1,
            decision: CapacityDecision::StaleEpoch,
            leases: Vec::new(),
            message: Some("capacity lease epoch is stale".to_string()),
        })));
    }
    let cell = cells_table
        .get((graph, current.cell_id.as_str()))?
        .ok_or_else(|| format!("capacity cell '{}' was not found", current.cell_id))
        .and_then(|value| decode_durable::<CapacityCell>(&crypto.unseal(value.value())?))?;
    validate_cell_bounds(&cell)?;
    if cell.epoch != current.lease_epoch {
        return Ok(FenceCheck::Rejected(Box::new(CapacityMutationResult {
            schema_version: NativeControlSchemaVersion::V1,
            decision: CapacityDecision::StaleEpoch,
            leases: vec![current],
            message: Some("capacity cell epoch has advanced".to_string()),
        })));
    }
    if current.fence_token != fence.fence_token {
        return Ok(FenceCheck::Rejected(Box::new(CapacityMutationResult {
            schema_version: NativeControlSchemaVersion::V1,
            decision: CapacityDecision::StaleFence,
            leases: Vec::new(),
            message: Some("capacity lease fence is stale".to_string()),
        })));
    }
    if matches!(
        current.state,
        LeaseState::Released | LeaseState::Expired | LeaseState::Reclaimed
    ) {
        return Ok(FenceCheck::Rejected(Box::new(CapacityMutationResult {
            schema_version: NativeControlSchemaVersion::V1,
            decision: CapacityDecision::Expired,
            leases: vec![current],
            message: Some("capacity lease is no longer active".to_string()),
        })));
    }
    if request.now_ms >= current.expires_at_ms {
        return Ok(FenceCheck::Rejected(Box::new(CapacityMutationResult {
            schema_version: NativeControlSchemaVersion::V1,
            decision: CapacityDecision::Expired,
            leases: vec![current],
            message: Some("capacity lease has expired".to_string()),
        })));
    }
    Ok(FenceCheck::Snapshot(Box::new(current)))
}

/// Apply a renewal or release to one already-fence-checked lease snapshot and
/// durably record the lease (and, on release, the cell's usage row).
fn apply_lease_mutation(
    leases_table: &mut CapacityRows<'_>,
    usage: &mut CapacityRows<'_>,
    graph: &str,
    request: &CapacityLeaseMutationRequest,
    renew: bool,
    mut lease: CapacityLease,
    crypto: DurableCrypto<'_>,
) -> Result<CapacityLease, String> {
    if renew {
        let ttl_ms = request.ttl_ms.unwrap_or(DEFAULT_TTL_MS);
        lease.expires_at_ms = request.now_ms.saturating_add(ttl_ms);
        lease.renewed_count = lease.renewed_count.saturating_add(1);
        lease.state = LeaseState::Renewed;
    } else {
        lease.state = LeaseState::Released;
        let mut row = usage
            .get((graph, lease.cell_id.as_str()))?
            .map(|value| decode_durable::<DurableUsage>(&crypto.unseal(value.value())?))
            .transpose()?
            .unwrap_or_default();
        row.leased_amount = row.leased_amount.checked_sub(lease.amount).ok_or_else(|| {
            "capacity usage underflow; ledger requires reconciliation".to_string()
        })?;
        let sealed_usage_bytes = rmp_serde::to_vec_named(&row).map_err(|e| e.to_string())?;
        let sealed_usage = crypto.seal(&sealed_usage_bytes);
        usage.insert((graph, lease.cell_id.as_str()), sealed_usage.as_ref())?;
    }
    lease
        .validate()
        .map_err(|error| format!("invalid capacity lease: {error:?}"))?;
    let sealed_bytes = rmp_serde::to_vec_named(&lease).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&sealed_bytes);
    leases_table.insert((graph, lease.lease_id.as_str()), sealed.as_ref())?;
    Ok(lease)
}

fn reclaim(
    write: &ShardWrite<'_>,
    graph: &str,
    request: &CapacityReclaimRequest,
    crypto: DurableCrypto<'_>,
) -> Result<CapacityReclaimResult, String> {
    validate_reclaim_request(request)?;
    let reclaimed = reclaim_expired_inner(
        write,
        graph,
        request.now_ms,
        ReclaimScope {
            tenant: Some(request.tenant_ref.as_str()),
            cell_id: request.cell_id.as_deref(),
            cursor: request.cursor.as_deref(),
            max_count: request.max_count as usize,
        },
        crypto,
    )?;
    let next_cursor = reclaimed.last().cloned();
    Ok(CapacityReclaimResult {
        schema_version: NativeControlSchemaVersion::V1,
        decision: if reclaimed.is_empty() {
            CapacityDecision::Accepted
        } else {
            CapacityDecision::Reclaimed
        },
        reclaimed_lease_ids: reclaimed,
        next_cursor,
    })
}

/// Which expired leases one reclaim pass considers, and how far it may page.
/// Grouped so the reclaim helper keeps a readable arity
/// (clippy::too_many_arguments) and so the three optional string filters cannot
/// be transposed at a call site.
struct ReclaimScope<'a> {
    tenant: Option<&'a str>,
    cell_id: Option<&'a str>,
    cursor: Option<&'a str>,
    max_count: usize,
}

fn reclaim_expired_inner(
    write: &ShardWrite<'_>,
    graph: &str,
    now_ms: u64,
    scope: ReclaimScope<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<String>, String> {
    let ReclaimScope {
        tenant,
        cell_id,
        cursor,
        max_count,
    } = scope;
    let rows = write.graph(graph)?;
    let mut leases = rows.open_scoped_table(LEASES)?;
    let mut usage = rows.open_scoped_table(USAGE)?;
    let candidates = scan_expired_candidates(
        &leases,
        now_ms,
        ExpiryScanFilter {
            tenant,
            cell_id,
            cursor,
            max_count,
        },
        crypto,
    )?;
    let mut expired = Vec::with_capacity(candidates.len());
    for (lease_id, lease) in candidates {
        expired.push(reclaim_one_lease(
            &mut leases,
            &mut usage,
            graph,
            lease_id,
            lease,
            crypto,
        )?);
    }
    Ok(expired)
}

/// The tenant/cell/cursor/count filter one expiry scan pass applies while
/// walking the LEASES table. Split out of `reclaim_expired_inner`'s
/// `ReclaimScope` so the scan helper's own arity stays readable
/// (clippy::too_many_arguments).
struct ExpiryScanFilter<'a> {
    tenant: Option<&'a str>,
    cell_id: Option<&'a str>,
    cursor: Option<&'a str>,
    max_count: usize,
}

/// Bounded scan of this graph's LEASES rows for active/renewed leases past
/// `now_ms`, honoring the tenant/cell/cursor filter and paging at
/// `max_count`. Does not mutate anything — callers reclaim the returned rows.
fn scan_expired_candidates(
    leases: &CapacityRows<'_>,
    now_ms: u64,
    filter: ExpiryScanFilter<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<(String, CapacityLease)>, String> {
    let ExpiryScanFilter {
        tenant,
        cell_id,
        cursor,
        max_count,
    } = filter;
    let mut candidates = Vec::new();
    let mut scanned = 0usize;
    for row in leases.scope_rows()? {
        let (key, value) = row?;
        let (_, lease_id) = key.value();
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
    Ok(candidates)
}

/// Mark one already-selected candidate lease Reclaimed and durably record it
/// plus its cell's decremented usage row.
fn reclaim_one_lease(
    leases: &mut CapacityRows<'_>,
    usage: &mut CapacityRows<'_>,
    graph: &str,
    lease_id: String,
    mut lease: CapacityLease,
    crypto: DurableCrypto<'_>,
) -> Result<String, String> {
    lease.state = LeaseState::Reclaimed;
    let mut row = usage
        .get((graph, lease.cell_id.as_str()))?
        .map(|value| decode_durable::<DurableUsage>(&crypto.unseal(value.value())?))
        .transpose()?
        .unwrap_or_default();
    row.leased_amount = row
        .leased_amount
        .checked_sub(lease.amount)
        .ok_or_else(|| "capacity usage underflow; ledger requires reconciliation".to_string())?;
    let sealed_usage_bytes = rmp_serde::to_vec_named(&row).map_err(|e| e.to_string())?;
    let sealed_usage = crypto.seal(&sealed_usage_bytes);
    usage.insert((graph, lease.cell_id.as_str()), sealed_usage.as_ref())?;
    let sealed_lease_bytes = rmp_serde::to_vec_named(&lease).map_err(|e| e.to_string())?;
    let sealed_lease = crypto.seal(&sealed_lease_bytes);
    leases.insert((graph, lease_id.as_str()), sealed_lease.as_ref())?;
    Ok(lease_id)
}

fn update_cell(
    write: &ShardWrite<'_>,
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
    let rows = write.graph(graph)?;
    let mut cells = rows.open_scoped_table(CELLS)?;
    let current = cells
        .get((graph, next_cell.cell_id.as_str()))?
        .map(|value| decode_durable::<CapacityCell>(&crypto.unseal(value.value())?))
        .transpose()?;
    if request.expected_epoch != current.as_ref().map(|cell| cell.epoch) {
        let cell = current.ok_or_else(|| "capacity cell was not found".to_string())?;
        return Ok(CapacityCellUpdateResult {
            schema_version: NativeControlSchemaVersion::V1,
            decision: CapacityDecision::StaleEpoch,
            cell,
            message: Some("capacity cell epoch CAS failed".to_string()),
        });
    }
    if current
        .as_ref()
        .is_some_and(|cell| next_cell.epoch <= cell.epoch)
    {
        return Err("capacity cell epoch must advance monotonically".to_string());
    }
    let usage = rows.open_scoped_table(USAGE)?;
    let leased = usage
        .get((graph, next_cell.cell_id.as_str()))?
        .map(|value| decode_durable::<DurableUsage>(&crypto.unseal(value.value())?))
        .transpose()?
        .map(|row| row.leased_amount)
        .unwrap_or(0);
    if leased > next_cell.capacity {
        return Err("capacity cell update would place capacity below active usage".to_string());
    }
    let sealed_bytes = rmp_serde::to_vec_named(&next_cell).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&sealed_bytes);
    cells.insert((graph, next_cell.cell_id.as_str()), sealed.as_ref())?;
    Ok(CapacityCellUpdateResult {
        schema_version: NativeControlSchemaVersion::V1,
        decision: CapacityDecision::Accepted,
        cell: next_cell,
        message: None,
    })
}

fn validate_id(value: &str, field: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > MAX_CAPACITY_ID_BYTES {
        return Err(format!(
            "{field} must be non-empty and at most {MAX_CAPACITY_ID_BYTES} bytes"
        ));
    }
    Ok(())
}

fn validate_cell_bounds(cell: &CapacityCell) -> Result<(), String> {
    cell.validate()
        .map_err(|error| format!("invalid capacity cell: {error:?}"))?;
    if cell.cell_id.len() > MAX_CAPACITY_ID_BYTES
        || cell
            .parent_id
            .as_deref()
            .is_some_and(|id| id.len() > MAX_CAPACITY_ID_BYTES)
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
    validate_acquire_batch_shape(request)?;
    for demand in &request.demands {
        validate_id(&demand.cell_id, "capacity demand cell_id")?;
        validate_demand_amount(demand.amount)?;
    }
    Ok(())
}

/// Batch-level acquire-request bounds that do not depend on any one demand:
/// demand count, ttl, budgets, and the single-demand `lease_id` rule.
fn validate_acquire_batch_shape(request: &CapacityAcquireRequest) -> Result<(), String> {
    if request.demands.is_empty() || request.demands.len() > MAX_CAPACITY_DEMANDS {
        return Err("capacity acquire demand count is outside native bounds".to_string());
    }
    if request.ttl_ms == 0 || request.ttl_ms > MAX_CAPACITY_TTL_MS {
        return Err("capacity acquire ttl_ms is outside native bounds".to_string());
    }
    if request
        .cost_budget_micros
        .is_some_and(|value| value > MAX_CAPACITY_BUDGET)
        || request
            .token_budget
            .is_some_and(|value| value > MAX_CAPACITY_BUDGET)
    {
        return Err("capacity budget is outside native bounds".to_string());
    }
    if request
        .lease_id
        .as_deref()
        .is_some_and(|id| id.len() > MAX_CAPACITY_ID_BYTES)
    {
        return Err("capacity lease_id exceeds native bounds".to_string());
    }
    if request.lease_id.is_some() && request.demands.len() != 1 {
        return Err("capacity lease_id is only valid for a single demand".to_string());
    }
    Ok(())
}

fn validate_demand_amount(amount: u64) -> Result<(), String> {
    if amount == 0 || amount > MAX_CAPACITY_AMOUNT {
        return Err("capacity demand amount is outside native bounds".to_string());
    }
    Ok(())
}

fn validate_mutation_request(
    request: &CapacityLeaseMutationRequest,
    renew: bool,
) -> Result<(), String> {
    if request.schema_version != NativeControlSchemaVersion::V1 {
        return Err("capacity lease mutation schema_version must be 1".to_string());
    }
    validate_id(&request.tenant_ref, "tenant_ref")?;
    validate_id(&request.owner_digest, "owner_digest")?;
    if request.leases.is_empty() || request.leases.len() > MAX_CAPACITY_MUTATION_BATCH {
        return Err("capacity lease mutation batch is outside native bounds".to_string());
    }
    if renew
        && request
            .ttl_ms
            .is_some_and(|ttl| ttl == 0 || ttl > MAX_CAPACITY_TTL_MS)
    {
        return Err("capacity renewal ttl_ms is outside native bounds".to_string());
    }
    if request
        .idempotency_key
        .as_deref()
        .is_some_and(|key| key.trim().is_empty() || key.len() > MAX_CAPACITY_ID_BYTES)
    {
        return Err("capacity idempotency_key is outside native bounds".to_string());
    }
    let mut ids = BTreeSet::new();
    for lease in &request.leases {
        validate_id(&lease.lease_id, "capacity lease_id")?;
        validate_lease_fence_unique(lease, &mut ids)?;
    }
    Ok(())
}

/// A mutation-batch lease fence must carry a nonzero epoch/fence token and
/// name each `lease_id` at most once within the batch.
fn validate_lease_fence_unique(
    lease: &eg_types::native_control::CapacityLeaseFence,
    ids: &mut BTreeSet<String>,
) -> Result<(), String> {
    if lease.lease_epoch == 0 || lease.fence_token == 0 || !ids.insert(lease.lease_id.clone()) {
        return Err("capacity lease fence is invalid or duplicated".to_string());
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
    if request
        .cell_id
        .as_deref()
        .is_some_and(|id| id.len() > MAX_CAPACITY_ID_BYTES)
    {
        return Err("capacity reclaim cell_id exceeds native bounds".to_string());
    }
    if request
        .cursor
        .as_deref()
        .is_some_and(|id| id.len() > MAX_CAPACITY_ID_BYTES)
    {
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
    if request
        .cell_id
        .as_deref()
        .is_some_and(|id| id.len() > MAX_CAPACITY_ID_BYTES)
        || request
            .lease_id
            .as_deref()
            .is_some_and(|id| id.len() > MAX_CAPACITY_ID_BYTES)
        || request
            .cursor
            .as_deref()
            .is_some_and(|id| id.len() > MAX_CAPACITY_ID_BYTES)
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
    write: &ShardWrite<'_>,
    graph: &str,
    tenant: &str,
    key: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableReplay>, String> {
    let table = write.graph(graph)?.open_scoped_table(IDEMPOTENCY)?;
    let found = table.get((graph, tenant, key))?;
    found
        .map(|value| decode_durable(&crypto.unseal(value.value())?))
        .transpose()
}

/// The identity of one replay record: which tenant/idempotency key it belongs to,
/// which operation produced it, and the digest it is keyed by. Grouped so the
/// writer keeps a readable arity (clippy::too_many_arguments) and so four
/// same-typed `&str` params cannot be passed in the wrong order.
struct ReplayKey<'a> {
    tenant: &'a str,
    key: &'a str,
    operation: &'a str,
    digest: &'a str,
}

fn write_replay<T: Serialize>(
    write: &ShardWrite<'_>,
    graph: &str,
    replay: ReplayKey<'_>,
    result: &T,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let ReplayKey {
        tenant,
        key,
        operation,
        digest,
    } = replay;
    let result = encode(result)?;
    let replay = DurableReplay {
        digest: digest.to_string(),
        operation: operation.to_string(),
        result,
    };
    let sealed_bytes = rmp_serde::to_vec_named(&replay).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&sealed_bytes);
    let mut table = write.graph(graph)?.open_scoped_table(IDEMPOTENCY)?;
    table.insert((graph, tenant, key), sealed.as_ref())?;
    Ok(())
}
