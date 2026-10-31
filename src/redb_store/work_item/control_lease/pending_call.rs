//! Paired pending-envelope storage. No approval or execution authority.
use super::*;
use medulla_types::control_lease::{PendingCallFence, PENDING_CALL_KIND};
use medulla_types::epistemic_operations_ext::CasWorkItemMetadataLeaseFence;

const PENDING_REF: &str = "pending_call_lease_ref";
const REGISTERED_WORK: &str = "pending_call_work_item";
const REGISTERED_FENCE: &str = "pending_call_worker_digest";

/// Check the native provenance table before trusting any mirrored lease tuple.
pub(super) fn require_claim(
    graph: &str,
    method: &Method,
    native_work_items: &NativeNodeRows<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let work_id = match method {
        Method::IssueControlLease { request } => request
            .pending_call_registration
            .as_ref()
            .map(|pending| pending.work_item.work_item_id.as_str()),
        Method::TransitionControlLease { request } => request
            .pending_call_consumption
            .as_ref()
            .map(|pending| pending.work_item.work_item_id.as_str()),
        _ => None,
    };
    if let Some(work_id) = work_id {
        crate::redb_store::work_item_capability::require_native_claim(
            native_work_items,
            graph,
            work_id,
            crypto,
        )?;
    }
    Ok(())
}

fn worker_digest(fence: &CasWorkItemMetadataLeaseFence) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"eg/pending-call-worker/v1\0");
    hash.update(serde_json::to_vec(fence).map_err(|e| e.to_string())?);
    Ok(hex::encode(hash.finalize()))
}

fn work_item(
    graph: &str,
    tenant: &str,
    fence: &PendingCallFence,
    nodes: &NativeNodeRows<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<NodeRow, String> {
    fence.validate()?;
    let row = load_row(nodes, graph, &fence.work_item_id, crypto)?
        .ok_or("pending WorkItem unavailable")?;
    let metadata = row
        .get("metadata")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    if property_string(&row, "tenant") != tenant
        || property_string(&row, "node_type") != "WorkItem"
        || property_u64(&row, "row_revision") != fence.expected_revision
        || metadata != serde_json::Value::Object(fence.expected_metadata.clone())
    {
        return Err("pending WorkItem preimage conflict".into());
    }
    Ok(row)
}

pub(super) fn live_worker(
    row: &NodeRow,
    fence: &CasWorkItemMetadataLeaseFence,
    now_ms: u64,
) -> bool {
    let expiry = property_f64(row, "lease_expires_at");
    let fenced = property_u64(row, "attempt") != 0
        && fence.lease_epoch != 0
        && fence.fencing_token != 0
        && !property_string(row, "work_item_fence").is_empty();
    fenced
        && expiry.is_finite()
        && expiry > 0.0
        && (expiry * 1000.0).floor() as u64 > now_ms
        && super::super::commit_work_item_lease_is_valid(
            row,
            &fence.worker_ref,
            fence.lease_epoch,
            fence.fencing_token,
            now_ms,
        )
}

pub(super) fn registration(
    graph: &str,
    request: &IssueControlLeaseRequest,
    lease: &mut NodeRow,
    nodes: &NativeNodeRows<'_>,
    now_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<Option<(String, NodeRow)>, String> {
    let Some(registration) = &request.pending_call_registration else {
        return Ok(None);
    };
    let fence = &registration.work_item;
    let mut work = work_item(graph, &request.tenant, fence, nodes, crypto)?;
    if work.contains_key(PENDING_REF)
        || !live_worker(&work, &registration.expected_lease, now_ms)
        || now_ms < request.issued_at_ms
        || now_ms >= request.expires_at_ms
    {
        return Err("pending registration fence or lifetime conflict".into());
    }
    lease.insert(REGISTERED_WORK.into(), fence.work_item_id.clone().into());
    lease.insert(
        REGISTERED_FENCE.into(),
        worker_digest(&registration.expected_lease)?.into(),
    );
    work.insert(PENDING_REF.into(), request.lease_id.clone().into());
    Ok(Some((fence.work_item_id.clone(), work)))
}

pub(super) fn transition(
    graph: &str,
    request: &TransitionControlLeaseRequest,
    lease: &NodeRow,
    nodes: &NativeNodeRows<'_>,
    now_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<Option<(String, NodeRow)>, String> {
    let pending = property_string(lease, "kind") == PENDING_CALL_KIND;
    if !pending {
        if request.pending_call_consumption.is_some() {
            return Err("pending consumption requires reserved lease kind".into());
        }
        return Ok(None);
    }
    let work_id = property_string(lease, REGISTERED_WORK);
    if work_id.is_empty() {
        return Err("pending lease lacks native registration".into());
    }
    // Ending an already consumed lease does not touch its formerly paired row.
    if property_string(lease, "status") == "consumed" {
        if request.pending_call_consumption.is_some() {
            return Err("pending reference already consumed".into());
        }
        return Ok(None);
    }
    let mut work = if request.to == ControlLeaseTarget::Consumed {
        consumption_work(graph, request, lease, nodes, now_ms, crypto)?
    } else {
        load_row(nodes, graph, work_id, crypto)?.ok_or("pending WorkItem unavailable")?
    };
    if property_string(&work, "tenant") != request.tenant
        || property_string(&work, PENDING_REF) != request.lease_id
    {
        return Err("pending lease reference mismatch".into());
    }
    work.remove(PENDING_REF);
    Ok(Some((work_id.to_owned(), work)))
}

fn consumption_work(
    graph: &str,
    request: &TransitionControlLeaseRequest,
    lease: &NodeRow,
    nodes: &NativeNodeRows<'_>,
    now_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<NodeRow, String> {
    let work_id = property_string(lease, REGISTERED_WORK);
    let consume = request
        .pending_call_consumption
        .as_ref()
        .ok_or("pending consumption fence required")?;
    if consume.work_item.work_item_id != work_id
        || lease.get("grant") != Some(&serde_json::Value::Object(consume.expected_grant.clone()))
    {
        return Err("pending envelope mismatch".into());
    }
    let work = work_item(graph, &request.tenant, &consume.work_item, nodes, crypto)?;
    let worker = CasWorkItemMetadataLeaseFence {
        worker_ref: property_string(&work, "lease_owner").to_owned(),
        lease_epoch: property_u64(&work, "lease_epoch"),
        fencing_token: property_u64(&work, "fencing_token"),
    };
    if property_string(lease, REGISTERED_FENCE) != worker_digest(&worker)?
        || !live_worker(&work, &worker, now_ms)
    {
        return Err("pending worker fence expired or replaced".into());
    }
    Ok(work)
}

pub(super) fn write_pair(
    graph: &str,
    pair: Option<(String, NodeRow)>,
    nodes: &mut NativeNodeRows<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<String>, String> {
    match pair {
        None => Ok(Vec::new()),
        Some((id, mut row)) => {
            write_work_item_props(nodes, graph, &id, &mut row, crypto)?;
            Ok(vec![id])
        }
    }
}
