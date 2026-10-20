//! Tenant-bound native WorkItem reads (EH-219): one row by id, or one bounded
//! keyset page. MVCC snapshot reads over the graph shard's own node table --
//! never the writer thread, never a MutationBatch.
//!
//! The projection, the tenant filter and the three-bound page rule live in
//! [`eg_types::work_item_read`]; this module only walks the stored rows.

use eg_types::keyset_page::{KeysetListing, KeysetPage, KeysetScan};
use eg_types::work_item_read::{
    validate_work_item_get, CommittedOutcomeRefs, WorkItemListRequest, WorkItemOutcomeView,
    WorkItemPage, WorkItemView,
};

use super::*;

type NodeTable = eg_storage::ScopedOwnerTable<(&'static str, &'static str), &'static [u8]>;
type NodeRow = serde_json::Map<String, serde_json::Value>;

/// One stored node row, decoded.
fn stored_row(
    nodes: &NodeTable,
    graph: &str,
    node_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<NodeRow>, String> {
    nodes
        .get((graph, node_id))?
        .map(|value| decode_durable(&crypto.unseal(value.value())?))
        .transpose()
}

/// Open `graph`'s node table on an MVCC snapshot and read one decoded row of
/// it. The table is returned so a caller can follow a reference the row holds
/// within the SAME snapshot.
pub(super) fn snapshot_row(
    shard: &Shard,
    graph: &str,
    node_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(NodeTable, Option<NodeRow>), String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let nodes = read.scoped_owner_table(NODES)?;
    let row = stored_row(&nodes, graph, node_id, crypto)?;
    Ok((nodes, row))
}

/// The caller's view of one WorkItem, `None` when no row with this id is a
/// WorkItem of `tenant`.
pub(crate) fn read_work_item(
    shard: &Shard,
    graph: &str,
    tenant: &str,
    work_item_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<WorkItemView>, String> {
    validate_work_item_get(tenant, work_item_id)?;
    let (_, row) = snapshot_row(shard, graph, work_item_id, crypto)?;
    row.map_or(Ok(None), |row| {
        WorkItemView::from_tenant_row(work_item_id, &row, tenant)
    })
}

/// `GetWorkItemOutcome` (graph-os EG-3): a WorkItem of `tenant` and the
/// provenance its native terminal commit bound. `None` when the item is not
/// visible to `tenant` or carries no committed outcome bundle.
pub(crate) fn read_work_item_outcome(
    shard: &Shard,
    graph: &str,
    tenant: &str,
    work_item_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<WorkItemOutcomeView>, String> {
    validate_work_item_get(tenant, work_item_id)?;
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let nodes = read.scoped_owner_table(NODES)?;
    let row = stored_row(&nodes, graph, work_item_id, crypto)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let Some(work_item) = WorkItemView::from_tenant_row(work_item_id, &row, tenant)? else {
        return Ok(None);
    };
    let Some(refs) = CommittedOutcomeRefs::from_row(&row) else {
        return Ok(None);
    };
    let outcome = verified_outcome(&nodes, graph, &refs, crypto)?;
    let terminal_event = match (
        row.get(eg_types::work_item_read::WORK_ITEM_EVENT_BATCH),
        row.get(eg_types::work_item_read::WORK_ITEM_EVENT_DIGEST),
    ) {
        (None, None) => eg_types::work_item_read::TerminalEventRead::default(),
        (Some(batch), Some(digest)) => {
            let batch = batch
                .as_str()
                .ok_or("invalid terminal event batch binding")?;
            let digest = digest
                .as_str()
                .ok_or("invalid terminal event digest binding")?;
            let receipt = eg_transaction::read_ledger(&read, batch)?
                .ok_or("committed terminal receipt is missing")?;
            let records = eg_transaction::read_outbox(&read, batch)?;
            terminal_event::project(
                read.scope(),
                tenant,
                work_item_id,
                batch,
                digest,
                &receipt,
                &records,
            )?
        }
        _ => return Err("incomplete terminal event binding".into()),
    };
    let committed_run_usage = row
        .get(eg_types::work_item_read::WORK_ITEM_COMMITTED_USAGE)
        .map(|value| {
            serde_json::from_value::<eg_types::run_usage::CommittedRunUsage>(value.clone())
                .map_err(|e| e.to_string())
        })
        .transpose()?;
    if let Some(usage) = &committed_run_usage {
        usage.validate()?;
    }
    Ok(Some(WorkItemOutcomeView {
        committed_run_usage,
        work_item,
        trace_ref: refs.trace_ref,
        tool_call_refs: refs.tool_call_refs,
        outcome_ref: refs.outcome_ref,
        outcome,
        terminal_event,
    }))
}

/// The OutcomeEvaluation receipt's stored properties, refused unless they are
/// byte-for-byte the ones the terminal commit bound (a generic writer can
/// reach a receipt row; it cannot make it match the recorded digest).
fn verified_outcome(
    nodes: &NodeTable,
    graph: &str,
    refs: &CommittedOutcomeRefs,
    crypto: DurableCrypto<'_>,
) -> Result<Option<NodeRow>, String> {
    use sha2::{Digest, Sha256};
    let Some(digest) = refs.outcome_digest.as_deref() else {
        return Ok(None);
    };
    let value = nodes
        .get((graph, refs.outcome_ref.as_str()))?
        .ok_or_else(|| "committed OutcomeEvaluation receipt is missing".to_string())?;
    let bytes = crypto.unseal(value.value())?;
    if hex::encode(Sha256::digest(&bytes)) != digest {
        return Err("OutcomeEvaluation receipt no longer matches its committed digest".into());
    }
    decode_durable(&bytes).map(Some)
}

/// One page of `tenant`'s WorkItems in node-id order, resumed strictly after
/// the request cursor's row.
pub(crate) fn list_work_items(
    shard: &Shard,
    graph: &str,
    request: &WorkItemListRequest,
    crypto: DurableCrypto<'_>,
) -> Result<WorkItemPage, String> {
    request.validate()?;
    let resume_after = request.resume_after()?;
    scan_keyset_page(shard, graph, request, resume_after.as_deref(), crypto).map(Into::into)
}

/// Walk `graph`'s node rows in key order from `resume_after` (exclusive),
/// feeding them to `listing`'s three-bound page. Shared by every tenant-bound
/// native listing.
pub(super) fn scan_keyset_page<L: KeysetListing>(
    shard: &Shard,
    graph: &str,
    listing: &L,
    resume_after: Option<&str>,
    crypto: DurableCrypto<'_>,
) -> Result<KeysetPage<L::Item>, String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let nodes = read.scoped_owner_table(NODES)?;
    let mut page = KeysetScan::new(listing);
    for row in nodes.scope_rows_from((graph, resume_after.unwrap_or("")))? {
        let (key, value) = row?;
        let (_, row_id) = key.value();
        // The seek start is inclusive; the cursor is exclusive.
        if resume_after == Some(row_id) {
            continue;
        }
        if !page.admits_another_row() {
            break;
        }
        let sealed = value.value();
        let props: NodeRow = decode_durable(&crypto.unseal(sealed)?)?;
        page.consume(row_id, sealed.len(), &props)?;
    }
    Ok(page.finish())
}

#[cfg(test)]
mod tests;

mod terminal_event;

/// Canonical bounded caller-context usage from one snapshot. The scan bound is
/// explicit coverage, not permission to treat an unscanned run as zero usage.
pub(crate) fn read_session_usage(
    shard: &Shard,
    graph: &str,
    tenant: &str,
    context_id: &str,
    principal: &str,
    crypto: DurableCrypto<'_>,
) -> Result<eg_types::run_usage::SessionUsage, String> {
    use eg_types::run_usage::{aggregate_session_usage, CommittedRunUsage, MAX_SESSION_USAGE_RUNS};
    validate_work_item_get(tenant, context_id)?;
    if principal.is_empty() {
        return Err("ACCESS_DENIED: usage owner required".into());
    }
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let nodes = read.scoped_owner_table(NODES)?;
    let mut expected = Vec::new();
    let mut records = Vec::new();
    let mut complete = true;
    let mut bytes = 0usize;
    for (examined, item) in nodes.scope_rows_from((graph, ""))?.enumerate() {
        if examined >= MAX_SESSION_USAGE_RUNS {
            complete = false;
            break;
        }
        let (key, value) = item?;
        bytes = bytes.saturating_add(value.value().len());
        if bytes > 32 * 1024 * 1024 {
            complete = false;
            break;
        }
        let row: NodeRow = decode_durable(&crypto.unseal(value.value())?)?;
        let owner = row
            .get("run_owner_binding")
            .and_then(serde_json::Value::as_object);
        let Some(owner) = owner else {
            continue;
        };
        if property_string(&row, "tenant") != tenant
            || property_string(&row, "node_type") != "WorkItem"
            || owner.get("tenant").and_then(serde_json::Value::as_str) != Some(tenant)
            || owner.get("principal").and_then(serde_json::Value::as_str) != Some(principal)
            || owner
                .get("work_item_id")
                .and_then(serde_json::Value::as_str)
                != Some(key.value().1)
            || owner.get("graph").and_then(serde_json::Value::as_str) != Some(graph)
        {
            continue;
        }
        if !matches!(
            property_string(&row, "status"),
            "succeeded" | "failed" | "cancelled" | "dead_letter"
        ) {
            continue;
        }
        let run_id = owner
            .get("run_id")
            .and_then(serde_json::Value::as_str)
            .ok_or("native run owner identity missing")?;
        if let Some(value) = row.get(eg_types::work_item_read::WORK_ITEM_COMMITTED_USAGE) {
            let record: CommittedRunUsage =
                serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
            record.validate()?;
            if record.run_id != run_id || record.tenant != tenant || record.principal != principal {
                return Err("committed usage native attribution mismatch".into());
            }
            if record.context_id != context_id {
                continue;
            }
            expected.push(run_id.to_string());
            records.push(record);
        } else if row
            .get("metadata")
            .and_then(serde_json::Value::as_object)
            .and_then(|metadata| metadata.get("a2a_context_id"))
            .and_then(serde_json::Value::as_str)
            == Some(context_id)
        {
            expected.push(run_id.to_string());
        }
    }
    let mut summary = aggregate_session_usage(tenant, principal, context_id, &expected, &records)?;
    summary.inventory_complete = complete;
    Ok(summary)
}
