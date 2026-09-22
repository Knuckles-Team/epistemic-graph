//! Tenant-bound native WorkItem reads (EH-219): one row by id, or one bounded
//! keyset page. MVCC snapshot reads over the graph shard's own node table --
//! never the writer thread, never a MutationBatch.
//!
//! The projection, the tenant filter and the three-bound page rule live in
//! [`eg_types::work_item_read`]; this module only walks the stored rows.

use eg_types::work_item_read::{
    validate_work_item_get, CommittedOutcomeRefs, WorkItemListRequest, WorkItemOutcomeView,
    WorkItemPage, WorkItemPageScan, WorkItemView,
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
    let (nodes, row) = snapshot_row(shard, graph, work_item_id, crypto)?;
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
    Ok(Some(WorkItemOutcomeView {
        work_item,
        trace_ref: refs.trace_ref,
        tool_call_refs: refs.tool_call_refs,
        outcome_ref: refs.outcome_ref,
        outcome,
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
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let nodes = read.scoped_owner_table(NODES)?;
    let start = resume_after.as_deref().unwrap_or("");
    let mut page = WorkItemPageScan::new(request);
    for row in nodes.scope_rows_from((graph, start))? {
        let (key, value) = row?;
        let (_, row_id) = key.value();
        // The seek start is inclusive; the cursor is exclusive.
        if resume_after.as_deref() == Some(row_id) {
            continue;
        }
        if !page.admits_another_row() {
            break;
        }
        let sealed = value.value();
        let props: serde_json::Map<String, serde_json::Value> =
            decode_durable(&crypto.unseal(sealed)?)?;
        page.consume(row_id, sealed.len(), &props)?;
    }
    Ok(page.finish())
}

#[cfg(test)]
mod tests;
