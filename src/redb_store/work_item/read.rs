//! Tenant-bound native WorkItem reads (EH-219): one row by id, or one bounded
//! keyset page. MVCC snapshot reads over the graph shard's own node table --
//! never the writer thread, never a MutationBatch.
//!
//! The projection, the tenant filter and the three-bound page rule live in
//! [`eg_types::work_item_read`]; this module only walks the stored rows.

use eg_types::work_item_read::{
    validate_work_item_get, WorkItemListRequest, WorkItemPage, WorkItemPageScan, WorkItemView,
};

use super::*;

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
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let nodes = read.scoped_owner_table(NODES)?;
    let Some(value) = nodes.get((graph, work_item_id))? else {
        return Ok(None);
    };
    let row: serde_json::Map<String, serde_json::Value> =
        decode_durable(&crypto.unseal(value.value())?)?;
    WorkItemView::from_tenant_row(work_item_id, &row, tenant)
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
