//! Tenant-bound Gap reads: one Gap by canonical id, or one bounded keyset page.
//! MVCC snapshot reads over the graph shard's own node table -- never the
//! writer thread, never a MutationBatch.

use eg_types::work_market::lifecycle::validate_gap_get;
use eg_types::work_market::{gap_row_key, is_tenant_gap, GapListRequest, GapPage, GapView};

use super::*;

/// `GapGet`: the caller's view of one Gap, `None` when `tenant` has no Gap
/// with this id. Another tenant's Gap lives under another key, so it is never
/// even addressed.
pub(crate) fn read_gap(
    shard: &Shard,
    graph: &str,
    tenant: &str,
    gap_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<GapView>, String> {
    validate_gap_get(tenant, gap_id)?;
    let key = gap_row_key(tenant, gap_id);
    let (_, row) = super::super::read::snapshot_row(shard, graph, &key, crypto)?;
    row.filter(|row| is_tenant_gap(row, tenant))
        .map(|row| GapView::from_row(&row))
        .transpose()
}

/// `GapList`: one bounded page of `request.tenant`'s Gaps in row-key order.
pub(crate) fn list_gaps(
    shard: &Shard,
    graph: &str,
    request: &GapListRequest,
    crypto: DurableCrypto<'_>,
) -> Result<GapPage, String> {
    request.validate()?;
    let resume_after = request.resume_after()?;
    let page = super::super::read::scan_keyset_page(
        shard,
        graph,
        request,
        resume_after.as_deref(),
        crypto,
    )?;
    Ok(GapPage {
        gaps: page.items,
        next_cursor: page.next_cursor,
    })
}
