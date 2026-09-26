//! Native work-market rows (EH-348): the canonical Gap and its derived offer.
//!
//! Every write runs inside the SAME durable WorkItem MutationBatch transaction
//! as the WorkItem transitions (replicated, audited, idempotent under the
//! caller's key) and writes through the same row writer, so the revision the
//! reads project is the one [`write_work_item_props`] maintains. `GapUpsert`
//! admits the Gap's WorkItem through [`apply_submit_work_item_rows`] in that
//! transaction, so the pair commits together or not at all.
//!
//! What each write DOES to a Gap is decided by [`eg_types::work_market`]; this
//! module only reads and writes rows.

use eg_types::work_market::{gap_row_key, is_tenant_gap, GapView};

use super::*;

mod lifecycle;
mod read;
mod upsert;

pub(crate) use read::{list_gaps, read_gap};

type NodeRow = serde_json::Map<String, serde_json::Value>;
type NodeRows<'table> = ScopedOwnerTableMut<'table, (&'static str, &'static str), &'static [u8]>;

/// The rows and commit scope a native record write (control lease or work
/// market) may touch: the node table, plus the edge and command-sequence
/// tables a `GapUpsert` needs to admit its WorkItem.
pub(crate) struct NativeRecordRequest<'args, 'table, 'crypto> {
    pub(crate) graph: &'args str,
    pub(crate) batch_id: &'args str,
    pub(crate) actor: Option<&'args str>,
    pub(crate) method: &'args Method,
    pub(crate) nodes: &'args mut NodeRows<'table>,
    pub(crate) edges: &'args mut NativeEdgeRows<'table>,
    pub(crate) command_sequences: &'args mut NativeSequenceRows<'table>,
    pub(crate) committed_at_ms: u64,
    pub(crate) crypto: DurableCrypto<'crypto>,
}

/// The native-record arm of the WorkItem-family applier: control leases, then
/// the work market. `None` for any method that is neither.
pub(crate) fn apply_native_record_rows(
    request: NativeRecordRequest<'_, '_, '_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let lease = apply_control_lease_rows(
        request.graph,
        request.method,
        request.nodes,
        request.crypto,
        request.committed_at_ms,
        request.actor,
    )?;
    if lease.is_some() {
        return Ok(lease);
    }
    match request.method {
        Method::GapUpsert { request: upsert } => upsert::apply_gap_upsert_rows(
            upsert,
            upsert::GapUpsertTables {
                graph: request.graph,
                nodes: request.nodes,
                edges: request.edges,
                command_sequences: request.command_sequences,
            },
            WorkItemCommitScope {
                crypto: request.crypto,
                authoritative_now_ms: request.committed_at_ms,
                outbox_id: request.batch_id,
            },
        )
        .map(Some),
        method => lifecycle::apply_gap_lifecycle_rows(lifecycle::GapLifecycleWrite {
            graph: request.graph,
            method,
            nodes: request.nodes,
            now_ms: request.committed_at_ms,
            crypto: request.crypto,
        }),
    }
}

/// `tenant`'s stored Gap `gap_id`, decoded -- `None` when no row holds it. A
/// row under the Gap's key that is not a Gap of `tenant` cannot be produced
/// by a native writer and is refused rather than overwritten.
fn load_gap(
    nodes: &NodeRows<'_>,
    graph: &str,
    tenant: &str,
    gap_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<GapView>, String> {
    let key = gap_row_key(tenant, gap_id);
    let Some(row) = super::control_lease::load_row(nodes, graph, &key, crypto)? else {
        return Ok(None);
    };
    if !is_tenant_gap(&row, tenant) {
        return Err(format!(
            "row '{key}' is not the canonical Gap it is keyed as"
        ));
    }
    GapView::from_row(&row).map(Some)
}

/// Write `gap` as `tenant`'s Gap row and return it as stored (with its new
/// revision).
fn store_gap(
    nodes: &mut NodeRows<'_>,
    graph: &str,
    tenant: &str,
    gap: &GapView,
    crypto: DurableCrypto<'_>,
) -> Result<GapView, String> {
    let key = gap_row_key(tenant, &gap.gap_id);
    let mut row = NodeRow::new();
    row.insert(
        eg_types::work_item_read::WORK_ITEM_ROW_REVISION.into(),
        gap.revision.into(),
    );
    gap.to_row(tenant, &mut row)?;
    write_work_item_props(nodes, graph, &key, &mut row, crypto)?;
    GapView::from_row(&row)
}

#[cfg(test)]
mod tests;
