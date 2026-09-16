//! Typed SQL source rows are authorized by the same INSERT grant and RLS
//! snapshot as native SQL inserts. No source descriptor or mapping digest
//! grants access.

use crate::server::access::CarrierAuthority;
use crate::server::sql_catalog_acl::{SqlPrivilege, SqlSourceAuthorityWrite, ACCESS_DENIED};
use eg_types::storage_wire::{SqlSourceBatchRequest, SqlSourceCell};

/// Authorize exact typed source cells under the existing INSERT grant and RLS
/// snapshot. Unlike literal SQL INSERT, this closed ingestion contract requires
/// an explicit matching RLS stamp so its canonical request and replay identity
/// stay unchanged.
pub(super) fn authorize_source_insert(
    source: &SqlSourceAuthorityWrite<'_, '_>,
    authority: &CarrierAuthority,
    request: &SqlSourceBatchRequest,
) -> Result<(), String> {
    let batch = request.as_batch();
    let snapshot = source.authorized_snapshot(batch.table.as_str(), SqlPrivilege::Insert)?;
    let Some(column) = snapshot.rls_column.as_deref() else {
        return Ok(());
    };
    require_source_insert_stamp(request, column, authority.agent_id())
}

fn require_source_insert_stamp(
    request: &SqlSourceBatchRequest,
    column: &str,
    agent_id: &str,
) -> Result<(), String> {
    let batch = request.as_batch();
    let index = batch
        .columns
        .iter()
        .position(|candidate| candidate.as_str() == column)
        .ok_or_else(|| ACCESS_DENIED.to_string())?;
    if batch.rows.iter().all(|row| {
        matches!(row.as_slice().get(index), Some(SqlSourceCell::Text(stamp)) if stamp.as_str() == agent_id)
    }) {
        Ok(())
    } else {
        Err(ACCESS_DENIED.to_string())
    }
}
