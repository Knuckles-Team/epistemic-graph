//! Native sealed-record retirement (EH-558): the owning op of a sealed record.
//!
//! Retirement runs inside the SAME durable WorkItem MutationBatch transaction as
//! the WorkItem and control-lease transitions (replicated, audited, idempotent under
//! the caller's key). It replaces the record row, in place, by the tombstone
//! [`RetireSealedRecordRequest::resolve`] builds. The rules and the row shapes are
//! owned by [`eg_types::sealed_record`]; this module only reads and writes the row.
//! Generic writers never reach this path, and the generic row guard refuses them
//! any change to the row.

use eg_types::result_contract::coordination::RetireSealedRecord;
use eg_types::sealed_record::RetireSealedRecordRequest;

use super::*;

type NodeRow = serde_json::Map<String, serde_json::Value>;

/// Resolve one retirement against the stored row and write the tombstone when it
/// retires the record.
pub(crate) fn apply_retire_sealed_record_row(
    graph: &str,
    request: &RetireSealedRecordRequest,
    nodes: &mut ScopedOwnerTableMut<'_, (&'static str, &'static str), &'static [u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    request.validate()?;
    let stored: Option<NodeRow> = nodes
        .get((graph, request.node_id.as_str()))?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?
        .map(|bytes| decode_durable(&bytes))
        .transpose()?;
    let resolution = request.resolve(stored.as_ref());
    if let Some(row) = &resolution.write {
        let bytes = rmp_serde::to_vec_named(row).map_err(|error| error.to_string())?;
        nodes
            .insert(
                (graph, request.node_id.as_str()),
                crypto.seal(&bytes).as_ref(),
            )
            .map_err(|error| error.to_string())?;
    }
    crate::protocol::ResultPayload::of::<RetireSealedRecord>(resolution.result()).map(Some)
}

#[cfg(test)]
#[path = "sealed_record_tests.rs"]
mod tests;
