//! Provider stream continuity and terminal publication inside one SQL owner gate.

use super::{Invocation, MutationBatch, SqlWrite};
use eg_storage::SQL_SOURCE_CHECKPOINTS;
use eg_types::change_envelope::CursorPosition;
use eg_types::contract::{BoundedVec, Digest256, ResourceId};
use eg_types::storage_wire::{SqlSourceBatchResult, SqlSourceText};
use redb::ReadableTable;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

const CHECKPOINT_SCHEMA_VERSION: u16 = 1;

/// This SQL-owned business row retains the latest provider cursor and its
/// exact terminal result. Idempotency and old results remain solely kernel rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Checkpoint {
    schema_version: u16,
    tenant: eg_types::mutation_batch::ScopeTenantId,
    source: ResourceId,
    partition: SqlSourceText,
    table: ResourceId,
    columns: BoundedVec<ResourceId, 256>,
    table_schema_version: u64,
    table_schema_digest: Digest256,
    pub(super) batch_id: String,
    pub(super) result: SqlSourceBatchResult,
}

pub(super) fn validate_previous(
    write: &SqlWrite<'_>,
    invocation: &Invocation<'_>,
) -> Result<(), String> {
    let submitted = invocation.request.as_batch();
    let table = write.open_table(SQL_SOURCE_CHECKPOINTS)?;
    let found = table
        .get((
            invocation.tenant,
            submitted.source.as_str(),
            submitted.partition.as_str(),
        ))
        .map_err(|error| error.to_string())?;
    let Some(value) = found else {
        if submitted.expected_previous.is_some() {
            return Err("SQL source expected_previous requires an existing checkpoint".into());
        }
        return Ok(());
    };
    let previous: Checkpoint = eg_storage::decode_ledger_record(value.value())?;
    validate_binding(&previous, invocation)?;
    validate_cursor_cas(
        &previous.result.accepted_position,
        submitted.expected_previous.as_ref(),
        &submitted.position,
    )
}

fn validate_binding(previous: &Checkpoint, invocation: &Invocation<'_>) -> Result<(), String> {
    let submitted = invocation.request.as_batch();
    if previous.schema_version != CHECKPOINT_SCHEMA_VERSION
        || previous.tenant.as_str() != invocation.tenant
        || previous.source != submitted.source
        || previous.partition != submitted.partition
        || previous.result.canonical_digests.source_digest != invocation.digests.source_digest
        || previous.result.canonical_digests.mapping_digest != invocation.digests.mapping_digest
    {
        return Err("SQL source checkpoint descriptor, mapping or schema binding differs".into());
    }
    validate_target_binding(previous, invocation)
}

/// Target columns and governed schema are a separate stable binding from the
/// provider stream. A remapping/re-schema requires an explicit future migration.
fn validate_target_binding(
    previous: &Checkpoint,
    invocation: &Invocation<'_>,
) -> Result<(), String> {
    let submitted = invocation.request.as_batch();
    if previous.table != submitted.table
        || previous.columns != submitted.columns
        || previous.table_schema_version != submitted.expected_schema_version
        || previous.table_schema_digest != submitted.expected_schema_digest
    {
        return Err("SQL source checkpoint target schema binding differs".into());
    }
    Ok(())
}

fn validate_cursor_cas(
    previous: &CursorPosition,
    expected: Option<&CursorPosition>,
    next: &CursorPosition,
) -> Result<(), String> {
    if expected != Some(previous) || !next.advances(previous) {
        return Err("SQL source checkpoint compare-and-swap conflict".into());
    }
    Ok(())
}

pub(super) fn finalize(
    write: &SqlWrite<'_>,
    batch: &MutationBatch,
    invocation: &Invocation<'_>,
    affected: usize,
    epoch: u64,
) -> Result<Vec<u8>, String> {
    let submitted = invocation.request.as_batch();
    let result = SqlSourceBatchResult {
        canonical_digests: invocation.digests.clone(),
        accepted_position: submitted.position.clone(),
        affected_count: u64::try_from(affected).map_err(|error| error.to_string())?,
        committed_source_epoch: NonZeroU64::new(epoch)
            .ok_or_else(|| "SQL source publication requires a nonzero staged epoch".to_string())?,
        authority_digest: invocation.authority_digest,
    };
    let checkpoint = Checkpoint {
        schema_version: CHECKPOINT_SCHEMA_VERSION,
        tenant: eg_types::mutation_batch::ScopeTenantId::new(invocation.tenant)?,
        source: submitted.source.clone(),
        partition: submitted.partition.clone(),
        table: submitted.table.clone(),
        columns: submitted.columns.clone(),
        table_schema_version: submitted.expected_schema_version,
        table_schema_digest: submitted.expected_schema_digest,
        batch_id: batch.batch_id.clone(),
        result: result.clone(),
    };
    let encoded = eg_storage::encode_bounded(&checkpoint, "SQL source checkpoint")?;
    write
        .open_table(SQL_SOURCE_CHECKPOINTS)?
        .insert(
            (
                invocation.tenant,
                submitted.source.as_str(),
                submitted.partition.as_str(),
            ),
            encoded.as_slice(),
        )
        .map_err(|error| error.to_string())?;
    eg_storage::encode_bounded(&result, "SQL source terminal result")
}
