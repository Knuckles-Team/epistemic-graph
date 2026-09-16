//! SQL source-ingestion wire contract. Publication is one SQL-owner transaction;
//! this module owns no store, cursor, or mutation ledger.

#[cfg(test)]
mod tests;
mod values;

use crate::change_envelope::CursorPosition;
use crate::contract::{BoundedVec, Digest256, RecordBytes, ResourceId};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeSet;
use std::num::NonZeroU64;

pub use values::{
    SqlSourceFloat, SqlSourceJson, SqlSourceText, SqlSourceVector, MAX_SQL_SOURCE_VECTOR_DIMENSIONS,
};

pub const MAX_SQL_SOURCE_ROWS: usize = 1_024;
pub const MAX_SQL_SOURCE_COLUMNS: usize = 256;
pub const MAX_SQL_SOURCE_BATCH_BYTES: usize = crate::contract::MAX_MUTATION_ENVELOPE_BYTES;

/// A closed cell vocabulary; bytes and canonical JSON ride as MessagePack bin,
/// integers/timestamps stay i64, and embeddings stay finite f32 values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SqlSourceCell {
    Null,
    Int(i64),
    FiniteFloat(SqlSourceFloat),
    Text(SqlSourceText),
    Bool(bool),
    /// Microseconds since Unix epoch, matching the native SQL cell convention.
    Timestamp(i64),
    Bytes(#[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))] RecordBytes),
    Json(SqlSourceJson),
    FiniteVector(SqlSourceVector),
}

pub type SqlSourceRow = BoundedVec<SqlSourceCell, MAX_SQL_SOURCE_COLUMNS>;
pub type SqlSourceRows = BoundedVec<SqlSourceRow, MAX_SQL_SOURCE_ROWS>;

/// Bounded source identity content. Metadata must be a JSON object and must not
/// contain credentials. A future adapter validates provider-specific authority;
/// merely presenting this descriptor does not grant source or table access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SqlSourceDescriptor {
    pub provider: ResourceId,
    pub dataset: ResourceId,
    pub metadata: SqlSourceJson,
}

/// Actual bounded mapping content, not a caller's unverified digest assertion.
/// Format interpretation/normalization belongs to the future trusted adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SqlSourceMappingDescriptor {
    pub format: ResourceId,
    #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
    pub content: RecordBytes,
}

/// Builder data for a checked submission. No request id, nonce, actor, tenant,
/// clock, or client-guessed epoch participates in its semantic content digest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SqlSourceBatch {
    pub source: ResourceId,
    /// Empty names the provider's unpartitioned stream. Tokens are preserved.
    pub partition: SqlSourceText,
    pub position: CursorPosition,
    pub expected_previous: Option<CursorPosition>,
    pub source_descriptor: SqlSourceDescriptor,
    pub mapping_descriptor: SqlSourceMappingDescriptor,
    pub table: ResourceId,
    pub columns: BoundedVec<ResourceId, MAX_SQL_SOURCE_COLUMNS>,
    pub rows: SqlSourceRows,
    pub expected_schema_version: u64,
    pub expected_schema_digest: Digest256,
}

/// Checked on deserialization before any SQL transaction can open. The
/// server must additionally preflight the outer frame using shared MsgpackLimits
/// and verify tenant/actor, mapping authority, schema and persisted cursor in its
/// authoritative transaction. This type cannot verify those external facts.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SqlSourceBatchRequest(SqlSourceBatch);

impl SqlSourceBatchRequest {
    pub fn new(batch: SqlSourceBatch) -> Result<Self, String> {
        validate_batch(&batch)?;
        Ok(Self(batch))
    }

    pub fn as_batch(&self) -> &SqlSourceBatch {
        &self.0
    }

    /// Deterministic bounded bytes for the server compiler. Source/mapping
    /// authority still requires validation; a digest is integrity, not permission.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, String> {
        values::bounded_msgpack(&self.0, MAX_SQL_SOURCE_BATCH_BYTES)
    }

    pub fn canonical_digests(&self) -> Result<SqlSourceBatchDigests, String> {
        let source =
            values::bounded_msgpack(&self.0.source_descriptor, MAX_SQL_SOURCE_BATCH_BYTES)?;
        let mapping =
            values::bounded_msgpack(&self.0.mapping_descriptor, MAX_SQL_SOURCE_BATCH_BYTES)?;
        Ok(SqlSourceBatchDigests {
            source_digest: Digest256::framed(b"eg/sql-source-descriptor/v1", &[&source])?,
            mapping_digest: Digest256::framed(b"eg/sql-source-mapping/v1", &[&mapping])?,
            batch_digest: Digest256::framed(
                b"eg/sql-source-batch/v1",
                &[&self.canonical_bytes()?],
            )?,
        })
    }
}

impl<'de> Deserialize<'de> for SqlSourceBatchRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(SqlSourceBatch::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SqlSourceBatchDigests {
    pub source_digest: Digest256,
    pub mapping_digest: Digest256,
    pub batch_digest: Digest256,
}

/// Durable terminal result, generated after the exact SQL source epoch increment
/// inside the owner transaction and retained unchanged for idempotent replay.
/// Never construct from a server preflight or a freshly sampled current epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SqlSourceBatchResult {
    pub canonical_digests: SqlSourceBatchDigests,
    pub accepted_position: CursorPosition,
    pub affected_count: u64,
    pub committed_source_epoch: NonZeroU64,
    pub authority_digest: Digest256,
}

fn validate_batch(batch: &SqlSourceBatch) -> Result<(), String> {
    validate_cursor(&batch.position)?;
    if let Some(previous) = &batch.expected_previous {
        validate_cursor(previous)?;
        if !batch.position.advances(previous) {
            return Err("SQL source position must advance expected_previous".into());
        }
    }
    validate_batch_shape(batch)?;
    values::bounded_msgpack(batch, MAX_SQL_SOURCE_BATCH_BYTES)?;
    Ok(())
}

fn validate_batch_shape(batch: &SqlSourceBatch) -> Result<(), String> {
    if batch.partition.as_str().len() > crate::contract::MAX_OPAQUE_ID_BYTES {
        return Err("SQL source partition exceeds the cursor token limit".into());
    }
    if !batch.source_descriptor.metadata.value()?.is_object()
        || batch.mapping_descriptor.content.as_slice().is_empty()
    {
        return Err("SQL source descriptors require object metadata and mapping content".into());
    }
    let names: BTreeSet<_> = batch.columns.iter().collect();
    if batch.columns.is_empty() || names.len() != batch.columns.len() || batch.rows.is_empty() {
        return Err("SQL source batch requires distinct columns and nonempty rows".into());
    }
    if batch
        .rows
        .iter()
        .any(|row| row.len() != batch.columns.len())
    {
        return Err("SQL source row width must match columns".into());
    }
    Ok(())
}

fn validate_cursor(position: &CursorPosition) -> Result<(), String> {
    if let CursorPosition::Opaque { cursor_type, value } = position {
        ResourceId::new(cursor_type.clone())?;
        if value.is_empty() || value.len() > crate::contract::MAX_OPAQUE_ID_BYTES {
            return Err("SQL source opaque position requires a bounded nonempty token".into());
        }
    }
    Ok(())
}
