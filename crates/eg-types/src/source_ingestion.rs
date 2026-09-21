//! Native raw-source ingestion contract (RF-ADR-009).
//!
//! Callers submit source records and an authoritative Connector Manifest mapping
//! reference.  They do not submit mapping content, tenant identity, graph
//! mutations, or a pre-mapped change envelope.  The server resolves the mapping
//! under the verified tenant and owns raw CAS admission, mapping, the cursor CAS,
//! provenance and the terminal durable receipt.

use std::collections::BTreeSet;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::contract::{
    BoundedVec, BoundedWriter, Digest256, RecordBytes, ResourceId, MAX_MUTATION_ENVELOPE_BYTES,
    MAX_RECORD_BYTES,
};

#[cfg(test)]
mod tests;

pub const SOURCE_INGESTION_CONTRACT_VERSION: u16 = 2;
pub const MAX_SOURCE_RECORDS: usize = 1_024;
pub const MAX_MAPPING_REFERENCE_BYTES: usize = 1_024;
pub const MAX_SOURCE_TEXT_BYTES: usize = 8_192;
/// Shared Rust/generated-client digest marker. The generated Python model must
/// preserve this declaration order when producing named MessagePack.
pub const SOURCE_INGESTION_DIGEST_DOMAIN: &str = "eg/source-ingestion-batch/v1";
pub const SOURCE_INGESTION_DIGEST_PROJECTION_FIELDS: &[&str] = &[
    "connector",
    "mapping_reference",
    "records",
    "cursor",
    "expected_previous_cursor",
];
/// Only these JSON-valued leaves are recursively key-sorted. Sorting their
/// containing DTO maps would destroy Rust named-struct declaration order.
pub const SOURCE_INGESTION_CANONICAL_JSON_PATHS: &[&str] = &[
    "records[*].payload",
    "cursor.position",
    "expected_previous_cursor.position",
];
/// Optional leaves that Rust serde omits when absent. The top-level
/// `expected_previous_cursor` is deliberately not listed: an initial page
/// emits that field as MessagePack nil.
pub const SOURCE_INGESTION_OMIT_NONE_PATHS: &[&str] = &[
    "records[*].updated_at",
    "cursor.watermark",
    "cursor.pending_watermark",
    "expected_previous_cursor.watermark",
    "expected_previous_cursor.pending_watermark",
];

/// Canonical, bounded JSON used for raw record payloads and provider cursor
/// positions.  The bytes are canonical JSON (not JSON embedded in a string), so
/// object key order cannot change a batch or cursor digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceJson(RecordBytes);

impl SourceJson {
    pub fn new(value: serde_json::Value) -> Result<Self, String> {
        validate_json_depth(&value, 0)?;
        let canonical = canonical_json(value);
        let mut buffer = BoundedWriter::new(
            MAX_RECORD_BYTES,
            "source JSON exceeds the record byte limit",
        );
        serde_json::to_writer(&mut buffer, &canonical)
            .map_err(|_| "source JSON exceeds the record byte limit")?;
        Ok(Self(RecordBytes::new(buffer.into_bytes())?))
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub fn value(&self) -> Result<serde_json::Value, String> {
        serde_json::from_slice(self.canonical_bytes()).map_err(|error| error.to_string())
    }
}

impl Serialize for SourceJson {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.value()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SourceJson {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = crate::msgpack::UniqueJsonValue::deserialize(deserializer)?.0;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "contract-schema")]
impl schemars::JsonSchema for SourceJson {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SourceJson".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        let _ = generator;
        schemars::Schema::try_from(serde_json::json!({
            "type": "object",
            "additionalProperties": true
        }))
        .expect("a JSON object schema is valid")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceRecordProvenance {
    pub connector: ResourceId,
    pub adapter_kind: ResourceId,
    pub server: String,
    pub tool: String,
    pub tool_schema_sha256: Digest256,
    pub source_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceRecord {
    pub stream: ResourceId,
    pub record_id: String,
    pub payload: SourceJson,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    pub provenance: SourceRecordProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceCursor {
    pub stream: ResourceId,
    pub position: SourceJson,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watermark: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_watermark: Option<String>,
}

impl SourceCursor {
    pub fn digest(&self) -> Result<Digest256, String> {
        Digest256::framed(
            b"eg/source-cursor/v1",
            &[
                self.stream.as_str().as_bytes(),
                self.position.canonical_bytes(),
                self.watermark.as_deref().unwrap_or_default().as_bytes(),
                self.pending_watermark
                    .as_deref()
                    .unwrap_or_default()
                    .as_bytes(),
            ],
        )
    }
}

pub type SourceRecords = BoundedVec<SourceRecord, MAX_SOURCE_RECORDS>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceIngestionBatch {
    pub connector: ResourceId,
    /// Opaque exact reference resolved by the tenant's Agent Library.  Mapping
    /// content is intentionally absent from this request.
    pub mapping_reference: String,
    pub records: SourceRecords,
    pub cursor: SourceCursor,
    /// `None` is valid only when no cursor has ever committed for this source.
    /// Otherwise the authoritative transaction compares this exact digest with
    /// the stored cursor and rejects stale/concurrent pages.
    pub expected_previous_cursor: Option<SourceCursor>,
}

/// A request whose structural invariants and byte bound were checked during
/// construction/deserialization.  Tenant, auth, mapping existence and the
/// persisted cursor are deliberately server-side authority checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceIngestionRequest(SourceIngestionBatch);

impl SourceIngestionRequest {
    pub fn new(batch: SourceIngestionBatch) -> Result<Self, String> {
        validate_batch(&batch)?;
        let request = Self(batch);
        request.canonical_bytes()?;
        Ok(request)
    }

    pub fn as_batch(&self) -> &SourceIngestionBatch {
        &self.0
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, String> {
        let mut buffer = BoundedWriter::new(
            MAX_MUTATION_ENVELOPE_BYTES,
            "source ingestion byte limit exceeded",
        );
        let mut serializer = rmp_serde::Serializer::new(&mut buffer).with_struct_map();
        self.0
            .serialize(&mut serializer)
            .map_err(|_| "source ingestion batch exceeds the mutation envelope byte limit")?;
        Ok(buffer.into_bytes())
    }

    pub fn batch_digest(&self) -> Result<Digest256, String> {
        Digest256::framed(
            SOURCE_INGESTION_DIGEST_DOMAIN.as_bytes(),
            &[&self.canonical_bytes()?],
        )
    }
}

impl<'de> Deserialize<'de> for SourceIngestionRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(SourceIngestionBatch::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SourceIngestionDisposition {
    Committed,
    Replayed,
}

/// One raw artifact admitted to content-addressed storage and bound to its
/// source identity. `deduplicated` reports an earlier identical artifact in
/// this batch (stable across replay); the CAS may also reuse bytes admitted by
/// prior batches. Every record retains its own durable provenance binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RawAdmissionReceipt {
    pub stream: ResourceId,
    pub record_id: String,
    pub raw_digest: Digest256,
    pub deduplicated: bool,
}

/// Terminal receipt returned only after canonical mutation, raw references,
/// provenance and cursor state have committed.  Its stable `receipt_digest`
/// is the replay currency for the same batch digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceIngestionReceipt {
    pub disposition: SourceIngestionDisposition,
    pub batch_digest: Digest256,
    pub mapping_reference: String,
    pub mapping_digest: Digest256,
    pub connector_pack_digest: Digest256,
    /// Exact served MCP catalog identity from the ConnectorPack head whose
    /// manifest mapping authorized this ingestion.
    pub catalog: crate::connector_pack::McpCatalogSnapshotBinding,
    pub raw_admissions: BoundedVec<RawAdmissionReceipt, MAX_SOURCE_RECORDS>,
    pub accepted_cursor: SourceCursor,
    pub accepted_cursor_digest: Digest256,
    pub affected_count: u64,
    pub committed_graph_version: u64,
    pub receipt_digest: Digest256,
}

fn validate_batch(batch: &SourceIngestionBatch) -> Result<(), String> {
    validate_batch_shape(batch)?;
    validate_cursor_fields(
        &batch.cursor,
        "source cursor",
        "cursor watermark",
        "cursor pending watermark",
    )?;
    validate_previous_cursor(batch)?;
    validate_records(batch)?;
    Ok(())
}

fn validate_batch_shape(batch: &SourceIngestionBatch) -> Result<(), String> {
    if batch.mapping_reference.is_empty()
        || batch.mapping_reference.len() > MAX_MAPPING_REFERENCE_BYTES
    {
        return Err("source ingestion requires a bounded mapping reference".into());
    }
    if batch.records.is_empty() {
        return Err("source ingestion requires at least one record".into());
    }
    Ok(())
}

fn validate_cursor_fields(
    cursor: &SourceCursor,
    cursor_field: &str,
    watermark_field: &str,
    pending_watermark_field: &str,
) -> Result<(), String> {
    validate_cursor(cursor, cursor_field)?;
    validate_cursor_text_fields(cursor, watermark_field, pending_watermark_field)
}

fn validate_cursor_text_fields(
    cursor: &SourceCursor,
    watermark_field: &str,
    pending_watermark_field: &str,
) -> Result<(), String> {
    validate_text(cursor.watermark.as_deref(), watermark_field)?;
    validate_text(cursor.pending_watermark.as_deref(), pending_watermark_field)
}

fn validate_previous_cursor(batch: &SourceIngestionBatch) -> Result<(), String> {
    let Some(previous) = &batch.expected_previous_cursor else {
        return Ok(());
    };
    if previous.stream != batch.cursor.stream {
        return Err("previous cursor stream must match the submitted cursor".into());
    }
    validate_cursor(previous, "previous cursor")?;
    if previous.digest()? == batch.cursor.digest()? {
        return Err("source cursor must differ from expected_previous_cursor".into());
    }
    validate_cursor_text_fields(
        previous,
        "previous cursor watermark",
        "previous cursor pending watermark",
    )?;
    Ok(())
}

fn validate_records(batch: &SourceIngestionBatch) -> Result<(), String> {
    let mut identities = BTreeSet::new();
    for record in &batch.records {
        validate_record(record, batch, &mut identities)?;
    }
    Ok(())
}

fn validate_record(
    record: &SourceRecord,
    batch: &SourceIngestionBatch,
    identities: &mut BTreeSet<(String, String)>,
) -> Result<(), String> {
    if record.stream != batch.cursor.stream {
        return Err("every source record stream must match the batch cursor".into());
    }
    if record.provenance.connector != batch.connector {
        return Err("source record provenance connector must match the batch connector".into());
    }
    validate_required_text(&record.record_id, "source record id")?;
    validate_text(record.updated_at.as_deref(), "source updated_at")?;
    validate_required_text(&record.provenance.server, "provenance server")?;
    validate_required_text(&record.provenance.tool, "provenance tool")?;
    validate_required_text(&record.provenance.source_uri, "provenance source_uri")?;
    if !record.payload.value()?.is_object() {
        return Err("source record payload must be a JSON object".into());
    }
    if !identities.insert((record.stream.as_str().to_owned(), record.record_id.clone())) {
        return Err("source ingestion batch contains a duplicate source identity".into());
    }
    Ok(())
}

fn validate_cursor(cursor: &SourceCursor, field: &str) -> Result<(), String> {
    if !cursor.position.value()?.is_object() {
        return Err(format!("{field} position must be a JSON object"));
    }
    Ok(())
}

fn validate_json_depth(value: &serde_json::Value, depth: usize) -> Result<(), String> {
    if depth > crate::msgpack::DEFAULT_MAX_DEPTH {
        return Err("source JSON nesting limit exceeded".into());
    }
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                validate_json_depth(value, depth + 1)?;
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                validate_json_depth(value, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn canonical_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json).collect())
        }
        serde_json::Value::Object(values) => {
            let sorted: std::collections::BTreeMap<_, _> = values.into_iter().collect();
            serde_json::Value::Object(
                sorted
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json(value)))
                    .collect(),
            )
        }
        other => other,
    }
}

fn validate_required_text(value: &str, field: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    validate_text(Some(value), field)
}

fn validate_text(value: Option<&str>, field: &str) -> Result<(), String> {
    if let Some(value) = value {
        if value.is_empty() {
            return Err(format!("{field} must not be empty when present"));
        }
        if value.len() > MAX_SOURCE_TEXT_BYTES {
            return Err(format!("{field} exceeds {MAX_SOURCE_TEXT_BYTES} bytes"));
        }
    }
    Ok(())
}
