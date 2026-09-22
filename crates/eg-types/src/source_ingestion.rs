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

pub const SOURCE_INGESTION_CONTRACT_VERSION: u16 = 3;
pub const MAX_SOURCE_RECORDS: usize = 1_024;
pub const MAX_SOURCE_RELATIONSHIPS: usize = 4_096;
pub const MAX_SOURCE_WITHDRAWALS: usize = 1_024;
pub const MAX_AUTHORITATIVE_LIVE_IDS: usize = 16_384;
pub const MAX_SOURCE_MAPPING_RECEIPTS: usize = MAX_SOURCE_RECORDS + MAX_SOURCE_RELATIONSHIPS;
pub const MAX_MAPPING_REFERENCE_BYTES: usize = 1_024;
pub const MAX_SOURCE_TEXT_BYTES: usize = 8_192;
/// Shared Rust/generated-client digest marker. The generated Python model must
/// preserve this declaration order when producing named MessagePack.
pub const SOURCE_INGESTION_DIGEST_DOMAIN: &str = "eg/source-ingestion-batch/v2";
pub const SOURCE_INGESTION_DIGEST_PROJECTION_FIELDS: &[&str] = &[
    "connector",
    "mode",
    "strict_schema",
    "records",
    "relationships",
    "provider_checkpoint",
    "expected_previous_checkpoint",
    "authoritative_live_ids",
    "empty_authoritative_approval",
    "withdrawals",
];
/// Only these JSON-valued leaves are recursively key-sorted. Sorting their
/// containing DTO maps would destroy Rust named-struct declaration order.
pub const SOURCE_INGESTION_CANONICAL_JSON_PATHS: &[&str] = &[
    "records[*].payload",
    "relationships[*].properties",
    "provider_checkpoint.position",
    "expected_previous_checkpoint.position",
];
/// Optional leaves that Rust serde omits when absent. The top-level
/// `expected_previous_checkpoint` is deliberately not listed: an initial page
/// emits that field as MessagePack nil.
pub const SOURCE_INGESTION_OMIT_NONE_PATHS: &[&str] = &[
    "records[*].updated_at",
    "relationships[*].properties",
    "provider_checkpoint.content_hash",
    "provider_checkpoint.watermark",
    "provider_checkpoint.pending_watermark",
    "expected_previous_checkpoint.content_hash",
    "expected_previous_checkpoint.watermark",
    "expected_previous_checkpoint.pending_watermark",
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
        schemars::Schema::try_from(serde_json::json!({}))
            .expect("an unconstrained JSON schema is valid")
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
    /// Exact Connector Manifest schema-mapping reference for this entity.
    pub mapping_reference: String,
    pub payload: SourceJson,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    pub provenance: SourceRecordProvenance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SourceIngestionMode {
    Full,
    Delta,
    Reconcile,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceEntityRef {
    pub stream: ResourceId,
    pub record_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
/// A provider-observed edge. EG derives its durable relationship identity from
/// verified tenant/source authority, the exact relation reference and endpoints;
/// the caller cannot supply a competing identity or digest.
pub struct SourceRelationship {
    pub source: SourceEntityRef,
    pub target: SourceEntityRef,
    /// Exact Connector Manifest resource-relation reference, for example
    /// `manifest:demo#resources/Document/relations/contains`.
    pub relation_reference: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<SourceJson>,
    pub provenance: SourceRecordProvenance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceWithdrawal {
    pub entity: SourceEntityRef,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceCheckpoint {
    pub stream: ResourceId,
    pub position: SourceJson,
    /// Provider-owned content identity for the returned page or snapshot.
    /// EG binds it into checkpoint CAS and replay; it never invents a value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<Digest256>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watermark: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_watermark: Option<String>,
}

impl SourceCheckpoint {
    pub fn digest(&self) -> Result<Digest256, String> {
        Digest256::framed(
            b"eg/source-checkpoint/v1",
            &[
                self.stream.as_str().as_bytes(),
                self.position.canonical_bytes(),
                self.content_hash
                    .as_ref()
                    .map(|digest| digest.as_bytes().as_slice())
                    .unwrap_or(&[]),
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
pub type SourceRelationships = BoundedVec<SourceRelationship, MAX_SOURCE_RELATIONSHIPS>;
pub type SourceWithdrawals = BoundedVec<SourceWithdrawal, MAX_SOURCE_WITHDRAWALS>;
pub type AuthoritativeLiveIds = BoundedVec<SourceEntityRef, MAX_AUTHORITATIVE_LIVE_IDS>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceIngestionBatch {
    pub connector: ResourceId,
    pub mode: SourceIngestionMode,
    /// Reject source payload/property keys not selected by their manifest map.
    pub strict_schema: bool,
    pub records: SourceRecords,
    #[serde(default)]
    pub relationships: SourceRelationships,
    pub provider_checkpoint: SourceCheckpoint,
    /// `None` is valid only when no cursor has ever committed for this source.
    /// Otherwise the authoritative transaction compares this exact digest with
    /// the stored cursor and rejects stale/concurrent pages.
    pub expected_previous_checkpoint: Option<SourceCheckpoint>,
    /// Required only for `reconcile`. EG compares it with its prior durable
    /// live-set marker and derives tombstones; callers never submit that diff.
    pub authoritative_live_ids: Option<AuthoritativeLiveIds>,
    /// Required with an empty authoritative live set. The verified request must
    /// also carry the dedicated empty-reconciliation capability.
    pub empty_authoritative_approval: Option<String>,
    /// Provider-declared tombstones are accepted only for `delta`.
    #[serde(default)]
    pub withdrawals: SourceWithdrawals,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RawRelationshipAdmissionReceipt {
    pub relationship_id: String,
    pub raw_digest: Digest256,
    pub deduplicated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceMappingReceipt {
    pub kind: SourceMappingKind,
    pub mapping_reference: String,
    pub mapping_digest: Digest256,
    pub connector_pack_digest: Digest256,
    pub catalog: crate::connector_pack::McpCatalogSnapshotBinding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SourceMappingKind {
    Entity,
    Relationship,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceTombstoneReceipt {
    pub entity: SourceEntityRef,
    pub node_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceRelationshipTombstoneReceipt {
    pub relationship_id: String,
    pub source: SourceEntityRef,
    pub target: SourceEntityRef,
    pub reason: String,
}

/// Terminal receipt returned only after canonical mutation, raw references,
/// provenance and cursor state have committed.  Its stable `receipt_digest`
/// is the replay currency for the same batch digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceIngestionReceipt {
    pub receipt_id: String,
    pub disposition: SourceIngestionDisposition,
    pub mode: SourceIngestionMode,
    pub batch_digest: Digest256,
    pub mappings: BoundedVec<SourceMappingReceipt, MAX_SOURCE_MAPPING_RECEIPTS>,
    pub raw_admissions: BoundedVec<RawAdmissionReceipt, MAX_SOURCE_RECORDS>,
    pub relationship_raw_admissions:
        BoundedVec<RawRelationshipAdmissionReceipt, MAX_SOURCE_RELATIONSHIPS>,
    pub tombstones: BoundedVec<SourceTombstoneReceipt, MAX_AUTHORITATIVE_LIVE_IDS>,
    pub relationship_tombstones:
        BoundedVec<SourceRelationshipTombstoneReceipt, MAX_SOURCE_RELATIONSHIPS>,
    pub accepted_checkpoint: SourceCheckpoint,
    pub accepted_checkpoint_digest: Digest256,
    pub content_hash: Option<Digest256>,
    pub live_set_digest: Option<Digest256>,
    pub relationship_live_set_digest: Option<Digest256>,
    pub affected_count: u64,
    pub relationship_count: u64,
    pub tombstoned_count: u64,
    pub relationship_tombstoned_count: u64,
    pub committed_graph_version: u64,
    pub receipt_digest: Digest256,
}

/// Read-only restart/failover key. Tenant and graph remain verified server
/// authority and therefore never appear in this caller payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceIngestStatusRequest {
    pub connector: ResourceId,
    pub stream: ResourceId,
}

/// Latest checkpoint accepted by the one EG ingestion authority for a source
/// partition. `None` means no batch has committed; callers must not substitute
/// a local checkpoint in that case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceIngestStatus {
    pub connector: ResourceId,
    pub stream: ResourceId,
    pub accepted_checkpoint: Option<SourceCheckpoint>,
    pub accepted_checkpoint_digest: Option<Digest256>,
    pub content_hash: Option<Digest256>,
    pub live_set_digest: Option<Digest256>,
    pub relationship_live_set_digest: Option<Digest256>,
    pub last_batch_digest: Option<Digest256>,
    pub last_receipt_id: Option<String>,
    pub committed_graph_version: Option<u64>,
}

fn validate_batch(batch: &SourceIngestionBatch) -> Result<(), String> {
    validate_cursor_fields(
        &batch.provider_checkpoint,
        "provider checkpoint",
        "checkpoint watermark",
        "checkpoint pending watermark",
    )?;
    validate_previous_checkpoint(batch)?;
    validate_records(batch)?;
    validate_relationships(batch)?;
    validate_reconciliation(batch)?;
    Ok(())
}

fn validate_cursor_fields(
    cursor: &SourceCheckpoint,
    cursor_field: &str,
    watermark_field: &str,
    pending_watermark_field: &str,
) -> Result<(), String> {
    validate_cursor(cursor, cursor_field)?;
    validate_cursor_text_fields(cursor, watermark_field, pending_watermark_field)
}

fn validate_cursor_text_fields(
    cursor: &SourceCheckpoint,
    watermark_field: &str,
    pending_watermark_field: &str,
) -> Result<(), String> {
    validate_text(cursor.watermark.as_deref(), watermark_field)?;
    validate_text(cursor.pending_watermark.as_deref(), pending_watermark_field)
}

fn validate_previous_checkpoint(batch: &SourceIngestionBatch) -> Result<(), String> {
    let Some(previous) = &batch.expected_previous_checkpoint else {
        return Ok(());
    };
    if previous.stream != batch.provider_checkpoint.stream {
        return Err("previous checkpoint stream must match the submitted checkpoint".into());
    }
    validate_cursor(previous, "previous checkpoint")?;
    if previous.digest()? == batch.provider_checkpoint.digest()? {
        return Err("provider checkpoint must differ from expected_previous_checkpoint".into());
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
    if record.stream != batch.provider_checkpoint.stream {
        return Err("every source record stream must match the provider checkpoint".into());
    }
    if record.provenance.connector != batch.connector {
        return Err("source record provenance connector must match the batch connector".into());
    }
    validate_required_text(&record.record_id, "source record id")?;
    validate_mapping_reference(&record.mapping_reference)?;
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

fn validate_relationships(batch: &SourceIngestionBatch) -> Result<(), String> {
    let mut endpoint_pairs = BTreeSet::new();
    for relationship in &batch.relationships {
        validate_mapping_reference(&relationship.relation_reference)?;
        validate_entity_ref(&relationship.source, &batch.provider_checkpoint.stream)?;
        validate_entity_ref(&relationship.target, &batch.provider_checkpoint.stream)?;
        if relationship.source == relationship.target {
            return Err("source relationship endpoints must differ".into());
        }
        if relationship.provenance.connector != batch.connector {
            return Err(
                "source relationship provenance connector must match the batch connector".into(),
            );
        }
        validate_required_text(
            &relationship.provenance.server,
            "relationship provenance server",
        )?;
        validate_required_text(
            &relationship.provenance.tool,
            "relationship provenance tool",
        )?;
        validate_required_text(
            &relationship.provenance.source_uri,
            "relationship provenance source_uri",
        )?;
        if relationship
            .properties
            .as_ref()
            .is_some_and(|properties| !properties.value().is_ok_and(|value| value.is_object()))
        {
            return Err("source relationship properties must be a JSON object".into());
        }
        if !endpoint_pairs.insert((&relationship.source, &relationship.target)) {
            return Err("source ingestion batch contains duplicate relationship endpoints".into());
        }
    }
    Ok(())
}

fn validate_reconciliation(batch: &SourceIngestionBatch) -> Result<(), String> {
    validate_mode_contract(batch)?;
    let live = validated_live_ids(batch)?;
    let withdrawals = validated_withdrawals(batch, &live)?;
    validate_reconciliation_membership(batch, &live, &withdrawals)
}

fn validate_mode_contract(batch: &SourceIngestionBatch) -> Result<(), String> {
    match batch.mode {
        SourceIngestionMode::Full => validate_full_mode(batch),
        SourceIngestionMode::Delta => validate_delta_mode(batch),
        SourceIngestionMode::Reconcile => validate_reconcile_mode(batch),
    }
}

fn validate_full_mode(batch: &SourceIngestionBatch) -> Result<(), String> {
    if batch.authoritative_live_ids.is_some() || !batch.withdrawals.is_empty() {
        return Err(
            "full ingestion is non-authoritative and cannot declare reconciliation state".into(),
        );
    }
    let empty = batch.records.is_empty() && batch.relationships.is_empty();
    match (empty, batch.empty_authoritative_approval.as_deref()) {
        (true, Some(approval)) => validate_required_text(approval, "empty authoritative approval"),
        (true, None) => {
            Err("empty full ingestion requires a governed authoritative-empty approval".into())
        }
        (false, Some(_)) => {
            Err("empty authoritative approval is valid only for an empty full snapshot".into())
        }
        (false, None) => Ok(()),
    }
}

fn validate_delta_mode(batch: &SourceIngestionBatch) -> Result<(), String> {
    if batch.authoritative_live_ids.is_some() || batch.empty_authoritative_approval.is_some() {
        Err("delta ingestion cannot declare an authoritative live set".into())
    } else {
        Ok(())
    }
}

fn validate_reconcile_mode(batch: &SourceIngestionBatch) -> Result<(), String> {
    if !batch.withdrawals.is_empty() {
        return Err("reconcile ingestion derives tombstones and forbids caller withdrawals".into());
    }
    let live = batch
        .authoritative_live_ids
        .as_ref()
        .ok_or_else(|| "reconcile ingestion requires authoritative_live_ids".to_string())?;
    match (
        live.is_empty(),
        batch.empty_authoritative_approval.as_deref(),
    ) {
        (true, Some(approval)) => validate_required_text(approval, "empty authoritative approval"),
        (true, None) => {
            Err("empty authoritative reconciliation requires a governed approval".into())
        }
        (false, Some(_)) => {
            Err("empty authoritative approval is valid only for an empty live set".into())
        }
        (false, None) => Ok(()),
    }
}

fn validated_live_ids(batch: &SourceIngestionBatch) -> Result<BTreeSet<SourceEntityRef>, String> {
    let mut live = BTreeSet::new();
    for entity in batch.authoritative_live_ids.iter().flatten() {
        validate_entity_ref(entity, &batch.provider_checkpoint.stream)?;
        if !live.insert(entity.clone()) {
            return Err("authoritative live ids must be unique".into());
        }
    }
    Ok(live)
}

fn validated_withdrawals(
    batch: &SourceIngestionBatch,
    live: &BTreeSet<SourceEntityRef>,
) -> Result<BTreeSet<SourceEntityRef>, String> {
    let mut withdrawals = BTreeSet::new();
    for withdrawal in &batch.withdrawals {
        validate_entity_ref(&withdrawal.entity, &batch.provider_checkpoint.stream)?;
        validate_required_text(&withdrawal.reason, "source withdrawal reason")?;
        if !withdrawals.insert(withdrawal.entity.clone()) {
            return Err("source withdrawals must be unique".into());
        }
        if live.contains(&withdrawal.entity) {
            return Err("a source entity cannot be both live and withdrawn".into());
        }
    }
    Ok(withdrawals)
}

fn validate_reconciliation_membership(
    batch: &SourceIngestionBatch,
    live: &BTreeSet<SourceEntityRef>,
    withdrawals: &BTreeSet<SourceEntityRef>,
) -> Result<(), String> {
    let observed: BTreeSet<_> = batch
        .records
        .iter()
        .map(|record| SourceEntityRef {
            stream: record.stream.clone(),
            record_id: record.record_id.clone(),
        })
        .collect();
    if observed.iter().any(|entity| withdrawals.contains(entity)) {
        return Err("a source entity cannot be both observed and withdrawn".into());
    }
    if batch.relationships.iter().any(|relationship| {
        withdrawals.contains(&relationship.source) || withdrawals.contains(&relationship.target)
    }) {
        return Err("a source relationship cannot reference a withdrawn entity".into());
    }
    if batch.mode == SourceIngestionMode::Reconcile {
        if observed.iter().any(|entity| !live.contains(entity)) {
            return Err("every reconciled source record must be in authoritative_live_ids".into());
        }
        if batch.relationships.iter().any(|relationship| {
            !live.contains(&relationship.source) || !live.contains(&relationship.target)
        }) {
            return Err("reconciled relationship endpoints must be authoritative live ids".into());
        }
    }
    Ok(())
}

fn validate_entity_ref(entity: &SourceEntityRef, stream: &ResourceId) -> Result<(), String> {
    if &entity.stream != stream {
        return Err("source entity stream must match the provider checkpoint".into());
    }
    validate_required_text(&entity.record_id, "source entity record id")
}

fn validate_mapping_reference(reference: &str) -> Result<(), String> {
    if reference.is_empty() || reference.len() > MAX_MAPPING_REFERENCE_BYTES {
        return Err("source ingestion requires bounded mapping references".into());
    }
    Ok(())
}

fn validate_cursor(cursor: &SourceCheckpoint, field: &str) -> Result<(), String> {
    cursor
        .position
        .value()
        .map(|_| ())
        .map_err(|error| format!("{field} position is invalid: {error}"))
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
