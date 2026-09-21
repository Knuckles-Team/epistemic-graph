//! RF-ADR-009 native raw-source ingestion preparation.
//!
//! This module owns CaptureRaw -> AdmitRaw -> ValidateBatch -> MapBatch.  The
//! existing ChangeEnvelope commit authority owns the final IngestBatch step, so
//! there is one graph mutation path and one cursor compare-and-swap authority.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use eg_types::contract::Digest256;
use eg_types::source_ingestion::{
    RawAdmissionReceipt, SourceIngestionDisposition, SourceIngestionReceipt, SourceIngestionRequest,
};
use tokio::sync::RwLock;

use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::CarrierAuthority;
use crate::server::auth::VerifiedRequestContext;
use crate::server::ServerState;

pub(crate) struct PrepareContext<'a> {
    pub state: &'a Arc<RwLock<ServerState>>,
    pub request_id: u64,
    pub graph_name: &'a str,
    pub tenant_scope: &'a str,
    pub verified: &'a VerifiedRequestContext,
    pub graph_version: u64,
    pub placement_epoch: u64,
    pub fencing_token: Option<u64>,
}

pub(crate) struct PreparedSourceIngestion {
    pub envelope: eg_types::change_envelope::ChangeEnvelope,
    batch_digest: Digest256,
    mapping_reference: String,
    mapping_digest: Digest256,
    connector_pack_digest: Digest256,
    catalog: eg_types::connector_pack::McpCatalogSnapshotBinding,
    raw_admissions: Vec<RawAdmissionReceipt>,
    accepted_cursor: eg_types::source_ingestion::SourceCursor,
    affected_count: u64,
    committed_graph_version: u64,
}

#[cfg(not(all(feature = "redb", feature = "blob")))]
pub(crate) async fn prepare(
    _ctx: PrepareContext<'_>,
    _request: SourceIngestionRequest,
) -> Result<PreparedSourceIngestion, String> {
    Err(
        "SOURCE_INGESTION_UNAVAILABLE: native source ingestion requires redb and blob support"
            .into(),
    )
}

#[cfg(all(feature = "redb", feature = "blob"))]
pub(crate) async fn prepare(
    ctx: PrepareContext<'_>,
    request: SourceIngestionRequest,
) -> Result<PreparedSourceIngestion, String> {
    if !ctx.verified.allows_method("source:ingest", true) {
        return Err("ACCESS_DENIED: source ingestion requires source:ingest".into());
    }
    let batch = request.as_batch();
    let batch_digest = request.batch_digest()?;
    let batch_id = format!("source-ingest:{}", batch_digest.to_hex());
    let authority = CarrierAuthority::from_verified(ctx.verified)?;
    let persistence = load_persistence(&ctx).await?;
    let graph_fname = crate::persist::sanitize(ctx.graph_name);
    if let Some(record) = persistence
        .read_change_envelope(&graph_fname, &batch_id)
        .await?
    {
        return prepared_replay(ctx, batch, batch_digest, record.envelope);
    }
    let resources = load_resources(&ctx).await?;
    let resolved = resolve_mapping(&ctx, batch, &resources)?;
    let mapping_digest = mapping_digest(&resolved.schema_mapping)?;
    let policy_subject = policy_subject(ctx.tenant_scope)?;
    let mut admitted = admit_records(
        &ctx,
        batch,
        &authority,
        &resources.blob,
        &resolved,
        &policy_subject,
    )?;
    admitted.methods.push(receipt_method(
        batch,
        &batch_id,
        &mapping_digest,
        &resolved,
    )?);
    admitted.policies.push(policy(
        &batch_id,
        ctx.tenant_scope,
        &policy_subject,
        resolved.entry_revision,
    ));
    let mutation = compile_mutation(&ctx, &batch_id, admitted.methods)?;
    let envelope = build_envelope(
        batch,
        &batch_id,
        batch_digest,
        mutation,
        admitted.blobs,
        admitted.policies,
        admitted.lineage,
        resolved.entry_revision,
    )?;
    Ok(PreparedSourceIngestion {
        envelope,
        batch_digest,
        mapping_reference: resolved.mapping_reference,
        mapping_digest,
        connector_pack_digest: resolved.pack_digest,
        catalog: resolved.catalog,
        raw_admissions: admitted.raw_admissions,
        accepted_cursor: batch.cursor.clone(),
        affected_count: batch.records.len() as u64,
        committed_graph_version: ctx.graph_version.saturating_add(1),
    })
}

#[cfg(all(feature = "redb", feature = "blob"))]
struct IngestionResources {
    agent_library: Arc<crate::server::persistence::agent_library::AgentLibraryStore>,
    blob: Arc<crate::server::blob::BlobCursors>,
}

#[cfg(all(feature = "redb", feature = "blob"))]
struct AdmittedRecords {
    raw_admissions: Vec<RawAdmissionReceipt>,
    blobs: BTreeMap<String, eg_types::change_envelope::BlobReference>,
    methods: Vec<Method>,
    lineage: Vec<eg_types::change_envelope::LineageRecord>,
    policies: Vec<eg_types::change_envelope::PolicyRecord>,
}

#[cfg(all(feature = "redb", feature = "blob"))]
async fn load_persistence(
    ctx: &PrepareContext<'_>,
) -> Result<Arc<dyn crate::server::persistence::PersistenceBackend>, String> {
    let state = ctx.state.read().await;
    state.persistence.clone().ok_or_else(|| {
        "SOURCE_INGESTION_UNAVAILABLE: graph persistence is not configured".to_string()
    })
}

#[cfg(all(feature = "redb", feature = "blob"))]
async fn load_resources(ctx: &PrepareContext<'_>) -> Result<IngestionResources, String> {
    let state = ctx.state.read().await;
    Ok(IngestionResources {
        agent_library: state.agent_library.clone().ok_or_else(|| {
            "SOURCE_INGESTION_UNAVAILABLE: Agent Library is not configured".to_string()
        })?,
        blob: state
            .blob
            .clone()
            .ok_or_else(|| "SOURCE_INGESTION_UNAVAILABLE: raw CAS is not configured".to_string())?,
    })
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn resolve_mapping(
    ctx: &PrepareContext<'_>,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    resources: &IngestionResources,
) -> Result<crate::server::persistence::connector_pack::ResolvedConnectorSchemaMapping, String> {
    // The request has no tenant or mapping body. Both are resolved exclusively
    // through verified server authority, and every resolver error is terminal.
    resources.agent_library.resolve_connector_schema_mapping(
        ctx.tenant_scope,
        &batch.connector,
        &batch.mapping_reference,
    )
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn policy_subject(tenant: &str) -> Result<String, String> {
    Digest256::framed(
        b"eg/source-ingestion-policy-subject/v1",
        &[tenant.as_bytes()],
    )
    .map(|digest| digest.to_hex())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn admit_records(
    ctx: &PrepareContext<'_>,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    authority: &CarrierAuthority,
    blob: &Arc<crate::server::blob::BlobCursors>,
    resolved: &crate::server::persistence::connector_pack::ResolvedConnectorSchemaMapping,
    policy_subject: &str,
) -> Result<AdmittedRecords, String> {
    let mut admitted = AdmittedRecords {
        raw_admissions: Vec::with_capacity(batch.records.len()),
        blobs: BTreeMap::new(),
        methods: Vec::with_capacity(batch.records.len() + 1),
        lineage: Vec::with_capacity(batch.records.len()),
        policies: Vec::with_capacity(batch.records.len() + 1),
    };
    let mut batch_raw_digests = BTreeSet::new();
    for record in &batch.records {
        let item = admit_record(
            ctx,
            batch,
            record,
            authority,
            blob,
            resolved,
            policy_subject,
        )?;
        let deduplicated = !batch_raw_digests.insert(item.raw_digest_hex.clone());
        admitted.raw_admissions.push(RawAdmissionReceipt {
            stream: record.stream.clone(),
            record_id: record.record_id.clone(),
            raw_digest: item.raw_digest.clone(),
            // Stable receipt semantics: this reports sharing within the
            // submitted batch. Cross-batch CAS reuse is a storage observation,
            // not mutable terminal receipt content.
            deduplicated,
        });
        admitted
            .blobs
            .entry(item.raw_digest_hex)
            .or_insert(item.blob);
        admitted.methods.push(item.method);
        admitted.lineage.push(item.lineage);
        admitted.policies.push(item.policy);
    }
    Ok(admitted)
}

#[cfg(all(feature = "redb", feature = "blob"))]
struct AdmittedRecord {
    raw_digest_hex: String,
    raw_digest: Digest256,
    blob: eg_types::change_envelope::BlobReference,
    method: Method,
    lineage: eg_types::change_envelope::LineageRecord,
    policy: eg_types::change_envelope::PolicyRecord,
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn admit_record(
    ctx: &PrepareContext<'_>,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    record: &eg_types::source_ingestion::SourceRecord,
    authority: &CarrierAuthority,
    blob: &Arc<crate::server::blob::BlobCursors>,
    resolved: &crate::server::persistence::connector_pack::ResolvedConnectorSchemaMapping,
    policy_subject: &str,
) -> Result<AdmittedRecord, String> {
    let raw = rmp_serde::to_vec_named(record)
        .map_err(|error| format!("raw source record encoding failed: {error}"))?;
    let raw_digest_hex = admit_raw(ctx, authority, blob, &raw)?;
    let raw_digest = Digest256::parse(&raw_digest_hex)?;
    let node_id = source_node_id(ctx.tenant_scope, batch.connector.as_str(), record)?;
    let properties = mapped_properties(
        record,
        &resolved.schema_mapping.ontology_class,
        &resolved.schema_mapping.fields,
        &batch.mapping_reference,
        &raw_digest_hex,
    )?;
    let method = Method::AddNode {
        node_id: node_id.clone(),
        properties_msgpack: rmp_serde::to_vec_named(&properties)
            .map_err(|error| format!("mapped source properties encoding failed: {error}"))?,
    };
    let lineage = eg_types::change_envelope::LineageRecord {
        lineage_id: format!("lineage:{}", short_digest(&node_id)?),
        operation: eg_types::change_envelope::MaterialOperation::Upsert,
        object_id: node_id.clone(),
        source_artifact_digest: raw_digest_hex.clone(),
        transform_name: "connector-manifest-schema-mapping".into(),
        transform_version: resolved.body_sha256.to_hex(),
        parent_content_digests: Vec::new(),
    };
    let policy = policy(
        &node_id,
        ctx.tenant_scope,
        policy_subject,
        resolved.entry_revision,
    );
    Ok(AdmittedRecord {
        raw_digest_hex: raw_digest_hex.clone(),
        raw_digest,
        blob: raw_blob_reference(&raw_digest_hex, raw.len()),
        method,
        lineage,
        policy,
    })
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn admit_raw(
    ctx: &PrepareContext<'_>,
    authority: &CarrierAuthority,
    blob: &Arc<crate::server::blob::BlobCursors>,
    raw: &[u8],
) -> Result<String, String> {
    let (raw_digest_hex, _was_new) = blob.store.put_chunk(raw)?;
    // Admission precedes mapping/graph commit by design. The owner-scoped
    // holder is set-like, so transport retries do not leak refcounts. The
    // holder change and its native receipt commit in one blob-owner WTX.
    let holder = crate::server::blob::store::HolderChange::owner_acquire(
        &raw_digest_hex,
        authority.owner_scope(),
    )?;
    let blob_method = Method::BlobRef {
        digest: raw_digest_hex.clone(),
    };
    let (holder_batch, admitted_at_ms) = crate::server::handlers::blob::compile_blob_batch(
        blob.store.as_ref(),
        ctx.request_id,
        authority,
        &blob_method,
    )?;
    blob.store
        .holder_batch(&holder, &holder_batch, admitted_at_ms)?;
    Ok(raw_digest_hex)
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn raw_blob_reference(raw_digest: &str, length: usize) -> eg_types::change_envelope::BlobReference {
    eg_types::change_envelope::BlobReference {
        blob_id: format!("raw:{raw_digest}"),
        operation: eg_types::change_envelope::MaterialOperation::Upsert,
        digest_algorithm: "sha256".into(),
        digest: raw_digest.into(),
        media_type: "application/vnd.epistemic-graph.source-record+msgpack".into(),
        length: length as u64,
    }
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn receipt_method(
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    batch_id: &str,
    mapping_digest: &Digest256,
    resolved: &crate::server::persistence::connector_pack::ResolvedConnectorSchemaMapping,
) -> Result<Method, String> {
    let properties = serde_json::json!({
        "type": "SourceIngestionReceipt",
        "connector": batch.connector.as_str(),
        "mapping_reference": batch.mapping_reference,
        "mapping_digest": mapping_digest.to_hex(),
        "connector_pack_digest": resolved.pack_digest.to_hex(),
        "configuration_revision": resolved.catalog.configuration_revision,
        "catalog_generation": resolved.catalog.catalog_generation,
        "catalog_snapshot_digest": resolved.catalog.snapshot_digest.to_hex(),
        "child_connection_generation": resolved.catalog.child_connection_generation,
        "authorization_scope_digest": resolved.catalog.authorization_scope_digest.to_hex(),
        "cursor_digest": batch.cursor.digest()?.to_hex(),
        "record_count": batch.records.len(),
    });
    Ok(Method::AddNode {
        node_id: batch_id.into(),
        properties_msgpack: rmp_serde::to_vec_named(&properties)
            .map_err(|error| format!("source ingestion receipt encoding failed: {error}"))?,
    })
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn compile_mutation(
    ctx: &PrepareContext<'_>,
    batch_id: &str,
    methods: Vec<Method>,
) -> Result<eg_types::mutation_batch::MutationBatch, String> {
    let principal = ctx.verified.principal_persistence_id();
    crate::server::mutation_batch::compile_methods(
        crate::server::mutation_batch::CompileBatch {
            batch_id,
            request_id: ctx.request_id,
            attempt_nonce: ctx.verified.attempt_nonce(),
            principal: Some(&principal),
            tenant: ctx.tenant_scope,
            graph: ctx.graph_name,
            placement_epoch: ctx.placement_epoch,
            idempotency_key: ctx.verified.idempotency_key(),
            expected_graph_version: Some(ctx.graph_version),
            fencing_token: ctx.fencing_token,
            created_at_ms: crate::server::dispatch::authoritative_now_ms(),
            default_surface: crate::mutation_batch::MutationSurface::Graph,
            authoritative_state: None,
        },
        methods,
    )
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn build_envelope(
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    batch_id: &str,
    batch_digest: Digest256,
    mutation: eg_types::mutation_batch::MutationBatch,
    blobs: BTreeMap<String, eg_types::change_envelope::BlobReference>,
    policies: Vec<eg_types::change_envelope::PolicyRecord>,
    lineage: Vec<eg_types::change_envelope::LineageRecord>,
    mapping_revision: u64,
) -> Result<eg_types::change_envelope::ChangeEnvelope, String> {
    let cursor_digest = batch.cursor.digest()?;
    let expected_previous = batch
        .expected_previous_cursor
        .as_ref()
        .map(|cursor| cursor.digest())
        .transpose()?;
    let envelope = eg_types::change_envelope::ChangeEnvelope {
        schema_version: eg_types::change_envelope::CHANGE_ENVELOPE_VERSION,
        envelope_id: batch_id.into(),
        mutation,
        content_version: eg_types::change_envelope::ContentVersion {
            object_id: batch_id.into(),
            digest_algorithm: "sha256".into(),
            digest: batch_digest.to_hex(),
            previous_digest: expected_previous.map(Digest256::to_hex),
            source_version: eg_types::change_envelope::ContentVersionPosition::Opaque {
                version_type: "source-cursor-sha256".into(),
                value: cursor_digest.to_hex(),
            },
        },
        cursor: Some(eg_types::change_envelope::ChangeCursor {
            source: format!(
                "{}:{}",
                batch.connector.as_str(),
                batch.cursor.stream.as_str()
            ),
            partition: String::new(),
            position: eg_types::change_envelope::CursorPosition::Opaque {
                cursor_type: "source-cursor-sha256".into(),
                value: cursor_digest.to_hex(),
            },
            expected_previous: expected_previous.map(|digest| {
                eg_types::change_envelope::CursorPosition::Opaque {
                    cursor_type: "source-cursor-sha256".into(),
                    value: digest.to_hex(),
                }
            }),
        }),
        blobs: blobs.into_values().collect(),
        features: Vec::new(),
        evidence: Vec::new(),
        policies,
        lineage,
        privacy: eg_types::change_envelope::PrivacyAttestation {
            policy_version: format!("connector-pack-revision-{mapping_revision}"),
            sanitizer_version: "source-ingestion-v1".into(),
            sanitized_payload_digest: batch_digest.to_hex(),
        },
        commit_seq: None,
        commit_descriptor_ref: None,
    };
    envelope.validate()?;
    Ok(envelope)
}

pub(crate) fn finish(prepared: PreparedSourceIngestion, response: Response) -> Response {
    if let Some(error) = response.error {
        return Response::err(response.id, error);
    }
    let applied = match response.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice::<
            eg_types::result_contract::transactions::ChangeEnvelopeApplied,
        >(&bytes)
        .map_err(|error| format!("invalid native ingestion commit result: {error}")),
        _ => Err("native ingestion commit returned no typed result".into()),
    };
    let applied = match applied {
        Ok(applied) => applied,
        Err(error) => return Response::err(response.id, error),
    };
    let disposition = if applied.commit.replayed {
        SourceIngestionDisposition::Replayed
    } else {
        SourceIngestionDisposition::Committed
    };
    let accepted_cursor_digest = match prepared.accepted_cursor.digest() {
        Ok(digest) => digest,
        Err(error) => return Response::err(response.id, error),
    };
    let raw_admissions = match eg_types::contract::BoundedVec::new(prepared.raw_admissions) {
        Ok(receipts) => receipts,
        Err(error) => return Response::err(response.id, error),
    };
    let raw_receipt_bytes = match rmp_serde::to_vec_named(&raw_admissions) {
        Ok(bytes) => bytes,
        Err(error) => {
            return Response::err(
                response.id,
                format!("source ingestion receipt encoding failed: {error}"),
            )
        }
    };
    let receipt_digest = match Digest256::framed(
        b"eg/source-ingestion-receipt/v1",
        &[
            prepared.batch_digest.as_bytes(),
            prepared.mapping_reference.as_bytes(),
            prepared.mapping_digest.as_bytes(),
            prepared.connector_pack_digest.as_bytes(),
            &prepared.catalog.configuration_revision.to_be_bytes(),
            &prepared.catalog.catalog_generation.to_be_bytes(),
            prepared.catalog.snapshot_digest.as_bytes(),
            &prepared.catalog.child_connection_generation.to_be_bytes(),
            prepared.catalog.authorization_scope_digest.as_bytes(),
            &raw_receipt_bytes,
            accepted_cursor_digest.as_bytes(),
            &prepared.affected_count.to_be_bytes(),
            prepared.committed_graph_version.to_be_bytes().as_slice(),
        ],
    ) {
        Ok(digest) => digest,
        Err(error) => return Response::err(response.id, error),
    };
    Response::ok(
        response.id,
        ResultPayload::of::<eg_types::result_contract::ingestion::SourceIngest>(
            SourceIngestionReceipt {
                disposition,
                batch_digest: prepared.batch_digest,
                mapping_reference: prepared.mapping_reference,
                mapping_digest: prepared.mapping_digest,
                connector_pack_digest: prepared.connector_pack_digest,
                catalog: prepared.catalog,
                raw_admissions,
                accepted_cursor: prepared.accepted_cursor,
                accepted_cursor_digest,
                affected_count: prepared.affected_count,
                committed_graph_version: prepared.committed_graph_version,
                receipt_digest,
            },
        ),
    )
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn prepared_replay(
    ctx: PrepareContext<'_>,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    batch_digest: Digest256,
    envelope: eg_types::change_envelope::ChangeEnvelope,
) -> Result<PreparedSourceIngestion, String> {
    validate_replay_authority(&ctx, batch_digest, &envelope)?;
    let batch_id = format!("source-ingest:{}", batch_digest.to_hex());
    let receipt = replay_receipt(&envelope, &batch_id)?;
    let (mapping_digest, connector_pack_digest, catalog) = replay_metadata(batch, &receipt)?;
    let raw_admissions = replay_admissions(&ctx, batch, &envelope)?;
    let committed_graph_version = replay_graph_version(&envelope)?;
    Ok(PreparedSourceIngestion {
        envelope,
        batch_digest,
        mapping_reference: batch.mapping_reference.clone(),
        mapping_digest,
        connector_pack_digest,
        catalog,
        raw_admissions,
        accepted_cursor: batch.cursor.clone(),
        affected_count: batch.records.len() as u64,
        committed_graph_version,
    })
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn validate_replay_authority(
    ctx: &PrepareContext<'_>,
    batch_digest: Digest256,
    envelope: &eg_types::change_envelope::ChangeEnvelope,
) -> Result<(), String> {
    let graph_name = envelope
        .mutation
        .identity
        .scope()
        .graph_name()
        .map(|name| name.as_str());
    if envelope.content_version.digest != batch_digest.to_hex()
        || envelope.mutation.identity.tenant().as_str() != ctx.tenant_scope
        || graph_name != Some(ctx.graph_name)
    {
        return Err(
            "SOURCE_INGESTION_REPLAY_CONFLICT: stored envelope authority or digest differs".into(),
        );
    }
    Ok(())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn replay_receipt(
    envelope: &eg_types::change_envelope::ChangeEnvelope,
    batch_id: &str,
) -> Result<serde_json::Value, String> {
    let properties = envelope
        .mutation
        .operations
        .iter()
        .find_map(|operation| match &operation.method {
            Method::AddNode {
                node_id,
                properties_msgpack,
            } if node_id == batch_id => Some(properties_msgpack),
            _ => None,
        })
        .ok_or_else(|| "SOURCE_INGESTION_REPLAY_INVALID: receipt node is missing".to_string())?;
    rmp_serde::from_slice(properties)
        .map_err(|error| format!("SOURCE_INGESTION_REPLAY_INVALID: {error}"))
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn replay_metadata(
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    receipt: &serde_json::Value,
) -> Result<
    (
        Digest256,
        Digest256,
        eg_types::connector_pack::McpCatalogSnapshotBinding,
    ),
    String,
> {
    if replay_receipt_text(receipt, "mapping_reference")? != batch.mapping_reference {
        return Err("SOURCE_INGESTION_REPLAY_CONFLICT: mapping reference differs".into());
    }
    let mapping_digest = Digest256::parse(replay_receipt_text(receipt, "mapping_digest")?)?;
    let connector_pack_digest =
        Digest256::parse(replay_receipt_text(receipt, "connector_pack_digest")?)?;
    let cursor_digest = batch.cursor.digest()?;
    if replay_receipt_text(receipt, "cursor_digest")? != cursor_digest.to_hex() {
        return Err("SOURCE_INGESTION_REPLAY_CONFLICT: cursor differs".into());
    }
    let catalog = eg_types::connector_pack::McpCatalogSnapshotBinding {
        configuration_revision: replay_receipt_u64(receipt, "configuration_revision")?,
        catalog_generation: replay_receipt_u64(receipt, "catalog_generation")?,
        snapshot_digest: Digest256::parse(replay_receipt_text(
            receipt,
            "catalog_snapshot_digest",
        )?)?,
        child_connection_generation: replay_receipt_u64(receipt, "child_connection_generation")?,
        authorization_scope_digest: Digest256::parse(replay_receipt_text(
            receipt,
            "authorization_scope_digest",
        )?)?,
    };
    Ok((mapping_digest, connector_pack_digest, catalog))
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn replay_admissions(
    ctx: &PrepareContext<'_>,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    envelope: &eg_types::change_envelope::ChangeEnvelope,
) -> Result<Vec<RawAdmissionReceipt>, String> {
    let mut batch_raw_digests = BTreeSet::new();
    let mut raw_admissions = Vec::with_capacity(batch.records.len());
    for record in &batch.records {
        raw_admissions.push(replay_admission(
            ctx,
            batch,
            record,
            envelope,
            &mut batch_raw_digests,
        )?);
    }
    Ok(raw_admissions)
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn replay_admission(
    ctx: &PrepareContext<'_>,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    record: &eg_types::source_ingestion::SourceRecord,
    envelope: &eg_types::change_envelope::ChangeEnvelope,
    batch_raw_digests: &mut BTreeSet<String>,
) -> Result<RawAdmissionReceipt, String> {
    let node_id = source_node_id(ctx.tenant_scope, batch.connector.as_str(), record)?;
    let digest = envelope
        .lineage
        .iter()
        .find(|lineage| lineage.object_id == node_id)
        .map(|lineage| lineage.source_artifact_digest.as_str())
        .ok_or_else(|| "SOURCE_INGESTION_REPLAY_INVALID: source lineage is missing".to_string())?;
    let raw_digest = Digest256::parse(digest)?;
    Ok(RawAdmissionReceipt {
        stream: record.stream.clone(),
        record_id: record.record_id.clone(),
        raw_digest,
        deduplicated: !batch_raw_digests.insert(digest.to_string()),
    })
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn replay_graph_version(
    envelope: &eg_types::change_envelope::ChangeEnvelope,
) -> Result<u64, String> {
    match envelope.mutation.version_expectation {
        crate::mutation_batch::VersionExpectation::Graph(version) => version
            .checked_add(1)
            .ok_or_else(|| "source ingestion graph version overflow".to_string()),
        _ => Err("SOURCE_INGESTION_REPLAY_INVALID: graph version expectation is missing".into()),
    }
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn replay_receipt_text<'a>(receipt: &'a serde_json::Value, field: &str) -> Result<&'a str, String> {
    receipt
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("SOURCE_INGESTION_REPLAY_INVALID: {field} is missing"))
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn replay_receipt_u64(receipt: &serde_json::Value, field: &str) -> Result<u64, String> {
    receipt
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("SOURCE_INGESTION_REPLAY_INVALID: {field} is missing"))
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn mapping_digest(
    mapping: &eg_types::connector_pack::ConnectorSchemaMapping,
) -> Result<Digest256, String> {
    let bytes = rmp_serde::to_vec_named(mapping)
        .map_err(|error| format!("connector schema mapping encoding failed: {error}"))?;
    Digest256::framed(b"eg/connector-schema-mapping/v1", &[&bytes])
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn mapped_properties(
    record: &eg_types::source_ingestion::SourceRecord,
    ontology_class: &str,
    fields: &BTreeMap<String, String>,
    mapping_reference: &str,
    raw_digest: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    if ontology_class.is_empty() {
        return Err("CONNECTOR_SCHEMA_MAPPING_INVALID: ontology_class is empty".into());
    }
    let payload = record.payload.value()?;
    let source = payload
        .as_object()
        .ok_or_else(|| "source record payload must be an object".to_string())?;
    let mut mapped = serde_json::Map::new();
    mapped.insert("type".into(), ontology_class.into());
    let mut targets: BTreeSet<String> = [
        "type",
        "source_record_id",
        "source_stream",
        "source_mapping_reference",
        "source_raw_sha256",
        "source_provenance",
        "source_updated_at",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    map_configured_fields(source, fields, &mut targets, &mut mapped)?;
    insert_source_metadata(&mut mapped, record, mapping_reference, raw_digest)?;
    Ok(mapped)
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn map_configured_fields(
    source: &serde_json::Map<String, serde_json::Value>,
    fields: &BTreeMap<String, String>,
    targets: &mut BTreeSet<String>,
    mapped: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    for (source_field, target_field) in fields {
        if source_field.is_empty()
            || target_field.is_empty()
            || !targets.insert(target_field.clone())
        {
            return Err(
                "CONNECTOR_SCHEMA_MAPPING_INVALID: field targets must be nonempty and unique"
                    .into(),
            );
        }
        let value = source.get(source_field).ok_or_else(|| {
            format!("SOURCE_MAPPING_FIELD_MISSING: required source field {source_field}")
        })?;
        mapped.insert(target_field.clone(), value.clone());
    }
    Ok(())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn insert_source_metadata(
    mapped: &mut serde_json::Map<String, serde_json::Value>,
    record: &eg_types::source_ingestion::SourceRecord,
    mapping_reference: &str,
    raw_digest: &str,
) -> Result<(), String> {
    mapped.insert("source_record_id".into(), record.record_id.clone().into());
    mapped.insert("source_stream".into(), record.stream.as_str().into());
    mapped.insert("source_mapping_reference".into(), mapping_reference.into());
    mapped.insert("source_raw_sha256".into(), raw_digest.into());
    mapped.insert(
        "source_provenance".into(),
        serde_json::to_value(&record.provenance)
            .map_err(|error| format!("source provenance encoding failed: {error}"))?,
    );
    if let Some(updated_at) = &record.updated_at {
        mapped.insert("source_updated_at".into(), updated_at.clone().into());
    }
    Ok(())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn source_node_id(
    tenant: &str,
    connector: &str,
    record: &eg_types::source_ingestion::SourceRecord,
) -> Result<String, String> {
    Ok(format!(
        "source:{}",
        Digest256::framed(
            b"eg/source-record-identity/v1",
            &[
                tenant.as_bytes(),
                connector.as_bytes(),
                record.stream.as_str().as_bytes(),
                record.record_id.as_bytes(),
            ],
        )?
        .to_hex()
    ))
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn short_digest(value: &str) -> Result<String, String> {
    Ok(Digest256::framed(b"eg/source-lineage-id/v1", &[value.as_bytes()])?.to_hex())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn policy(
    object_id: &str,
    tenant: &str,
    subject_set_digest: &str,
    revision: u64,
) -> eg_types::change_envelope::PolicyRecord {
    eg_types::change_envelope::PolicyRecord {
        policy_id: format!("policy:{}", object_id),
        operation: eg_types::change_envelope::MaterialOperation::Upsert,
        object_id: object_id.into(),
        tenant: tenant.into(),
        classification: "tenant-private".into(),
        policy_version: format!("connector-pack-revision-{revision}"),
        subject_set_digest: subject_set_digest.into(),
        retention_policy: "source-ingestion".into(),
        legal_hold: false,
    }
}

#[cfg(all(test, feature = "redb", feature = "blob"))]
mod tests {
    use super::*;
    use eg_types::contract::ResourceId;
    use eg_types::source_ingestion::{SourceJson, SourceRecord, SourceRecordProvenance};

    fn record() -> SourceRecord {
        SourceRecord {
            stream: ResourceId::new("items").unwrap(),
            record_id: "item-1".into(),
            payload: SourceJson::new(serde_json::json!({"id": 7, "name": "seven"})).unwrap(),
            updated_at: None,
            provenance: SourceRecordProvenance {
                connector: ResourceId::new("demo").unwrap(),
                adapter_kind: ResourceId::new("mcp").unwrap(),
                server: "server".into(),
                tool: "list_items".into(),
                tool_schema_sha256: Digest256::from_bytes([3; 32]),
                source_uri: "demo:items:item-1".into(),
            },
        }
    }

    #[test]
    fn mapping_is_manifest_owned_and_drops_unmapped_source_fields() {
        let mapping = BTreeMap::from([("name".to_string(), "label".to_string())]);
        let properties = mapped_properties(
            &record(),
            "Document",
            &mapping,
            "manifest:demo#schema_mappings/item",
            &"a".repeat(64),
        )
        .unwrap();
        assert_eq!(properties.get("type").unwrap(), "Document");
        assert_eq!(properties.get("label").unwrap(), "seven");
        assert!(!properties.contains_key("id"));
    }

    #[test]
    fn mapping_refuses_missing_fields_and_reserved_targets() {
        let missing = BTreeMap::from([("absent".to_string(), "label".to_string())]);
        assert!(mapped_properties(
            &record(),
            "Document",
            &missing,
            "manifest:demo#schema_mappings/item",
            &"a".repeat(64),
        )
        .unwrap_err()
        .contains("SOURCE_MAPPING_FIELD_MISSING"));

        let reserved = BTreeMap::from([("name".to_string(), "source_raw_sha256".to_string())]);
        assert!(mapped_properties(
            &record(),
            "Document",
            &reserved,
            "manifest:demo#schema_mappings/item",
            &"a".repeat(64),
        )
        .unwrap_err()
        .contains("targets"));
    }
}
