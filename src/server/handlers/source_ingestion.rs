//! RF-ADR-009 native raw-source ingestion preparation.
//!
//! This module owns CaptureRaw -> AdmitRaw -> ValidateBatch -> MapBatch.  The
//! existing ChangeEnvelope commit authority owns the final IngestBatch step, so
//! there is one graph mutation path and one cursor compare-and-swap authority.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use eg_types::contract::Digest256;
use eg_types::source_ingestion::{
    RawAdmissionReceipt, RawRelationshipAdmissionReceipt, SourceEntityRef,
    SourceIngestionDisposition, SourceIngestionMode, SourceIngestionReceipt,
    SourceIngestionRequest, SourceMappingKind, SourceMappingReceipt,
    SourceRelationshipTombstoneReceipt, SourceTombstoneReceipt, SourceWithdrawal,
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
    mode: SourceIngestionMode,
    mappings: Vec<SourceMappingReceipt>,
    raw_admissions: Vec<RawAdmissionReceipt>,
    relationship_raw_admissions: Vec<RawRelationshipAdmissionReceipt>,
    tombstones: Vec<SourceTombstoneReceipt>,
    relationship_tombstones: Vec<SourceRelationshipTombstoneReceipt>,
    accepted_checkpoint: eg_types::source_ingestion::SourceCheckpoint,
    live_set_digest: Option<Digest256>,
    relationship_live_set_digest: Option<Digest256>,
    affected_count: u64,
    relationship_count: u64,
    tombstoned_count: u64,
    relationship_tombstoned_count: u64,
    committed_graph_version: u64,
}

#[cfg(all(feature = "redb", feature = "blob"))]
pub(crate) async fn status(
    request_id: u64,
    graph_name: &str,
    tenant_scope: &str,
    verified: &VerifiedRequestContext,
    persistence: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    request: eg_types::source_ingestion::SourceIngestStatusRequest,
) -> Response {
    if !verified.allows_method("source:ingest", false) {
        return Response::err(
            request_id,
            "ACCESS_DENIED: source ingestion status requires source:ingest",
        );
    }
    let marker_id = match source_marker_id_for(tenant_scope, &request.connector, &request.stream) {
        Ok(marker_id) => marker_id,
        Err(error) => return Response::err(request_id, error),
    };
    let marker = match persistence
        .read_node(&crate::persist::sanitize(graph_name), &marker_id)
        .await
        .and_then(|bytes| {
            bytes
                .map(|bytes| {
                    rmp_serde::from_slice::<SourceLiveSetMarker>(&bytes)
                        .map_err(|error| format!("SOURCE_INGESTION_STATUS_INVALID: {error}"))
                })
                .transpose()
        }) {
        Ok(marker) => marker,
        Err(error) => return Response::err(request_id, error),
    };
    let body = if let Some(marker) = marker {
        eg_types::source_ingestion::SourceIngestStatus {
            connector: request.connector,
            stream: request.stream,
            content_hash: marker.checkpoint.content_hash,
            accepted_checkpoint: Some(marker.checkpoint),
            accepted_checkpoint_digest: Some(marker.checkpoint_digest),
            live_set_digest: marker.live_set_digest,
            relationship_live_set_digest: marker.relationship_live_set_digest,
            last_batch_digest: Some(marker.batch_digest),
            last_receipt_id: Some(marker.receipt_id),
            committed_graph_version: Some(marker.committed_graph_version),
        }
    } else {
        eg_types::source_ingestion::SourceIngestStatus {
            connector: request.connector.clone(),
            stream: request.stream.clone(),
            accepted_checkpoint: None,
            accepted_checkpoint_digest: None,
            content_hash: None,
            live_set_digest: None,
            relationship_live_set_digest: None,
            last_batch_digest: None,
            last_receipt_id: None,
            committed_graph_version: None,
        }
    };
    Response::ok(
        request_id,
        ResultPayload::of::<eg_types::result_contract::ingestion::SourceIngestStatus>(body),
    )
}

#[cfg(not(all(feature = "redb", feature = "blob")))]
pub(crate) async fn status(
    request_id: u64,
    _graph_name: &str,
    _tenant_scope: &str,
    _verified: &VerifiedRequestContext,
    _persistence: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    _request: eg_types::source_ingestion::SourceIngestStatusRequest,
) -> Response {
    Response::err(
        request_id,
        "SOURCE_INGESTION_UNAVAILABLE: status requires redb and blob support",
    )
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
    authorize_ingestion(&ctx)?;
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
    authorize_empty_reconciliation(&ctx, batch)?;
    let resolved = resolve_mappings(&ctx, batch, &resources)?;
    let reconciliation = reconcile_batch(
        &ctx,
        batch,
        batch_digest,
        &batch_id,
        persistence.as_ref(),
        &graph_fname,
        resolved.entry_revision,
    )
    .await?;
    let policy_subject = policy_subject(ctx.tenant_scope)?;
    let mut admitted = admit_records(
        &ctx,
        batch,
        &authority,
        &resources.blob,
        &resolved,
        &policy_subject,
    )?;
    admit_relationship_tombstones(
        &ctx,
        batch,
        &reconciliation.relationship_tombstones,
        &policy_subject,
        reconciliation.entry_revision,
        &mut admitted,
    )?;
    admit_relationships(
        RelationshipAdmissionContext {
            ctx: &ctx,
            batch,
            authority: &authority,
            blob: &resources.blob,
            resolved: &resolved,
            policy_subject: &policy_subject,
            persistence: persistence.as_ref(),
            graph_fname: &graph_fname,
        },
        &mut admitted,
    )?;
    admit_tombstones(
        &ctx,
        batch,
        &reconciliation.tombstones,
        &policy_subject,
        reconciliation.entry_revision,
        &mut admitted,
    )?;
    if let Some(marker) = reconciliation.marker.clone() {
        admitted.methods.push(marker);
    }
    admitted.policies.push(policy(
        &reconciliation.marker_id,
        ctx.tenant_scope,
        &policy_subject,
        reconciliation.entry_revision,
    ));
    admitted.methods.push(receipt_method(
        batch,
        &batch_id,
        &resolved,
        &reconciliation,
        &admitted.raw_admissions,
        &admitted.relationship_raw_admissions,
    )?);
    admitted.policies.push(policy(
        &batch_id,
        ctx.tenant_scope,
        &policy_subject,
        reconciliation.entry_revision,
    ));
    let mutation = compile_mutation(&ctx, &batch_id, admitted.methods)?;
    let envelope = build_envelope(
        batch,
        &batch_id,
        EnvelopeContents {
            batch_digest,
            mutation,
            blobs: admitted.blobs,
            policies: admitted.policies,
            lineage: admitted.lineage,
            mapping_revision: reconciliation.entry_revision,
            previous_batch_digest: reconciliation.previous_batch_digest,
        },
    )?;
    Ok(PreparedSourceIngestion {
        envelope,
        batch_digest,
        mode: batch.mode,
        mappings: resolved.receipts,
        raw_admissions: admitted.raw_admissions,
        relationship_raw_admissions: admitted.relationship_raw_admissions,
        tombstones: tombstone_receipts(&ctx, batch, &reconciliation.tombstones)?,
        relationship_tombstones: relationship_tombstone_receipts(
            &reconciliation.relationship_tombstones,
        ),
        accepted_checkpoint: batch.provider_checkpoint.clone(),
        live_set_digest: reconciliation.live_set_digest,
        relationship_live_set_digest: reconciliation.relationship_live_set_digest,
        affected_count: (batch.records.len()
            + batch.relationships.len()
            + admitted.tombstone_count
            + reconciliation.relationship_tombstones.len()) as u64,
        relationship_count: batch.relationships.len() as u64,
        tombstoned_count: admitted.tombstone_count as u64,
        relationship_tombstoned_count: reconciliation.relationship_tombstones.len() as u64,
        committed_graph_version: ctx.graph_version.saturating_add(1),
    })
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn authorize_ingestion(ctx: &PrepareContext<'_>) -> Result<(), String> {
    if ctx.verified.allows_method("source:ingest", true) {
        Ok(())
    } else {
        Err("ACCESS_DENIED: source ingestion requires source:ingest".into())
    }
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn authorize_empty_reconciliation(
    ctx: &PrepareContext<'_>,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
) -> Result<(), String> {
    let empty_live_set = batch
        .authoritative_live_ids
        .as_ref()
        .is_some_and(|ids| ids.is_empty())
        || is_authoritative_empty_full(batch);
    let allowed = ctx
        .verified
        .claims()
        .scopes
        .iter()
        .any(|scope| scope == "source:reconcile-empty");
    if empty_live_set && !allowed {
        Err("ACCESS_DENIED: empty reconciliation requires source:reconcile-empty".into())
    } else {
        Ok(())
    }
}

#[cfg(all(feature = "redb", feature = "blob"))]
struct IngestionResources {
    agent_library: Arc<crate::server::persistence::agent_library::AgentLibraryStore>,
    blob: Arc<crate::server::blob::BlobCursors>,
}

#[cfg(all(feature = "redb", feature = "blob"))]
struct AdmittedRecords {
    raw_admissions: Vec<RawAdmissionReceipt>,
    relationship_raw_admissions: Vec<RawRelationshipAdmissionReceipt>,
    blobs: BTreeMap<String, eg_types::change_envelope::BlobReference>,
    methods: Vec<Method>,
    lineage: Vec<eg_types::change_envelope::LineageRecord>,
    policies: Vec<eg_types::change_envelope::PolicyRecord>,
    tombstone_count: usize,
}

#[cfg(all(feature = "redb", feature = "blob"))]
struct ResolvedIngestionMappings {
    entities: BTreeMap<
        String,
        crate::server::persistence::connector_pack::ResolvedConnectorSchemaMapping,
    >,
    relationships: BTreeMap<
        String,
        crate::server::persistence::connector_pack::ResolvedConnectorRelationshipMapping,
    >,
    receipts: Vec<SourceMappingReceipt>,
    entry_revision: Option<u64>,
}

#[cfg(all(feature = "redb", feature = "blob"))]
struct ReconciliationPlan {
    tombstones: Vec<SourceWithdrawal>,
    relationship_tombstones: Vec<PlannedRelationshipTombstone>,
    live_set_digest: Option<Digest256>,
    relationship_live_set_digest: Option<Digest256>,
    marker: Option<Method>,
    marker_id: String,
    entry_revision: u64,
    previous_batch_digest: Option<Digest256>,
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
fn resolve_mappings(
    ctx: &PrepareContext<'_>,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    resources: &IngestionResources,
) -> Result<ResolvedIngestionMappings, String> {
    // The request has no tenant or mapping body. Both are resolved exclusively
    // through verified server authority, and every resolver error is terminal.
    let mut entities = BTreeMap::new();
    let mut relationships = BTreeMap::new();
    let mut receipts = Vec::new();
    let mut entry_revision = None;
    for record in &batch.records {
        if entities.contains_key(&record.mapping_reference) {
            continue;
        }
        let resolved = resources.agent_library.resolve_connector_schema_mapping(
            ctx.tenant_scope,
            &batch.connector,
            &record.mapping_reference,
        )?;
        bind_mapping_revision(&mut entry_revision, resolved.entry_revision)?;
        receipts.push(SourceMappingReceipt {
            kind: SourceMappingKind::Entity,
            mapping_reference: resolved.mapping_reference.clone(),
            mapping_digest: mapping_digest(&resolved.schema_mapping)?,
            connector_pack_digest: resolved.pack_digest,
            catalog: resolved.catalog.clone(),
        });
        entities.insert(record.mapping_reference.clone(), resolved);
    }
    for relationship in &batch.relationships {
        if relationships.contains_key(&relationship.relation_reference) {
            continue;
        }
        let resolved = resources
            .agent_library
            .resolve_connector_relationship_mapping(
                ctx.tenant_scope,
                &batch.connector,
                &relationship.relation_reference,
            )?;
        bind_mapping_revision(&mut entry_revision, resolved.entry_revision)?;
        receipts.push(SourceMappingReceipt {
            kind: SourceMappingKind::Relationship,
            mapping_reference: resolved.relation_reference.clone(),
            mapping_digest: relationship_mapping_digest(&resolved.relation)?,
            connector_pack_digest: resolved.pack_digest,
            catalog: resolved.catalog.clone(),
        });
        relationships.insert(relationship.relation_reference.clone(), resolved);
    }
    receipts.sort_by(|left, right| left.mapping_reference.cmp(&right.mapping_reference));
    if entry_revision.is_none() {
        entry_revision = Some(
            resources
                .agent_library
                .resolve_connector_ingestion_revision(ctx.tenant_scope, &batch.connector)?,
        );
    }
    Ok(ResolvedIngestionMappings {
        entities,
        relationships,
        receipts,
        entry_revision,
    })
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn bind_mapping_revision(current: &mut Option<u64>, revision: u64) -> Result<(), String> {
    if current.is_some_and(|current| current != revision) {
        return Err("STALE_MAPPING_REFERENCE: mappings resolve to different pack revisions".into());
    }
    *current = Some(revision);
    Ok(())
}

#[cfg(all(feature = "redb", feature = "blob"))]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceLiveSetMarker {
    #[serde(rename = "type")]
    node_type: String,
    connector: String,
    stream: String,
    live_ids: Vec<SourceEntityRef>,
    live_relationships: Vec<SourceLiveRelationship>,
    live_set_digest: Option<Digest256>,
    relationship_live_set_digest: Option<Digest256>,
    checkpoint: eg_types::source_ingestion::SourceCheckpoint,
    checkpoint_digest: Digest256,
    batch_digest: Digest256,
    receipt_id: String,
    committed_graph_version: u64,
    mapping_revision: u64,
}

#[cfg(all(feature = "redb", feature = "blob"))]
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceLiveRelationship {
    relationship_id: String,
    source: SourceEntityRef,
    target: SourceEntityRef,
}

#[cfg(all(feature = "redb", feature = "blob"))]
struct PlannedRelationshipTombstone {
    relationship: SourceLiveRelationship,
    reason: String,
}

#[cfg(all(feature = "redb", feature = "blob"))]
struct EntityReconciliation {
    live_ids: Vec<SourceEntityRef>,
    live_set_digest: Option<Digest256>,
    tombstones: Vec<SourceWithdrawal>,
}

#[cfg(all(feature = "redb", feature = "blob"))]
struct RelationshipReconciliation {
    live_relationships: Vec<SourceLiveRelationship>,
    live_set_digest: Option<Digest256>,
    tombstones: Vec<PlannedRelationshipTombstone>,
}

#[cfg(all(feature = "redb", feature = "blob"))]
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SourceRelationshipMarker {
    #[serde(rename = "type")]
    node_type: String,
    relationship_id: String,
    source_id: String,
    target_id: String,
    relation_reference: String,
    raw_digest: Digest256,
}

#[cfg(all(feature = "redb", feature = "blob"))]
async fn reconcile_batch(
    ctx: &PrepareContext<'_>,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    batch_digest: Digest256,
    receipt_id: &str,
    persistence: &dyn crate::server::persistence::PersistenceBackend,
    graph_fname: &str,
    resolved_revision: Option<u64>,
) -> Result<ReconciliationPlan, String> {
    let marker_id = source_marker_id(ctx.tenant_scope, batch)?;
    let prior = persistence
        .read_node(graph_fname, &marker_id)
        .await?
        .map(|bytes| {
            rmp_serde::from_slice::<SourceLiveSetMarker>(&bytes)
                .map_err(|error| format!("SOURCE_RECONCILIATION_MARKER_INVALID: {error}"))
        })
        .transpose()?;
    let prior_live: BTreeSet<_> = prior
        .as_ref()
        .map(|marker| marker.live_ids.iter().cloned().collect())
        .unwrap_or_default();
    let entities = reconcile_entities(batch, &prior_live, prior.as_ref())?;
    if let Some(unknown) = entities
        .tombstones
        .iter()
        .find(|withdrawal| !prior_live.contains(&withdrawal.entity))
    {
        return Err(format!(
            "SOURCE_WITHDRAWAL_UNKNOWN: {} is not in the durable live set",
            unknown.entity.record_id
        ));
    }
    if batch.mode == SourceIngestionMode::Reconcile {
        validate_authoritative_live_targets(ctx, batch, persistence, graph_fname)?;
    }
    validate_tombstone_targets(ctx, batch, &entities.tombstones, persistence, graph_fname)?;
    let relationships = reconcile_relationships(
        ctx.tenant_scope,
        batch,
        prior.as_ref(),
        &entities.tombstones,
    )?;
    let entry_revision = resolved_revision
        .or_else(|| prior.as_ref().map(|marker| marker.mapping_revision))
        .ok_or_else(|| {
            "SOURCE_MAPPING_AUTHORITY_UNAVAILABLE: submission has no resolved mapping and no prior authority marker"
                .to_string()
        })?;
    let marker = SourceLiveSetMarker {
        node_type: "SourceLiveSetMarker".into(),
        connector: batch.connector.as_str().into(),
        stream: batch.provider_checkpoint.stream.as_str().into(),
        live_ids: entities.live_ids,
        live_relationships: relationships.live_relationships,
        live_set_digest: entities.live_set_digest,
        relationship_live_set_digest: relationships.live_set_digest,
        checkpoint: batch.provider_checkpoint.clone(),
        checkpoint_digest: batch.provider_checkpoint.digest()?,
        batch_digest,
        receipt_id: receipt_id.into(),
        committed_graph_version: ctx.graph_version.saturating_add(1),
        mapping_revision: entry_revision,
    };
    Ok(ReconciliationPlan {
        tombstones: entities.tombstones,
        relationship_tombstones: relationships.tombstones,
        live_set_digest: entities.live_set_digest,
        relationship_live_set_digest: relationships.live_set_digest,
        marker: Some(Method::AddNode {
            node_id: marker_id.clone(),
            properties_msgpack: rmp_serde::to_vec_named(&marker)
                .map_err(|error| format!("source live-set marker encoding failed: {error}"))?,
        }),
        marker_id,
        entry_revision,
        previous_batch_digest: prior.as_ref().map(|marker| marker.batch_digest),
    })
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn reconcile_entities(
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    prior_live: &BTreeSet<SourceEntityRef>,
    prior: Option<&SourceLiveSetMarker>,
) -> Result<EntityReconciliation, String> {
    if batch.mode == SourceIngestionMode::Reconcile {
        return reconcile_authoritative_entities(batch, prior_live);
    }
    if is_authoritative_empty_full(batch) {
        return Ok(EntityReconciliation {
            live_ids: Vec::new(),
            live_set_digest: Some(digest_live_set(&[])?),
            tombstones: prior_live
                .iter()
                .cloned()
                .map(|entity| SourceWithdrawal {
                    entity,
                    reason: "absent_from_authoritative_empty_full_snapshot".into(),
                })
                .collect(),
        });
    }
    let mut observed = prior_live.clone();
    observed.extend(batch.records.iter().map(|record| SourceEntityRef {
        stream: record.stream.clone(),
        record_id: record.record_id.clone(),
    }));
    let withdrawn: BTreeSet<_> = batch
        .withdrawals
        .iter()
        .map(|withdrawal| withdrawal.entity.clone())
        .collect();
    let live_ids: Vec<_> = observed.difference(&withdrawn).cloned().collect();
    let changed = !batch.withdrawals.is_empty() || !batch.records.is_empty();
    Ok(EntityReconciliation {
        live_set_digest: if changed {
            Some(digest_live_set(&live_ids)?)
        } else {
            prior.and_then(|marker| marker.live_set_digest)
        },
        live_ids,
        tombstones: batch.withdrawals.as_slice().to_vec(),
    })
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn reconcile_authoritative_entities(
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    prior_live: &BTreeSet<SourceEntityRef>,
) -> Result<EntityReconciliation, String> {
    let current: BTreeSet<_> = batch
        .authoritative_live_ids
        .as_ref()
        .ok_or_else(|| "reconcile ingestion requires authoritative_live_ids".to_string())?
        .iter()
        .cloned()
        .collect();
    let live_ids: Vec<_> = current.iter().cloned().collect();
    Ok(EntityReconciliation {
        live_set_digest: Some(digest_live_set(&live_ids)?),
        live_ids,
        tombstones: prior_live
            .difference(&current)
            .cloned()
            .map(|entity| SourceWithdrawal {
                entity,
                reason: "absent_from_authoritative_snapshot".into(),
            })
            .collect(),
    })
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn reconcile_relationships(
    tenant: &str,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    prior: Option<&SourceLiveSetMarker>,
    entity_tombstones: &[SourceWithdrawal],
) -> Result<RelationshipReconciliation, String> {
    let prior_relationships = relationship_state_by_id(
        prior
            .map(|marker| marker.live_relationships.as_slice())
            .unwrap_or_default(),
    );
    let current_relationships = submitted_relationship_state(tenant, batch)?;
    let authoritative =
        batch.mode == SourceIngestionMode::Reconcile || is_authoritative_empty_full(batch);
    let mut retained =
        retained_relationship_state(&prior_relationships, &current_relationships, authoritative)?;
    let tombstoned_entities: BTreeSet<_> = entity_tombstones
        .iter()
        .map(|item| item.entity.clone())
        .collect();
    retained.retain(|_, relationship| {
        !tombstoned_entities.contains(&relationship.source)
            && !tombstoned_entities.contains(&relationship.target)
    });
    validate_unique_relationship_endpoints(&retained)?;
    let tombstones =
        planned_relationship_tombstones(&prior_relationships, &retained, &tombstoned_entities);
    let live_relationships: Vec<_> = retained.into_values().collect();
    let changed = authoritative || !current_relationships.is_empty() || !tombstones.is_empty();
    Ok(RelationshipReconciliation {
        live_set_digest: if changed {
            Some(digest_relationship_live_set(&live_relationships)?)
        } else {
            prior.and_then(|marker| marker.relationship_live_set_digest)
        },
        live_relationships,
        tombstones,
    })
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn relationship_state_by_id(
    relationships: &[SourceLiveRelationship],
) -> BTreeMap<String, SourceLiveRelationship> {
    relationships
        .iter()
        .cloned()
        .map(|relationship| (relationship.relationship_id.clone(), relationship))
        .collect()
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn submitted_relationship_state(
    tenant: &str,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
) -> Result<BTreeMap<String, SourceLiveRelationship>, String> {
    batch
        .relationships
        .iter()
        .map(|relationship| {
            let relationship_id = canonical_relationship_id(tenant, batch, relationship)?;
            let state = SourceLiveRelationship {
                relationship_id,
                source: relationship.source.clone(),
                target: relationship.target.clone(),
            };
            Ok((state.relationship_id.clone(), state))
        })
        .collect()
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn retained_relationship_state(
    prior: &BTreeMap<String, SourceLiveRelationship>,
    current: &BTreeMap<String, SourceLiveRelationship>,
    authoritative: bool,
) -> Result<BTreeMap<String, SourceLiveRelationship>, String> {
    if authoritative {
        return Ok(current.clone());
    }
    let mut retained = prior.clone();
    for (identity, relationship) in current {
        if retained
            .insert(identity.clone(), relationship.clone())
            .is_some_and(|prior| prior != *relationship)
        {
            return Err(
                "SOURCE_RELATIONSHIP_IDENTITY_CONFLICT: relationship endpoints changed".into(),
            );
        }
    }
    Ok(retained)
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn validate_unique_relationship_endpoints(
    relationships: &BTreeMap<String, SourceLiveRelationship>,
) -> Result<(), String> {
    let mut endpoints = BTreeSet::new();
    if relationships.values().any(|relationship| {
        !endpoints.insert((relationship.source.clone(), relationship.target.clone()))
    }) {
        Err(
            "SOURCE_RELATIONSHIP_ENDPOINT_CONFLICT: source partition has parallel endpoint identities"
                .into(),
        )
    } else {
        Ok(())
    }
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn planned_relationship_tombstones(
    prior: &BTreeMap<String, SourceLiveRelationship>,
    retained: &BTreeMap<String, SourceLiveRelationship>,
    tombstoned_entities: &BTreeSet<SourceEntityRef>,
) -> Vec<PlannedRelationshipTombstone> {
    prior
        .iter()
        .filter(|(identity, _)| !retained.contains_key(*identity))
        .map(|(_, relationship)| PlannedRelationshipTombstone {
            relationship: relationship.clone(),
            reason: if tombstoned_entities.contains(&relationship.source)
                || tombstoned_entities.contains(&relationship.target)
            {
                "endpoint_withdrawn".into()
            } else {
                "absent_from_authoritative_snapshot".into()
            },
        })
        .collect()
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn is_authoritative_empty_full(batch: &eg_types::source_ingestion::SourceIngestionBatch) -> bool {
    batch.mode == SourceIngestionMode::Full
        && batch.records.is_empty()
        && batch.relationships.is_empty()
        && batch.empty_authoritative_approval.is_some()
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn validate_tombstone_targets(
    ctx: &PrepareContext<'_>,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    withdrawals: &[SourceWithdrawal],
    persistence: &dyn crate::server::persistence::PersistenceBackend,
    graph_fname: &str,
) -> Result<(), String> {
    for withdrawal in withdrawals {
        let node_id = source_node_id(
            ctx.tenant_scope,
            batch.connector.as_str(),
            withdrawal.entity.stream.as_str(),
            &withdrawal.entity.record_id,
        )?;
        if persistence
            .read_node_blocking(graph_fname, &node_id)?
            .is_none()
        {
            return Err(format!(
                "SOURCE_WITHDRAWAL_UNKNOWN: {} is not a live canonical entity",
                withdrawal.entity.record_id
            ));
        }
    }
    Ok(())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn validate_authoritative_live_targets(
    ctx: &PrepareContext<'_>,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    persistence: &dyn crate::server::persistence::PersistenceBackend,
    graph_fname: &str,
) -> Result<(), String> {
    let observed: BTreeSet<_> = batch
        .records
        .iter()
        .map(|record| SourceEntityRef {
            stream: record.stream.clone(),
            record_id: record.record_id.clone(),
        })
        .collect();
    let live = batch
        .authoritative_live_ids
        .as_ref()
        .ok_or_else(|| "reconcile ingestion requires authoritative_live_ids".to_string())?;
    for entity in live.iter().filter(|entity| !observed.contains(*entity)) {
        let node_id = source_node_id(
            ctx.tenant_scope,
            batch.connector.as_str(),
            entity.stream.as_str(),
            &entity.record_id,
        )?;
        let properties = persistence
            .read_node_blocking(graph_fname, &node_id)?
            .ok_or_else(|| {
                format!(
                    "SOURCE_AUTHORITATIVE_LIVE_ID_UNKNOWN: {} has no canonical observation",
                    entity.record_id
                )
            })?;
        let properties: serde_json::Value = rmp_serde::from_slice(&properties)
            .map_err(|error| format!("SOURCE_AUTHORITATIVE_LIVE_ID_INVALID: {error}"))?;
        if properties
            .get("source_tombstoned")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            return Err(format!(
                "SOURCE_AUTHORITATIVE_LIVE_ID_WITHDRAWN: {} requires a new observation",
                entity.record_id
            ));
        }
    }
    Ok(())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn digest_live_set(live_ids: &[SourceEntityRef]) -> Result<Digest256, String> {
    let bytes = rmp_serde::to_vec_named(live_ids)
        .map_err(|error| format!("source live set encoding failed: {error}"))?;
    Digest256::framed(b"eg/source-live-set/v1", &[&bytes])
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn digest_relationship_live_set(
    relationships: &[SourceLiveRelationship],
) -> Result<Digest256, String> {
    let bytes = rmp_serde::to_vec_named(relationships)
        .map_err(|error| format!("source relationship live set encoding failed: {error}"))?;
    Digest256::framed(b"eg/source-relationship-live-set/v1", &[&bytes])
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn source_marker_id(
    tenant: &str,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
) -> Result<String, String> {
    source_marker_id_for(tenant, &batch.connector, &batch.provider_checkpoint.stream)
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn source_marker_id_for(
    tenant: &str,
    connector: &eg_types::contract::ResourceId,
    stream: &eg_types::contract::ResourceId,
) -> Result<String, String> {
    Ok(format!(
        "source-live-set:{}",
        Digest256::framed(
            b"eg/source-live-set-identity/v1",
            &[
                tenant.as_bytes(),
                connector.as_str().as_bytes(),
                stream.as_str().as_bytes(),
            ],
        )?
        .to_hex()
    ))
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
    resolved: &ResolvedIngestionMappings,
    policy_subject: &str,
) -> Result<AdmittedRecords, String> {
    let mut admitted = AdmittedRecords {
        raw_admissions: Vec::with_capacity(batch.records.len()),
        relationship_raw_admissions: Vec::with_capacity(batch.relationships.len()),
        blobs: BTreeMap::new(),
        methods: Vec::with_capacity(batch.records.len() + 1),
        lineage: Vec::with_capacity(batch.records.len()),
        policies: Vec::with_capacity(batch.records.len() + 1),
        tombstone_count: 0,
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
            raw_digest: item.raw_digest,
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
struct RelationshipAdmissionContext<'a, 'ctx> {
    ctx: &'a PrepareContext<'ctx>,
    batch: &'a eg_types::source_ingestion::SourceIngestionBatch,
    authority: &'a CarrierAuthority,
    blob: &'a Arc<crate::server::blob::BlobCursors>,
    resolved: &'a ResolvedIngestionMappings,
    policy_subject: &'a str,
    persistence: &'a dyn crate::server::persistence::PersistenceBackend,
    graph_fname: &'a str,
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn admit_relationships(
    context: RelationshipAdmissionContext<'_, '_>,
    admitted: &mut AdmittedRecords,
) -> Result<(), String> {
    let RelationshipAdmissionContext {
        ctx,
        batch,
        authority,
        blob,
        resolved,
        policy_subject,
        persistence,
        graph_fname,
    } = context;
    let mut raw_digests = BTreeSet::new();
    let submitted: BTreeMap<_, _> = batch
        .records
        .iter()
        .map(|record| {
            let mapping = resolved
                .entities
                .get(&record.mapping_reference)
                .ok_or_else(|| {
                    "UNKNOWN_MAPPING_REFERENCE: relationship endpoint mapping was not resolved"
                        .to_string()
                })?;
            Ok((
                SourceEntityRef {
                    stream: record.stream.clone(),
                    record_id: record.record_id.clone(),
                },
                mapping.schema_mapping.ontology_class.clone(),
            ))
        })
        .collect::<Result<_, String>>()?;
    for relationship in &batch.relationships {
        let relationship_id = canonical_relationship_id(ctx.tenant_scope, batch, relationship)?;
        let relation = resolved
            .relationships
            .get(&relationship.relation_reference)
            .ok_or_else(|| {
                "UNKNOWN_RELATIONSHIP_REFERENCE: relation was not resolved".to_string()
            })?;
        validate_relationship_endpoint(
            ctx,
            batch,
            &submitted,
            &relationship.source,
            &relation.relation.source_resource,
            persistence,
            graph_fname,
        )?;
        validate_relationship_endpoint(
            ctx,
            batch,
            &submitted,
            &relationship.target,
            &relation.relation.target_resource,
            persistence,
            graph_fname,
        )?;
        let source_id = source_node_id(
            ctx.tenant_scope,
            batch.connector.as_str(),
            relationship.source.stream.as_str(),
            &relationship.source.record_id,
        )?;
        let target_id = source_node_id(
            ctx.tenant_scope,
            batch.connector.as_str(),
            relationship.target.stream.as_str(),
            &relationship.target.record_id,
        )?;
        let raw = rmp_serde::to_vec_named(relationship)
            .map_err(|error| format!("raw source relationship encoding failed: {error}"))?;
        let raw_digest_hex = admit_raw(ctx, authority, blob, &raw)?;
        let raw_digest = Digest256::parse(&raw_digest_hex)?;
        let properties = relationship_properties(
            relationship,
            &relationship_id,
            relation,
            &raw_digest_hex,
            batch.strict_schema,
        )?;
        admitted
            .blobs
            .entry(raw_digest_hex.clone())
            .or_insert_with(|| {
                raw_blob_reference(
                    &raw_digest_hex,
                    raw.len(),
                    "application/vnd.epistemic-graph.source-relationship+msgpack",
                )
            });
        admitted
            .relationship_raw_admissions
            .push(RawRelationshipAdmissionReceipt {
                relationship_id: relationship_id.clone(),
                raw_digest,
                deduplicated: !raw_digests.insert(raw_digest_hex.clone()),
            });
        let marker_id = source_relationship_marker_id(
            ctx.tenant_scope,
            batch.connector.as_str(),
            batch.provider_checkpoint.stream.as_str(),
            &relationship_id,
        )?;
        let marker = SourceRelationshipMarker {
            node_type: "SourceRelationshipMarker".into(),
            relationship_id: relationship_id.clone(),
            source_id: source_id.clone(),
            target_id: target_id.clone(),
            relation_reference: relationship.relation_reference.clone(),
            raw_digest,
        };
        let prior = persistence
            .read_node_blocking(graph_fname, &marker_id)?
            .map(|bytes| {
                rmp_serde::from_slice::<SourceRelationshipMarker>(&bytes)
                    .map_err(|error| format!("SOURCE_RELATIONSHIP_MARKER_INVALID: {error}"))
            })
            .transpose()?;
        if let Some(prior) = prior {
            if prior != marker {
                return Err(
                    "SOURCE_RELATIONSHIP_IDENTITY_CONFLICT: relationship identity changed".into(),
                );
            }
            continue;
        }
        admitted.methods.push(Method::AddNode {
            node_id: marker_id.clone(),
            properties_msgpack: rmp_serde::to_vec_named(&marker)
                .map_err(|error| format!("source relationship marker encoding failed: {error}"))?,
        });
        admitted.methods.push(Method::AddEdge {
            source_id: source_id.clone(),
            target_id: target_id.clone(),
            properties_msgpack: rmp_serde::to_vec_named(&properties)
                .map_err(|error| format!("mapped source relationship encoding failed: {error}"))?,
        });
        admitted.policies.push(policy(
            &marker_id,
            ctx.tenant_scope,
            policy_subject,
            relation.entry_revision,
        ));
        admitted.policies.push(policy(
            &format!("{source_id}->{target_id}"),
            ctx.tenant_scope,
            policy_subject,
            relation.entry_revision,
        ));
        admitted
            .lineage
            .push(eg_types::change_envelope::LineageRecord {
                lineage_id: format!("lineage:{}", relationship_lineage_digest(&relationship_id)?),
                operation: eg_types::change_envelope::MaterialOperation::Upsert,
                object_id: format!("{source_id}->{target_id}"),
                source_artifact_digest: raw_digest_hex,
                transform_name: "connector-manifest-resource-relation".into(),
                transform_version: relation.body_sha256.to_hex(),
                parent_content_digests: Vec::new(),
            });
    }
    Ok(())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn validate_relationship_endpoint(
    ctx: &PrepareContext<'_>,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    submitted: &BTreeMap<SourceEntityRef, String>,
    entity: &SourceEntityRef,
    expected_type: &str,
    persistence: &dyn crate::server::persistence::PersistenceBackend,
    graph_fname: &str,
) -> Result<(), String> {
    if let Some(actual_type) = submitted.get(entity) {
        return validate_relationship_endpoint_type(actual_type, expected_type, entity);
    }
    let node_id = source_node_id(
        ctx.tenant_scope,
        batch.connector.as_str(),
        entity.stream.as_str(),
        &entity.record_id,
    )?;
    let properties = persistence
        .read_node_blocking(graph_fname, &node_id)?
        .ok_or_else(|| format!("SOURCE_RELATIONSHIP_ENDPOINT_UNKNOWN: {}", entity.record_id))?;
    let properties: serde_json::Value = rmp_serde::from_slice(&properties)
        .map_err(|error| format!("SOURCE_RELATIONSHIP_ENDPOINT_INVALID: {error}"))?;
    if properties
        .get("source_tombstoned")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        return Err(
            "SOURCE_RELATIONSHIP_ENDPOINT_WITHDRAWN: canonical endpoint is tombstoned".into(),
        );
    }
    let actual_type = properties
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            "SOURCE_RELATIONSHIP_ENDPOINT_INVALID: canonical endpoint has no type".to_string()
        })?;
    validate_relationship_endpoint_type(actual_type, expected_type, entity)
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn validate_relationship_endpoint_type(
    actual_type: &str,
    expected_type: &str,
    entity: &SourceEntityRef,
) -> Result<(), String> {
    if actual_type == expected_type {
        Ok(())
    } else {
        Err(format!(
            "SOURCE_RELATIONSHIP_ENDPOINT_TYPE_MISMATCH: {} is {actual_type}, expected {expected_type}",
            entity.record_id
        ))
    }
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn relationship_properties(
    relationship: &eg_types::source_ingestion::SourceRelationship,
    relationship_id: &str,
    resolved: &crate::server::persistence::connector_pack::ResolvedConnectorRelationshipMapping,
    raw_digest: &str,
    strict_schema: bool,
) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    let supplied = relationship
        .properties
        .as_ref()
        .map(|properties| properties.value())
        .transpose()?
        .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
    let supplied = supplied
        .as_object()
        .ok_or_else(|| "source relationship properties must be an object".to_string())?;
    if strict_schema && !supplied.is_empty() {
        return Err(
            "SOURCE_RELATIONSHIP_SCHEMA_INVALID: manifest relation declares no property map".into(),
        );
    }
    let relation_type = if resolved.relation.lpg_rel_type.is_empty() {
        &resolved.relation.relationship
    } else {
        &resolved.relation.lpg_rel_type
    };
    let mut mapped = supplied.clone();
    mapped.insert("relationship".into(), relation_type.clone().into());
    mapped.insert("source_relationship_id".into(), relationship_id.into());
    mapped.insert(
        "source_relation_reference".into(),
        relationship.relation_reference.clone().into(),
    );
    mapped.insert("source_raw_sha256".into(), raw_digest.into());
    mapped.insert(
        "source_provenance".into(),
        serde_json::to_value(&relationship.provenance)
            .map_err(|error| format!("relationship provenance encoding failed: {error}"))?,
    );
    Ok(mapped)
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn admit_tombstones(
    ctx: &PrepareContext<'_>,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    tombstones: &[SourceWithdrawal],
    policy_subject: &str,
    mapping_revision: u64,
    admitted: &mut AdmittedRecords,
) -> Result<(), String> {
    let checkpoint_digest = batch.provider_checkpoint.digest()?.to_hex();
    for withdrawal in tombstones {
        let node_id = source_node_id(
            ctx.tenant_scope,
            batch.connector.as_str(),
            withdrawal.entity.stream.as_str(),
            &withdrawal.entity.record_id,
        )?;
        let updates = serde_json::json!({
            "source_tombstoned": true,
            "source_withdrawal_reason": withdrawal.reason,
            "source_withdrawal_checkpoint": checkpoint_digest,
        });
        admitted.methods.push(Method::CompareAndSetNodeFields {
            node_id: node_id.clone(),
            conditions_msgpack: rmp_serde::to_vec_named(&serde_json::json!({}))
                .map_err(|error| format!("source tombstone condition encoding failed: {error}"))?,
            updates_msgpack: rmp_serde::to_vec_named(&updates)
                .map_err(|error| format!("source tombstone encoding failed: {error}"))?,
        });
        admitted.policies.push(policy(
            &node_id,
            ctx.tenant_scope,
            policy_subject,
            mapping_revision,
        ));
        admitted.tombstone_count += 1;
    }
    Ok(())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn admit_relationship_tombstones(
    ctx: &PrepareContext<'_>,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    tombstones: &[PlannedRelationshipTombstone],
    policy_subject: &str,
    mapping_revision: u64,
    admitted: &mut AdmittedRecords,
) -> Result<(), String> {
    for tombstone in tombstones {
        let source_id = source_node_id(
            ctx.tenant_scope,
            batch.connector.as_str(),
            tombstone.relationship.source.stream.as_str(),
            &tombstone.relationship.source.record_id,
        )?;
        let target_id = source_node_id(
            ctx.tenant_scope,
            batch.connector.as_str(),
            tombstone.relationship.target.stream.as_str(),
            &tombstone.relationship.target.record_id,
        )?;
        let marker_id = source_relationship_marker_id(
            ctx.tenant_scope,
            batch.connector.as_str(),
            batch.provider_checkpoint.stream.as_str(),
            &tombstone.relationship.relationship_id,
        )?;
        admitted.methods.push(Method::RemoveEdge {
            source_id: source_id.clone(),
            target_id: target_id.clone(),
        });
        admitted.methods.push(Method::RemoveNode {
            node_id: marker_id.clone(),
        });
        admitted.policies.push(policy_with_operation(
            &format!("{source_id}->{target_id}"),
            ctx.tenant_scope,
            policy_subject,
            mapping_revision,
            eg_types::change_envelope::MaterialOperation::Delete,
        ));
        admitted.policies.push(policy_with_operation(
            &marker_id,
            ctx.tenant_scope,
            policy_subject,
            mapping_revision,
            eg_types::change_envelope::MaterialOperation::Delete,
        ));
    }
    Ok(())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn tombstone_receipts(
    ctx: &PrepareContext<'_>,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    tombstones: &[SourceWithdrawal],
) -> Result<Vec<SourceTombstoneReceipt>, String> {
    tombstones
        .iter()
        .map(|withdrawal| {
            Ok(SourceTombstoneReceipt {
                entity: withdrawal.entity.clone(),
                node_id: source_node_id(
                    ctx.tenant_scope,
                    batch.connector.as_str(),
                    withdrawal.entity.stream.as_str(),
                    &withdrawal.entity.record_id,
                )?,
                reason: withdrawal.reason.clone(),
            })
        })
        .collect()
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn relationship_tombstone_receipts(
    tombstones: &[PlannedRelationshipTombstone],
) -> Vec<SourceRelationshipTombstoneReceipt> {
    tombstones
        .iter()
        .map(|tombstone| SourceRelationshipTombstoneReceipt {
            relationship_id: tombstone.relationship.relationship_id.clone(),
            source: tombstone.relationship.source.clone(),
            target: tombstone.relationship.target.clone(),
            reason: tombstone.reason.clone(),
        })
        .collect()
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
    resolved: &ResolvedIngestionMappings,
    policy_subject: &str,
) -> Result<AdmittedRecord, String> {
    let raw = rmp_serde::to_vec_named(record)
        .map_err(|error| format!("raw source record encoding failed: {error}"))?;
    let raw_digest_hex = admit_raw(ctx, authority, blob, &raw)?;
    let raw_digest = Digest256::parse(&raw_digest_hex)?;
    let node_id = source_node_id(
        ctx.tenant_scope,
        batch.connector.as_str(),
        record.stream.as_str(),
        &record.record_id,
    )?;
    let mapping = resolved
        .entities
        .get(&record.mapping_reference)
        .ok_or_else(|| "UNKNOWN_MAPPING_REFERENCE: record mapping was not resolved".to_string())?;
    let properties = mapped_properties(
        record,
        &mapping.schema_mapping.ontology_class,
        &mapping.schema_mapping.fields,
        &record.mapping_reference,
        &raw_digest_hex,
        batch.strict_schema,
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
        transform_version: mapping.body_sha256.to_hex(),
        parent_content_digests: Vec::new(),
    };
    let policy = policy(
        &node_id,
        ctx.tenant_scope,
        policy_subject,
        mapping.entry_revision,
    );
    Ok(AdmittedRecord {
        raw_digest_hex: raw_digest_hex.clone(),
        raw_digest,
        blob: raw_blob_reference(
            &raw_digest_hex,
            raw.len(),
            "application/vnd.epistemic-graph.source-record+msgpack",
        ),
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
fn raw_blob_reference(
    raw_digest: &str,
    length: usize,
    media_type: &str,
) -> eg_types::change_envelope::BlobReference {
    eg_types::change_envelope::BlobReference {
        blob_id: format!("raw:{raw_digest}"),
        operation: eg_types::change_envelope::MaterialOperation::Upsert,
        digest_algorithm: "sha256".into(),
        digest: raw_digest.into(),
        media_type: media_type.into(),
        length: length as u64,
    }
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn receipt_method(
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    batch_id: &str,
    resolved: &ResolvedIngestionMappings,
    reconciliation: &ReconciliationPlan,
    raw_admissions: &[RawAdmissionReceipt],
    relationship_raw_admissions: &[RawRelationshipAdmissionReceipt],
) -> Result<Method, String> {
    let properties = serde_json::json!({
        "type": "SourceIngestionReceipt",
        "connector": batch.connector.as_str(),
        "mode": batch.mode,
        "mappings": resolved.receipts,
        "raw_admissions": raw_admissions,
        "relationship_raw_admissions": relationship_raw_admissions,
        "checkpoint": batch.provider_checkpoint,
        "checkpoint_digest": batch.provider_checkpoint.digest()?.to_hex(),
        "content_hash": batch.provider_checkpoint.content_hash,
        "live_set_digest": reconciliation.live_set_digest,
        "relationship_live_set_digest": reconciliation.relationship_live_set_digest,
        "withdrawals": reconciliation.tombstones,
        "relationship_tombstones": relationship_tombstone_receipts(&reconciliation.relationship_tombstones),
        "record_count": batch.records.len(),
        "relationship_count": batch.relationships.len(),
        "tombstoned_count": reconciliation.tombstones.len(),
        "relationship_tombstoned_count": reconciliation.relationship_tombstones.len(),
        "affected_count": batch.records.len() + batch.relationships.len() + reconciliation.tombstones.len() + reconciliation.relationship_tombstones.len(),
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
struct EnvelopeContents {
    batch_digest: Digest256,
    mutation: eg_types::mutation_batch::MutationBatch,
    blobs: BTreeMap<String, eg_types::change_envelope::BlobReference>,
    policies: Vec<eg_types::change_envelope::PolicyRecord>,
    lineage: Vec<eg_types::change_envelope::LineageRecord>,
    mapping_revision: u64,
    previous_batch_digest: Option<Digest256>,
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn build_envelope(
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    batch_id: &str,
    contents: EnvelopeContents,
) -> Result<eg_types::change_envelope::ChangeEnvelope, String> {
    let EnvelopeContents {
        batch_digest,
        mutation,
        blobs,
        policies,
        lineage,
        mapping_revision,
        previous_batch_digest,
    } = contents;
    let checkpoint_digest = batch.provider_checkpoint.digest()?;
    let expected_previous = batch
        .expected_previous_checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.digest())
        .transpose()?;
    let envelope = eg_types::change_envelope::ChangeEnvelope {
        schema_version: eg_types::change_envelope::CHANGE_ENVELOPE_VERSION,
        envelope_id: batch_id.into(),
        mutation,
        content_version: eg_types::change_envelope::ContentVersion {
            object_id: batch_id.into(),
            digest_algorithm: "sha256".into(),
            digest: batch_digest.to_hex(),
            previous_digest: previous_batch_digest.map(Digest256::to_hex),
            source_version: eg_types::change_envelope::ContentVersionPosition::Opaque {
                version_type: "source-cursor-sha256".into(),
                value: checkpoint_digest.to_hex(),
            },
        },
        cursor: Some(eg_types::change_envelope::ChangeCursor {
            source: format!(
                "{}:{}",
                batch.connector.as_str(),
                batch.provider_checkpoint.stream.as_str()
            ),
            partition: String::new(),
            position: eg_types::change_envelope::CursorPosition::Opaque {
                cursor_type: "source-cursor-sha256".into(),
                value: checkpoint_digest.to_hex(),
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

/// Slim build (no `redb`+`blob`): `prepare` above refuses every request, so no
/// `PreparedSourceIngestion` is ever produced and this is unreachable in practice.
/// It still refuses by the same code rather than panicking, and it exists so the
/// shared dispatch route compiles identically in every feature profile.
#[cfg(not(all(feature = "redb", feature = "blob")))]
pub(crate) fn finish(_prepared: PreparedSourceIngestion, response: Response) -> Response {
    Response::err(
        response.id,
        "SOURCE_INGESTION_UNAVAILABLE: native source ingestion requires redb and blob support",
    )
}

#[cfg(all(feature = "redb", feature = "blob"))]
pub(crate) fn finish(prepared: PreparedSourceIngestion, response: Response) -> Response {
    let response_id = response.id;
    if let Some(error) = response.error {
        return Response::err(response_id, error);
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
        Err(error) => return Response::err(response_id, error),
    };
    let disposition = if applied.commit.replayed {
        SourceIngestionDisposition::Replayed
    } else {
        SourceIngestionDisposition::Committed
    };
    match build_terminal_receipt(prepared, disposition) {
        Ok(receipt) => Response::ok(
            response_id,
            ResultPayload::of::<eg_types::result_contract::ingestion::SourceIngest>(receipt),
        ),
        Err(error) => Response::err(response_id, error),
    }
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn build_terminal_receipt(
    prepared: PreparedSourceIngestion,
    disposition: SourceIngestionDisposition,
) -> Result<SourceIngestionReceipt, String> {
    let accepted_checkpoint_digest = prepared.accepted_checkpoint.digest()?;
    let raw_admissions = eg_types::contract::BoundedVec::new(prepared.raw_admissions)?;
    let relationship_raw_admissions =
        eg_types::contract::BoundedVec::new(prepared.relationship_raw_admissions)?;
    let mappings = eg_types::contract::BoundedVec::new(prepared.mappings)?;
    let tombstones = eg_types::contract::BoundedVec::new(prepared.tombstones)?;
    let relationship_tombstones =
        eg_types::contract::BoundedVec::new(prepared.relationship_tombstones)?;
    let receipt_digest = Digest256::framed(
        b"eg/source-ingestion-receipt/v2",
        &[
            prepared.batch_digest.as_bytes(),
            &receipt_bytes(&prepared.mode)?,
            &receipt_bytes(&mappings)?,
            &receipt_bytes(&raw_admissions)?,
            &receipt_bytes(&relationship_raw_admissions)?,
            &receipt_bytes(&tombstones)?,
            &receipt_bytes(&relationship_tombstones)?,
            accepted_checkpoint_digest.as_bytes(),
            prepared
                .accepted_checkpoint
                .content_hash
                .as_ref()
                .map(|digest| digest.as_bytes().as_slice())
                .unwrap_or(&[]),
            prepared
                .live_set_digest
                .as_ref()
                .map(|digest| digest.as_bytes().as_slice())
                .unwrap_or(&[]),
            prepared
                .relationship_live_set_digest
                .as_ref()
                .map(|digest| digest.as_bytes().as_slice())
                .unwrap_or(&[]),
            &prepared.affected_count.to_be_bytes(),
            &prepared.relationship_count.to_be_bytes(),
            &prepared.tombstoned_count.to_be_bytes(),
            &prepared.relationship_tombstoned_count.to_be_bytes(),
            prepared.committed_graph_version.to_be_bytes().as_slice(),
        ],
    )?;
    Ok(SourceIngestionReceipt {
        receipt_id: format!("source-ingest:{}", prepared.batch_digest.to_hex()),
        disposition,
        mode: prepared.mode,
        batch_digest: prepared.batch_digest,
        mappings,
        raw_admissions,
        relationship_raw_admissions,
        tombstones,
        relationship_tombstones,
        content_hash: prepared.accepted_checkpoint.content_hash,
        accepted_checkpoint: prepared.accepted_checkpoint,
        accepted_checkpoint_digest,
        live_set_digest: prepared.live_set_digest,
        relationship_live_set_digest: prepared.relationship_live_set_digest,
        affected_count: prepared.affected_count,
        relationship_count: prepared.relationship_count,
        tombstoned_count: prepared.tombstoned_count,
        relationship_tombstoned_count: prepared.relationship_tombstoned_count,
        committed_graph_version: prepared.committed_graph_version,
        receipt_digest,
    })
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn receipt_bytes(value: &impl serde::Serialize) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(value)
        .map_err(|error| format!("source ingestion receipt encoding failed: {error}"))
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
    validate_replay_metadata(batch, &receipt)?;
    let mappings = replay_receipt_value::<Vec<SourceMappingReceipt>>(&receipt, "mappings")?;
    let withdrawals = replay_receipt_value::<Vec<SourceWithdrawal>>(&receipt, "withdrawals")?;
    let tombstones = tombstone_receipts(&ctx, batch, &withdrawals)?;
    let relationship_tombstones = replay_receipt_value::<Vec<SourceRelationshipTombstoneReceipt>>(
        &receipt,
        "relationship_tombstones",
    )?;
    let live_set_digest = receipt
        .get("live_set_digest")
        .filter(|value| !value.is_null())
        .map(|value| {
            serde_json::from_value::<Digest256>(value.clone())
                .map_err(|error| format!("SOURCE_INGESTION_REPLAY_INVALID: {error}"))
        })
        .transpose()?;
    let relationship_live_set_digest = receipt
        .get("relationship_live_set_digest")
        .filter(|value| !value.is_null())
        .map(|value| {
            serde_json::from_value::<Digest256>(value.clone())
                .map_err(|error| format!("SOURCE_INGESTION_REPLAY_INVALID: {error}"))
        })
        .transpose()?;
    let raw_admissions =
        replay_receipt_value::<Vec<RawAdmissionReceipt>>(&receipt, "raw_admissions")?;
    let relationship_raw_admissions = replay_receipt_value::<Vec<RawRelationshipAdmissionReceipt>>(
        &receipt,
        "relationship_raw_admissions",
    )?;
    let committed_graph_version = replay_graph_version(&envelope)?;
    Ok(PreparedSourceIngestion {
        envelope,
        batch_digest,
        mode: batch.mode,
        mappings,
        raw_admissions,
        relationship_raw_admissions,
        tombstones,
        relationship_tombstones,
        accepted_checkpoint: batch.provider_checkpoint.clone(),
        live_set_digest,
        relationship_live_set_digest,
        affected_count: replay_receipt_u64(&receipt, "affected_count")?,
        relationship_count: replay_receipt_u64(&receipt, "relationship_count")?,
        tombstoned_count: replay_receipt_u64(&receipt, "tombstoned_count")?,
        relationship_tombstoned_count: replay_receipt_u64(
            &receipt,
            "relationship_tombstoned_count",
        )?,
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
fn validate_replay_metadata(
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    receipt: &serde_json::Value,
) -> Result<(), String> {
    let checkpoint_digest = batch.provider_checkpoint.digest()?;
    if replay_receipt_text(receipt, "checkpoint_digest")? != checkpoint_digest.to_hex() {
        return Err("SOURCE_INGESTION_REPLAY_CONFLICT: checkpoint differs".into());
    }
    let stored_mode = replay_receipt_value::<SourceIngestionMode>(receipt, "mode")?;
    if stored_mode != batch.mode {
        return Err("SOURCE_INGESTION_REPLAY_CONFLICT: ingestion mode differs".into());
    }
    Ok(())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn replay_receipt_value<T: serde::de::DeserializeOwned>(
    receipt: &serde_json::Value,
    field: &str,
) -> Result<T, String> {
    serde_json::from_value(
        receipt
            .get(field)
            .cloned()
            .ok_or_else(|| format!("SOURCE_INGESTION_REPLAY_INVALID: {field} is missing"))?,
    )
    .map_err(|error| format!("SOURCE_INGESTION_REPLAY_INVALID: {field}: {error}"))
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
fn relationship_mapping_digest(
    mapping: &eg_types::connector_pack::ConnectorRelationshipMapping,
) -> Result<Digest256, String> {
    let bytes = rmp_serde::to_vec_named(mapping)
        .map_err(|error| format!("connector relationship encoding failed: {error}"))?;
    Digest256::framed(b"eg/connector-relationship-mapping/v1", &[&bytes])
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn mapped_properties(
    record: &eg_types::source_ingestion::SourceRecord,
    ontology_class: &str,
    fields: &BTreeMap<String, String>,
    mapping_reference: &str,
    raw_digest: &str,
    strict_schema: bool,
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
    validate_strict_source_fields(source, fields, strict_schema)?;
    validate_mapping_selector(source, mapping_reference)?;
    map_configured_fields(source, fields, &mut targets, &mut mapped)?;
    map_external_tool_id(source, &mut mapped);
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
        map_configured_field(source, source_field, target_field, targets, mapped)?;
    }
    Ok(())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn map_configured_field(
    source: &serde_json::Map<String, serde_json::Value>,
    source_field: &str,
    target_field: &str,
    targets: &mut BTreeSet<String>,
    mapped: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    let output_field = mapped_output_field(source, source_field, target_field)?;
    validate_mapping_target(source_field, target_field, &output_field, targets)?;
    if let Some(value) = source.get(source_field) {
        mapped.insert(output_field, value.clone());
    }
    Ok(())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn validate_mapping_target(
    source_field: &str,
    target_field: &str,
    output_field: &str,
    targets: &mut BTreeSet<String>,
) -> Result<(), String> {
    if source_field.is_empty() || target_field.is_empty() || !targets.insert(output_field.into()) {
        return Err(
            "CONNECTOR_SCHEMA_MAPPING_INVALID: field targets must be nonempty and unique".into(),
        );
    }
    Ok(())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn map_external_tool_id(
    source: &serde_json::Map<String, serde_json::Value>,
    mapped: &mut serde_json::Map<String, serde_json::Value>,
) {
    if let Some(external_id) = source.get("externalToolId") {
        mapped.insert("externalToolId".into(), external_id.clone());
    }
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn validate_strict_source_fields(
    source: &serde_json::Map<String, serde_json::Value>,
    fields: &BTreeMap<String, String>,
    strict_schema: bool,
) -> Result<(), String> {
    if !strict_schema {
        return Ok(());
    }
    let mut allowed: BTreeSet<_> = fields.keys().map(String::as_str).collect();
    allowed.extend(["id", "node_type", "externalToolId"]);
    match source.keys().find(|key| !allowed.contains(key.as_str())) {
        Some(unknown) => Err(format!(
            "SOURCE_SCHEMA_FIELD_UNKNOWN: strict mapping does not select {unknown}"
        )),
        None => Ok(()),
    }
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn mapped_output_field(
    source: &serde_json::Map<String, serde_json::Value>,
    source_field: &str,
    target_field: &str,
) -> Result<String, String> {
    if !target_field.starts_with("xsd:") {
        return Ok(target_field.to_owned());
    }
    validate_xsd_value(source.get(source_field), target_field, source_field)?;
    Ok(source_field.to_owned())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn validate_mapping_selector(
    source: &serde_json::Map<String, serde_json::Value>,
    mapping_reference: &str,
) -> Result<(), String> {
    let Some(declared) = source.get("node_type").and_then(serde_json::Value::as_str) else {
        return Ok(());
    };
    let selected = mapping_reference
        .rsplit_once("#schema_mappings/")
        .map(|(_, key)| key)
        .ok_or_else(|| "SOURCE_MAPPING_SELECTOR_INVALID: keyed mapping is required".to_string())?;
    if declared != selected {
        return Err("SOURCE_MAPPING_SELECTOR_MISMATCH: node_type and mapping differ".into());
    }
    Ok(())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn validate_xsd_value(
    value: Option<&serde_json::Value>,
    datatype: &str,
    field: &str,
) -> Result<(), String> {
    let Some(value) = value else {
        return match datatype {
            "xsd:string"
            | "xsd:date"
            | "xsd:dateTime"
            | "xsd:anyURI"
            | "xsd:boolean"
            | "xsd:integer"
            | "xsd:int"
            | "xsd:long"
            | "xsd:nonNegativeInteger"
            | "xsd:decimal"
            | "xsd:double"
            | "xsd:float" => Ok(()),
            _ => Err(format!(
                "CONNECTOR_SCHEMA_MAPPING_INVALID: unknown datatype {datatype}"
            )),
        };
    };
    let valid = match datatype {
        "xsd:string" | "xsd:date" | "xsd:dateTime" | "xsd:anyURI" => value.is_string(),
        "xsd:boolean" => value.is_boolean(),
        "xsd:integer" | "xsd:int" | "xsd:long" => {
            value.as_i64().is_some() || value.as_u64().is_some()
        }
        "xsd:nonNegativeInteger" => value.as_u64().is_some(),
        "xsd:decimal" | "xsd:double" | "xsd:float" => value.is_number(),
        _ => {
            return Err(format!(
                "CONNECTOR_SCHEMA_MAPPING_INVALID: unknown datatype {datatype}"
            ))
        }
    };
    if valid {
        Ok(())
    } else {
        Err(format!(
            "SOURCE_SCHEMA_TYPE_MISMATCH: {field} does not satisfy {datatype}"
        ))
    }
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
    stream: &str,
    record_id: &str,
) -> Result<String, String> {
    source_scoped_id(
        "source",
        b"eg/source-record-identity/v1",
        [tenant, connector, stream, record_id],
    )
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn canonical_relationship_id(
    tenant: &str,
    batch: &eg_types::source_ingestion::SourceIngestionBatch,
    relationship: &eg_types::source_ingestion::SourceRelationship,
) -> Result<String, String> {
    Ok(format!(
        "source-relation:{}",
        Digest256::framed(
            b"eg/source-relationship-identity/v2",
            &[
                tenant.as_bytes(),
                batch.connector.as_str().as_bytes(),
                batch.provider_checkpoint.stream.as_str().as_bytes(),
                relationship.relation_reference.as_bytes(),
                relationship.source.stream.as_str().as_bytes(),
                relationship.source.record_id.as_bytes(),
                relationship.target.stream.as_str().as_bytes(),
                relationship.target.record_id.as_bytes(),
            ],
        )?
        .to_hex()
    ))
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn source_relationship_marker_id(
    tenant: &str,
    connector: &str,
    stream: &str,
    relationship_id: &str,
) -> Result<String, String> {
    source_scoped_id(
        "source-relationship",
        b"eg/source-relationship-identity/v1",
        [tenant, connector, stream, relationship_id],
    )
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn source_scoped_id(prefix: &str, domain: &[u8], parts: [&str; 4]) -> Result<String, String> {
    let framed = parts.map(str::as_bytes);
    Ok(format!(
        "{prefix}:{}",
        Digest256::framed(domain, &framed)?.to_hex()
    ))
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn short_digest(value: &str) -> Result<String, String> {
    Ok(Digest256::framed(b"eg/source-lineage-id/v1", &[value.as_bytes()])?.to_hex())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn relationship_lineage_digest(value: &str) -> Result<String, String> {
    Ok(Digest256::framed(b"eg/source-relationship-lineage-id/v1", &[value.as_bytes()])?.to_hex())
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn policy(
    object_id: &str,
    tenant: &str,
    subject_set_digest: &str,
    revision: u64,
) -> eg_types::change_envelope::PolicyRecord {
    policy_with_operation(
        object_id,
        tenant,
        subject_set_digest,
        revision,
        eg_types::change_envelope::MaterialOperation::Upsert,
    )
}

#[cfg(all(feature = "redb", feature = "blob"))]
fn policy_with_operation(
    object_id: &str,
    tenant: &str,
    subject_set_digest: &str,
    revision: u64,
    operation: eg_types::change_envelope::MaterialOperation,
) -> eg_types::change_envelope::PolicyRecord {
    eg_types::change_envelope::PolicyRecord {
        policy_id: format!("policy:{}", object_id),
        operation,
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
            mapping_reference: "manifest:demo#schema_mappings/item".into(),
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
            false,
        )
        .unwrap();
        assert_eq!(properties.get("type").unwrap(), "Document");
        assert_eq!(properties.get("label").unwrap(), "seven");
        assert!(!properties.contains_key("id"));
    }

    #[test]
    fn mapping_accepts_optional_fields_and_refuses_reserved_targets() {
        let missing = BTreeMap::from([("absent".to_string(), "label".to_string())]);
        assert!(mapped_properties(
            &record(),
            "Document",
            &missing,
            "manifest:demo#schema_mappings/item",
            &"a".repeat(64),
            false,
        )
        .is_ok());

        let reserved = BTreeMap::from([("name".to_string(), "source_raw_sha256".to_string())]);
        assert!(mapped_properties(
            &record(),
            "Document",
            &reserved,
            "manifest:demo#schema_mappings/item",
            &"a".repeat(64),
            false,
        )
        .unwrap_err()
        .contains("targets"));
    }

    #[test]
    fn strict_schema_and_datatype_mapping_fail_closed() {
        let xsd = BTreeMap::from([("id".to_string(), "xsd:string".to_string())]);
        assert!(mapped_properties(
            &record(),
            "Document",
            &xsd,
            "manifest:demo#schema_mappings/item",
            &"a".repeat(64),
            false,
        )
        .unwrap_err()
        .contains("SOURCE_SCHEMA_TYPE_MISMATCH"));

        let strict = BTreeMap::from([("name".to_string(), "xsd:string".to_string())]);
        assert!(mapped_properties(
            &record(),
            "Document",
            &strict,
            "manifest:demo#schema_mappings/item",
            &"a".repeat(64),
            true,
        )
        .unwrap_err()
        .contains("SOURCE_SCHEMA_FIELD_UNKNOWN"));
    }

    #[test]
    fn declared_node_type_must_match_the_exact_mapping_selector() {
        let mut selected = record();
        selected.payload = SourceJson::new(serde_json::json!({
            "node_type": "other",
            "name": "seven"
        }))
        .unwrap();
        assert!(mapped_properties(
            &selected,
            "Document",
            &BTreeMap::from([("name".to_string(), "xsd:string".to_string())]),
            "manifest:demo#schema_mappings/item",
            &"a".repeat(64),
            true,
        )
        .unwrap_err()
        .contains("SOURCE_MAPPING_SELECTOR_MISMATCH"));
    }
}
