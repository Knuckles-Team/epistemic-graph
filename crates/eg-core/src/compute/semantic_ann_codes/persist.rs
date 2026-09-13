//! Owner-row persistence inside an admitted write: stage artifacts,
//! generation checkpoints and their CAS-protected heads.
//!
//! Every function here takes an `AdmittedOwnerWrite`, which only the
//! serving-scope door can open.

use super::checkpoint::{
    authoritative_generation_state, current_checkpoint_from_tables, GenerationCoordinates,
};
use super::record::{encode, row_bytes, CanonicalRow};
use super::tombstone::{is_reconciled_sql_tombstone, replace_reconciled_sql_manifest};
use super::{corrupt, ensure, kernel_error, refused, semantic_contract_error, SemanticCodeError};
use eg_storage::{
    SemanticIndexOwner, SEMANTIC_ANN, SEMANTIC_AUTH_RECEIPTS, SEMANTIC_CHECKPOINTS,
    SEMANTIC_CHECKPOINT_HEADS, SEMANTIC_DEAD_LETTERS, SEMANTIC_GRAPH_PROJECTIONS, SEMANTIC_LEXICAL,
    SEMANTIC_SOURCE_PROGRESS, SEMANTIC_SQL_SOURCES, SEMANTIC_STAGES, SEMANTIC_VECTORS,
};
use eg_transaction::AdmittedOwnerWrite;
use eg_types::mutation_batch::MutationOutboxLease;
use eg_types::semantic_index::{
    SemanticAuthorizationReceipt, SemanticDigest, SemanticGenerationAggregate,
    SemanticGenerationArtifact, SemanticGenerationCheckpoint, SemanticGenerationCheckpointDraft,
    SemanticGenerationCheckpointUpdate, SemanticGenerationDependency, SemanticGenerationMember,
    SemanticSqlSourceManifest, SemanticStage, SemanticStageArtifact, SemanticStageIntent,
    SemanticStagePredecessor, SemanticStageTransition,
};
use redb::{ReadableTable, TableDefinition};
use std::collections::BTreeMap;

type OwnerRows<'r> = AdmittedOwnerWrite<'r, SemanticIndexOwner>;
type EntityKey<'a> = (&'a str, &'a str, u64, &'a str);

pub(super) fn persist_transition_checkpoints(
    rows: &OwnerRows<'_>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    successor: Option<&SemanticStageIntent>,
) -> Result<(), SemanticCodeError> {
    if transition.intent.stage == SemanticStage::Vector {
        persist_vector_checkpoint(rows, tenant, binding, transition, successor)?;
    }
    if let SemanticStagePredecessor::GenerationCoverage { checkpoint } =
        &transition.intent.predecessor
    {
        let at = GenerationCoordinates::of_checkpoint(tenant, binding, checkpoint);
        let current = current_checkpoint_in(rows, at, SemanticStage::Vector)?;
        ensure(
            current.as_ref() == Some(&**checkpoint) && checkpoint.require_complete().is_ok(),
            "semantic checkpoint predecessor is not the current durable head",
        )?;
    }
    if let Some(update) = &transition.generation_checkpoint {
        persist_generation_checkpoint_update(rows, tenant, binding, transition, update)?;
    }
    Ok(())
}

/// A completed S4 advances the vector coverage checkpoint by exactly its own
/// entity, and its S5 successor must carry exactly that checkpoint.
fn persist_vector_checkpoint(
    rows: &OwnerRows<'_>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    successor: Option<&SemanticStageIntent>,
) -> Result<(), SemanticCodeError> {
    let coverage = vector_coverage(successor)?;
    let lexical_checkpoint_digest = lexical_predecessor_digest(transition)?;
    let at = GenerationCoordinates::of_intent(tenant, binding, &transition.intent);
    let lexical = current_checkpoint_in(rows, at, SemanticStage::LexicalIndex)?
        .ok_or_else(|| refused("S4 transition has no current lexical generation checkpoint"))?;
    ensure(
        lexical.checkpoint_digest == lexical_checkpoint_digest
            && lexical.require_complete().is_ok(),
        "S4 transition lexical checkpoint is stale or incomplete",
    )?;
    let previous_vector = current_checkpoint_in(rows, at, SemanticStage::Vector)?;
    let previous_completed_count = previous_vector
        .as_ref()
        .map_or(0, |checkpoint| checkpoint.aggregate.completed_entity_count);
    let current_entity = transition
        .intent
        .scope
        .source_entity_id()
        .ok_or_else(|| refused("S4 transition must be entity-scoped"))?;
    let (aggregate, members) =
        authoritative_state_in(rows, at, SemanticStage::Vector, Some(current_entity))?;
    ensure(
        aggregate.completed_entity_count == previous_completed_count.saturating_add(1),
        "S4 vector checkpoint is stale relative to authoritative entity progress",
    )?;
    let member = members.get(current_entity).ok_or_else(|| {
        refused("S4 vector checkpoint has no authoritative current entity member")
    })?;
    let expected = vector_checkpoint(transition, aggregate, lexical_checkpoint_digest)?;
    ensure(
        coverage == &expected,
        "S4 successor coverage is not the authoritative vector checkpoint",
    )?;
    ensure(
        (member.receipt_digest, member.artifact_digest)
            == (
                transition.receipt.receipt_digest(),
                transition.receipt.output_digest,
            ),
        "S4 vector checkpoint member differs from the durable transition",
    )?;
    store_checkpoint(rows, tenant, binding, &expected, previous_vector.as_ref())
}

pub(super) fn persist_generation_checkpoint_update(
    rows: &OwnerRows<'_>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    update: &SemanticGenerationCheckpointUpdate,
) -> Result<(), SemanticCodeError> {
    update.validate().map_err(semantic_contract_error)?;
    let stage = transition.intent.stage;
    ensure(
        matches!(stage, SemanticStage::LexicalIndex | SemanticStage::AnnIndex),
        "only S3 and S5 may advance a generation checkpoint",
    )?;
    let at = GenerationCoordinates::of_intent(tenant, binding, &transition.intent);
    let current = current_checkpoint_in(rows, at, stage)?;
    ensure_checkpoint_cas(current.as_ref(), update)?;
    let current_entity = transition.intent.scope.source_entity_id();
    let (aggregate, members) = authoritative_state_in(rows, at, stage, current_entity)?;
    let member = current_entity
        .and_then(|source_entity_id| members.get(source_entity_id))
        .ok_or_else(|| {
            refused("semantic checkpoint update has no authoritative current entity member")
        })?;
    ensure(
        member == &update.member
            && aggregate.completed_entity_count
                == update.expected_previous_completed_count.saturating_add(1),
        "semantic checkpoint successor does not match authoritative entity progress",
    )?;
    ensure(
        with_aggregate(&update.successor, aggregate)? == update.successor,
        "semantic checkpoint successor digest or aggregate is not authoritative",
    )?;
    store_checkpoint(rows, tenant, binding, &update.successor, current.as_ref())
}

pub(super) fn persist_stage_artifact_with_lease(
    rows: &OwnerRows<'_>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    lease: Option<&MutationOutboxLease>,
    artifact: &SemanticStageArtifact,
) -> Result<(), SemanticCodeError> {
    let entity_key = |message: &str| {
        let source_entity_id = transition
            .intent
            .scope
            .source_entity_id()
            .ok_or_else(|| refused(message))?;
        Ok::<_, SemanticCodeError>((
            tenant,
            binding,
            transition.intent.generation,
            source_entity_id,
        ))
    };
    match artifact {
        SemanticStageArtifact::None => Ok(()),
        SemanticStageArtifact::SqlSourceManifest {
            manifest,
            authorization,
        } => {
            let key = entity_key("SQL source artifact requires an entity-scoped transition")?;
            persist_sql_source_manifest(rows, key, manifest, transition, lease)?;
            persist_authorization_receipt(rows, key, authorization, transition, lease)
        }
        SemanticStageArtifact::GraphProjectionManifest { manifest } => {
            let key = entity_key("graph projection artifact requires an entity-scoped transition")?;
            put_row_once(rows, SEMANTIC_GRAPH_PROJECTIONS, key, manifest.as_ref())
        }
        SemanticStageArtifact::Vector { vector } => {
            let key = entity_key("vector artifact requires an entity-scoped transition")?;
            put_row_once(rows, SEMANTIC_VECTORS, key, vector.as_ref())
        }
        SemanticStageArtifact::DeadLetter { dead_letter } => {
            let intent_digest = dead_letter.intent.intent_digest.to_string();
            put_row_once(
                rows,
                SEMANTIC_DEAD_LETTERS,
                (tenant, binding, intent_digest.as_str(), dead_letter.attempt),
                dead_letter.as_ref(),
            )
        }
    }
}

pub(super) fn persist_generation_artifact(
    rows: &OwnerRows<'_>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    artifact: &SemanticGenerationArtifact,
) -> Result<(), SemanticCodeError> {
    let checkpoint = transition
        .generation_checkpoint
        .as_ref()
        .ok_or_else(|| refused("generation artifact has no transition checkpoint"))?;
    artifact
        .validate_against(&checkpoint.successor)
        .map_err(semantic_contract_error)?;
    let key = (tenant, binding, transition.intent.generation);
    match artifact {
        SemanticGenerationArtifact::LexicalIndexManifest { manifest } => {
            put_row_once(rows, SEMANTIC_LEXICAL, key, manifest.as_ref())
        }
        SemanticGenerationArtifact::AnnIndexManifest { manifest } => {
            put_row_once(rows, SEMANTIC_ANN, key, manifest.as_ref())
        }
        SemanticGenerationArtifact::Activation { .. } => Err(refused(
            "activation artifacts belong to the admitted S6 finalization path",
        )),
    }
}

/// Write one checkpoint row once and advance its stage head from `previous`.
pub(super) fn store_checkpoint(
    rows: &OwnerRows<'_>,
    tenant: &str,
    binding: &str,
    checkpoint: &SemanticGenerationCheckpoint,
    previous: Option<&SemanticGenerationCheckpoint>,
) -> Result<(), SemanticCodeError> {
    let key = checkpoint.checkpoint_digest.to_string();
    put_row_once(
        rows,
        SEMANTIC_CHECKPOINTS,
        (tenant, binding, checkpoint.generation, key.as_str()),
        checkpoint,
    )?;
    advance_checkpoint_head(rows, tenant, binding, checkpoint, previous)
}

pub(super) fn advance_checkpoint_head(
    rows: &OwnerRows<'_>,
    tenant: &str,
    binding: &str,
    successor: &SemanticGenerationCheckpoint,
    previous: Option<&SemanticGenerationCheckpoint>,
) -> Result<(), SemanticCodeError> {
    successor.validate().map_err(semantic_contract_error)?;
    let successor_bytes = encode(successor)?;
    let checkpoint_key = successor.checkpoint_digest.to_string();
    let checkpoint = row_bytes(
        rows.open_table(SEMANTIC_CHECKPOINTS)
            .map_err(kernel_error)?
            .get((
                tenant,
                binding,
                successor.generation,
                checkpoint_key.as_str(),
            )),
    )?
    .ok_or_else(|| corrupt("semantic checkpoint head advance has no checkpoint row"))?;
    if checkpoint != successor_bytes {
        return Err(corrupt(
            "semantic checkpoint head advance does not match checkpoint row bytes",
        ));
    }
    let mut heads = rows
        .open_table(SEMANTIC_CHECKPOINT_HEADS)
        .map_err(kernel_error)?;
    let head_key = (
        tenant,
        binding,
        successor.generation,
        successor.source_revision.as_str(),
        successor.stage.as_str(),
    );
    let existing = row_bytes(heads.get(head_key))?;
    match (previous, existing.as_deref()) {
        (Some(previous), Some(existing)) => {
            ensure(
                existing == encode(previous)?.as_slice(),
                "semantic checkpoint head CAS predecessor is stale",
            )?;
        }
        (Some(_), None) => {
            return Err(refused(
                "semantic checkpoint head CAS predecessor is absent",
            ));
        }
        (None, Some(existing)) if existing != successor_bytes.as_slice() => {
            return Err(refused(
                "semantic checkpoint head already names another successor",
            ));
        }
        (None, Some(_)) => return Ok(()),
        (None, None) => {}
    }
    replace_bytes(&mut heads, head_key, &successor_bytes)
}

pub(super) fn put_bytes_once<'k, K>(
    table: &mut redb::Table<'_, K, &[u8]>,
    key: K::SelfType<'k>,
    bytes: &[u8],
) -> Result<(), SemanticCodeError>
where
    K: redb::Key + 'static,
{
    if let Some(existing) = table
        .get(&key)
        .map_err(kernel_error)?
        .map(|value| value.value().to_vec())
    {
        if existing != bytes {
            return Err(refused(
                "semantic artifact key already names different bytes",
            ));
        }
        return Ok(());
    }
    table.insert(&key, bytes).map_err(kernel_error)?;
    Ok(())
}

pub(super) fn replace_bytes<'k, K>(
    table: &mut redb::Table<'_, K, &[u8]>,
    key: K::SelfType<'k>,
    bytes: &[u8],
) -> Result<(), SemanticCodeError>
where
    K: redb::Key + 'static,
{
    table.insert(&key, bytes).map_err(kernel_error)?;
    Ok(())
}

/// Encode `record` and write it under `key` of `definition`, once.
fn put_row_once<'k, K, R>(
    rows: &OwnerRows<'_>,
    definition: TableDefinition<'static, K, &'static [u8]>,
    key: K::SelfType<'k>,
    record: &R,
) -> Result<(), SemanticCodeError>
where
    K: redb::Key + 'static,
    R: CanonicalRow,
{
    let bytes = encode(record)?;
    let mut table = rows.open_table(definition).map_err(kernel_error)?;
    put_bytes_once(&mut table, key, &bytes)
}

/// The current head of `stage` in the generation, read inside the write.
fn current_checkpoint_in(
    rows: &OwnerRows<'_>,
    at: GenerationCoordinates<'_>,
    stage: SemanticStage,
) -> Result<Option<SemanticGenerationCheckpoint>, SemanticCodeError> {
    let checkpoints = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?;
    let heads = rows
        .open_table(SEMANTIC_CHECKPOINT_HEADS)
        .map_err(kernel_error)?;
    current_checkpoint_from_tables(&checkpoints, &heads, at, stage)
}

/// The generation's authoritative `stage` aggregate and members, read inside
/// the write.
fn authoritative_state_in(
    rows: &OwnerRows<'_>,
    at: GenerationCoordinates<'_>,
    stage: SemanticStage,
    current_entity: Option<&str>,
) -> Result<
    (
        SemanticGenerationAggregate,
        BTreeMap<String, SemanticGenerationMember>,
    ),
    SemanticCodeError,
> {
    let source_progress = rows
        .open_table(SEMANTIC_SOURCE_PROGRESS)
        .map_err(kernel_error)?;
    let stages = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
    authoritative_generation_state(&source_progress, &stages, at, stage, stage, current_entity)
}

/// The checkpoint an update's successor must be before it may replace the
/// current head: absent only from an empty start, otherwise exactly the
/// count and digest the update names.
fn ensure_checkpoint_cas(
    current: Option<&SemanticGenerationCheckpoint>,
    update: &SemanticGenerationCheckpointUpdate,
) -> Result<(), SemanticCodeError> {
    let expected = (
        update.expected_previous_completed_count,
        update.expected_previous_checkpoint_digest,
    );
    match current {
        None => ensure(
            expected == (0, None),
            "semantic checkpoint CAS predecessor is absent",
        ),
        Some(previous) => ensure(
            (
                previous.aggregate.completed_entity_count,
                Some(previous.checkpoint_digest),
            ) == expected,
            "semantic checkpoint CAS predecessor is stale",
        ),
    }
}

/// `checkpoint` recomputed over `aggregate`, so its digest is authoritative.
fn with_aggregate(
    checkpoint: &SemanticGenerationCheckpoint,
    aggregate: SemanticGenerationAggregate,
) -> Result<SemanticGenerationCheckpoint, SemanticCodeError> {
    SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
        binding_id: checkpoint.binding_id.clone(),
        binding_digest: checkpoint.binding_digest,
        generation: checkpoint.generation,
        source_revision: checkpoint.source_revision.clone(),
        stage: checkpoint.stage,
        aggregate,
        dependency: checkpoint.dependency.clone(),
        artifact_digest: checkpoint.artifact_digest,
        completed_at: checkpoint.completed_at.clone(),
    })
    .map_err(semantic_contract_error)
}

/// The vector coverage checkpoint S4 must hand its S5 successor.
fn vector_checkpoint(
    transition: &SemanticStageTransition,
    aggregate: SemanticGenerationAggregate,
    lexical_checkpoint_digest: SemanticDigest,
) -> Result<SemanticGenerationCheckpoint, SemanticCodeError> {
    let intent = &transition.intent;
    SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
        binding_id: intent.binding_id.clone(),
        binding_digest: intent.binding_digest,
        generation: intent.generation,
        source_revision: intent.source_revision.clone(),
        stage: SemanticStage::Vector,
        artifact_digest: aggregate.aggregate_artifact_digest,
        aggregate,
        dependency: SemanticGenerationDependency::Checkpoint {
            stage: SemanticStage::LexicalIndex,
            checkpoint_digest: lexical_checkpoint_digest,
        },
        completed_at: transition.receipt.completed_at.clone(),
    })
    .map_err(semantic_contract_error)
}

fn vector_coverage(
    successor: Option<&SemanticStageIntent>,
) -> Result<&SemanticGenerationCheckpoint, SemanticCodeError> {
    let successor = successor
        .ok_or_else(|| refused("completed S4 transition has no generation coverage successor"))?;
    let SemanticStagePredecessor::GenerationCoverage { checkpoint } = &successor.predecessor else {
        return Err(refused(
            "S4 successor has no generation coverage checkpoint",
        ));
    };
    Ok(checkpoint)
}

fn lexical_predecessor_digest(
    transition: &SemanticStageTransition,
) -> Result<SemanticDigest, SemanticCodeError> {
    match &transition.intent.predecessor {
        SemanticStagePredecessor::GenerationCheckpoint {
            stage: SemanticStage::LexicalIndex,
            checkpoint_digest,
        } => Ok(*checkpoint_digest),
        _ => Err(refused(
            "S4 transition has no lexical checkpoint predecessor",
        )),
    }
}

fn persist_sql_source_manifest(
    rows: &OwnerRows<'_>,
    key: EntityKey<'_>,
    manifest: &SemanticSqlSourceManifest,
    transition: &SemanticStageTransition,
    lease: Option<&MutationOutboxLease>,
) -> Result<(), SemanticCodeError> {
    let manifest_bytes = encode(manifest)?;
    let mut manifests = rows
        .open_table(SEMANTIC_SQL_SOURCES)
        .map_err(kernel_error)?;
    match row_bytes(manifests.get(key))? {
        None => {
            manifests
                .insert(key, manifest_bytes.as_slice())
                .map_err(kernel_error)?;
            Ok(())
        }
        Some(existing) if existing == manifest_bytes => Ok(()),
        Some(existing) => replace_reconciled_sql_manifest(
            &mut manifests,
            key,
            &existing,
            manifest,
            &manifest_bytes,
            transition,
            lease,
        ),
    }
}

fn persist_authorization_receipt(
    rows: &OwnerRows<'_>,
    key: EntityKey<'_>,
    authorization: &SemanticAuthorizationReceipt,
    transition: &SemanticStageTransition,
    lease: Option<&MutationOutboxLease>,
) -> Result<(), SemanticCodeError> {
    let receipt_bytes = encode(authorization)?;
    let mut receipts = rows
        .open_table(SEMANTIC_AUTH_RECEIPTS)
        .map_err(kernel_error)?;
    match row_bytes(receipts.get(key))? {
        None => {
            receipts
                .insert(key, receipt_bytes.as_slice())
                .map_err(kernel_error)?;
            Ok(())
        }
        Some(existing) if existing == receipt_bytes => Ok(()),
        Some(_) if is_reconciled_sql_tombstone(transition, lease)? => {
            replace_bytes(&mut receipts, key, &receipt_bytes)
        }
        Some(_) => Err(refused(
            "semantic authorization receipt key already names different bytes",
        )),
    }
}
