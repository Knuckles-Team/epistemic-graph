//! Owner-row persistence inside an admitted write: stage artifacts,
//! generation checkpoints and their CAS-protected heads.
//!
//! Every function here takes an `AdmittedOwnerWrite`, which only the
//! serving-scope door can open.

use super::checkpoint::{
    authoritative_generation_state, current_checkpoint_from_tables, GenerationCoordinates,
};
use super::tombstone::{is_reconciled_sql_tombstone, replace_reconciled_sql_manifest};
use super::{kernel_error, semantic_contract_error, SemanticCodeError};
use eg_storage::{
    SemanticIndexOwner, SEMANTIC_ANN, SEMANTIC_AUTH_RECEIPTS, SEMANTIC_CHECKPOINTS,
    SEMANTIC_CHECKPOINT_HEADS, SEMANTIC_DEAD_LETTERS, SEMANTIC_GRAPH_PROJECTIONS, SEMANTIC_LEXICAL,
    SEMANTIC_SOURCE_PROGRESS, SEMANTIC_SQL_SOURCES, SEMANTIC_STAGES, SEMANTIC_VECTORS,
};
use eg_transaction::AdmittedOwnerWrite;
use eg_types::mutation_batch::MutationOutboxLease;
use eg_types::semantic_index::{
    SemanticGenerationArtifact, SemanticGenerationCheckpoint, SemanticGenerationCheckpointDraft,
    SemanticGenerationCheckpointUpdate, SemanticGenerationDependency, SemanticStage,
    SemanticStageArtifact, SemanticStageIntent, SemanticStagePredecessor, SemanticStageTransition,
};
use redb::ReadableTable;

pub(super) fn persist_transition_checkpoints(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    successor: Option<&SemanticStageIntent>,
) -> Result<(), SemanticCodeError> {
    if transition.intent.stage == SemanticStage::Vector {
        persist_vector_checkpoint(rows, tenant, binding, transition, successor)?;
    }
    {
        let table = rows
            .open_table(SEMANTIC_CHECKPOINTS)
            .map_err(kernel_error)?;
        let head_table = rows
            .open_table(SEMANTIC_CHECKPOINT_HEADS)
            .map_err(kernel_error)?;
        let require_current = |checkpoint: &SemanticGenerationCheckpoint,
                               stage: SemanticStage|
         -> Result<(), SemanticCodeError> {
            let current = current_checkpoint_from_tables(
                &table,
                &head_table,
                GenerationCoordinates {
                    tenant,
                    binding,
                    generation: checkpoint.generation,
                    binding_digest: checkpoint.binding_digest,
                    source_revision: &checkpoint.source_revision,
                },
                stage,
            )?;
            if current.as_ref() != Some(checkpoint) || checkpoint.require_complete().is_err() {
                return Err(SemanticCodeError::Refused(
                    "semantic checkpoint predecessor is not the current durable head".to_string(),
                ));
            }
            Ok(())
        };
        if let SemanticStagePredecessor::GenerationCoverage { checkpoint } =
            &transition.intent.predecessor
        {
            require_current(checkpoint, SemanticStage::Vector)?;
        }
    }
    if let Some(update) = &transition.generation_checkpoint {
        persist_generation_checkpoint_update(rows, tenant, binding, transition, update)?;
    }
    Ok(())
}

fn persist_vector_checkpoint(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    successor: Option<&SemanticStageIntent>,
) -> Result<(), SemanticCodeError> {
    let successor = successor.ok_or_else(|| {
        SemanticCodeError::Refused(
            "completed S4 transition has no generation coverage successor".to_string(),
        )
    })?;
    let SemanticStagePredecessor::GenerationCoverage {
        checkpoint: coverage,
    } = &successor.predecessor
    else {
        return Err(SemanticCodeError::Refused(
            "S4 successor has no generation coverage checkpoint".to_string(),
        ));
    };
    let lexical_checkpoint_digest = match &transition.intent.predecessor {
        SemanticStagePredecessor::GenerationCheckpoint {
            stage: SemanticStage::LexicalIndex,
            checkpoint_digest,
        } => *checkpoint_digest,
        _ => {
            return Err(SemanticCodeError::Refused(
                "S4 transition has no lexical checkpoint predecessor".to_string(),
            ));
        }
    };
    let checkpoint_table = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?;
    let checkpoint_head_table = rows
        .open_table(SEMANTIC_CHECKPOINT_HEADS)
        .map_err(kernel_error)?;
    let lexical = current_checkpoint_from_tables(
        &checkpoint_table,
        &checkpoint_head_table,
        GenerationCoordinates {
            tenant,
            binding,
            generation: transition.intent.generation,
            binding_digest: transition.intent.binding_digest,
            source_revision: &transition.intent.source_revision,
        },
        SemanticStage::LexicalIndex,
    )?
    .ok_or_else(|| {
        SemanticCodeError::Refused(
            "S4 transition has no current lexical generation checkpoint".to_string(),
        )
    })?;
    if lexical.checkpoint_digest != lexical_checkpoint_digest || lexical.require_complete().is_err()
    {
        return Err(SemanticCodeError::Refused(
            "S4 transition lexical checkpoint is stale or incomplete".to_string(),
        ));
    }
    let previous_vector = current_checkpoint_from_tables(
        &checkpoint_table,
        &checkpoint_head_table,
        GenerationCoordinates {
            tenant,
            binding,
            generation: transition.intent.generation,
            binding_digest: transition.intent.binding_digest,
            source_revision: &transition.intent.source_revision,
        },
        SemanticStage::Vector,
    )?;
    let previous_completed_count = previous_vector
        .as_ref()
        .map_or(0, |checkpoint| checkpoint.aggregate.completed_entity_count);
    drop(checkpoint_head_table);
    drop(checkpoint_table);

    let source_progress_table = rows
        .open_table(SEMANTIC_SOURCE_PROGRESS)
        .map_err(kernel_error)?;
    let stage_table = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
    let current_entity = transition.intent.scope.source_entity_id().ok_or_else(|| {
        SemanticCodeError::Refused("S4 transition must be entity-scoped".to_string())
    })?;
    let (aggregate, members) = authoritative_generation_state(
        &source_progress_table,
        &stage_table,
        GenerationCoordinates {
            tenant,
            binding,
            generation: transition.intent.generation,
            binding_digest: transition.intent.binding_digest,
            source_revision: &transition.intent.source_revision,
        },
        SemanticStage::Vector,
        SemanticStage::Vector,
        Some(current_entity),
    )?;
    if aggregate.completed_entity_count != previous_completed_count.saturating_add(1) {
        return Err(SemanticCodeError::Refused(
            "S4 vector checkpoint is stale relative to authoritative entity progress".to_string(),
        ));
    }
    let member = members.get(current_entity).ok_or_else(|| {
        SemanticCodeError::Refused(
            "S4 vector checkpoint has no authoritative current entity member".to_string(),
        )
    })?;
    let expected = SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
        binding_id: transition.intent.binding_id.clone(),
        binding_digest: transition.intent.binding_digest,
        generation: transition.intent.generation,
        source_revision: transition.intent.source_revision.clone(),
        stage: SemanticStage::Vector,
        aggregate: aggregate.clone(),
        dependency: SemanticGenerationDependency::Checkpoint {
            stage: SemanticStage::LexicalIndex,
            checkpoint_digest: lexical_checkpoint_digest,
        },
        artifact_digest: aggregate.aggregate_artifact_digest,
        completed_at: transition.receipt.completed_at.clone(),
    })
    .map_err(semantic_contract_error)?;
    if coverage.as_ref() != &expected {
        return Err(SemanticCodeError::Refused(
            "S4 successor coverage is not the authoritative vector checkpoint".to_string(),
        ));
    }
    if member.receipt_digest != transition.receipt.receipt_digest()
        || member.artifact_digest != transition.receipt.output_digest
    {
        return Err(SemanticCodeError::Refused(
            "S4 vector checkpoint member differs from the durable transition".to_string(),
        ));
    }
    let bytes = expected
        .to_canonical_cbor()
        .map_err(semantic_contract_error)?;
    let key = expected.checkpoint_digest.to_string();
    let mut table = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?;
    put_bytes_once(
        &mut table,
        (tenant, binding, expected.generation, key.as_str()),
        &bytes,
    )?;
    drop(table);
    advance_checkpoint_head(rows, tenant, binding, &expected, previous_vector.as_ref())
}

pub(super) fn persist_generation_checkpoint_update(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    update: &SemanticGenerationCheckpointUpdate,
) -> Result<(), SemanticCodeError> {
    update.validate().map_err(semantic_contract_error)?;
    let stage = transition.intent.stage;
    if !matches!(stage, SemanticStage::LexicalIndex | SemanticStage::AnnIndex) {
        return Err(SemanticCodeError::Refused(
            "only S3 and S5 may advance a generation checkpoint".to_string(),
        ));
    }
    let checkpoint_table = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?;
    let checkpoint_head_table = rows
        .open_table(SEMANTIC_CHECKPOINT_HEADS)
        .map_err(kernel_error)?;
    let current = current_checkpoint_from_tables(
        &checkpoint_table,
        &checkpoint_head_table,
        GenerationCoordinates {
            tenant,
            binding,
            generation: transition.intent.generation,
            binding_digest: transition.intent.binding_digest,
            source_revision: &transition.intent.source_revision,
        },
        stage,
    )?;
    match current.as_ref() {
        None if update.expected_previous_completed_count != 0
            || update.expected_previous_checkpoint_digest.is_some() =>
        {
            return Err(SemanticCodeError::Refused(
                "semantic checkpoint CAS predecessor is absent".to_string(),
            ));
        }
        None => {}
        Some(previous)
            if previous.aggregate.completed_entity_count
                != update.expected_previous_completed_count
                || Some(previous.checkpoint_digest)
                    != update.expected_previous_checkpoint_digest =>
        {
            return Err(SemanticCodeError::Refused(
                "semantic checkpoint CAS predecessor is stale".to_string(),
            ));
        }
        Some(_) => {}
    }
    drop(checkpoint_head_table);
    drop(checkpoint_table);

    let source_progress_table = rows
        .open_table(SEMANTIC_SOURCE_PROGRESS)
        .map_err(kernel_error)?;
    let stage_table = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
    let current_entity = transition.intent.scope.source_entity_id();
    let (aggregate, members) = authoritative_generation_state(
        &source_progress_table,
        &stage_table,
        GenerationCoordinates {
            tenant,
            binding,
            generation: transition.intent.generation,
            binding_digest: transition.intent.binding_digest,
            source_revision: &transition.intent.source_revision,
        },
        stage,
        stage,
        current_entity,
    )?;
    let member = current_entity
        .and_then(|source_entity_id| members.get(source_entity_id))
        .ok_or_else(|| {
            SemanticCodeError::Refused(
                "semantic checkpoint update has no authoritative current entity member".to_string(),
            )
        })?;
    if member != &update.member
        || aggregate.completed_entity_count
            != update.expected_previous_completed_count.saturating_add(1)
    {
        return Err(SemanticCodeError::Refused(
            "semantic checkpoint successor does not match authoritative entity progress"
                .to_string(),
        ));
    }
    let expected_successor =
        SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
            binding_id: update.successor.binding_id.clone(),
            binding_digest: update.successor.binding_digest,
            generation: update.successor.generation,
            source_revision: update.successor.source_revision.clone(),
            stage: update.successor.stage,
            aggregate,
            dependency: update.successor.dependency.clone(),
            artifact_digest: update.successor.artifact_digest,
            completed_at: update.successor.completed_at.clone(),
        })
        .map_err(semantic_contract_error)?;
    if expected_successor != update.successor {
        return Err(SemanticCodeError::Refused(
            "semantic checkpoint successor digest or aggregate is not authoritative".to_string(),
        ));
    }

    let mut table = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?;
    let key = update.successor.checkpoint_digest.to_string();
    let bytes = update
        .successor
        .to_canonical_cbor()
        .map_err(semantic_contract_error)?;
    put_bytes_once(
        &mut table,
        (tenant, binding, update.successor.generation, key.as_str()),
        &bytes,
    )?;
    drop(table);
    advance_checkpoint_head(rows, tenant, binding, &update.successor, current.as_ref())
}

pub(super) fn persist_stage_artifact_with_lease(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    lease: Option<&MutationOutboxLease>,
    artifact: &SemanticStageArtifact,
) -> Result<(), SemanticCodeError> {
    match artifact {
        SemanticStageArtifact::None => Ok(()),
        SemanticStageArtifact::SqlSourceManifest {
            manifest,
            authorization,
        } => {
            let source_entity_id = transition.intent.scope.source_entity_id().ok_or_else(|| {
                SemanticCodeError::Refused(
                    "SQL source artifact requires an entity-scoped transition".to_string(),
                )
            })?;
            let mut manifests = rows
                .open_table(SEMANTIC_SQL_SOURCES)
                .map_err(kernel_error)?;
            let manifest_bytes = manifest
                .to_canonical_cbor()
                .map_err(semantic_contract_error)?;
            let manifest_key = (
                tenant,
                binding,
                transition.intent.generation,
                source_entity_id,
            );
            let existing_manifest = manifests
                .get(manifest_key)
                .map_err(kernel_error)?
                .map(|value| value.value().to_vec());
            match existing_manifest {
                None => {
                    manifests
                        .insert(manifest_key, manifest_bytes.as_slice())
                        .map_err(kernel_error)?;
                }
                Some(existing) if existing == manifest_bytes => {}
                Some(existing) => {
                    replace_reconciled_sql_manifest(
                        &mut manifests,
                        manifest_key,
                        &existing,
                        manifest,
                        &manifest_bytes,
                        transition,
                        lease,
                    )?;
                }
            }
            let mut receipts = rows
                .open_table(SEMANTIC_AUTH_RECEIPTS)
                .map_err(kernel_error)?;
            let receipt_bytes = authorization
                .to_canonical_cbor()
                .map_err(semantic_contract_error)?;
            let receipt_key = (
                tenant,
                binding,
                transition.intent.generation,
                source_entity_id,
            );
            let existing_receipt = receipts
                .get(receipt_key)
                .map_err(kernel_error)?
                .map(|value| value.value().to_vec());
            match existing_receipt {
                None => {
                    receipts
                        .insert(receipt_key, receipt_bytes.as_slice())
                        .map_err(kernel_error)?;
                    Ok(())
                }
                Some(existing) if existing == receipt_bytes => Ok(()),
                Some(_) if is_reconciled_sql_tombstone(transition, lease)? => {
                    replace_bytes(&mut receipts, receipt_key, &receipt_bytes)
                }
                Some(_) => Err(SemanticCodeError::Refused(
                    "semantic authorization receipt key already names different bytes".to_string(),
                )),
            }
        }
        SemanticStageArtifact::GraphProjectionManifest { manifest } => {
            let source_entity_id = transition.intent.scope.source_entity_id().ok_or_else(|| {
                SemanticCodeError::Refused(
                    "graph projection artifact requires an entity-scoped transition".to_string(),
                )
            })?;
            let mut table = rows
                .open_table(SEMANTIC_GRAPH_PROJECTIONS)
                .map_err(kernel_error)?;
            let bytes = manifest
                .to_canonical_cbor()
                .map_err(semantic_contract_error)?;
            put_bytes_once(
                &mut table,
                (
                    tenant,
                    binding,
                    transition.intent.generation,
                    source_entity_id,
                ),
                &bytes,
            )
        }
        SemanticStageArtifact::Vector { vector } => {
            let source_entity_id = transition.intent.scope.source_entity_id().ok_or_else(|| {
                SemanticCodeError::Refused(
                    "vector artifact requires an entity-scoped transition".to_string(),
                )
            })?;
            let mut table = rows.open_table(SEMANTIC_VECTORS).map_err(kernel_error)?;
            let bytes = vector
                .to_canonical_cbor()
                .map_err(semantic_contract_error)?;
            put_bytes_once(
                &mut table,
                (
                    tenant,
                    binding,
                    transition.intent.generation,
                    source_entity_id,
                ),
                &bytes,
            )
        }
        SemanticStageArtifact::DeadLetter { dead_letter } => {
            let mut table = rows
                .open_table(SEMANTIC_DEAD_LETTERS)
                .map_err(kernel_error)?;
            let bytes = dead_letter
                .to_canonical_cbor()
                .map_err(semantic_contract_error)?;
            let intent_digest = dead_letter.intent.intent_digest.to_string();
            put_bytes_once(
                &mut table,
                (tenant, binding, intent_digest.as_str(), dead_letter.attempt),
                &bytes,
            )
        }
    }
}

pub(super) fn persist_generation_artifact(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    artifact: &SemanticGenerationArtifact,
) -> Result<(), SemanticCodeError> {
    let checkpoint = transition.generation_checkpoint.as_ref().ok_or_else(|| {
        SemanticCodeError::Refused("generation artifact has no transition checkpoint".to_string())
    })?;
    artifact
        .validate_against(&checkpoint.successor)
        .map_err(semantic_contract_error)?;
    match artifact {
        SemanticGenerationArtifact::LexicalIndexManifest { manifest } => {
            let bytes = manifest
                .to_canonical_cbor()
                .map_err(semantic_contract_error)?;
            let mut table = rows.open_table(SEMANTIC_LEXICAL).map_err(kernel_error)?;
            put_bytes_once(
                &mut table,
                (tenant, binding, transition.intent.generation),
                &bytes,
            )
        }
        SemanticGenerationArtifact::AnnIndexManifest { manifest } => {
            let bytes = manifest
                .to_canonical_cbor()
                .map_err(semantic_contract_error)?;
            let mut table = rows.open_table(SEMANTIC_ANN).map_err(kernel_error)?;
            put_bytes_once(
                &mut table,
                (tenant, binding, transition.intent.generation),
                &bytes,
            )
        }
        SemanticGenerationArtifact::Activation { .. } => Err(SemanticCodeError::Refused(
            "activation artifacts belong to the admitted S6 finalization path".to_string(),
        )),
    }
}

pub(super) fn advance_checkpoint_head(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    successor: &SemanticGenerationCheckpoint,
    previous: Option<&SemanticGenerationCheckpoint>,
) -> Result<(), SemanticCodeError> {
    successor.validate().map_err(semantic_contract_error)?;
    let successor_bytes = successor
        .to_canonical_cbor()
        .map_err(semantic_contract_error)?;
    let checkpoint_key = successor.checkpoint_digest.to_string();
    let checkpoint = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?
        .get((
            tenant,
            binding,
            successor.generation,
            checkpoint_key.as_str(),
        ))
        .map_err(kernel_error)?
        .map(|value| value.value().to_vec())
        .ok_or_else(|| {
            SemanticCodeError::Corrupt(
                "semantic checkpoint head advance has no checkpoint row".to_string(),
            )
        })?;
    if checkpoint != successor_bytes {
        return Err(SemanticCodeError::Corrupt(
            "semantic checkpoint head advance does not match checkpoint row bytes".to_string(),
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
    let existing = heads
        .get(head_key)
        .map_err(kernel_error)?
        .map(|value| value.value().to_vec());
    match (previous, existing.as_deref()) {
        (Some(previous), Some(existing)) => {
            let previous_bytes = previous
                .to_canonical_cbor()
                .map_err(semantic_contract_error)?;
            if existing != previous_bytes.as_slice() {
                return Err(SemanticCodeError::Refused(
                    "semantic checkpoint head CAS predecessor is stale".to_string(),
                ));
            }
        }
        (Some(_), None) => {
            return Err(SemanticCodeError::Refused(
                "semantic checkpoint head CAS predecessor is absent".to_string(),
            ));
        }
        (None, Some(existing)) if existing != successor_bytes.as_slice() => {
            return Err(SemanticCodeError::Refused(
                "semantic checkpoint head already names another successor".to_string(),
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
            return Err(SemanticCodeError::Refused(
                "semantic artifact key already names different bytes".to_string(),
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
