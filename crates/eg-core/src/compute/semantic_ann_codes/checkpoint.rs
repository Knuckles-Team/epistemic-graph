//! Generation checkpoint reads: the durable head, the authoritative
//! generation state and the S6 checkpoint proof.

use super::reconciliation::SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY;
use super::stage::is_stage_receipt_index_key;
use super::{kernel_error, semantic_contract_error, SemanticCodeError};
use eg_types::semantic_index::{
    SemanticDigest, SemanticExpectedEntity, SemanticGenerationAggregate,
    SemanticGenerationCheckpoint, SemanticGenerationDependency, SemanticGenerationMember,
    SemanticIndexMutation, SemanticSourceProgress, SemanticStage, SemanticStageOutcome,
};
use std::collections::BTreeMap;

/// Where one generation's rows live, and the binding identity those rows must
/// agree with.
///
/// Fifteen call sites plucked exactly these five off a
/// `SemanticGenerationCheckpoint`, a `SemanticBinding` or a
/// `SemanticStageTransition` and passed them one by one.
/// `tenant`/`binding`/`generation` are the row-key prefix of every
/// per-generation semantic table; `source_revision` selects the checkpoint head
/// inside that prefix; and `binding_digest` is the identity the rows found
/// there must carry, so a row written under a DIFFERENT binding of the same
/// name is refused rather than read. The digest travelling with the key is the
/// point of the grouping: a lookup given only the key could not tell those two
/// apart, and every caller that has the key has the digest.
pub(super) struct GenerationCoordinates<'a> {
    pub(super) tenant: &'a str,
    pub(super) binding: &'a str,
    pub(super) generation: u64,
    pub(super) binding_digest: SemanticDigest,
    pub(super) source_revision: &'a str,
}

pub(super) fn current_checkpoint_from_tables<TC, TH>(
    checkpoint_table: &TC,
    head_table: &TH,
    at: GenerationCoordinates<'_>,
    stage: SemanticStage,
) -> Result<Option<SemanticGenerationCheckpoint>, SemanticCodeError>
where
    TC: redb::ReadableTable<(&'static str, &'static str, u64, &'static str), &'static [u8]>,
    TH: redb::ReadableTable<
        (&'static str, &'static str, u64, &'static str, &'static str),
        &'static [u8],
    >,
{
    let GenerationCoordinates {
        tenant,
        binding,
        generation,
        binding_digest,
        source_revision,
    } = at;
    let Some(head_bytes) = head_table
        .get((tenant, binding, generation, source_revision, stage.as_str()))
        .map_err(kernel_error)?
        .map(|value| value.value().to_vec())
    else {
        return Ok(None);
    };
    let head = SemanticGenerationCheckpoint::from_canonical_cbor(&head_bytes)
        .map_err(semantic_contract_error)?;
    head.validate().map_err(semantic_contract_error)?;
    if head.binding_id != binding
        || head.binding_digest != binding_digest
        || head.generation != generation
        || head.source_revision != source_revision
        || head.stage != stage
    {
        return Err(SemanticCodeError::Corrupt(
            "semantic checkpoint head pointer coordinates do not match its key".to_string(),
        ));
    }
    let checkpoint_key = head.checkpoint_digest.to_string();
    let checkpoint_bytes = checkpoint_table
        .get((tenant, binding, generation, checkpoint_key.as_str()))
        .map_err(kernel_error)?
        .map(|value| value.value().to_vec())
        .ok_or_else(|| {
            SemanticCodeError::Corrupt(
                "semantic checkpoint head points to a missing checkpoint row".to_string(),
            )
        })?;
    if checkpoint_bytes != head_bytes {
        return Err(SemanticCodeError::Corrupt(
            "semantic checkpoint head bytes differ from its checkpoint row".to_string(),
        ));
    }
    let checkpoint = SemanticGenerationCheckpoint::from_canonical_cbor(&checkpoint_bytes)
        .map_err(semantic_contract_error)?;
    if checkpoint != head || checkpoint.checkpoint_digest.to_string() != checkpoint_key {
        return Err(SemanticCodeError::Corrupt(
            "semantic checkpoint head key or row bytes do not match the checkpoint digest"
                .to_string(),
        ));
    }
    Ok(Some(checkpoint))
}

pub(super) fn authoritative_generation_state<TP, TS>(
    source_progress_table: &TP,
    stage_table: &TS,
    at: GenerationCoordinates<'_>,
    member_stage: SemanticStage,
    aggregate_stage: SemanticStage,
    current_entity: Option<&str>,
) -> Result<
    (
        SemanticGenerationAggregate,
        BTreeMap<String, SemanticGenerationMember>,
    ),
    SemanticCodeError,
>
where
    TP: redb::ReadableTable<(&'static str, &'static str, u64, &'static str), &'static [u8]>,
    TS: redb::ReadableTable<(&'static str, &'static str, &'static str), &'static [u8]>,
{
    let GenerationCoordinates {
        tenant,
        binding,
        generation,
        binding_digest,
        source_revision,
    } = at;
    let mut progress_by_entity = BTreeMap::new();
    let progress_rows = source_progress_table
        .range((tenant, binding, generation, "")..)
        .map_err(kernel_error)?;
    for row in progress_rows {
        let (key, value) = row.map_err(kernel_error)?;
        let key = key.value();
        if key.0 != tenant || key.1 != binding || key.2 != generation {
            break;
        }
        if key.3 == SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY {
            continue;
        }
        let progress = SemanticSourceProgress::from_canonical_cbor(value.value())
            .map_err(semantic_contract_error)?;
        if progress.binding_id != binding
            || progress.binding_digest != binding_digest
            || progress.generation != generation
            || progress.source_entity_id != key.3
            || progress.source_revision != source_revision
        {
            return Err(SemanticCodeError::Refused(
                "semantic checkpoint source-progress row is outside the current generation"
                    .to_string(),
            ));
        }
        if progress.superseded_by_revision.is_some() {
            return Err(SemanticCodeError::Refused(
                "semantic checkpoint source-progress row is superseded".to_string(),
            ));
        }
        if progress_by_entity
            .insert(progress.source_entity_id.clone(), progress)
            .is_some()
        {
            return Err(SemanticCodeError::Corrupt(
                "semantic checkpoint source-progress rows duplicate an entity".to_string(),
            ));
        }
    }
    if progress_by_entity.is_empty() {
        return Err(SemanticCodeError::Refused(
            "semantic checkpoint has no authoritative source-progress entities".to_string(),
        ));
    }

    let mut members_by_entity = BTreeMap::new();
    let stage_rows = stage_table
        .range((tenant, binding, "")..)
        .map_err(kernel_error)?;
    for row in stage_rows {
        let (key, value) = row.map_err(kernel_error)?;
        let key = key.value();
        if key.0 != tenant || key.1 != binding {
            break;
        }
        // Receipt index rows carry the same canonical transition bytes as the
        // intent row. They are direct proof lookups, not additional completed
        // members of the generation aggregate.
        if is_stage_receipt_index_key(key.2) {
            continue;
        }
        let mutation = SemanticIndexMutation::from_canonical_cbor(value.value())
            .map_err(semantic_contract_error)?;
        let SemanticIndexMutation::RecordStageTransition { transition, .. } = mutation else {
            continue;
        };
        if transition.intent.binding_id != binding
            || transition.intent.binding_digest != binding_digest
            || transition.intent.generation != generation
            || transition.intent.source_revision != source_revision
            || transition.intent.stage != member_stage
        {
            continue;
        }
        let Some(source_entity_id) = transition.intent.scope.source_entity_id() else {
            continue;
        };
        if !matches!(
            transition.receipt.outcome,
            SemanticStageOutcome::Completed | SemanticStageOutcome::IdempotentNoop
        ) {
            continue;
        }
        let member = SemanticGenerationMember {
            source_entity_id: source_entity_id.to_string(),
            source_revision: source_revision.to_string(),
            receipt_digest: transition.receipt.receipt_digest(),
            artifact_digest: transition.receipt.output_digest,
        };
        if members_by_entity
            .insert(member.source_entity_id.clone(), member)
            .is_some()
        {
            return Err(SemanticCodeError::Corrupt(
                "semantic checkpoint stage rows contain duplicate completed entities".to_string(),
            ));
        }
    }

    let mut expected = Vec::with_capacity(progress_by_entity.len());
    let mut completed = Vec::new();
    for (source_entity_id, progress) in &progress_by_entity {
        expected.push(SemanticExpectedEntity {
            source_entity_id: source_entity_id.clone(),
            source_revision: source_revision.to_string(),
        });
        let is_current_entity = current_entity == Some(source_entity_id.as_str());
        let completed_at_stage = progress
            .completed_stage
            .is_some_and(|completed| completed >= member_stage);
        if completed_at_stage || is_current_entity {
            let member = members_by_entity.get(source_entity_id).ok_or_else(|| {
                SemanticCodeError::Refused(
                    "semantic checkpoint has completed source progress without a stage receipt"
                        .to_string(),
                )
            })?;
            if progress.completed_stage == Some(member_stage)
                && progress.completed_receipt_digest != Some(member.receipt_digest)
            {
                return Err(SemanticCodeError::Refused(
                    "semantic checkpoint source-progress receipt differs from its stage receipt"
                        .to_string(),
                ));
            }
            completed.push(member.clone());
        } else if members_by_entity.contains_key(source_entity_id) {
            return Err(SemanticCodeError::Refused(
                "semantic checkpoint has an uncommitted stage receipt for an incomplete entity"
                    .to_string(),
            ));
        }
    }
    for source_entity_id in members_by_entity.keys() {
        if !progress_by_entity.contains_key(source_entity_id) {
            return Err(SemanticCodeError::Refused(
                "semantic checkpoint stage receipt names an omitted entity".to_string(),
            ));
        }
    }
    let aggregate = SemanticGenerationAggregate::create(
        binding,
        binding_digest,
        generation,
        source_revision,
        aggregate_stage,
        expected,
        completed,
    )
    .map_err(semantic_contract_error)?;
    Ok((aggregate, members_by_entity))
}

pub(super) fn validate_six_checkpoint_from_tables<TC, TH, TP, TS>(
    checkpoint_table: &TC,
    checkpoint_head_table: &TH,
    source_progress_table: &TP,
    stage_table: &TS,
    tenant: &str,
    binding: &str,
    checkpoint: &SemanticGenerationCheckpoint,
) -> Result<(), SemanticCodeError>
where
    TC: redb::ReadableTable<(&'static str, &'static str, u64, &'static str), &'static [u8]>,
    TH: redb::ReadableTable<
        (&'static str, &'static str, u64, &'static str, &'static str),
        &'static [u8],
    >,
    TP: redb::ReadableTable<(&'static str, &'static str, u64, &'static str), &'static [u8]>,
    TS: redb::ReadableTable<(&'static str, &'static str, &'static str), &'static [u8]>,
{
    checkpoint
        .require_complete()
        .map_err(semantic_contract_error)?;
    if checkpoint.binding_id != binding || checkpoint.stage != SemanticStage::ReconcileAndActivate {
        return Err(SemanticCodeError::Refused(
            "S6 checkpoint is outside the current semantic binding".to_string(),
        ));
    }
    let lexical = current_checkpoint_from_tables(
        checkpoint_table,
        checkpoint_head_table,
        GenerationCoordinates {
            tenant,
            binding,
            generation: checkpoint.generation,
            binding_digest: checkpoint.binding_digest,
            source_revision: &checkpoint.source_revision,
        },
        SemanticStage::LexicalIndex,
    )?
    .ok_or_else(|| {
        SemanticCodeError::Refused(
            "S6 checkpoint has no current lexical generation checkpoint".to_string(),
        )
    })?;
    lexical
        .require_complete()
        .map_err(semantic_contract_error)?;
    let ann = current_checkpoint_from_tables(
        checkpoint_table,
        checkpoint_head_table,
        GenerationCoordinates {
            tenant,
            binding,
            generation: checkpoint.generation,
            binding_digest: checkpoint.binding_digest,
            source_revision: &checkpoint.source_revision,
        },
        SemanticStage::AnnIndex,
    )?
    .ok_or_else(|| {
        SemanticCodeError::Refused(
            "S6 checkpoint has no current ANN generation checkpoint".to_string(),
        )
    })?;
    ann.require_complete().map_err(semantic_contract_error)?;
    let SemanticGenerationDependency::Activation {
        lexical_checkpoint_digest,
        ann_checkpoint_digest,
    } = &checkpoint.dependency
    else {
        return Err(SemanticCodeError::Refused(
            "S6 checkpoint has no exact lexical/ANN dependency proof".to_string(),
        ));
    };
    if *lexical_checkpoint_digest != lexical.checkpoint_digest
        || *ann_checkpoint_digest != ann.checkpoint_digest
    {
        return Err(SemanticCodeError::Refused(
            "S6 checkpoint dependency is not the current lexical/ANN head".to_string(),
        ));
    }
    let (aggregate, _) = authoritative_generation_state(
        source_progress_table,
        stage_table,
        GenerationCoordinates {
            tenant,
            binding,
            generation: checkpoint.generation,
            binding_digest: checkpoint.binding_digest,
            source_revision: &checkpoint.source_revision,
        },
        SemanticStage::AnnIndex,
        SemanticStage::ReconcileAndActivate,
        None,
    )?;
    if checkpoint.aggregate != aggregate {
        return Err(SemanticCodeError::Refused(
            "S6 checkpoint aggregate is not the authoritative current generation state".to_string(),
        ));
    }
    Ok(())
}
