//! Stage predecessor proofs: a stage may run or be recorded only on top of
//! the durable receipt, checkpoint and manifests its predecessor names.

use super::checkpoint::{
    current_checkpoint_from_tables, validate_six_checkpoint_from_tables, GenerationCoordinates,
};
use super::{kernel_error, semantic_contract_error, SemanticCodeError, SemanticCodeStore};
use eg_storage::{
    ScopedRead, SemanticIndexOwner, SEMANTIC_ANN, SEMANTIC_CHECKPOINTS, SEMANTIC_CHECKPOINT_HEADS,
    SEMANTIC_LEXICAL, SEMANTIC_SOURCE_PROGRESS, SEMANTIC_STAGES,
};
use eg_transaction::{AdmittedMutation, AdmittedOwnerWrite};
use eg_types::semantic_index::{
    SemanticAnnIndexManifest, SemanticBinding, SemanticDigest, SemanticGenerationCheckpoint,
    SemanticLexicalIndexManifest, SemanticSourceProgress, SemanticStage, SemanticStageIntent,
    SemanticStagePredecessor,
};

impl SemanticCodeStore {
    pub(super) fn validate_stage_predecessor_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        intent: &SemanticStageIntent,
        reject_completed: bool,
    ) -> Result<(), SemanticCodeError> {
        match &intent.scope {
            eg_types::semantic_index::SemanticStageScope::Entity { source_entity_id } => {
                let raw = read
                    .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?
                    .get((
                        self.tenant.as_str(),
                        self.binding.as_str(),
                        intent.generation,
                        source_entity_id.as_str(),
                    ))
                    .map_err(kernel_error)?
                    .map(|value| value.value().to_vec())
                    .ok_or_else(|| {
                        SemanticCodeError::Refused(
                            "semantic stage lease has no durable source-progress row".to_string(),
                        )
                    })?;
                let progress = SemanticSourceProgress::from_canonical_cbor(&raw)
                    .map_err(semantic_contract_error)?;
                if progress.binding_digest != intent.binding_digest
                    || progress.generation != intent.generation
                    || progress.source_entity_id != *source_entity_id
                    || progress.source_revision != intent.source_revision
                {
                    return Err(SemanticCodeError::Refused(
                        "semantic stage lease source-progress proof does not match intent"
                            .to_string(),
                    ));
                }
                if progress.superseded_by_revision.is_some() {
                    return Err(SemanticCodeError::Refused(
                        "semantic stage lease names a superseded source revision".to_string(),
                    ));
                }
                if reject_completed
                    && progress
                        .completed_stage
                        .is_some_and(|completed| completed >= intent.stage)
                {
                    return Err(SemanticCodeError::Refused(
                        "semantic stage lease is already completed".to_string(),
                    ));
                }
                self.validate_predecessor_proof_in(read, intent, &Some(&progress))?;
            }
            eg_types::semantic_index::SemanticStageScope::Generation => {
                if intent.stage != SemanticStage::ReconcileAndActivate {
                    return Err(SemanticCodeError::Refused(
                        "only S6 may use a generation-scoped lease".to_string(),
                    ));
                }
                self.validate_predecessor_proof_in(read, intent, &None)?;
            }
        }
        Ok(())
    }

    fn validate_predecessor_proof_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        intent: &SemanticStageIntent,
        progress: &Option<&SemanticSourceProgress>,
    ) -> Result<(), SemanticCodeError> {
        let predecessor = &intent.predecessor;
        match predecessor {
            SemanticStagePredecessor::None => {
                if progress.is_some_and(|progress| progress.completed_stage.is_some()) {
                    return Err(SemanticCodeError::Refused(
                        "S1 source progress already has a completed predecessor".to_string(),
                    ));
                }
            }
            SemanticStagePredecessor::EntityReceipt {
                stage,
                receipt_digest,
            } => {
                let Some(progress) = progress else {
                    return Err(SemanticCodeError::Refused(
                        "entity receipt requires entity source progress".to_string(),
                    ));
                };
                if progress.completed_stage != Some(*stage)
                    || progress.completed_receipt_digest != Some(*receipt_digest)
                {
                    return Err(SemanticCodeError::Refused(
                        "entity predecessor receipt is not the durable receipt".to_string(),
                    ));
                }
            }
            SemanticStagePredecessor::GenerationCheckpoint {
                stage,
                checkpoint_digest,
            } => {
                if !progress.is_some_and(|progress| progress.completed_stage == Some(*stage))
                    || !self.has_checkpoint_in(read, intent, *checkpoint_digest, *stage)?
                {
                    return Err(SemanticCodeError::Refused(
                        "generation predecessor checkpoint is absent or stale".to_string(),
                    ));
                }
            }
            SemanticStagePredecessor::GenerationCoverage { checkpoint } => {
                checkpoint
                    .require_complete()
                    .map_err(semantic_contract_error)?;
                if !progress
                    .is_some_and(|progress| progress.completed_stage == Some(SemanticStage::Vector))
                    || !self.has_checkpoint_in(
                        read,
                        intent,
                        checkpoint.checkpoint_digest,
                        SemanticStage::Vector,
                    )?
                {
                    return Err(SemanticCodeError::Refused(
                        "generation coverage checkpoint is absent or stale".to_string(),
                    ));
                }
            }
            SemanticStagePredecessor::Activation {
                lexical_checkpoint_digest,
                ann_checkpoint_digest,
            } => {
                if !self.has_checkpoint_in(
                    read,
                    intent,
                    *lexical_checkpoint_digest,
                    SemanticStage::LexicalIndex,
                )? || !self.has_checkpoint_in(
                    read,
                    intent,
                    *ann_checkpoint_digest,
                    SemanticStage::AnnIndex,
                )? {
                    return Err(SemanticCodeError::Refused(
                        "S6 activation checkpoints are absent, mixed, or stale".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn has_checkpoint_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        intent: &SemanticStageIntent,
        digest: SemanticDigest,
        stage: SemanticStage,
    ) -> Result<bool, SemanticCodeError> {
        let checkpoints = read
            .open_owner_table(SEMANTIC_CHECKPOINTS)
            .map_err(kernel_error)?;
        let heads = read
            .open_owner_table(SEMANTIC_CHECKPOINT_HEADS)
            .map_err(kernel_error)?;
        let current = current_checkpoint_from_tables(
            &checkpoints,
            &heads,
            GenerationCoordinates {
                tenant: self.tenant.as_str(),
                binding: self.binding.as_str(),
                generation: intent.generation,
                binding_digest: intent.binding_digest,
                source_revision: &intent.source_revision,
            },
            stage,
        )?;
        Ok(current.is_some_and(|checkpoint| {
            checkpoint.checkpoint_digest == digest && checkpoint.require_complete().is_ok()
        }))
    }

    pub(super) fn validate_six_checkpoint_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        checkpoint: &SemanticGenerationCheckpoint,
    ) -> Result<(), SemanticCodeError> {
        let checkpoints = read
            .open_owner_table(SEMANTIC_CHECKPOINTS)
            .map_err(kernel_error)?;
        let heads = read
            .open_owner_table(SEMANTIC_CHECKPOINT_HEADS)
            .map_err(kernel_error)?;
        validate_six_checkpoint_from_tables(
            &checkpoints,
            &heads,
            &read
                .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
                .map_err(kernel_error)?,
            &read
                .open_owner_table(SEMANTIC_STAGES)
                .map_err(kernel_error)?,
            self.tenant.as_str(),
            self.binding.as_str(),
            checkpoint,
        )
    }

    pub(super) fn validate_generation_manifests_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        binding: &SemanticBinding,
        intent: &SemanticStageIntent,
    ) -> Result<(), SemanticCodeError> {
        let lexical_raw = read
            .open_owner_table(SEMANTIC_LEXICAL)
            .map_err(kernel_error)?
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                intent.generation,
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| {
                SemanticCodeError::Refused(
                    "S6 activation has no durable lexical index manifest".to_string(),
                )
            })?;
        let lexical = SemanticLexicalIndexManifest::from_canonical_cbor(&lexical_raw)
            .map_err(semantic_contract_error)?;
        lexical.validate().map_err(semantic_contract_error)?;
        if lexical.binding_id != binding.binding_id
            || lexical.binding_digest != binding.binding_digest
            || lexical.generation != binding.generation
            || lexical.source_revision != binding.source_revision
            || lexical.identity != binding.lexical_index_identity
        {
            return Err(SemanticCodeError::Refused(
                "S6 lexical manifest is not bound to the durable binding".to_string(),
            ));
        }

        let ann_raw = read
            .open_owner_table(SEMANTIC_ANN)
            .map_err(kernel_error)?
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                intent.generation,
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| {
                SemanticCodeError::Refused(
                    "S6 activation has no durable ANN index manifest".to_string(),
                )
            })?;
        let ann = SemanticAnnIndexManifest::from_canonical_cbor(&ann_raw)
            .map_err(semantic_contract_error)?;
        ann.validate().map_err(semantic_contract_error)?;
        if ann.binding_id != binding.binding_id
            || ann.binding_digest != binding.binding_digest
            || ann.generation != binding.generation
            || ann.source_revision != binding.source_revision
            || ann.identity != binding.ann_index_identity
        {
            return Err(SemanticCodeError::Refused(
                "S6 ANN manifest is not bound to the durable binding".to_string(),
            ));
        }
        Ok(())
    }
}

pub(super) fn validate_six_checkpoint_write(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    checkpoint: &SemanticGenerationCheckpoint,
) -> Result<(), SemanticCodeError> {
    let checkpoint_table = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?;
    let source_progress_table = rows
        .open_table(SEMANTIC_SOURCE_PROGRESS)
        .map_err(kernel_error)?;
    let stage_table = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
    let checkpoint_head_table = rows
        .open_table(SEMANTIC_CHECKPOINT_HEADS)
        .map_err(kernel_error)?;
    validate_six_checkpoint_from_tables(
        &checkpoint_table,
        &checkpoint_head_table,
        &source_progress_table,
        &stage_table,
        tenant,
        binding,
        checkpoint,
    )
}

pub(super) fn validate_stage_predecessor_write(
    write: &AdmittedMutation<'_, SemanticIndexOwner>,
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    intent: &SemanticStageIntent,
    reject_completed: bool,
) -> Result<(), SemanticCodeError> {
    let progress = if let Some(source_entity_id) = intent.scope.source_entity_id() {
        let raw = write
            .open_read_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?
            .get((tenant, binding, intent.generation, source_entity_id))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| {
                SemanticCodeError::Refused(
                    "semantic stage transition has no source-progress row".to_string(),
                )
            })?;
        let progress =
            SemanticSourceProgress::from_canonical_cbor(&raw).map_err(semantic_contract_error)?;
        if progress.binding_digest != intent.binding_digest
            || progress.generation != intent.generation
            || progress.source_entity_id != source_entity_id
            || progress.source_revision != intent.source_revision
        {
            return Err(SemanticCodeError::Refused(
                "semantic transition source-progress proof does not match intent".to_string(),
            ));
        }
        if progress.superseded_by_revision.is_some() {
            return Err(SemanticCodeError::Refused(
                "semantic transition names a superseded source revision".to_string(),
            ));
        }
        if reject_completed
            && progress
                .completed_stage
                .is_some_and(|completed| completed >= intent.stage)
        {
            return Err(SemanticCodeError::Refused(
                "semantic transition stage is already completed".to_string(),
            ));
        }
        Some(progress)
    } else {
        if intent.stage != SemanticStage::ReconcileAndActivate {
            return Err(SemanticCodeError::Refused(
                "only S6 may use a generation scope".to_string(),
            ));
        }
        None
    };
    let predecessor = &intent.predecessor;
    let checkpoint_table = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?;
    let checkpoint_head_table = rows
        .open_table(SEMANTIC_CHECKPOINT_HEADS)
        .map_err(kernel_error)?;
    let checkpoint = |digest: SemanticDigest,
                      stage: SemanticStage|
     -> Result<bool, SemanticCodeError> {
        Ok(current_checkpoint_from_tables(
            &checkpoint_table,
            &checkpoint_head_table,
            GenerationCoordinates {
                tenant,
                binding,
                generation: intent.generation,
                binding_digest: intent.binding_digest,
                source_revision: &intent.source_revision,
            },
            stage,
        )?
        .is_some_and(|value| value.checkpoint_digest == digest && value.require_complete().is_ok()))
    };
    match predecessor {
        SemanticStagePredecessor::None => {
            if progress.is_some_and(|value| value.completed_stage.is_some()) {
                return Err(SemanticCodeError::Refused(
                    "S1 transition has a completed predecessor".to_string(),
                ));
            }
        }
        SemanticStagePredecessor::EntityReceipt {
            stage,
            receipt_digest,
        } => {
            let Some(progress) = progress.as_ref() else {
                return Err(SemanticCodeError::Refused(
                    "entity receipt requires source progress".to_string(),
                ));
            };
            if progress.completed_stage != Some(*stage)
                || progress.completed_receipt_digest != Some(*receipt_digest)
            {
                return Err(SemanticCodeError::Refused(
                    "entity receipt is not the durable predecessor receipt".to_string(),
                ));
            }
        }
        SemanticStagePredecessor::GenerationCheckpoint {
            stage,
            checkpoint_digest,
        } => {
            if progress.is_none_or(|value| value.completed_stage != Some(*stage))
                || !checkpoint(*checkpoint_digest, *stage)?
            {
                return Err(SemanticCodeError::Refused(
                    "generation checkpoint is absent or mismatched".to_string(),
                ));
            }
        }
        SemanticStagePredecessor::GenerationCoverage { checkpoint: value } => {
            value.require_complete().map_err(semantic_contract_error)?;
            if progress.is_none_or(|row| row.completed_stage != Some(SemanticStage::Vector))
                || !checkpoint(value.checkpoint_digest, SemanticStage::Vector)?
            {
                return Err(SemanticCodeError::Refused(
                    "generation coverage is absent or mismatched".to_string(),
                ));
            }
        }
        SemanticStagePredecessor::Activation {
            lexical_checkpoint_digest,
            ann_checkpoint_digest,
        } => {
            if !checkpoint(*lexical_checkpoint_digest, SemanticStage::LexicalIndex)?
                || !checkpoint(*ann_checkpoint_digest, SemanticStage::AnnIndex)?
            {
                return Err(SemanticCodeError::Refused(
                    "S6 activation proof is absent or mixed-generation".to_string(),
                ));
            }
        }
    }
    Ok(())
}
