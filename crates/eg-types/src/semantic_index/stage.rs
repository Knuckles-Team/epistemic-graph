use serde::{Deserialize, Serialize};

use super::digest::{
    cbor, domain_digest, SemanticDigest, SEMANTIC_STAGE_INTENT_DIGEST_DOMAIN,
    SEMANTIC_STAGE_RECEIPT_DIGEST_DOMAIN,
};
use super::generation::{
    SemanticGenerationCheckpoint, SemanticGenerationCheckpointUpdate, SemanticGenerationDependency,
    SemanticStageScope,
};
use super::identity::{validate_generation, validate_text};
use super::state::SemanticIndexError;

pub const SEMANTIC_QUEUE_PROFILE_ID: &str = "semantic-index-queue/v1";
pub const SEMANTIC_FAST_CAPACITY: u32 = 8_192;
pub const SEMANTIC_MEDIUM_CAPACITY: u32 = 2_048;
pub const SEMANTIC_SLOW_HEAVY_CAPACITY: u32 = 256;
pub const SEMANTIC_LIGHT_CONCURRENCY: u32 = 32;
pub const SEMANTIC_MEDIUM_CONCURRENCY: u32 = 8;
pub const SEMANTIC_HEAVY_CONCURRENCY: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SemanticStage {
    #[serde(rename = "S1")]
    SourceCommit,
    #[serde(rename = "S2")]
    GraphProjection,
    #[serde(rename = "S3")]
    LexicalIndex,
    #[serde(rename = "S4")]
    Vector,
    #[serde(rename = "S5")]
    AnnIndex,
    #[serde(rename = "S6")]
    ReconcileAndActivate,
}

impl SemanticStage {
    pub fn as_str(self) -> &'static str {
        const IDS: [&str; 6] = ["S1", "S2", "S3", "S4", "S5", "S6"];
        IDS[self as usize]
    }

    pub fn predecessor(self) -> Option<Self> {
        match self {
            Self::SourceCommit => None,
            Self::GraphProjection => Some(Self::SourceCommit),
            Self::LexicalIndex => Some(Self::GraphProjection),
            Self::Vector => Some(Self::LexicalIndex),
            Self::AnnIndex => Some(Self::Vector),
            Self::ReconcileAndActivate => Some(Self::AnnIndex),
        }
    }

    pub fn queue_class(self) -> SemanticQueueClass {
        match self {
            Self::SourceCommit => SemanticQueueClass::Fast,
            Self::GraphProjection | Self::LexicalIndex => SemanticQueueClass::Medium,
            Self::Vector | Self::AnnIndex | Self::ReconcileAndActivate => {
                SemanticQueueClass::SlowHeavy
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SemanticQueueClass {
    Fast,
    Medium,
    SlowHeavy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SemanticStageOutcome {
    Completed,
    IdempotentNoop,
    DeferredBackpressured,
    ParkedAwaitingPredecessor,
    RejectedDeadLetter,
}

impl SemanticStageOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::IdempotentNoop => "idempotent_noop",
            Self::DeferredBackpressured => "deferred_backpressured",
            Self::ParkedAwaitingPredecessor => "parked_awaiting_predecessor",
            Self::RejectedDeadLetter => "rejected_dead_letter",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "proof", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SemanticStagePredecessor {
    None,
    EntityReceipt {
        stage: SemanticStage,
        receipt_digest: SemanticDigest,
    },
    GenerationCheckpoint {
        stage: SemanticStage,
        checkpoint_digest: SemanticDigest,
    },
    GenerationCoverage {
        checkpoint: Box<SemanticGenerationCheckpoint>,
    },
    Activation {
        lexical_checkpoint_digest: SemanticDigest,
        ann_checkpoint_digest: SemanticDigest,
    },
}

impl SemanticStagePredecessor {
    pub(crate) fn canonical_cbor(&self) -> Vec<u8> {
        match self {
            Self::None => cbor::map([("proof", cbor::text("none"))]),
            Self::EntityReceipt {
                stage,
                receipt_digest,
            } => cbor::map([
                ("proof", cbor::text("entity_receipt")),
                ("stage", cbor::text(stage.as_str())),
                ("receipt_digest", cbor::digest(*receipt_digest)),
            ]),
            Self::GenerationCheckpoint {
                stage,
                checkpoint_digest,
            } => cbor::map([
                ("proof", cbor::text("generation_checkpoint")),
                ("stage", cbor::text(stage.as_str())),
                ("checkpoint_digest", cbor::digest(*checkpoint_digest)),
            ]),
            Self::GenerationCoverage { checkpoint } => cbor::map([
                ("proof", cbor::text("generation_coverage")),
                ("checkpoint", checkpoint.canonical_cbor_subject()),
            ]),
            Self::Activation {
                lexical_checkpoint_digest,
                ann_checkpoint_digest,
            } => cbor::map([
                ("proof", cbor::text("activation")),
                (
                    "lexical_checkpoint_digest",
                    cbor::digest(*lexical_checkpoint_digest),
                ),
                (
                    "ann_checkpoint_digest",
                    cbor::digest(*ann_checkpoint_digest),
                ),
            ]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticStageIntent {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub scope: SemanticStageScope,
    pub source_revision: String,
    pub stage: SemanticStage,
    pub predecessor: SemanticStagePredecessor,
    pub input_digest: SemanticDigest,
    pub intent_digest: SemanticDigest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticStageIntentDraft {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub scope: SemanticStageScope,
    pub source_revision: String,
    pub stage: SemanticStage,
    pub predecessor: SemanticStagePredecessor,
    pub input_digest: SemanticDigest,
}

impl SemanticStageIntent {
    pub fn create(draft: SemanticStageIntentDraft) -> Result<Self, SemanticIndexError> {
        let intent_digest = stage_intent_digest(&draft);
        let intent = Self {
            binding_id: draft.binding_id,
            binding_digest: draft.binding_digest,
            generation: draft.generation,
            scope: draft.scope,
            source_revision: draft.source_revision,
            stage: draft.stage,
            predecessor: draft.predecessor,
            input_digest: draft.input_digest,
            intent_digest,
        };
        intent.validate()?;
        Ok(intent)
    }

    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        validate_text("binding_id", &self.binding_id)?;
        validate_generation(self.generation)?;
        self.scope.validate()?;
        validate_text("source_revision", &self.source_revision)?;
        validate_stage_scope(self.stage, &self.scope)?;
        validate_predecessor(self.stage, &self.predecessor)?;
        if let SemanticStagePredecessor::GenerationCoverage { checkpoint } = &self.predecessor {
            if checkpoint.binding_id != self.binding_id
                || checkpoint.binding_digest != self.binding_digest
                || checkpoint.generation != self.generation
                || checkpoint.source_revision != self.source_revision
            {
                return Err(SemanticIndexError::GenerationCheckpointMismatch);
            }
        }
        if self.intent_digest != stage_intent_digest_from_record(self) {
            return Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_stage_intent".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticStageReceipt {
    pub intent_digest: SemanticDigest,
    pub output_digest: SemanticDigest,
    pub cursor: String,
    pub completed_at: String,
    pub outcome: SemanticStageOutcome,
}

impl SemanticStageReceipt {
    pub fn receipt_digest(&self) -> SemanticDigest {
        let subject = cbor::map([
            ("intent_digest", cbor::digest(self.intent_digest)),
            ("output_digest", cbor::digest(self.output_digest)),
            ("cursor", cbor::text(&self.cursor)),
            ("completed_at", cbor::text(&self.completed_at)),
            ("outcome", cbor::text(self.outcome.as_str())),
        ]);
        domain_digest(SEMANTIC_STAGE_RECEIPT_DIGEST_DOMAIN, subject)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticStageTransition {
    pub intent: SemanticStageIntent,
    pub receipt: SemanticStageReceipt,
    pub generation_checkpoint: Option<Box<SemanticGenerationCheckpointUpdate>>,
}

impl SemanticStageTransition {
    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        self.intent.validate()?;
        validate_text("receipt_cursor", &self.receipt.cursor)?;
        validate_text("receipt_completed_at", &self.receipt.completed_at)?;
        if self.receipt.intent_digest != self.intent.intent_digest {
            return Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_stage_transition".to_string(),
            });
        }
        self.validate_checkpoint_update()
    }

    pub fn intent_digest(&self) -> SemanticDigest {
        self.intent.intent_digest
    }

    fn validate_checkpoint_update(&self) -> Result<(), SemanticIndexError> {
        let needs_update = self.receipt.outcome == SemanticStageOutcome::Completed
            && matches!(
                self.intent.stage,
                SemanticStage::LexicalIndex | SemanticStage::AnnIndex
            );
        if needs_update != self.generation_checkpoint.is_some() {
            return Err(SemanticIndexError::GenerationCheckpointMismatch);
        }
        let Some(update) = &self.generation_checkpoint else {
            return Ok(());
        };
        update.validate()?;
        let checkpoint = &update.successor;
        checkpoint.validate()?;
        validate_checkpoint_coordinates(&self.intent, checkpoint)?;
        validate_checkpoint_member(&self.intent, &self.receipt, update)?;
        if checkpoint_dependency_matches_intent(&self.intent, checkpoint) {
            Ok(())
        } else {
            Err(SemanticIndexError::GenerationCheckpointMismatch)
        }
    }
}

fn validate_checkpoint_coordinates(
    intent: &SemanticStageIntent,
    checkpoint: &SemanticGenerationCheckpoint,
) -> Result<(), SemanticIndexError> {
    if checkpoint.binding_id != intent.binding_id
        || checkpoint.binding_digest != intent.binding_digest
        || checkpoint.generation != intent.generation
        || checkpoint.source_revision != intent.source_revision
        || checkpoint.stage != intent.stage
    {
        return Err(SemanticIndexError::GenerationCheckpointMismatch);
    }
    Ok(())
}

fn validate_checkpoint_member(
    intent: &SemanticStageIntent,
    receipt: &SemanticStageReceipt,
    update: &SemanticGenerationCheckpointUpdate,
) -> Result<(), SemanticIndexError> {
    let source_entity_id = intent
        .scope
        .source_entity_id()
        .ok_or(SemanticIndexError::GenerationCheckpointMismatch)?;
    if update.member.source_entity_id != source_entity_id
        || update.member.source_revision != intent.source_revision
        || update.member.receipt_digest != receipt.receipt_digest()
        || update.member.artifact_digest != receipt.output_digest
        || update.successor.completed_at != receipt.completed_at
    {
        return Err(SemanticIndexError::GenerationCheckpointMismatch);
    }
    Ok(())
}

fn checkpoint_dependency_matches_intent(
    intent: &SemanticStageIntent,
    checkpoint: &SemanticGenerationCheckpoint,
) -> bool {
    match (&intent.predecessor, &checkpoint.dependency) {
        (
            SemanticStagePredecessor::GenerationCoverage {
                checkpoint: coverage,
            },
            SemanticGenerationDependency::Checkpoint {
                stage: SemanticStage::Vector,
                checkpoint_digest,
            },
        ) => *checkpoint_digest == coverage.checkpoint_digest,
        (_, SemanticGenerationDependency::None) => intent.stage == SemanticStage::LexicalIndex,
        _ => false,
    }
}

fn validate_stage_scope(
    stage: SemanticStage,
    scope: &SemanticStageScope,
) -> Result<(), SemanticIndexError> {
    let valid = match stage {
        SemanticStage::SourceCommit
        | SemanticStage::GraphProjection
        | SemanticStage::LexicalIndex
        | SemanticStage::Vector
        | SemanticStage::AnnIndex => matches!(scope, SemanticStageScope::Entity { .. }),
        SemanticStage::ReconcileAndActivate => matches!(scope, SemanticStageScope::Generation),
    };
    if valid {
        Ok(())
    } else {
        Err(SemanticIndexError::StageScopeMismatch { stage })
    }
}

fn validate_predecessor(
    stage: SemanticStage,
    predecessor: &SemanticStagePredecessor,
) -> Result<(), SemanticIndexError> {
    let valid = match (stage, predecessor) {
        (SemanticStage::SourceCommit, SemanticStagePredecessor::None) => true,
        (
            SemanticStage::GraphProjection,
            SemanticStagePredecessor::EntityReceipt {
                stage: SemanticStage::SourceCommit,
                ..
            },
        ) => true,
        (
            SemanticStage::LexicalIndex,
            SemanticStagePredecessor::EntityReceipt {
                stage: SemanticStage::GraphProjection,
                ..
            },
        ) => true,
        (
            SemanticStage::Vector,
            SemanticStagePredecessor::GenerationCheckpoint {
                stage: SemanticStage::LexicalIndex,
                ..
            },
        ) => true,
        (SemanticStage::AnnIndex, SemanticStagePredecessor::GenerationCoverage { checkpoint }) => {
            checkpoint.stage == SemanticStage::Vector && checkpoint.require_complete().is_ok()
        }
        (SemanticStage::ReconcileAndActivate, SemanticStagePredecessor::Activation { .. }) => true,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(SemanticIndexError::PredecessorReceiptMismatch { stage })
    }
}

fn stage_intent_digest(draft: &SemanticStageIntentDraft) -> SemanticDigest {
    stage_intent_digest_parts(
        (&draft.binding_id, draft.binding_digest, draft.generation),
        (&draft.scope, &draft.source_revision, draft.stage),
        (&draft.predecessor, draft.input_digest),
    )
}

fn stage_intent_digest_from_record(record: &SemanticStageIntent) -> SemanticDigest {
    stage_intent_digest_parts(
        (&record.binding_id, record.binding_digest, record.generation),
        (&record.scope, &record.source_revision, record.stage),
        (&record.predecessor, record.input_digest),
    )
}

fn stage_intent_digest_parts(
    identity: (&str, SemanticDigest, u64),
    source: (&SemanticStageScope, &str, SemanticStage),
    work: (&SemanticStagePredecessor, SemanticDigest),
) -> SemanticDigest {
    let subject = cbor::map([
        ("binding_id", cbor::text(identity.0)),
        ("binding_digest", cbor::digest(identity.1)),
        ("generation", cbor::unsigned(identity.2)),
        ("scope", source.0.canonical_cbor()),
        ("source_revision", cbor::text(source.1)),
        ("stage", cbor::text(source.2.as_str())),
        ("predecessor", work.0.canonical_cbor()),
        ("input_digest", cbor::digest(work.1)),
    ]);
    domain_digest(SEMANTIC_STAGE_INTENT_DIGEST_DOMAIN, subject)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::semantic_index::{
        SemanticExpectedEntity, SemanticGenerationAggregate, SemanticGenerationCheckpointDraft,
        SemanticGenerationDependency, SemanticGenerationMember,
    };

    fn digest(byte: u8) -> SemanticDigest {
        SemanticDigest::from_bytes([byte; 32])
    }

    fn vector_checkpoint(completed: usize) -> SemanticGenerationCheckpoint {
        let expected = ["entity:a", "entity:b"]
            .into_iter()
            .map(|source_entity_id| SemanticExpectedEntity {
                source_entity_id: source_entity_id.to_string(),
                source_revision: "r1".to_string(),
            })
            .collect();
        let members = ["entity:a", "entity:b"]
            .into_iter()
            .take(completed)
            .enumerate()
            .map(|(index, source_entity_id)| SemanticGenerationMember {
                source_entity_id: source_entity_id.to_string(),
                source_revision: "r1".to_string(),
                receipt_digest: digest(index as u8 + 1),
                artifact_digest: digest(index as u8 + 3),
            })
            .collect();
        let aggregate = SemanticGenerationAggregate::create(
            "binding-a",
            digest(7),
            1,
            "r1",
            SemanticStage::Vector,
            expected,
            members,
        )
        .unwrap();
        SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
            binding_id: "binding-a".to_string(),
            binding_digest: digest(7),
            generation: 1,
            source_revision: "r1".to_string(),
            stage: SemanticStage::Vector,
            artifact_digest: aggregate.aggregate_artifact_digest,
            aggregate,
            dependency: SemanticGenerationDependency::Checkpoint {
                stage: SemanticStage::LexicalIndex,
                checkpoint_digest: digest(8),
            },
            completed_at: "2026-09-04T00:00:00Z".to_string(),
        })
        .unwrap()
    }

    #[test]
    fn ann_work_requires_exact_full_vector_generation_coverage() {
        let draft = |checkpoint| SemanticStageIntentDraft {
            binding_id: "binding-a".to_string(),
            binding_digest: digest(7),
            generation: 1,
            scope: SemanticStageScope::Entity {
                source_entity_id: "entity:a".to_string(),
            },
            source_revision: "r1".to_string(),
            stage: SemanticStage::AnnIndex,
            predecessor: SemanticStagePredecessor::GenerationCoverage {
                checkpoint: Box::new(checkpoint),
            },
            input_digest: digest(9),
        };
        assert_eq!(
            SemanticStageIntent::create(draft(vector_checkpoint(1))),
            Err(SemanticIndexError::PredecessorReceiptMismatch {
                stage: SemanticStage::AnnIndex,
            })
        );
        assert!(SemanticStageIntent::create(draft(vector_checkpoint(2))).is_ok());
    }

    #[test]
    fn ann_checkpoint_must_bind_the_intents_exact_vector_coverage() {
        let coverage = vector_checkpoint(2);
        let intent = SemanticStageIntent::create(SemanticStageIntentDraft {
            binding_id: "binding-a".to_string(),
            binding_digest: digest(7),
            generation: 1,
            scope: SemanticStageScope::Entity {
                source_entity_id: "entity:a".to_string(),
            },
            source_revision: "r1".to_string(),
            stage: SemanticStage::AnnIndex,
            predecessor: SemanticStagePredecessor::GenerationCoverage {
                checkpoint: Box::new(coverage.clone()),
            },
            input_digest: digest(9),
        })
        .unwrap();
        let aggregate = SemanticGenerationAggregate::create(
            "binding-a",
            digest(7),
            1,
            "r1",
            SemanticStage::AnnIndex,
            ["entity:a", "entity:b"]
                .into_iter()
                .map(|source_entity_id| SemanticExpectedEntity {
                    source_entity_id: source_entity_id.to_string(),
                    source_revision: "r1".to_string(),
                })
                .collect(),
            vec![SemanticGenerationMember {
                source_entity_id: "entity:a".to_string(),
                source_revision: "r1".to_string(),
                receipt_digest: digest(10),
                artifact_digest: digest(11),
            }],
        )
        .unwrap();
        let checkpoint = |checkpoint_digest| {
            SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
                binding_id: "binding-a".to_string(),
                binding_digest: digest(7),
                generation: 1,
                source_revision: "r1".to_string(),
                stage: SemanticStage::AnnIndex,
                artifact_digest: aggregate.aggregate_artifact_digest,
                aggregate: aggregate.clone(),
                dependency: SemanticGenerationDependency::Checkpoint {
                    stage: SemanticStage::Vector,
                    checkpoint_digest,
                },
                completed_at: "2026-09-04T00:00:10Z".to_string(),
            })
            .unwrap()
        };
        let transition = |successor| {
            let receipt = SemanticStageReceipt {
                intent_digest: intent.intent_digest,
                output_digest: digest(11),
                cursor: "cursor-a".to_string(),
                completed_at: "2026-09-04T00:00:10Z".to_string(),
                outcome: SemanticStageOutcome::Completed,
            };
            SemanticStageTransition {
                intent: intent.clone(),
                generation_checkpoint: Some(Box::new(SemanticGenerationCheckpointUpdate {
                    expected_previous_checkpoint_digest: None,
                    expected_previous_completed_count: 0,
                    member: SemanticGenerationMember {
                        source_entity_id: "entity:a".to_string(),
                        source_revision: "r1".to_string(),
                        receipt_digest: receipt.receipt_digest(),
                        artifact_digest: receipt.output_digest,
                    },
                    successor,
                })),
                receipt,
            }
        };
        assert!(transition(checkpoint(coverage.checkpoint_digest))
            .validate()
            .is_ok());
        assert_eq!(
            transition(checkpoint(digest(99))).validate(),
            Err(SemanticIndexError::GenerationCheckpointMismatch)
        );
    }

    #[test]
    fn six_stage_order_and_queue_classes_remain_closed() {
        assert_eq!(SemanticStage::SourceCommit.predecessor(), None);
        assert_eq!(
            SemanticStage::ReconcileAndActivate.predecessor(),
            Some(SemanticStage::AnnIndex)
        );
        assert_eq!(
            SemanticStage::GraphProjection.queue_class(),
            SemanticQueueClass::Medium
        );
        assert_eq!(
            SemanticStage::AnnIndex.queue_class(),
            SemanticQueueClass::SlowHeavy
        );
    }

    #[test]
    fn completed_lexical_member_requires_atomic_checkpoint_update() {
        let aggregate = SemanticGenerationAggregate::create(
            "binding-a",
            digest(7),
            1,
            "r1",
            SemanticStage::LexicalIndex,
            ["entity:a", "entity:b"]
                .into_iter()
                .map(|source_entity_id| SemanticExpectedEntity {
                    source_entity_id: source_entity_id.to_string(),
                    source_revision: "r1".to_string(),
                })
                .collect(),
            vec![SemanticGenerationMember {
                source_entity_id: "entity:a".to_string(),
                source_revision: "r1".to_string(),
                receipt_digest: digest(1),
                artifact_digest: digest(2),
            }],
        )
        .unwrap();
        let checkpoint = SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
            binding_id: "binding-a".to_string(),
            binding_digest: digest(7),
            generation: 1,
            source_revision: "r1".to_string(),
            stage: SemanticStage::LexicalIndex,
            artifact_digest: aggregate.aggregate_artifact_digest,
            aggregate,
            dependency: SemanticGenerationDependency::None,
            completed_at: "2026-09-04T00:00:00Z".to_string(),
        })
        .unwrap();
        let intent = SemanticStageIntent::create(SemanticStageIntentDraft {
            binding_id: "binding-a".to_string(),
            binding_digest: digest(7),
            generation: 1,
            scope: SemanticStageScope::Entity {
                source_entity_id: "entity:a".to_string(),
            },
            source_revision: "r1".to_string(),
            stage: SemanticStage::LexicalIndex,
            predecessor: SemanticStagePredecessor::EntityReceipt {
                stage: SemanticStage::GraphProjection,
                receipt_digest: digest(3),
            },
            input_digest: digest(4),
        })
        .unwrap();
        let receipt = SemanticStageReceipt {
            intent_digest: intent.intent_digest,
            output_digest: digest(2),
            cursor: "cursor-a".to_string(),
            completed_at: "2026-09-04T00:00:00Z".to_string(),
            outcome: SemanticStageOutcome::Completed,
        };
        let transition = SemanticStageTransition {
            intent: intent.clone(),
            receipt: receipt.clone(),
            generation_checkpoint: Some(Box::new(SemanticGenerationCheckpointUpdate {
                expected_previous_checkpoint_digest: None,
                expected_previous_completed_count: 0,
                member: SemanticGenerationMember {
                    source_entity_id: "entity:a".to_string(),
                    source_revision: "r1".to_string(),
                    receipt_digest: receipt.receipt_digest(),
                    artifact_digest: receipt.output_digest,
                },
                successor: checkpoint,
            })),
        };
        assert!(transition.validate().is_ok());

        let successor = &transition.generation_checkpoint.as_ref().unwrap().successor;
        let wrong_time = SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
            binding_id: successor.binding_id.clone(),
            binding_digest: successor.binding_digest,
            generation: successor.generation,
            source_revision: successor.source_revision.clone(),
            stage: successor.stage,
            aggregate: successor.aggregate.clone(),
            dependency: successor.dependency.clone(),
            artifact_digest: successor.artifact_digest,
            completed_at: "2026-09-04T00:00:01Z".to_string(),
        })
        .unwrap();
        let mut altered_time = transition.clone();
        altered_time
            .generation_checkpoint
            .as_mut()
            .unwrap()
            .successor = wrong_time;
        assert_eq!(
            altered_time.validate(),
            Err(SemanticIndexError::GenerationCheckpointMismatch)
        );

        let mut altered_member = transition.clone();
        altered_member
            .generation_checkpoint
            .as_mut()
            .unwrap()
            .member
            .artifact_digest = digest(99);
        assert_eq!(
            altered_member.validate(),
            Err(SemanticIndexError::GenerationCheckpointMismatch)
        );

        let mut regressed_cas = transition;
        let update = regressed_cas.generation_checkpoint.as_mut().unwrap();
        update.expected_previous_completed_count = 1;
        update.expected_previous_checkpoint_digest = Some(digest(98));
        assert_eq!(
            regressed_cas.validate(),
            Err(SemanticIndexError::GenerationCheckpointMismatch)
        );
        assert_eq!(
            SemanticStageTransition {
                intent,
                receipt,
                generation_checkpoint: None,
            }
            .validate(),
            Err(SemanticIndexError::GenerationCheckpointMismatch)
        );
    }
}
