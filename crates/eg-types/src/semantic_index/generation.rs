use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::digest::{
    cbor, domain_digest, SemanticDigest, SEMANTIC_AGGREGATE_ARTIFACT_DIGEST_DOMAIN,
    SEMANTIC_AGGREGATE_RECEIPT_DIGEST_DOMAIN, SEMANTIC_ENTITY_SET_DIGEST_DOMAIN,
    SEMANTIC_GENERATION_CHECKPOINT_DIGEST_DOMAIN,
};
use super::identity::{validate_generation, validate_text};
use super::stage::SemanticStage;
use super::state::SemanticIndexError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case", deny_unknown_fields)]
pub enum SemanticStageScope {
    Entity { source_entity_id: String },
    Generation,
}

impl SemanticStageScope {
    pub fn source_entity_id(&self) -> Option<&str> {
        match self {
            Self::Entity { source_entity_id } => Some(source_entity_id),
            Self::Generation => None,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), SemanticIndexError> {
        if let Self::Entity { source_entity_id } = self {
            validate_text("source_entity_id", source_entity_id)?;
        }
        Ok(())
    }

    pub(crate) fn canonical_cbor(&self) -> Vec<u8> {
        match self {
            Self::Entity { source_entity_id } => cbor::map([
                ("scope", cbor::text("entity")),
                ("source_entity_id", cbor::text(source_entity_id)),
            ]),
            Self::Generation => cbor::map([("scope", cbor::text("generation"))]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticExpectedEntity {
    pub source_entity_id: String,
    pub source_revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticGenerationMember {
    pub source_entity_id: String,
    pub source_revision: String,
    pub receipt_digest: SemanticDigest,
    pub artifact_digest: SemanticDigest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticGenerationAggregate {
    pub expected_entity_count: u64,
    pub completed_entity_count: u64,
    pub entity_set_digest: SemanticDigest,
    pub aggregate_receipt_digest: SemanticDigest,
    pub aggregate_artifact_digest: SemanticDigest,
}

impl SemanticGenerationAggregate {
    pub fn create(
        binding_id: &str,
        binding_digest: SemanticDigest,
        generation: u64,
        source_revision: &str,
        stage: SemanticStage,
        expected: Vec<SemanticExpectedEntity>,
        completed: Vec<SemanticGenerationMember>,
    ) -> Result<Self, SemanticIndexError> {
        validate_generation_coordinates(binding_id, generation, source_revision)?;
        if expected.is_empty() {
            return Err(invalid_count("expected_entity_count"));
        }
        let expected = canonical_expected(expected, source_revision)?;
        let completed = canonical_completed(completed, source_revision, &expected)?;
        let expected_entity_count =
            u64::try_from(expected.len()).map_err(|_| invalid_count("expected_entity_count"))?;
        let completed_entity_count =
            u64::try_from(completed.len()).map_err(|_| invalid_count("completed_entity_count"))?;
        Ok(Self {
            expected_entity_count,
            completed_entity_count,
            entity_set_digest: entity_set_digest(
                binding_id,
                binding_digest,
                generation,
                source_revision,
                &expected,
            ),
            aggregate_receipt_digest: member_digest(
                SEMANTIC_AGGREGATE_RECEIPT_DIGEST_DOMAIN,
                binding_id,
                binding_digest,
                generation,
                source_revision,
                stage,
                &completed,
                |member| member.receipt_digest,
            ),
            aggregate_artifact_digest: member_digest(
                SEMANTIC_AGGREGATE_ARTIFACT_DIGEST_DOMAIN,
                binding_id,
                binding_digest,
                generation,
                source_revision,
                stage,
                &completed,
                |member| member.artifact_digest,
            ),
        })
    }

    pub fn is_complete(&self) -> bool {
        self.expected_entity_count > 0 && self.completed_entity_count == self.expected_entity_count
    }

    pub(crate) fn validate(&self) -> Result<(), SemanticIndexError> {
        if self.expected_entity_count == 0
            || self.completed_entity_count > self.expected_entity_count
        {
            return Err(invalid_count("generation_aggregate"));
        }
        Ok(())
    }

    pub(crate) fn canonical_cbor(&self) -> Vec<u8> {
        cbor::map([
            (
                "expected_entity_count",
                cbor::unsigned(self.expected_entity_count),
            ),
            (
                "completed_entity_count",
                cbor::unsigned(self.completed_entity_count),
            ),
            ("entity_set_digest", cbor::digest(self.entity_set_digest)),
            (
                "aggregate_receipt_digest",
                cbor::digest(self.aggregate_receipt_digest),
            ),
            (
                "aggregate_artifact_digest",
                cbor::digest(self.aggregate_artifact_digest),
            ),
        ])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "dependency", rename_all = "snake_case", deny_unknown_fields)]
pub enum SemanticGenerationDependency {
    None,
    Checkpoint {
        stage: SemanticStage,
        checkpoint_digest: SemanticDigest,
    },
    Activation {
        lexical_checkpoint_digest: SemanticDigest,
        ann_checkpoint_digest: SemanticDigest,
    },
}

impl SemanticGenerationDependency {
    fn canonical_cbor(&self) -> Vec<u8> {
        match self {
            Self::None => cbor::map([("dependency", cbor::text("none"))]),
            Self::Checkpoint {
                stage,
                checkpoint_digest,
            } => cbor::map([
                ("dependency", cbor::text("checkpoint")),
                ("stage", cbor::text(stage.as_str())),
                ("checkpoint_digest", cbor::digest(*checkpoint_digest)),
            ]),
            Self::Activation {
                lexical_checkpoint_digest,
                ann_checkpoint_digest,
            } => cbor::map([
                ("dependency", cbor::text("activation")),
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
pub struct SemanticGenerationCheckpoint {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub source_revision: String,
    pub stage: SemanticStage,
    pub aggregate: SemanticGenerationAggregate,
    pub dependency: SemanticGenerationDependency,
    pub artifact_digest: SemanticDigest,
    pub completed_at: String,
    pub checkpoint_digest: SemanticDigest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticGenerationCheckpointDraft {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub source_revision: String,
    pub stage: SemanticStage,
    pub aggregate: SemanticGenerationAggregate,
    pub dependency: SemanticGenerationDependency,
    pub artifact_digest: SemanticDigest,
    pub completed_at: String,
}

/// One exact checkpoint compare-and-swap carried with an S3/S5 completion.
/// The owner store must load the expected predecessor, insert `member`,
/// recompute the complete sorted aggregate from authoritative entity rows,
/// and compare it with `successor` in the same transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticGenerationCheckpointUpdate {
    pub expected_previous_checkpoint_digest: Option<SemanticDigest>,
    pub expected_previous_completed_count: u64,
    pub member: SemanticGenerationMember,
    pub successor: SemanticGenerationCheckpoint,
}

impl SemanticGenerationCheckpointUpdate {
    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        self.successor.validate()?;
        validate_member_identity(
            &self.member.source_entity_id,
            &self.member.source_revision,
            &self.successor.source_revision,
        )?;
        let first = self.expected_previous_completed_count == 0;
        if first != self.expected_previous_checkpoint_digest.is_none()
            || self.successor.aggregate.completed_entity_count
                != self
                    .expected_previous_completed_count
                    .checked_add(1)
                    .ok_or_else(|| invalid_count("expected_previous_completed_count"))?
            || self.successor.aggregate.completed_entity_count
                > self.successor.aggregate.expected_entity_count
            || self.expected_previous_checkpoint_digest == Some(self.successor.checkpoint_digest)
        {
            return Err(SemanticIndexError::GenerationCheckpointMismatch);
        }
        Ok(())
    }
}

impl SemanticGenerationCheckpoint {
    pub fn create(draft: SemanticGenerationCheckpointDraft) -> Result<Self, SemanticIndexError> {
        let checkpoint_digest = checkpoint_digest(&draft);
        let checkpoint = Self {
            binding_id: draft.binding_id,
            binding_digest: draft.binding_digest,
            generation: draft.generation,
            source_revision: draft.source_revision,
            stage: draft.stage,
            aggregate: draft.aggregate,
            dependency: draft.dependency,
            artifact_digest: draft.artifact_digest,
            completed_at: draft.completed_at,
            checkpoint_digest,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        validate_generation_coordinates(&self.binding_id, self.generation, &self.source_revision)?;
        validate_text("completed_at", &self.completed_at)?;
        self.aggregate.validate()?;
        validate_dependency(self.stage, &self.dependency)?;
        if matches!(
            self.stage,
            SemanticStage::LexicalIndex | SemanticStage::Vector | SemanticStage::AnnIndex
        ) && self.artifact_digest != self.aggregate.aggregate_artifact_digest
        {
            return Err(SemanticIndexError::GenerationCheckpointMismatch);
        }
        if self.checkpoint_digest != checkpoint_digest_from_record(self) {
            return Err(SemanticIndexError::GenerationCheckpointMismatch);
        }
        Ok(())
    }

    pub fn require_complete(&self) -> Result<(), SemanticIndexError> {
        self.validate()?;
        if !self.aggregate.is_complete() {
            return Err(SemanticIndexError::GenerationIncomplete {
                expected: self.aggregate.expected_entity_count,
                completed: self.aggregate.completed_entity_count,
            });
        }
        Ok(())
    }

    pub(crate) fn canonical_cbor_subject(&self) -> Vec<u8> {
        cbor::map([
            ("binding_id", cbor::text(&self.binding_id)),
            ("binding_digest", cbor::digest(self.binding_digest)),
            ("generation", cbor::unsigned(self.generation)),
            ("source_revision", cbor::text(&self.source_revision)),
            ("stage", cbor::text(self.stage.as_str())),
            ("aggregate", self.aggregate.canonical_cbor()),
            ("dependency", self.dependency.canonical_cbor()),
            ("artifact_digest", cbor::digest(self.artifact_digest)),
            ("completed_at", cbor::text(&self.completed_at)),
            ("checkpoint_digest", cbor::digest(self.checkpoint_digest)),
        ])
    }
}

fn validate_dependency(
    stage: SemanticStage,
    dependency: &SemanticGenerationDependency,
) -> Result<(), SemanticIndexError> {
    let valid = match (stage, dependency) {
        (SemanticStage::LexicalIndex, SemanticGenerationDependency::None) => true,
        (
            SemanticStage::Vector,
            SemanticGenerationDependency::Checkpoint {
                stage: SemanticStage::LexicalIndex,
                ..
            },
        ) => true,
        (
            SemanticStage::AnnIndex,
            SemanticGenerationDependency::Checkpoint {
                stage: SemanticStage::Vector,
                ..
            },
        ) => true,
        (SemanticStage::ReconcileAndActivate, SemanticGenerationDependency::Activation { .. }) => {
            true
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(SemanticIndexError::GenerationCheckpointMismatch)
    }
}

fn checkpoint_digest(draft: &SemanticGenerationCheckpointDraft) -> SemanticDigest {
    checkpoint_digest_parts(
        (&draft.binding_id, draft.binding_digest, draft.generation),
        (&draft.source_revision, draft.stage),
        (&draft.aggregate, &draft.dependency),
        (draft.artifact_digest, &draft.completed_at),
    )
}

fn checkpoint_digest_from_record(record: &SemanticGenerationCheckpoint) -> SemanticDigest {
    checkpoint_digest_parts(
        (&record.binding_id, record.binding_digest, record.generation),
        (&record.source_revision, record.stage),
        (&record.aggregate, &record.dependency),
        (record.artifact_digest, &record.completed_at),
    )
}

fn checkpoint_digest_parts(
    identity: (&str, SemanticDigest, u64),
    source: (&str, SemanticStage),
    generation: (&SemanticGenerationAggregate, &SemanticGenerationDependency),
    completion: (SemanticDigest, &str),
) -> SemanticDigest {
    let subject = cbor::map([
        ("binding_id", cbor::text(identity.0)),
        ("binding_digest", cbor::digest(identity.1)),
        ("generation", cbor::unsigned(identity.2)),
        ("source_revision", cbor::text(source.0)),
        ("stage", cbor::text(source.1.as_str())),
        ("aggregate", generation.0.canonical_cbor()),
        ("dependency", generation.1.canonical_cbor()),
        ("artifact_digest", cbor::digest(completion.0)),
        ("completed_at", cbor::text(completion.1)),
    ]);
    domain_digest(SEMANTIC_GENERATION_CHECKPOINT_DIGEST_DOMAIN, subject)
}

fn canonical_expected(
    expected: Vec<SemanticExpectedEntity>,
    source_revision: &str,
) -> Result<Vec<SemanticExpectedEntity>, SemanticIndexError> {
    let mut canonical = BTreeMap::new();
    for entity in expected {
        validate_member_identity(
            &entity.source_entity_id,
            &entity.source_revision,
            source_revision,
        )?;
        if canonical
            .insert(entity.source_entity_id.clone(), entity)
            .is_some()
        {
            return Err(SemanticIndexError::DuplicateSourceEntity);
        }
    }
    Ok(canonical.into_values().collect())
}

fn canonical_completed(
    completed: Vec<SemanticGenerationMember>,
    source_revision: &str,
    expected: &[SemanticExpectedEntity],
) -> Result<Vec<SemanticGenerationMember>, SemanticIndexError> {
    let expected: BTreeMap<_, _> = expected
        .iter()
        .map(|entity| {
            (
                entity.source_entity_id.as_str(),
                entity.source_revision.as_str(),
            )
        })
        .collect();
    let mut canonical = BTreeMap::new();
    for member in completed {
        validate_member_identity(
            &member.source_entity_id,
            &member.source_revision,
            source_revision,
        )?;
        if expected.get(member.source_entity_id.as_str()) != Some(&member.source_revision.as_str())
        {
            return Err(SemanticIndexError::GenerationEntitySetMismatch);
        }
        if canonical
            .insert(member.source_entity_id.clone(), member)
            .is_some()
        {
            return Err(SemanticIndexError::DuplicateSourceEntity);
        }
    }
    Ok(canonical.into_values().collect())
}

fn validate_member_identity(
    source_entity_id: &str,
    member_revision: &str,
    source_revision: &str,
) -> Result<(), SemanticIndexError> {
    validate_text("source_entity_id", source_entity_id)?;
    validate_text("member_source_revision", member_revision)?;
    if member_revision != source_revision {
        return Err(SemanticIndexError::SourceRevisionStale);
    }
    Ok(())
}

fn entity_set_digest(
    binding_id: &str,
    binding_digest: SemanticDigest,
    generation: u64,
    source_revision: &str,
    expected: &[SemanticExpectedEntity],
) -> SemanticDigest {
    let members = expected.iter().map(|member| {
        cbor::map([
            ("source_entity_id", cbor::text(&member.source_entity_id)),
            ("source_revision", cbor::text(&member.source_revision)),
        ])
    });
    let subject = generation_subject(
        binding_id,
        binding_digest,
        generation,
        source_revision,
        None,
        cbor::array(members),
    );
    domain_digest(SEMANTIC_ENTITY_SET_DIGEST_DOMAIN, subject)
}

fn member_digest<F>(
    domain: &[u8],
    binding_id: &str,
    binding_digest: SemanticDigest,
    generation: u64,
    source_revision: &str,
    stage: SemanticStage,
    completed: &[SemanticGenerationMember],
    select: F,
) -> SemanticDigest
where
    F: Fn(&SemanticGenerationMember) -> SemanticDigest,
{
    let members = completed.iter().map(|member| {
        cbor::map([
            ("source_entity_id", cbor::text(&member.source_entity_id)),
            ("source_revision", cbor::text(&member.source_revision)),
            ("member_digest", cbor::digest(select(member))),
        ])
    });
    let subject = generation_subject(
        binding_id,
        binding_digest,
        generation,
        source_revision,
        Some(stage),
        cbor::array(members),
    );
    domain_digest(domain, subject)
}

fn generation_subject(
    binding_id: &str,
    binding_digest: SemanticDigest,
    generation: u64,
    source_revision: &str,
    stage: Option<SemanticStage>,
    members: Vec<u8>,
) -> Vec<u8> {
    cbor::map(vec![
        ("binding_id", cbor::text(binding_id)),
        ("binding_digest", cbor::digest(binding_digest)),
        ("generation", cbor::unsigned(generation)),
        ("source_revision", cbor::text(source_revision)),
        (
            "stage",
            stage
                .map(|value| cbor::text(value.as_str()))
                .unwrap_or_else(cbor::null),
        ),
        ("members", members),
    ])
}

fn validate_generation_coordinates(
    binding_id: &str,
    generation: u64,
    source_revision: &str,
) -> Result<(), SemanticIndexError> {
    validate_text("binding_id", binding_id)?;
    validate_generation(generation)?;
    validate_text("source_revision", source_revision)
}

fn invalid_count(field: &str) -> SemanticIndexError {
    SemanticIndexError::InvalidField {
        field: field.to_string(),
        reason: "generation counts must be nonzero, bounded, and monotonic".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(byte: u8) -> SemanticDigest {
        SemanticDigest::from_bytes([byte; 32])
    }

    fn expected(order: [&str; 2]) -> Vec<SemanticExpectedEntity> {
        order
            .into_iter()
            .map(|source_entity_id| SemanticExpectedEntity {
                source_entity_id: source_entity_id.to_string(),
                source_revision: "r1".to_string(),
            })
            .collect()
    }

    fn completed(order: [(&str, u8, u8); 2]) -> Vec<SemanticGenerationMember> {
        order
            .into_iter()
            .map(
                |(source_entity_id, receipt, artifact)| SemanticGenerationMember {
                    source_entity_id: source_entity_id.to_string(),
                    source_revision: "r1".to_string(),
                    receipt_digest: digest(receipt),
                    artifact_digest: digest(artifact),
                },
            )
            .collect()
    }

    #[test]
    fn two_entity_aggregate_is_order_independent_and_golden() {
        let first = SemanticGenerationAggregate::create(
            "binding-a",
            digest(7),
            3,
            "r1",
            SemanticStage::LexicalIndex,
            expected(["b", "a"]),
            completed([("b", 2, 4), ("a", 1, 3)]),
        )
        .unwrap();
        let second = SemanticGenerationAggregate::create(
            "binding-a",
            digest(7),
            3,
            "r1",
            SemanticStage::LexicalIndex,
            expected(["a", "b"]),
            completed([("a", 1, 3), ("b", 2, 4)]),
        )
        .unwrap();
        assert_eq!(first, second);
        assert_eq!(first.expected_entity_count, 2);
        assert_eq!(first.completed_entity_count, 2);
        assert_eq!(
            first.entity_set_digest.to_hex(),
            "34f54d325caf72f428fbb02bb97b0e9b9803c09f7823b78a52db4af72ab8d728"
        );
        assert_eq!(
            first.aggregate_receipt_digest.to_hex(),
            "7335b39beca14fae39fa1de7e4e1195c4b20b6f979dd947bf247c375fbf4a690"
        );
        assert_eq!(
            first.aggregate_artifact_digest.to_hex(),
            "ba771dbbcd321a719370473060ec5afcab20feff398f09d1b5a702a46e6baea2"
        );
    }

    #[test]
    fn aggregate_rejects_duplicate_unknown_and_revision_drift() {
        assert_eq!(
            SemanticGenerationAggregate::create(
                "binding-a",
                digest(7),
                3,
                "r1",
                SemanticStage::LexicalIndex,
                expected(["a", "a"]),
                Vec::new(),
            ),
            Err(SemanticIndexError::DuplicateSourceEntity)
        );
        let mut unknown = completed([("a", 1, 3), ("c", 2, 4)]);
        assert_eq!(
            SemanticGenerationAggregate::create(
                "binding-a",
                digest(7),
                3,
                "r1",
                SemanticStage::LexicalIndex,
                expected(["a", "b"]),
                std::mem::take(&mut unknown),
            ),
            Err(SemanticIndexError::GenerationEntitySetMismatch)
        );
        let mut drift = expected(["a", "b"]);
        drift[1].source_revision = "r2".to_string();
        assert_eq!(
            SemanticGenerationAggregate::create(
                "binding-a",
                digest(7),
                3,
                "r1",
                SemanticStage::LexicalIndex,
                drift,
                Vec::new(),
            ),
            Err(SemanticIndexError::SourceRevisionStale)
        );
    }

    #[test]
    fn checkpoint_tamper_and_partial_completion_fail_closed() {
        let aggregate = SemanticGenerationAggregate::create(
            "binding-a",
            digest(7),
            3,
            "r1",
            SemanticStage::LexicalIndex,
            expected(["a", "b"]),
            vec![SemanticGenerationMember {
                source_entity_id: "a".to_string(),
                source_revision: "r1".to_string(),
                receipt_digest: digest(1),
                artifact_digest: digest(3),
            }],
        )
        .unwrap();
        let mut checkpoint =
            SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
                binding_id: "binding-a".to_string(),
                binding_digest: digest(7),
                generation: 3,
                source_revision: "r1".to_string(),
                stage: SemanticStage::LexicalIndex,
                artifact_digest: aggregate.aggregate_artifact_digest,
                aggregate,
                dependency: SemanticGenerationDependency::None,
                completed_at: "2026-09-04T00:00:00Z".to_string(),
            })
            .unwrap();
        assert_eq!(
            checkpoint.require_complete(),
            Err(SemanticIndexError::GenerationIncomplete {
                expected: 2,
                completed: 1,
            })
        );
        checkpoint.aggregate.completed_entity_count = 2;
        assert_eq!(
            checkpoint.validate(),
            Err(SemanticIndexError::GenerationCheckpointMismatch)
        );
    }
}
