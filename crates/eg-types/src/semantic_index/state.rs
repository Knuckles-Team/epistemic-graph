use serde::{Deserialize, Serialize};

use super::config::SemanticBindingState;
use super::digest::{cbor, domain_digest, SemanticDigest, SEMANTIC_BINDING_STATE_RECEIPT_DOMAIN};
use super::generation::SemanticGenerationCheckpoint;
use super::identity::{validate_generation, validate_text, SemanticBinding, SemanticVector};
use super::persistence::{
    SemanticActivationTarget, SemanticActivePointer, SemanticAnnIndexManifest, SemanticDeadLetter,
    SemanticLexicalIndexManifest,
};
use super::provenance::SemanticAuthorizationReceipt;
use super::request::SemanticIndexOperation;
use super::selector::SemanticSelectorKind;
use super::source_manifest::{SemanticGraphProjectionManifest, SemanticSqlSourceManifest};
use super::stage::{
    SemanticQueueClass, SemanticStage, SemanticStageOutcome, SemanticStageTransition,
};
use super::tombstone::SemanticTombstone;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticBindingStateTransition {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub expected: SemanticBindingState,
    pub next: SemanticBindingState,
    pub reason: String,
    pub state_receipt_digest: SemanticDigest,
}

impl SemanticBindingStateTransition {
    pub fn create(
        binding: &SemanticBinding,
        next: SemanticBindingState,
        reason: impl Into<String>,
    ) -> Result<Self, SemanticIndexError> {
        let mut transition = Self {
            binding_id: binding.binding_id.clone(),
            binding_digest: binding.binding_digest,
            generation: binding.generation,
            expected: binding.durable_state,
            next,
            reason: reason.into(),
            state_receipt_digest: SemanticDigest::from_bytes([0; 32]),
        };
        transition.state_receipt_digest = transition.compute_receipt_digest();
        transition.validate_against(binding)?;
        Ok(transition)
    }

    pub fn validate_against(&self, binding: &SemanticBinding) -> Result<(), SemanticIndexError> {
        self.validate_identity_against(binding)?;
        self.validate_receipt()
    }

    pub fn validate_receipt(&self) -> Result<(), SemanticIndexError> {
        validate_text("binding_id", &self.binding_id)?;
        validate_text("reason", &self.reason)?;
        validate_generation(self.generation)?;
        if !valid_state_transition(self.expected, self.next) {
            return Err(SemanticIndexError::InvalidBindingStateTransition {
                from: self.expected,
                to: self.next,
            });
        }
        if self.state_receipt_digest != self.compute_receipt_digest() {
            return Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_binding_state_receipt".to_string(),
            });
        }
        Ok(())
    }

    fn validate_identity_against(
        &self,
        binding: &SemanticBinding,
    ) -> Result<(), SemanticIndexError> {
        if self.binding_id != binding.binding_id || self.binding_digest != binding.binding_digest {
            return Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_binding_state_transition".to_string(),
            });
        }
        if self.generation != binding.generation {
            return Err(SemanticIndexError::GenerationMismatch {
                expected: binding.generation,
                actual: self.generation,
            });
        }
        if self.expected != binding.durable_state {
            return Err(SemanticIndexError::BindingStateMismatch {
                expected: binding.durable_state,
                actual: self.expected,
            });
        }
        Ok(())
    }

    fn compute_receipt_digest(&self) -> SemanticDigest {
        let subject = cbor::map([
            ("binding_id", cbor::text(&self.binding_id)),
            ("binding_digest", cbor::digest(self.binding_digest)),
            ("generation", cbor::unsigned(self.generation)),
            ("expected", cbor::text(self.expected.as_str())),
            ("next", cbor::text(self.next.as_str())),
            ("reason", cbor::text(&self.reason)),
        ]);
        domain_digest(SEMANTIC_BINDING_STATE_RECEIPT_DOMAIN, subject)
    }
}

fn valid_state_transition(from: SemanticBindingState, to: SemanticBindingState) -> bool {
    use SemanticBindingState::{Building, Disabled, Dropping, Failed, Live, Pending};
    matches!(
        (from, to),
        (Pending, Building)
            | (Building, Live | Failed)
            | (Live, Disabled | Dropping)
            | (Disabled | Failed, Pending | Dropping)
    )
}

/// Closed owner-row mutation plan. It is intentionally not yet attached to
/// `MutationOperation` or admitted as `Native(SemanticIndex)`; that atomic
/// compiler/store cutover belongs to the activation slice.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mutation", rename_all = "snake_case", deny_unknown_fields)]
pub enum SemanticIndexMutation {
    StoreBinding {
        binding: Box<SemanticBinding>,
    },
    /// Boxed for the same reason as `StoreBinding` above: the transition
    /// carries its intent and receipt and dominates the enum's size.
    RecordStageTransition {
        transition: Box<SemanticStageTransition>,
        artifact: SemanticStageArtifact,
    },
    FinalizeGeneration {
        checkpoint: Box<SemanticGenerationCheckpoint>,
        artifact: SemanticGenerationArtifact,
    },
    SupersedeSourceRevision {
        binding_id: String,
        binding_digest: SemanticDigest,
        generation: u64,
        source_entity_id: String,
        superseded_revision: String,
        replacement_revision: String,
        retained_receipt_digest: SemanticDigest,
    },
    SetBindingState {
        transition: SemanticBindingStateTransition,
    },
    DeleteBinding {
        tombstone: Box<SemanticTombstone>,
    },
}

impl SemanticIndexMutation {
    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        match self {
            Self::StoreBinding { binding } => {
                binding.validate()?;
                if binding.durable_state != SemanticBindingState::Pending {
                    return Err(SemanticIndexError::CallerSetBindingState);
                }
                Ok(())
            }
            Self::RecordStageTransition {
                transition,
                artifact,
            } => {
                transition.validate()?;
                artifact.validate_against(transition)
            }
            Self::FinalizeGeneration {
                checkpoint,
                artifact,
            } => {
                checkpoint.require_complete()?;
                artifact.validate_against(checkpoint)
            }
            Self::SetBindingState { transition } => transition.validate_receipt(),
            Self::SupersedeSourceRevision {
                binding_id,
                generation,
                source_entity_id,
                superseded_revision,
                replacement_revision,
                ..
            } => validate_supersession(
                binding_id,
                *generation,
                source_entity_id,
                superseded_revision,
                replacement_revision,
            ),
            Self::DeleteBinding { tombstone } => tombstone.validate(),
        }
    }
}

/// Closed entity-stage owner artifact carried inside the exact-replay mutation.
/// Global index manifests and activation are committed only by
/// [`SemanticIndexMutation::FinalizeGeneration`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "artifact", rename_all = "snake_case", deny_unknown_fields)]
pub enum SemanticStageArtifact {
    None,
    SqlSourceManifest {
        manifest: Box<SemanticSqlSourceManifest>,
        authorization: Box<SemanticAuthorizationReceipt>,
    },
    GraphProjectionManifest {
        manifest: Box<SemanticGraphProjectionManifest>,
    },
    Vector {
        vector: Box<SemanticVector>,
    },
    DeadLetter {
        dead_letter: Box<SemanticDeadLetter>,
    },
}

impl SemanticStageArtifact {
    pub fn validate_against(
        &self,
        transition: &SemanticStageTransition,
    ) -> Result<(), SemanticIndexError> {
        match transition.receipt.outcome {
            SemanticStageOutcome::Completed => self.validate_completed(transition),
            SemanticStageOutcome::RejectedDeadLetter => self.validate_rejected(transition),
            _ if matches!(self, Self::None) => Ok(()),
            _ => Err(SemanticIndexError::StageArtifactMismatch),
        }
    }

    fn validate_completed(
        &self,
        transition: &SemanticStageTransition,
    ) -> Result<(), SemanticIndexError> {
        match (transition.intent.stage, self) {
            (
                SemanticStage::SourceCommit,
                Self::SqlSourceManifest {
                    manifest,
                    authorization,
                },
            ) => {
                manifest.validate_against_transition(transition)?;
                authorization.validate_against_transition(transition)?;
                if manifest.authorization_receipt_digest
                    != authorization.authorization_receipt_digest
                    || manifest.source_acl_revision
                        != authorization.policy_identity.components.source_acl_revision
                    || manifest.source_acl_digest
                        != authorization.policy_identity.components.source_acl_digest
                {
                    return Err(SemanticIndexError::AuthorizationReceiptMismatch);
                }
                Ok(())
            }
            (SemanticStage::GraphProjection, Self::GraphProjectionManifest { manifest }) => {
                manifest.validate_against_transition(transition)
            }
            (SemanticStage::LexicalIndex | SemanticStage::AnnIndex, Self::None) => Ok(()),
            (SemanticStage::Vector, Self::Vector { vector }) => {
                vector.validate()?;
                validate_vector_artifact(transition, vector)?;
                if transition.receipt.output_digest != vector.values_digest {
                    return Err(SemanticIndexError::StageArtifactMismatch);
                }
                Ok(())
            }
            _ => Err(SemanticIndexError::StageArtifactMismatch),
        }
    }

    fn validate_rejected(
        &self,
        transition: &SemanticStageTransition,
    ) -> Result<(), SemanticIndexError> {
        let Self::DeadLetter { dead_letter } = self else {
            return Err(SemanticIndexError::StageArtifactMismatch);
        };
        dead_letter.validate()?;
        validate_dead_letter_artifact(transition, dead_letter)
    }
}

/// Closed generation-finalization artifact. This keeps the exact replay batch
/// authoritative for global lexical/ANN identities and the active pointer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "artifact", rename_all = "snake_case", deny_unknown_fields)]
pub enum SemanticGenerationArtifact {
    LexicalIndexManifest {
        manifest: Box<SemanticLexicalIndexManifest>,
    },
    AnnIndexManifest {
        manifest: Box<SemanticAnnIndexManifest>,
    },
    Activation {
        target: Box<SemanticActivationTarget>,
        pointer: Box<SemanticActivePointer>,
    },
}

impl SemanticGenerationArtifact {
    pub fn validate_against(
        &self,
        checkpoint: &SemanticGenerationCheckpoint,
    ) -> Result<(), SemanticIndexError> {
        match (checkpoint.stage, self) {
            (SemanticStage::LexicalIndex, Self::LexicalIndexManifest { manifest }) => {
                manifest.validate()?;
                if manifest.row_count != checkpoint.aggregate.completed_entity_count {
                    return Err(SemanticIndexError::StageArtifactMismatch);
                }
                validate_generation_artifact(
                    checkpoint,
                    (
                        &manifest.binding_id,
                        manifest.binding_digest,
                        manifest.generation,
                        &manifest.source_revision,
                    ),
                    (
                        manifest.artifact_digest,
                        manifest.completed_receipt_digest,
                        &manifest.completed_at,
                    ),
                )
            }
            (SemanticStage::AnnIndex, Self::AnnIndexManifest { manifest }) => {
                manifest.validate()?;
                if manifest.vector_count != checkpoint.aggregate.completed_entity_count {
                    return Err(SemanticIndexError::StageArtifactMismatch);
                }
                validate_generation_artifact(
                    checkpoint,
                    (
                        &manifest.binding_id,
                        manifest.binding_digest,
                        manifest.generation,
                        &manifest.source_revision,
                    ),
                    (
                        manifest.artifact_digest,
                        manifest.completed_receipt_digest,
                        &manifest.completed_at,
                    ),
                )
            }
            (SemanticStage::ReconcileAndActivate, Self::Activation { target, pointer }) => {
                validate_activation(checkpoint, target, pointer)
            }
            _ => Err(SemanticIndexError::StageArtifactMismatch),
        }
    }
}

fn validate_activation(
    checkpoint: &SemanticGenerationCheckpoint,
    target: &SemanticActivationTarget,
    pointer: &SemanticActivePointer,
) -> Result<(), SemanticIndexError> {
    target.validate()?;
    pointer.validate()?;
    let coordinates_match = pointer.binding_id == checkpoint.binding_id
        && pointer.binding_digest == checkpoint.binding_digest
        && pointer.generation == checkpoint.generation
        && pointer.source_revision == checkpoint.source_revision;
    let receipts_match = pointer.activation_receipt_digest == checkpoint.checkpoint_digest
        && pointer.activated_at == checkpoint.completed_at
        && target.target_digest == checkpoint.artifact_digest;
    let target_identity = (
        &target.tenant_id,
        &target.binding_id,
        target.binding_digest,
        target.generation,
        &target.source_revision,
        &target.vector_target_id,
        &target.lexical_index_identity,
        &target.ann_index_identity,
        &target.composite_policy_digest,
    );
    let pointer_identity = (
        &pointer.tenant_id,
        &pointer.binding_id,
        pointer.binding_digest,
        pointer.generation,
        &pointer.source_revision,
        &pointer.vector_target_id,
        &pointer.lexical_index_identity,
        &pointer.ann_index_identity,
        &pointer.composite_policy_digest,
    );
    if !coordinates_match || !receipts_match || target_identity != pointer_identity {
        return Err(SemanticIndexError::StageArtifactMismatch);
    }
    Ok(())
}

fn validate_manifest_artifact(
    transition: &SemanticStageTransition,
    binding_id: &str,
    binding_digest: SemanticDigest,
    generation: u64,
    source_revision: &str,
) -> Result<(), SemanticIndexError> {
    if binding_id != transition.intent.binding_id
        || binding_digest != transition.intent.binding_digest
        || generation != transition.intent.generation
        || source_revision != transition.intent.source_revision
    {
        return Err(SemanticIndexError::StageArtifactMismatch);
    }
    Ok(())
}

fn validate_vector_artifact(
    transition: &SemanticStageTransition,
    vector: &SemanticVector,
) -> Result<(), SemanticIndexError> {
    validate_manifest_artifact(
        transition,
        &vector.binding_id,
        vector.binding_digest,
        vector.generation,
        &vector.source_revision,
    )?;
    if transition.intent.scope.source_entity_id() != Some(vector.source_entity_id.as_str()) {
        return Err(SemanticIndexError::StageArtifactMismatch);
    }
    Ok(())
}

fn validate_dead_letter_artifact(
    transition: &SemanticStageTransition,
    dead_letter: &SemanticDeadLetter,
) -> Result<(), SemanticIndexError> {
    if dead_letter.intent != transition.intent
        || dead_letter.failure_digest != transition.receipt.output_digest
        || dead_letter.failed_at != transition.receipt.completed_at
    {
        return Err(SemanticIndexError::StageArtifactMismatch);
    }
    Ok(())
}

/// `coordinates` is `(binding_id, binding_digest, generation, source_revision)`
/// and `proof` is `(artifact_digest, completed_receipt_digest, completed_at)`:
/// the identity of the generation, and the evidence offered for it.
fn validate_generation_artifact(
    checkpoint: &SemanticGenerationCheckpoint,
    coordinates: (&str, SemanticDigest, u64, &str),
    proof: (SemanticDigest, SemanticDigest, &str),
) -> Result<(), SemanticIndexError> {
    let (binding_id, binding_digest, generation, source_revision) = coordinates;
    let (artifact_digest, completed_receipt_digest, completed_at) = proof;
    if binding_id != checkpoint.binding_id
        || binding_digest != checkpoint.binding_digest
        || generation != checkpoint.generation
        || source_revision != checkpoint.source_revision
        || artifact_digest != checkpoint.artifact_digest
        || completed_receipt_digest != checkpoint.aggregate.aggregate_receipt_digest
        || completed_at != checkpoint.completed_at
    {
        return Err(SemanticIndexError::StageArtifactMismatch);
    }
    Ok(())
}

fn validate_supersession(
    binding_id: &str,
    generation: u64,
    source_entity_id: &str,
    superseded_revision: &str,
    replacement_revision: &str,
) -> Result<(), SemanticIndexError> {
    validate_generation(generation)?;
    for (field, value) in [
        ("binding_id", binding_id),
        ("source_entity_id", source_entity_id),
        ("superseded_revision", superseded_revision),
        ("replacement_revision", replacement_revision),
    ] {
        validate_text(field, value)?;
    }
    if superseded_revision == replacement_revision {
        return Err(SemanticIndexError::SourceRevisionStale);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticQueueStatus {
    pub queue_profile_id: String,
    pub class: SemanticQueueClass,
    pub capacity: u32,
    pub inflight: u32,
    pub oldest_age_ms: u64,
    pub lag_rows: u64,
    pub lag_ms: u64,
    pub retry_count: u64,
    pub rejection_count: u64,
    pub live: bool,
    pub saturated: bool,
    pub tenant_id: String,
    pub consecutive_claim_percent: u8,
    pub fairness_relaxation_count: u64,
    pub trace_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticIndexStatus {
    Accepted,
    DeferredBackpressured,
    Partial,
    Rejected,
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "code", rename_all = "snake_case", deny_unknown_fields)]
pub enum SemanticIndexError {
    UnsupportedSelector {
        selector: SemanticSelectorKind,
    },
    InvalidField {
        field: String,
        reason: String,
    },
    DigestMismatch {
        subject: String,
    },
    GenerationMismatch {
        expected: u64,
        actual: u64,
    },
    BindingStateMismatch {
        expected: SemanticBindingState,
        actual: SemanticBindingState,
    },
    InvalidBindingStateTransition {
        from: SemanticBindingState,
        to: SemanticBindingState,
    },
    PolicyUnresolved,
    SourceRevisionStale,
    PredecessorIncomplete {
        required: SemanticStage,
    },
    PredecessorReceiptMismatch {
        stage: SemanticStage,
    },
    StageScopeMismatch {
        stage: SemanticStage,
    },
    GenerationCheckpointMismatch,
    GenerationIncomplete {
        expected: u64,
        completed: u64,
    },
    DuplicateSourceEntity,
    GenerationEntitySetMismatch,
    SourceManifestMismatch,
    GraphProjectionManifestMismatch,
    AuthorizationReceiptMismatch,
    LineageMismatch,
    UnsupportedAnnMetricPair,
    SearchProbeModelMismatch,
    MaintainedIndexUnavailable,
    ApprovalRequired,
    ApprovalOperationMismatch,
    ApprovalDigestMismatch,
    ApprovalExpired,
    AuthorizationContextMismatch,
    CallerSetBindingState,
    UnmanagedBindingResource,
    VectorBindingMismatch,
    ActivePointerMismatch,
    IndexManifestMismatch,
    ProgressReceiptMismatch,
    DeadLetterIdentityMismatch,
    StageArtifactMismatch,
    CanonicalRecordTooLarge,
    MalformedCanonicalRecord {
        reason: String,
    },
    NonCanonicalRecord,
    OperationResultMismatch {
        operation: SemanticIndexOperation,
    },
    MutationReceiptRequired {
        operation: SemanticIndexOperation,
    },
    PartialMutationOutcome {
        operation: SemanticIndexOperation,
    },
}
