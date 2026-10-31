use serde::{Deserialize, Serialize};

use super::config::SemanticBindingState;
use super::digest::{
    cbor, domain_digest, SemanticDigest, SEMANTIC_ACTIVATION_TARGET_DIGEST_DOMAIN,
    SEMANTIC_DEAD_LETTER_DIGEST_DOMAIN,
};
use super::identity::{
    validate_generation, validate_text, SemanticAnnIndexIdentity, SemanticBinding,
    SemanticLexicalIndexIdentity,
};
use super::stage::{SemanticStage, SemanticStageIntent};
use super::state::SemanticIndexError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticActivationTarget {
    pub tenant_id: String,
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub source_revision: String,
    pub vector_target_id: String,
    pub lexical_index_identity: SemanticLexicalIndexIdentity,
    pub ann_index_identity: SemanticAnnIndexIdentity,
    pub composite_policy_digest: String,
    pub target_digest: SemanticDigest,
}

impl SemanticActivationTarget {
    pub fn create(binding: &SemanticBinding) -> Result<Self, SemanticIndexError> {
        binding.validate()?;
        let mut target = Self {
            tenant_id: binding.tenant_id.clone(),
            binding_id: binding.binding_id.clone(),
            binding_digest: binding.binding_digest,
            generation: binding.generation,
            source_revision: binding.source_revision.clone(),
            vector_target_id: binding.vector_target_id.clone(),
            lexical_index_identity: binding.lexical_index_identity.clone(),
            ann_index_identity: binding.ann_index_identity.clone(),
            composite_policy_digest: binding.policy_digest.clone(),
            target_digest: SemanticDigest::from_bytes([0; 32]),
        };
        target.target_digest = target.compute_digest();
        Ok(target)
    }

    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        validate_generation(self.generation)?;
        for (field, value) in [
            ("tenant_id", self.tenant_id.as_str()),
            ("binding_id", self.binding_id.as_str()),
            ("source_revision", self.source_revision.as_str()),
            ("vector_target_id", self.vector_target_id.as_str()),
            (
                "composite_policy_digest",
                self.composite_policy_digest.as_str(),
            ),
        ] {
            validate_text(field, value)?;
        }
        self.lexical_index_identity.validate()?;
        self.ann_index_identity.validate()?;
        validate_index_pair(
            &self.binding_id,
            self.binding_digest,
            self.generation,
            &self.source_revision,
            &self.composite_policy_digest,
            &self.lexical_index_identity,
            &self.ann_index_identity,
        )?;
        if self.target_digest != self.compute_digest() {
            return Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_activation_target".to_string(),
            });
        }
        Ok(())
    }

    pub fn validate_against_binding(
        &self,
        binding: &SemanticBinding,
    ) -> Result<(), SemanticIndexError> {
        self.validate()?;
        let expected = Self::create(binding)?;
        if self != &expected {
            return Err(SemanticIndexError::ActivePointerMismatch);
        }
        Ok(())
    }

    fn compute_digest(&self) -> SemanticDigest {
        let subject = cbor::map([
            ("tenant_id", cbor::text(&self.tenant_id)),
            ("binding_id", cbor::text(&self.binding_id)),
            ("binding_digest", cbor::digest(self.binding_digest)),
            ("generation", cbor::unsigned(self.generation)),
            ("source_revision", cbor::text(&self.source_revision)),
            ("vector_target_id", cbor::text(&self.vector_target_id)),
            (
                "lexical_index_digest",
                cbor::digest(self.lexical_index_identity.lexical_index_digest),
            ),
            (
                "ann_index_digest",
                cbor::digest(self.ann_index_identity.ann_index_digest),
            ),
            (
                "composite_policy_digest",
                cbor::text(&self.composite_policy_digest),
            ),
        ]);
        domain_digest(SEMANTIC_ACTIVATION_TARGET_DIGEST_DOMAIN, subject)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticActivePointer {
    pub tenant_id: String,
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub source_revision: String,
    pub vector_target_id: String,
    pub lexical_index_identity: SemanticLexicalIndexIdentity,
    pub ann_index_identity: SemanticAnnIndexIdentity,
    pub composite_policy_digest: String,
    pub activation_receipt_digest: SemanticDigest,
    pub activated_at: String,
}

impl SemanticActivePointer {
    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        validate_pointer_text(self)?;
        validate_generation(self.generation)?;
        self.lexical_index_identity.validate()?;
        self.ann_index_identity.validate()?;
        validate_index_pair(
            &self.binding_id,
            self.binding_digest,
            self.generation,
            &self.source_revision,
            &self.composite_policy_digest,
            &self.lexical_index_identity,
            &self.ann_index_identity,
        )
    }

    pub fn validate_against(
        &self,
        binding: &SemanticBinding,
        manifest: &SemanticIndexManifest,
    ) -> Result<(), SemanticIndexError> {
        self.validate()?;
        binding.validate()?;
        manifest.validate_against(binding)?;
        if !pointer_matches_binding(self, binding) || !pointer_matches_manifest(self, manifest) {
            return Err(SemanticIndexError::ActivePointerMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticSourceProgress {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub source_entity_id: String,
    pub source_revision: String,
    pub completed_stage: Option<SemanticStage>,
    pub completed_receipt_digest: Option<SemanticDigest>,
    pub superseded_by_revision: Option<String>,
    pub updated_at: String,
}

impl SemanticSourceProgress {
    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        validate_generation(self.generation)?;
        for (field, value) in [
            ("binding_id", self.binding_id.as_str()),
            ("source_entity_id", self.source_entity_id.as_str()),
            ("source_revision", self.source_revision.as_str()),
            ("updated_at", self.updated_at.as_str()),
        ] {
            validate_text(field, value)?;
        }
        if self.completed_stage.is_some() != self.completed_receipt_digest.is_some() {
            return Err(SemanticIndexError::ProgressReceiptMismatch);
        }
        if self.completed_stage == Some(SemanticStage::ReconcileAndActivate) {
            return Err(SemanticIndexError::StageScopeMismatch {
                stage: SemanticStage::ReconcileAndActivate,
            });
        }
        if let Some(revision) = &self.superseded_by_revision {
            validate_text("superseded_by_revision", revision)?;
            if revision == &self.source_revision {
                return Err(SemanticIndexError::SourceRevisionStale);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticLexicalIndexManifest {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub source_revision: String,
    pub identity: SemanticLexicalIndexIdentity,
    pub artifact_digest: SemanticDigest,
    pub row_count: u64,
    pub completed_receipt_digest: SemanticDigest,
    pub completed_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticAnnIndexManifest {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub source_revision: String,
    pub identity: SemanticAnnIndexIdentity,
    pub artifact_digest: SemanticDigest,
    pub vector_count: u64,
    pub completed_receipt_digest: SemanticDigest,
    pub completed_at: String,
}

impl SemanticLexicalIndexManifest {
    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        validate_manifest_text(&self.source_revision, &self.completed_at)?;
        validate_text("binding_id", &self.binding_id)?;
        validate_generation(self.generation)?;
        self.identity.validate()?;
        if !lexical_matches(
            &self.identity,
            &self.binding_id,
            self.binding_digest,
            self.generation,
            &self.source_revision,
            &self.identity.policy_digest,
        ) {
            return Err(SemanticIndexError::IndexManifestMismatch);
        }
        Ok(())
    }
}

impl SemanticAnnIndexManifest {
    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        validate_manifest_text(&self.source_revision, &self.completed_at)?;
        validate_text("binding_id", &self.binding_id)?;
        validate_generation(self.generation)?;
        self.identity.validate()?;
        if !ann_matches(
            &self.identity,
            &self.binding_id,
            self.binding_digest,
            self.generation,
            &self.source_revision,
            &self.identity.policy_digest,
        ) {
            return Err(SemanticIndexError::IndexManifestMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticIndexManifest {
    pub lexical: SemanticLexicalIndexManifest,
    pub ann: SemanticAnnIndexManifest,
}

impl SemanticIndexManifest {
    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        self.lexical.validate()?;
        self.ann.validate()?;
        if self.lexical.binding_id != self.ann.binding_id
            || self.lexical.binding_digest != self.ann.binding_digest
            || self.lexical.generation != self.ann.generation
            || self.lexical.source_revision != self.ann.source_revision
        {
            return Err(SemanticIndexError::IndexManifestMismatch);
        }
        validate_index_pair(
            &self.lexical.binding_id,
            self.lexical.binding_digest,
            self.lexical.generation,
            &self.lexical.source_revision,
            &self.lexical.identity.policy_digest,
            &self.lexical.identity,
            &self.ann.identity,
        )
    }

    pub fn validate_against(&self, binding: &SemanticBinding) -> Result<(), SemanticIndexError> {
        self.validate()?;
        binding.validate()?;
        if self.lexical.binding_id != binding.binding_id
            || self.lexical.binding_digest != binding.binding_digest
            || self.lexical.generation != binding.generation
            || self.lexical.source_revision != binding.source_revision
            || self.lexical.identity != binding.lexical_index_identity
            || self.ann.identity != binding.ann_index_identity
        {
            return Err(SemanticIndexError::IndexManifestMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticDeadLetter {
    pub intent: SemanticStageIntent,
    pub attempt: u32,
    pub error_code: String,
    pub reason: String,
    pub failure_digest: SemanticDigest,
    pub failed_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticDeadLetterDraft {
    pub intent: SemanticStageIntent,
    pub attempt: u32,
    pub error_code: String,
    pub reason: String,
    pub failed_at: String,
}

impl SemanticDeadLetter {
    pub fn create(draft: SemanticDeadLetterDraft) -> Result<Self, SemanticIndexError> {
        let failure_digest = dead_letter_digest(&draft);
        let record = Self {
            intent: draft.intent,
            attempt: draft.attempt,
            error_code: draft.error_code,
            reason: draft.reason,
            failure_digest,
            failed_at: draft.failed_at,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        self.intent.validate()?;
        for (field, value) in [
            ("error_code", self.error_code.as_str()),
            ("reason", self.reason.as_str()),
            ("failed_at", self.failed_at.as_str()),
        ] {
            validate_text(field, value)?;
        }
        if self.attempt == 0 || self.failure_digest != dead_letter_digest_from_record(self) {
            return Err(SemanticIndexError::DeadLetterIdentityMismatch);
        }
        Ok(())
    }
}

fn dead_letter_digest(draft: &SemanticDeadLetterDraft) -> SemanticDigest {
    dead_letter_digest_parts(
        draft.intent.intent_digest,
        draft.attempt,
        (&draft.error_code, &draft.reason, &draft.failed_at),
    )
}

fn dead_letter_digest_from_record(record: &SemanticDeadLetter) -> SemanticDigest {
    dead_letter_digest_parts(
        record.intent.intent_digest,
        record.attempt,
        (&record.error_code, &record.reason, &record.failed_at),
    )
}

fn dead_letter_digest_parts(
    intent_digest: SemanticDigest,
    attempt: u32,
    failure: (&str, &str, &str),
) -> SemanticDigest {
    let (error_code, reason, failed_at) = failure;
    let subject = cbor::map(vec![
        ("intent_digest", cbor::digest(intent_digest)),
        ("attempt", cbor::unsigned(attempt.into())),
        ("error_code", cbor::text(error_code)),
        ("reason", cbor::text(reason)),
        ("failed_at", cbor::text(failed_at)),
    ]);
    domain_digest(SEMANTIC_DEAD_LETTER_DIGEST_DOMAIN, subject)
}

fn validate_pointer_text(pointer: &SemanticActivePointer) -> Result<(), SemanticIndexError> {
    for (field, value) in [
        ("tenant_id", pointer.tenant_id.as_str()),
        ("binding_id", pointer.binding_id.as_str()),
        ("source_revision", pointer.source_revision.as_str()),
        ("vector_target_id", pointer.vector_target_id.as_str()),
        (
            "composite_policy_digest",
            pointer.composite_policy_digest.as_str(),
        ),
        ("activated_at", pointer.activated_at.as_str()),
    ] {
        validate_text(field, value)?;
    }
    Ok(())
}

fn validate_manifest_text(
    source_revision: &str,
    completed_at: &str,
) -> Result<(), SemanticIndexError> {
    validate_text("source_revision", source_revision)?;
    validate_text("completed_at", completed_at)
}

fn validate_index_pair(
    binding_id: &str,
    binding_digest: SemanticDigest,
    generation: u64,
    source_revision: &str,
    policy_digest: &str,
    lexical: &SemanticLexicalIndexIdentity,
    ann: &SemanticAnnIndexIdentity,
) -> Result<(), SemanticIndexError> {
    if !lexical_matches(
        lexical,
        binding_id,
        binding_digest,
        generation,
        source_revision,
        policy_digest,
    ) || !ann_matches(
        ann,
        binding_id,
        binding_digest,
        generation,
        source_revision,
        policy_digest,
    ) {
        return Err(SemanticIndexError::IndexManifestMismatch);
    }
    Ok(())
}

fn lexical_matches(
    identity: &SemanticLexicalIndexIdentity,
    binding_id: &str,
    binding_digest: SemanticDigest,
    generation: u64,
    source_revision: &str,
    policy_digest: &str,
) -> bool {
    identity.binding_id == binding_id
        && identity.binding_digest == binding_digest
        && identity.generation == generation
        && identity.source_revision == source_revision
        && identity.policy_digest == policy_digest
}

fn ann_matches(
    identity: &SemanticAnnIndexIdentity,
    binding_id: &str,
    binding_digest: SemanticDigest,
    generation: u64,
    source_revision: &str,
    policy_digest: &str,
) -> bool {
    identity.binding_id == binding_id
        && identity.binding_digest == binding_digest
        && identity.generation == generation
        && identity.source_revision == source_revision
        && identity.policy_digest == policy_digest
}

fn pointer_matches_binding(pointer: &SemanticActivePointer, binding: &SemanticBinding) -> bool {
    binding.durable_state == SemanticBindingState::Live
        && pointer.tenant_id == binding.tenant_id
        && pointer.binding_id == binding.binding_id
        && pointer.binding_digest == binding.binding_digest
        && pointer.generation == binding.generation
        && pointer.source_revision == binding.source_revision
        && pointer.vector_target_id == binding.vector_target_id
        && pointer.composite_policy_digest == binding.policy_digest
}

fn pointer_matches_manifest(
    pointer: &SemanticActivePointer,
    manifest: &SemanticIndexManifest,
) -> bool {
    pointer.lexical_index_identity == manifest.lexical.identity
        && pointer.ann_index_identity == manifest.ann.identity
}
