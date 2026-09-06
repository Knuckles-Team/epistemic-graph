use serde::{Deserialize, Serialize};

use super::digest::{
    cbor, domain_digest, SemanticDigest, SEMANTIC_AUTHORIZATION_RECEIPT_DIGEST_DOMAIN,
    SEMANTIC_LINEAGE_DIGEST_DOMAIN,
};
use super::generation::SemanticStageScope;
use super::identity::{validate_generation, validate_text};
use super::policy::SemanticPolicyIdentity;
use super::stage::{SemanticStage, SemanticStageTransition};
use super::state::SemanticIndexError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAuthorizationReceipt {
    pub tenant_id: String,
    pub actor_scope: String,
    pub effective_actor_scope: String,
    pub purpose_id: String,
    pub policy_identity: SemanticPolicyIdentity,
    pub policy_decision_digest: String,
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub scope: SemanticStageScope,
    pub source_revision: String,
    pub authorized_at: String,
    pub authorization_receipt_digest: SemanticDigest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAuthorizationReceiptDraft {
    pub tenant_id: String,
    pub actor_scope: String,
    pub effective_actor_scope: String,
    pub purpose_id: String,
    pub policy_identity: SemanticPolicyIdentity,
    pub policy_decision_digest: String,
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub scope: SemanticStageScope,
    pub source_revision: String,
    pub authorized_at: String,
}

impl SemanticAuthorizationReceipt {
    pub fn create(draft: SemanticAuthorizationReceiptDraft) -> Result<Self, SemanticIndexError> {
        let authorization_receipt_digest = authorization_receipt_digest(&draft);
        let receipt = Self {
            tenant_id: draft.tenant_id,
            actor_scope: draft.actor_scope,
            effective_actor_scope: draft.effective_actor_scope,
            purpose_id: draft.purpose_id,
            policy_identity: draft.policy_identity,
            policy_decision_digest: draft.policy_decision_digest,
            binding_id: draft.binding_id,
            binding_digest: draft.binding_digest,
            generation: draft.generation,
            scope: draft.scope,
            source_revision: draft.source_revision,
            authorized_at: draft.authorized_at,
            authorization_receipt_digest,
        };
        receipt.validate()?;
        Ok(receipt)
    }

    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        validate_generation(self.generation)?;
        self.scope.validate()?;
        self.policy_identity.validate()?;
        for (field, value) in [
            ("tenant_id", self.tenant_id.as_str()),
            ("actor_scope", self.actor_scope.as_str()),
            ("effective_actor_scope", self.effective_actor_scope.as_str()),
            ("purpose_id", self.purpose_id.as_str()),
            (
                "policy_decision_digest",
                self.policy_decision_digest.as_str(),
            ),
            ("binding_id", self.binding_id.as_str()),
            ("source_revision", self.source_revision.as_str()),
            ("authorized_at", self.authorized_at.as_str()),
        ] {
            validate_text(field, value)?;
        }
        if self.policy_identity.tenant_id != self.tenant_id
            || self.policy_identity.effective_actor_scope != self.effective_actor_scope
            || self.policy_identity.purpose_id != self.purpose_id
        {
            return Err(SemanticIndexError::AuthorizationReceiptMismatch);
        }
        if self.authorization_receipt_digest != authorization_receipt_digest_from_record(self) {
            return Err(SemanticIndexError::AuthorizationReceiptMismatch);
        }
        Ok(())
    }

    pub fn validate_against_transition(
        &self,
        transition: &SemanticStageTransition,
    ) -> Result<(), SemanticIndexError> {
        self.validate()?;
        if transition.intent.stage != SemanticStage::SourceCommit
            || self.binding_id != transition.intent.binding_id
            || self.binding_digest != transition.intent.binding_digest
            || self.generation != transition.intent.generation
            || self.scope != transition.intent.scope
            || self.source_revision != transition.intent.source_revision
        {
            return Err(SemanticIndexError::AuthorizationReceiptMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticLineage {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub source_entity_id: String,
    pub source_revision: String,
    pub purpose_id: String,
    pub policy_digest: String,
    pub model_digest: String,
    pub preprocess_digest: String,
    pub lexical_index_digest: SemanticDigest,
    pub ann_index_digest: SemanticDigest,
    pub generation_checkpoint_digest: SemanticDigest,
    pub authorization_receipt_digest: SemanticDigest,
    pub lineage_digest: SemanticDigest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticLineageDraft {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub source_entity_id: String,
    pub source_revision: String,
    pub purpose_id: String,
    pub policy_digest: String,
    pub model_digest: String,
    pub preprocess_digest: String,
    pub lexical_index_digest: SemanticDigest,
    pub ann_index_digest: SemanticDigest,
    pub generation_checkpoint_digest: SemanticDigest,
    pub authorization_receipt_digest: SemanticDigest,
}

impl SemanticLineage {
    pub fn create(draft: SemanticLineageDraft) -> Result<Self, SemanticIndexError> {
        let lineage_digest = lineage_digest(&draft);
        let lineage = Self {
            binding_id: draft.binding_id,
            binding_digest: draft.binding_digest,
            generation: draft.generation,
            source_entity_id: draft.source_entity_id,
            source_revision: draft.source_revision,
            purpose_id: draft.purpose_id,
            policy_digest: draft.policy_digest,
            model_digest: draft.model_digest,
            preprocess_digest: draft.preprocess_digest,
            lexical_index_digest: draft.lexical_index_digest,
            ann_index_digest: draft.ann_index_digest,
            generation_checkpoint_digest: draft.generation_checkpoint_digest,
            authorization_receipt_digest: draft.authorization_receipt_digest,
            lineage_digest,
        };
        lineage.validate()?;
        Ok(lineage)
    }

    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        validate_generation(self.generation)?;
        for (field, value) in [
            ("binding_id", self.binding_id.as_str()),
            ("source_entity_id", self.source_entity_id.as_str()),
            ("source_revision", self.source_revision.as_str()),
            ("purpose_id", self.purpose_id.as_str()),
            ("policy_digest", self.policy_digest.as_str()),
            ("model_digest", self.model_digest.as_str()),
            ("preprocess_digest", self.preprocess_digest.as_str()),
        ] {
            validate_text(field, value)?;
        }
        if self.lineage_digest != lineage_digest_from_record(self) {
            return Err(SemanticIndexError::LineageMismatch);
        }
        Ok(())
    }
}

fn authorization_receipt_digest(draft: &SemanticAuthorizationReceiptDraft) -> SemanticDigest {
    authorization_digest_parts(
        (
            &draft.tenant_id,
            &draft.actor_scope,
            &draft.effective_actor_scope,
        ),
        (
            &draft.purpose_id,
            &draft.policy_identity,
            &draft.policy_decision_digest,
        ),
        (&draft.binding_id, draft.binding_digest, draft.generation),
        (&draft.scope, &draft.source_revision, &draft.authorized_at),
    )
}

fn authorization_receipt_digest_from_record(
    record: &SemanticAuthorizationReceipt,
) -> SemanticDigest {
    authorization_digest_parts(
        (
            &record.tenant_id,
            &record.actor_scope,
            &record.effective_actor_scope,
        ),
        (
            &record.purpose_id,
            &record.policy_identity,
            &record.policy_decision_digest,
        ),
        (&record.binding_id, record.binding_digest, record.generation),
        (
            &record.scope,
            &record.source_revision,
            &record.authorized_at,
        ),
    )
}

fn authorization_digest_parts(
    authority: (&str, &str, &str),
    policy: (&str, &SemanticPolicyIdentity, &str),
    binding: (&str, SemanticDigest, u64),
    source: (&SemanticStageScope, &str, &str),
) -> SemanticDigest {
    let subject = cbor::map([
        ("tenant_id", cbor::text(authority.0)),
        ("actor_scope", cbor::text(authority.1)),
        ("effective_actor_scope", cbor::text(authority.2)),
        ("purpose_id", cbor::text(policy.0)),
        ("policy_identity", policy.1.canonical_cbor()),
        ("policy_decision_digest", cbor::text(policy.2)),
        ("binding_id", cbor::text(binding.0)),
        ("binding_digest", cbor::digest(binding.1)),
        ("generation", cbor::unsigned(binding.2)),
        ("scope", source.0.canonical_cbor()),
        ("source_revision", cbor::text(source.1)),
        ("authorized_at", cbor::text(source.2)),
    ]);
    domain_digest(SEMANTIC_AUTHORIZATION_RECEIPT_DIGEST_DOMAIN, subject)
}

fn lineage_digest(draft: &SemanticLineageDraft) -> SemanticDigest {
    lineage_digest_parts(
        (&draft.binding_id, draft.binding_digest, draft.generation),
        (&draft.source_entity_id, &draft.source_revision),
        (
            &draft.purpose_id,
            &draft.policy_digest,
            &draft.model_digest,
            &draft.preprocess_digest,
        ),
        (
            draft.lexical_index_digest,
            draft.ann_index_digest,
            draft.generation_checkpoint_digest,
            draft.authorization_receipt_digest,
        ),
    )
}

fn lineage_digest_from_record(record: &SemanticLineage) -> SemanticDigest {
    lineage_digest_parts(
        (&record.binding_id, record.binding_digest, record.generation),
        (&record.source_entity_id, &record.source_revision),
        (
            &record.purpose_id,
            &record.policy_digest,
            &record.model_digest,
            &record.preprocess_digest,
        ),
        (
            record.lexical_index_digest,
            record.ann_index_digest,
            record.generation_checkpoint_digest,
            record.authorization_receipt_digest,
        ),
    )
}

fn lineage_digest_parts(
    binding: (&str, SemanticDigest, u64),
    source: (&str, &str),
    context: (&str, &str, &str, &str),
    proof: (
        SemanticDigest,
        SemanticDigest,
        SemanticDigest,
        SemanticDigest,
    ),
) -> SemanticDigest {
    let subject = cbor::map([
        ("binding_id", cbor::text(binding.0)),
        ("binding_digest", cbor::digest(binding.1)),
        ("generation", cbor::unsigned(binding.2)),
        ("source_entity_id", cbor::text(source.0)),
        ("source_revision", cbor::text(source.1)),
        ("purpose_id", cbor::text(context.0)),
        ("policy_digest", cbor::text(context.1)),
        ("model_digest", cbor::text(context.2)),
        ("preprocess_digest", cbor::text(context.3)),
        ("lexical_index_digest", cbor::digest(proof.0)),
        ("ann_index_digest", cbor::digest(proof.1)),
        ("generation_checkpoint_digest", cbor::digest(proof.2)),
        ("authorization_receipt_digest", cbor::digest(proof.3)),
    ]);
    domain_digest(SEMANTIC_LINEAGE_DIGEST_DOMAIN, subject)
}
