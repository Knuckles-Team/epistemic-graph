use serde::{Deserialize, Serialize};

use super::digest::{
    cbor, domain_digest, SemanticDigest, SEMANTIC_TOMBSTONE_RECEIPT_DIGEST_DOMAIN,
};
use super::identity::{validate_generation, validate_text, SemanticBinding};
use super::state::SemanticIndexError;

/// Durable deletion fact for one exact binding generation. The store must
/// persist this complete canonical record; a raw digest row cannot prove which
/// tenant, generation, or deletion time the receipt authorized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticTombstone {
    pub tenant_id: String,
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub tombstone_receipt_digest: SemanticDigest,
    pub deleted_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticTombstoneDraft {
    pub tenant_id: String,
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub deleted_at: String,
}

impl SemanticTombstone {
    pub fn create(draft: SemanticTombstoneDraft) -> Result<Self, SemanticIndexError> {
        let tombstone_receipt_digest = tombstone_receipt_digest(&draft);
        let tombstone = Self {
            tenant_id: draft.tenant_id,
            binding_id: draft.binding_id,
            binding_digest: draft.binding_digest,
            generation: draft.generation,
            tombstone_receipt_digest,
            deleted_at: draft.deleted_at,
        };
        tombstone.validate()?;
        Ok(tombstone)
    }

    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        validate_text("tenant_id", &self.tenant_id)?;
        validate_text("binding_id", &self.binding_id)?;
        validate_text("deleted_at", &self.deleted_at)?;
        validate_generation(self.generation)?;
        if self.tombstone_receipt_digest != tombstone_receipt_digest_from_record(self) {
            return Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_tombstone_receipt".to_string(),
            });
        }
        Ok(())
    }

    pub fn validate_against(&self, binding: &SemanticBinding) -> Result<(), SemanticIndexError> {
        self.validate()?;
        binding.validate()?;
        if self.tenant_id != binding.tenant_id
            || self.binding_id != binding.binding_id
            || self.binding_digest != binding.binding_digest
        {
            return Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_tombstone_binding".to_string(),
            });
        }
        if self.generation != binding.generation {
            return Err(SemanticIndexError::GenerationMismatch {
                expected: binding.generation,
                actual: self.generation,
            });
        }
        Ok(())
    }
}

fn tombstone_receipt_digest(draft: &SemanticTombstoneDraft) -> SemanticDigest {
    tombstone_receipt_digest_parts(
        &draft.tenant_id,
        &draft.binding_id,
        draft.binding_digest,
        draft.generation,
        &draft.deleted_at,
    )
}

fn tombstone_receipt_digest_from_record(record: &SemanticTombstone) -> SemanticDigest {
    tombstone_receipt_digest_parts(
        &record.tenant_id,
        &record.binding_id,
        record.binding_digest,
        record.generation,
        &record.deleted_at,
    )
}

fn tombstone_receipt_digest_parts(
    tenant_id: &str,
    binding_id: &str,
    binding_digest: SemanticDigest,
    generation: u64,
    deleted_at: &str,
) -> SemanticDigest {
    let subject = cbor::map([
        ("tenant_id", cbor::text(tenant_id)),
        ("binding_id", cbor::text(binding_id)),
        ("binding_digest", cbor::digest(binding_digest)),
        ("generation", cbor::unsigned(generation)),
        ("deleted_at", cbor::text(deleted_at)),
    ]);
    domain_digest(SEMANTIC_TOMBSTONE_RECEIPT_DIGEST_DOMAIN, subject)
}
