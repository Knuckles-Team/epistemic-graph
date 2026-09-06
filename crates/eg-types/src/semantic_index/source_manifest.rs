use serde::{Deserialize, Serialize};

use super::digest::{
    cbor, domain_digest, SemanticDigest, SEMANTIC_GRAPH_PROJECTION_MANIFEST_DIGEST_DOMAIN,
    SEMANTIC_SQL_SOURCE_IDENTITY_DIGEST_DOMAIN, SEMANTIC_SQL_SOURCE_MANIFEST_DIGEST_DOMAIN,
};
use super::generation::SemanticStageScope;
use super::identity::{validate_generation, validate_text, SemanticBinding};
use super::selector::SemanticSourceSelector;
use super::selector_ref::{SqlColumnRef, SEMANTIC_SQL_CATALOG_ID, SEMANTIC_SQL_SCHEMA_ID};
use super::stage::{SemanticStage, SemanticStageTransition};
use super::state::SemanticIndexError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticSqlSourceIdentity {
    pub tenant_scope: String,
    pub catalog_id: String,
    pub schema_id: String,
    pub table_id: String,
    pub column_id: String,
    /// Server-derived digest of the stable row identity. Raw predicates and
    /// primary-key values are intentionally not transport fields.
    pub record_identity_digest: SemanticDigest,
    pub source_identity_digest: SemanticDigest,
}

impl SemanticSqlSourceIdentity {
    pub fn create(
        selector: &SqlColumnRef,
        tenant_scope: impl Into<String>,
        record_identity_digest: SemanticDigest,
    ) -> Self {
        let mut identity = Self {
            tenant_scope: tenant_scope.into(),
            catalog_id: selector.catalog_id.clone(),
            schema_id: selector.schema_id.clone(),
            table_id: selector.table_id.clone(),
            column_id: selector.column_id.clone(),
            record_identity_digest,
            source_identity_digest: SemanticDigest::from_bytes([0; 32]),
        };
        identity.source_identity_digest = identity.compute_digest();
        identity
    }

    pub fn source_entity_id(&self) -> String {
        format!("semantic-sql-source:{}", self.source_identity_digest)
    }

    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        for (field, value) in [
            ("tenant_scope", self.tenant_scope.as_str()),
            ("catalog_id", self.catalog_id.as_str()),
            ("schema_id", self.schema_id.as_str()),
            ("table_id", self.table_id.as_str()),
            ("column_id", self.column_id.as_str()),
        ] {
            validate_text(field, value)?;
        }
        if self.catalog_id != SEMANTIC_SQL_CATALOG_ID || self.schema_id != SEMANTIC_SQL_SCHEMA_ID {
            return Err(SemanticIndexError::SourceManifestMismatch);
        }
        if self.source_identity_digest != self.compute_digest() {
            return Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_sql_source_identity".to_string(),
            });
        }
        Ok(())
    }

    pub fn validate_against_binding(
        &self,
        binding: &SemanticBinding,
    ) -> Result<(), SemanticIndexError> {
        self.validate()?;
        let SemanticSourceSelector::SqlColumnRef(selector) = &binding.source_selector else {
            return Err(SemanticIndexError::SourceManifestMismatch);
        };
        if self.tenant_scope != binding.tenant_id
            || self.catalog_id != selector.catalog_id
            || self.schema_id != selector.schema_id
            || self.table_id != selector.table_id
            || self.column_id != selector.column_id
        {
            return Err(SemanticIndexError::SourceManifestMismatch);
        }
        Ok(())
    }

    fn compute_digest(&self) -> SemanticDigest {
        let subject = cbor::map([
            ("tenant_scope", cbor::text(&self.tenant_scope)),
            ("catalog_id", cbor::text(&self.catalog_id)),
            ("schema_id", cbor::text(&self.schema_id)),
            ("table_id", cbor::text(&self.table_id)),
            ("column_id", cbor::text(&self.column_id)),
            (
                "record_identity_digest",
                cbor::digest(self.record_identity_digest),
            ),
        ]);
        domain_digest(SEMANTIC_SQL_SOURCE_IDENTITY_DIGEST_DOMAIN, subject)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticSqlSourceManifest {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub source_identity: SemanticSqlSourceIdentity,
    pub source_entity_id: String,
    pub source_revision: String,
    pub source_content_digest: SemanticDigest,
    pub source_schema_revision: u64,
    pub source_schema_digest: String,
    pub source_field_set_digest: String,
    pub source_acl_revision: u64,
    pub source_acl_digest: String,
    pub authorization_receipt_digest: SemanticDigest,
    pub completed_receipt_digest: SemanticDigest,
    pub completed_at: String,
    pub manifest_digest: SemanticDigest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticSqlSourceManifestDraft {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub source_identity: SemanticSqlSourceIdentity,
    pub source_revision: String,
    pub source_content_digest: SemanticDigest,
    pub source_schema_revision: u64,
    pub source_schema_digest: String,
    pub source_field_set_digest: String,
    pub source_acl_revision: u64,
    pub source_acl_digest: String,
    pub authorization_receipt_digest: SemanticDigest,
    pub completed_receipt_digest: SemanticDigest,
    pub completed_at: String,
}

impl SemanticSqlSourceManifest {
    pub fn create(draft: SemanticSqlSourceManifestDraft) -> Result<Self, SemanticIndexError> {
        let source_entity_id = draft.source_identity.source_entity_id();
        let manifest_digest = sql_manifest_digest(&draft, &source_entity_id);
        let manifest = Self {
            binding_id: draft.binding_id,
            binding_digest: draft.binding_digest,
            generation: draft.generation,
            source_identity: draft.source_identity,
            source_entity_id,
            source_revision: draft.source_revision,
            source_content_digest: draft.source_content_digest,
            source_schema_revision: draft.source_schema_revision,
            source_schema_digest: draft.source_schema_digest,
            source_field_set_digest: draft.source_field_set_digest,
            source_acl_revision: draft.source_acl_revision,
            source_acl_digest: draft.source_acl_digest,
            authorization_receipt_digest: draft.authorization_receipt_digest,
            completed_receipt_digest: draft.completed_receipt_digest,
            completed_at: draft.completed_at,
            manifest_digest,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        validate_manifest_coordinates(
            &self.binding_id,
            self.generation,
            &self.source_entity_id,
            &self.source_revision,
            &self.completed_at,
        )?;
        self.source_identity.validate()?;
        if self.source_acl_revision == 0
            || self.source_entity_id != self.source_identity.source_entity_id()
        {
            return Err(SemanticIndexError::SourceManifestMismatch);
        }
        for (field, value) in [
            ("source_schema_digest", self.source_schema_digest.as_str()),
            (
                "source_field_set_digest",
                self.source_field_set_digest.as_str(),
            ),
            ("source_acl_digest", self.source_acl_digest.as_str()),
        ] {
            validate_text(field, value)?;
        }
        if self.manifest_digest != sql_manifest_digest_from_record(self) {
            return Err(SemanticIndexError::SourceManifestMismatch);
        }
        Ok(())
    }

    pub fn validate_against_transition(
        &self,
        transition: &SemanticStageTransition,
    ) -> Result<(), SemanticIndexError> {
        self.validate()?;
        validate_manifest_transition(
            transition,
            SemanticStage::SourceCommit,
            ManifestTransitionEvidence {
                binding_id: &self.binding_id,
                binding_digest: self.binding_digest,
                generation: self.generation,
                source_entity_id: &self.source_entity_id,
                source_revision: &self.source_revision,
                output_digest: self.source_content_digest,
                receipt_digest: self.completed_receipt_digest,
                completed_at: &self.completed_at,
            },
        )
    }

    pub fn validate_against_binding(
        &self,
        binding: &SemanticBinding,
    ) -> Result<(), SemanticIndexError> {
        self.validate()?;
        binding.validate()?;
        self.source_identity.validate_against_binding(binding)?;
        if self.binding_id != binding.binding_id
            || self.binding_digest != binding.binding_digest
            || self.generation != binding.generation
            || self.source_revision != binding.source_revision
            || self.source_schema_digest != binding.source_schema_digest
            || self.source_field_set_digest != binding.source_field_set_digest
            || self.source_acl_revision != binding.policy_identity.components.source_acl_revision
            || self.source_acl_digest != binding.policy_identity.components.source_acl_digest
        {
            return Err(SemanticIndexError::SourceManifestMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticGraphProjectionManifest {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub source_entity_id: String,
    pub source_revision: String,
    pub source_manifest_digest: SemanticDigest,
    pub projection_entity_digest: SemanticDigest,
    pub projection_content_digest: SemanticDigest,
    pub completed_receipt_digest: SemanticDigest,
    pub completed_at: String,
    pub manifest_digest: SemanticDigest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticGraphProjectionManifestDraft {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub source_entity_id: String,
    pub source_revision: String,
    pub source_manifest_digest: SemanticDigest,
    pub projection_entity_digest: SemanticDigest,
    pub projection_content_digest: SemanticDigest,
    pub completed_receipt_digest: SemanticDigest,
    pub completed_at: String,
}

impl SemanticGraphProjectionManifest {
    pub fn create(draft: SemanticGraphProjectionManifestDraft) -> Result<Self, SemanticIndexError> {
        let manifest_digest = graph_manifest_digest(&draft);
        let manifest = Self {
            binding_id: draft.binding_id,
            binding_digest: draft.binding_digest,
            generation: draft.generation,
            source_entity_id: draft.source_entity_id,
            source_revision: draft.source_revision,
            source_manifest_digest: draft.source_manifest_digest,
            projection_entity_digest: draft.projection_entity_digest,
            projection_content_digest: draft.projection_content_digest,
            completed_receipt_digest: draft.completed_receipt_digest,
            completed_at: draft.completed_at,
            manifest_digest,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        validate_manifest_coordinates(
            &self.binding_id,
            self.generation,
            &self.source_entity_id,
            &self.source_revision,
            &self.completed_at,
        )?;
        if self.manifest_digest != graph_manifest_digest_from_record(self) {
            return Err(SemanticIndexError::GraphProjectionManifestMismatch);
        }
        Ok(())
    }

    pub fn validate_against_transition(
        &self,
        transition: &SemanticStageTransition,
    ) -> Result<(), SemanticIndexError> {
        self.validate()?;
        if self.source_manifest_digest != transition.intent.input_digest {
            return Err(SemanticIndexError::GraphProjectionManifestMismatch);
        }
        validate_manifest_transition(
            transition,
            SemanticStage::GraphProjection,
            ManifestTransitionEvidence {
                binding_id: &self.binding_id,
                binding_digest: self.binding_digest,
                generation: self.generation,
                source_entity_id: &self.source_entity_id,
                source_revision: &self.source_revision,
                output_digest: self.projection_content_digest,
                receipt_digest: self.completed_receipt_digest,
                completed_at: &self.completed_at,
            },
        )
    }
}

struct ManifestTransitionEvidence<'a> {
    binding_id: &'a str,
    binding_digest: SemanticDigest,
    generation: u64,
    source_entity_id: &'a str,
    source_revision: &'a str,
    output_digest: SemanticDigest,
    receipt_digest: SemanticDigest,
    completed_at: &'a str,
}

fn validate_manifest_transition(
    transition: &SemanticStageTransition,
    stage: SemanticStage,
    evidence: ManifestTransitionEvidence<'_>,
) -> Result<(), SemanticIndexError> {
    let entity_matches = matches!(
        &transition.intent.scope,
        SemanticStageScope::Entity { source_entity_id: actual }
            if actual == evidence.source_entity_id
    );
    if transition.intent.stage != stage
        || transition.intent.binding_id != evidence.binding_id
        || transition.intent.binding_digest != evidence.binding_digest
        || transition.intent.generation != evidence.generation
        || transition.intent.source_revision != evidence.source_revision
        || !entity_matches
        || transition.receipt.output_digest != evidence.output_digest
        || transition.receipt.receipt_digest() != evidence.receipt_digest
        || transition.receipt.completed_at != evidence.completed_at
    {
        return Err(SemanticIndexError::StageArtifactMismatch);
    }
    Ok(())
}

fn validate_manifest_coordinates(
    binding_id: &str,
    generation: u64,
    source_entity_id: &str,
    source_revision: &str,
    completed_at: &str,
) -> Result<(), SemanticIndexError> {
    validate_generation(generation)?;
    for (field, value) in [
        ("binding_id", binding_id),
        ("source_entity_id", source_entity_id),
        ("source_revision", source_revision),
        ("completed_at", completed_at),
    ] {
        validate_text(field, value)?;
    }
    Ok(())
}

fn sql_manifest_digest(
    draft: &SemanticSqlSourceManifestDraft,
    source_entity_id: &str,
) -> SemanticDigest {
    sql_manifest_digest_parts(
        (&draft.binding_id, draft.binding_digest, draft.generation),
        (
            &draft.source_identity,
            source_entity_id,
            &draft.source_revision,
        ),
        (
            draft.source_content_digest,
            draft.source_schema_revision,
            &draft.source_schema_digest,
            &draft.source_field_set_digest,
        ),
        (
            draft.source_acl_revision,
            &draft.source_acl_digest,
            draft.authorization_receipt_digest,
        ),
        (draft.completed_receipt_digest, &draft.completed_at),
    )
}

fn sql_manifest_digest_from_record(record: &SemanticSqlSourceManifest) -> SemanticDigest {
    sql_manifest_digest_parts(
        (&record.binding_id, record.binding_digest, record.generation),
        (
            &record.source_identity,
            &record.source_entity_id,
            &record.source_revision,
        ),
        (
            record.source_content_digest,
            record.source_schema_revision,
            &record.source_schema_digest,
            &record.source_field_set_digest,
        ),
        (
            record.source_acl_revision,
            &record.source_acl_digest,
            record.authorization_receipt_digest,
        ),
        (record.completed_receipt_digest, &record.completed_at),
    )
}

fn sql_manifest_digest_parts(
    binding: (&str, SemanticDigest, u64),
    source: (&SemanticSqlSourceIdentity, &str, &str),
    content: (SemanticDigest, u64, &str, &str),
    authority: (u64, &str, SemanticDigest),
    completion: (SemanticDigest, &str),
) -> SemanticDigest {
    let subject = cbor::map([
        ("binding_id", cbor::text(binding.0)),
        ("binding_digest", cbor::digest(binding.1)),
        ("generation", cbor::unsigned(binding.2)),
        (
            "source_identity_digest",
            cbor::digest(source.0.source_identity_digest),
        ),
        ("source_entity_id", cbor::text(source.1)),
        ("source_revision", cbor::text(source.2)),
        ("source_content_digest", cbor::digest(content.0)),
        ("source_schema_revision", cbor::unsigned(content.1)),
        ("source_schema_digest", cbor::text(content.2)),
        ("source_field_set_digest", cbor::text(content.3)),
        ("source_acl_revision", cbor::unsigned(authority.0)),
        ("source_acl_digest", cbor::text(authority.1)),
        ("authorization_receipt_digest", cbor::digest(authority.2)),
        ("completed_receipt_digest", cbor::digest(completion.0)),
        ("completed_at", cbor::text(completion.1)),
    ]);
    domain_digest(SEMANTIC_SQL_SOURCE_MANIFEST_DIGEST_DOMAIN, subject)
}

fn graph_manifest_digest(draft: &SemanticGraphProjectionManifestDraft) -> SemanticDigest {
    graph_manifest_digest_parts(
        (&draft.binding_id, draft.binding_digest, draft.generation),
        (&draft.source_entity_id, &draft.source_revision),
        (
            draft.source_manifest_digest,
            draft.projection_entity_digest,
            draft.projection_content_digest,
        ),
        (draft.completed_receipt_digest, &draft.completed_at),
    )
}

fn graph_manifest_digest_from_record(record: &SemanticGraphProjectionManifest) -> SemanticDigest {
    graph_manifest_digest_parts(
        (&record.binding_id, record.binding_digest, record.generation),
        (&record.source_entity_id, &record.source_revision),
        (
            record.source_manifest_digest,
            record.projection_entity_digest,
            record.projection_content_digest,
        ),
        (record.completed_receipt_digest, &record.completed_at),
    )
}

fn graph_manifest_digest_parts(
    binding: (&str, SemanticDigest, u64),
    source: (&str, &str),
    projection: (SemanticDigest, SemanticDigest, SemanticDigest),
    completion: (SemanticDigest, &str),
) -> SemanticDigest {
    let subject = cbor::map([
        ("binding_id", cbor::text(binding.0)),
        ("binding_digest", cbor::digest(binding.1)),
        ("generation", cbor::unsigned(binding.2)),
        ("source_entity_id", cbor::text(source.0)),
        ("source_revision", cbor::text(source.1)),
        ("source_manifest_digest", cbor::digest(projection.0)),
        ("projection_entity_digest", cbor::digest(projection.1)),
        ("projection_content_digest", cbor::digest(projection.2)),
        ("completed_receipt_digest", cbor::digest(completion.0)),
        ("completed_at", cbor::text(completion.1)),
    ]);
    domain_digest(SEMANTIC_GRAPH_PROJECTION_MANIFEST_DIGEST_DOMAIN, subject)
}
