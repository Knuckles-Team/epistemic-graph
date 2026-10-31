use serde::{Deserialize, Serialize};

use crate::embedding::MAX_MAINTAINED_ANN_DIMENSIONS;

use super::config::{
    SemanticAnnIndexMethod, SemanticAnnIndexSpec, SemanticBindingState, SemanticLexicalIndexSpec,
    SemanticModelIdentity, SemanticVectorMetric,
};
use super::digest::{
    cbor, domain_digest, SemanticDigest, SEMANTIC_ANN_INDEX_DIGEST_DOMAIN,
    SEMANTIC_BINDING_DIGEST_DOMAIN, SEMANTIC_LEXICAL_INDEX_DIGEST_DOMAIN,
    SEMANTIC_VECTOR_DIGEST_DOMAIN,
};
use super::policy::{SemanticPolicyComponents, SemanticPolicyIdentity};
use super::selector::SemanticSourceSelector;
use super::stage::SEMANTIC_QUEUE_PROFILE_ID;
use super::state::{SemanticBindingStateTransition, SemanticIndexError};

const MAX_IDENTITY_TEXT_BYTES: usize = 4 * 1024;

pub const SEMANTIC_BINDING_SCHEMA: &str = "semantic-binding/v1";
pub const SEMANTIC_VECTOR_SCHEMA: &str = "semantic-vector/v1";
pub const SEMANTIC_LEXICAL_INDEX_SCHEMA: &str = "semantic-lexical-index/v1";
pub const SEMANTIC_ANN_INDEX_SCHEMA: &str = "semantic-ann-index/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticLexicalIndexIdentity {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub analyzer_id: String,
    pub analyzer_revision: String,
    pub analyzer_config_digest: String,
    pub generation: u64,
    pub source_revision: String,
    pub purpose_id: String,
    pub policy_digest: String,
    pub lexical_index_digest: SemanticDigest,
}

impl SemanticLexicalIndexIdentity {
    fn derive(
        binding_id: &str,
        binding_digest: SemanticDigest,
        spec: &SemanticLexicalIndexSpec,
        generation: u64,
        source_revision: &str,
        purpose_id: &str,
        policy_digest: &str,
    ) -> Self {
        let subject = cbor::map([
            ("binding_id", cbor::text(binding_id)),
            ("binding_digest", cbor::digest(binding_digest)),
            ("analyzer_id", cbor::text(&spec.analyzer_id)),
            ("analyzer_revision", cbor::text(&spec.analyzer_revision)),
            (
                "analyzer_config_digest",
                cbor::text(&spec.analyzer_config_digest),
            ),
            ("generation", cbor::unsigned(generation)),
            ("source_revision", cbor::text(source_revision)),
            ("purpose_id", cbor::text(purpose_id)),
            ("policy_digest", cbor::text(policy_digest)),
        ]);
        Self {
            binding_id: binding_id.to_string(),
            binding_digest,
            analyzer_id: spec.analyzer_id.clone(),
            analyzer_revision: spec.analyzer_revision.clone(),
            analyzer_config_digest: spec.analyzer_config_digest.clone(),
            generation,
            source_revision: source_revision.to_string(),
            purpose_id: purpose_id.to_string(),
            policy_digest: policy_digest.to_string(),
            lexical_index_digest: domain_digest(SEMANTIC_LEXICAL_INDEX_DIGEST_DOMAIN, subject),
        }
    }

    pub fn spec(&self) -> SemanticLexicalIndexSpec {
        SemanticLexicalIndexSpec {
            analyzer_id: self.analyzer_id.clone(),
            analyzer_revision: self.analyzer_revision.clone(),
            analyzer_config_digest: self.analyzer_config_digest.clone(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), SemanticIndexError> {
        for (field, value) in [
            ("binding_id", self.binding_id.as_str()),
            ("analyzer_id", self.analyzer_id.as_str()),
            ("analyzer_revision", self.analyzer_revision.as_str()),
            (
                "analyzer_config_digest",
                self.analyzer_config_digest.as_str(),
            ),
            ("source_revision", self.source_revision.as_str()),
            ("purpose_id", self.purpose_id.as_str()),
            ("policy_digest", self.policy_digest.as_str()),
        ] {
            validate_text(field, value)?;
        }
        validate_generation(self.generation)?;
        let expected = Self::derive(
            &self.binding_id,
            self.binding_digest,
            &self.spec(),
            self.generation,
            &self.source_revision,
            &self.purpose_id,
            &self.policy_digest,
        );
        if self != &expected {
            return Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_lexical_index".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticAnnIndexIdentity {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub method: SemanticAnnIndexMethod,
    pub parameters_digest: String,
    pub generation: u64,
    pub source_revision: String,
    pub model_digest: String,
    pub dimension: u32,
    pub purpose_id: String,
    pub policy_digest: String,
    pub ann_index_digest: SemanticDigest,
}

impl SemanticAnnIndexIdentity {
    fn derive(input: AnnIdentityInput<'_>) -> Self {
        let AnnIdentityInput {
            binding_id,
            binding_digest,
            spec,
            generation,
            source_revision,
            model_digest,
            dimension,
            purpose_id,
            policy_digest,
        } = input;
        let subject = cbor::map([
            ("binding_id", cbor::text(binding_id)),
            ("binding_digest", cbor::digest(binding_digest)),
            ("method", cbor::text(spec.method.as_str())),
            ("parameters_digest", cbor::text(&spec.parameters_digest)),
            ("generation", cbor::unsigned(generation)),
            ("source_revision", cbor::text(source_revision)),
            ("model_digest", cbor::text(model_digest)),
            ("dimension", cbor::unsigned(dimension.into())),
            ("purpose_id", cbor::text(purpose_id)),
            ("policy_digest", cbor::text(policy_digest)),
        ]);
        Self {
            binding_id: binding_id.to_string(),
            binding_digest,
            method: spec.method,
            parameters_digest: spec.parameters_digest.clone(),
            generation,
            source_revision: source_revision.to_string(),
            model_digest: model_digest.to_string(),
            dimension,
            purpose_id: purpose_id.to_string(),
            policy_digest: policy_digest.to_string(),
            ann_index_digest: domain_digest(SEMANTIC_ANN_INDEX_DIGEST_DOMAIN, subject),
        }
    }

    pub fn spec(&self) -> SemanticAnnIndexSpec {
        SemanticAnnIndexSpec {
            method: self.method,
            parameters_digest: self.parameters_digest.clone(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), SemanticIndexError> {
        for (field, value) in [
            ("binding_id", self.binding_id.as_str()),
            ("parameters_digest", self.parameters_digest.as_str()),
            ("source_revision", self.source_revision.as_str()),
            ("model_digest", self.model_digest.as_str()),
            ("purpose_id", self.purpose_id.as_str()),
            ("policy_digest", self.policy_digest.as_str()),
        ] {
            validate_text(field, value)?;
        }
        validate_generation(self.generation)?;
        if self.dimension == 0 || self.dimension as usize > MAX_MAINTAINED_ANN_DIMENSIONS {
            return Err(SemanticIndexError::InvalidField {
                field: "dimension".to_string(),
                reason: "outside maintained ANN dimension bounds".to_string(),
            });
        }
        let expected = Self::derive(AnnIdentityInput {
            binding_id: &self.binding_id,
            binding_digest: self.binding_digest,
            spec: &self.spec(),
            generation: self.generation,
            source_revision: &self.source_revision,
            model_digest: &self.model_digest,
            dimension: self.dimension,
            purpose_id: &self.purpose_id,
            policy_digest: &self.policy_digest,
        });
        if self != &expected {
            return Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_ann_index".to_string(),
            });
        }
        Ok(())
    }
}

struct AnnIdentityInput<'a> {
    binding_id: &'a str,
    binding_digest: SemanticDigest,
    spec: &'a SemanticAnnIndexSpec,
    generation: u64,
    source_revision: &'a str,
    model_digest: &'a str,
    dimension: u32,
    purpose_id: &'a str,
    policy_digest: &'a str,
}

/// Caller-supplied fields; state and final digests are constructor-owned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticBindingDraft {
    pub binding_id: String,
    pub tenant_id: String,
    /// Originating principal scope from the authenticated carrier authority.
    pub actor_scope: String,
    /// Opaque server-derived scope for the verified delegated agent, distinct
    /// from the originating principal and never inferred from approval.
    pub effective_actor_scope: String,
    pub purpose_id: String,
    pub policy: SemanticPolicyComponents,
    pub source_selector: SemanticSourceSelector,
    pub source_schema_digest: String,
    pub source_revision: String,
    pub source_field_set_digest: String,
    pub dimension: u32,
    pub metric: SemanticVectorMetric,
    pub model: SemanticModelIdentity,
    pub generation: u64,
    pub maintenance_policy_id: String,
    pub lexical_index: SemanticLexicalIndexSpec,
    pub ann_index: SemanticAnnIndexSpec,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticBinding {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub tenant_id: String,
    pub actor_scope: String,
    pub effective_actor_scope: String,
    pub purpose_id: String,
    pub policy_identity: SemanticPolicyIdentity,
    pub policy_digest: String,
    pub source_selector: SemanticSourceSelector,
    pub source_schema_digest: String,
    pub source_revision: String,
    pub source_field_set_digest: String,
    pub vector_target_id: String,
    pub dimension: u32,
    pub metric: SemanticVectorMetric,
    pub model_id: String,
    pub model_revision: String,
    pub preprocess_digest: String,
    pub model_digest: String,
    pub generation: u64,
    pub maintenance_policy_id: String,
    pub queue_profile_id: String,
    pub lexical_index_identity: SemanticLexicalIndexIdentity,
    pub ann_index_identity: SemanticAnnIndexIdentity,
    pub created_at: String,
    pub durable_state: SemanticBindingState,
}

impl SemanticBinding {
    /// Derive the binding digest first, then both complete child identities.
    pub fn create(draft: SemanticBindingDraft) -> Result<Self, SemanticIndexError> {
        validate_draft(&draft)?;
        let policy_identity = policy_identity_for_draft(&draft)?;
        let policy_digest = policy_identity.policy_digest.clone();
        let durable_state = SemanticBindingState::Pending;
        let vector_target_id = managed_vector_target_id(&draft.binding_id);
        let binding_digest = binding_digest(&draft, &policy_identity, &vector_target_id);
        let lexical_index_identity = SemanticLexicalIndexIdentity::derive(
            &draft.binding_id,
            binding_digest,
            &draft.lexical_index,
            draft.generation,
            &draft.source_revision,
            &draft.purpose_id,
            &policy_digest,
        );
        let ann_index_identity = SemanticAnnIndexIdentity::derive(AnnIdentityInput {
            binding_id: &draft.binding_id,
            binding_digest,
            spec: &draft.ann_index,
            generation: draft.generation,
            source_revision: &draft.source_revision,
            model_digest: &draft.model.model_digest,
            dimension: draft.dimension,
            purpose_id: &draft.purpose_id,
            policy_digest: &policy_digest,
        });
        Ok(Self {
            binding_id: draft.binding_id,
            binding_digest,
            tenant_id: draft.tenant_id,
            actor_scope: draft.actor_scope,
            effective_actor_scope: draft.effective_actor_scope,
            purpose_id: draft.purpose_id,
            policy_identity,
            policy_digest,
            source_selector: draft.source_selector,
            source_schema_digest: draft.source_schema_digest,
            source_revision: draft.source_revision,
            source_field_set_digest: draft.source_field_set_digest,
            vector_target_id,
            dimension: draft.dimension,
            metric: draft.metric,
            model_id: draft.model.model_id,
            model_revision: draft.model.model_revision,
            preprocess_digest: draft.model.preprocess_digest,
            model_digest: draft.model.model_digest,
            generation: draft.generation,
            maintenance_policy_id: draft.maintenance_policy_id,
            queue_profile_id: SEMANTIC_QUEUE_PROFILE_ID.to_string(),
            lexical_index_identity,
            ann_index_identity,
            created_at: draft.created_at,
            durable_state,
        })
    }

    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        let draft = self.to_draft();
        validate_draft(&draft)?;
        let expected_target = managed_vector_target_id(&self.binding_id);
        if self.vector_target_id != expected_target
            || self.queue_profile_id != SEMANTIC_QUEUE_PROFILE_ID
        {
            return Err(SemanticIndexError::UnmanagedBindingResource);
        }
        self.policy_identity.validate()?;
        let expected_policy = policy_identity_for_draft(&draft)?;
        if self.policy_identity != expected_policy
            || self.policy_digest != self.policy_identity.policy_digest
        {
            return Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_policy".to_string(),
            });
        }
        let expected_digest = binding_digest(&draft, &expected_policy, &expected_target);
        if self.binding_digest != expected_digest {
            return Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_binding".to_string(),
            });
        }
        let expected_lexical = SemanticLexicalIndexIdentity::derive(
            &self.binding_id,
            self.binding_digest,
            &draft.lexical_index,
            self.generation,
            &self.source_revision,
            &self.purpose_id,
            &self.policy_digest,
        );
        if self.lexical_index_identity != expected_lexical {
            return Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_lexical_index".to_string(),
            });
        }
        let expected_ann = SemanticAnnIndexIdentity::derive(AnnIdentityInput {
            binding_id: &self.binding_id,
            binding_digest: self.binding_digest,
            spec: &draft.ann_index,
            generation: self.generation,
            source_revision: &self.source_revision,
            model_digest: &self.model_digest,
            dimension: self.dimension,
            purpose_id: &self.purpose_id,
            policy_digest: &self.policy_digest,
        });
        if self.ann_index_identity != expected_ann {
            return Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_ann_index".to_string(),
            });
        }
        Ok(())
    }

    pub fn apply_state_transition(
        &self,
        transition: &SemanticBindingStateTransition,
    ) -> Result<Self, SemanticIndexError> {
        self.validate()?;
        transition.validate_against(self)?;
        let mut updated = self.clone();
        updated.durable_state = transition.next;
        Ok(updated)
    }

    fn to_draft(&self) -> SemanticBindingDraft {
        SemanticBindingDraft {
            binding_id: self.binding_id.clone(),
            tenant_id: self.tenant_id.clone(),
            actor_scope: self.actor_scope.clone(),
            effective_actor_scope: self.effective_actor_scope.clone(),
            purpose_id: self.purpose_id.clone(),
            policy: self.policy_identity.components.clone(),
            source_selector: self.source_selector.clone(),
            source_schema_digest: self.source_schema_digest.clone(),
            source_revision: self.source_revision.clone(),
            source_field_set_digest: self.source_field_set_digest.clone(),
            dimension: self.dimension,
            metric: self.metric,
            model: SemanticModelIdentity {
                model_id: self.model_id.clone(),
                model_revision: self.model_revision.clone(),
                preprocess_digest: self.preprocess_digest.clone(),
                model_digest: self.model_digest.clone(),
            },
            generation: self.generation,
            maintenance_policy_id: self.maintenance_policy_id.clone(),
            lexical_index: self.lexical_index_identity.spec(),
            ann_index: self.ann_index_identity.spec(),
            created_at: self.created_at.clone(),
        }
    }
}

fn validate_draft(draft: &SemanticBindingDraft) -> Result<(), SemanticIndexError> {
    draft.source_selector.require_implemented()?;
    for (field, value) in [
        ("binding_id", draft.binding_id.as_str()),
        ("tenant_id", draft.tenant_id.as_str()),
        ("actor_scope", draft.actor_scope.as_str()),
        (
            "effective_actor_scope",
            draft.effective_actor_scope.as_str(),
        ),
        ("purpose_id", draft.purpose_id.as_str()),
        ("source_schema_digest", draft.source_schema_digest.as_str()),
        ("source_revision", draft.source_revision.as_str()),
        (
            "source_field_set_digest",
            draft.source_field_set_digest.as_str(),
        ),
        ("model_id", draft.model.model_id.as_str()),
        ("model_revision", draft.model.model_revision.as_str()),
        ("preprocess_digest", draft.model.preprocess_digest.as_str()),
        ("model_digest", draft.model.model_digest.as_str()),
        (
            "maintenance_policy_id",
            draft.maintenance_policy_id.as_str(),
        ),
        ("analyzer_id", draft.lexical_index.analyzer_id.as_str()),
        (
            "analyzer_revision",
            draft.lexical_index.analyzer_revision.as_str(),
        ),
        (
            "analyzer_config_digest",
            draft.lexical_index.analyzer_config_digest.as_str(),
        ),
        (
            "parameters_digest",
            draft.ann_index.parameters_digest.as_str(),
        ),
        ("created_at", draft.created_at.as_str()),
    ] {
        validate_text(field, value)?;
    }
    if draft.dimension == 0 || draft.dimension as usize > MAX_MAINTAINED_ANN_DIMENSIONS {
        return Err(SemanticIndexError::InvalidField {
            field: "dimension".to_string(),
            reason: format!(
                "must be within 1..={MAX_MAINTAINED_ANN_DIMENSIONS} for a maintained ANN binding"
            ),
        });
    }
    if draft.generation == 0 {
        return Err(SemanticIndexError::InvalidField {
            field: "generation".to_string(),
            reason: "must start at one".to_string(),
        });
    }
    if draft.ann_index.method == SemanticAnnIndexMethod::IvfPq
        && draft.metric == SemanticVectorMetric::DotProduct
    {
        return Err(SemanticIndexError::UnsupportedAnnMetricPair);
    }
    Ok(())
}

pub(crate) fn validate_text(field: &str, value: &str) -> Result<(), SemanticIndexError> {
    if value.is_empty()
        || value.len() > MAX_IDENTITY_TEXT_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(SemanticIndexError::InvalidField {
            field: field.to_string(),
            reason: "must be non-empty, bounded, and preserve exact visible bytes".to_string(),
        });
    }
    Ok(())
}

pub(crate) fn validate_generation(generation: u64) -> Result<(), SemanticIndexError> {
    if generation == 0 {
        return Err(SemanticIndexError::InvalidField {
            field: "generation".to_string(),
            reason: "must start at one".to_string(),
        });
    }
    Ok(())
}

fn managed_vector_target_id(binding_id: &str) -> String {
    format!("semantic-vector-target:{binding_id}")
}

fn policy_identity_for_draft(
    draft: &SemanticBindingDraft,
) -> Result<SemanticPolicyIdentity, SemanticIndexError> {
    SemanticPolicyIdentity::create(
        draft.tenant_id.clone(),
        draft.effective_actor_scope.clone(),
        draft.purpose_id.clone(),
        draft.policy.clone(),
    )
}

fn binding_digest(
    draft: &SemanticBindingDraft,
    policy_identity: &SemanticPolicyIdentity,
    vector_target_id: &str,
) -> SemanticDigest {
    // Corrected constructible subject: the two pre-binding specs replace the
    // circular complete child identities in the superseded formula. Lifecycle
    // state is receipt-bound mutable data, not immutable binding identity.
    let subject = cbor::map(vec![
        ("binding_id", cbor::text(&draft.binding_id)),
        ("tenant_id", cbor::text(&draft.tenant_id)),
        ("actor_scope", cbor::text(&draft.actor_scope)),
        (
            "effective_actor_scope",
            cbor::text(&draft.effective_actor_scope),
        ),
        ("purpose_id", cbor::text(&draft.purpose_id)),
        ("policy_identity", policy_identity.canonical_cbor()),
        ("source_selector", draft.source_selector.canonical_cbor()),
        (
            "source_schema_digest",
            cbor::text(&draft.source_schema_digest),
        ),
        ("source_revision", cbor::text(&draft.source_revision)),
        (
            "source_field_set_digest",
            cbor::text(&draft.source_field_set_digest),
        ),
        ("vector_target_id", cbor::text(vector_target_id)),
        ("dimension", cbor::unsigned(draft.dimension.into())),
        ("metric", cbor::text(draft.metric.as_str())),
        ("model_id", cbor::text(&draft.model.model_id)),
        ("model_revision", cbor::text(&draft.model.model_revision)),
        (
            "preprocess_digest",
            cbor::text(&draft.model.preprocess_digest),
        ),
        ("model_digest", cbor::text(&draft.model.model_digest)),
        ("generation", cbor::unsigned(draft.generation)),
        (
            "maintenance_policy_id",
            cbor::text(&draft.maintenance_policy_id),
        ),
        ("queue_profile_id", cbor::text(SEMANTIC_QUEUE_PROFILE_ID)),
        ("lexical_index_spec", draft.lexical_index.canonical_cbor()),
        ("ann_index_spec", draft.ann_index.canonical_cbor()),
        ("created_at", cbor::text(&draft.created_at)),
    ]);
    domain_digest(SEMANTIC_BINDING_DIGEST_DOMAIN, subject)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticVector {
    pub binding_id: String,
    pub binding_digest: SemanticDigest,
    pub generation: u64,
    pub source_entity_id: String,
    pub source_revision: String,
    pub model_digest: String,
    pub preprocess_digest: String,
    pub dimension: u32,
    pub purpose_id: String,
    pub policy_digest: String,
    pub values: Vec<f32>,
    pub values_digest: SemanticDigest,
}

impl SemanticVector {
    pub fn create(
        binding: &SemanticBinding,
        source_entity_id: impl Into<String>,
        source_revision: impl Into<String>,
        values: Vec<f32>,
    ) -> Result<Self, SemanticIndexError> {
        binding.validate()?;
        let source_entity_id = source_entity_id.into();
        let source_revision = source_revision.into();
        validate_text("source_entity_id", &source_entity_id)?;
        validate_text("source_revision", &source_revision)?;
        if values.len() != binding.dimension as usize {
            return Err(SemanticIndexError::InvalidField {
                field: "values".to_string(),
                reason: format!(
                    "expected {} components but received {}",
                    binding.dimension,
                    values.len()
                ),
            });
        }
        let mut vector = Self {
            binding_id: binding.binding_id.clone(),
            binding_digest: binding.binding_digest,
            generation: binding.generation,
            source_entity_id,
            source_revision,
            model_digest: binding.model_digest.clone(),
            preprocess_digest: binding.preprocess_digest.clone(),
            dimension: binding.dimension,
            purpose_id: binding.purpose_id.clone(),
            policy_digest: binding.policy_digest.clone(),
            values,
            values_digest: SemanticDigest::from_bytes([0; 32]),
        };
        vector.values_digest = vector.compute_digest()?;
        Ok(vector)
    }

    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        for (field, value) in [
            ("binding_id", self.binding_id.as_str()),
            ("source_entity_id", self.source_entity_id.as_str()),
            ("source_revision", self.source_revision.as_str()),
            ("model_digest", self.model_digest.as_str()),
            ("preprocess_digest", self.preprocess_digest.as_str()),
            ("purpose_id", self.purpose_id.as_str()),
            ("policy_digest", self.policy_digest.as_str()),
        ] {
            validate_text(field, value)?;
        }
        if self.dimension == 0 || self.dimension as usize > MAX_MAINTAINED_ANN_DIMENSIONS {
            return Err(SemanticIndexError::InvalidField {
                field: "dimension".to_string(),
                reason: "outside maintained ANN dimension bounds".to_string(),
            });
        }
        if self.values.len() != self.dimension as usize {
            return Err(SemanticIndexError::InvalidField {
                field: "values".to_string(),
                reason: "vector width does not match dimension".to_string(),
            });
        }
        if self.values_digest != self.compute_digest()? {
            return Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_vector".to_string(),
            });
        }
        Ok(())
    }

    pub fn validate_against(&self, binding: &SemanticBinding) -> Result<(), SemanticIndexError> {
        self.validate()?;
        binding.validate()?;
        if self.binding_id != binding.binding_id
            || self.binding_digest != binding.binding_digest
            || self.generation != binding.generation
            || self.model_digest != binding.model_digest
            || self.preprocess_digest != binding.preprocess_digest
            || self.dimension != binding.dimension
            || self.purpose_id != binding.purpose_id
            || self.policy_digest != binding.policy_digest
        {
            return Err(SemanticIndexError::VectorBindingMismatch);
        }
        Ok(())
    }

    fn compute_digest(&self) -> Result<SemanticDigest, SemanticIndexError> {
        let values = self
            .values
            .iter()
            .copied()
            .map(cbor::finite_f32)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|reason| SemanticIndexError::InvalidField {
                field: "values".to_string(),
                reason,
            })?;
        let subject = cbor::map(vec![
            ("binding_id", cbor::text(&self.binding_id)),
            ("binding_digest", cbor::digest(self.binding_digest)),
            ("generation", cbor::unsigned(self.generation)),
            ("source_entity_id", cbor::text(&self.source_entity_id)),
            ("source_revision", cbor::text(&self.source_revision)),
            ("model_digest", cbor::text(&self.model_digest)),
            ("preprocess_digest", cbor::text(&self.preprocess_digest)),
            ("dimension", cbor::unsigned(self.dimension.into())),
            ("purpose_id", cbor::text(&self.purpose_id)),
            ("policy_digest", cbor::text(&self.policy_digest)),
            ("values", cbor::array(values)),
        ]);
        Ok(domain_digest(SEMANTIC_VECTOR_DIGEST_DOMAIN, subject))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::semantic_index::{SemanticIndexMutation, SqlColumnRef, SEMANTIC_SQL_CATALOG_ID};

    fn draft() -> SemanticBindingDraft {
        SemanticBindingDraft {
            binding_id: "binding:articles-body".into(),
            tenant_id: "tenant-a".into(),
            actor_scope: "semantic:index-maintainer".into(),
            effective_actor_scope: "semantic:agent:index-maintainer".into(),
            purpose_id: "retrieval".into(),
            policy: SemanticPolicyComponents {
                rbac_policy_revision: 1,
                rbac_policy_digest: "sha256:rbac".into(),
                row_policy_revision: 1,
                row_policy_digest: "sha256:row-policy".into(),
                source_acl_revision: 1,
                source_acl_digest: "sha256:source-acl".into(),
            },
            source_selector: SemanticSourceSelector::SqlColumnRef(SqlColumnRef {
                catalog_id: SEMANTIC_SQL_CATALOG_ID.into(),
                schema_id: "public".into(),
                table_id: "articles".into(),
                column_id: "body".into(),
            }),
            source_schema_digest: "sha256:schema".into(),
            source_revision: "42".into(),
            source_field_set_digest: "sha256:field-set".into(),
            dimension: 3,
            metric: SemanticVectorMetric::Cosine,
            model: SemanticModelIdentity {
                model_id: "embedding-model".into(),
                model_revision: "revision-1".into(),
                preprocess_digest: "sha256:preprocess".into(),
                model_digest: "sha256:model".into(),
            },
            generation: 1,
            maintenance_policy_id: "semantic-maintenance".into(),
            lexical_index: SemanticLexicalIndexSpec {
                analyzer_id: "standard".into(),
                analyzer_revision: "1".into(),
                analyzer_config_digest: "sha256:analyzer".into(),
            },
            ann_index: SemanticAnnIndexSpec {
                method: SemanticAnnIndexMethod::IvfPq,
                parameters_digest: "sha256:ann-parameters".into(),
            },
            created_at: "2026-09-04T00:00:00Z".into(),
        }
    }

    #[test]
    fn two_phase_identity_construction_is_deterministic() {
        let first = SemanticBinding::create(draft()).unwrap();
        let second = SemanticBinding::create(draft()).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first.binding_digest,
            first.lexical_index_identity.binding_digest
        );
        assert_eq!(
            first.binding_digest,
            first.ann_index_identity.binding_digest
        );
        assert_eq!(
            first.binding_digest.to_string(),
            "sha256:0628e8ee9143e80637119825b2276bb015987f65e9228c9b791c278c8087462d"
        );
        assert_eq!(
            first
                .lexical_index_identity
                .lexical_index_digest
                .to_string(),
            "sha256:235eb88137115d79afaba122366510b6cbb8c65f78aac94b6dbb61e0fe4fce56"
        );
        assert_eq!(
            first.ann_index_identity.ann_index_digest.to_string(),
            "sha256:6c89d305043519323591c25688b0c1ddd0dc3bcbb6c77b27d2da884abdb3dc45"
        );
        assert!(first.validate().is_ok());
    }

    #[test]
    fn ivf_pq_dot_product_is_not_a_representable_maintained_binding() {
        let mut unsupported = draft();
        unsupported.metric = SemanticVectorMetric::DotProduct;
        assert_eq!(
            SemanticBinding::create(unsupported),
            Err(SemanticIndexError::UnsupportedAnnMetricPair)
        );
    }

    #[test]
    fn child_identity_or_spec_drift_is_rejected() {
        let mut binding = SemanticBinding::create(draft()).unwrap();
        binding.lexical_index_identity.analyzer_id = "different".into();
        assert_eq!(
            binding.validate(),
            Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_binding".into(),
            })
        );

        let mut binding = SemanticBinding::create(draft()).unwrap();
        binding.ann_index_identity.ann_index_digest = SemanticDigest::from_bytes([7; 32]);
        assert_eq!(
            binding.validate(),
            Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_ann_index".into(),
            })
        );
    }

    #[test]
    fn every_caller_supplied_binding_subject_field_changes_the_digest() {
        let baseline = SemanticBinding::create(draft()).unwrap().binding_digest;
        let value = serde_json::to_value(draft()).unwrap();
        let mutations = [
            ("/binding_id", serde_json::json!("binding:different")),
            ("/tenant_id", serde_json::json!("tenant-b")),
            ("/actor_scope", serde_json::json!("different-origin")),
            (
                "/effective_actor_scope",
                serde_json::json!("different-effective"),
            ),
            ("/purpose_id", serde_json::json!("different-purpose")),
            (
                "/policy/rbac_policy_digest",
                serde_json::json!("different-policy"),
            ),
            (
                "/source_selector/selector/column_id",
                serde_json::json!("title"),
            ),
            (
                "/source_schema_digest",
                serde_json::json!("different-schema"),
            ),
            ("/source_revision", serde_json::json!("43")),
            (
                "/source_field_set_digest",
                serde_json::json!("different-field-set"),
            ),
            ("/dimension", serde_json::json!(4)),
            ("/metric", serde_json::json!("euclidean")),
            ("/model/model_id", serde_json::json!("different-model")),
            (
                "/model/model_revision",
                serde_json::json!("different-revision"),
            ),
            (
                "/model/preprocess_digest",
                serde_json::json!("different-preprocess"),
            ),
            (
                "/model/model_digest",
                serde_json::json!("different-model-digest"),
            ),
            ("/generation", serde_json::json!(2)),
            (
                "/maintenance_policy_id",
                serde_json::json!("different-maintenance"),
            ),
            (
                "/lexical_index/analyzer_id",
                serde_json::json!("different-analyzer"),
            ),
            ("/lexical_index/analyzer_revision", serde_json::json!("2")),
            (
                "/lexical_index/analyzer_config_digest",
                serde_json::json!("different-analyzer-config"),
            ),
            ("/ann_index/method", serde_json::json!("hnsw")),
            (
                "/ann_index/parameters_digest",
                serde_json::json!("different-ann-parameters"),
            ),
            ("/created_at", serde_json::json!("2026-09-04T00:00:01Z")),
        ];
        for (pointer, replacement) in mutations {
            let mut changed = value.clone();
            *changed.pointer_mut(pointer).unwrap() = replacement;
            let changed: SemanticBindingDraft = serde_json::from_value(changed).unwrap();
            assert_ne!(
                SemanticBinding::create(changed).unwrap().binding_digest,
                baseline
            );
        }
    }

    #[test]
    fn vector_digest_binds_every_value_and_rejects_non_finite_values() {
        let binding = SemanticBinding::create(draft()).unwrap();
        let mut vector =
            SemanticVector::create(&binding, "article:1", "7", vec![1.0, 0.5, 0.0]).unwrap();
        assert_eq!(
            vector.values_digest.to_string(),
            "sha256:791ab0d118a6f05b9b94825f0c96aa394796f6624bbd589ddb1e2dbaa21caa98"
        );
        assert!(vector.validate().is_ok());
        vector.values[1] = 0.25;
        assert!(matches!(
            vector.validate(),
            Err(SemanticIndexError::DigestMismatch { .. })
        ));
        assert!(
            SemanticVector::create(&binding, "article:1", "7", vec![1.0, f32::NAN, 0.0]).is_err()
        );

        let other = SemanticBinding::create(SemanticBindingDraft {
            binding_id: "binding:other".into(),
            ..draft()
        })
        .unwrap();
        let vector =
            SemanticVector::create(&binding, "article:1", "7", vec![1.0, 0.5, 0.0]).unwrap();
        assert_eq!(
            vector.validate_against(&other),
            Err(SemanticIndexError::VectorBindingMismatch)
        );
    }

    #[test]
    fn caller_cannot_supply_live_state_or_final_digests_in_a_draft() {
        let mut value = serde_json::to_value(draft()).unwrap();
        value["durable_state"] = serde_json::json!("live");
        value["binding_digest"] = serde_json::json!(format!("sha256:{}", "00".repeat(32)));
        value["vector_target_id"] = serde_json::json!("caller-owned-target");
        value["queue_profile_id"] = serde_json::json!("unbounded-queue");
        assert!(serde_json::from_value::<SemanticBindingDraft>(value).is_err());
    }

    #[test]
    fn store_binding_rejects_caller_set_live_state() {
        let mut binding = SemanticBinding::create(draft()).unwrap();
        binding.durable_state = SemanticBindingState::Live;
        let mutation = SemanticIndexMutation::StoreBinding {
            binding: Box::new(binding),
        };
        assert_eq!(
            mutation.validate(),
            Err(SemanticIndexError::CallerSetBindingState)
        );
    }

    #[test]
    fn lifecycle_transitions_preserve_identities_and_reject_replay_or_alteration() {
        let pending = SemanticBinding::create(draft()).unwrap();
        let pending_vector =
            SemanticVector::create(&pending, "article:1", "7", vec![1.0, 0.5, 0.0]).unwrap();
        let to_building = SemanticBindingStateTransition::create(
            &pending,
            SemanticBindingState::Building,
            "maintenance-started",
        )
        .unwrap();
        let building = pending.apply_state_transition(&to_building).unwrap();
        assert_eq!(building.binding_digest, pending.binding_digest);
        assert_eq!(
            building.lexical_index_identity,
            pending.lexical_index_identity
        );
        assert_eq!(building.ann_index_identity, pending.ann_index_identity);
        assert_eq!(
            SemanticVector::create(&building, "article:1", "7", vec![1.0, 0.5, 0.0])
                .unwrap()
                .values_digest,
            pending_vector.values_digest
        );
        assert!(matches!(
            building.apply_state_transition(&to_building),
            Err(SemanticIndexError::BindingStateMismatch { .. })
        ));

        let mut altered_reason = to_building.clone();
        altered_reason.reason = "different-reason".into();
        assert!(matches!(
            pending.apply_state_transition(&altered_reason),
            Err(SemanticIndexError::DigestMismatch { .. })
        ));

        let mut altered = to_building.clone();
        altered.next = SemanticBindingState::Live;
        assert!(matches!(
            pending.apply_state_transition(&altered),
            Err(SemanticIndexError::InvalidBindingStateTransition { .. })
        ));

        let to_live = SemanticBindingStateTransition::create(
            &building,
            SemanticBindingState::Live,
            "generation-reconciled",
        )
        .unwrap();
        let live = building.apply_state_transition(&to_live).unwrap();
        assert_eq!(live.binding_digest, pending.binding_digest);
        assert_eq!(live.ann_index_identity, pending.ann_index_identity);
    }
}
