use serde::{Deserialize, Serialize};

use super::digest::{cbor, domain_digest, SemanticDigest, SEMANTIC_APPROVAL_DIGEST_DOMAIN};
use super::identity::{
    validate_generation, validate_text, SemanticBinding, SemanticBindingDraft, SemanticVector,
};
use super::policy::SemanticPolicyIdentity;
use super::state::SemanticIndexError;

pub const SEMANTIC_INDEX_REQUEST_SCHEMA: &str = "semantic-index-request/v1";
pub const SEMANTIC_INDEX_RESPONSE_SCHEMA: &str = "semantic-index-response/v1";
pub const SEMANTIC_INDEX_FILTER_SCHEMA: &str = "semantic-index-filter/v1";
pub const SEMANTIC_INDEX_ERROR_SCHEMA: &str = "semantic-index-error/v1";
pub const SEMANTIC_INDEX_APPROVAL_SCHEMA: &str = "semantic-index-approval/v1";
pub const SEMANTIC_INDEX_STATUS_SCHEMA: &str = "semantic-index-status/v1";
pub const SEMANTIC_FILTER_MAX_ENTITY_IDS: usize = 4_096;
pub const SEMANTIC_FILTER_MAX_RESULTS: u32 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum SemanticIndexOperation {
    #[serde(rename = "semantic_binding_create")]
    CreateBinding,
    #[serde(rename = "semantic_binding_get")]
    GetBinding,
    #[serde(rename = "semantic_binding_list")]
    ListBindings,
    #[serde(rename = "semantic_binding_refresh")]
    RefreshBinding,
    #[serde(rename = "semantic_binding_disable")]
    DisableBinding,
    #[serde(rename = "semantic_binding_drop")]
    DropBinding,
    #[serde(rename = "semantic_search")]
    Search,
    #[serde(rename = "semantic_queue_status")]
    QueueStatus,
}

impl SemanticIndexOperation {
    pub const ALL: [Self; 8] = [
        Self::CreateBinding,
        Self::GetBinding,
        Self::ListBindings,
        Self::RefreshBinding,
        Self::DisableBinding,
        Self::DropBinding,
        Self::Search,
        Self::QueueStatus,
    ];

    pub fn as_str(self) -> &'static str {
        const IDS: [&str; 8] = [
            "semantic_binding_create",
            "semantic_binding_get",
            "semantic_binding_list",
            "semantic_binding_refresh",
            "semantic_binding_disable",
            "semantic_binding_drop",
            "semantic_search",
            "semantic_queue_status",
        ];
        IDS[self as usize]
    }

    pub fn requires_approval(self) -> bool {
        matches!(
            self,
            Self::CreateBinding | Self::RefreshBinding | Self::DisableBinding | Self::DropBinding
        )
    }
}

/// Approval identity supplied by the AU application service. EG verifies this
/// bond; it does not infer approval from a caller role or transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticIndexApproval {
    pub approval_id: String,
    pub approval_digest: SemanticDigest,
    pub operation: SemanticIndexOperation,
    pub binding_id: String,
    pub tenant_id: String,
    pub actor_scope: String,
    pub effective_actor_scope: String,
    pub purpose_id: String,
    pub policy_digest: String,
    pub approved_at_ms: u64,
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticIndexApprovalDraft {
    pub approval_id: String,
    pub operation: SemanticIndexOperation,
    pub binding_id: String,
    pub tenant_id: String,
    pub actor_scope: String,
    pub effective_actor_scope: String,
    pub purpose_id: String,
    pub policy_digest: String,
    pub approved_at_ms: u64,
    pub expires_at_ms: u64,
}

impl SemanticIndexApproval {
    pub fn create(draft: SemanticIndexApprovalDraft) -> Result<Self, SemanticIndexError> {
        validate_approval_text([
            ("approval_id", draft.approval_id.as_str()),
            ("binding_id", draft.binding_id.as_str()),
            ("tenant_id", draft.tenant_id.as_str()),
            ("actor_scope", draft.actor_scope.as_str()),
            (
                "effective_actor_scope",
                draft.effective_actor_scope.as_str(),
            ),
            ("purpose_id", draft.purpose_id.as_str()),
            ("policy_digest", draft.policy_digest.as_str()),
        ])?;
        validate_approval_window(draft.approved_at_ms, draft.expires_at_ms)?;
        let approval_digest = approval_digest(&draft);
        Ok(Self {
            approval_id: draft.approval_id,
            approval_digest,
            operation: draft.operation,
            binding_id: draft.binding_id,
            tenant_id: draft.tenant_id,
            actor_scope: draft.actor_scope,
            effective_actor_scope: draft.effective_actor_scope,
            purpose_id: draft.purpose_id,
            policy_digest: draft.policy_digest,
            approved_at_ms: draft.approved_at_ms,
            expires_at_ms: draft.expires_at_ms,
        })
    }

    fn validate_at(&self, now_ms: u64) -> Result<(), SemanticIndexError> {
        validate_approval_text([
            ("approval_id", self.approval_id.as_str()),
            ("binding_id", self.binding_id.as_str()),
            ("tenant_id", self.tenant_id.as_str()),
            ("actor_scope", self.actor_scope.as_str()),
            ("effective_actor_scope", self.effective_actor_scope.as_str()),
            ("purpose_id", self.purpose_id.as_str()),
            ("policy_digest", self.policy_digest.as_str()),
        ])?;
        validate_approval_window(self.approved_at_ms, self.expires_at_ms)?;
        if now_ms < self.approved_at_ms || now_ms >= self.expires_at_ms {
            return Err(SemanticIndexError::ApprovalExpired);
        }
        let draft = SemanticIndexApprovalDraft {
            approval_id: self.approval_id.clone(),
            operation: self.operation,
            binding_id: self.binding_id.clone(),
            tenant_id: self.tenant_id.clone(),
            actor_scope: self.actor_scope.clone(),
            effective_actor_scope: self.effective_actor_scope.clone(),
            purpose_id: self.purpose_id.clone(),
            policy_digest: self.policy_digest.clone(),
            approved_at_ms: self.approved_at_ms,
            expires_at_ms: self.expires_at_ms,
        };
        if self.approval_digest != approval_digest(&draft) {
            return Err(SemanticIndexError::ApprovalDigestMismatch);
        }
        Ok(())
    }
}

fn validate_approval_text<'a>(
    fields: impl IntoIterator<Item = (&'static str, &'a str)>,
) -> Result<(), SemanticIndexError> {
    for (field, value) in fields {
        validate_text(field, value)?;
    }
    Ok(())
}

fn validate_approval_window(
    approved_at_ms: u64,
    expires_at_ms: u64,
) -> Result<(), SemanticIndexError> {
    if approved_at_ms >= expires_at_ms {
        return Err(SemanticIndexError::InvalidField {
            field: "approval_window".to_string(),
            reason: "expiry must be strictly after approval".to_string(),
        });
    }
    Ok(())
}

fn approval_digest(draft: &SemanticIndexApprovalDraft) -> SemanticDigest {
    let subject = cbor::map([
        ("approval_id", cbor::text(&draft.approval_id)),
        ("operation", cbor::text(draft.operation.as_str())),
        ("binding_id", cbor::text(&draft.binding_id)),
        ("tenant_id", cbor::text(&draft.tenant_id)),
        ("actor_scope", cbor::text(&draft.actor_scope)),
        (
            "effective_actor_scope",
            cbor::text(&draft.effective_actor_scope),
        ),
        ("purpose_id", cbor::text(&draft.purpose_id)),
        ("policy_digest", cbor::text(&draft.policy_digest)),
        ("approved_at_ms", cbor::unsigned(draft.approved_at_ms)),
        ("expires_at_ms", cbor::unsigned(draft.expires_at_ms)),
    ]);
    domain_digest(SEMANTIC_APPROVAL_DIGEST_DOMAIN, subject)
}

/// Closed relational/identity filter. Raw predicates and SQL expressions have
/// no representable field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticIndexFilter {
    pub source_entity_ids: Vec<String>,
    pub required_source_revision: Option<String>,
    pub max_results: u32,
}

impl SemanticIndexFilter {
    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        if self.max_results == 0 || self.max_results > SEMANTIC_FILTER_MAX_RESULTS {
            return Err(SemanticIndexError::InvalidField {
                field: "max_results".to_string(),
                reason: format!("must be within 1..={SEMANTIC_FILTER_MAX_RESULTS}"),
            });
        }
        if self.source_entity_ids.len() > SEMANTIC_FILTER_MAX_ENTITY_IDS {
            return Err(SemanticIndexError::InvalidField {
                field: "source_entity_ids".to_string(),
                reason: format!("must contain at most {SEMANTIC_FILTER_MAX_ENTITY_IDS} values"),
            });
        }
        let mut canonical = std::collections::BTreeSet::new();
        for source_entity_id in &self.source_entity_ids {
            validate_text("source_entity_id", source_entity_id)?;
            if !canonical.insert(source_entity_id) {
                return Err(SemanticIndexError::DuplicateSourceEntity);
            }
        }
        if let Some(source_revision) = &self.required_source_revision {
            validate_text("required_source_revision", source_revision)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "probe", rename_all = "snake_case", deny_unknown_fields)]
pub enum SemanticSearchProbe {
    CanonicalText {
        text: String,
        model_digest: String,
        preprocess_digest: String,
    },
    Vector {
        vector: Box<SemanticVector>,
    },
}

impl SemanticSearchProbe {
    fn validate(&self) -> Result<(), SemanticIndexError> {
        match self {
            Self::CanonicalText {
                text,
                model_digest,
                preprocess_digest,
            } => {
                validate_text("canonical_text", text)?;
                validate_text("model_digest", model_digest)?;
                validate_text("preprocess_digest", preprocess_digest)
            }
            Self::Vector { vector } => vector.validate(),
        }
    }

    fn validate_against_binding(
        &self,
        binding: &SemanticBinding,
    ) -> Result<(), SemanticIndexError> {
        self.validate()?;
        match self {
            Self::CanonicalText {
                model_digest,
                preprocess_digest,
                ..
            } if model_digest != &binding.model_digest
                || preprocess_digest != &binding.preprocess_digest =>
            {
                Err(SemanticIndexError::SearchProbeModelMismatch)
            }
            Self::Vector { vector } => vector.validate_against(binding),
            Self::CanonicalText { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", deny_unknown_fields)]
pub enum SemanticIndexCommand {
    #[serde(rename = "semantic_binding_create")]
    CreateBinding { draft: Box<SemanticBindingDraft> },
    #[serde(rename = "semantic_binding_get")]
    GetBinding { binding_id: String },
    #[serde(rename = "semantic_binding_list")]
    ListBindings {
        filter: SemanticIndexFilter,
        cursor: Option<String>,
    },
    #[serde(rename = "semantic_binding_refresh")]
    RefreshBinding {
        binding_id: String,
        expected_generation: u64,
    },
    #[serde(rename = "semantic_binding_disable")]
    DisableBinding {
        binding_id: String,
        expected_generation: u64,
    },
    #[serde(rename = "semantic_binding_drop")]
    DropBinding {
        binding_id: String,
        expected_generation: u64,
    },
    #[serde(rename = "semantic_search")]
    Search {
        binding_id: String,
        probe: SemanticSearchProbe,
        filter: SemanticIndexFilter,
    },
    #[serde(rename = "semantic_queue_status")]
    QueueStatus { binding_id: Option<String> },
}

impl SemanticIndexCommand {
    pub fn operation(&self) -> SemanticIndexOperation {
        let index = match self {
            Self::CreateBinding { .. } => 0,
            Self::GetBinding { .. } => 1,
            Self::ListBindings { .. } => 2,
            Self::RefreshBinding { .. } => 3,
            Self::DisableBinding { .. } => 4,
            Self::DropBinding { .. } => 5,
            Self::Search { .. } => 6,
            Self::QueueStatus { .. } => 7,
        };
        SemanticIndexOperation::ALL[index]
    }

    fn binding_id(&self) -> Option<&str> {
        match self {
            Self::CreateBinding { draft } => Some(&draft.binding_id),
            Self::GetBinding { binding_id }
            | Self::RefreshBinding { binding_id, .. }
            | Self::DisableBinding { binding_id, .. }
            | Self::DropBinding { binding_id, .. }
            | Self::Search { binding_id, .. } => Some(binding_id),
            Self::ListBindings { .. } | Self::QueueStatus { binding_id: None } => None,
            Self::QueueStatus {
                binding_id: Some(binding_id),
            } => Some(binding_id),
        }
    }

    fn validate(&self) -> Result<(), SemanticIndexError> {
        match self {
            Self::CreateBinding { draft } => SemanticBinding::create((**draft).clone()).map(|_| ()),
            Self::GetBinding { binding_id } => validate_text("binding_id", binding_id),
            Self::ListBindings { filter, cursor } => {
                filter.validate()?;
                validate_optional_text("list_cursor", cursor)
            }
            Self::RefreshBinding {
                binding_id,
                expected_generation,
            }
            | Self::DisableBinding {
                binding_id,
                expected_generation,
            }
            | Self::DropBinding {
                binding_id,
                expected_generation,
            } => {
                validate_text("binding_id", binding_id)?;
                validate_generation(*expected_generation)
            }
            Self::Search {
                binding_id,
                probe,
                filter,
            } => {
                validate_text("binding_id", binding_id)?;
                probe.validate()?;
                filter.validate()
            }
            Self::QueueStatus { binding_id } => validate_optional_text("binding_id", binding_id),
        }
    }
}

fn validate_optional_text(field: &str, value: &Option<String>) -> Result<(), SemanticIndexError> {
    if let Some(value) = value {
        validate_text(field, value)?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticIndexRequest {
    pub request_id: String,
    pub tenant_id: String,
    /// Originating principal scope; this is not the delegated effective actor.
    pub actor_scope: String,
    /// Opaque scope derived by the server from the verified delegated agent.
    /// The later handler must compare it with its non-serializable guard.
    pub effective_actor_scope: String,
    pub purpose_id: String,
    pub policy_identity: SemanticPolicyIdentity,
    pub command: SemanticIndexCommand,
    pub approval: Option<SemanticIndexApproval>,
}

impl SemanticIndexRequest {
    /// Validate DTO consistency only. A successful result does not authorize
    /// the request; the future handler must bind these claims to its private,
    /// transport-derived authorization guard before executing any operation.
    pub fn validate_at(&self, now_ms: u64) -> Result<(), SemanticIndexError> {
        self.validate_context()?;
        self.validate_approval_at(now_ms)?;
        self.command.validate()?;
        if let SemanticIndexCommand::CreateBinding { draft } = &self.command {
            if self.actor_scope != draft.actor_scope
                || self.policy_identity
                    != SemanticPolicyIdentity::create(
                        draft.tenant_id.clone(),
                        draft.effective_actor_scope.clone(),
                        draft.purpose_id.clone(),
                        draft.policy.clone(),
                    )?
            {
                return Err(SemanticIndexError::AuthorizationContextMismatch);
            }
        }
        Ok(())
    }

    fn validate_context(&self) -> Result<(), SemanticIndexError> {
        for (field, value) in [
            ("request_id", self.request_id.as_str()),
            ("tenant_id", self.tenant_id.as_str()),
            ("actor_scope", self.actor_scope.as_str()),
            ("effective_actor_scope", self.effective_actor_scope.as_str()),
            ("purpose_id", self.purpose_id.as_str()),
        ] {
            validate_text(field, value)?;
        }
        self.policy_identity.validate()?;
        if self.tenant_id != self.policy_identity.tenant_id
            || self.effective_actor_scope != self.policy_identity.effective_actor_scope
            || self.purpose_id != self.policy_identity.purpose_id
        {
            return Err(SemanticIndexError::AuthorizationContextMismatch);
        }
        Ok(())
    }

    pub fn validate_against_binding(
        &self,
        binding: &SemanticBinding,
        now_ms: u64,
    ) -> Result<(), SemanticIndexError> {
        self.validate_at(now_ms)?;
        if self.command.binding_id() != Some(binding.binding_id.as_str())
            || self.tenant_id != binding.tenant_id
            || self.actor_scope != binding.actor_scope
            || self.effective_actor_scope != binding.effective_actor_scope
            || self.purpose_id != binding.purpose_id
            || self.policy_identity != binding.policy_identity
        {
            return Err(SemanticIndexError::AuthorizationContextMismatch);
        }
        if let SemanticIndexCommand::Search { probe, .. } = &self.command {
            probe.validate_against_binding(binding)?;
        }
        Ok(())
    }

    fn validate_approval_at(&self, now_ms: u64) -> Result<(), SemanticIndexError> {
        let operation = self.command.operation();
        if !operation.requires_approval() {
            if self.approval.is_some() {
                return Err(SemanticIndexError::ApprovalOperationMismatch);
            }
            return Ok(());
        }
        let approval = self
            .approval
            .as_ref()
            .ok_or(SemanticIndexError::ApprovalRequired)?;
        approval.validate_at(now_ms)?;
        if approval.operation != operation
            || self.command.binding_id() != Some(approval.binding_id.as_str())
        {
            return Err(SemanticIndexError::ApprovalOperationMismatch);
        }
        if approval.tenant_id != self.tenant_id
            || approval.actor_scope != self.actor_scope
            || approval.effective_actor_scope != self.effective_actor_scope
            || approval.purpose_id != self.purpose_id
            || approval.policy_digest != self.policy_identity.policy_digest
        {
            return Err(SemanticIndexError::AuthorizationContextMismatch);
        }
        Ok(())
    }

    pub fn require_implemented_selector(&self) -> Result<(), SemanticIndexError> {
        if let SemanticIndexCommand::CreateBinding { draft } = &self.command {
            draft.source_selector.require_implemented()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::semantic_index::{
        SemanticAnnIndexMethod, SemanticAnnIndexSpec, SemanticLexicalIndexSpec,
        SemanticModelIdentity, SemanticPolicyComponents, SemanticSelectorKind,
        SemanticSourceSelector, SemanticVectorMetric, SqlColumnRef, SEMANTIC_SQL_CATALOG_ID,
    };

    fn binding(binding_id: &str, model_digest: &str) -> SemanticBinding {
        SemanticBinding::create(SemanticBindingDraft {
            binding_id: binding_id.into(),
            tenant_id: "tenant-a".into(),
            actor_scope: "maintainer".into(),
            effective_actor_scope: "agent:index-maintainer".into(),
            purpose_id: "retrieval".into(),
            policy: SemanticPolicyComponents {
                rbac_policy_revision: 1,
                rbac_policy_digest: "rbac-a".into(),
                row_policy_revision: 1,
                row_policy_digest: "row-a".into(),
                source_acl_revision: 1,
                source_acl_digest: "acl-a".into(),
            },
            source_selector: SemanticSourceSelector::SqlColumnRef(SqlColumnRef {
                catalog_id: SEMANTIC_SQL_CATALOG_ID.into(),
                schema_id: "public".into(),
                table_id: "articles".into(),
                column_id: "body".into(),
            }),
            source_schema_digest: "schema-a".into(),
            source_revision: "1".into(),
            source_field_set_digest: "fields-a".into(),
            dimension: 2,
            metric: SemanticVectorMetric::Cosine,
            model: SemanticModelIdentity {
                model_id: "model-a".into(),
                model_revision: "1".into(),
                preprocess_digest: "preprocess-a".into(),
                model_digest: model_digest.into(),
            },
            generation: 1,
            maintenance_policy_id: "maintenance-a".into(),
            lexical_index: SemanticLexicalIndexSpec {
                analyzer_id: "standard".into(),
                analyzer_revision: "1".into(),
                analyzer_config_digest: "analyzer-a".into(),
            },
            ann_index: SemanticAnnIndexSpec {
                method: SemanticAnnIndexMethod::Hnsw,
                parameters_digest: "ann-a".into(),
            },
            created_at: "2026-09-04T00:00:00Z".into(),
        })
        .unwrap()
    }

    #[test]
    fn operation_registry_is_closed_and_exact() {
        let ids: Vec<_> = SemanticIndexOperation::ALL
            .into_iter()
            .map(SemanticIndexOperation::as_str)
            .collect();
        assert_eq!(
            ids,
            [
                "semantic_binding_create",
                "semantic_binding_get",
                "semantic_binding_list",
                "semantic_binding_refresh",
                "semantic_binding_disable",
                "semantic_binding_drop",
                "semantic_search",
                "semantic_queue_status",
            ]
        );
        for operation in SemanticIndexOperation::ALL {
            assert_eq!(serde_json::to_value(operation).unwrap(), operation.as_str());
        }
        let encoded = serde_json::to_value(SemanticIndexCommand::GetBinding {
            binding_id: "binding-a".into(),
        })
        .unwrap();
        assert_eq!(encoded["operation"], "semantic_binding_get");
    }

    #[test]
    fn filters_reject_raw_predicate_fields() {
        let value = serde_json::json!({
            "source_entity_ids": [],
            "required_source_revision": null,
            "max_results": 10,
            "raw_predicate": "tenant_id = '*'"
        });
        assert!(serde_json::from_value::<SemanticIndexFilter>(value).is_err());
    }

    #[test]
    fn filters_are_bounded_and_reject_duplicate_identities() {
        let valid = SemanticIndexFilter {
            source_entity_ids: vec!["article:1".into(), "article:2".into()],
            required_source_revision: Some("42".into()),
            max_results: SEMANTIC_FILTER_MAX_RESULTS,
        };
        assert!(valid.validate().is_ok());

        let mut duplicate = valid.clone();
        duplicate.source_entity_ids[1] = duplicate.source_entity_ids[0].clone();
        assert_eq!(
            duplicate.validate(),
            Err(SemanticIndexError::DuplicateSourceEntity)
        );

        let mut too_many = valid;
        too_many.max_results = SEMANTIC_FILTER_MAX_RESULTS + 1;
        assert!(matches!(
            too_many.validate(),
            Err(SemanticIndexError::InvalidField { .. })
        ));
    }

    #[test]
    fn request_commands_and_approvals_reject_unbounded_or_zero_identity_fields() {
        assert!(SemanticIndexCommand::GetBinding {
            binding_id: String::new(),
        }
        .validate()
        .is_err());
        assert!(SemanticIndexCommand::RefreshBinding {
            binding_id: "binding-a".into(),
            expected_generation: 0,
        }
        .validate()
        .is_err());
        assert!(SemanticIndexApproval::create(SemanticIndexApprovalDraft {
            approval_id: String::new(),
            operation: SemanticIndexOperation::DisableBinding,
            binding_id: "binding-a".into(),
            tenant_id: "tenant-a".into(),
            actor_scope: "maintainer".into(),
            effective_actor_scope: "agent:index-maintainer".into(),
            purpose_id: "retrieval".into(),
            policy_digest: "sha256:policy".into(),
            approved_at_ms: 1,
            expires_at_ms: 2,
        })
        .is_err());
    }

    #[test]
    fn every_operation_binds_policy_context_and_rejects_extraneous_approval() {
        let binding = binding("binding-a", "model-a");
        let filter = SemanticIndexFilter {
            source_entity_ids: Vec::new(),
            required_source_revision: None,
            max_results: 10,
        };
        let request = SemanticIndexRequest {
            request_id: "request-a".into(),
            tenant_id: "different-tenant".into(),
            actor_scope: binding.actor_scope.clone(),
            effective_actor_scope: binding.effective_actor_scope.clone(),
            purpose_id: binding.purpose_id.clone(),
            policy_identity: binding.policy_identity.clone(),
            command: SemanticIndexCommand::ListBindings {
                filter,
                cursor: None,
            },
            approval: None,
        };
        assert_eq!(
            request.validate_at(10),
            Err(SemanticIndexError::AuthorizationContextMismatch)
        );

        let approval = SemanticIndexApproval::create(SemanticIndexApprovalDraft {
            approval_id: "approval-a".into(),
            operation: SemanticIndexOperation::DisableBinding,
            binding_id: binding.binding_id.clone(),
            tenant_id: binding.tenant_id.clone(),
            actor_scope: binding.actor_scope.clone(),
            effective_actor_scope: binding.effective_actor_scope.clone(),
            purpose_id: binding.purpose_id.clone(),
            policy_digest: binding.policy_digest.clone(),
            approved_at_ms: 1,
            expires_at_ms: 100,
        })
        .unwrap();
        let mut request = SemanticIndexRequest {
            request_id: "request-b".into(),
            tenant_id: binding.tenant_id.clone(),
            actor_scope: binding.actor_scope.clone(),
            effective_actor_scope: binding.effective_actor_scope.clone(),
            purpose_id: "different-purpose".into(),
            policy_identity: binding.policy_identity.clone(),
            command: SemanticIndexCommand::QueueStatus { binding_id: None },
            approval: None,
        };
        assert_eq!(
            request.validate_at(10),
            Err(SemanticIndexError::AuthorizationContextMismatch)
        );
        request.purpose_id = binding.purpose_id;
        request.approval = Some(approval);
        assert_eq!(
            request.validate_at(10),
            Err(SemanticIndexError::ApprovalOperationMismatch)
        );
    }

    #[test]
    fn canonical_text_probe_must_match_generation_model_and_preprocess() {
        let binding = binding("binding-a", "model-a");
        let filter = SemanticIndexFilter {
            source_entity_ids: Vec::new(),
            required_source_revision: None,
            max_results: 10,
        };
        let request = |model_digest: &str, preprocess_digest: &str| SemanticIndexRequest {
            request_id: "request-a".into(),
            tenant_id: binding.tenant_id.clone(),
            actor_scope: binding.actor_scope.clone(),
            effective_actor_scope: binding.effective_actor_scope.clone(),
            purpose_id: binding.purpose_id.clone(),
            policy_identity: binding.policy_identity.clone(),
            command: SemanticIndexCommand::Search {
                binding_id: binding.binding_id.clone(),
                probe: SemanticSearchProbe::CanonicalText {
                    text: "canonical query".into(),
                    model_digest: model_digest.into(),
                    preprocess_digest: preprocess_digest.into(),
                },
                filter: filter.clone(),
            },
            approval: None,
        };
        assert!(request("model-a", "preprocess-a")
            .validate_against_binding(&binding, 10)
            .is_ok());
        assert_eq!(
            request("model-b", "preprocess-a").validate_against_binding(&binding, 10),
            Err(SemanticIndexError::SearchProbeModelMismatch)
        );
    }

    #[test]
    fn mutating_commands_require_exact_operation_approval() {
        let policy_identity = binding("binding-a", "model-a").policy_identity;
        let command = SemanticIndexCommand::DisableBinding {
            binding_id: "binding-a".into(),
            expected_generation: 4,
        };
        let mut request = SemanticIndexRequest {
            request_id: "request-a".into(),
            tenant_id: "tenant-a".into(),
            actor_scope: "maintainer".into(),
            effective_actor_scope: "agent:index-maintainer".into(),
            purpose_id: "retrieval".into(),
            policy_identity: policy_identity.clone(),
            command,
            approval: None,
        };
        assert_eq!(
            request.validate_at(10),
            Err(SemanticIndexError::ApprovalRequired)
        );
        request.approval = Some(
            SemanticIndexApproval::create(SemanticIndexApprovalDraft {
                approval_id: "approval-a".into(),
                operation: SemanticIndexOperation::RefreshBinding,
                binding_id: "binding-a".into(),
                tenant_id: "tenant-a".into(),
                actor_scope: "maintainer".into(),
                effective_actor_scope: "agent:index-maintainer".into(),
                purpose_id: "retrieval".into(),
                policy_digest: policy_identity.policy_digest.clone(),
                approved_at_ms: 1,
                expires_at_ms: 100,
            })
            .unwrap(),
        );
        assert_eq!(
            request.validate_at(10),
            Err(SemanticIndexError::ApprovalOperationMismatch)
        );

        request.approval.as_mut().unwrap().operation = SemanticIndexOperation::DisableBinding;
        assert_eq!(
            request.validate_at(10),
            Err(SemanticIndexError::ApprovalDigestMismatch)
        );
        request.approval = Some(
            SemanticIndexApproval::create(SemanticIndexApprovalDraft {
                approval_id: "approval-a".into(),
                operation: SemanticIndexOperation::DisableBinding,
                binding_id: "binding-a".into(),
                tenant_id: "tenant-a".into(),
                actor_scope: "maintainer".into(),
                effective_actor_scope: "agent:index-maintainer".into(),
                purpose_id: "retrieval".into(),
                policy_digest: policy_identity.policy_digest.clone(),
                approved_at_ms: 1,
                expires_at_ms: 100,
            })
            .unwrap(),
        );
        assert_eq!(
            request.validate_at(100),
            Err(SemanticIndexError::ApprovalExpired)
        );
    }

    #[test]
    fn declared_selector_error_is_typed() {
        let error = SemanticIndexError::UnsupportedSelector {
            selector: SemanticSelectorKind::CanonicalTextAsset,
        };
        assert!(serde_json::to_string(&error)
            .unwrap()
            .contains("unsupported_selector"));
    }

    #[test]
    fn vector_probe_is_checked_against_the_requested_binding() {
        let primary = binding("binding-a", "model-digest-a");
        let other = binding("binding-b", "model-digest-b");
        let vector = SemanticVector::create(&other, "article:1", "1", vec![1.0, 0.0]).unwrap();
        let request = SemanticIndexRequest {
            request_id: "request-a".into(),
            tenant_id: primary.tenant_id.clone(),
            actor_scope: primary.actor_scope.clone(),
            effective_actor_scope: primary.effective_actor_scope.clone(),
            purpose_id: primary.purpose_id.clone(),
            policy_identity: primary.policy_identity.clone(),
            command: SemanticIndexCommand::Search {
                binding_id: primary.binding_id.clone(),
                probe: SemanticSearchProbe::Vector {
                    vector: Box::new(vector),
                },
                filter: SemanticIndexFilter {
                    source_entity_ids: Vec::new(),
                    required_source_revision: None,
                    max_results: 10,
                },
            },
            approval: None,
        };
        assert_eq!(
            request.validate_against_binding(&primary, 10),
            Err(SemanticIndexError::VectorBindingMismatch)
        );

        let mut unmanaged = primary.clone();
        unmanaged.vector_target_id = "caller-owned-target".into();
        assert_eq!(
            unmanaged.validate(),
            Err(SemanticIndexError::UnmanagedBindingResource)
        );
        unmanaged = primary;
        unmanaged.queue_profile_id = "unbounded-queue".into();
        assert_eq!(
            unmanaged.validate(),
            Err(SemanticIndexError::UnmanagedBindingResource)
        );
    }
}
