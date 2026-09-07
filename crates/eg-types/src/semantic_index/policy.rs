use serde::{Deserialize, Serialize};

use super::digest::{cbor, domain_digest, SemanticDigest, SEMANTIC_POLICY_DIGEST_DOMAIN};
use super::identity::validate_text;
use super::state::SemanticIndexError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticPolicyComponents {
    pub rbac_policy_revision: u64,
    pub rbac_policy_digest: String,
    pub row_policy_revision: u64,
    pub row_policy_digest: String,
    pub source_acl_revision: u64,
    pub source_acl_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticPolicyIdentity {
    pub tenant_id: String,
    pub effective_actor_scope: String,
    pub purpose_id: String,
    pub components: SemanticPolicyComponents,
    pub policy_digest: String,
}

impl SemanticPolicyIdentity {
    pub fn create(
        tenant_id: impl Into<String>,
        effective_actor_scope: impl Into<String>,
        purpose_id: impl Into<String>,
        components: SemanticPolicyComponents,
    ) -> Result<Self, SemanticIndexError> {
        let mut identity = Self {
            tenant_id: tenant_id.into(),
            effective_actor_scope: effective_actor_scope.into(),
            purpose_id: purpose_id.into(),
            components,
            policy_digest: String::new(),
        };
        identity.policy_digest = identity.compute_digest().to_string();
        identity.validate()?;
        Ok(identity)
    }

    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        for (field, value) in [
            ("tenant_id", self.tenant_id.as_str()),
            ("effective_actor_scope", self.effective_actor_scope.as_str()),
            ("purpose_id", self.purpose_id.as_str()),
            (
                "rbac_policy_digest",
                self.components.rbac_policy_digest.as_str(),
            ),
            (
                "row_policy_digest",
                self.components.row_policy_digest.as_str(),
            ),
            (
                "source_acl_digest",
                self.components.source_acl_digest.as_str(),
            ),
        ] {
            validate_text(field, value)?;
        }
        if self.components.rbac_policy_revision == 0
            || self.components.row_policy_revision == 0
            || self.components.source_acl_revision == 0
        {
            return Err(SemanticIndexError::InvalidField {
                field: "semantic_policy_revision".to_string(),
                reason: "all policy component revisions must start at one".to_string(),
            });
        }
        if self.policy_digest != self.compute_digest().to_string() {
            return Err(SemanticIndexError::DigestMismatch {
                subject: "semantic_policy".to_string(),
            });
        }
        Ok(())
    }

    pub(crate) fn canonical_cbor(&self) -> Vec<u8> {
        cbor::map([
            ("tenant_id", cbor::text(&self.tenant_id)),
            (
                "effective_actor_scope",
                cbor::text(&self.effective_actor_scope),
            ),
            ("purpose_id", cbor::text(&self.purpose_id)),
            (
                "rbac_policy_revision",
                cbor::unsigned(self.components.rbac_policy_revision),
            ),
            (
                "rbac_policy_digest",
                cbor::text(&self.components.rbac_policy_digest),
            ),
            (
                "row_policy_revision",
                cbor::unsigned(self.components.row_policy_revision),
            ),
            (
                "row_policy_digest",
                cbor::text(&self.components.row_policy_digest),
            ),
            (
                "source_acl_revision",
                cbor::unsigned(self.components.source_acl_revision),
            ),
            (
                "source_acl_digest",
                cbor::text(&self.components.source_acl_digest),
            ),
        ])
    }

    fn compute_digest(&self) -> SemanticDigest {
        domain_digest(SEMANTIC_POLICY_DIGEST_DOMAIN, self.canonical_cbor())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn components() -> SemanticPolicyComponents {
        SemanticPolicyComponents {
            rbac_policy_revision: 1,
            rbac_policy_digest: "sha256:rbac".to_string(),
            row_policy_revision: 1,
            row_policy_digest: "sha256:row-policy".to_string(),
            source_acl_revision: 1,
            source_acl_digest: "sha256:source-acl".to_string(),
        }
    }

    #[test]
    fn composite_policy_is_derived_and_every_authority_component_is_bound() {
        let baseline = SemanticPolicyIdentity::create(
            "tenant-a",
            "semantic:agent:index-maintainer",
            "retrieval",
            components(),
        )
        .unwrap();
        assert_eq!(
            baseline.policy_digest,
            "sha256:3ad17e87cf7993a47ad2b1442cbecdef0e1b82f44c2a49fba89c71dd77c42d2c"
        );
        let mut changed = components();
        changed.source_acl_revision += 1;
        assert_ne!(
            baseline.policy_digest,
            SemanticPolicyIdentity::create(
                "tenant-a",
                "semantic:agent:index-maintainer",
                "retrieval",
                changed,
            )
            .unwrap()
            .policy_digest
        );
        let mut tampered = baseline;
        tampered.components.row_policy_digest = "sha256:different".to_string();
        assert!(matches!(
            tampered.validate(),
            Err(SemanticIndexError::DigestMismatch { .. })
        ));
    }
}
