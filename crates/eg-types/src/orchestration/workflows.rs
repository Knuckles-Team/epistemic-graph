//! Workflow registration identity and registry model (EG-DECISION-ENGINE-R166.1).
//!
//! This is the serializable type-level registry state. Engine initialization and
//! served invocation are owned by the runtime layer.

use serde::{Deserialize, Serialize};

/// Identity of one immutable workflow definition.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WorkflowIdentity {
    pub workflow_id: String,
    pub version: String,
    /// Canonical `sha256:` digest of the workflow content.
    pub content_digest: String,
}

impl WorkflowIdentity {
    pub fn validate(&self) -> Result<(), WorkflowRegistryError> {
        if self.workflow_id.trim().is_empty() {
            return Err(WorkflowRegistryError::WorkflowIdMissing);
        }
        if self.version.trim().is_empty() {
            return Err(WorkflowRegistryError::VersionMissing);
        }
        let digest = self
            .content_digest
            .strip_prefix("sha256:")
            .ok_or(WorkflowRegistryError::ContentDigestInvalid)?;
        crate::contract::Digest256::parse(digest)
            .map_err(|_| WorkflowRegistryError::ContentDigestInvalid)?;
        Ok(())
    }
}

/// The registered workflow data is its identity; content is addressed by digest.
pub type WorkflowRegistration = WorkflowIdentity;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum WorkflowRegistryError {
    WorkflowIdMissing,
    VersionMissing,
    ContentDigestInvalid,
    IdentityUnknown,
}

/// Deterministic, serializable registration records with exact-identity lookup.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WorkflowRegistry {
    registrations: Vec<WorkflowRegistration>,
}

impl WorkflowRegistry {
    pub fn register(
        &mut self,
        registration: WorkflowRegistration,
    ) -> Result<WorkflowRegistration, WorkflowRegistryError> {
        registration.validate()?;
        if !self.registrations.contains(&registration) {
            self.registrations.push(registration.clone());
            self.registrations.sort();
        }
        Ok(registration)
    }

    pub fn read(
        &self,
        identity: &WorkflowIdentity,
    ) -> Result<&WorkflowRegistration, WorkflowRegistryError> {
        identity.validate()?;
        self.registrations
            .iter()
            .find(|registration| *registration == identity)
            .ok_or(WorkflowRegistryError::IdentityUnknown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> WorkflowIdentity {
        WorkflowIdentity {
            workflow_id: "summarize".into(),
            version: "1.2.0".into(),
            content_digest: format!("sha256:{}", "a".repeat(64)),
        }
    }

    // spec: EG-DECISION-ENGINE-R166.1
    #[test]
    fn registration_round_trips_and_reads_by_exact_identity() {
        let expected = identity();
        let mut registry = WorkflowRegistry::default();
        assert_eq!(registry.register(expected.clone()), Ok(expected.clone()));
        let encoded = serde_json::to_string(&registry).unwrap();
        let restored: WorkflowRegistry = serde_json::from_str(&encoded).unwrap();
        let read = restored.read(&expected).unwrap();
        assert_eq!(read.workflow_id, expected.workflow_id);
        assert_eq!(read.version, expected.version);
        assert_eq!(read.content_digest, expected.content_digest);
    }

    // spec: EG-DECISION-ENGINE-R166.1
    #[test]
    fn invalid_or_unknown_identity_is_typed_refusal() {
        let mut registry = WorkflowRegistry::default();
        let mut invalid = identity();
        invalid.workflow_id.clear();
        assert_eq!(
            registry.register(invalid),
            Err(WorkflowRegistryError::WorkflowIdMissing)
        );
        let mut invalid = identity();
        invalid.version.clear();
        assert_eq!(
            registry.register(invalid),
            Err(WorkflowRegistryError::VersionMissing)
        );
        let mut invalid = identity();
        invalid.content_digest.clear();
        assert_eq!(
            registry.register(invalid),
            Err(WorkflowRegistryError::ContentDigestInvalid)
        );
        assert_eq!(
            registry.read(&identity()),
            Err(WorkflowRegistryError::IdentityUnknown)
        );
    }
}
