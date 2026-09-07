use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{MutationOperation, MutationRequestContext, MutationScopeIdentity};

/// Digest/version descriptor for authenticated authoritative graph material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MutationStateDescriptor {
    pub algorithm: String,
    pub digest: String,
    pub source_graph_version: u64,
    pub target_graph_version: u64,
}

/// A projection/index/CDC/audit/lineage notification to publish after commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MutationOutboxIntent {
    pub topic: String,
    pub key: String,
    #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    pub headers: BTreeMap<String, String>,
}

/// Explicit OCC semantics captured before validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum VersionExpectation {
    Graph(u64),
    Native(u64),
    /// Valid only for a capability-bearing reserved-system operation.
    Unversioned,
}

/// Exact authoritative version transition committed with a durable record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommittedVersion {
    Graph { source: u64, target: u64 },
    Native { source: u64, target: u64 },
    None,
}

impl CommittedVersion {
    pub fn checked_graph(source: u64) -> Result<Self, String> {
        Ok(Self::Graph {
            source,
            target: source
                .checked_add(1)
                .ok_or_else(|| "graph mutation version overflow".to_string())?,
        })
    }

    pub fn checked_native(source: u64) -> Result<Self, String> {
        Ok(Self::Native {
            source,
            target: source
                .checked_add(1)
                .ok_or_else(|| "native mutation version overflow".to_string())?,
        })
    }

    pub fn target(self) -> Option<u64> {
        match self {
            Self::Graph { target, .. } | Self::Native { target, .. } => Some(target),
            Self::None => None,
        }
    }
}

/// Universal, deterministic durable mutation unit.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MutationBatch {
    pub schema_version: u16,
    /// Stable identity for status lookup and outbox correlation.
    pub batch_id: String,
    pub context: MutationRequestContext,
    /// Exact tenant, typed logical owner, lifecycle generation, and digest.
    pub identity: MutationScopeIdentity,
    /// Catalog epoch used to resolve a placed owner.
    pub placement_epoch: u64,
    /// Caller-stable retry key.
    pub idempotency_key: String,
    pub version_expectation: VersionExpectation,
    /// Lease/worker fencing epoch for work-driven writes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<u64>,
    /// Present when operations are represented by a complete staged graph image.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authoritative_state: Option<MutationStateDescriptor>,
    pub operations: Vec<MutationOperation>,
    pub outbox: Vec<MutationOutboxIntent>,
    /// Creation time supplied by the request boundary.
    pub created_at_ms: u64,
}

impl MutationBatch {
    pub fn validate_identity(&self) -> Result<(), String> {
        self.identity.validate_digest()
    }
}
