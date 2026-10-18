//! Pending-call storage preconditions; these establish no human approval.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::epistemic_operations_ext::CasWorkItemMetadataLeaseFence;

pub const PENDING_CALL_KIND: &str = "work_item.pending_call";

/// Compare the exact claimed WorkItem revision and scheduling metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PendingCallFence {
    pub work_item_id: String,
    pub expected_revision: u64,
    pub expected_metadata: Map<String, Value>,
}

/// Registration additionally requires the current live worker fence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PendingCallRegistration {
    pub work_item: PendingCallFence,
    pub expected_lease: CasWorkItemMetadataLeaseFence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<PendingPlanPreimage>,
}

/// Compare the entire immutable envelope, not caller-selected grant fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PendingCallConsumption {
    pub work_item: PendingCallFence,
    pub expected_grant: Map<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<PendingPlanPreimage>,
}

impl PendingCallFence {
    pub fn validate(&self) -> Result<(), String> {
        super::bounded("pending WorkItem", &self.work_item_id)?;
        if self.expected_revision == 0 {
            return Err("pending WorkItem revision must be positive".into());
        }
        let bytes = serde_json::to_vec(&self.expected_metadata).map_err(|e| e.to_string())?;
        if bytes.len() > super::MAX_CONTROL_LEASE_GRANT_BYTES {
            return Err("pending metadata preimage exceeds native bounds".into());
        }
        Ok(())
    }
}

/// Exact immutable stored-data expectation supplied by the caller.
/// This is not an authorization grant; Arbora owns permission decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PendingPlanPreimage {
    pub lease_id: String,
    pub expected_revision: u64,
    pub expected_kind: String,
    pub expected_grant: Map<String, Value>,
}

impl PendingPlanPreimage {
    pub fn validate(&self) -> Result<(), String> {
        super::bounded("plan lease", &self.lease_id)?;
        super::bounded("plan kind", &self.expected_kind)?;
        if self.expected_revision == 0 || self.expected_kind == PENDING_CALL_KIND {
            return Err("plan preimage requires a positive revision and non-pending kind".into());
        }
        let bytes = serde_json::to_vec(&self.expected_grant).map_err(|e| e.to_string())?;
        if bytes.len() > super::MAX_CONTROL_LEASE_GRANT_BYTES {
            return Err("plan grant preimage exceeds native bounds".into());
        }
        Ok(())
    }
}
