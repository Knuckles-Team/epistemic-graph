//! Wire result DTOs for served-modality operations.

use serde::{Deserialize, Serialize};

#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ServedModalityClassification {
    Public,
    Internal,
    Confidential,
    Restricted,
}

#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ServedModalityAuthority {
    pub tenant_ref: String,
    pub access_policy_ref: String,
    pub purpose_ref: String,
    pub maximum_classification: ServedModalityClassification,
}

#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ServedModalityApplyDisposition {
    Applied,
    IdempotentReplay,
}

#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ServedModalityApplyOutcome {
    pub disposition: ServedModalityApplyDisposition,
    pub observation_version: u64,
    pub event_sequence: u64,
}

#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ServedModalityEventKind {
    Ingested,
    Updated,
    Deleted,
    MovedToCold,
    Restored,
    Reindexed,
}

#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ServedModalityEvent {
    pub sequence: u64,
    pub occurrence_id: String,
    pub observation_version: u64,
    pub kind: ServedModalityEventKind,
    pub tenant_ref: String,
    pub access_policy_ref: String,
}

#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ServedModalityStats {
    pub active_records: usize,
    pub total_records: usize,
    pub tombstoned_records: usize,
    pub modality_index_postings: usize,
    pub segment_index_postings: usize,
    pub native_index_keys: usize,
    pub native_index_postings: usize,
    pub events: usize,
    pub snapshot_bytes: usize,
}

#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ServedModalityTombstoneCollection {
    pub collected: usize,
}

#[cfg(feature = "modality-serving")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ServedModalityCapabilities {
    pub component_ready: bool,
    pub component_pass: usize,
    pub component_not_applicable: usize,
    pub component_total: usize,
}
