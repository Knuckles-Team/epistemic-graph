use super::common::deserialize_required_option;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceHostUpdateRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceHostUpdateRequestTargetKind {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "inventory_alias")]
    InventoryAlias,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceHostUpdateResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceHostUpdateResultReason {
    #[serde(rename = "accepted")]
    Accepted,
    #[serde(rename = "stale_host")]
    StaleHost,
    #[serde(rename = "conflict")]
    Conflict,
    #[serde(rename = "not_found")]
    NotFound,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceHostUpdateSnapshotTargetKind {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "inventory_alias")]
    InventoryAlias,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ResourceHostUpdateRequest {
    pub schema_version: ResourceHostUpdateRequestSchemaVersion,
    pub tenant_ref: String,
    pub host_ref: String,
    pub revision: u64,
    pub capacity: ResourceCapacity,
    pub observed: ResourceCapacity,
    pub heartbeat_at_ms: u64,
    pub heartbeat_ttl_ms: u64,
    pub now_ms: u64,
    pub draining: bool,
    pub quarantined: bool,
    pub labels: Vec<String>,
    pub target_kind: ResourceHostUpdateRequestTargetKind,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub target_alias: Option<String>,
    pub disk_used_mib: u64,
    pub disk_capacity_mib: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ResourceCapacity {
    pub cpu_weight: u64,
    pub memory_mib: u64,
    pub disk_mib: u64,
    pub process_slots: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResourceHostUpdateCapacitySnapshot {
    pub cpu_weight: u64,
    pub memory_mib: u64,
    pub disk_mib: u64,
    pub process_slots: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResourceHostUpdateDiskPolicySnapshot {
    pub policy_key: String,
    pub blocked: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub low_watermark_mib: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub high_watermark_mib: Option<u64>,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResourceHostUpdateResult {
    pub schema_version: ResourceHostUpdateResultSchemaVersion,
    pub accepted: bool,
    pub reason: ResourceHostUpdateResultReason,
    pub host_ref: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub host_snapshot: Option<ResourceHostUpdateSnapshot>,
    pub revision: u64,
    pub held_cpu_weight: u64,
    pub held_memory_mib: u64,
    pub held_disk_mib: u64,
    pub held_process_slots: u64,
    pub draining: bool,
    pub quarantined: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResourceHostUpdateSnapshot {
    pub host_ref: String,
    pub revision: u64,
    pub capacity: ResourceHostUpdateCapacitySnapshot,
    pub observed: ResourceHostUpdateCapacitySnapshot,
    pub heartbeat_at_ms: u64,
    pub heartbeat_ttl_ms: u64,
    pub draining: bool,
    pub quarantined: bool,
    pub labels: Vec<String>,
    pub target_kind: ResourceHostUpdateSnapshotTargetKind,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub target_alias: Option<String>,
    pub disk_used_mib: u64,
    pub disk_capacity_mib: u64,
    pub held_cpu_weight: u64,
    pub held_memory_mib: u64,
    pub held_disk_mib: u64,
    pub held_process_slots: u64,
    pub disk_policies: Vec<ResourceHostUpdateDiskPolicySnapshot>,
}
