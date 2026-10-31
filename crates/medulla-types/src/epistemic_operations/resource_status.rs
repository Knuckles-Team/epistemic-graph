use super::common::deserialize_required_option;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceTargetSnapshotKind {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "inventory_alias")]
    InventoryAlias,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceReservationStatusRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceReservationHostSnapshotTargetKind {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "inventory_alias")]
    InventoryAlias,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceReservationStatusResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceReservationSummaryState {
    #[serde(rename = "reserved")]
    Reserved,
    #[serde(rename = "released")]
    Released,
    #[serde(rename = "reclaimed")]
    Reclaimed,
    #[serde(rename = "expired")]
    Expired,
    #[serde(rename = "superseded")]
    Superseded,
    #[serde(rename = "absent")]
    Absent,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResourceTargetSnapshot {
    pub kind: ResourceTargetSnapshotKind,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub alias: Option<String>,
    pub capability_labels: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ResourceReservationStatusRequest {
    pub schema_version: ResourceReservationStatusRequestSchemaVersion,
    pub tenant_ref: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub work_item_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub reservation_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub host_ref: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub owner_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub fence: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub attempt: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub lease_epoch: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub fencing_token: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub input_fingerprint: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub fairness_group: Option<String>,
    pub limit: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cursor: Option<String>,
    pub now_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResourceReservationDiskPolicySnapshot {
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
pub struct ResourceReservationHostCapacitySnapshot {
    pub cpu_weight: u64,
    pub memory_mib: u64,
    pub disk_mib: u64,
    pub process_slots: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResourceReservationHostSnapshot {
    pub host_ref: String,
    pub revision: u64,
    pub capacity: ResourceReservationHostCapacitySnapshot,
    pub observed: ResourceReservationHostCapacitySnapshot,
    pub heartbeat_at_ms: u64,
    pub heartbeat_ttl_ms: u64,
    pub draining: bool,
    pub quarantined: bool,
    pub labels: Vec<String>,
    pub target_kind: ResourceReservationHostSnapshotTargetKind,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub target_alias: Option<String>,
    pub disk_used_mib: u64,
    pub disk_capacity_mib: u64,
    pub held_cpu_weight: u64,
    pub held_memory_mib: u64,
    pub held_disk_mib: u64,
    pub held_process_slots: u64,
    pub disk_policies: Vec<ResourceReservationDiskPolicySnapshot>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResourceReservationStatusResult {
    pub schema_version: ResourceReservationStatusResultSchemaVersion,
    pub complete: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub next_cursor: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub host_snapshot: Option<ResourceReservationHostSnapshot>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub host_ref: Option<String>,
    pub host_revision: u64,
    pub held_cpu_weight: u64,
    pub held_memory_mib: u64,
    pub held_disk_mib: u64,
    pub held_process_slots: u64,
    pub fairness_debt: u64,
    pub reservations: Vec<ResourceReservationSummary>,
    pub orphan_count: u64,
    pub superseded_count: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResourceReservationSummary {
    pub reservation_id: String,
    pub work_item_id: String,
    pub attempt: u64,
    pub host_ref: String,
    pub profile_name: String,
    pub fairness_group: String,
    pub state: ResourceReservationSummaryState,
    pub revision: u64,
    pub expires_at_ms: u64,
    pub held_cpu_weight: u64,
    pub held_memory_mib: u64,
    pub held_disk_mib: u64,
    pub held_process_slots: u64,
    pub tombstone: bool,
}
