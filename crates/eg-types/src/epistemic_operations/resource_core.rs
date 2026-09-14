use super::common::deserialize_required_option;
use super::resource_status::ResourceTargetSnapshot;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceReservationRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceReservationRequestTargetKind {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "inventory_alias")]
    InventoryAlias,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceReservationRecordTargetKind {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "inventory_alias")]
    InventoryAlias,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceReservationRecordState {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceReservationResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceReservationResultDecision {
    #[serde(rename = "accepted")]
    Accepted,
    #[serde(rename = "idempotent")]
    Idempotent,
    #[serde(rename = "stale")]
    Stale,
    #[serde(rename = "conflict")]
    Conflict,
    #[serde(rename = "input_conflict")]
    InputConflict,
    #[serde(rename = "capacity")]
    Capacity,
    #[serde(rename = "policy")]
    Policy,
    #[serde(rename = "drained")]
    Drained,
    #[serde(rename = "quarantined")]
    Quarantined,
    #[serde(rename = "stale_host")]
    StaleHost,
    #[serde(rename = "labels")]
    Labels,
    #[serde(rename = "anti_affinity")]
    AntiAffinity,
    #[serde(rename = "disk")]
    Disk,
    #[serde(rename = "concurrency")]
    Concurrency,
    #[serde(rename = "exclusivity")]
    Exclusivity,
    #[serde(rename = "not_found")]
    NotFound,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceReservationResultState {
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
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ResourceReservationRequest {
    pub schema_version: ResourceReservationRequestSchemaVersion,
    pub tenant_ref: String,
    pub work_item_id: String,
    pub owner_id: String,
    pub fence: String,
    pub lease_epoch: u64,
    pub fencing_token: u64,
    pub attempt: u64,
    pub reservation_id: String,
    pub input_fingerprint: String,
    pub profile_name: String,
    pub profile_version: String,
    pub host_ref: String,
    pub requirement: ResourceRequirement,
    pub target_kind: ResourceReservationRequestTargetKind,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub target_alias: Option<String>,
    pub repository_id: String,
    pub branch: String,
    pub concurrency_key: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub concurrency_limit: Option<u64>,
    pub repository_exclusive: bool,
    pub branch_exclusive: bool,
    pub required_labels: Vec<String>,
    pub anti_affinity: Vec<String>,
    pub fairness_group: String,
    pub fairness_cost: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub disk_low_watermark_mib: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub disk_high_watermark_mib: Option<u64>,
    pub disk_policy_key: String,
    pub reserved_at_ms: u64,
    pub expires_at_ms: u64,
    pub idempotency_key: String,
    pub now_ms: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_host_revision: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_lifecycle_revision: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResourceCapacitySnapshot {
    pub cpu_weight: u64,
    pub memory_mib: u64,
    pub disk_mib: u64,
    pub process_slots: u64,
    pub host_revision: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ResourceRequirement {
    pub cpu_weight: u64,
    pub memory_mib: u64,
    pub disk_mib: u64,
    pub process_slots: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResourceReservationRecord {
    pub reservation_id: String,
    pub tenant_ref: String,
    pub owner_id: String,
    pub work_item_id: String,
    pub fence: String,
    pub attempt: u64,
    pub lease_epoch: u64,
    pub fencing_token: u64,
    pub input_fingerprint: String,
    pub host_ref: String,
    pub profile_name: String,
    pub profile_version: String,
    pub requirement: ResourceRequirement,
    pub capacity_snapshot: ResourceCapacitySnapshot,
    pub selected_target: ResourceTargetSnapshot,
    pub target_kind: ResourceReservationRecordTargetKind,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub target_alias: Option<String>,
    pub repository_id: String,
    pub branch: String,
    pub concurrency_key: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub concurrency_limit: Option<u64>,
    pub repository_exclusive: bool,
    pub branch_exclusive: bool,
    pub required_labels: Vec<String>,
    pub anti_affinity: Vec<String>,
    pub fairness_group: String,
    pub fairness_cost: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub disk_low_watermark_mib: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub disk_high_watermark_mib: Option<u64>,
    pub disk_policy_key: String,
    pub reserved_at_ms: u64,
    pub expires_at_ms: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_host_revision: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_lifecycle_revision: Option<u64>,
    pub state: ResourceReservationRecordState,
    pub revision: u64,
    pub lifecycle_revision: u64,
    pub tombstone: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ResourceReservationResult {
    pub schema_version: ResourceReservationResultSchemaVersion,
    pub decision: ResourceReservationResultDecision,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub reservation_id: Option<String>,
    pub work_item_id: String,
    pub attempt: u64,
    pub lease_epoch: u64,
    pub fencing_token: u64,
    pub lifecycle_revision: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub host_ref: Option<String>,
    pub host_revision: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub record: Option<ResourceReservationRecord>,
    pub state: ResourceReservationResultState,
    pub held_cpu_weight: u64,
    pub held_memory_mib: u64,
    pub held_disk_mib: u64,
    pub held_process_slots: u64,
    pub fairness_debt: u64,
    pub tombstone: bool,
    pub changed_work_item_ids: Vec<String>,
}
