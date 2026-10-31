use super::common::deserialize_required_option;
use super::development_quota::DevelopmentLaneQuotaCharge;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneHoldSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneHoldHostTargetKind {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "inventory_alias")]
    InventoryAlias,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneHoldState {
    #[serde(rename = "allocating")]
    Allocating,
    #[serde(rename = "active")]
    Active,
    #[serde(rename = "submitted")]
    Submitted,
    #[serde(rename = "released")]
    Released,
    #[serde(rename = "expired")]
    Expired,
    #[serde(rename = "cleanup_pending")]
    CleanupPending,
    #[serde(rename = "cleaned")]
    Cleaned,
    #[serde(rename = "aborted")]
    Aborted,
    #[serde(rename = "absent")]
    Absent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneObserveRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneObserveResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneObserveResultDecision {
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
    #[serde(rename = "quota")]
    Quota,
    #[serde(rename = "policy")]
    Policy,
    #[serde(rename = "drained")]
    Drained,
    #[serde(rename = "not_found")]
    NotFound,
    #[serde(rename = "wrong_kind")]
    WrongKind,
    #[serde(rename = "wrong_tenant")]
    WrongTenant,
    #[serde(rename = "wrong_owner")]
    WrongOwner,
    #[serde(rename = "wrong_attempt")]
    WrongAttempt,
    #[serde(rename = "wrong_lease_epoch")]
    WrongLeaseEpoch,
    #[serde(rename = "wrong_fence")]
    WrongFence,
    #[serde(rename = "expired")]
    Expired,
    #[serde(rename = "terminal")]
    Terminal,
    #[serde(rename = "cleanup_required")]
    CleanupRequired,
    #[serde(rename = "exclusivity")]
    Exclusivity,
    #[serde(rename = "invalid")]
    Invalid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneQueryRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneQueryResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneQueryResultDecision {
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
    #[serde(rename = "quota")]
    Quota,
    #[serde(rename = "policy")]
    Policy,
    #[serde(rename = "drained")]
    Drained,
    #[serde(rename = "not_found")]
    NotFound,
    #[serde(rename = "wrong_kind")]
    WrongKind,
    #[serde(rename = "wrong_tenant")]
    WrongTenant,
    #[serde(rename = "wrong_owner")]
    WrongOwner,
    #[serde(rename = "wrong_attempt")]
    WrongAttempt,
    #[serde(rename = "wrong_lease_epoch")]
    WrongLeaseEpoch,
    #[serde(rename = "wrong_fence")]
    WrongFence,
    #[serde(rename = "expired")]
    Expired,
    #[serde(rename = "terminal")]
    Terminal,
    #[serde(rename = "cleanup_required")]
    CleanupRequired,
    #[serde(rename = "exclusivity")]
    Exclusivity,
    #[serde(rename = "invalid")]
    Invalid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneStatusRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneStatusResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DevelopmentLaneHold {
    pub schema_version: DevelopmentLaneHoldSchemaVersion,
    pub hold_id: String,
    pub lane_id: String,
    pub tenant_ref: String,
    pub request_id: String,
    pub work_item_id: String,
    pub owner_id: String,
    pub session_id: String,
    pub fairness_group: String,
    pub workspace_ref: String,
    pub repository_id: String,
    pub base_ref: String,
    pub base_sha: String,
    pub branch: String,
    pub worktree_locator: String,
    pub host_target_kind: DevelopmentLaneHoldHostTargetKind,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub host_target_alias: Option<String>,
    pub host_ref: String,
    pub quota_policy_name: String,
    pub quota_policy_version: String,
    pub input_fingerprint: String,
    pub predicted_disk_bytes: u64,
    pub observed_disk_bytes: u64,
    pub retained_disk_bytes: u64,
    pub active_count_charged: bool,
    pub quota_charge: DevelopmentLaneQuotaCharge,
    pub state: DevelopmentLaneHoldState,
    pub attempt: u64,
    pub lease_epoch: u64,
    pub fencing_token: u64,
    pub work_item_fence: String,
    pub hold_revision: u64,
    pub lifecycle_revision: u64,
    pub allocation_revision: u64,
    pub cleanup_revision: u64,
    pub expires_at_ms: u64,
    pub last_renewed_at_ms: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cleanup_work_item_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cleanup_work_item_fence: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cleanup_attempt: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cleanup_lease_epoch: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cleanup_fencing_token: Option<u64>,
    pub tombstone: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DevelopmentLaneObserveRequest {
    pub schema_version: DevelopmentLaneObserveRequestSchemaVersion,
    pub tenant_ref: String,
    pub work_item_id: String,
    pub owner_id: String,
    pub attempt: u64,
    pub lease_epoch: u64,
    pub fencing_token: u64,
    pub work_item_fence: String,
    pub hold_id: String,
    pub expected_hold_revision: u64,
    pub observed_disk_bytes: u64,
    pub observation_revision: u64,
    pub idempotency_key: String,
    pub now_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DevelopmentLaneObserveResult {
    pub schema_version: DevelopmentLaneObserveResultSchemaVersion,
    pub decision: DevelopmentLaneObserveResultDecision,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub hold: Option<DevelopmentLaneHold>,
    pub hold_revision: u64,
    pub lifecycle_revision: u64,
    pub tombstone: bool,
    pub changed_work_item_ids: Vec<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub quota_charge: Option<DevelopmentLaneQuotaCharge>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DevelopmentLaneQueryRequest {
    pub schema_version: DevelopmentLaneQueryRequestSchemaVersion,
    pub tenant_ref: String,
    pub hold_id: String,
    pub now_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DevelopmentLaneQueryResult {
    pub schema_version: DevelopmentLaneQueryResultSchemaVersion,
    pub decision: DevelopmentLaneQueryResultDecision,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub hold: Option<DevelopmentLaneHold>,
    pub hold_revision: u64,
    pub lifecycle_revision: u64,
    pub tombstone: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DevelopmentLaneStatusRequest {
    pub schema_version: DevelopmentLaneStatusRequestSchemaVersion,
    pub tenant_ref: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub hold_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub lane_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub work_item_id: Option<String>,
    pub limit: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cursor: Option<String>,
    pub now_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DevelopmentLaneStatusResult {
    pub schema_version: DevelopmentLaneStatusResultSchemaVersion,
    pub complete: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub next_cursor: Option<String>,
    pub holds: Vec<DevelopmentLaneHold>,
    pub counters: DevelopmentLaneQuotaCharge,
    pub tenant_active_count: u64,
    pub tenant_retained_disk_bytes: u64,
    pub tombstone: bool,
}
