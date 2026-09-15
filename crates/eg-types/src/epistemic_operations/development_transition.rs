use super::common::deserialize_required_option;
use super::development_quota::DevelopmentLaneQuotaCharge;
use super::development_state::DevelopmentLaneHold;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneCleanupCompleteRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneCleanupCompleteResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneCleanupCompleteResultDecision {
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
pub enum DevelopmentLaneFinishRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneFinishRequestTerminalState {
    #[serde(rename = "succeeded")]
    Succeeded,
    #[serde(rename = "failed")]
    Failed,
    #[serde(rename = "cancelled")]
    Cancelled,
    #[serde(rename = "dead_letter")]
    DeadLetter,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneFinishResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneFinishResultDecision {
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
pub enum DevelopmentLaneRenewRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneRenewResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneRenewResultDecision {
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
pub enum DevelopmentLaneResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneResultDecision {
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DevelopmentLaneCleanupCompleteRequest {
    pub schema_version: DevelopmentLaneCleanupCompleteRequestSchemaVersion,
    pub tenant_ref: String,
    pub work_item_id: String,
    pub owner_id: String,
    pub attempt: u64,
    pub lease_epoch: u64,
    pub fencing_token: u64,
    pub work_item_fence: String,
    pub cleanup_work_item_id: String,
    pub cleanup_work_item_fence: String,
    pub cleanup_attempt: u64,
    pub cleanup_lease_epoch: u64,
    pub cleanup_fencing_token: u64,
    pub hold_id: String,
    pub expected_hold_revision: u64,
    pub removal_proof_ref: String,
    pub idempotency_key: String,
    pub now_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DevelopmentLaneCleanupCompleteResult {
    pub schema_version: DevelopmentLaneCleanupCompleteResultSchemaVersion,
    pub decision: DevelopmentLaneCleanupCompleteResultDecision,
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
pub struct DevelopmentLaneFinishRequest {
    pub schema_version: DevelopmentLaneFinishRequestSchemaVersion,
    pub tenant_ref: String,
    pub work_item_id: String,
    pub owner_id: String,
    pub attempt: u64,
    pub lease_epoch: u64,
    pub fencing_token: u64,
    pub work_item_fence: String,
    pub hold_id: String,
    pub expected_hold_revision: u64,
    pub terminal_state: DevelopmentLaneFinishRequestTerminalState,
    pub idempotency_key: String,
    pub now_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DevelopmentLaneFinishResult {
    pub schema_version: DevelopmentLaneFinishResultSchemaVersion,
    pub decision: DevelopmentLaneFinishResultDecision,
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
pub struct DevelopmentLaneRenewRequest {
    pub schema_version: DevelopmentLaneRenewRequestSchemaVersion,
    pub tenant_ref: String,
    pub work_item_id: String,
    pub owner_id: String,
    pub attempt: u64,
    pub lease_epoch: u64,
    pub fencing_token: u64,
    pub work_item_fence: String,
    pub hold_id: String,
    pub expected_hold_revision: u64,
    pub ttl_ms: u64,
    pub idempotency_key: String,
    pub now_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DevelopmentLaneRenewResult {
    pub schema_version: DevelopmentLaneRenewResultSchemaVersion,
    pub decision: DevelopmentLaneRenewResultDecision,
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
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DevelopmentLaneResult {
    pub schema_version: DevelopmentLaneResultSchemaVersion,
    pub decision: DevelopmentLaneResultDecision,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub hold: Option<DevelopmentLaneHold>,
    pub hold_revision: u64,
    pub lifecycle_revision: u64,
    pub tombstone: bool,
    pub changed_work_item_ids: Vec<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub quota_charge: Option<DevelopmentLaneQuotaCharge>,
}
