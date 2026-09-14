use super::common::deserialize_required_option;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneQuotaChargeSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneQuotaPolicySchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneQuotaUpdateRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneQuotaUpdateResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneQuotaUpdateResultDecision {
    #[serde(rename = "accepted")]
    Accepted,
    #[serde(rename = "idempotent")]
    Idempotent,
    #[serde(rename = "stale")]
    Stale,
    #[serde(rename = "conflict")]
    Conflict,
    #[serde(rename = "quota")]
    Quota,
    #[serde(rename = "policy")]
    Policy,
    #[serde(rename = "drained")]
    Drained,
    #[serde(rename = "invalid")]
    Invalid,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DevelopmentLaneQuotaCharge {
    pub schema_version: DevelopmentLaneQuotaChargeSchemaVersion,
    pub tenant_count: u64,
    pub owner_count: u64,
    pub session_count: u64,
    pub workspace_count: u64,
    pub repository_count: u64,
    pub host_count: u64,
    pub global_count: u64,
    pub tenant_predicted_disk_bytes: u64,
    pub owner_predicted_disk_bytes: u64,
    pub session_predicted_disk_bytes: u64,
    pub workspace_predicted_disk_bytes: u64,
    pub repository_predicted_disk_bytes: u64,
    pub host_predicted_disk_bytes: u64,
    pub global_predicted_disk_bytes: u64,
    pub tenant_observed_disk_bytes: u64,
    pub owner_observed_disk_bytes: u64,
    pub session_observed_disk_bytes: u64,
    pub workspace_observed_disk_bytes: u64,
    pub repository_observed_disk_bytes: u64,
    pub host_observed_disk_bytes: u64,
    pub global_observed_disk_bytes: u64,
    pub tenant_retained_disk_bytes: u64,
    pub owner_retained_disk_bytes: u64,
    pub session_retained_disk_bytes: u64,
    pub workspace_retained_disk_bytes: u64,
    pub repository_retained_disk_bytes: u64,
    pub host_retained_disk_bytes: u64,
    pub global_retained_disk_bytes: u64,
    pub revision: u64,
    pub policy_revision: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DevelopmentLaneQuotaPolicy {
    pub schema_version: DevelopmentLaneQuotaPolicySchemaVersion,
    pub policy_name: String,
    pub policy_version: String,
    pub tenant_count_limit: u64,
    pub owner_count_limit: u64,
    pub session_count_limit: u64,
    pub workspace_count_limit: u64,
    pub repository_count_limit: u64,
    pub host_count_limit: u64,
    pub global_count_limit: u64,
    pub tenant_predicted_disk_bytes: u64,
    pub owner_predicted_disk_bytes: u64,
    pub session_predicted_disk_bytes: u64,
    pub workspace_predicted_disk_bytes: u64,
    pub repository_predicted_disk_bytes: u64,
    pub host_predicted_disk_bytes: u64,
    pub global_predicted_disk_bytes: u64,
    pub tenant_observed_disk_bytes: u64,
    pub owner_observed_disk_bytes: u64,
    pub session_observed_disk_bytes: u64,
    pub workspace_observed_disk_bytes: u64,
    pub repository_observed_disk_bytes: u64,
    pub host_observed_disk_bytes: u64,
    pub global_observed_disk_bytes: u64,
    pub tenant_retained_disk_bytes: u64,
    pub owner_retained_disk_bytes: u64,
    pub session_retained_disk_bytes: u64,
    pub workspace_retained_disk_bytes: u64,
    pub repository_retained_disk_bytes: u64,
    pub host_retained_disk_bytes: u64,
    pub global_retained_disk_bytes: u64,
    pub min_ttl_ms: u64,
    pub max_ttl_ms: u64,
    pub max_observation_staleness_ms: u64,
    pub drain_only: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DevelopmentLaneQuotaUpdateRequest {
    pub schema_version: DevelopmentLaneQuotaUpdateRequestSchemaVersion,
    pub tenant_ref: String,
    pub policy: DevelopmentLaneQuotaPolicy,
    pub expected_policy_revision: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_policy_version: Option<String>,
    pub idempotency_key: String,
    pub now_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DevelopmentLaneQuotaUpdateResult {
    pub schema_version: DevelopmentLaneQuotaUpdateResultSchemaVersion,
    pub decision: DevelopmentLaneQuotaUpdateResultDecision,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub policy: Option<DevelopmentLaneQuotaPolicy>,
    pub counters: DevelopmentLaneQuotaCharge,
    pub policy_revision: u64,
}
