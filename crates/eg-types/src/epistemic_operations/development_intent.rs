use super::common::deserialize_required_option;
use super::context_mutation::RequestContext;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum WorkItemSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneIntentSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneIntentHostTargetKind {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "inventory_alias")]
    InventoryAlias,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevelopmentLaneCleanupIntentSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DevelopmentLaneReserveRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct WorkItem {
    pub schema_version: WorkItemSchemaVersion,
    pub work_item_id: String,
    pub context: RequestContext,
    pub kind: String,
    pub state: String,
    pub priority: i64,
    pub depends_on: Vec<String>,
    pub input_artifact_refs: Vec<String>,
    pub output_artifact_refs: Vec<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub lease_holder_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub lease_expires_at_ms: Option<u64>,
    pub attempt: u64,
    pub max_attempts: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub idempotency_key: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub lane_intent: Option<DevelopmentLaneIntent>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "contract-schema", schemars(transform = super::common::require_marked_nullable_fields))]
pub struct DevelopmentLaneIntent {
    pub schema_version: DevelopmentLaneIntentSchemaVersion,
    #[cfg_attr(feature = "contract-schema", schemars(length(min = 1), extend("x-eg-utf8-max-bytes" = 512, "x-eg-no-control-bytes" = true)))]
    pub tenant_ref: String,
    #[cfg_attr(feature = "contract-schema", schemars(length(min = 1), extend("x-eg-utf8-max-bytes" = 512, "x-eg-no-control-bytes" = true)))]
    pub request_id: String,
    #[cfg_attr(feature = "contract-schema", schemars(length(min = 1), extend("x-eg-utf8-max-bytes" = 512, "x-eg-no-control-bytes" = true)))]
    pub lane_id: String,
    #[cfg_attr(feature = "contract-schema", schemars(length(min = 1), extend("x-eg-utf8-max-bytes" = 512, "x-eg-no-control-bytes" = true)))]
    pub repository_id: String,
    #[cfg_attr(feature = "contract-schema", schemars(length(min = 1), extend("x-eg-utf8-max-bytes" = 512, "x-eg-no-control-bytes" = true)))]
    pub base_ref: String,
    pub base_sha: String,
    #[cfg_attr(feature = "contract-schema", schemars(length(min = 1), extend("x-eg-utf8-max-bytes" = 512, "x-eg-no-control-bytes" = true)))]
    pub branch: String,
    pub host_target_kind: DevelopmentLaneIntentHostTargetKind,
    #[serde(deserialize_with = "deserialize_required_option")]
    #[cfg_attr(feature = "contract-schema", schemars(length(min = 1), extend("x-eg-utf8-max-bytes" = 512, "x-eg-no-control-bytes" = true, "x-eg-required-presence" = true)))]
    pub host_target_alias: Option<String>,
    #[cfg_attr(feature = "contract-schema", schemars(length(min = 1), extend("x-eg-utf8-max-bytes" = 512, "x-eg-no-control-bytes" = true)))]
    pub host_ref: String,
    #[cfg_attr(feature = "contract-schema", schemars(length(min = 1), extend("x-eg-utf8-max-bytes" = 512, "x-eg-no-control-bytes" = true)))]
    pub resource_reservation_id: String,
    #[cfg_attr(feature = "contract-schema", schemars(length(min = 1), extend("x-eg-utf8-max-bytes" = 512, "x-eg-no-control-bytes" = true)))]
    pub workspace_ref: String,
    pub worktree_locator: String,
    #[cfg_attr(feature = "contract-schema", schemars(length(min = 1), extend("x-eg-utf8-max-bytes" = 512, "x-eg-no-control-bytes" = true)))]
    pub owner_id: String,
    #[cfg_attr(feature = "contract-schema", schemars(length(min = 1), extend("x-eg-utf8-max-bytes" = 512, "x-eg-no-control-bytes" = true)))]
    pub session_id: String,
    #[cfg_attr(feature = "contract-schema", schemars(length(min = 1), extend("x-eg-utf8-max-bytes" = 512, "x-eg-no-control-bytes" = true)))]
    pub fairness_group: String,
    #[cfg_attr(feature = "contract-schema", schemars(length(min = 1), extend("x-eg-utf8-max-bytes" = 512, "x-eg-no-control-bytes" = true)))]
    pub quota_policy_name: String,
    #[cfg_attr(feature = "contract-schema", schemars(length(min = 1), extend("x-eg-utf8-max-bytes" = 512, "x-eg-no-control-bytes" = true)))]
    pub quota_policy_version: String,
    #[cfg_attr(feature = "contract-schema", schemars(extend("x-eg-strict-scalar" = true)))]
    pub predicted_disk_bytes: u64,
    #[cfg_attr(feature = "contract-schema", schemars(extend("x-eg-strict-scalar" = true)))]
    pub ttl_ms: u64,
    #[cfg_attr(
        feature = "contract-schema",
        schemars(length(min = 67, max = 67), regex(pattern = "^v1:[0-9a-f]{64}$"))
    )]
    pub input_fingerprint: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DevelopmentLaneCleanupIntent {
    pub schema_version: DevelopmentLaneCleanupIntentSchemaVersion,
    pub hold_id: String,
    pub lane_id: String,
    pub expected_hold_revision: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DevelopmentLaneReserveRequest {
    pub schema_version: DevelopmentLaneReserveRequestSchemaVersion,
    pub tenant_ref: String,
    pub work_item_id: String,
    pub owner_id: String,
    pub attempt: u64,
    pub lease_epoch: u64,
    pub fencing_token: u64,
    pub work_item_fence: String,
    pub intent: DevelopmentLaneIntent,
    pub idempotency_key: String,
    pub now_ms: u64,
}
