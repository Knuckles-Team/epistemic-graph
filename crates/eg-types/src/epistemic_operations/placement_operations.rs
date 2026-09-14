use super::common::deserialize_required_option;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PlacementRouteSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PlacementRouteRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ClaimWorkItemRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ClaimWorkItemResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ClaimWorkItemResultReason {
    #[serde(rename = "claimed")]
    Claimed,
    #[serde(rename = "empty")]
    Empty,
    #[serde(rename = "tenant_quota")]
    TenantQuota,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum EvidenceBundleSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationResultStatus {
    #[serde(rename = "succeeded")]
    Succeeded,
    #[serde(rename = "failed")]
    Failed,
    #[serde(rename = "redirected")]
    Redirected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationRedirectKind {
    #[serde(rename = "placement")]
    Placement,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlacementRoute {
    pub schema_version: PlacementRouteSchemaVersion,
    pub route_id: String,
    pub tenant_ref: String,
    pub partition_ref: String,
    pub authoritative: bool,
    pub placed: bool,
    pub group: u64,
    pub epoch: u64,
    pub fencing_token: u64,
    pub stale: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub leader_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PlacementRouteRequest {
    pub schema_version: PlacementRouteRequestSchemaVersion,
    pub tenant_ref: String,
    pub partition_ref: String,
    pub client_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClaimWorkItemRequest {
    pub schema_version: ClaimWorkItemRequestSchemaVersion,
    pub tenant_ref: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub work_item_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub queue_ref: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub resource_class: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub fairness_group: Option<String>,
    pub worker_ref: String,
    pub now_ms: u64,
    pub lease_ms: u64,
    pub max_tenant_in_flight: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ClaimWorkItemResult {
    pub schema_version: ClaimWorkItemResultSchemaVersion,
    pub claimed: bool,
    pub reason: ClaimWorkItemResultReason,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub work_item_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub kind: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub payload_ref: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub lease_holder_ref: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub lease_epoch: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub fencing_token: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub lease_expires_at_ms: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub attempt: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub max_attempts: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub tenant_in_flight: Option<u64>,
    pub changed_work_item_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EvidenceBundle {
    pub schema_version: EvidenceBundleSchemaVersion,
    pub bundle_id: String,
    pub resolved: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub answer_ref: Option<String>,
    pub claims: Vec<EvidenceClaim>,
    pub policy_exclusions: Vec<String>,
    pub next_action_refs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EvidenceClaim {
    pub claim_ref: String,
    pub kind: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub score: Option<f64>,
    pub confidence: f64,
    pub valid_time: EvidenceTimeRange,
    pub transaction_time: EvidenceTimeRange,
    pub source_refs: Vec<String>,
    pub evidence_locus_refs: Vec<String>,
    pub contradiction_refs: Vec<String>,
    pub proof_refs: Vec<String>,
    pub policy_labels: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EvidenceTimeRange {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub start_ms: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub end_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationResult {
    pub schema_version: OperationResultSchemaVersion,
    pub operation_id: String,
    pub status: OperationResultStatus,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub result_kind: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub result_ref: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub error: Option<OperationError>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub redirect: Option<OperationRedirect>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationError {
    pub code: String,
    pub retryable: bool,
    pub correlation_id: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub detail_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationRedirect {
    pub kind: OperationRedirectKind,
    pub target_ref: String,
    pub group: u64,
    pub epoch: u64,
    pub fencing_token: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub leader_ref: Option<String>,
}
