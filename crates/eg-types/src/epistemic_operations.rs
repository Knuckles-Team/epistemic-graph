//! Generated strict Epistemic Operations Protocol serde projections.
//!
//! JSON Schema is authoritative. Regenerate with the AU protocol gate.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestContextSchemaVersion {
    #[serde(rename = "2")]
    V2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestContextAuthenticationMethod {
    #[serde(rename = "workload_identity")]
    WorkloadIdentity,
    #[serde(rename = "oidc")]
    Oidc,
    #[serde(rename = "mutual_tls")]
    MutualTls,
    #[serde(rename = "local_process")]
    LocalProcess,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationBatchSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationOperationDomain {
    #[serde(rename = "graph")]
    Graph,
    #[serde(rename = "rdf")]
    Rdf,
    #[serde(rename = "vector")]
    Vector,
    #[serde(rename = "timeseries")]
    Timeseries,
    #[serde(rename = "artifact")]
    Artifact,
    #[serde(rename = "job")]
    Job,
    #[serde(rename = "work_item")]
    WorkItem,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationOperationAction {
    #[serde(rename = "upsert")]
    Upsert,
    #[serde(rename = "delete")]
    Delete,
    #[serde(rename = "append")]
    Append,
    #[serde(rename = "transition")]
    Transition,
    #[serde(rename = "link")]
    Link,
    #[serde(rename = "unlink")]
    Unlink,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeEnvelopeSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeEnvelopeOperation {
    #[serde(rename = "upsert")]
    Upsert,
    #[serde(rename = "delete")]
    Delete,
    #[serde(rename = "snapshot_complete")]
    SnapshotComplete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceAccessClassification {
    #[serde(rename = "public")]
    Public,
    #[serde(rename = "internal")]
    Internal,
    #[serde(rename = "confidential")]
    Confidential,
    #[serde(rename = "restricted")]
    Restricted,
    #[serde(rename = "regulated")]
    Regulated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkItemSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactClassification {
    #[serde(rename = "public")]
    Public,
    #[serde(rename = "internal")]
    Internal,
    #[serde(rename = "confidential")]
    Confidential,
    #[serde(rename = "restricted")]
    Restricted,
    #[serde(rename = "regulated")]
    Regulated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactLocusKind {
    #[serde(rename = "document_span")]
    DocumentSpan,
    #[serde(rename = "table_cell_range")]
    TableCellRange,
    #[serde(rename = "image_region")]
    ImageRegion,
    #[serde(rename = "page_box")]
    PageBox,
    #[serde(rename = "audio_segment")]
    AudioSegment,
    #[serde(rename = "video_frame_range")]
    VideoFrameRange,
    #[serde(rename = "metric_window")]
    MetricWindow,
    #[serde(rename = "row_version")]
    RowVersion,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KnowledgeBatchSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KnowledgeBatchEncoding {
    #[serde(rename = "json_rows")]
    JsonRows,
    #[serde(rename = "arrow_ipc")]
    ArrowIpc,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KnowledgeFieldDataType {
    #[serde(rename = "null")]
    Null,
    #[serde(rename = "boolean")]
    Boolean,
    #[serde(rename = "i64")]
    I64,
    #[serde(rename = "u64")]
    U64,
    #[serde(rename = "f64")]
    F64,
    #[serde(rename = "utf8")]
    Utf8,
    #[serde(rename = "binary")]
    Binary,
    #[serde(rename = "timestamp_ms")]
    TimestampMs,
    #[serde(rename = "json")]
    Json,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnalyticsJobSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnalyticsJobState {
    #[serde(rename = "submitted")]
    Submitted,
    #[serde(rename = "running")]
    Running,
    #[serde(rename = "succeeded")]
    Succeeded,
    #[serde(rename = "failed")]
    Failed,
    #[serde(rename = "cancelled")]
    Cancelled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceOutcomeSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceOutcomeStatus {
    #[serde(rename = "succeeded")]
    Succeeded,
    #[serde(rename = "failed")]
    Failed,
    #[serde(rename = "cancelled")]
    Cancelled,
    #[serde(rename = "denied")]
    Denied,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceOutcomePolicyDecision {
    #[serde(rename = "allow")]
    Allow,
    #[serde(rename = "deny")]
    Deny,
    #[serde(rename = "not_applicable")]
    NotApplicable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementRouteSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementRouteRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimWorkItemRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimWorkItemResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClaimWorkItemResultReason {
    #[serde(rename = "claimed")]
    Claimed,
    #[serde(rename = "empty")]
    Empty,
    #[serde(rename = "tenant_quota")]
    TenantQuota,
}

/// Versioned native capability request.  The engine derives every authority
/// field (tenant, owner, lease epoch/fence, attempt, and expiry) from the
/// authenticated request context and the live WorkItem row; callers provide
/// only the opaque WorkItem identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkItemClaimCapabilityRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

/// Stable result schema for the narrow native claim-capability checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkItemClaimCapabilityResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

/// Privacy-safe decision vocabulary.  No authority tuple, owner, lease, or
/// capability metadata is projected in the result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkItemClaimCapabilityDecision {
    #[serde(rename = "minted")]
    Minted,
    #[serde(rename = "replayed")]
    Replayed,
    #[serde(rename = "verified")]
    Verified,
    #[serde(rename = "input_conflict")]
    InputConflict,
    #[serde(rename = "not_found")]
    NotFound,
    #[serde(rename = "unauthorized")]
    Unauthorized,
    #[serde(rename = "expired")]
    Expired,
    #[serde(rename = "stale")]
    Stale,
    #[serde(rename = "malformed")]
    Malformed,
    #[serde(rename = "retention_exhausted")]
    RetentionExhausted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceReservationRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceReservationRequestTargetKind {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "inventory_alias")]
    InventoryAlias,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceReservationRecordTargetKind {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "inventory_alias")]
    InventoryAlias,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
pub enum ResourceReservationResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceTargetSnapshotKind {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "inventory_alias")]
    InventoryAlias,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceReservationStatusRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceReservationHostSnapshotTargetKind {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "inventory_alias")]
    InventoryAlias,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceReservationStatusResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceHostUpdateRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceHostUpdateRequestTargetKind {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "inventory_alias")]
    InventoryAlias,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceHostUpdateResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
pub enum ResourceHostUpdateSnapshotTargetKind {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "inventory_alias")]
    InventoryAlias,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevelopmentLaneIntentSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
pub enum DevelopmentLaneCleanupCompleteRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevelopmentLaneCleanupCompleteResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
pub enum DevelopmentLaneFinishRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
pub enum DevelopmentLaneFinishResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
pub enum DevelopmentLaneHoldSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevelopmentLaneHoldHostTargetKind {
    #[serde(rename = "local")]
    Local,
    #[serde(rename = "inventory_alias")]
    InventoryAlias,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
pub enum DevelopmentLaneObserveRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevelopmentLaneObserveResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
pub enum DevelopmentLaneQueryRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevelopmentLaneQueryResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
pub enum DevelopmentLaneQuotaChargeSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevelopmentLaneQuotaPolicySchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevelopmentLaneQuotaUpdateRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevelopmentLaneQuotaUpdateResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevelopmentLaneRenewRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevelopmentLaneRenewResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
pub enum DevelopmentLaneReserveRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevelopmentLaneResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevelopmentLaneStatusRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevelopmentLaneStatusResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestContext {
    pub schema_version: RequestContextSchemaVersion,
    pub request_id: String,
    pub subject_id: String,
    pub tenant_id: String,
    pub agent_id: String,
    pub scopes: Vec<String>,
    pub audience: String,
    pub authentication_method: RequestContextAuthenticationMethod,
    pub policy_version: String,
    pub graph: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub placement_epoch: Option<u64>,
    pub trace_id: String,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationBatch {
    pub schema_version: MutationBatchSchemaVersion,
    pub batch_id: String,
    pub context: RequestContext,
    pub graph: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub placement_epoch: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_graph_version: Option<u64>,
    pub idempotency_key: String,
    pub operations: Vec<MutationOperation>,
    pub submitted_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationOperation {
    pub operation_id: String,
    pub domain: MutationOperationDomain,
    pub action: MutationOperationAction,
    pub target_id: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub payload_ref: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub payload_digest: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_version: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeEnvelope {
    pub schema_version: ChangeEnvelopeSchemaVersion,
    pub envelope_id: String,
    pub context: RequestContext,
    pub connector_kind: String,
    pub source_instance_id: String,
    pub source_object_id: String,
    pub source_version: String,
    pub operation: ChangeEnvelopeOperation,
    pub schema_id: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub event_time_ms: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub valid_time_ms: Option<u64>,
    pub observed_time_ms: u64,
    pub artifact_refs: Vec<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub payload_ref: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub payload_digest: Option<String>,
    pub access: SourceAccess,
    pub provenance_refs: Vec<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub checkpoint_ref: Option<String>,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceAccess {
    pub classification: SourceAccessClassification,
    pub read_scopes: Vec<String>,
    pub purpose_tags: Vec<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub retention_policy_id: Option<String>,
    pub legal_hold: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
pub struct Artifact {
    pub schema_version: ArtifactSchemaVersion,
    pub artifact_id: String,
    pub tenant_id: String,
    pub media_type: String,
    pub digest: String,
    pub byte_length: u64,
    pub content_ref: String,
    pub classification: ArtifactClassification,
    pub provenance_refs: Vec<String>,
    pub occurrence_ids: Vec<String>,
    pub rendition_ids: Vec<String>,
    pub segment_ids: Vec<String>,
    pub feature_ids: Vec<String>,
    pub derivation_ids: Vec<String>,
    pub loci: Vec<ArtifactLocus>,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLocus {
    pub kind: ArtifactLocusKind,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub start: Option<u64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub end: Option<u64>,
    pub selector: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeBatch {
    pub schema_version: KnowledgeBatchSchemaVersion,
    pub batch_id: String,
    pub context: RequestContext,
    pub fields: Vec<KnowledgeField>,
    pub encoding: KnowledgeBatchEncoding,
    pub rows: Vec<Vec<Value>>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub data_ref: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub cursor: Option<String>,
    pub end_of_stream: bool,
    pub source_refs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeField {
    pub name: String,
    pub data_type: KnowledgeFieldDataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnalyticsJob {
    pub schema_version: AnalyticsJobSchemaVersion,
    pub job_id: String,
    pub context: RequestContext,
    pub kind: String,
    pub state: AnalyticsJobState,
    pub input_artifact_refs: Vec<String>,
    pub parameters_digest: String,
    pub algorithm: String,
    pub algorithm_version: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub checkpoint_ref: Option<String>,
    pub output_artifact_refs: Vec<String>,
    pub progress: f64,
    pub attempt: u64,
    pub max_attempts: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub error: Option<AnalyticsError>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnalyticsError {
    pub code: String,
    pub retryable: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub detail_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceOutcome {
    pub schema_version: TraceOutcomeSchemaVersion,
    pub outcome_id: String,
    pub trace_id: String,
    pub context_id: String,
    pub operation: String,
    pub status: TraceOutcomeStatus,
    pub started_at_ms: u64,
    pub ended_at_ms: u64,
    pub input_artifact_refs: Vec<String>,
    pub output_artifact_refs: Vec<String>,
    pub metrics: BTreeMap<String, f64>,
    pub policy_decision: TraceOutcomePolicyDecision,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub error_code: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub error_detail_ref: Option<String>,
    pub evaluation_scores: BTreeMap<String, f64>,
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
pub struct PlacementRouteRequest {
    pub schema_version: PlacementRouteRequestSchemaVersion,
    pub tenant_ref: String,
    pub partition_ref: String,
    pub client_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// Native mint input.  The authenticated worker/session and current lease
/// authority are deliberately absent: only the engine can derive them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItemClaimCapabilityMintRequest {
    pub schema_version: WorkItemClaimCapabilityRequestSchemaVersion,
    pub work_item_id: String,
}

/// Native verify input.  The capability is an opaque bounded byte string; no
/// public DTO can reconstruct authority from owner/epoch/fence/attempt fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItemClaimCapabilityVerifyRequest {
    pub schema_version: WorkItemClaimCapabilityRequestSchemaVersion,
    pub work_item_id: String,
    #[serde(with = "serde_bytes")]
    pub capability: Vec<u8>,
}

/// Capability operation result.  Only mint/replay returns the opaque bytes;
/// verification returns a boolean and a privacy-safe decision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkItemClaimCapabilityResult {
    pub schema_version: WorkItemClaimCapabilityResultSchemaVersion,
    pub decision: WorkItemClaimCapabilityDecision,
    pub valid: bool,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub capability: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
pub struct ResourceRequirement {
    pub cpu_weight: u64,
    pub memory_mib: u64,
    pub disk_mib: u64,
    pub process_slots: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceTargetSnapshot {
    pub kind: ResourceTargetSnapshotKind,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub alias: Option<String>,
    pub capability_labels: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct ResourceReservationHostCapacitySnapshot {
    pub cpu_weight: u64,
    pub memory_mib: u64,
    pub disk_mib: u64,
    pub process_slots: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
pub struct ResourceCapacity {
    pub cpu_weight: u64,
    pub memory_mib: u64,
    pub disk_mib: u64,
    pub process_slots: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceHostUpdateCapacitySnapshot {
    pub cpu_weight: u64,
    pub memory_mib: u64,
    pub disk_mib: u64,
    pub process_slots: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DevelopmentLaneIntent {
    pub schema_version: DevelopmentLaneIntentSchemaVersion,
    pub tenant_ref: String,
    pub request_id: String,
    pub lane_id: String,
    pub repository_id: String,
    pub base_ref: String,
    pub base_sha: String,
    pub branch: String,
    pub host_target_kind: DevelopmentLaneIntentHostTargetKind,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub host_target_alias: Option<String>,
    /// Opaque selected host identity from the authorized WorkItem extension.
    /// Local targets must still carry this identity; `host_target_alias` is
    /// only the inventory-placement alias.
    pub host_ref: String,
    /// RMDD-27 native resource reservation bound to this WorkItem attempt.
    pub resource_reservation_id: String,
    pub workspace_ref: String,
    pub worktree_locator: String,
    pub owner_id: String,
    pub session_id: String,
    pub fairness_group: String,
    pub quota_policy_name: String,
    pub quota_policy_version: String,
    pub predicted_disk_bytes: u64,
    pub ttl_ms: u64,
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
pub struct DevelopmentLaneQueryRequest {
    pub schema_version: DevelopmentLaneQueryRequestSchemaVersion,
    pub tenant_ref: String,
    pub hold_id: String,
    pub now_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
#[serde(deny_unknown_fields)]
pub struct DevelopmentLaneQuotaUpdateResult {
    pub schema_version: DevelopmentLaneQuotaUpdateResultSchemaVersion,
    pub decision: DevelopmentLaneQuotaUpdateResultDecision,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub policy: Option<DevelopmentLaneQuotaPolicy>,
    pub counters: DevelopmentLaneQuotaCharge,
    pub policy_revision: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

// ── CasWorkItemMetadata (BUG-111) ───────────────────────────────────────
//
// The native, typed compare-and-set for a WorkItem's non-authority
// SCHEDULING METADATA (`checkpoint_id` / `metadata` / `prio_bucket`).
// `work_item_capability::validate_generic_method` (RMDD-29) unconditionally
// refuses a generic `CompareAndSetNodeFields` against any already-claimed
// WorkItem row, with no carve-out for a harmless field — so four
// agent-utilities `work_item.py` call sites (checkpoint / request-input /
// submit-input / set-priority) had no atomic native path at all. This
// closes that gap the same way `ClaimWorkItem`/`RenewWorkItemLease` do:
// one field, atomically compare-and-set inside the SAME durable WorkItem
// transaction, never a side path.
//
// The struct can never touch `status`/`lease_owner`/`lease_epoch`/
// `fencing_token`/`tenant` — there is no field on it that names them — so it
// can never manufacture or extend native lease authority; the guard's own
// purpose is preserved while giving these four sites a real, typed answer
// instead of an unconditional refusal.

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CasWorkItemMetadataRequestSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CasWorkItemMetadataResultSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

/// Three distinct outcomes — deliberately never collapsed to a bool.  A
/// falsy/empty return here would be indistinguishable at the call site from
/// "no such item" (AU-P0-3 fail-closed doctrine): a worker that cannot tell
/// "I lost a race" from "the item vanished" cannot safely decide whether to
/// retry, re-read, or abandon its lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CasWorkItemMetadataOutcome {
    /// The compare-and-set matched and the field was durably written.
    #[serde(rename = "applied")]
    Applied,
    /// The row exists but the observed condition (status / lease fence /
    /// the field's own expected prior value) no longer matches — a genuine
    /// lost race, not an error. The caller re-reads and decides whether to
    /// retry.
    #[serde(rename = "conflict")]
    Conflict,
    /// No WorkItem row `work_item_id` exists in this graph for `tenant_ref`.
    #[serde(rename = "not_found")]
    NotFound,
}

/// The live lease tuple a lease-holding caller (checkpoint / request-input)
/// fences on, exactly like `RenewWorkItemLease`. Omit `expected_lease` on
/// the request entirely for a lease-less external/scheduler caller
/// (submit-input / set-priority), which fences on `expected_status` (and,
/// for submit-input, the tenant + the field's own expected prior value)
/// instead.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CasWorkItemMetadataLeaseFence {
    pub worker_ref: String,
    pub lease_epoch: u64,
    pub fencing_token: u64,
}

/// Atomic single-field compare-and-set on one WorkItem's scheduling
/// metadata. Exactly one field-pair — (`expected_checkpoint_id`,
/// `set_checkpoint_id`) xor (`expected_metadata_msgpack`,
/// `set_metadata_msgpack`) xor (`expected_prio_bucket`, `set_prio_bucket`)
/// — must be present; the engine rejects a request naming zero or more than
/// one. `expected_status` must be non-empty: the caller always names the
/// exact status set the row must currently be in.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CasWorkItemMetadataRequest {
    pub schema_version: CasWorkItemMetadataRequestSchemaVersion,
    pub tenant_ref: String,
    pub work_item_id: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_lease: Option<CasWorkItemMetadataLeaseFence>,
    pub expected_status: Vec<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_checkpoint_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub set_checkpoint_id: Option<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_metadata_msgpack: Option<Vec<u8>>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub set_metadata_msgpack: Option<Vec<u8>>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub expected_prio_bucket: Option<i64>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub set_prio_bucket: Option<i64>,
    pub now_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CasWorkItemMetadataResult {
    pub schema_version: CasWorkItemMetadataResultSchemaVersion,
    pub outcome: CasWorkItemMetadataOutcome,
    pub work_item_id: String,
    pub changed_work_item_ids: Vec<String>,
}
