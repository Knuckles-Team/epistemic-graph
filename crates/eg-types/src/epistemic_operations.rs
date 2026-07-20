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
pub enum WorkItemState {
    #[serde(rename = "submitted")]
    Submitted,
    #[serde(rename = "ready")]
    Ready,
    #[serde(rename = "leased")]
    Leased,
    #[serde(rename = "running")]
    Running,
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
    pub state: WorkItemState,
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
