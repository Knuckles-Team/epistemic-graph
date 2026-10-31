use super::common::deserialize_required_option;
use super::context_mutation::RequestContext;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

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
