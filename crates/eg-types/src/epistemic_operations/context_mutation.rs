use super::common::deserialize_required_option;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum RequestContextSchemaVersion {
    #[serde(rename = "2")]
    V2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum MutationBatchSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ChangeEnvelopeSchemaVersion {
    #[serde(rename = "1")]
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ChangeEnvelopeOperation {
    #[serde(rename = "upsert")]
    Upsert,
    #[serde(rename = "delete")]
    Delete,
    #[serde(rename = "snapshot_complete")]
    SnapshotComplete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SourceAccess {
    pub classification: SourceAccessClassification,
    pub read_scopes: Vec<String>,
    pub purpose_tags: Vec<String>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub retention_policy_id: Option<String>,
    pub legal_hold: bool,
}
