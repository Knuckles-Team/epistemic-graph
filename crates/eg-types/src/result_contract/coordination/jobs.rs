//! Analytics job result bodies of the `coordination` domain.
//!
//! The engine's durable job record lives in `eg-jobs`, above this crate. These are its
//! wire projections: a response decodes the record's JSON into them strictly
//! (`deny_unknown_fields`), so a field added to the record without its projection is a
//! refused response rather than an undeclared one. The executor payload
//! (`input_payload`) is durable implementation detail, handed only to the claiming
//! worker.

use serde::{Deserialize, Serialize};

use crate::jobs::JobResult;

/// The graph snapshot a job reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct JobInputSnapshot {
    pub graph: String,
    pub version: u64,
    pub dataset_ref: String,
    pub content_digest: String,
}

/// A job's resource budget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct JobResourceBudget {
    pub cpu_ms: Option<u64>,
    pub memory_bytes: Option<u64>,
    pub io_bytes: Option<u64>,
    pub output_bytes: Option<u64>,
}

/// Where a job may run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct JobPlacement {
    pub pool: String,
    pub region: String,
    pub required_capabilities: Vec<String>,
}

/// The authority and limits a job runs under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct JobPolicy {
    pub tenant: String,
    pub actor: String,
    pub purpose: String,
    pub priority: i32,
    pub quota_cpu_ms: Option<u64>,
    pub deadline_unix_ms: Option<i64>,
    pub policy_fingerprint: String,
    pub resources: JobResourceBudget,
    pub placement: JobPlacement,
}

/// The full algorithm lineage a job's result is bound to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct JobAlgoVersion {
    pub family: String,
    pub algorithm: String,
    pub params_digest: String,
    pub code_version: String,
    pub env_version: String,
}

/// A job's retry policy and attempt counter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct JobRetryPolicy {
    pub max_attempts: u32,
    pub backoff_ms: u64,
    pub attempts_made: u32,
}

/// The last progress checkpoint a job wrote.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct JobCheckpoint {
    pub progress: f64,
    pub stage: String,
    /// An opaque external state reference, as bytes.
    pub state_blob: Option<Vec<u8>>,
    pub updated_at_ms: i64,
}

/// A job's lifecycle state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum JobState {
    Submitted,
    Running {
        checkpoint: JobCheckpoint,
    },
    /// The typed result is durable; its evidence-bearing claims are not yet.
    Publishing {
        result_ref: String,
        checkpoint: JobCheckpoint,
    },
    Succeeded {
        result_ref: String,
        checkpoint: JobCheckpoint,
    },
    Failed {
        reason: String,
        checkpoint: Option<JobCheckpoint>,
    },
    Cancelled {
        checkpoint: Option<JobCheckpoint>,
    },
}

/// Fenced ownership of one job attempt by a worker slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct JobWorkerLease {
    pub worker_ref: String,
    pub epoch: u64,
    pub acquired_at_ms: i64,
    pub expires_at_ms: i64,
}

/// The durable analytics-job record, as a caller sees it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AnalyticsJobRecord {
    pub job_id: String,
    pub input_snapshot: JobInputSnapshot,
    pub policy: JobPolicy,
    pub algo: JobAlgoVersion,
    pub retry: JobRetryPolicy,
    pub state: JobState,
    pub cancel_requested: bool,
    pub lease_epoch: u64,
    pub lease: Option<JobWorkerLease>,
    pub last_worker_ref: String,
    pub not_before_ms: i64,
    pub output: Option<JobResult>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// The job a worker claimed: its record plus the opaque executor payload the worker
/// runs, which only the claiming worker ever receives.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClaimedAnalyticsJob {
    #[serde(flatten)]
    pub record: AnalyticsJobRecord,
    pub input_payload: Option<Vec<u8>>,
}

/// `AnalyticsJob` op `WorkerClaim`: the claimed job and the lease fencing it.
#[derive(Debug, Clone, Serialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WorkerJobClaim {
    pub job: ClaimedAnalyticsJob,
    pub lease: JobWorkerLease,
}
