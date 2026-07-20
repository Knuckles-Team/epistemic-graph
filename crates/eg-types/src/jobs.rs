//! `JobOp` — the wire op `Method::AnalyticsJob` carries (CONCEPT:INT-P2-1), gated
//! `jobs`. Mirrors `acl::RbacAdminOp`'s "one Method variant, one internal op enum"
//! shape so the durable analytics-job plane's caller and fenced remote-worker
//! surfaces cost the wire protocol exactly ONE new `Method` variant.
//!
//! Pure serde — no dependency on `eg-core`/`eg-jobs` (which sit ABOVE this crate in
//! the DAG). The facade (`src/server/handlers/jobs.rs`) is the one place that
//! translates a `SubmitJobSpec` into an `eg_jobs::store::SubmitSpec` + runs the
//! actual analytics work; this module only shapes what crosses the wire.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The analytics-job control-plane operation (CONCEPT:INT-P2-1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobOp {
    /// Submit a new job. Runs asynchronously off the request — the response carries
    /// the freshly durable `Submitted` job record (including its server-issued id),
    /// not the eventual result.
    Submit(SubmitJobSpec),
    /// Fetch a job's current durable state (including checkpoint/progress).
    Status { job_id: String },
    /// Cooperatively cancel a job (immediate if not yet started; the running
    /// executor observes the flag and stops otherwise).
    Cancel { job_id: String },
    /// Resume a job from its last checkpoint — a `Failed` job with retries
    /// remaining, or a `Running` job orphaned by a crashed/restarted process.
    Resume { job_id: String },
    /// Atomically claim the highest-priority eligible job. `worker_instance`
    /// distinguishes concurrent slots for one authenticated principal; the server
    /// hashes it with the verified identity before persistence.
    WorkerClaim {
        #[serde(default)]
        worker_instance: String,
        #[serde(default)]
        capabilities: Vec<String>,
        #[serde(default = "default_lease_ms")]
        lease_ms: u64,
    },
    /// Renew the exact lease epoch owned by this authenticated worker slot.
    WorkerRenew {
        job_id: String,
        worker_instance: String,
        lease_epoch: u64,
        #[serde(default = "default_lease_ms")]
        lease_ms: u64,
    },
    /// Persist a fenced progress checkpoint. `state_ref`, when present, must be an
    /// opaque external state reference rather than inline source/user content.
    WorkerCheckpoint {
        job_id: String,
        worker_instance: String,
        lease_epoch: u64,
        progress: f64,
        stage: String,
        #[serde(default)]
        state_ref: Option<String>,
    },
    /// Atomically stage the complete typed result before publication.
    WorkerStage {
        job_id: String,
        worker_instance: String,
        lease_epoch: u64,
        result: JobResult,
    },
    /// Commit evidence-bearing claims for a staged result and then mark success.
    WorkerPublish {
        job_id: String,
        worker_instance: String,
        lease_epoch: u64,
    },
    /// Release a failed compute attempt through server-owned retry/backoff policy.
    WorkerFail {
        job_id: String,
        worker_instance: String,
        lease_epoch: u64,
        reason_code: String,
    },
    /// Confirm that a worker cooperatively stopped after a cancellation request.
    WorkerCancel {
        job_id: String,
        worker_instance: String,
        lease_epoch: u64,
    },
}

fn default_lease_ms() -> u64 {
    60_000
}

/// Portable typed result column for remote analytics workers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResultColumn {
    pub name: String,
    pub logical_type: String,
    pub nullable: bool,
}

/// Reproducibility manifest bound server-side to the leased job lineage.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobReproducibilityManifest {
    pub input_dataset_ref: String,
    pub input_content_digest: String,
    pub input_snapshot_version: u64,
    pub algorithm_ref: String,
    pub params_digest: String,
    pub implementation_version: String,
    pub environment_version: String,
    pub policy_fingerprint: String,
}

/// KnowledgeBatch-shaped result currency accepted by [`JobOp::WorkerStage`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResult {
    pub schema_version: u16,
    pub dataset_ref: String,
    pub content_digest: String,
    pub schema: Vec<JobResultColumn>,
    pub rows: Vec<BTreeMap<String, serde_json::Value>>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub counterexample_refs: Vec<String>,
    #[serde(default)]
    pub uncertainty: Option<f64>,
    #[serde(default)]
    pub calibration: Option<(f64, f64)>,
    pub reproducibility: JobReproducibilityManifest,
}

/// Wire spec for [`JobOp::Submit`] (CONCEPT:INT-P2-1). `graph` is BOTH the tenancy
/// anchor (where the eventual result claim lands) and the input-snapshot handle's
/// graph name — the facade stamps in the graph's live `GraphCore::version()` at
/// submit time, so the snapshot handle is never client-supplied (a client cannot
/// forge which graph-version a job actually ran against).
///
/// Worker kernels reuse the same durable lease/publication state machine. The jobs
/// feature provides association-rule mining; the program-optimization feature adds
/// the graph-native, governed LM-program compiler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitJobSpec {
    /// Target graph: where the result claim lands, and whose `version()` at submit
    /// time becomes the job's immutable input-snapshot handle.
    pub graph: String,
    #[serde(default)]
    pub tenant: String,
    #[serde(default)]
    pub actor: String,
    #[serde(default)]
    pub purpose: String,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub deadline_unix_ms: Option<i64>,
    #[serde(default)]
    pub quota_cpu_ms: Option<u64>,
    #[serde(default)]
    pub memory_bytes: Option<u64>,
    #[serde(default)]
    pub io_bytes: Option<u64>,
    #[serde(default)]
    pub output_bytes: Option<u64>,
    #[serde(default)]
    pub worker_pool: String,
    #[serde(default)]
    pub worker_region: String,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    /// Retry cap (including the first attempt); `0`/`1` ⇒ no retry. Default `1`.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default)]
    pub backoff_ms: u64,
    /// The job kind + its parameters. `params_digest` lineage is derived from
    /// `kind`'s own fields by the facade — a client never supplies it directly.
    pub kind: JobKind,
}

fn default_max_attempts() -> u32 {
    1
}

/// Which analytics computation a job runs (CONCEPT:INT-P2-1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobKind {
    /// Association-rule mining over explicit transactions (mirrors
    /// `Method::MineAssociate`'s non-graph-derived path).
    MineAssociate {
        transactions: Vec<Vec<String>>,
        #[serde(default = "default_min_support")]
        min_support: f64,
        #[serde(default = "default_min_confidence")]
        min_confidence: f64,
        /// `"apriori" | "fpgrowth" | "eclat"`; unrecognized ⇒ facade error.
        #[serde(default = "default_algorithm")]
        algorithm: String,
    },
    /// Governed graph-native program compilation. The nested MessagePack is a
    /// versioned `eg_program::OptimizationRequest`; `eg-types` remains the bottom
    /// of the crate DAG and the server decodes/rebinds it at the verified boundary.
    #[cfg(feature = "program-optimization")]
    ProgramOptimize {
        #[serde(with = "serde_bytes")]
        request_msgpack: Vec<u8>,
    },
}

fn default_min_support() -> f64 {
    0.1
}
fn default_min_confidence() -> f64 {
    0.5
}
fn default_algorithm() -> String {
    "fpgrowth".to_string()
}
