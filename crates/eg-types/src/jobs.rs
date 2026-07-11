//! `JobOp` — the wire op `Method::AnalyticsJob` carries (CONCEPT:INT-P2-1), gated
//! `jobs`. Mirrors `acl::RbacAdminOp`'s "one Method variant, one internal op enum"
//! shape so the durable analytics-job plane's submit/status/cancel/resume surface
//! costs the wire protocol exactly ONE new `Method` variant rather than four.
//!
//! Pure serde — no dependency on `eg-core`/`eg-jobs` (which sit ABOVE this crate in
//! the DAG). The facade (`src/server/handlers/jobs.rs`) is the one place that
//! translates a `SubmitJobSpec` into an `eg_jobs::store::SubmitSpec` + runs the
//! actual analytics work; this module only shapes what crosses the wire.

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
}

/// Wire spec for [`JobOp::Submit`] (CONCEPT:INT-P2-1). `graph` is BOTH the tenancy
/// anchor (where the eventual result claim lands) and the input-snapshot handle's
/// graph name — the facade stamps in the graph's live `GraphCore::version()` at
/// submit time, so the snapshot handle is never client-supplied (a client cannot
/// forge which graph-version a job actually ran against).
///
/// V1 ships ONE reference job kind — association-rule mining over EXPLICIT
/// transactions (`eg_compute::mining::association`, the same kernel the
/// synchronous `Method::MineAssociate` writeback uses) — proving the full job-plane
/// mechanics end to end. Graph-derived transaction sources and the other 17 mining
/// families are a Wave-2 follow-up (see `src/server/handlers/jobs.rs` docs); adding
/// them is new `kind` variants here, not a change to the state machine.
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

/// Which analytics computation a job runs (CONCEPT:INT-P2-1). One variant today
/// (`MineAssociate`); more mining families land as additional variants (Wave-2).
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
