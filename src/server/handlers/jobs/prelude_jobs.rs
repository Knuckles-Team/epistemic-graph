//! Shared domain imports for private analytics-job modules.

pub(super) use medulla_compute::mining::association::{self, Algorithm};
pub(super) use medulla_jobs::model::{
    AlgoVersion, AnalyticsJob, Checkpoint, InputSnapshotHandle, JobPolicy, JobState,
};
pub(super) use medulla_jobs::store::{JobStore, SubmitSpec, TenantJobQuota, WorkerClaim};
pub(super) use medulla_jobs::{ReproducibilityManifest, ResultColumn, TypedJobResult};
pub(super) use medulla_types::contract::Nonce;
pub(super) use medulla_types::jobs::{JobKind, JobOp, JobResult, SubmitJobSpec};
