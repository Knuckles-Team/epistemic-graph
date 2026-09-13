//! Shared domain imports for private analytics-job modules.

pub(super) use eg_compute::mining::association::{self, Algorithm};
pub(super) use eg_core::graph::GraphCore;
pub(super) use eg_jobs::model::{
    AlgoVersion, AnalyticsJob, Checkpoint, InputSnapshotHandle, JobPolicy, JobState,
};
pub(super) use eg_jobs::store::{JobStore, SubmitSpec, TenantJobQuota, WorkerClaim};
pub(super) use eg_jobs::{ReproducibilityManifest, ResultColumn, TypedJobResult};
pub(super) use eg_types::contract::Nonce;
pub(super) use eg_types::jobs::{JobKind, JobOp, JobResult, SubmitJobSpec};
