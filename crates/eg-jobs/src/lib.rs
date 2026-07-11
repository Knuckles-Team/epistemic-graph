//! `eg-jobs` — the durable analytics-job plane (CONCEPT:INT-P2-1).
//!
//! The engine's mining/analytics surface (`eg-compute::mining`, 18 families) runs
//! SYNCHRONOUSLY within one request: submit a big clustering/association/anomaly run
//! and the caller's connection blocks until it finishes, with no snapshot/status/
//! resume and no result lineage beyond the single `:Claim`/`:Evidence` pair the
//! synchronous `as_claim` writeback (`src/server/handlers/mining.rs`) creates.
//!
//! This crate adds the missing durable JOB layer ON TOP of that (it does not
//! replace or reimplement any mining kernel): an [`AnalyticsJob`] record + state
//! machine (`submitted -> running(checkpoint, progress) ->
//! succeeded(result_ref) | failed | cancelled`), persisted durably in its own
//! `jobs.redb` (mirrors `eg-tsdb`/`eg-kvcache`'s own-file pattern), carrying full
//! tenant/actor/purpose + an IMMUTABLE input-snapshot HANDLE (a graph name + OCC
//! version, never a copy of rows) + policy/quota/priority/deadline + algorithm/
//! params/code/env version lineage + a retry policy + checkpoint/progress.
//!
//! Determinism + idempotency (CONCEPT:INT-P2-1): [`AnalyticsJob::result_ref`] is a
//! pure hash of `(input_snapshot, algo)` — NOT of `job_id` — so re-running the same
//! computation (a retry, or two independent submissions) converges on the SAME
//! `result_ref`; [`claim::commit_result_claim`] uses that as the claim's
//! deterministic node id, so committing it twice is a no-op (verified by
//! `GraphCore::has_node` before any write).
//!
//! ## Crate shape
//!
//! A leaf crate ALONG the DAG: `eg-types` (nothing from it directly today, but the
//! facade's wire `JobOp` lives there per the `RbacAdminOp` precedent) + `eg-core`
//! (the `GraphCore` write primitive `claim.rs` needs — the SAME surface
//! `eg-epistemic` reads and `mining.rs` writes through). NOT a dependency of the
//! main `epistemic-graph` package's default build; see the root `Cargo.toml`'s
//! `members` comment and the opt-in `jobs` facade feature.
//!
//! ## What lives where
//!
//! * [`model`] — the `AnalyticsJob` record + every value type it carries
//!   (`InputSnapshotHandle`, `AlgoVersion`, `JobPolicy`, `RetryPolicy`, `Checkpoint`,
//!   `JobState`).
//! * [`store`] — [`store::JobStore`], the durable redb-backed state machine: guarded
//!   `submit`/`start_running`/`checkpoint`/`succeed`/`fail`/`request_cancel`/
//!   `mark_cancelled`/`resume`, plus the `mark_result_committed` idempotency ledger.
//! * [`claim`] — [`claim::commit_result_claim`], turning a `Succeeded` job into a
//!   provenance'd `:Claim`/`:Evidence` pair in a live `GraphCore`.
//!
//! The facade wires this crate's `submit`/`get`/`request_cancel`/`resume` behind
//! ONE protocol surface (`Method::AnalyticsJob { op: JobOp }`,
//! `src/server/handlers/jobs.rs`) that spawns the actual analytics work
//! asynchronously off the request. See that module's docs for the reference job
//! kind (association-rule mining) and what remains a Wave-2 follow-up (more mining
//! families as job kinds; an AU-side feature/model/experiment registry).

pub mod claim;
pub mod model;
pub mod store;

pub use claim::{commit_result_claim, CalibrationInput, ClaimCommitOutcome};
pub use model::{
    compute_result_ref, digest_params, AlgoVersion, AnalyticsJob, Checkpoint, InputSnapshotHandle,
    JobId, JobPolicy, JobState, RetryPolicy,
};
pub use store::{JobError, JobStore, SubmitSpec};
