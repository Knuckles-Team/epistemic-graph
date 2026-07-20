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
//! machine (`submitted -> running(checkpoint, fenced lease) ->
//! publishing(typed result) -> succeeded | failed | cancelled`). In clustered mode
//! its transitions are Raft-ordered and `jobs.redb` is a deterministic local
//! projection; single-node mode commits directly to that same store. Records carry
//! pseudonymized policy identity + an immutable input dataset/content digest and OCC
//! version + policy/quota/priority/deadline + algorithm/
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
//! A leaf crate ALONG the DAG: `eg-types` (wire and mutation currency) + `eg-core`
//! (the `GraphCore` write primitive `claim.rs` needs — the SAME surface
//! `eg-epistemic` reads and `mining.rs` writes through). NOT a dependency of the
//! facade's self-contained `full` contract includes the `jobs` feature.
//!
//! ## What lives where
//!
//! * [`model`] — the `AnalyticsJob` record + every value type it carries
//!   (`InputSnapshotHandle`, `AlgoVersion`, `JobPolicy`, `RetryPolicy`, `Checkpoint`,
//!   `JobState`).
//! * [`store`] — [`store::JobStore`], the durable redb-backed state machine with
//!   atomic claim, renewable leases, epoch fencing, quotas, retry backoff,
//!   typed-result staging and publication barriers, plus the result idempotency ledger
//!   AND (daemon-consolidation design Phase 3) the generalized
//!   `claim_idempotency`/`idempotency_claimed_by` ledger + the `JobIntent` registry
//!   (`register_intent`/`get_intent`/`list_intents`/`due_intents`/
//!   `record_intent_tick`).
//! * [`claim`] — [`claim::commit_result_claim`], turning a `Succeeded` job into a
//!   provenance'd `:Claim`/`:Evidence` pair in a live `GraphCore`.
//! * [`intent`] — [`intent::JobIntent`] + [`intent::Trigger`] (CONCEPT:INT-P2-1,
//!   daemon-consolidation design Phase 3, `reports/daemon-consolidation-design.md`):
//!   a job DECLARED with a schedule (cron/interval/manual) rather than driven by an
//!   external caller, additive to `AnalyticsJob` and persisted in the SAME
//!   `jobs.redb`. [`intent::cold_offload_intent`] is the proof-of-concept: it
//!   expresses the engine's own off-by-default cold-tenant idle-offload sweep as a
//!   `JobIntent` WITHOUT changing what actually drives that sweep today.
//!
//! The facade wires caller control and verified remote-worker operations behind one
//! protocol surface (`Method::AnalyticsJob { op: JobOp }`). There is no elected
//! application coordinator or pod-number dependency: the current Raft leader orders
//! transitions and any caught-up replica can take over after leader failure.

pub mod claim;
pub mod intent;
pub mod model;
pub mod result;
pub mod store;

pub use claim::{
    commit_result_claim, plan_result_claim, CalibrationInput, ClaimCommitOutcome, ClaimWritePlan,
};
pub use intent::{cold_offload_intent, JobIntent, Trigger};
pub use model::{
    compute_result_ref, digest_params, AlgoVersion, AnalyticsJob, Checkpoint, InputSnapshotHandle,
    JobId, JobPlacement, JobPolicy, JobState, ResourceBudget, RetryPolicy, WorkerLease,
};
pub use result::{ReproducibilityManifest, ResultColumn, TypedJobResult};
pub use store::{JobError, JobStore, SubmitSpec, TenantJobQuota, WorkerClaim};
