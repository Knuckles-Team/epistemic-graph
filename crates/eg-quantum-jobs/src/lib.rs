//! `eg-quantum-jobs` — quantum as a durable job, not a synchronous `Op` (lanes
//! Q5/Q6, register `D-QN-1` family). See
//! `plans/au-eg-program/program/quantum-native.md` and its
//! `quantum-external-providers.md` addendum for the full design; this crate
//! implements the addendum's §1.1/§1.2 exactly, no more:
//!
//! > "★ THE CENTRAL DESIGN DECISION — quantum must be an ASYNC job, not a
//! > synchronous Op. Real QPU queues are minutes to hours... ★★ CRITICAL
//! > COORDINATION — REUSE, DO NOT FORK, THE DURABLE-EXECUTION PLANE... A quantum job
//! > is an `eg-jobs` job with a quantum payload."
//!
//! ## What this crate is
//!
//! [`job::submit_quantum_job`] / [`job::claim_and_run_quantum_job`] /
//! [`job::join_quantum_result_rows`] wrap the REAL `eg_jobs::JobStore` — its durable
//! `AnalyticsJob` state machine, fenced-lease `claim_next`, and `TypedJobResult`
//! staging — with a quantum-shaped payload ([`job::QuantumJobPayload`]: a
//! [`eg_quantum_core::ir::QuantumProgram`] built from a candidate id list + an
//! induced-subgraph edge set, see [`circuit`]). No new durability primitive, no
//! parallel job table, no wire `Method`/`Op` — this is exactly "an `eg-jobs` job with
//! a quantum payload," nothing beside it.
//!
//! [`numeric_bridge`] is lane Q6: pure functions turning a `QuantumResult`'s
//! `Outcome` into `eg-numeric` (`ndarray`) arrays — per-qubit marginal probabilities,
//! the full bitstring distribution, and a batched expectation-value array — plus
//! [`numeric_bridge::run_batch`], the "many small circuits" batch entry point.
//!
//! [`rowset_bridge`] (feature `federation-bridge`, off by default) is staging-
//! discipline step 2: it wraps [`job::join_quantum_result_rows`] as an
//! `eg_plan::federation::ForeignSource` closure, so a submitted-and-completed
//! quantum job's `(id, score)` rows compose into a UQL plan through the EXISTING
//! federation seam (`crates/eg-plan/src/federation.rs`) with ZERO change to
//! `eg_types::wire::Op`.
//!
//! ## What this crate deliberately does NOT do
//!
//! Per the addendum's staging discipline ("Do not skip to (3)... Deliver the native
//! Op only if steps 1-2 prove the pattern"), this crate does NOT add
//! `Op::SubmitQuantum` / `Op::JoinQuantumResults` to `eg_types::wire::Op`, does NOT
//! touch `crates/eg-plan/src/exec.rs`/`dag_exec.rs`/`runtime.rs`'s executor match
//! arms, and does NOT fork or duplicate any part of `eg-jobs`' durable state machine
//! — `JobStore::submit`/`claim_next`/`checkpoint_fenced`/`stage_result_fenced`/
//! `complete_publication_fenced` are called exactly as any other analytics-job
//! producer calls them. A future lane may promote this to a native `Op` once this
//! crate's step-1/step-2 pattern has carried a real production workload; that lane
//! is out of THIS crate's scope.
//!
//! Everything here is default-off (the crate itself is not linked by the root
//! `epistemic-graph` package's `full`/`node`/`cluster` builds unless the facade's
//! `quantum-jobs`/`quantum-jobs-federation` features are explicitly turned on,
//! mirroring the sibling `quantum`/`quantum-sim` posture) and adds zero new
//! crates.io dependencies — every dependency is an existing workspace member
//! (`eg-jobs`, `eg-quantum-core`, `eg-quantum-sim`, `eg-numeric`, optionally
//! `eg-plan`) already vendored for the sibling lanes.

pub mod circuit;
pub mod job;
pub mod numeric_bridge;

#[cfg(feature = "federation-bridge")]
pub mod rowset_bridge;

pub use job::{
    claim_and_run_quantum_job, consistency_scores, join_quantum_result_rows, submit_quantum_job,
    BackendSet, QuantumJobError, QuantumJobPayload, GHZ_RANKING_ALGORITHM, QUANTUM_ALGO_FAMILY,
};
pub use numeric_bridge::{
    bitstring_distribution, expectation_array, marginal_probabilities, run_batch,
    BitstringDistribution,
};
