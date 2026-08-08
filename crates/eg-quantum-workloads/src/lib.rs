//! `eg-quantum-workloads` — Q7 (epistemic hooks: "propose, don't commit", one real
//! end-to-end use case) + Q11 (domain workloads, benchmarked at realistic graph
//! sizes), per `plans/au-eg-program/program/quantum-native.md` and its
//! `quantum-external-providers.md` addendum §1.4.
//!
//! ## The one real end-to-end use case: QAOA Max-Cut over an epistemic-graph subgraph
//!
//! ```text
//! subgraph.rs   pull_candidate_subgraph(core, uql, max_qubits)
//!                 -> parse+execute UQL against a GraphCore snapshot, materialize the
//!                    induced subgraph (CandidateSubgraph: node ids + weighted edges)
//! circuit.rs    build_qaoa_program / optimize_qaoa_params
//!                 -> standard depth-p QAOA ansatz on eg-quantum-core's IR; a real
//!                    classical variational search (grid + coordinate-ascent
//!                    refinement) against EXACT statevector expectation values
//! run.rs        run_qaoa_maxcut_local_sim
//!                 -> bind the optimized angles into the MEASURED circuit, submit to
//!                    eg-quantum-sim's StateVectorSimulator (local sim is the
//!                    default; PROGRAM.md), producing the ONE official QuantumResult
//! propose.rs    MaxCutProposal::from_run + commit_maxcut_proposal
//!                 -> write back as a TYPED PROPOSAL (never a hard constraint) —
//!                    :QuantumJob + :Claim + :Evidence + SUPPORTS, through the SAME
//!                    convention eg-jobs::claim already established, so
//!                    eg-epistemic's TMS/confidence machinery governs belief
//! ```
//!
//! ## The rule this crate exists to enforce (read `propose.rs` first)
//!
//! QAOA is variational and its result is NEVER `exact: true` as a Max-Cut ANSWER —
//! independent of what `eg-quantum-sim`'s own noiseless-simulation `QuantumResult`
//! flag reports (it reports `true` for a local run with no noise model, which is
//! correct at ITS level and a genuine loophole this crate closes at the DOMAIN
//! level; see `propose.rs`'s module doc and its acceptance test
//! `qaoa_result_cannot_become_a_hard_constraint_even_when_backend_reports_exact`).
//!
//! ## Staging note (do not extend this into a job-plane abstraction here)
//!
//! This crate is the addendum's §1.2 "zero core change" stage-1 tool path — a
//! synchronous, in-process pipeline a caller invokes directly. The sibling
//! `w6-quantum-q5q6-jobplane` lane owns `Op::SubmitQuantum`/`Op::JoinQuantumResults`
//! on the durable job plane (`eg-jobs`); this crate's `run_qaoa_maxcut`/
//! `plan_maxcut_proposal` are written so that lane can later host them behind an
//! async job without this crate inventing a competing queue/poll/cancel surface of
//! its own.

pub mod circuit;
pub mod demo;
pub mod propose;
pub mod run;
pub mod subgraph;

pub use circuit::{
    brute_force_max_cut, build_qaoa_program, cut_value_of_bitstring, expected_cut_value,
    optimize_qaoa_params,
};
pub use propose::{
    claim_node_id, commit_maxcut_proposal, evidence_node_id, plan_maxcut_proposal,
    quantum_job_node_id, MaxCutProposal, ProposalCommitOutcome, ProposalWritePlan, ProposeError,
    CLAIM_VALIDATION_STATE,
};
pub use run::{
    default_backend_id, run_qaoa_maxcut, run_qaoa_maxcut_local_sim, MaxCutRun, QaoaConfig, RunError,
};
pub use subgraph::{
    materialize_induced_subgraph, pull_candidate_subgraph, CandidateSubgraph, SubgraphError,
};
