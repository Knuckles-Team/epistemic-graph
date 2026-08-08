//! Step 4 of the Q7/Q11 pipeline — the write-back, and the ONE module in this crate
//! that carries the load-bearing correctness rule
//! (`plans/au-eg-program/program/quantum-native.md`'s Q0 contract,
//! `quantum-external-providers.md` §0/§1.4):
//!
//! > Only `exact: true` results may be treated as hard constraints. Everything else
//! > is a PROPOSAL — quantum subroutines propose, the classical TMS/confidence/
//! > causal machinery validates, and commits go through the normal ChangeEnvelope /
//! > MutationBatch path. QAOA is variational and is NEVER `exact: true`.
//!
//! ## Why `QuantumResult::is_exact()` is NOT trusted here, even though it could be
//!
//! `eg-quantum-sim`'s `StateVectorSimulator::execute` returns `QuantumResult::new_exact`
//! whenever no `noise_model_id` was requested — INCLUDING for our measured QAOA
//! circuit, which requests no noise model. So `quantum_result.is_exact()` for a
//! genuine local-sim QAOA run is `true` today (verified in `run.rs`'s own test). That
//! flag is correct AT ITS OWN LEVEL: it certifies the SIMULATION was noiseless
//! (no hardware error, no approximation in applying the unitary), which is a true
//! fact about `eg-quantum-sim`'s execution. It says NOTHING about whether the
//! resulting bitstring is a correct — let alone optimal — Max-Cut partition: QAOA is
//! a HEURISTIC ansatz whose classically-searched angles (`circuit.rs`) generally do
//! not reach the true optimum even run on a perfect noiseless simulator with
//! infinite shots. Conflating "the quantum simulation was exact" with "the
//! algorithm's answer is exact" is precisely the bug this module exists to prevent.
//!
//! So [`MaxCutProposal`] is constructed ONLY via [`QuantumResult::into_proposal`]
//! (the unconditional, always-available conversion `eg-quantum-core`'s own doc calls
//! "the ONLY thing quantum subroutines produce that classical validation machinery
//! consumes directly") — **never** via `into_hard_constraint`. There is no function
//! anywhere in this crate, public or private, that accepts a `HardConstraint` and
//! writes it to the graph. A caller who separately, manually, calls
//! `quantum_result.into_hard_constraint()` on an exact QAOA result (the type system
//! permits it, since the flag really is `true`) still cannot pass that
//! `HardConstraint` to [`commit_maxcut_proposal`] — its signature takes a
//! [`MaxCutProposal`], and the only public constructor for that type discards
//! exactness information entirely (it stores only `Proposal`-shaped data: a
//! `QuantumResult` clone plus the domain-derived partition, never a `HardConstraint`).
//!
//! ## Write shape (mirrors `eg-jobs::claim` — see that module's docs)
//!
//! One `:QuantumJob` node holds the FULL Q0 result metadata (`backend_id`,
//! `formalism`, `seed`, `shots`, `circuit_hash`, `exact` — the BACKEND's flag,
//! recorded honestly for observability/Q9, but never load-bearing here —
//! `noise_model_id`, `fidelity_hint`, `wall_time_ms`, `peak_memory_bytes`) plus the
//! QAOA-specific run metadata (`p`, `optimized bindings`, `search_evaluations`).
//! One `:Claim` node carries the domain proposal itself (`partition`, `cut_value`,
//! `approx_ratio`) with `confidence` seeded from the run quality (the SAME
//! "quality score seeds claim confidence" convention `eg-jobs`/`mining.rs` use) and
//! `validation_state: "unvalidated"` — asserted, not yet independently validated.
//! One `:Evidence` node justifies it, linked `Evidence --SUPPORTS--> Claim`
//! (`SUPPORTS` is in `eg_epistemic::classify_relationship`'s whitelist, so
//! `BeliefGraph`/`propagate_confidence` treats this exactly like any other mined
//! finding — Bayesian-UPDATED by corroborating/contradicting evidence, never taken
//! as ground truth on its own). `Claim --GENERATED_BY--> QuantumJob` is a neutral
//! PROV-style link (NOT in the whitelist, so it never itself feeds belief).
//!
//! **What this module never does:** it never writes to, or even reads for writing,
//! any property of the ORIGINAL candidate subgraph's own nodes/edges — the anti-
//! pattern this whole lane exists to avoid wrote `confidence: approx_ratio` directly
//! onto the pre-existing graph EDGES. Every node/edge this module creates is NEW
//! (`:QuantumJob`/`:Claim`/`:Evidence` + their own provenance edges); the candidate
//! subgraph's own data is read-only input, proven untouched in `tests/end_to_end.rs`.

use std::collections::BTreeMap;

use eg_core::graph::GraphCore;
use eg_quantum_core::result::{Formalism, Proposal, QuantumResult};
use eg_types::protocol::Method;

use crate::run::MaxCutRun;
use crate::subgraph::CandidateSubgraph;

/// `validation_state` seeded on every fresh QAOA-committed `:Claim`/`:Evidence` —
/// same convention as `eg-jobs::claim::CLAIM_VALIDATION_STATE`.
pub const CLAIM_VALIDATION_STATE: &str = "unvalidated";

/// A QAOA Max-Cut result, ALWAYS in PROPOSAL form. The ONLY public constructor
/// ([`MaxCutProposal::from_run`]) takes ownership of a [`MaxCutRun`] and calls
/// `QuantumResult::into_proposal()` UNCONDITIONALLY — it does not branch on
/// `is_exact()`, does not expose it as a choice, and stores no `HardConstraint`
/// anywhere in this type. There is deliberately no `into_hard_constraint`-shaped
/// method on this type: promoting a QAOA proposal to a hard fact is not a
/// capability this crate offers under any argument or flag.
#[derive(Debug, Clone)]
pub struct MaxCutProposal {
    proposal: Proposal,
    pub node_ids: Vec<String>,
    pub partition: Vec<bool>,
    pub cut_value: f64,
    pub mean_sampled_cut_value: f64,
    pub approx_ratio: Option<f64>,
    pub qaoa_p: usize,
    pub optimized_bindings: BTreeMap<String, f64>,
    pub search_evaluations: usize,
}

impl MaxCutProposal {
    /// The ONLY way to build a [`MaxCutProposal`]. Always routes the run's
    /// `QuantumResult` through `into_proposal()` — see this module's doc for why
    /// that is unconditional, not a decision made per-call.
    pub fn from_run(subgraph: &CandidateSubgraph, run: MaxCutRun) -> Self {
        MaxCutProposal {
            proposal: run.quantum_result.into_proposal(),
            node_ids: subgraph.node_ids.clone(),
            partition: run.best_partition,
            cut_value: run.best_cut_value,
            mean_sampled_cut_value: run.mean_sampled_cut_value,
            approx_ratio: run.approx_ratio,
            qaoa_p: run.qaoa_p,
            optimized_bindings: run.optimized_bindings,
            search_evaluations: run.search_evaluations,
        }
    }

    /// Read-only access to the underlying [`QuantumResult`]'s data — for
    /// observability/logging (Q9) only. Returns `&QuantumResult`, never a
    /// `HardConstraint`; there is no method on [`Proposal`] that yields one either
    /// (see `eg_quantum_core::result` — `HardConstraint` has exactly one producer,
    /// `QuantumResult::into_hard_constraint`, and `Proposal` does not wrap one).
    pub fn quantum_result(&self) -> &QuantumResult {
        self.proposal.result()
    }
}

/// Deterministic node ids — one `:QuantumJob`/`:Claim`/`:Evidence` triple per
/// circuit hash, so re-running the SAME optimized circuit (same subgraph, same
/// bindings, same backend) converges on the SAME claim instead of accumulating
/// duplicates, mirroring `eg-jobs::claim`'s `result_ref`-keyed determinism.
pub fn quantum_job_node_id(circuit_hash: &str) -> String {
    format!("quantumjob:maxcut:{circuit_hash}")
}
pub fn claim_node_id(circuit_hash: &str) -> String {
    format!("quantumclaim:maxcut:{circuit_hash}")
}
pub fn evidence_node_id(circuit_hash: &str) -> String {
    format!("quantumevidence:maxcut:{circuit_hash}")
}

/// Deterministic graph write-set for one `MaxCutProposal` — the SAME
/// "lower to `Method`s without applying them" shape `eg-jobs::claim::ClaimWritePlan`
/// uses, so a caller that wants to route this through the server's authoritative
/// MutationBatch gateway (instead of the direct `GraphCore` apply
/// [`commit_maxcut_proposal`] performs, for parity with how `eg-jobs` itself is
/// tested and used) has a ready-made `Vec<Method>` to hand it.
#[derive(Debug, Clone)]
pub struct ProposalWritePlan {
    pub claim_id: String,
    pub methods: Vec<Method>,
}

#[derive(Debug, thiserror::Error)]
pub enum ProposeError {
    #[error("failed to serialize proposal properties: {0}")]
    Serialize(String),
}

/// Lower `proposal` to its canonical `Method` write-set. Pure/deterministic: same
/// proposal (same circuit hash) -> byte-identical node/edge ids and properties.
pub fn plan_maxcut_proposal(proposal: &MaxCutProposal) -> Result<ProposalWritePlan, ProposeError> {
    let result = proposal.quantum_result();
    let circuit_hash = result.circuit_hash.to_string();

    let job_id = quantum_job_node_id(&circuit_hash);
    let claim_id = claim_node_id(&circuit_hash);
    let evidence_id = evidence_node_id(&circuit_hash);

    // The ONLY signal that seeds `confidence`: how good the observed cut is,
    // relative to the true optimum when known (small graphs) or to the trivial
    // "cut half the edges" baseline otherwise. Clamped [0,1], the SAME convention
    // `eg-jobs::claim::plan_result_claim` uses for its "quality score seeds claim
    // confidence" — a NODE property on the Claim, never an edge property, and never
    // treated as anything but a PRIOR for `eg_epistemic::propagate_confidence`'s
    // Bayesian update.
    let quality = proposal.approx_ratio.unwrap_or_else(|| {
        let n_edges = proposal.node_ids.len().max(1) as f64;
        (proposal.mean_sampled_cut_value / n_edges).clamp(0.0, 1.0)
    });
    let confidence = quality.clamp(0.0, 1.0);

    let job_props = serde_json::json!({
        "type": "QuantumJob",
        "algorithm": "qaoa-maxcut",
        "qaoa_p": proposal.qaoa_p,
        "n_qubits": proposal.node_ids.len(),
        "candidate_node_ids": proposal.node_ids,
        "optimized_bindings": proposal.optimized_bindings,
        "search_evaluations": proposal.search_evaluations,
        // Full Q0 result metadata, verbatim (register D-QN-1's contract):
        "backend_id": result.backend_id.0,
        "formalism": formalism_label(result.formalism),
        "seed": result.seed,
        "shots": result.shots,
        "circuit_hash": circuit_hash,
        // Recorded honestly for observability (Q9) — the BACKEND's own noiseless-
        // simulation flag. NEVER read by this crate's commit path to decide
        // anything; see this module's doc for why that would be wrong even though
        // it is `true` for a local noiseless run.
        "backend_reported_exact": result.is_exact(),
        "noise_model_id": result.noise_model_id,
        "fidelity_hint": result.fidelity_hint,
        "wall_time_ms": result.wall_time_ms,
        "peak_memory_bytes": result.peak_memory_bytes,
    });

    let claim_props = serde_json::json!({
        "type": "Claim",
        "family": "quantum.qaoa.maxcut",
        "about": claim_id,
        "confidence": confidence,
        "validation_state": CLAIM_VALIDATION_STATE,
        // The domain-level exactness flag this crate actually enforces: a QAOA
        // Max-Cut partition claim is ALWAYS `exact: false`, unconditionally — never
        // read from `result.is_exact()`. This is what keeps a noiseless-simulator
        // QAOA run from silently qualifying as a hard fact even though its
        // UNDERLYING QuantumResult's own flag says `true`.
        "exact": false,
        "partition": proposal
            .node_ids
            .iter()
            .zip(proposal.partition.iter())
            .map(|(id, side)| serde_json::json!({ "node_id": id, "partition": if *side { 1 } else { 0 } }))
            .collect::<Vec<_>>(),
        "cut_value": proposal.cut_value,
        "mean_sampled_cut_value": proposal.mean_sampled_cut_value,
        "approx_ratio": proposal.approx_ratio,
        "job_id": job_id,
        "circuit_hash": circuit_hash,
        "invalidation_deps": [evidence_id.as_str()],
    });

    let evidence_props = serde_json::json!({
        "type": "Evidence",
        "family": "quantum.qaoa.maxcut",
        "about": claim_id,
        "provenance": format!("quantum:{}:{}", result.backend_id.0, circuit_hash),
        "confidence": confidence,
        "validation_state": CLAIM_VALIDATION_STATE,
        "backend_id": result.backend_id.0,
        "shots": result.shots,
        "seed": result.seed,
    });

    let supports = rmp_serde::to_vec_named(&serde_json::json!({ "relationship": "SUPPORTS" }))
        .map_err(|e| ProposeError::Serialize(e.to_string()))?;
    let generated_by =
        rmp_serde::to_vec_named(&serde_json::json!({ "relationship": "GENERATED_BY" }))
            .map_err(|e| ProposeError::Serialize(e.to_string()))?;

    let methods = vec![
        Method::AddNode {
            node_id: job_id.clone(),
            properties_msgpack: rmp_serde::to_vec_named(&job_props)
                .map_err(|e| ProposeError::Serialize(e.to_string()))?,
        },
        Method::AddNode {
            node_id: claim_id.clone(),
            properties_msgpack: rmp_serde::to_vec_named(&claim_props)
                .map_err(|e| ProposeError::Serialize(e.to_string()))?,
        },
        Method::AddNode {
            node_id: evidence_id.clone(),
            properties_msgpack: rmp_serde::to_vec_named(&evidence_props)
                .map_err(|e| ProposeError::Serialize(e.to_string()))?,
        },
        Method::AddEdge {
            source_id: evidence_id,
            target_id: claim_id.clone(),
            properties_msgpack: supports,
        },
        Method::AddEdge {
            source_id: claim_id.clone(),
            target_id: job_id,
            properties_msgpack: generated_by,
        },
    ];

    Ok(ProposalWritePlan { claim_id, methods })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalCommitOutcome {
    Committed { claim_id: String },
    AlreadyCommitted { claim_id: String },
}

impl ProposalCommitOutcome {
    pub fn claim_id(&self) -> &str {
        match self {
            ProposalCommitOutcome::Committed { claim_id }
            | ProposalCommitOutcome::AlreadyCommitted { claim_id } => claim_id,
        }
    }
}

/// Commit `proposal` into `core` — idempotent on `circuit_hash` (a re-run of the
/// SAME optimized circuit is a no-op), mirroring `eg-jobs::claim::commit_result_claim`.
/// This is the crate's ONE write entry point: its parameter type is
/// [`&MaxCutProposal`], never anything HardConstraint-shaped, so there is no call
/// signature anywhere in this crate that COULD commit a QAOA result as a hard fact.
pub fn commit_maxcut_proposal(
    core: &GraphCore,
    proposal: &MaxCutProposal,
) -> Result<ProposalCommitOutcome, ProposeError> {
    let circuit_hash = proposal.quantum_result().circuit_hash.to_string();
    let claim_id = claim_node_id(&circuit_hash);
    if core.has_node(&claim_id) {
        return Ok(ProposalCommitOutcome::AlreadyCommitted { claim_id });
    }

    let plan = plan_maxcut_proposal(proposal)?;
    for method in plan.methods {
        match method {
            Method::AddNode {
                node_id,
                properties_msgpack,
            } => core.add_node(node_id, properties_msgpack),
            Method::AddEdge {
                source_id,
                target_id,
                properties_msgpack,
            } => core
                .add_edge(source_id, target_id, properties_msgpack)
                .map_err(|e| ProposeError::Serialize(e.to_string()))?,
            _ => unreachable!("plan_maxcut_proposal only ever emits AddNode/AddEdge"),
        }
    }

    Ok(ProposalCommitOutcome::Committed { claim_id })
}

fn formalism_label(f: Formalism) -> &'static str {
    match f {
        Formalism::Statevector => "statevector",
        Formalism::DensityMatrix => "density_matrix",
        Formalism::Stabilizer => "stabilizer",
        Formalism::MatrixProductState => "matrix_product_state",
        Formalism::Trajectory => "trajectory",
        Formalism::Hardware => "hardware",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run::{run_qaoa_maxcut_local_sim, QaoaConfig};
    use crate::subgraph::CandidateSubgraph;

    fn triangle() -> CandidateSubgraph {
        CandidateSubgraph {
            node_ids: vec!["a".into(), "b".into(), "c".into()],
            edges: vec![(0, 1, 1.0), (1, 2, 1.0), (0, 2, 1.0)],
        }
    }

    /// ★ THE load-bearing acceptance test: a noisy/variational QAOA result — even
    /// one whose underlying `QuantumResult.is_exact()` reports `true` (the real
    /// `eg-quantum-sim` loophole this module's doc explains) — CANNOT become a hard
    /// constraint through this crate's write path. It is committed with
    /// `exact: false` and `validation_state: "unvalidated"` regardless of the
    /// backend flag, via the Claim/Evidence/SUPPORTS shape the classical TMS reads,
    /// never as a raw fact.
    #[test]
    fn qaoa_result_cannot_become_a_hard_constraint_even_when_backend_reports_exact() {
        let subgraph = triangle();
        let config = QaoaConfig {
            p: 1,
            grid_resolution: 10,
            shots: 128,
            seed: 3,
        };
        let run = run_qaoa_maxcut_local_sim(&subgraph, &config).unwrap();

        // Confirm the loophole condition actually triggers: the backend DID report
        // this result as exact (noiseless local simulation, no noise model).
        assert!(
            run.quantum_result.is_exact(),
            "test premise requires the backend-reported-exact loophole to be live"
        );
        // And structurally: a caller COULD obtain a HardConstraint from this exact
        // result via eg-quantum-core's own type-state (proving the flag alone is not
        // protective) --
        let result_for_bypass_attempt = run.quantum_result.clone();
        let hard_constraint = result_for_bypass_attempt.into_hard_constraint();
        assert!(
            hard_constraint.is_ok(),
            "eg-quantum-core's type-state correctly allows an EXACT result to become \
             a HardConstraint -- the point of this test is that our domain code never \
             calls this path for QAOA, not that the type system blocks it"
        );

        // -- yet this crate's ONLY commit entry point never accepts one: it takes a
        // MaxCutProposal, which is constructed only via `into_proposal()`.
        let proposal = MaxCutProposal::from_run(&subgraph, run);
        let core = GraphCore::new();
        let outcome = commit_maxcut_proposal(&core, &proposal).unwrap();
        let claim_id = outcome.claim_id().to_string();

        let blob = core.get_node_properties(&claim_id).unwrap();
        let props: serde_json::Value = rmp_serde::from_slice(&blob).unwrap();
        assert_eq!(
            props["exact"], false,
            "a QAOA Max-Cut claim must NEVER be marked exact, regardless of the \
             backend's own noiseless-simulation flag"
        );
        assert_eq!(props["validation_state"], CLAIM_VALIDATION_STATE);
        // confidence is a calibratable PRIOR in [0,1], never hardcoded to 1.0 by
        // this path (it is derived from the observed cut quality).
        let confidence = props["confidence"].as_f64().unwrap();
        assert!((0.0..=1.0).contains(&confidence));

        // The write is via Claim + Evidence + SUPPORTS -- eg-epistemic's
        // classify_relationship whitelist -- not a raw fact.
        let evidence_id = evidence_node_id(&proposal.quantum_result().circuit_hash.to_string());
        assert!(core.has_node(&evidence_id));
        assert!(core.has_edge(&evidence_id, &claim_id));
        let evidence_blob = core.get_node_properties(&evidence_id).unwrap();
        let evidence_props: serde_json::Value = rmp_serde::from_slice(&evidence_blob).unwrap();
        assert_eq!(evidence_props["type"], "Evidence");

        let edge_blobs = core.get_edge_properties(&evidence_id, &claim_id);
        assert_eq!(edge_blobs.len(), 1, "expected exactly one SUPPORTS edge");
        let edge_props: serde_json::Value = rmp_serde::from_slice(&edge_blobs[0]).unwrap();
        assert_eq!(edge_props["relationship"], "SUPPORTS");
    }

    #[test]
    fn inexact_result_is_rejected_by_eg_quantum_cores_own_type_state() {
        // A hardware/noisy result -- constructed the same way a REAL hardware
        // backend (Q10, not touched by this crate) would -- can never become a
        // HardConstraint at all, even before reaching this crate's proposal layer.
        let tiny_program = crate::circuit::build_qaoa_program(2, &[(0, 1, 1.0)], 1, true);
        let circuit_hash = tiny_program.circuit_hash().unwrap();
        let inexact = eg_quantum_core::result::QuantumResult::new_inexact(
            eg_quantum_core::backend::BackendId::from("hardware-ionq"),
            Formalism::Hardware,
            Some(1),
            Some(1000),
            circuit_hash,
            Some("device-calibration-2026-08".to_string()),
            Some(0.97),
            42,
            1024,
            outcome_counts_for_test(),
        );
        assert!(!inexact.is_exact());
        let err = inexact.into_hard_constraint().unwrap_err();
        assert_eq!(
            err.backend_id,
            eg_quantum_core::backend::BackendId::from("hardware-ionq")
        );
    }

    fn outcome_counts_for_test() -> eg_quantum_core::result::Outcome {
        use std::collections::BTreeMap;
        let mut m = BTreeMap::new();
        m.insert("0".to_string(), 1000u64);
        eg_quantum_core::result::Outcome::Counts(m)
    }

    #[test]
    fn recommitting_the_same_circuit_is_idempotent() {
        let subgraph = triangle();
        let config = QaoaConfig {
            p: 1,
            grid_resolution: 8,
            shots: 64,
            seed: 1,
        };
        let core = GraphCore::new();

        let run_a = run_qaoa_maxcut_local_sim(&subgraph, &config).unwrap();
        let proposal_a = MaxCutProposal::from_run(&subgraph, run_a);
        let first = commit_maxcut_proposal(&core, &proposal_a).unwrap();
        assert!(matches!(first, ProposalCommitOutcome::Committed { .. }));

        // Same subgraph, same config, same seed -> same optimized angles -> SAME
        // circuit_hash for the measured program -> idempotent re-commit.
        let run_b = run_qaoa_maxcut_local_sim(&subgraph, &config).unwrap();
        let proposal_b = MaxCutProposal::from_run(&subgraph, run_b);
        let second = commit_maxcut_proposal(&core, &proposal_b).unwrap();
        assert!(matches!(second, ProposalCommitOutcome::AlreadyCommitted { .. }));
        assert_eq!(first.claim_id(), second.claim_id());
    }
}
