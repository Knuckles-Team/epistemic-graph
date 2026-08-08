//! Step 3 of the Q7/Q11 pipeline: the ONE official, provenance-carrying execution —
//! classically-optimized QAOA parameters (from `circuit.rs`) bound into the
//! MEASURED ansatz and submitted to a real [`QuantumBackend`] (`eg-quantum-sim`'s
//! [`StateVectorSimulator`] by default, per PROGRAM.md's "local simulation is the
//! default, external providers are for validation/noise studies/overflow work
//! only"), producing the ONE [`QuantumResult`] this run's `propose.rs` write-back
//! carries the FULL Q0 metadata of.

use std::collections::BTreeMap;

use eg_quantum_core::backend::{BackendId, QuantumBackend, RunOptions};
use eg_quantum_core::result::{Outcome, QuantumResult};
use eg_quantum_sim::statevector::StateVectorSimulator;

use crate::circuit::{
    brute_force_max_cut, build_qaoa_program, cut_value_of_bitstring, optimize_qaoa_params,
};
use crate::subgraph::CandidateSubgraph;

/// Above this qubit count, `MaxCutRun` skips the `2^n`-enumeration brute-force
/// optimum (used only to report an `approx_ratio`, never part of the committed
/// result) — the search+run pipeline itself has no such cap (bounded only by the
/// backend's own `max_qubits_statevector`, currently 24).
pub const MAX_BRUTE_FORCE_QUBITS: u32 = 20;

#[derive(Debug, Clone)]
pub struct QaoaConfig {
    /// QAOA circuit depth (number of cost+mixer layer pairs).
    pub p: usize,
    /// Per-angle grid points in the classical parameter search (`circuit.rs`'s
    /// `optimize_qaoa_params`).
    pub grid_resolution: usize,
    /// Shots for the ONE official measured run submitted to the backend.
    pub shots: u64,
    /// Seed threaded into both the backend's `RunOptions.seed` (for reproducible
    /// Born-rule sampling) and recorded verbatim on the committed `QuantumResult`/
    /// `QuantumJob` node.
    pub seed: u64,
}

impl Default for QaoaConfig {
    fn default() -> Self {
        QaoaConfig {
            p: 1,
            grid_resolution: 16,
            shots: 512,
            seed: 42,
        }
    }
}

/// Full output of one QAOA Max-Cut run: the exactness-typed `QuantumResult` (the
/// Q0 artifact — carries backend_id/formalism/seed/shots/circuit_hash/exact/
/// noise_model_id/fidelity_hint/wall_time_ms/peak_memory_bytes verbatim), the
/// classically-decoded best partition, and benchmarking-only classical context
/// (`search_evaluations`, `approx_ratio` when computable).
#[derive(Debug, Clone)]
pub struct MaxCutRun {
    pub quantum_result: QuantumResult,
    pub n_qubits: u32,
    pub qaoa_p: usize,
    /// Optimized `(cost_angle, mixer_angle)` bindings per layer, as bound into the
    /// FINAL measured circuit.
    pub optimized_bindings: BTreeMap<String, f64>,
    /// How many exact-statevector evaluations the classical search performed —
    /// itself an honest measure of the classical overhead QAOA parameter-fitting
    /// costs, reportable alongside the quantum wall-clock (Q11 benchmarking).
    pub search_evaluations: usize,
    /// Best (highest-count) measured bitstring's partition: `partition[i]` is qubit
    /// `i`'s side of the cut (`false`/`true`), in the SAME node order as the
    /// `CandidateSubgraph` this run came from.
    pub best_partition: Vec<bool>,
    /// The Max-Cut value (sum of cut edge weights) of `best_partition`.
    pub best_cut_value: f64,
    /// Mean cut value over ALL measured shots (not just the best one) — the
    /// sampling-noise-exposed complement to `best_cut_value`.
    pub mean_sampled_cut_value: f64,
    /// `best_cut_value / brute_force_optimum`, when `n_qubits <=
    /// MAX_BRUTE_FORCE_QUBITS` made computing the true optimum tractable; `None`
    /// above that (an honest absence, never a fabricated estimate).
    pub approx_ratio: Option<f64>,
}

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("backend error: {0}")]
    Backend(String),
    #[error("circuit hash error: {0}")]
    Hash(String),
    #[error("QAOA measured 0 shots (Outcome was not Counts, or Counts was empty)")]
    NoSamples,
}

/// Run QAOA Max-Cut over `subgraph`: optimize `(cost_angle, mixer_angle)`
/// classically against the exact noiseless statevector (`circuit.rs`), bind the
/// result into the MEASURED ansatz, and submit that ONE final circuit to `backend`
/// with `config.shots` shots. Returns the full [`MaxCutRun`] — including the raw
/// [`QuantumResult`] `propose.rs` writes back verbatim.
pub fn run_qaoa_maxcut(
    subgraph: &CandidateSubgraph,
    backend: &dyn QuantumBackend,
    config: &QaoaConfig,
) -> Result<MaxCutRun, RunError> {
    let n_qubits = subgraph.n_qubits();
    let optimized =
        optimize_qaoa_params(n_qubits, &subgraph.edges, config.p, config.grid_resolution);

    let measured_program = build_qaoa_program(n_qubits, &subgraph.edges, config.p, true);
    let opts = RunOptions {
        shots: Some(config.shots),
        seed: Some(config.seed),
        noise_model_id: None,
        parameter_bindings: optimized.bindings.clone(),
        timeout_ms: None,
    };
    let quantum_result = backend
        .run(&measured_program, &opts)
        .map_err(|e| RunError::Backend(e.to_string()))?;

    let counts = match &quantum_result.outcome {
        Outcome::Counts(counts) => counts,
        Outcome::ExpectationValue { .. } => return Err(RunError::NoSamples),
    };
    if counts.is_empty() {
        return Err(RunError::NoSamples);
    }

    let (best_bitstring, _best_count) = counts
        .iter()
        .max_by_key(|(_, count)| **count)
        .expect("counts is non-empty, checked above");
    let best_partition: Vec<bool> = best_bitstring.chars().map(|c| c == '1').collect();
    let best_idx = bitstring_to_index(best_bitstring);
    let best_cut_value = cut_value_of_bitstring(best_idx, &subgraph.edges);

    let total_shots: u64 = counts.values().sum();
    let mean_sampled_cut_value: f64 = counts
        .iter()
        .map(|(bs, count)| {
            cut_value_of_bitstring(bitstring_to_index(bs), &subgraph.edges) * (*count as f64)
        })
        .sum::<f64>()
        / total_shots.max(1) as f64;

    let approx_ratio = if n_qubits <= MAX_BRUTE_FORCE_QUBITS {
        let optimum = brute_force_max_cut(n_qubits, &subgraph.edges);
        if optimum > 0.0 {
            Some(best_cut_value / optimum)
        } else {
            Some(1.0) // no edges at all: any partition is trivially optimal
        }
    } else {
        None
    };

    Ok(MaxCutRun {
        quantum_result,
        n_qubits,
        qaoa_p: config.p,
        optimized_bindings: optimized.bindings,
        search_evaluations: optimized.evaluations,
        best_partition,
        best_cut_value,
        mean_sampled_cut_value,
        approx_ratio,
    })
}

/// Convenience: run against the default in-process `eg-quantum-sim` backend
/// (`StateVectorSimulator`) — PROGRAM.md's "local simulation is the default" path,
/// and the ONLY path this crate exercises (Q10 external/hardware providers are a
/// separate, feature-gated lane this crate does not touch).
pub fn run_qaoa_maxcut_local_sim(
    subgraph: &CandidateSubgraph,
    config: &QaoaConfig,
) -> Result<MaxCutRun, RunError> {
    let backend = StateVectorSimulator::new();
    run_qaoa_maxcut(subgraph, &backend, config)
}

pub fn default_backend_id() -> BackendId {
    StateVectorSimulator::new().backend_id()
}

fn bitstring_to_index(bitstring: &str) -> usize {
    let mut idx = 0usize;
    for (qubit, ch) in bitstring.chars().enumerate() {
        if ch == '1' {
            idx |= 1 << qubit;
        }
    }
    idx
}

/// Re-exported so callers building a custom `RunOptions`/inspecting layer symbol
/// names don't need a separate `use eg_quantum_workloads::circuit::*`.
pub use crate::circuit::OptimizedParams;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subgraph::CandidateSubgraph;

    fn triangle() -> CandidateSubgraph {
        CandidateSubgraph {
            node_ids: vec!["a".into(), "b".into(), "c".into()],
            edges: vec![(0, 1, 1.0), (1, 2, 1.0), (0, 2, 1.0)],
        }
    }

    #[test]
    fn run_produces_a_result_and_a_sensible_partition() {
        let subgraph = triangle();
        let config = QaoaConfig {
            p: 1,
            grid_resolution: 10,
            shots: 256,
            seed: 7,
        };
        let run = run_qaoa_maxcut_local_sim(&subgraph, &config).unwrap();
        assert_eq!(run.n_qubits, 3);
        assert_eq!(run.best_partition.len(), 3);
        // Triangle's true optimum is 2.0 (odd cycle); QAOA depth-1 should find a
        // partition cutting at least 1 of the 3 edges (better than a random
        // 1-in-8 chance of the all-same partition cutting 0).
        assert!(run.best_cut_value >= 1.0);
        assert_eq!(run.approx_ratio.unwrap(), run.best_cut_value / 2.0);
        // The backend's own noiseless-simulation exactness flag: true, since no
        // noise model was requested — the exact loophole `propose.rs` must close
        // independent of this flag (see that module's tests).
        assert!(run.quantum_result.is_exact());
        assert_eq!(run.quantum_result.shots, Some(256));
        assert_eq!(run.quantum_result.seed, Some(7));
    }
}
