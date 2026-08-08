//! Q6 proof: the numeric bridge on REAL simulator output (not stubs), plus the
//! "many small circuits" batch API run against the SAME backend selected the SAME
//! way the job-plane worker would select it (`estimate()` + `select_backend()`).

use eg_quantum_core::backend::{BackendDescriptor, QuantumBackend, RunOptions};
use eg_quantum_core::estimate::{estimate, EstimateOptions};
use eg_quantum_core::planner::{select_backend, PlannerOptions};
use eg_quantum_jobs::circuit::induced_subgraph_ghz_program;
use eg_quantum_jobs::numeric_bridge::{bitstring_distribution, marginal_probabilities, run_batch};
use eg_quantum_sim::stabilizer::StabilizerSimulator;

fn descriptor(backend: &dyn QuantumBackend) -> BackendDescriptor {
    BackendDescriptor {
        id: backend.backend_id(),
        family: backend.family(),
        capabilities: backend.capabilities(),
    }
}

#[test]
fn ghz_bitstring_distribution_and_marginals_match_theory() {
    let program = induced_subgraph_ghz_program(3, &[(0, 1), (1, 2)]);
    let est = estimate(&program, &EstimateOptions::default());
    let stabilizer = StabilizerSimulator::new();
    let decision = select_backend(&est, &[descriptor(&stabilizer)], &PlannerOptions::default())
        .expect("a Clifford circuit with a registered stabilizer backend always selects one");
    assert_eq!(decision.chosen.0, "stabilizer");

    let result = stabilizer
        .run(
            &program,
            &RunOptions {
                shots: Some(1000),
                seed: Some(7),
                ..Default::default()
            },
        )
        .expect("noiseless run succeeds");

    let dist = bitstring_distribution(&result.outcome).unwrap();
    // A perfect 3-qubit GHZ has support ONLY on "000" and "111".
    assert_eq!(dist.bitstrings, vec!["000", "111"]);
    assert_eq!(dist.total_shots, 1000);
    assert!((dist.probabilities.sum() - 1.0).abs() < 1e-12);

    let marginals = marginal_probabilities(&result.outcome, 3).unwrap();
    for &p in marginals.iter() {
        assert!(
            (p - 0.5).abs() < 0.1,
            "GHZ marginal should be ~0.5, got {p}"
        );
    }
}

#[test]
fn run_batch_executes_many_small_circuits_in_order() {
    let stabilizer = StabilizerSimulator::new();
    let programs: Vec<_> = (1..=5u32)
        .map(|n| {
            let edges: Vec<(u32, u32)> = (0..n.saturating_sub(1)).map(|i| (i, i + 1)).collect();
            (
                induced_subgraph_ghz_program(n, &edges),
                RunOptions {
                    shots: Some(50),
                    seed: Some(n as u64),
                    ..Default::default()
                },
            )
        })
        .collect();

    let results = run_batch(&stabilizer, &programs);
    assert_eq!(results.len(), 5);
    for (i, result) in results.into_iter().enumerate() {
        let result = result.unwrap_or_else(|e| panic!("circuit {i} failed: {e}"));
        assert_eq!(result.shots, Some(50));
        assert!(result.is_exact(), "noiseless stabilizer run is exact");
    }
}
