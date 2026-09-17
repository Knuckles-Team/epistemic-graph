//! Planted assemblies: the unique optimum is known by construction.

use eg_compute::solve::{SolveStatus, SolverConfig, Verdict};

use super::generators::{planted_assembly, AssemblyShape};
use super::support::{config, model, solve_and_verify};

fn assert_recovers(shape: AssemblyShape, seed: u64, solver: &SolverConfig) -> SolveStatus {
    let planted = planted_assembly(shape, seed);
    let (certificate, verdict) = solve_and_verify(&model(&planted.spec), solver);
    let incumbent = certificate
        .incumbent
        .as_ref()
        .expect("planted instances are feasible");
    assert_eq!(
        incumbent.selected, planted.optimum,
        "seed {seed}: {:?}",
        certificate.status
    );
    match &verdict {
        Verdict::ProvenOptimal { objective }
        | Verdict::OptimalityRequiresReplay { objective, .. } => {
            assert_eq!(*objective, incumbent.objective.scalar)
        }
        other => panic!("seed {seed}: unexpected verdict {other:?}"),
    }
    certificate.status
}

#[test]
fn planted_assemblies_recover_the_unique_optimum_with_a_certificate() {
    let shape = AssemblyShape {
        components: 24,
        capabilities: 12,
        models: 3,
        unknown_cost_decoys: 3,
    };
    for seed in 0..16 {
        let status = assert_recovers(shape, seed, &config(200_000, 1 << 16, 0, 1));
        assert_eq!(status, SolveStatus::Optimal, "seed {seed}");
    }
}

#[test]
fn assembly_scale_planted_instances_are_solved_under_the_default_budget() {
    let shape = AssemblyShape {
        components: 64,
        capabilities: 32,
        models: 4,
        unknown_cost_decoys: 6,
    };
    for seed in 100..104 {
        assert_recovers(shape, seed, &SolverConfig::default());
    }
}
