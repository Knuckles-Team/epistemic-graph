//! Assembly-scale 0-1 solver bench (Decide layer package A3).
//!
//! Solves and then verifies planted assemblies of 64 candidate components,
//! 32 required capabilities and 4 model profiles, with exactly-one,
//! implication, requirement, budget and cardinality rows and a three-level
//! lexicographic objective (cost with an unknown-cost tier, component count,
//! declared p95 latency). The instances are SYNTHETIC and their optimum is
//! known by construction; the bench asserts it before timing anything. Work is
//! bounded by the default deterministic node budget, never by wall clock.
//!
//! Run: cargo bench -p eg-compute --features solve --bench solve_cover

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use eg_compute::solve::model::Model;
use eg_compute::solve::{solve, verify, SolverConfig};

#[path = "../tests/solve/generators.rs"]
mod generators;

use generators::{planted_assembly, AssemblyShape};

const SHAPE: AssemblyShape = AssemblyShape {
    components: 64,
    capabilities: 32,
    models: 4,
    unknown_cost_decoys: 6,
};

fn instances() -> Vec<Model> {
    (0..8u64)
        .map(|seed| {
            let planted = planted_assembly(SHAPE, seed);
            let model = Model::try_from(planted.spec).expect("generated specs are valid");
            let certificate = solve(&model, &SolverConfig::default());
            let found = certificate
                .incumbent
                .as_ref()
                .map(|incumbent| &incumbent.selected);
            assert_eq!(
                found,
                Some(&planted.optimum),
                "seed {seed} must recover its planted optimum"
            );
            model
        })
        .collect()
}

fn bench_solve_cover(c: &mut Criterion) {
    let models = instances();
    let config = SolverConfig::default();
    c.bench_function("solve_64x32_planted_assembly", |b| {
        b.iter(|| {
            for model in &models {
                black_box(solve(black_box(model), &config));
            }
        })
    });
    let certificates: Vec<_> = models.iter().map(|model| solve(model, &config)).collect();
    c.bench_function("verify_64x32_planted_assembly", |b| {
        b.iter(|| {
            for (model, certificate) in models.iter().zip(&certificates) {
                black_box(verify(model, certificate).expect("solver output verifies"));
            }
        })
    });
}

criterion_group!(benches, bench_solve_cover);
criterion_main!(benches);
