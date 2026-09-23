//! Brute-force oracle agreement for n ≤ 20 (DECIDE-LAYER-DESIGN §11.1), on a
//! random generator and on an adversarial one (many overlapping,
//! near-equal-cost candidates), plus the latency report at 64 × 32.

use eg_types::decision::{DecisionOutcome, SolverBudget};
use eg_types::solve::{Relation, VarId};

use super::super::{assemble, replay_check};
use super::fixture::*;
use crate::solve::Model;

/// A small deterministic generator (SplitMix64), so every case is replayable.
struct Seeded(u64);

impl Seeded {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

const CAPABILITIES: [&str; 6] = [
    "eg:capability/retrieval/web-search",
    "eg:capability/retrieval/vector-search",
    "eg:capability/analysis/summarize",
    "eg:capability/analysis/extract",
    "eg:capability/reasoning/plan",
    "eg:capability/generation/code",
];

fn library(
    rng: &mut Seeded,
    tools: usize,
    adversarial: bool,
) -> Vec<eg_types::decision::CandidateFacts> {
    let mut out = agent_basics();
    out.extend([cheap_model("m-0"), cheap_model("m-1"), prompt("p-0", 100)]);
    for index in 0..tools {
        let classes: Vec<&str> = CAPABILITIES
            .iter()
            .copied()
            .filter(|_| rng.below(if adversarial { 2 } else { 3 }) == 0)
            .collect();
        let price = if adversarial {
            100 + rng.below(3)
        } else {
            1 + rng.below(50)
        };
        out.push(tool(&format!("t-{index:02}"), &classes, Some(price)));
    }
    out
}

/// The optimum by enumeration over every assignment of the model.
fn brute_force(model: &Model) -> Option<i128> {
    let n = model.variable_count();
    (0u32..(1 << n))
        .filter_map(|mask| {
            let selected: Vec<bool> = (0..n).map(|bit| mask & (1 << bit) != 0).collect();
            feasible(model, &selected).then(|| {
                model
                    .objective_value(&selected)
                    .expect("sized")
                    .scalar
                    .get()
            })
        })
        .min()
}

fn feasible(model: &Model, selected: &[bool]) -> bool {
    model.rows().iter().all(|row| {
        let lhs: i64 = row
            .terms()
            .iter()
            .filter(|term| selected[VarId::index(term.var)])
            .map(|term| term.coefficient)
            .sum();
        match row.relation() {
            Relation::LessEqual => lhs <= row.rhs(),
            Relation::GreaterEqual => lhs >= row.rhs(),
            Relation::Equal => lhs == row.rhs(),
        }
    })
}

fn agree(seed: u64, adversarial: bool) {
    let mut rng = Seeded(seed);
    let tools = 8 + rng.below(10) as usize;
    let capabilities: Vec<&str> = CAPABILITIES
        .iter()
        .copied()
        .take(2 + rng.below(4) as usize)
        .collect();
    let mut asked = request(&[], &capabilities);
    asked.solver = Some(SolverBudget {
        node_budget: 100_000,
        max_why_not_per_slot: 0,
    });
    let assembly = assemble(
        inputs(asked, library(&mut rng, tools, adversarial)),
        identity(),
    )
    .expect("assembles");
    replay_check(&assembly.record).expect("replays");
    match (&assembly.record.outcome, &assembly.model) {
        (DecisionOutcome::Solved { certificate, .. }, Some(spec)) => {
            let model = Model::try_from(spec.clone()).expect("valid");
            let solved = certificate
                .incumbent
                .as_ref()
                .expect("incumbent")
                .objective
                .scalar
                .get();
            assert_eq!(
                Some(solved),
                brute_force(&model),
                "seed {seed} adversarial {adversarial}"
            );
        }
        (DecisionOutcome::Abstained { reasons }, _) => {
            // An abstention must be typed; a random library may leave a
            // requirement uncovered or the rows infeasible.
            assert!(!reasons.is_empty(), "seed {seed}");
        }
        (DecisionOutcome::Solved { .. }, None) => panic!("a solved assembly carries its model"),
    }
}

#[test]
fn the_solver_agrees_with_brute_force_on_random_libraries() {
    for seed in 0..12 {
        agree(seed, false);
    }
}

#[test]
fn the_solver_agrees_with_brute_force_on_adversarial_libraries() {
    for seed in 1_000..1_008 {
        agree(seed, true);
    }
}

/// Derivation plus solve at the record bound: 64 candidates, and the widest
/// requirement set the vocabulary allows. Reported, with the node budget as
/// the real bound (§11.1 latency). Printed with `--nocapture`.
#[test]
fn latency_report_at_the_record_bound() {
    let mut rng = Seeded(7);
    let candidates = library(&mut rng, 59, false);
    let capabilities: Vec<&str> = CAPABILITIES.to_vec();
    let mut samples = Vec::new();
    for _ in 0..25 {
        let started = std::time::Instant::now();
        let assembly = assemble(
            inputs(
                request(&["eg:task/research"], &capabilities),
                candidates.clone(),
            ),
            identity(),
        )
        .expect("assembles");
        samples.push(started.elapsed());
        assert!(assembly.record.inputs.candidates.len() == 64);
    }
    samples.sort();
    let p50 = samples[samples.len() / 2];
    let p99 = samples[samples.len() - 1];
    println!(
        "ASSEMBLE-LATENCY candidates=64 samples={} p50={p50:?} p99={p99:?}",
        samples.len()
    );
}
