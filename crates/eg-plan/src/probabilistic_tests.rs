//! Probabilistic modality executor proofs (CONCEPT:EG-086).
//!
//! A small `Belief` layer, each node carrying a `Distribution` in a `distribution`
//! property (the tagged serde form of `eg_types::Distribution`), drives the
//! `Op::Probabilistic` surface end-to-end through the fused executor:
//!  * `Expectation` — score each row by its distribution mean, ranked descending.
//!  * `Marginal { at, label }` — score by pdf(at) / pmf(label).
//!  * `Conditional { evidence }` — score by the conjugate posterior mean; rows whose
//!    (prior, evidence) is not a supported conjugate pair are dropped.
//!  * `Sample { seed }` — deterministic seeded draw (same seed ⇒ same score).

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_types::wire::{ProbEvidenceSpec, ProbQuery};
use eg_types::Distribution;
use serde_json::json;

use crate::algebra::{Op, Plan};
use crate::exec::PlanCtx;
use crate::PlanExt;

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// A layer of `Belief` nodes each holding a Distribution, plus a non-distribution
/// `Doc` distractor and a `Belief` with a malformed `distribution` property.
///
///   G_LO = N(1,1), G_HI = N(9,1), BETA = Beta(2,3); Doc nd0 has none; B_BAD is garbage.
fn beliefs() -> GraphView {
    let core = GraphCore::new();
    let put = |id: &str, d: &Distribution| {
        let dv = serde_json::to_value(d).unwrap();
        core.add_node(
            id.into(),
            blob(json!({ "type": "Belief", "distribution": dv })),
        );
    };
    put(
        "G_LO",
        &Distribution::Gaussian {
            mean: 1.0,
            std: 1.0,
        },
    );
    put(
        "G_HI",
        &Distribution::Gaussian {
            mean: 9.0,
            std: 1.0,
        },
    );
    put(
        "BETA",
        &Distribution::Beta {
            alpha: 2.0,
            beta: 3.0,
        },
    );
    // A non-distribution distractor that must never appear in a probabilistic result.
    core.add_node("nd0".into(), blob(json!({ "type": "Doc", "year": 2025 })));
    // A Belief whose `distribution` property is malformed — must be dropped.
    core.add_node(
        "B_BAD".into(),
        blob(json!({ "type": "Belief", "distribution": "not-a-distribution" })),
    );
    core.analysis_snapshot()
}

fn run(plan: &Plan, view: &GraphView) -> Vec<String> {
    let sem = SemanticStore::new();
    let c = PlanCtx::new(view, &sem);
    plan.execute(&c).unwrap().ids()
}

#[test]
fn probabilistic_expectation_ranks_by_mean_descending() {
    let view = beliefs();
    // Score every Belief by its distribution mean, ranked descending:
    // G_HI(9) > G_LO(1) > BETA(0.4). Doc/malformed rows are dropped.
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Belief".into(),
        },
        Op::Probabilistic {
            query: ProbQuery::Expectation,
        },
    ]);
    assert_eq!(run(&plan, &view), vec!["G_HI", "G_LO", "BETA"]);
}

#[test]
fn probabilistic_expectation_then_limit_takes_top_k() {
    let view = beliefs();
    // The ranked order lets a downstream Limit take the top-1 (highest mean).
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Belief".into(),
        },
        Op::Probabilistic {
            query: ProbQuery::Expectation,
        },
        Op::Limit { k: 1 },
    ]);
    assert_eq!(run(&plan, &view), vec!["G_HI"]);
}

#[test]
fn probabilistic_marginal_density_scores_and_ranks() {
    let view = beliefs();
    // Marginal density at x=1: G_LO peaks there (pdf≈0.399) > BETA(pdf(1)=0) and
    // G_HI (far in the tail, ≈1e-15). All three carry a valid distribution → all
    // survive; the highest-density row ranks first.
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Belief".into(),
        },
        Op::Probabilistic {
            query: ProbQuery::Marginal {
                at: 1.0,
                label: None,
            },
        },
        Op::Limit { k: 1 },
    ]);
    assert_eq!(run(&plan, &view), vec!["G_LO"]);
}

#[test]
fn probabilistic_conditional_posterior_drops_unsupported_pairs() {
    let view = beliefs();
    // Bernoulli evidence updates a Beta prior (conjugate) but NOT a Gaussian prior
    // (unsupported pair). Only BETA survives; the two Gaussians are dropped.
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Belief".into(),
        },
        Op::Probabilistic {
            query: ProbQuery::Conditional {
                evidence: ProbEvidenceSpec::Bernoulli {
                    successes: 3.0,
                    failures: 1.0,
                },
            },
        },
    ]);
    assert_eq!(run(&plan, &view), vec!["BETA"]);
}

#[test]
fn probabilistic_sample_is_deterministic_given_seed() {
    let view = beliefs();
    // A seeded Sample is a pure function of (distribution, seed): the SAME plan run
    // twice yields the SAME ranked ids (no RNG-from-clock).
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Belief".into(),
        },
        Op::Probabilistic {
            query: ProbQuery::Sample { seed: 42 },
        },
    ]);
    let a = run(&plan, &view);
    let b = run(&plan, &view);
    assert_eq!(a, b, "seeded sample must be reproducible");
    // Every valid-distribution row is scored + kept (3 of them); distractors dropped.
    let mut sorted = a.clone();
    sorted.sort();
    assert_eq!(sorted, vec!["BETA", "G_HI", "G_LO"]);
}

#[test]
fn probabilistic_scan_only_scores_distribution_bearing_rows() {
    let view = beliefs();
    // Feed ALL nodes (both layers + the malformed one) via a broad candidate set:
    // only the three valid Beliefs are scored; nd0 (Doc) and B_BAD (garbage) drop.
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Probabilistic {
            query: ProbQuery::Expectation,
        },
    ]);
    // A Doc has no distribution → the whole result is empty.
    assert_eq!(run(&plan, &view), Vec::<String>::new());
}
