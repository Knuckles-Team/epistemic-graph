//! Confidence propagation + justification over the support/contradiction/attack graph.
//!
//! The numeric core is the existing conjugate Bayesian update
//! (`eg_compute::probabilistic::bayesian_update`): a node's own stored confidence seeds
//! a `Beta` prior, each supporting neighbour contributes Bernoulli "successes" and each
//! contradicting/attacking neighbour contributes "failures" (scaled by
//! [`AuthorityPolicy`]), and the posterior mean is the propagated belief. The walk is
//! bounded and **cycle-guarded** — support/attack graphs are NOT acyclic, so a node
//! already on the recursion stack breaks the cycle by falling back to its stored prior.

use std::collections::{HashMap, HashSet};

use eg_compute::probabilistic::{bayesian_update, Evidence};
use eg_types::Distribution;

use crate::adapter::BeliefGraph;
use crate::model::{
    AuthorityPolicy, BeliefState, EdgeKind, JustRule, JustificationGraph, ProofNode,
};

/// Default belief for a node with no stored confidence — maximal ignorance.
const DEFAULT_PRIOR: f64 = 0.5;
/// Depth cap for the justification tree, so `EXPLAIN BELIEF` terminates on a deep or
/// cyclic evidence graph (the belief number itself is exact; only the rendered tree is
/// depth-bounded).
const MAX_EXPLAIN_DEPTH: usize = 32;

fn prior_of(bg: &BeliefGraph, id: &str) -> f64 {
    bg.priors.get(id).copied().unwrap_or(DEFAULT_PRIOR)
}

/// Compute the propagated [`BeliefState`] for `seed`.
pub fn propagate_confidence(bg: &BeliefGraph, seed: &str, policy: &AuthorityPolicy) -> BeliefState {
    let mut memo: HashMap<String, f64> = HashMap::new();
    let mut visiting: HashSet<String> = HashSet::new();
    let confidence = belief_of(bg, seed, policy, &mut memo, &mut visiting);

    let (mut supporting, mut contradicting, mut attacking) = (Vec::new(), Vec::new(), Vec::new());
    if let Some(ins) = bg.in_edges.get(seed) {
        for (src, kind) in ins {
            match kind {
                EdgeKind::Supports => supporting.push(src.clone()),
                EdgeKind::Contradicts => contradicting.push(src.clone()),
                EdgeKind::Attacks => attacking.push(src.clone()),
            }
        }
    }
    BeliefState {
        node_id: seed.to_string(),
        confidence,
        supporting,
        contradicting,
        attacking,
        as_of: bg.as_of,
    }
}

fn belief_of(
    bg: &BeliefGraph,
    id: &str,
    policy: &AuthorityPolicy,
    memo: &mut HashMap<String, f64>,
    visiting: &mut HashSet<String>,
) -> f64 {
    if let Some(v) = memo.get(id) {
        return *v;
    }
    let base = prior_of(bg, id);
    // Cycle break: a node already on the recursion stack contributes its stored prior
    // rather than recursing forever (support/attack graphs are not guaranteed acyclic).
    if visiting.contains(id) {
        return base;
    }
    visiting.insert(id.to_string());

    let result = match bg.in_edges.get(id) {
        Some(edges) if !edges.is_empty() => {
            let mut successes = 0.0_f64;
            let mut failures = 0.0_f64;
            for (src, kind) in edges {
                let source_belief = belief_of(bg, src, policy, memo, visiting);
                let mass = policy.edge_mass(*kind, source_belief);
                match kind {
                    EdgeKind::Supports => successes += mass,
                    EdgeKind::Contradicts | EdgeKind::Attacks => failures += mass,
                }
            }
            if successes == 0.0 && failures == 0.0 {
                // No effective evidence ⇒ the belief is exactly the stored prior.
                base
            } else {
                let k = policy.prior_strength.max(0.0);
                let prior = Distribution::Beta {
                    alpha: 1.0 + base * k,
                    beta: 1.0 + (1.0 - base) * k,
                };
                let evidence = Evidence::Bernoulli {
                    successes,
                    failures,
                };
                match bayesian_update(&prior, &evidence) {
                    Ok(posterior) => posterior.mean(),
                    Err(_) => base,
                }
            }
        }
        _ => base,
    };

    visiting.remove(id);
    memo.insert(id.to_string(), result);
    result
}

/// Build the justification tree for `seed` — the answer to `EXPLAIN BELIEF <id>`. The
/// posterior confidences are exact (computed via [`propagate_confidence`]); the tree is
/// depth-bounded so a deep/cyclic graph still terminates.
pub fn explain_belief(
    bg: &BeliefGraph,
    seed: &str,
    policy: &AuthorityPolicy,
) -> JustificationGraph {
    let mut memo: HashMap<String, f64> = HashMap::new();
    let mut visiting: HashSet<String> = HashSet::new();
    // Prime the memo so every rendered confidence matches propagate_confidence exactly.
    belief_of(bg, seed, policy, &mut memo, &mut visiting);

    let mut on_path: HashSet<String> = HashSet::new();
    let root = build_proof(bg, seed, &memo, &mut on_path, 0);
    JustificationGraph { root }
}

// `build_proof` reconstructs the tree from the already-primed `memo`, so it needs no
// `AuthorityPolicy` (the policy shaped the numbers during `belief_of`, not here).
fn build_proof(
    bg: &BeliefGraph,
    id: &str,
    memo: &HashMap<String, f64>,
    on_path: &mut HashSet<String>,
    depth: usize,
) -> ProofNode {
    let confidence = memo.get(id).copied().unwrap_or_else(|| prior_of(bg, id));
    let ins = bg.in_edges.get(id);
    let is_leaf = depth >= MAX_EXPLAIN_DEPTH
        || on_path.contains(id)
        || ins.map(|e| e.is_empty()).unwrap_or(true);
    if is_leaf {
        return ProofNode {
            claim: id.to_string(),
            rule: JustRule::Asserted,
            confidence,
            premises: Vec::new(),
        };
    }
    on_path.insert(id.to_string());
    let premises = ins
        .unwrap()
        .iter()
        .map(|(src, kind)| {
            let sub = build_proof(bg, src, memo, on_path, depth + 1);
            let rule = match kind {
                EdgeKind::Supports => JustRule::DerivedSupport,
                EdgeKind::Contradicts | EdgeKind::Attacks => JustRule::DerivedContradiction,
            };
            ProofNode {
                claim: src.clone(),
                rule,
                confidence: sub.confidence,
                premises: sub.premises,
            }
        })
        .collect();
    on_path.remove(id);
    ProofNode {
        claim: id.to_string(),
        rule: JustRule::BayesianUpdate,
        confidence,
        premises,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::EdgeKind;

    // A support chain e -> c raises c above its bare prior.
    #[test]
    fn support_raises_confidence() {
        let bg = BeliefGraph::from_parts(
            [("claim", 0.5), ("evidence", 0.9)],
            [("evidence", "claim", EdgeKind::Supports)],
        );
        let bs = propagate_confidence(&bg, "claim", &AuthorityPolicy::default());
        assert!(
            bs.confidence > 0.5,
            "support should raise belief, got {}",
            bs.confidence
        );
        assert_eq!(bs.supporting, vec!["evidence".to_string()]);
    }

    // A contradiction lowers the claim below its prior.
    #[test]
    fn contradiction_lowers_confidence() {
        let bg = BeliefGraph::from_parts(
            [("claim", 0.5), ("counter", 0.9)],
            [("counter", "claim", EdgeKind::Contradicts)],
        );
        let bs = propagate_confidence(&bg, "claim", &AuthorityPolicy::default());
        assert!(
            bs.confidence < 0.5,
            "contradiction should lower belief, got {}",
            bs.confidence
        );
        assert_eq!(bs.contradicting, vec!["counter".to_string()]);
    }

    // An attack discounts harder than a plain contradiction (attack_multiplier > 1).
    #[test]
    fn attack_discounts_more_than_contradiction() {
        let contra = BeliefGraph::from_parts(
            [("claim", 0.5), ("x", 0.9)],
            [("x", "claim", EdgeKind::Contradicts)],
        );
        let attack = BeliefGraph::from_parts(
            [("claim", 0.5), ("x", 0.9)],
            [("x", "claim", EdgeKind::Attacks)],
        );
        let p = AuthorityPolicy::default();
        let c = propagate_confidence(&contra, "claim", &p).confidence;
        let a = propagate_confidence(&attack, "claim", &p).confidence;
        assert!(
            a < c,
            "attack ({a}) should discount more than contradiction ({c})"
        );
    }

    // A support/attack CYCLE terminates and stays in [0,1].
    #[test]
    fn cycle_terminates() {
        let bg = BeliefGraph::from_parts(
            [("a", 0.6), ("b", 0.6)],
            [
                ("a", "b", EdgeKind::Supports),
                ("b", "a", EdgeKind::Attacks),
            ],
        );
        let bs = propagate_confidence(&bg, "a", &AuthorityPolicy::default());
        assert!((0.0..=1.0).contains(&bs.confidence));
    }

    // No evidence ⇒ the belief is exactly the stored prior.
    #[test]
    fn no_evidence_is_prior() {
        let bg = BeliefGraph::from_parts([("solo", 0.73)], Vec::<(&str, &str, EdgeKind)>::new());
        let bs = propagate_confidence(&bg, "solo", &AuthorityPolicy::default());
        assert!((bs.confidence - 0.73).abs() < 1e-9);
    }

    // EXPLAIN BELIEF renders a tree whose root confidence matches propagation.
    #[test]
    fn explain_matches_propagation() {
        let bg = BeliefGraph::from_parts(
            [("claim", 0.5), ("evidence", 0.9)],
            [("evidence", "claim", EdgeKind::Supports)],
        );
        let p = AuthorityPolicy::default();
        let bs = propagate_confidence(&bg, "claim", &p);
        let j = explain_belief(&bg, "claim", &p);
        assert_eq!(j.root.claim, "claim");
        assert_eq!(j.root.rule, JustRule::BayesianUpdate);
        assert!((j.root.confidence - bs.confidence).abs() < 1e-9);
        assert_eq!(j.root.premises.len(), 1);
        assert_eq!(j.root.premises[0].rule, JustRule::DerivedSupport);
    }
}
