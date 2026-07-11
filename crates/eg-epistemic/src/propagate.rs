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
    AuthorityPolicy, BeliefState, Calibration, EdgeKind, JustRule, JustificationGraph, ProofNode,
};

/// Default belief for a node with no stored confidence — maximal ignorance.
const DEFAULT_PRIOR: f64 = 0.5;
/// Depth cap for the justification tree, so `EXPLAIN BELIEF` terminates on a deep or
/// cyclic evidence graph (the belief number itself is exact; only the rendered tree is
/// depth-bounded).
const MAX_EXPLAIN_DEPTH: usize = 32;
/// Default credible-mass for [`Calibration::level`] (EPI-P3-3) — the conventional
/// 95% central credible interval, consistent with the `Distribution::credible_interval`
/// doc-tested default elsewhere in the engine.
const DEFAULT_CALIBRATION_LEVEL: f64 = 0.95;

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
    // EPI-P3-3 calibration: re-derive the SAME top-level Beta posterior `belief_of`
    // folded into `confidence` (reusing the now-memoized point beliefs of every
    // premise, so this is one cheap O(in-edges) pass, not a re-walk of the graph),
    // and report its credible interval. `None` when there was no evidence to
    // update on — `confidence` is then exactly the stored prior, nothing to
    // calibrate beyond it.
    let calibration =
        top_level_posterior(bg, seed, policy, &memo).map(|(dist, evidence_count)| Calibration {
            interval: dist.credible_interval(DEFAULT_CALIBRATION_LEVEL),
            level: DEFAULT_CALIBRATION_LEVEL,
            evidence_count,
        });
    BeliefState {
        node_id: seed.to_string(),
        confidence,
        supporting,
        contradicting,
        attacking,
        as_of: bg.as_of,
        calibration,
    }
}

/// Recompute `id`'s Beta posterior distribution (not just its `.mean()`) from
/// its ALREADY-memoized premise beliefs — the exact same combination step
/// `belief_of` performs, just returning the full [`Distribution`] instead of
/// collapsing it to a point. Returns `None` when `id` has no in-edges or none
/// of them carry effective mass (mirrors `belief_of`'s "no evidence ⇒ prior"
/// branch exactly, so `Some`/`None` here matches whether `belief_of` actually
/// ran the conjugate update for `id`).
fn top_level_posterior(
    bg: &BeliefGraph,
    id: &str,
    policy: &AuthorityPolicy,
    point_beliefs: &HashMap<String, f64>,
) -> Option<(Distribution, usize)> {
    let edges = bg.in_edges.get(id)?;
    if edges.is_empty() {
        return None;
    }
    let mut successes = 0.0_f64;
    let mut failures = 0.0_f64;
    for (src, kind) in edges {
        let source_belief = point_beliefs
            .get(src)
            .copied()
            .unwrap_or_else(|| prior_of(bg, src));
        let mass = policy.edge_mass(*kind, source_belief);
        match kind {
            EdgeKind::Supports => successes += mass,
            EdgeKind::Contradicts | EdgeKind::Attacks => failures += mass,
        }
    }
    if successes == 0.0 && failures == 0.0 {
        return None;
    }
    let base = prior_of(bg, id);
    let k = policy.prior_strength.max(0.0);
    let prior = Distribution::Beta {
        alpha: 1.0 + base * k,
        beta: 1.0 + (1.0 - base) * k,
    };
    let evidence = Evidence::Bernoulli {
        successes,
        failures,
    };
    bayesian_update(&prior, &evidence)
        .ok()
        .map(|posterior| (posterior, edges.len()))
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

    // A belief with real evidence carries a calibrated credible interval whose
    // mean matches the reported point confidence, and it narrows toward the
    // stored prior (unlike a fabricated fixed-width band).
    #[test]
    fn belief_with_evidence_carries_calibrated_interval() {
        let bg = BeliefGraph::from_parts(
            [("claim", 0.5), ("evidence", 0.9)],
            [("evidence", "claim", EdgeKind::Supports)],
        );
        let bs = propagate_confidence(&bg, "claim", &AuthorityPolicy::default());
        let cal = bs
            .calibration
            .expect("evidence ⇒ a real calibration signal");
        assert_eq!(cal.evidence_count, 1);
        assert!((cal.level - DEFAULT_CALIBRATION_LEVEL).abs() < 1e-9);
        let (lo, hi) = cal.interval;
        assert!(
            (0.0..=1.0).contains(&lo) && (0.0..=1.0).contains(&hi) && lo < hi,
            "interval ({lo}, {hi}) must be a valid sub-interval of [0,1]"
        );
        // The interval must bracket the point confidence it was derived from.
        assert!(
            lo <= bs.confidence && bs.confidence <= hi,
            "interval ({lo}, {hi}) must bracket confidence {}",
            bs.confidence
        );
    }

    // No evidence ⇒ no calibration signal (an honest `None`, not a fabricated
    // zero-width interval around the bare prior).
    #[test]
    fn belief_with_no_evidence_has_no_calibration() {
        let bg = BeliefGraph::from_parts([("solo", 0.73)], Vec::<(&str, &str, EdgeKind)>::new());
        let bs = propagate_confidence(&bg, "solo", &AuthorityPolicy::default());
        assert!(bs.calibration.is_none());
    }

    // More corroborating evidence tightens the calibrated interval — the
    // reliability/evidence-count signal the calibration slot documents.
    #[test]
    fn more_evidence_narrows_the_calibrated_interval() {
        let one = BeliefGraph::from_parts(
            [("claim", 0.5), ("e1", 0.9)],
            [("e1", "claim", EdgeKind::Supports)],
        );
        let three = BeliefGraph::from_parts(
            [("claim", 0.5), ("e1", 0.9), ("e2", 0.9), ("e3", 0.9)],
            [
                ("e1", "claim", EdgeKind::Supports),
                ("e2", "claim", EdgeKind::Supports),
                ("e3", "claim", EdgeKind::Supports),
            ],
        );
        let p = AuthorityPolicy::default();
        let bs_one = propagate_confidence(&one, "claim", &p);
        let bs_three = propagate_confidence(&three, "claim", &p);
        let width_one =
            bs_one.calibration.unwrap().interval.1 - bs_one.calibration.unwrap().interval.0;
        let width_three =
            bs_three.calibration.unwrap().interval.1 - bs_three.calibration.unwrap().interval.0;
        assert!(
            width_three < width_one,
            "3 corroborating sources ({width_three}) should calibrate tighter than 1 ({width_one})"
        );
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
