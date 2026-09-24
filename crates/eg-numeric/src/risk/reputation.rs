//! Learned reputation (EH-525, ANALYTICS-HARVEST AH-05): a subject's success
//! probability, learned from its independently evaluated outcomes.
//!
//! * The population prior is EMPIRICAL BAYES over every subject's full history: the
//!   subjects are siblings under one root in [`pool_hierarchy`], so a subject with few
//!   outcomes shrinks toward the population and one with many keeps its own rate.
//! * The subject's own evidence is DISCOUNTED: an outcome observed `age` before `now`
//!   weighs `0.5^(age / half_life)`, so a subject that drifted is re-learned (no
//!   half-life ⇒ every outcome weighs 1).
//! * Below the [`SampleGate`] the answer is [`Reputation::InsufficientHistory`], never a
//!   number — the gate counts raw independent outcomes, not discounted weight.
//!
//! Subjects are visited in key order and every sum is serial, so the relation this
//! feeds replays bit-identically.

use std::collections::BTreeMap;

use super::pooling::{
    pool_hierarchy, BetaDistribution, ConcentrationBounds, GroupCounts, PoolTree, PooledNode,
};
use super::sample_gate::SampleGate;
use crate::detkernel::{math, Level, StatResult};

/// One independent outcome of a subject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Observation<'a> {
    pub subject: &'a str,
    pub at_ms: u64,
    pub success: bool,
}

/// How reputations are learned.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReputationRules {
    /// Outcome half-life; `None` weighs every outcome equally.
    pub half_life_ms: Option<u64>,
    /// The instant ages are measured from.
    pub now_ms: u64,
    /// Minimum independent outcomes before an estimate is made.
    pub gate: SampleGate,
    /// Total tail mass of the credible interval.
    pub delta: Level,
    /// Clamp on the fitted population concentration.
    pub bounds: ConcentrationBounds,
}

/// A learned estimate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Estimate {
    /// Posterior mean.
    pub mean: f64,
    pub lower: f64,
    pub upper: f64,
    /// The population prior's mean this subject shrank toward.
    pub prior_mean: f64,
    /// The prior's concentration (pseudo-count strength).
    pub prior_strength: f64,
}

/// A subject's reputation, or an abstention.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Reputation {
    Estimated(Estimate),
    /// Fewer independent outcomes than the gate requires.
    InsufficientHistory {
        n_min: u64,
    },
}

/// One subject's row.
#[derive(Debug, Clone, PartialEq)]
pub struct SubjectReputation {
    pub subject: String,
    /// Independent outcomes (undiscounted).
    pub trials: u64,
    pub successes: u64,
    /// `Σ weight` and `Σ weight · success` after discounting.
    pub weight: f64,
    pub weighted_successes: f64,
    pub reputation: Reputation,
}

#[derive(Default)]
struct Tally {
    trials: u64,
    successes: u64,
    weight: f64,
    weighted_successes: f64,
}

fn discount(rules: &ReputationRules, at_ms: u64) -> f64 {
    match rules.half_life_ms {
        Some(half_life) if half_life > 0 => {
            let age = rules.now_ms.saturating_sub(at_ms) as f64;
            math::pow(0.5, age / half_life as f64)
        }
        _ => 1.0,
    }
}

fn tallies<'a>(
    observations: &[Observation<'a>],
    rules: &ReputationRules,
) -> BTreeMap<&'a str, Tally> {
    let mut out: BTreeMap<&str, Tally> = BTreeMap::new();
    for observation in observations {
        let tally = out.entry(observation.subject).or_default();
        let weight = discount(rules, observation.at_ms);
        tally.trials += 1;
        tally.successes += u64::from(observation.success);
        tally.weight += weight;
        if observation.success {
            tally.weighted_successes += weight;
        }
    }
    out
}

fn pooled(tallies: &BTreeMap<&str, Tally>, bounds: ConcentrationBounds) -> StatResult<PooledNode> {
    let leaves = tallies
        .iter()
        .map(|(subject, t)| {
            Ok((
                subject.to_string(),
                PoolTree::Leaf(GroupCounts::new(t.successes, t.trials)?),
            ))
        })
        .collect::<StatResult<BTreeMap<_, _>>>()?;
    pool_hierarchy(
        &PoolTree::Branch(leaves),
        BetaDistribution::new(1.0, 1.0)?,
        bounds,
    )
}

/// The subject's prior: its pooled posterior with its own counts taken back out.
fn prior_of(node: &PooledNode) -> StatResult<BetaDistribution> {
    let failures = (node.counts.trials() - node.counts.successes()) as f64;
    BetaDistribution::new(
        node.posterior.alpha() - node.counts.successes() as f64,
        node.posterior.beta() - failures,
    )
}

/// `prior` updated with `(weighted_successes, weight)` discounted pseudo-counts.
fn updated(
    prior: BetaDistribution,
    weighted_successes: f64,
    weight: f64,
    delta: Level,
) -> StatResult<Estimate> {
    let posterior = BetaDistribution::new(
        prior.alpha() + weighted_successes,
        prior.beta() + (weight - weighted_successes).max(0.0),
    )?;
    let (lower, upper) = posterior.credible_interval(delta)?;
    Ok(Estimate {
        mean: posterior.mean(),
        lower,
        upper,
        prior_mean: prior.mean(),
        prior_strength: prior.concentration(),
    })
}

fn estimate(node: &PooledNode, tally: &Tally, rules: &ReputationRules) -> StatResult<Reputation> {
    if !rules.gate.admits(tally.trials) {
        return Ok(Reputation::InsufficientHistory {
            n_min: rules.gate.n_min(),
        });
    }
    let prior = prior_of(node)?;
    updated(prior, tally.weighted_successes, tally.weight, rules.delta).map(Reputation::Estimated)
}

/// Learn every observed subject's reputation, in subject order.
pub fn learn_reputation(
    observations: &[Observation],
    rules: &ReputationRules,
) -> StatResult<Vec<SubjectReputation>> {
    let tallies = tallies(observations, rules);
    if tallies.is_empty() {
        return Ok(Vec::new());
    }
    let root = pooled(&tallies, rules.bounds)?;
    tallies
        .iter()
        .map(|(subject, tally)| {
            let node = &root.children[*subject];
            Ok(SubjectReputation {
                subject: subject.to_string(),
                trials: tally.trials,
                successes: tally.successes,
                weight: tally.weight,
                weighted_successes: tally.weighted_successes,
                reputation: estimate(node, tally, rules)?,
            })
        })
        .collect()
}

/// A prior-anchored reliability: `prior_mean` held with `prior_strength` pseudo-counts
/// (a stored constant or a belief-graph confidence), updated with a subject's learned,
/// discounted evidence. With no evidence it is the prior mean itself.
pub fn anchored_reliability(
    prior_mean: f64,
    prior_strength: f64,
    row: &SubjectReputation,
    delta: Level,
) -> StatResult<Estimate> {
    let prior = BetaDistribution::new(
        (prior_mean * prior_strength).max(f64::MIN_POSITIVE),
        ((1.0 - prior_mean) * prior_strength).max(f64::MIN_POSITIVE),
    )?;
    updated(prior, row.weighted_successes, row.weight, delta)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(half_life_ms: Option<u64>, n_min: u64) -> ReputationRules {
        ReputationRules {
            half_life_ms,
            now_ms: 1_000_000,
            gate: SampleGate::new(n_min).unwrap(),
            delta: Level::new(1, 20).unwrap(),
            bounds: ConcentrationBounds::new(1.0, 1_000.0).unwrap(),
        }
    }

    fn outcomes(subject: &str, successes: usize, failures: usize) -> Vec<Observation<'_>> {
        let mut out = Vec::new();
        for i in 0..successes + failures {
            out.push(Observation {
                subject,
                at_ms: 1_000_000,
                success: i < successes,
            });
        }
        out
    }

    fn estimated(row: &SubjectReputation) -> Estimate {
        match row.reputation {
            Reputation::Estimated(e) => e,
            Reputation::InsufficientHistory { .. } => panic!("{} abstained", row.subject),
        }
    }

    #[test]
    fn the_posterior_is_the_conjugate_closed_form() {
        let mut obs = outcomes("a", 7, 3);
        obs.extend(outcomes("b", 2, 8));
        let rows = learn_reputation(&obs, &rules(None, 1)).unwrap();
        for row in &rows {
            let e = estimated(row);
            let alpha = e.prior_mean * e.prior_strength + row.successes as f64;
            let beta =
                (1.0 - e.prior_mean) * e.prior_strength + (row.trials - row.successes) as f64;
            assert!(
                (e.mean - alpha / (alpha + beta)).abs() < 1e-12,
                "{}",
                row.subject
            );
            assert!(e.lower < e.mean && e.mean < e.upper);
        }
    }

    #[test]
    fn shrinkage_toward_the_population_weakens_as_history_grows() {
        let mut gap = f64::INFINITY;
        for scale in [1usize, 4, 16, 64] {
            let mut obs = outcomes("x", 9 * scale, scale);
            obs.extend(outcomes("y", 5, 5));
            obs.extend(outcomes("z", 4, 6));
            let rows = learn_reputation(&obs, &rules(None, 1)).unwrap();
            let x = rows.iter().find(|r| r.subject == "x").unwrap();
            let own = x.successes as f64 / x.trials as f64;
            let now = (estimated(x).mean - own).abs();
            assert!(now < gap, "shrinkage must fall with n: {now} >= {gap}");
            gap = now;
        }
    }

    #[test]
    fn discounting_reaches_a_steady_state_weight() {
        let half_life = 1_000u64;
        let obs: Vec<Observation> = (0..200u64)
            .map(|k| Observation {
                subject: "s",
                at_ms: 1_000_000 - k * 100,
                success: true,
            })
            .collect();
        let row = &learn_reputation(&obs, &rules(Some(half_life), 1)).unwrap()[0];
        // A geometric series with ratio 0.5^(100/1000): its limit is 1 / (1 - r).
        let limit = 1.0 / (1.0 - math::pow(0.5, 0.1));
        assert!(
            (row.weight - limit).abs() / limit < 1e-5,
            "{} vs {limit}",
            row.weight
        );
        assert_eq!(row.trials, 200);
    }

    #[test]
    fn below_the_gate_there_is_no_number() {
        let rows = learn_reputation(&outcomes("new", 2, 1), &rules(None, 5)).unwrap();
        assert_eq!(
            rows[0].reputation,
            Reputation::InsufficientHistory { n_min: 5 }
        );
    }

    #[test]
    fn an_anchored_reliability_without_evidence_is_its_prior() {
        let row = SubjectReputation {
            subject: "s".into(),
            trials: 0,
            successes: 0,
            weight: 0.0,
            weighted_successes: 0.0,
            reputation: Reputation::InsufficientHistory { n_min: 1 },
        };
        let e = anchored_reliability(0.8, 10.0, &row, Level::new(1, 20).unwrap()).unwrap();
        assert!((e.mean - 0.8).abs() < 1e-12);
    }
}
