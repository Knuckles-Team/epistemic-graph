//! Off-policy evaluation of a candidate head on bandit labels (EH-006, EH-023).
//!
//! The candidate policy is the head's greedy choice. Each admitted logged item
//! becomes an exact-propensity `LoggedDecision`; an item whose greedy option
//! the logging policy never plays lies OUTSIDE the support and is not
//! estimated on -- its mass is reported instead. Each requested estimator
//! reports its value, a Chebyshev interval (`value +- se / sqrt(delta)`, valid
//! for any reward distribution) and its effective sample size. Promotion is
//! blocked below the policy's minimum ESS, by any unsupported mass, and by an
//! estimate below the logging policy's own observed value (regression).
//! Per-option success rates are pooled class -> option (Beta-Binomial) and
//! reported only at `min_support` (k-anonymity for small counts).

use std::collections::BTreeMap;

use eg_types::decision::jobs::{OpeEstimateView, OpeEstimatorKind, PooledRate};
use eg_types::decision::statistical::dataset::{ItemLabel, LabelledDataset, LabelledItem};
use eg_types::decision::statistical::head::DecisionHeadBody;

use super::evaluate::{read_item, EvalSpec};
use super::head_eval::top_index;
use super::quant::{q32, unit_wire, value_of};
use super::refusal::RefusalResult;
use crate::detkernel::Propensity;
use crate::ope::{clipped_ips, doubly_robust, ips, snips, switch, LoggedDecision, OpeEstimate};
use crate::risk::{pool_hierarchy, BetaDistribution, ConcentrationBounds, GroupCounts, PoolTree};

/// Weight cap of clipped IPS.
pub const CLIP_CAP: f64 = 10.0;
/// Weight threshold of SWITCH.
pub const SWITCH_TAU: f64 = 10.0;
/// Interval half-width bound kept inside the fixed-point range.
const INTERVAL_LIMIT: f64 = 1_000_000.0;

/// What the bandit half of an evaluation produced.
#[derive(Debug, Clone, Default)]
pub(crate) struct BanditReport {
    pub(crate) estimates: Vec<OpeEstimateView>,
    pub(crate) pooled: Vec<PooledRate>,
    pub(crate) failed: Vec<String>,
}

struct Logged {
    supported: Vec<LoggedDecision>,
    unsupported_mass: f64,
    total: usize,
    observed_value: f64,
}

fn logged_decision(
    head: &DecisionHeadBody,
    dataset: &LabelledDataset,
    item: &LabelledItem,
) -> RefusalResult<Option<LoggedDecision>> {
    let ItemLabel::Logged(logged) = &item.label else {
        return Ok(None);
    };
    let (Some(action), Some(success)) =
        (item.index_of(&logged.executed), logged.evaluation.success)
    else {
        return Ok(None);
    };
    let Some(p) = read_item(head, dataset, item)?.probabilities else {
        return Ok(None);
    };
    let greedy = top_index(&p).unwrap_or(0);
    let target: Vec<f64> = (0..p.len())
        .map(|i| if i == greedy { 1.0 } else { 0.0 })
        .collect();
    let logging = logged
        .logging_propensities
        .iter()
        .map(|w| Propensity::new(w.numerator(), w.denominator()))
        .collect::<Result<Vec<_>, _>>()?;
    let reward = if success { 1.0 } else { 0.0 };
    Ok(Some(
        LoggedDecision::new(action, reward, logging, target)?.with_reward_model(p)?,
    ))
}

fn collect(
    head: &DecisionHeadBody,
    dataset: &LabelledDataset,
    items: &[&LabelledItem],
) -> RefusalResult<Logged> {
    let mut out = Logged {
        supported: Vec::new(),
        unsupported_mass: 0.0,
        total: 0,
        observed_value: 0.0,
    };
    for item in items {
        let Some(decision) = logged_decision(head, dataset, item)? else {
            continue;
        };
        out.total += 1;
        out.observed_value += decision.reward();
        out.unsupported_mass += decision.unsupported_mass();
        if decision.unsupported_action().is_none() {
            out.supported.push(decision);
        }
    }
    if out.total > 0 {
        out.unsupported_mass /= out.total as f64;
        out.observed_value /= out.total as f64;
    }
    Ok(out)
}

fn run(kind: OpeEstimatorKind, records: &[LoggedDecision]) -> RefusalResult<OpeEstimate> {
    Ok(match kind {
        OpeEstimatorKind::Ips => ips(records)?,
        OpeEstimatorKind::ClippedIps => clipped_ips(records, CLIP_CAP)?,
        OpeEstimatorKind::Snips => snips(records)?,
        OpeEstimatorKind::Switch => switch(records, SWITCH_TAU)?,
        OpeEstimatorKind::DoublyRobust => doubly_robust(records)?,
    })
}

fn bounded_q(value: f64) -> RefusalResult<eg_types::decision::QuantisedValue> {
    let finite = if value.is_finite() {
        value
    } else {
        value.signum() * INTERVAL_LIMIT
    };
    q32(finite.clamp(-INTERVAL_LIMIT, INTERVAL_LIMIT))
}

fn view(
    kind: OpeEstimatorKind,
    estimate: &OpeEstimate,
    delta: f64,
    unsupported: f64,
) -> RefusalResult<OpeEstimateView> {
    let half = estimate.std_error / delta.sqrt();
    Ok(OpeEstimateView {
        estimator: kind,
        value: bounded_q(estimate.value)?,
        lower: bounded_q(estimate.value - half)?,
        upper: bounded_q(estimate.value + half)?,
        effective_sample_size: bounded_q(estimate.effective_sample_size)?,
        unsupported_mass: unit_wire(unsupported)?,
    })
}

fn pooled(items: &[&LabelledItem], min_support: u64) -> RefusalResult<Vec<PooledRate>> {
    let mut counts: BTreeMap<String, BTreeMap<String, (u64, u64)>> = BTreeMap::new();
    for item in items {
        let ItemLabel::Logged(logged) = &item.label else {
            continue;
        };
        let Some(success) = logged.evaluation.success else {
            continue;
        };
        let cell = counts
            .entry(item.class_key.clone())
            .or_default()
            .entry(logged.executed.clone())
            .or_default();
        cell.0 += u64::from(success);
        cell.1 += 1;
    }
    let mut tree = BTreeMap::new();
    for (class, options) in &counts {
        let leaves = options
            .iter()
            .map(|(option, (s, t))| Ok((option.clone(), PoolTree::Leaf(GroupCounts::new(*s, *t)?))))
            .collect::<RefusalResult<BTreeMap<_, _>>>()?;
        tree.insert(class.clone(), PoolTree::Branch(leaves));
    }
    if tree.is_empty() {
        return Ok(Vec::new());
    }
    let root = pool_hierarchy(
        &PoolTree::Branch(tree),
        BetaDistribution::new(1.0, 1.0)?,
        ConcentrationBounds::new(1.0, 1_000.0)?,
    )?;
    let mut out = Vec::new();
    for (class, node) in &root.children {
        for (option, leaf) in &node.children {
            if leaf.counts.trials() >= min_support {
                out.push(PooledRate {
                    class_key: class.clone(),
                    option_id: option.clone(),
                    trials: leaf.counts.trials(),
                    posterior_mean: q32(leaf.posterior.mean())?,
                });
            }
        }
    }
    Ok(out)
}

fn estimate_gates(
    report: &mut BanditReport,
    logged: &Logged,
    spec: &EvalSpec,
) -> RefusalResult<()> {
    let delta = super::quant::rational_value(spec.statistical.delta);
    let min_ess = value_of(spec.statistical.min_ess);
    for &kind in spec.estimators {
        let estimate = run(kind, &logged.supported)?;
        if estimate.effective_sample_size < min_ess {
            report.failed.push(format!("ess:{kind:?}"));
        }
        if estimate.value < logged.observed_value {
            report
                .failed
                .push(format!("off_policy_regression:{kind:?}"));
        }
        report
            .estimates
            .push(view(kind, &estimate, delta, logged.unsupported_mass)?);
    }
    Ok(())
}

/// Estimate the candidate head's value and apply the promotion gates.
pub(crate) fn bandit_gates(
    head: &DecisionHeadBody,
    dataset: &LabelledDataset,
    items: &[&LabelledItem],
    spec: &EvalSpec,
) -> RefusalResult<BanditReport> {
    let logged = collect(head, dataset, items)?;
    let mut report = BanditReport {
        pooled: pooled(items, spec.statistical.min_support)?,
        ..BanditReport::default()
    };
    if logged.unsupported_mass > 0.0 {
        report.failed.push("unsupported_mass".to_string());
    }
    if spec.estimators.is_empty() {
        report.failed.push("no_estimator".to_string());
    }
    if logged.supported.is_empty() {
        report.failed.push("no_supported_records".to_string());
        return Ok(report);
    }
    estimate_gates(&mut report, &logged, spec)?;
    Ok(report)
}
