//! Off-policy value estimators over logged decisions, all refusing records
//! outside the logging support.
//!
//! With `w_i = pi(a_i) / mu(a_i)`:
//! * IPS: `mean_i w_i r_i` (unbiased within support);
//! * clipped IPS: `mean_i min(w_i, cap) r_i`;
//! * SNIPS: `sum_i w_i r_i / sum_i w_i`;
//! * SWITCH: IPS on actions with weight `<= tau`, the reward model elsewhere;
//! * doubly robust: `mean_i [sum_a pi(a) q(a) + w_i (r_i - q(a_i))]` (unbiased
//!   within support whatever the reward model `q`).
//!
//! Every estimate reports its effective sample size `(sum w)^2 / sum w^2` over
//! the weights it used; promotion gates compare that with a minimum.

use super::logged::{require_support, LoggedDecision};
use crate::detkernel::reduce::{serial_mean, serial_sum};
use crate::detkernel::{validate, StatError, StatResult};

/// The estimator behind an estimate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Estimator {
    /// Inverse propensity scoring.
    Ips,
    /// IPS with weights capped at `cap`.
    ClippedIps { cap: f64 },
    /// Self-normalised IPS.
    Snips,
    /// SWITCH with weight threshold `tau`.
    Switch { tau: f64 },
    /// Doubly robust.
    DoublyRobust,
}

/// An off-policy value estimate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpeEstimate {
    /// Estimator used.
    pub estimator: Estimator,
    /// Estimated policy value.
    pub value: f64,
    /// Standard error (infinite for a single record).
    pub std_error: f64,
    /// Records used.
    pub n: u64,
    /// `(sum w)^2 / sum w^2` over the weights used (0 when all weights are 0).
    pub effective_sample_size: f64,
    /// Largest weight used.
    pub max_weight: f64,
}

/// `(sum w)^2 / sum w^2`, 0 when every weight is 0.
pub fn effective_sample_size(weights: &[f64]) -> f64 {
    let squares: Vec<f64> = weights.iter().map(|w| w * w).collect();
    let square_sum = serial_sum(&squares);
    if square_sum == 0.0 {
        return 0.0;
    }
    let sum = serial_sum(weights);
    sum * sum / square_sum
}

fn standard_error(contributions: &[f64], mean: f64) -> f64 {
    if contributions.len() < 2 {
        return f64::INFINITY;
    }
    let squares: Vec<f64> = contributions.iter().map(|c| (c - mean) * (c - mean)).collect();
    let n = contributions.len() as f64;
    (serial_sum(&squares) / (n - 1.0) / n).sqrt()
}

fn estimate(estimator: Estimator, contributions: &[f64], weights: &[f64]) -> StatResult<OpeEstimate> {
    let value = serial_mean(contributions, "logged decisions")?;
    Ok(OpeEstimate {
        estimator,
        value,
        std_error: standard_error(contributions, value),
        n: contributions.len() as u64,
        effective_sample_size: effective_sample_size(weights),
        max_weight: weights.iter().copied().fold(0.0, f64::max),
    })
}

fn positive_cap(cap: f64, name: &'static str) -> StatResult<()> {
    validate::parameter(cap.is_finite() && cap > 0.0, name, "finite and > 0")
}

fn weighted_rewards(
    records: &[LoggedDecision],
    estimator: Estimator,
    weight: impl Fn(&LoggedDecision) -> f64,
) -> StatResult<OpeEstimate> {
    require_support(records)?;
    let weights: Vec<f64> = records.iter().map(weight).collect();
    let contributions: Vec<f64> = records.iter().zip(&weights).map(|(r, w)| w * r.reward()).collect();
    estimate(estimator, &contributions, &weights)
}

/// Inverse propensity scoring.
pub fn ips(records: &[LoggedDecision]) -> StatResult<OpeEstimate> {
    weighted_rewards(records, Estimator::Ips, LoggedDecision::executed_weight)
}

/// IPS with every weight capped at `cap > 0`.
pub fn clipped_ips(records: &[LoggedDecision], cap: f64) -> StatResult<OpeEstimate> {
    positive_cap(cap, "cap")?;
    weighted_rewards(records, Estimator::ClippedIps { cap }, |r| r.executed_weight().min(cap))
}

/// Self-normalised IPS; refuses a log whose weights sum to 0.
pub fn snips(records: &[LoggedDecision]) -> StatResult<OpeEstimate> {
    require_support(records)?;
    let weights: Vec<f64> = records.iter().map(LoggedDecision::executed_weight).collect();
    let total = serial_sum(&weights);
    validate::parameter(total > 0.0, "weights", "positive total weight")?;
    let n = records.len() as f64;
    let contributions: Vec<f64> = records
        .iter()
        .zip(&weights)
        .map(|(r, w)| w * r.reward() * n / total)
        .collect();
    let mut result = estimate(Estimator::Snips, &contributions, &weights)?;
    let residuals: Vec<f64> = records
        .iter()
        .zip(&weights)
        .map(|(r, w)| w * (r.reward() - result.value) * n / total)
        .collect();
    result.std_error = standard_error(&residuals, 0.0);
    Ok(result)
}

fn reward_model(record: &LoggedDecision) -> StatResult<&[f64]> {
    record.reward_model().ok_or(StatError::InvalidParameter {
        name: "reward_model",
        requirement: "present on every record",
    })
}

/// `sum_a pi(a) q(a)` over the actions selected by `include`.
fn model_value(record: &LoggedDecision, model: &[f64], include: impl Fn(usize) -> bool) -> f64 {
    let terms: Vec<f64> = (0..model.len())
        .filter(|&a| include(a))
        .map(|a| record.target()[a] * model[a])
        .collect();
    serial_sum(&terms)
}

/// Run `per_record` over every record, collecting its `(contribution, weight)`
/// pair into the two parallel vectors [`estimate`] expects. Shared by
/// [`switch`] and [`doubly_robust`], whose only difference is this closure.
fn accumulate(
    records: &[LoggedDecision],
    mut per_record: impl FnMut(&LoggedDecision) -> StatResult<(f64, f64)>,
) -> StatResult<(Vec<f64>, Vec<f64>)> {
    let mut contributions = Vec::with_capacity(records.len());
    let mut weights = Vec::with_capacity(records.len());
    for record in records {
        let (contribution, weight) = per_record(record)?;
        contributions.push(contribution);
        weights.push(weight);
    }
    Ok((contributions, weights))
}

/// One record's SWITCH contribution and weight at threshold `tau`: the model
/// term for actions whose weight exceeds `tau`, plus the executed weight's own
/// contribution when it does not.
fn switch_contribution(record: &LoggedDecision, tau: f64) -> StatResult<(f64, f64)> {
    let model = reward_model(record)?;
    let above = |a: usize| record.weight_of(a).is_some_and(|w| w > tau);
    let w = record.executed_weight();
    let used = if w <= tau { w } else { 0.0 };
    Ok((model_value(record, model, above) + used * record.reward(), used))
}

/// SWITCH estimator with weight threshold `tau > 0`.
pub fn switch(records: &[LoggedDecision], tau: f64) -> StatResult<OpeEstimate> {
    positive_cap(tau, "tau")?;
    require_support(records)?;
    let (contributions, weights) = accumulate(records, |record| switch_contribution(record, tau))?;
    estimate(Estimator::Switch { tau }, &contributions, &weights)
}

/// One record's doubly robust contribution and weight: the full model value
/// plus the executed action's importance-weighted correction.
fn doubly_robust_contribution(record: &LoggedDecision) -> StatResult<(f64, f64)> {
    let model = reward_model(record)?;
    let w = record.executed_weight();
    let correction = w * (record.reward() - model[record.action()]);
    Ok((model_value(record, model, |_| true) + correction, w))
}

/// Doubly robust estimator.
pub fn doubly_robust(records: &[LoggedDecision]) -> StatResult<OpeEstimate> {
    require_support(records)?;
    let (contributions, weights) = accumulate(records, doubly_robust_contribution)?;
    estimate(Estimator::DoublyRobust, &contributions, &weights)
}

/// A minimum effective sample size for promotion.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EssGate {
    minimum: f64,
}

impl EssGate {
    /// `minimum >= 1`, finite.
    pub fn new(minimum: f64) -> StatResult<Self> {
        validate::parameter(minimum.is_finite() && minimum >= 1.0, "minimum", "finite and >= 1")?;
        Ok(Self { minimum })
    }

    /// Refuse an estimate below the minimum effective sample size.
    pub fn check(self, estimate: &OpeEstimate) -> StatResult<()> {
        if estimate.effective_sample_size >= self.minimum {
            return Ok(());
        }
        Err(StatError::InsufficientSamples {
            what: "effective sample size",
            required: self.minimum.ceil() as u64,
            actual: estimate.effective_sample_size.floor() as u64,
        })
    }
}
