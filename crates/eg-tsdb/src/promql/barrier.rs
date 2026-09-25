//! PromQL barrier functions (EH-527): the probability that a series reaches a
//! level within a horizon, and the time until it does, from a first-passage law
//! fitted to the range window — the uncertainty-aware counterpart of
//! `predict_linear` for disk, quota, memory and error-budget exhaustion.
//!
//! * `barrier_hit_probability(v range-vector, level scalar, horizon_seconds scalar)`
//! * `time_to_exhaustion(v range-vector, level scalar, quantile scalar)` — seconds;
//!   `+Inf` when the series reaches the level with probability below `quantile`.
//!
//! Each series' drift and volatility are the maximum-likelihood fit of
//! arithmetic Brownian motion to its points (seconds on the time axis), and the
//! law is `eg_numeric::risk::barrier` — the one kernel every surface uses. A
//! series with fewer than three points is dropped (as `predict_linear` drops one
//! with fewer than two); so is the metric name.

use eg_numeric::risk::barrier::{
    first_passage_cdf, fit_drift_diffusion, time_to_barrier_quantile, Barrier, DriftDiffusion,
};

use super::{InstantSample, RangeSeries, METRIC_NAME, NS_PER_SEC};

/// The two barrier functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BarrierFn {
    HitProbability,
    TimeToExhaustion,
}

impl BarrierFn {
    /// The function a PromQL name selects.
    pub(super) fn named(name: &str) -> Option<Self> {
        match name {
            "barrier_hit_probability" => Some(Self::HitProbability),
            "time_to_exhaustion" => Some(Self::TimeToExhaustion),
            _ => None,
        }
    }
}

/// The fitted law of one series, anchored at its last point.
fn fitted(series: &RangeSeries, level: f64) -> Option<(Barrier, DriftDiffusion)> {
    let first = series.points.first()?.0;
    let times: Vec<f64> = series
        .points
        .iter()
        .map(|(ts, _)| (*ts - first) as f64 / NS_PER_SEC)
        .collect();
    let values: Vec<f64> = series.points.iter().map(|(_, v)| *v).collect();
    let fit = fit_drift_diffusion(&times, &values).ok()?;
    let x0 = *values.last()?;
    Some((Barrier { x0, level }, fit))
}

/// Evaluate `function` over each series with its scalar `level` and `argument`
/// (the horizon in seconds, or the quantile).
pub(super) fn evaluate(
    function: BarrierFn,
    series: Vec<RangeSeries>,
    level: f64,
    argument: f64,
) -> Vec<InstantSample> {
    series
        .into_iter()
        .filter_map(|s| {
            let (barrier, fit) = fitted(&s, level)?;
            let value = match function {
                BarrierFn::HitProbability => first_passage_cdf(barrier, fit, argument),
                BarrierFn::TimeToExhaustion => time_to_barrier_quantile(barrier, fit, argument)
                    .ok()?
                    .unwrap_or(f64::INFINITY),
            };
            let mut labels = s.labels;
            labels.remove(METRIC_NAME);
            Some(InstantSample { labels, value })
        })
        .collect()
}

/// Nanoseconds as the `Ts` the evaluator passes, for the tests.
#[cfg(test)]
pub(super) const SECOND: crate::point::Ts = 1_000_000_000;

#[cfg(test)]
mod tests;
