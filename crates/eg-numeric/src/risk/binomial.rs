//! Exact binomial tails and Clopper–Pearson intervals.
//!
//! `P(X <= k)` for `X ~ Bin(n, p)` is `I_{1-p}(n - k, k + 1)`. The two-sided
//! Clopper–Pearson interval at miscoverage `delta` is
//! `[B(delta/2; k, n-k+1), B(1-delta/2; k+1, n-k)]` with `B` the beta quantile,
//! `0` below for `k = 0` and `1` above for `k = n`. The upper end is computed as
//! `1 - B(delta/2; n-k, k+1)` so that small tails keep full precision.

use super::beta::{beta_quantile, regularized_incomplete_beta};
use super::counts::Counts;
use crate::detkernel::{validate, Level, StatResult};
use std::ops::Deref;

/// `successes` out of `trials`, with `1 <= trials` and `successes <= trials`.
/// `Deref`s to [`Counts`] for `successes()`/`trials()`, defined once there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BinomialCounts(Counts);

impl Deref for BinomialCounts {
    type Target = Counts;

    fn deref(&self) -> &Counts {
        &self.0
    }
}

impl BinomialCounts {
    /// Validate counts.
    pub fn new(successes: u64, trials: u64) -> StatResult<Self> {
        validate::parameter(trials >= 1, "trials", "trials >= 1")?;
        let counts = Counts::checked(successes, trials)?;
        Ok(Self(counts))
    }
}

/// Which ends of the interval are bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntervalSide {
    /// Both ends, each at `delta / 2`.
    TwoSided,
    /// Upper bound only at `delta`; the lower end is 0.
    Upper,
    /// Lower bound only at `delta`; the upper end is 1.
    Lower,
}

/// A Clopper–Pearson interval with the inputs it was built from.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BinomialInterval {
    /// Lower end.
    pub lower: f64,
    /// Upper end.
    pub upper: f64,
    /// The counts.
    pub counts: BinomialCounts,
    /// Miscoverage of the interval.
    pub delta: Level,
    /// Bounded ends.
    pub side: IntervalSide,
}

/// `P(X <= k)` for `X ~ Bin(n, p)`.
pub fn binomial_cdf(k: u64, n: u64, p: f64) -> StatResult<f64> {
    validate::all_unit_interval(&[p], "binomial p")?;
    if k >= n {
        return Ok(1.0);
    }
    regularized_incomplete_beta(1.0 - p, (n - k) as f64, k as f64 + 1.0)
}

fn lower_end(counts: BinomialCounts, tail: f64) -> StatResult<f64> {
    if counts.successes() == 0 {
        return Ok(0.0);
    }
    let failures = counts.trials() - counts.successes();
    beta_quantile(tail, counts.successes() as f64, failures as f64 + 1.0)
}

fn upper_end(counts: BinomialCounts, tail: f64) -> StatResult<f64> {
    if counts.successes() == counts.trials() {
        return Ok(1.0);
    }
    let failures = counts.trials() - counts.successes();
    Ok(1.0 - beta_quantile(tail, failures as f64, counts.successes() as f64 + 1.0)?)
}

/// The exact (conservative) Clopper–Pearson interval.
pub fn clopper_pearson(
    counts: BinomialCounts,
    delta: Level,
    side: IntervalSide,
) -> StatResult<BinomialInterval> {
    let d = delta.to_f64();
    let (lower, upper) = match side {
        IntervalSide::TwoSided => (lower_end(counts, d * 0.5)?, upper_end(counts, d * 0.5)?),
        IntervalSide::Upper => (0.0, upper_end(counts, d)?),
        IntervalSide::Lower => (lower_end(counts, d)?, 1.0),
    };
    Ok(BinomialInterval {
        lower,
        upper,
        counts,
        delta,
        side,
    })
}
