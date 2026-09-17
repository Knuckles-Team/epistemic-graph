//! Split-conformal quantiles.
//!
//! With calibration nonconformity scores `s_1..s_n` and level `alpha`, the
//! threshold is the `ceil((n + 1)(1 - alpha))`-th smallest score, or infinite
//! when that rank exceeds `n`. A new item's set contains every option whose
//! score is at most the threshold, and under exchangeability the true option is
//! in the set with probability at least `1 - alpha`. The rank is computed in
//! exact integers from the rational level.

use crate::detkernel::reduce::{order_ascending, serial_sum, sorted_ascending};
use crate::detkernel::{validate, Level, StatResult};
use crate::risk::beta::beta_interval;

/// The score cut-off of a conformal set: an option is admitted when its score
/// is at most the threshold.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Threshold {
    /// Admits nothing.
    Empty,
    /// Admits scores `<=` the value.
    Finite(f64),
    /// Admits everything (not enough calibration data for the level).
    Infinite,
}

impl Threshold {
    /// `true` when `score` is admitted.
    pub fn admits(self, score: f64) -> bool {
        match self {
            Threshold::Empty => false,
            Threshold::Finite(cut) => score <= cut,
            Threshold::Infinite => true,
        }
    }
}

/// A calibrated split-conformal threshold with its level and calibration size.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ConformalQuantile {
    alpha: Level,
    n_calibration: u64,
    threshold: Threshold,
}

impl ConformalQuantile {
    /// The miscoverage level.
    pub fn alpha(&self) -> Level {
        self.alpha
    }

    /// Calibration-set size.
    pub fn n_calibration(&self) -> u64 {
        self.n_calibration
    }

    /// The threshold.
    pub fn threshold(&self) -> Threshold {
        self.threshold
    }

    /// A symmetric interval `[center - q, center + q]` for absolute-residual
    /// scores; `(-inf, inf)` when the threshold is infinite.
    pub fn interval(&self, center: f64) -> (f64, f64) {
        match self.threshold {
            Threshold::Empty => (center, center),
            Threshold::Finite(q) => (center - q, center + q),
            Threshold::Infinite => (f64::NEG_INFINITY, f64::INFINITY),
        }
    }
}

/// The threshold at a 1-based rank over ascending scores.
pub(crate) fn threshold_at_rank(ascending: &[f64], rank: u64) -> Threshold {
    if rank == 0 {
        Threshold::Empty
    } else if rank > ascending.len() as u64 {
        Threshold::Infinite
    } else {
        Threshold::Finite(ascending[rank as usize - 1])
    }
}

fn checked_scores(scores: &[f64]) -> StatResult<()> {
    validate::non_empty(scores, "conformal scores")?;
    validate::all_finite(scores, "conformal scores")
}

/// Calibrate a split-conformal threshold.
pub fn split_conformal(scores: &[f64], alpha: Level) -> StatResult<ConformalQuantile> {
    checked_scores(scores)?;
    let n = scores.len() as u64;
    let ascending = sorted_ascending(scores);
    Ok(ConformalQuantile {
        alpha,
        n_calibration: n,
        threshold: threshold_at_rank(&ascending, alpha.conformal_rank(n)),
    })
}

/// Weighted split conformal (Tibshirani et al. 2019): calibration item `i` has
/// weight `w_i` and the test item `w_test`, for example inverse inclusion
/// probabilities of audit-sampled labels. The threshold is the smallest score at
/// which the normalised weight mass reaches `1 - alpha`, with the test weight
/// placed at `+inf`.
pub fn weighted_split_conformal(
    scores: &[f64],
    weights: &[f64],
    test_weight: f64,
    alpha: Level,
) -> StatResult<ConformalQuantile> {
    checked_scores(scores)?;
    validate::same_len(scores.len(), weights.len(), "conformal weights")?;
    validate::all_finite(weights, "conformal weights")?;
    let non_negative = weights.iter().all(|&w| w >= 0.0);
    validate::parameter(non_negative, "weights", "every weight >= 0")?;
    let test_ok = test_weight.is_finite() && test_weight >= 0.0;
    validate::parameter(test_ok, "test_weight", "finite and >= 0")?;
    let total = serial_sum(weights) + test_weight;
    validate::parameter(total > 0.0, "weights", "positive total")?;
    let (num, den) = (alpha.rational().numerator(), alpha.rational().denominator());
    let needed = (den - num) as f64 * total;
    let mut cumulative = 0.0;
    let mut threshold = Threshold::Infinite;
    for index in order_ascending(scores) {
        cumulative += weights[index];
        if cumulative * den as f64 >= needed {
            threshold = Threshold::Finite(scores[index]);
            break;
        }
    }
    Ok(ConformalQuantile {
        alpha,
        n_calibration: scores.len() as u64,
        threshold,
    })
}

/// Equal-tailed interval (total tail mass `delta`) for the realised coverage of
/// a split-conformal set built from `n` calibration items at `alpha`: coverage
/// is `Beta(l, n + 1 - l)` with `l = ceil((n + 1)(1 - alpha))`. `None` when the
/// set is trivial (`l > n`, coverage 1).
pub fn realised_coverage_interval(alpha: Level, n: u64, delta: Level) -> StatResult<Option<(f64, f64)>> {
    let rank = alpha.conformal_rank(n);
    if rank > n {
        return Ok(None);
    }
    beta_interval(rank as f64, (n + 1 - rank) as f64, delta).map(Some)
}
