//! Adaptive conformal inference (Gibbs and Candès, 2021) for drifting streams.
//!
//! The working level is updated after every observed label:
//! `alpha_{t+1} = alpha_t + gamma (alpha - err_t)`, where `err_t = 1` when the
//! set missed. The set at level `alpha_t` is full for `alpha_t <= 0` and empty
//! for `alpha_t >= 1`. For any sequence whatsoever, the long-run miscoverage
//! satisfies `|misses / T - alpha| <= (max(alpha_1, 1 - alpha_1) + gamma) / (gamma T)`.
//! That deterministic bound is the only claim reported; it is not a coverage
//! guarantee for any single step.

use super::quantile::{threshold_at_rank, Threshold};
use crate::detkernel::reduce::sorted_ascending;
use crate::detkernel::{validate, Level, StatResult};

/// The running state of adaptive conformal inference.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdaptiveConformal {
    target: Level,
    gamma: f64,
    alpha_t: f64,
    steps: u64,
    misses: u64,
}

/// The long-run miscoverage report.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdaptiveReport {
    /// Target miscoverage.
    pub target: Level,
    /// Step size.
    pub gamma: f64,
    /// Labels observed.
    pub steps: u64,
    /// Sets that missed.
    pub misses: u64,
    /// `misses / steps` (0 before the first step).
    pub empirical_miscoverage: f64,
    /// `(max(alpha, 1 - alpha) + gamma) / (gamma * steps)`; infinite before the
    /// first step.
    pub miscoverage_bound: f64,
}

impl AdaptiveConformal {
    /// Start at `alpha_1 = target` with step size `0 < gamma <= 1`.
    pub fn new(target: Level, gamma: f64) -> StatResult<Self> {
        validate::parameter(gamma > 0.0 && gamma <= 1.0, "gamma", "0 < gamma <= 1")?;
        Ok(Self {
            target,
            gamma,
            alpha_t: target.to_f64(),
            steps: 0,
            misses: 0,
        })
    }

    /// The current working level `alpha_t`.
    pub fn working_level(&self) -> f64 {
        self.alpha_t
    }

    /// The threshold at `alpha_t` over calibration scores (for example a
    /// rolling window): rank `ceil((n + 1)(1 - alpha_t))`.
    pub fn threshold(&self, calibration_scores: &[f64]) -> StatResult<Threshold> {
        validate::all_finite(calibration_scores, "conformal scores")?;
        if self.alpha_t >= 1.0 {
            return Ok(Threshold::Empty);
        }
        if self.alpha_t <= 0.0 {
            return Ok(Threshold::Infinite);
        }
        let n = calibration_scores.len();
        let rank = ((n as f64 + 1.0) * (1.0 - self.alpha_t)).ceil();
        Ok(threshold_at_rank(&sorted_ascending(calibration_scores), rank as u64))
    }

    /// Record whether the last set covered the observed label.
    pub fn observe(&mut self, covered: bool) {
        let miss = if covered { 0.0 } else { 1.0 };
        self.alpha_t += self.gamma * (self.target.to_f64() - miss);
        self.steps += 1;
        self.misses += u64::from(!covered);
    }

    /// The deterministic long-run miscoverage report.
    pub fn report(&self) -> AdaptiveReport {
        let alpha = self.target.to_f64();
        let (empirical, bound) = if self.steps == 0 {
            (0.0, f64::INFINITY)
        } else {
            let steps = self.steps as f64;
            (
                self.misses as f64 / steps,
                (alpha.max(1.0 - alpha) + self.gamma) / (self.gamma * steps),
            )
        };
        AdaptiveReport {
            target: self.target,
            gamma: self.gamma,
            steps: self.steps,
            misses: self.misses,
            empirical_miscoverage: empirical,
            miscoverage_bound: bound,
        }
    }
}
