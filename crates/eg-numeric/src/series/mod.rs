//! Incremental series kernels (EH-522, ANALYTICS-HARVEST-20260924 §4 AH-02) — the ONE
//! implementation behind UQL `DERIVE`, the SQL `eg_*` window functions, the PromQL
//! functions whose semantics coincide, and the generic finance signal kernels.
//!
//! Every kernel is a step state machine: [`State::step`] consumes one observation and
//! returns the output at that observation, or `None` while it warms up. The whole state is
//! plain serde data, so it IS the checkpoint: advancing a restored state gives exactly the
//! values a whole-history run gives (the same code runs both ways).
//!
//! Conventions (fixed here, documented once):
//! * a missing input (`None`) is skipped — the output is `None` and the state is untouched,
//!   so a window counts VALID observations and each derived feature keeps its own warm-up;
//! * the rolling mean / std / sum / z-score, `rcorr` and `wsum` run on exact wide-integer
//!   running sums (EH-562): O(1) per step, no add/remove residue, each output rounded once
//!   from the exact value — a flat window has exactly zero deviation;
//! * `rstd` and `zscore` use the population deviation (`ddof = 0`, PromQL
//!   `stddev_over_time`); a deviation below [`window::STD_FLOOR`] makes the z-score `0`;
//! * `ewma` is the recursive form seeded with the first observation (pandas
//!   `ewm(adjust=False)`), `α = 2/(span+1)` or `α = 1 − 2^(−1/halflife)`;
//! * `rank` is the average 1-based rank of the newest value in its window (pandas
//!   `rolling(w).rank()`); `rcorr` is Pearson and `ic` Spearman (Pearson of window ranks).
//!
//! Transcendentals go through [`crate::detkernel::math`], so outputs are bit-identical on
//! every release target.

pub mod distance;
mod exact;
#[cfg(feature = "motif")]
mod fft;
pub mod kalman;
mod kernel;
#[cfg(feature = "motif")]
pub mod mass;
#[cfg(feature = "motif")]
pub mod matrix_profile;
pub mod stampi;
mod wide;
pub mod window;

use serde::{Deserialize, Serialize};

pub use kernel::State;

use crate::error::{NumericError, Result};

/// Largest window / lag a kernel accepts (its state is O(window)).
pub const MAX_WINDOW: usize = 1 << 20;

/// A kernel over the last `k` valid observations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Shift {
    /// `x[t-k]`.
    Lag,
    /// `x[t] - x[t-k]`.
    Diff,
    /// `x[t] / x[t-k] - 1`.
    Ret,
    /// `ln(x[t] / x[t-k])` (both positive).
    LogRet,
}

/// A statistic over a sliding window of the last `w` valid observations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Rolling {
    Mean,
    Std,
    Sum,
    Min,
    Max,
    Rank,
    Zscore,
}

/// EWMA smoothing, by span or by half-life (in observations).
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Smoothing {
    Span(f64),
    HalfLife(f64),
}

/// A stateless per-observation map.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Map {
    Abs,
    Sign,
    Neg,
    Clip { lo: f64, hi: f64 },
}

/// Pointwise arithmetic of two series (`None` on a zero divisor). The comparisons are
/// indicator series (`1` where the relation holds, else `0`) — the shape predicates a
/// UQL `EVENTS` stage turns into CEP events (EH-529).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Arith {
    Add,
    Sub,
    Mul,
    Div,
    Gt,
    Lt,
    Max,
    Min,
}

/// A statistic of a PAIR of series over a sliding window of `w` valid pairs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PairStat {
    /// Pearson correlation.
    Corr,
    /// Spearman rank correlation — the rolling information coefficient.
    RankCorr,
    /// `Σ x·w` — the rolling sum of `x` weighted by the second series (VWAP is
    /// `wsum(price, volume, w) / rsum(volume, w)`).
    WeightedSum,
}

/// A scalar Kalman filter's noise: process variance `q`, measurement variance `r`.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct KalmanNoise {
    pub q: f64,
    pub r: f64,
}

/// One kernel to build.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Spec {
    Shift(Shift, usize),
    Rolling(Rolling, usize),
    Ewma(Smoothing),
    Map(Map),
    Arith(Arith),
    Pair(PairStat, usize),
    /// Local-level Kalman filter (random-walk state, `H = 1`), seeded with the first
    /// observation at variance `r`: the filtered level.
    KalmanLevel(KalmanNoise),
    /// Dynamic regression coefficient: the random-walk `β` in `x = β·y + v`, seeded at
    /// `β = 0`, variance 1 — the Kalman beta of `x` on `y`.
    KalmanBeta(KalmanNoise),
    /// Streaming left matrix profile (STAMPI, EH-529): the z-normalised distance of the
    /// newest length-`m` subsequence to its nearest earlier non-trivial neighbour among
    /// the last `history` subsequences — an incremental discord score.
    LeftProfile { m: usize, history: usize },
}

impl Spec {
    /// How many input series the kernel reads (1 or 2).
    pub fn arity(&self) -> usize {
        match self {
            Spec::Shift(..)
            | Spec::Rolling(..)
            | Spec::Ewma(_)
            | Spec::Map(_)
            | Spec::KalmanLevel(_)
            | Spec::LeftProfile { .. } => 1,
            Spec::Arith(_) | Spec::Pair(..) | Spec::KalmanBeta(_) => 2,
        }
    }

    /// Refuse a spec whose parameters are out of domain.
    pub fn validate(&self) -> Result<()> {
        match *self {
            Spec::Shift(_, k) | Spec::Rolling(_, k) => window_bound(k, 1),
            Spec::Pair(_, w) => window_bound(w, 2),
            Spec::Ewma(Smoothing::Span(s)) => positive("ewma span", s, 1.0),
            Spec::Ewma(Smoothing::HalfLife(h)) => positive("ewma halflife", h, f64::MIN_POSITIVE),
            Spec::Map(Map::Clip { lo, hi }) => clip_bounds(lo, hi),
            Spec::Map(_) | Spec::Arith(_) => Ok(()),
            Spec::KalmanLevel(noise) | Spec::KalmanBeta(noise) => kalman_noise(noise),
            Spec::LeftProfile { m, history } => {
                window_bound(m, distance::MIN_LENGTH)?;
                window_bound(history, 1)
            }
        }
    }
}

fn window_bound(w: usize, least: usize) -> Result<()> {
    if w < least {
        return Err(NumericError::bounds(format!(
            "a window of {w} is below {least}"
        )));
    }
    if w > MAX_WINDOW {
        return Err(NumericError::resource(format!(
            "a window of {w} exceeds {MAX_WINDOW}"
        )));
    }
    Ok(())
}

fn clip_bounds(lo: f64, hi: f64) -> Result<()> {
    if lo <= hi {
        return Ok(());
    }
    Err(NumericError::bounds("clip needs lo <= hi"))
}

fn kalman_noise(noise: KalmanNoise) -> Result<()> {
    positive("kalman q", noise.q, 0.0)?;
    positive("kalman r", noise.r, 0.0)
}

fn positive(what: &str, v: f64, least: f64) -> Result<()> {
    if v.is_finite() && v >= least {
        return Ok(());
    }
    Err(NumericError::bounds(format!(
        "{what} must be finite and >= {least}"
    )))
}

/// Run a one-input kernel over a whole series.
pub fn apply(spec: Spec, xs: &[f64]) -> Result<Vec<Option<f64>>> {
    let mut state = State::new(spec)?;
    Ok(xs.iter().map(|&x| state.step(Some(x), None)).collect())
}

/// Run a two-input kernel over a pair of equal-length series.
pub fn apply_pair(spec: Spec, xs: &[f64], ys: &[f64]) -> Result<Vec<Option<f64>>> {
    if xs.len() != ys.len() {
        return Err(NumericError::shape("the two series differ in length"));
    }
    let mut state = State::new(spec)?;
    Ok(xs
        .iter()
        .zip(ys)
        .map(|(&x, &y)| state.step(Some(x), Some(y)))
        .collect())
}

/// [`apply`] with warm-up and undefined outputs as `NaN` (the array-kernel convention the
/// finance surface returns).
pub fn apply_nan(spec: Spec, xs: &[f64]) -> Result<Vec<f64>> {
    Ok(apply(spec, xs)?
        .into_iter()
        .map(|v| v.unwrap_or(f64::NAN))
        .collect())
}

#[cfg(test)]
mod tests;
#[cfg(all(test, feature = "motif"))]
mod motif_tests;
