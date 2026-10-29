//! The kernel state machines. [`State`] is the checkpoint: it serialises whole.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use super::kalman::Kalman;
use super::stampi::LeftProfile;
use super::streaming::{RollingMoments, RollingPairs};
use super::window::{pearson, ranks, Extremum, Moments, Side, Sorted, STD_FLOOR};
use super::{Arith, Map, PairStat, Rolling, Shift, Smoothing, Spec};
use crate::detkernel::math;
use crate::error::Result;

/// A kernel's whole state — its checkpoint.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum State {
    Shift(ShiftState),
    Rolling(RollingState),
    Ewma(EwmaState),
    Map(Map),
    Arith(Arith),
    Pair(PairState),
    /// A Kalman filter; `regressor` marks the beta form (`H` = the second input).
    Kalman {
        filter: Kalman,
        regressor: bool,
    },
    /// The streaming left matrix profile (EH-529).
    Profile(LeftProfile),
}

impl State {
    /// A fresh state for `spec` (refused when the spec is out of domain).
    pub fn new(spec: Spec) -> Result<Self> {
        spec.validate()?;
        Ok(match spec {
            Spec::Shift(op, k) => State::Shift(ShiftState::new(op, k)),
            Spec::Rolling(op, w) => State::Rolling(RollingState::new(op, w)),
            Spec::Ewma(smoothing) => State::Ewma(EwmaState::new(smoothing)),
            Spec::Map(map) => State::Map(map),
            Spec::Arith(op) => State::Arith(op),
            Spec::Pair(op, w) => State::Pair(PairState::new(op, w)),
            Spec::KalmanLevel(noise) => State::Kalman {
                filter: Kalman::level(noise),
                regressor: false,
            },
            Spec::KalmanBeta(noise) => State::Kalman {
                filter: Kalman::beta(noise),
                regressor: true,
            },
            Spec::LeftProfile { m, history } => State::Profile(LeftProfile::new(m, history)),
        })
    }

    /// Consume one observation (`y` is the second input of a two-input kernel and is
    /// ignored by the others). `None` while warming up, on a skipped input, or where the
    /// output is undefined.
    pub fn step(&mut self, x: Option<f64>, y: Option<f64>) -> Option<f64> {
        match self {
            State::Shift(s) => s.step(x?),
            State::Rolling(s) => s.step(x?),
            State::Ewma(s) => s.step(x?),
            State::Map(map) => Some(map_value(*map, x?)),
            State::Arith(op) => arith(*op, x?, y?),
            State::Pair(s) => s.step(x?, y?),
            State::Kalman { filter, regressor } => {
                let h = if *regressor { y? } else { 1.0 };
                Some(filter.observe(x?, h).0)
            }
            State::Profile(profile) => profile.step(x?),
        }
    }
}

fn map_value(map: Map, x: f64) -> f64 {
    match map {
        Map::Abs => x.abs(),
        Map::Sign => sign(x),
        Map::Neg => -x,
        Map::Clip { lo, hi } => x.clamp(lo, hi),
    }
}

fn sign(x: f64) -> f64 {
    if x > 0.0 {
        1.0
    } else if x < 0.0 {
        -1.0
    } else {
        0.0
    }
}

fn arith(op: Arith, x: f64, y: f64) -> Option<f64> {
    match op {
        Arith::Add => Some(x + y),
        Arith::Sub => Some(x - y),
        Arith::Mul => Some(x * y),
        Arith::Div => (y != 0.0).then(|| x / y),
        Arith::Gt => Some(indicator(x > y)),
        Arith::Lt => Some(indicator(x < y)),
        Arith::Max => Some(x.max(y)),
        Arith::Min => Some(x.min(y)),
    }
}

fn indicator(holds: bool) -> f64 {
    if holds {
        1.0
    } else {
        0.0
    }
}

/// `lag`/`diff`/`ret`/`logret` over the last `k` valid observations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ShiftState {
    op: Shift,
    k: usize,
    history: VecDeque<f64>,
}

impl ShiftState {
    fn new(op: Shift, k: usize) -> Self {
        Self {
            op,
            k,
            history: VecDeque::with_capacity(k + 1),
        }
    }

    fn step(&mut self, x: f64) -> Option<f64> {
        self.history.push_back(x);
        if self.history.len() > self.k + 1 {
            self.history.pop_front();
        }
        let prior = (self.history.len() == self.k + 1).then(|| self.history[0])?;
        match self.op {
            Shift::Lag => Some(prior),
            Shift::Diff => Some(x - prior),
            Shift::Ret => (prior != 0.0).then(|| x / prior - 1.0),
            Shift::LogRet => (x > 0.0 && prior > 0.0).then(|| math::ln(x / prior)),
        }
    }
}

/// A rolling statistic over the last `w` valid observations; the moment statistics are
/// O(1) per step (EH-562, [`super::streaming`]).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RollingState {
    Moments { op: Rolling, stats: RollingMoments },
    Extremum(Extremum),
    Rank(Sorted),
}

impl RollingState {
    fn new(op: Rolling, window: usize) -> Self {
        match op {
            Rolling::Min => RollingState::Extremum(Extremum::new(Side::Min, window)),
            Rolling::Max => RollingState::Extremum(Extremum::new(Side::Max, window)),
            Rolling::Rank => RollingState::Rank(Sorted::new(window)),
            Rolling::Mean | Rolling::Std | Rolling::Sum | Rolling::Zscore => {
                RollingState::Moments {
                    op,
                    stats: RollingMoments::new(window),
                }
            }
        }
    }

    fn step(&mut self, x: f64) -> Option<f64> {
        match self {
            RollingState::Extremum(e) => e.step(x),
            RollingState::Rank(r) => r.step(x),
            RollingState::Moments { op, stats } => {
                stats.step(x).and_then(|m| moment_stat(*op, &m, x))
            }
        }
    }
}

/// The moment-based statistic `op` of a full window whose newest value is `x`.
fn moment_stat(op: Rolling, m: &Moments, x: f64) -> Option<f64> {
    match op {
        Rolling::Mean => Some(m.mean),
        Rolling::Std => Some(m.population_std()),
        Rolling::Sum => Some(m.sum),
        Rolling::Zscore => zscore(m, x),
        Rolling::Min | Rolling::Max | Rolling::Rank => None,
    }
}

fn zscore(m: &Moments, x: f64) -> Option<f64> {
    let std = m.population_std();
    if !x.is_finite() || !m.mean.is_finite() || !std.is_finite() {
        return None;
    }
    Some(if std > STD_FLOOR {
        (x - m.mean) / std
    } else {
        0.0
    })
}

/// The recursive EWMA seeded with the first observation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EwmaState {
    alpha: f64,
    last: Option<f64>,
}

impl EwmaState {
    fn new(smoothing: Smoothing) -> Self {
        let alpha = match smoothing {
            Smoothing::Span(span) => 2.0 / (span + 1.0),
            Smoothing::HalfLife(h) => 1.0 - math::pow(2.0, -1.0 / h),
        };
        Self { alpha, last: None }
    }

    /// A non-finite observation is undefined: it yields no output and leaves the
    /// recursion untouched rather than poisoning every later value.
    fn step(&mut self, x: f64) -> Option<f64> {
        if !x.is_finite() {
            return None;
        }
        let next = match self.last {
            Some(prev) => self.alpha * x + (1.0 - self.alpha) * prev,
            None => x,
        };
        self.last = Some(next);
        Some(next)
    }
}

/// A windowed statistic of a pair of series. Correlation and the weighted sum are O(1)
/// per step (EH-562); the rank correlation ranks the window, O(w log w).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PairState {
    op: PairStat,
    window: RollingPairs,
}

impl PairState {
    fn new(op: PairStat, window: usize) -> Self {
        Self {
            op,
            window: RollingPairs::new(window),
        }
    }

    fn step(&mut self, x: f64, y: f64) -> Option<f64> {
        self.window.push(x, y).then_some(())?;
        match (self.op, self.window.sums()) {
            (PairStat::RankCorr, _) | (_, None) => self.two_pass(),
            (PairStat::Corr, Some(_)) if self.window.has_flat_side() => None,
            (PairStat::Corr, Some(sums)) => sums.correlation(self.window.pairs().len()),
            (PairStat::WeightedSum, Some(sums)) => Some(sums.product_sum()),
        }
    }

    /// The statistic recomputed from the window's pairs: the rank correlation always, the
    /// others while a non-finite pair is in the window.
    fn two_pass(&self) -> Option<f64> {
        let (xs, ys): (Vec<f64>, Vec<f64>) = self.window.pairs().iter().copied().unzip();
        match self.op {
            PairStat::Corr => pearson(&xs, &ys),
            PairStat::RankCorr => pearson(&ranks(&xs), &ranks(&ys)),
            PairStat::WeightedSum => Some(xs.iter().zip(&ys).fold(0.0, |acc, (x, w)| acc + x * w)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::{apply, MAX_WINDOW};

    // spec: MEDULLA-FINANCE-PRIMITIVES-R002.1.1
    #[test]
    fn rolling_z_score_independent_vectors() {
        // Window 3, population standard deviation, worked by hand:
        // [1,2,4]: mean 7/3, variance 14/9, z(4) = 5/sqrt(14);
        // [2,4,7]: mean 13/3, variance 38/9, z(7) = 8/sqrt(38).
        let z = apply(Spec::Rolling(Rolling::Zscore, 3), &[1.0, 2.0, 4.0, 7.0]).unwrap();
        assert_eq!(&z[..2], &[None, None]);
        assert!((z[2].unwrap() - 1.336_306_209_562_121_9).abs() < 1e-12);
        assert!((z[3].unwrap() - 1.297_771_369_046_100_4).abs() < 1e-12);
        // A series shorter than the window is all warm-up, never a fabricated score.
        let short = apply(Spec::Rolling(Rolling::Zscore, 3), &[1.0, 2.0]).unwrap();
        assert_eq!(short, vec![None, None]);
        // A window holding a non-finite observation is undefined, never a fabricated
        // zero, and scoring resumes once it has slid past.
        let gapped = apply(
            Spec::Rolling(Rolling::Zscore, 2),
            &[1.0, f64::NAN, 3.0, 5.0],
        )
        .unwrap();
        assert_eq!(&gapped[..3], &[None, None, None]);
        assert!((gapped[3].unwrap() - 1.0).abs() < 1e-12);
        // Malformed windows refuse.
        assert!(State::new(Spec::Rolling(Rolling::Zscore, 0)).is_err());
        assert!(State::new(Spec::Rolling(Rolling::Zscore, MAX_WINDOW + 1)).is_err());
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R002.1.2
    #[test]
    fn ewma_independent_vectors() {
        // Span 4: alpha = 2/(4+1) = 0.4, seeded with the first value.
        // 10; 0.4*20 + 0.6*10 = 14; 0.4*40 + 0.6*14 = 24.4.
        let span = apply(Spec::Ewma(Smoothing::Span(4.0)), &[10.0, 20.0, 40.0]).unwrap();
        assert_eq!(span[0], Some(10.0));
        assert!((span[1].unwrap() - 14.0).abs() < 1e-12);
        assert!((span[2].unwrap() - 24.4).abs() < 1e-12);
        // Half-life 1: alpha = 1 - 2^-1 = 0.5; 8; 0.5*4 + 0.5*8 = 6; 0.5*2 + 0.5*6 = 4.
        let half = apply(Spec::Ewma(Smoothing::HalfLife(1.0)), &[8.0, 4.0, 2.0]).unwrap();
        assert_eq!(half, vec![Some(8.0), Some(6.0), Some(4.0)]);
        // A non-finite observation is undefined and does not poison the recursion.
        let gapped = apply(
            Spec::Ewma(Smoothing::HalfLife(1.0)),
            &[8.0, f64::NAN, 4.0, f64::INFINITY, 2.0],
        )
        .unwrap();
        assert_eq!(gapped, vec![Some(8.0), None, Some(6.0), None, Some(4.0)]);
        // An empty series has no output; malformed parameters refuse.
        assert!(apply(Spec::Ewma(Smoothing::Span(4.0)), &[])
            .unwrap()
            .is_empty());
        assert!(State::new(Spec::Ewma(Smoothing::Span(0.5))).is_err());
        assert!(State::new(Spec::Ewma(Smoothing::HalfLife(0.0))).is_err());
        assert!(State::new(Spec::Ewma(Smoothing::HalfLife(f64::NAN))).is_err());
    }
}
