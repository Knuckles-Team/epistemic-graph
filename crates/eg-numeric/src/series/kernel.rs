//! The kernel state machines. [`State`] is the checkpoint: it serialises whole.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use super::exact::{ExactPairs, MomentWindow, PairSums};
use super::kalman::Kalman;
use super::stampi::LeftProfile;
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
            State::Ewma(s) => Some(s.step(x?)),
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

/// A rolling statistic over the last `w` valid observations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RollingState {
    /// Mean / std / sum / z-score over exact O(1) running sums (EH-562).
    Moments { op: Rolling, window: MomentWindow },
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
                    window: MomentWindow::new(window),
                }
            }
        }
    }

    fn step(&mut self, x: f64) -> Option<f64> {
        match self {
            RollingState::Extremum(e) => e.step(x),
            RollingState::Rank(r) => r.step(x),
            RollingState::Moments { op, window } => {
                window.step(x).and_then(|m| moment_stat(*op, &m, x))
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

    fn step(&mut self, x: f64) -> f64 {
        let next = match self.last {
            Some(prev) => self.alpha * x + (1.0 - self.alpha) * prev,
            None => x,
        };
        self.last = Some(next);
        next
    }
}

/// A windowed statistic of a pair of series. Correlation and the weighted sum read exact
/// O(1) running sums (EH-562), rebuilt from `pairs` after a restore; the rank
/// correlation re-ranks its window.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PairState {
    op: PairStat,
    window: usize,
    pairs: VecDeque<(f64, f64)>,
    #[serde(skip)]
    sums: PairSums,
}

impl PartialEq for PairState {
    fn eq(&self, other: &Self) -> bool {
        (self.op, self.window) == (other.op, other.window) && self.pairs == other.pairs
    }
}

impl PairState {
    fn new(op: PairStat, window: usize) -> Self {
        Self {
            op,
            window,
            pairs: VecDeque::with_capacity(window + 1),
            sums: PairSums::default(),
        }
    }

    fn step(&mut self, x: f64, y: f64) -> Option<f64> {
        self.pairs.push_back((x, y));
        let evicted = (self.pairs.len() > self.window)
            .then(|| self.pairs.pop_front())
            .flatten();
        let (op, full) = (self.op, self.pairs.len() == self.window);
        let exact = match op {
            PairStat::RankCorr => None,
            PairStat::Corr | PairStat::WeightedSum => {
                let sums = self.sums.advance(&self.pairs, (x, y), evicted);
                (full && sums.all_finite()).then(|| exact_pair_stat(op, sums))
            }
        };
        full.then_some(())?;
        exact.unwrap_or_else(|| self.recompute())
    }

    /// The two-pass statistic over the window (the rank correlation, and any window
    /// holding a non-finite value).
    fn recompute(&self) -> Option<f64> {
        let (xs, ys): (Vec<f64>, Vec<f64>) = self.pairs.iter().copied().unzip();
        match self.op {
            PairStat::Corr => pearson(&xs, &ys),
            PairStat::RankCorr => pearson(&ranks(&xs), &ranks(&ys)),
            PairStat::WeightedSum => Some(xs.iter().zip(&ys).fold(0.0, |acc, (x, w)| acc + x * w)),
        }
    }
}

/// A pair statistic read off the exact running sums.
fn exact_pair_stat(op: PairStat, sums: &mut ExactPairs) -> Option<f64> {
    match op {
        PairStat::Corr => sums.correlation(),
        PairStat::WeightedSum => Some(sums.weighted_sum()),
        PairStat::RankCorr => None,
    }
}
