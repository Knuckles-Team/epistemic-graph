//! The kernel state machines. [`State`] is the checkpoint: it serialises whole.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

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
    Moments {
        op: Rolling,
        window: usize,
        values: VecDeque<f64>,
    },
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
                    window,
                    values: VecDeque::with_capacity(window + 1),
                }
            }
        }
    }

    fn step(&mut self, x: f64) -> Option<f64> {
        match self {
            RollingState::Extremum(e) => e.step(x),
            RollingState::Rank(r) => r.step(x),
            RollingState::Moments { op, window, values } => {
                values.push_back(x);
                if values.len() > *window {
                    values.pop_front();
                }
                let full = values.len() == *window;
                full.then(|| Moments::of(values.iter().copied()))
                    .flatten()
                    .and_then(|m| moment_stat(*op, &m, x))
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
    let (mean, std) = (m.mean()?, m.population_std()?);
    Some(if std > STD_FLOOR {
        (x - mean) / std
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

/// A windowed statistic of a pair of series.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PairState {
    op: PairStat,
    window: usize,
    pairs: VecDeque<(f64, f64)>,
}

impl PairState {
    fn new(op: PairStat, window: usize) -> Self {
        Self {
            op,
            window,
            pairs: VecDeque::with_capacity(window + 1),
        }
    }

    fn step(&mut self, x: f64, y: f64) -> Option<f64> {
        self.pairs.push_back((x, y));
        if self.pairs.len() > self.window {
            self.pairs.pop_front();
        }
        (self.pairs.len() == self.window).then_some(())?;
        let (xs, ys): (Vec<f64>, Vec<f64>) = self.pairs.iter().copied().unzip();
        match self.op {
            PairStat::Corr => pearson(&xs, &ys),
            PairStat::RankCorr => pearson(&ranks(&xs), &ranks(&ys)),
            PairStat::WeightedSum => Some(xs.iter().zip(&ys).fold(0.0, |acc, (x, w)| acc + x * w)),
        }
    }
}
