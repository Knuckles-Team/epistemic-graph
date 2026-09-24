//! O(1), numerically exact rolling moments (EH-562).
//!
//! A sliding window keeps `Σx` and `Σx²` (and, for a pair, `Σy`, `Σy²`, `Σxy`) as exact
//! wide integers ([`super::wide`]): a step adds the new value and removes the evicted one
//! in O(1), exactly, so nothing drifts. Every statistic is then computed from the exact
//! sums and rounded ONCE (twice for the variance's `/n²` via an exact quotient):
//!
//! * `sum = round(Σx)`, `mean = round(Σx / n)` — a flat window's mean is its value;
//! * `variance = round((n·Σx² − (Σx)²) / n²)` — the numerator is an exact integer, zero
//!   exactly when the window is flat, never negative;
//! * `corr = C / (√Nx · √Ny)` with `C = n·Σxy − Σx·Σy` exact, `None` when either
//!   numerator is exactly zero;
//! * `wsum = round(Σxy)`.
//!
//! A window holding a non-finite value falls back to the two-pass recompute over its
//! values (the only place the old O(window) path remains), so `NaN`/`inf` propagate as
//! they always did.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use super::window::Moments;
use super::wide::{
    add_product, add_value, Mag, Sign, Wide, PRODUCT_BIAS, PRODUCT_LIMBS, SUM_BIAS,
    SUM_LIMBS,
};

/// Exact `n`, `Σx`, `Σx²` of a window.
#[derive(Clone, Debug)]
pub(super) struct ExactMoments {
    n: u32,
    sum: Wide,
    squares: Wide,
    /// Non-finite values currently in the window (held outside the exact sums).
    nonfinite: usize,
}

impl Default for ExactMoments {
    fn default() -> Self {
        Self {
            n: 0,
            sum: Wide::new(SUM_LIMBS),
            squares: Wide::new(PRODUCT_LIMBS),
            nonfinite: 0,
        }
    }
}

impl ExactMoments {
    /// Add (`Sign::Plus`) or remove (`Sign::Minus`) one value.
    pub(super) fn update(&mut self, x: f64, direction: Sign) {
        self.n = match direction {
            Sign::Plus => self.n + 1,
            Sign::Minus => self.n - 1,
        };
        if add_value(&mut self.sum, x, direction) {
            add_product(&mut self.squares, x, x, direction);
            return;
        }
        self.nonfinite = match direction {
            Sign::Plus => self.nonfinite + 1,
            Sign::Minus => self.nonfinite - 1,
        };
    }

    /// Whether every value in the window is finite (the exact path applies).
    pub(super) fn all_finite(&self) -> bool {
        self.nonfinite == 0
    }

    pub(super) fn count(&self) -> usize {
        self.n as usize
    }

    /// `round(Σx)`.
    pub(super) fn sum(&mut self) -> f64 {
        let (sign, s) = self.sum.read();
        sign.unit() * s.round(SUM_BIAS, 0, false)
    }

    /// `round(Σx / n)`.
    pub(super) fn mean(&mut self) -> f64 {
        let (sign, mut s) = self.sum.read();
        let inexact = s.divide(self.n.max(1));
        sign.unit() * s.round(SUM_BIAS, 0, inexact)
    }

    /// The exact variance numerator `n·Σx² − (Σx)²` (at the product scale).
    fn spread(&mut self) -> Mag {
        let (_, s) = self.sum.read();
        let (_, q) = self.squares.read();
        let (_, spread) = q.scale(self.n).minus(&s.mul(&s));
        spread
    }

    /// Population variance `round(spread / n²)`; exactly `0` on a flat window.
    pub(super) fn variance(&mut self) -> f64 {
        let n = self.n.max(1);
        let mut spread = self.spread();
        let first = spread.divide(n);
        let second = spread.divide(n);
        spread.round(PRODUCT_BIAS, 0, first || second)
    }
}

/// Exact sums of a window of pairs.
#[derive(Clone, Debug)]
pub struct ExactPairs {
    x: ExactMoments,
    y: ExactMoments,
    cross: Wide,
}

impl Default for ExactPairs {
    fn default() -> Self {
        Self {
            x: ExactMoments::default(),
            y: ExactMoments::default(),
            cross: Wide::new(PRODUCT_LIMBS),
        }
    }
}

impl ExactPairs {
    pub(super) fn update(&mut self, x: f64, y: f64, direction: Sign) {
        self.x.update(x, direction);
        self.y.update(y, direction);
        add_product(&mut self.cross, x, y, direction);
    }

    pub fn all_finite(&self) -> bool {
        self.x.all_finite() && self.y.all_finite()
    }

    /// `round(Σxy)`.
    pub fn weighted_sum(&mut self) -> f64 {
        let (sign, c) = self.cross.read();
        sign.unit() * c.round(PRODUCT_BIAS, 0, false)
    }

    /// Pearson correlation (`None` with fewer than two pairs or a flat side).
    pub fn correlation(&mut self) -> Option<f64> {
        let nx = self.x.spread();
        let ny = self.y.spread();
        let (ex, ey) = (half_log2(&nx)?, half_log2(&ny)?);
        let (sx, x_sum) = self.x.sum.read();
        let (sy, y_sum) = self.y.sum.read();
        let (sc, cross) = self.cross.read();
        let (sign, cov) = difference((sc, cross.scale(self.x.n)), (sx.times(sy), x_sum.mul(&y_sum)));
        let root_x = nx.round(PRODUCT_BIAS, -2 * ex, false).sqrt();
        let root_y = ny.round(PRODUCT_BIAS, -2 * ey, false).sqrt();
        let c = sign.unit() * cov.round(PRODUCT_BIAS, -(ex + ey), false);
        (self.x.n > 1).then(|| (c / (root_x * root_y)).clamp(-1.0, 1.0))
    }
}

/// Half the binary exponent of a spread (the scale that brings its root near 1), `None`
/// when the spread is exactly zero.
fn half_log2(spread: &Mag) -> Option<i32> {
    spread.log2(PRODUCT_BIAS).map(|e| e.div_euclid(2))
}

/// `a − b` of two signed magnitudes, exactly.
fn difference(a: (Sign, Mag), b: (Sign, Mag)) -> (Sign, Mag) {
    let ((sa, ma), (sb, mb)) = (a, b);
    if sa != sb {
        return (sa, ma.plus(&mb));
    }
    let (s, m) = ma.minus(&mb);
    (s.times(sa), m)
}

/// A sliding window of the last `window` values with exact running sums — the state of
/// the rolling mean / std / sum / z-score kernels. It serialises as its values; the sums
/// are rebuilt from them (once, O(window)) on the first step after a restore.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MomentWindow {
    window: usize,
    values: VecDeque<f64>,
    #[serde(skip)]
    sums: Option<ExactMoments>,
}

impl PartialEq for MomentWindow {
    fn eq(&self, other: &Self) -> bool {
        self.window == other.window && self.values == other.values
    }
}

impl MomentWindow {
    pub fn new(window: usize) -> Self {
        Self {
            window,
            values: VecDeque::with_capacity(window + 1),
            sums: None,
        }
    }

    /// Push `x`; the window's moments once it is full.
    pub fn step(&mut self, x: f64) -> Option<Moments> {
        self.values.push_back(x);
        let evicted = (self.values.len() > self.window)
            .then(|| self.values.pop_front())
            .flatten();
        match self.sums.as_mut() {
            Some(sums) => {
                sums.update(x, Sign::Plus);
                if let Some(old) = evicted {
                    sums.update(old, Sign::Minus);
                }
            }
            None => self.sums = Some(rebuild(&self.values)),
        }
        let sums = self.sums.as_mut()?;
        if self.values.len() < self.window {
            return None;
        }
        if !sums.all_finite() {
            return Moments::of(self.values.iter().copied());
        }
        Some(Moments {
            n: sums.count(),
            sum: sums.sum(),
            mean: sums.mean(),
            variance: sums.variance(),
        })
    }
}

fn rebuild(values: &VecDeque<f64>) -> ExactMoments {
    let mut sums = ExactMoments::default();
    values.iter().for_each(|&x| sums.update(x, Sign::Plus));
    sums
}

/// Exact running sums over a window of pairs, rebuilt from the pairs after a restore.
#[derive(Clone, Debug, Default)]
pub struct PairSums(Option<ExactPairs>);

impl PairSums {
    /// The sums of `pairs` after `pushed` entered and `evicted` left it (`pairs` is the
    /// window as it now stands).
    pub fn advance(
        &mut self,
        pairs: &VecDeque<(f64, f64)>,
        pushed: (f64, f64),
        evicted: Option<(f64, f64)>,
    ) -> &mut ExactPairs {
        match self.0.as_mut() {
            Some(sums) => {
                sums.update(pushed.0, pushed.1, Sign::Plus);
                if let Some((x, y)) = evicted {
                    sums.update(x, y, Sign::Minus);
                }
            }
            None => {
                let mut sums = ExactPairs::default();
                pairs.iter().for_each(|&(x, y)| sums.update(x, y, Sign::Plus));
                self.0 = Some(sums);
            }
        }
        self.0.get_or_insert_with(ExactPairs::default)
    }
}

#[cfg(test)]
#[path = "exact_tests.rs"]
mod tests;
