//! O(1) rolling window statistics (EH-562).
//!
//! A sliding window keeps compensated running sums of its values' deviations from an
//! ANCHOR, so an arriving value and the departing one each cost O(1). Three rules keep the
//! sums exact enough to serve and exact where exactness is observable:
//!
//! * **Neumaier-compensated sums** ([`Compensated`]) carry each add's and each removal's
//!   rounding error forward instead of letting it accumulate;
//! * **periodic exact re-anchor**: every `window` steps the sums are rebuilt two-pass from
//!   the window's values about their mean, so residue never outlives one window and the
//!   rebuild costs O(1) amortised;
//! * **constant-window detection**: monotonic min/max deques ([`Range`]) spot a flat
//!   window, whose mean is its value and whose deviation is exactly `0` — the case where an
//!   add/remove residue would otherwise surface as a non-zero std or z-score.
//!
//! A non-finite value poisons a running sum for good, so it never enters one: while such a
//! value is in the window the caller falls back to the two-pass form over the values.
//! Every field is plain serde data, so the state stays the kernel's checkpoint and a
//! restored state continues bit-identically.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use super::window::{Extremum, Moments, Side, STD_FLOOR};

/// A Neumaier-compensated running sum that also takes removals (adds of `-v`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Compensated {
    total: f64,
    compensation: f64,
}

impl Compensated {
    pub fn add(&mut self, value: f64) {
        let next = self.total + value;
        self.compensation += if self.total.abs() >= value.abs() {
            (self.total - next) + value
        } else {
            (value - next) + self.total
        };
        self.total = next;
    }

    pub fn value(&self) -> f64 {
        self.total + self.compensation
    }
}

/// The running sums a sliding window keeps, and how an item enters and leaves them.
pub trait Accumulator: Clone + Default + std::fmt::Debug + PartialEq {
    type Item: Copy + std::fmt::Debug + PartialEq;

    /// Whether `item` may enter the running sums.
    fn is_finite(item: Self::Item) -> bool;
    fn enter(&mut self, item: Self::Item);
    fn leave(&mut self, item: Self::Item);
    /// The sums rebuilt two-pass over `items`, anchored at their mean.
    fn rebuild<I>(items: I) -> Self
    where
        I: Iterator<Item = Self::Item> + Clone;
}

/// The last `window` items and their running sums, re-anchored every `window` steps.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "A: Serialize, A::Item: Serialize",
    deserialize = "A: Deserialize<'de>, A::Item: Deserialize<'de>"
))]
pub struct Sliding<A: Accumulator> {
    window: usize,
    items: VecDeque<A::Item>,
    sums: A,
    since_anchor: usize,
    non_finite: usize,
}

impl<A: Accumulator> Sliding<A> {
    pub fn new(window: usize) -> Self {
        Self {
            window,
            items: VecDeque::with_capacity(window + 1),
            sums: A::default(),
            since_anchor: 0,
            non_finite: 0,
        }
    }

    /// Push `item`, evicting the oldest past `window`; `true` once the window is full.
    pub fn push(&mut self, item: A::Item) -> bool {
        self.items.push_back(item);
        if A::is_finite(item) {
            self.sums.enter(item);
        } else {
            self.non_finite += 1;
        }
        if self.items.len() > self.window {
            if let Some(old) = self.items.pop_front() {
                self.evict(old);
            }
        }
        self.since_anchor += 1;
        if self.since_anchor >= self.window {
            self.reanchor();
        }
        self.items.len() == self.window
    }

    fn evict(&mut self, old: A::Item) {
        if A::is_finite(old) {
            self.sums.leave(old);
        } else {
            self.non_finite -= 1;
        }
    }

    fn reanchor(&mut self) {
        let finite = |item: &A::Item| A::is_finite(*item);
        self.sums = A::rebuild(self.items.iter().copied().filter(finite));
        self.since_anchor = 0;
    }

    pub fn items(&self) -> &VecDeque<A::Item> {
        &self.items
    }

    /// The running sums, or `None` while a non-finite item is in the window.
    pub fn sums(&self) -> Option<&A> {
        (self.non_finite == 0).then_some(&self.sums)
    }
}

/// A window's min and max, to detect a constant window.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Range {
    lo: Extremum,
    hi: Extremum,
}

impl Range {
    pub fn new(window: usize) -> Self {
        Self {
            lo: Extremum::new(Side::Min, window),
            hi: Extremum::new(Side::Max, window),
        }
    }

    /// Push `x`; the window's value when the full window is constant.
    pub fn step(&mut self, x: f64) -> Option<f64> {
        let lo = self.lo.step(x);
        let hi = self.hi.step(x);
        match (lo, hi) {
            (Some(lo), Some(hi)) if lo == hi => Some(lo),
            _ => None,
        }
    }
}

/// First and second shifted power sums of one coordinate about an anchor.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Univariate {
    anchor: f64,
    first: Compensated,
    second: Compensated,
}

impl Univariate {
    fn deviation(&self, x: f64) -> f64 {
        x - self.anchor
    }

    /// Σ(x − x̄)² over `n` values: the second central sum.
    fn central_second(&self, n: f64) -> f64 {
        let first = self.first.value();
        (self.second.value() - first * first / n).max(0.0)
    }

    /// The window's moments over `n` values.
    pub fn moments(&self, n: usize) -> Moments {
        let count = n as f64;
        let first = self.first.value();
        Moments {
            n,
            sum: self.anchor * count + first,
            mean: self.anchor + first / count,
            variance: self.central_second(count) / count,
        }
    }
}

impl Accumulator for Univariate {
    type Item = f64;

    fn is_finite(item: f64) -> bool {
        item.is_finite()
    }

    fn enter(&mut self, x: f64) {
        let d = self.deviation(x);
        self.first.add(d);
        self.second.add(d * d);
    }

    fn leave(&mut self, x: f64) {
        let d = self.deviation(x);
        self.first.add(-d);
        self.second.add(-(d * d));
    }

    fn rebuild<I>(items: I) -> Self
    where
        I: Iterator<Item = f64> + Clone,
    {
        let anchor = Moments::of(items.clone()).map_or(0.0, |m| m.mean);
        let mut sums = Self {
            anchor,
            ..Self::default()
        };
        items.for_each(|x| sums.enter(x));
        sums
    }
}

/// The moments of a flat window of `n` copies of `value`: exact by construction.
fn constant_moments(value: f64, n: usize) -> Moments {
    Moments {
        n,
        sum: value * n as f64,
        mean: value,
        variance: 0.0,
    }
}

/// Rolling moments in O(1) per step.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RollingMoments {
    sliding: Sliding<Univariate>,
    range: Range,
}

impl RollingMoments {
    pub fn new(window: usize) -> Self {
        Self {
            sliding: Sliding::new(window),
            range: Range::new(window),
        }
    }

    /// Push `x`; the moments of the last `window` values once the window is full.
    pub fn step(&mut self, x: f64) -> Option<Moments> {
        let constant = self.range.step(x);
        if !self.sliding.push(x) {
            return None;
        }
        let n = self.sliding.items().len();
        match (self.sliding.sums(), constant) {
            (None, _) => Moments::of(self.sliding.items().iter().copied()),
            (Some(_), Some(value)) => Some(constant_moments(value, n)),
            (Some(sums), None) => Some(sums.moments(n)),
        }
    }
}

/// Both coordinates' shifted sums, their cross sum and the raw product sum.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Bivariate {
    x: Univariate,
    y: Univariate,
    cross: Compensated,
    product: Compensated,
}

impl Bivariate {
    /// Pearson correlation over `n` pairs (`None` when either side has no spread).
    pub fn correlation(&self, n: usize) -> Option<f64> {
        let count = n as f64;
        let cross = self.cross.value() - self.x.first.value() * self.y.first.value() / count;
        let denom = (self.x.central_second(count) * self.y.central_second(count)).sqrt();
        (n > 1 && denom > STD_FLOOR).then(|| (cross / denom).clamp(-1.0, 1.0))
    }

    /// `Σ x·y` over the window.
    pub fn product_sum(&self) -> f64 {
        self.product.value()
    }

    fn cross_terms(&mut self, (x, y): (f64, f64), sign: f64) {
        let (dx, dy) = (self.x.deviation(x), self.y.deviation(y));
        self.cross.add(sign * (dx * dy));
        self.product.add(sign * (x * y));
    }
}

impl Accumulator for Bivariate {
    type Item = (f64, f64);

    fn is_finite((x, y): (f64, f64)) -> bool {
        x.is_finite() && y.is_finite()
    }

    fn enter(&mut self, pair: (f64, f64)) {
        self.cross_terms(pair, 1.0);
        self.x.enter(pair.0);
        self.y.enter(pair.1);
    }

    fn leave(&mut self, pair: (f64, f64)) {
        self.cross_terms(pair, -1.0);
        self.x.leave(pair.0);
        self.y.leave(pair.1);
    }

    fn rebuild<I>(items: I) -> Self
    where
        I: Iterator<Item = (f64, f64)> + Clone,
    {
        let mut sums = Self {
            x: Univariate::rebuild(items.clone().map(|(x, _)| x)),
            y: Univariate::rebuild(items.clone().map(|(_, y)| y)),
            ..Self::default()
        };
        items.for_each(|pair| sums.cross_terms(pair, 1.0));
        sums
    }
}

/// A rolling window of pairs with O(1) correlation and weighted sum.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RollingPairs {
    sliding: Sliding<Bivariate>,
    x_range: Range,
    y_range: Range,
    flat: bool,
}

impl RollingPairs {
    pub fn new(window: usize) -> Self {
        Self {
            sliding: Sliding::new(window),
            x_range: Range::new(window),
            y_range: Range::new(window),
            flat: false,
        }
    }

    /// Push `(x, y)`; `true` once the window is full.
    pub fn push(&mut self, x: f64, y: f64) -> bool {
        let x_flat = self.x_range.step(x).is_some();
        let y_flat = self.y_range.step(y).is_some();
        self.flat = x_flat || y_flat;
        self.sliding.push((x, y))
    }

    /// The window's pairs, oldest first.
    pub fn pairs(&self) -> &VecDeque<(f64, f64)> {
        self.sliding.items()
    }

    /// The running sums, or `None` while a non-finite pair is in the window.
    pub fn sums(&self) -> Option<&Bivariate> {
        self.sliding.sums()
    }

    /// Whether either side of the full window is constant.
    pub fn has_flat_side(&self) -> bool {
        self.flat
    }
}

#[cfg(test)]
#[path = "streaming_tests.rs"]
mod tests;
