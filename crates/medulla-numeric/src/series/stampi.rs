//! Streaming left matrix profile (STAMPI, EH-529): the series kernel behind
//! `mprofile(x, m, h)`. At each observation it answers "how far is the newest length-`m`
//! subsequence from its nearest earlier neighbour among the last `h` subsequences" — the
//! incremental discord score. A step costs O(h): the newest subsequence's dot products
//! with every retained subsequence come from the previous step's by the STAMPI update
//! `QT(t, j) = QT(t−1, j−1) − x[t−1]·x[j−1] + x[t+m−1]·x[j+m−1]`, re-anchored by a
//! direct O(h·m) recompute every [`ANCHOR_EVERY`] steps so rounding cannot drift.
//!
//! The whole state is serde data (the checkpoint); values are centred on the first
//! observation, so a restored state continues bit for bit.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use super::distance::{dot, exclusion_zone, znorm_distance};
use super::streaming::RollingMoments;

/// Steps between direct recomputes of the dot-product row.
pub const ANCHOR_EVERY: usize = 1024;

/// The streaming left-profile state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LeftProfile {
    m: usize,
    history: usize,
    /// The centring origin (the first observation).
    origin: Option<f64>,
    /// Index of the oldest retained subsequence.
    first: usize,
    /// Centred values from subsequence `first`'s start onward.
    values: VecDeque<f64>,
    /// `(μ, σ)` of each retained subsequence, oldest first.
    stats: VecDeque<(f64, f64)>,
    /// `QT(newest, j)` for each retained subsequence `j`, oldest first.
    qt: VecDeque<f64>,
    /// Moments of the newest `m` values.
    window: RollingMoments,
    since_anchor: usize,
}

impl LeftProfile {
    pub fn new(m: usize, history: usize) -> Self {
        Self {
            m,
            history,
            origin: None,
            first: 0,
            values: VecDeque::new(),
            stats: VecDeque::new(),
            qt: VecDeque::new(),
            window: RollingMoments::new(m),
            since_anchor: ANCHOR_EVERY,
        }
    }

    /// Consume one observation; the newest subsequence's left-profile distance once it
    /// has an earlier non-trivial neighbour.
    pub fn step(&mut self, x: f64) -> Option<f64> {
        let c = x - *self.origin.get_or_insert(x);
        self.values.push_back(c);
        let moments = self.window.step(c)?;
        let newest = self.first + self.stats.len();
        self.stats
            .push_back((moments.mean, moments.population_std()));
        self.advance_row(newest);
        self.trim(newest);
        self.nearest(newest)
    }

    /// Bring the dot-product row from subsequence `newest − 1` to `newest`.
    fn advance_row(&mut self, newest: usize) {
        let rows = self.stats.len();
        if self.since_anchor >= ANCHOR_EVERY || rows < 2 || self.qt.len() + 1 != rows {
            self.qt = (0..rows)
                .map(|k| self.direct(newest, self.first + k))
                .collect();
            self.since_anchor = 0;
            return;
        }
        self.since_anchor += 1;
        let entering = self.value(newest + self.m - 1);
        let leaving = self.value(newest - 1);
        let mut row = VecDeque::with_capacity(rows);
        row.push_back(self.direct(newest, self.first));
        for (k, &previous) in self.qt.iter().enumerate().take(rows - 1) {
            let j = self.first + k + 1;
            row.push_back(
                previous - leaving * self.value(j - 1) + entering * self.value(j + self.m - 1),
            );
        }
        self.qt = row;
    }

    /// Drop subsequences older than the history bound (and the values only they used).
    fn trim(&mut self, newest: usize) {
        while newest - self.first > self.history {
            self.stats.pop_front();
            self.qt.pop_front();
            self.values.pop_front();
            self.first += 1;
        }
    }

    /// The nearest earlier non-trivial neighbour's distance.
    fn nearest(&self, newest: usize) -> Option<f64> {
        let last = newest.checked_sub(exclusion_zone(self.m) + 1)?;
        let here = *self.stats.back()?;
        (self.first..=last)
            .map(|j| {
                let k = j - self.first;
                znorm_distance(self.qt[k], self.m, here, self.stats[k])
            })
            .min_by(f64::total_cmp)
    }

    fn value(&self, at: usize) -> f64 {
        self.values[at - self.first]
    }

    fn direct(&self, a: usize, b: usize) -> f64 {
        let (a0, b0) = (a - self.first, b - self.first);
        let sub = |start: usize| self.values.range(start..start + self.m);
        sub(a0).zip(sub(b0)).fold(0.0, |acc, (x, y)| acc + x * y)
    }
}

/// The left profile of a whole series by direct recompute — the batch oracle the
/// streaming kernel must equal (`None` where no non-trivial earlier neighbour exists).
pub fn left_profile_batch(xs: &[f64], m: usize, history: usize) -> Vec<Option<f64>> {
    let origin = xs.first().copied().unwrap_or(0.0);
    let c: Vec<f64> = xs.iter().map(|x| x - origin).collect();
    let stats = super::distance::SubsequenceStats::of(&c, m);
    let mut out = vec![None; xs.len()];
    for t in 0..stats.mean.len() {
        let lo = t.saturating_sub(history);
        let hi = t.checked_sub(exclusion_zone(m) + 1);
        out[t + m - 1] = hi.and_then(|hi| {
            (lo..=hi)
                .map(|j| {
                    znorm_distance(dot(&c[t..t + m], &c[j..j + m]), m, stats.at(t), stats.at(j))
                })
                .min_by(f64::total_cmp)
        });
    }
    out
}

#[cfg(test)]
#[path = "stampi_tests.rs"]
mod tests;
