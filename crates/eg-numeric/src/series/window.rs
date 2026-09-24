//! Window statistics behind the rolling series kernels (EH-522): exact two-pass moments
//! and correlation over the window's values, a monotonic deque for min/max and a sorted
//! window for rank. The stateful ones are plain data (serde), so a kernel's whole state
//! is its checkpoint.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

/// Below this a window's standard deviation is treated as zero.
pub const STD_FLOOR: f64 = 1e-12;

/// The moments of a window: count, sum, mean and population (`ddof = 0`) variance,
/// computed two-pass and in order from the values themselves — exact on a flat window
/// (no add/remove residue) and bit-identical wherever the window is the same.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Moments {
    pub n: usize,
    pub sum: f64,
    pub mean: f64,
    pub variance: f64,
}

impl Moments {
    /// The moments of `xs` (`None` when empty). Also the batch form PromQL's
    /// `avg/sum/stddev/stdvar_over_time` share.
    pub fn of<I>(xs: I) -> Option<Self>
    where
        I: Iterator<Item = f64> + Clone,
    {
        let (n, sum) = xs.clone().fold((0usize, 0.0), |(n, s), x| (n + 1, s + x));
        if n == 0 {
            return None;
        }
        let mean = sum / n as f64;
        let squares = xs.fold(0.0, |acc, x| {
            let d = x - mean;
            acc + d * d
        });
        Some(Self {
            n,
            sum,
            mean,
            variance: squares / n as f64,
        })
    }

    /// Population standard deviation.
    pub fn population_std(&self) -> f64 {
        self.variance.sqrt()
    }
}

/// Which extremum a monotonic deque keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Min,
    Max,
}

/// A monotonic deque of `(sequence, value)`: the front is the window's extremum.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Extremum {
    side: Side,
    window: usize,
    seen: u64,
    deque: VecDeque<(u64, f64)>,
}

impl Extremum {
    pub fn new(side: Side, window: usize) -> Self {
        Self {
            side,
            window,
            seen: 0,
            deque: VecDeque::new(),
        }
    }

    /// Push `x`; the extremum of the last `window` values once the window is full.
    pub fn step(&mut self, x: f64) -> Option<f64> {
        let dominated = |held: f64| match self.side {
            Side::Min => held >= x,
            Side::Max => held <= x,
        };
        while self.deque.back().is_some_and(|&(_, v)| dominated(v)) {
            self.deque.pop_back();
        }
        self.deque.push_back((self.seen, x));
        self.seen += 1;
        let oldest = self.seen.saturating_sub(self.window as u64);
        while self.deque.front().is_some_and(|&(i, _)| i < oldest) {
            self.deque.pop_front();
        }
        (self.seen >= self.window as u64)
            .then(|| self.deque.front().map(|&(_, v)| v))
            .flatten()
    }
}

/// The last `window` values, oldest first, with a sorted copy for rank queries.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Sorted {
    window: usize,
    order: VecDeque<f64>,
    sorted: Vec<f64>,
}

impl Sorted {
    pub fn new(window: usize) -> Self {
        Self {
            window,
            order: VecDeque::new(),
            sorted: Vec::new(),
        }
    }

    /// Push `x`; its average rank (1-based, ties averaged — pandas `rank()`) in the
    /// window once the window is full.
    pub fn step(&mut self, x: f64) -> Option<f64> {
        self.order.push_back(x);
        let at = self.sorted.partition_point(|v| v.total_cmp(&x).is_lt());
        self.sorted.insert(at, x);
        if self.order.len() > self.window {
            if let Some(old) = self.order.pop_front() {
                let at = self.sorted.partition_point(|v| v.total_cmp(&old).is_lt());
                self.sorted.remove(at);
            }
        }
        (self.order.len() == self.window).then(|| average_rank(&self.sorted, x))
    }
}

/// The average 1-based rank of `x` in the ascending `sorted` slice.
fn average_rank(sorted: &[f64], x: f64) -> f64 {
    let below = sorted.partition_point(|v| v.total_cmp(&x).is_lt());
    let upto = sorted.partition_point(|v| v.total_cmp(&x).is_le());
    let ties = (upto - below) as f64;
    below as f64 + (ties + 1.0) / 2.0
}

/// Average 1-based ranks of `xs` (ties averaged), in input order.
pub fn ranks(xs: &[f64]) -> Vec<f64> {
    let mut sorted = xs.to_vec();
    sorted.sort_by(f64::total_cmp);
    xs.iter().map(|&x| average_rank(&sorted, x)).collect()
}

/// Pearson correlation of two equal-length series, two-pass (`None` when either is
/// constant or there are fewer than two pairs).
pub fn pearson(xs: &[f64], ys: &[f64]) -> Option<f64> {
    let mx = Moments::of(xs.iter().copied())?.mean;
    let my = Moments::of(ys.iter().copied())?.mean;
    let (mut cxy, mut sxx, mut syy) = (0.0, 0.0, 0.0);
    for (&x, &y) in xs.iter().zip(ys) {
        let (dx, dy) = (x - mx, y - my);
        cxy += dx * dy;
        sxx += dx * dx;
        syy += dy * dy;
    }
    let denom = (sxx * syy).sqrt();
    (xs.len() > 1 && denom > STD_FLOOR).then(|| cxy / denom)
}
