//! Sliding-window accumulators behind the rolling series kernels (EH-522): Welford
//! moments with removal, a monotonic deque for min/max, a sorted window for rank, and
//! bivariate co-moments for correlation. Every one is plain data (serde), so a kernel's
//! whole state is its checkpoint.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

/// Below this a window's standard deviation is treated as zero.
pub const STD_FLOOR: f64 = 1e-12;

/// Welford mean / second moment over a sliding window, plus the running sum.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Moments {
    n: usize,
    mean: f64,
    m2: f64,
    sum: f64,
}

impl Moments {
    /// The moments of a whole slice (the batch form PromQL's `*_over_time` shares).
    pub fn of(xs: &[f64]) -> Self {
        let mut m = Self::default();
        xs.iter().for_each(|&x| m.add(x));
        m
    }

    /// Add an observation.
    pub fn add(&mut self, x: f64) {
        self.n += 1;
        let d = x - self.mean;
        self.mean += d / self.n as f64;
        self.m2 += d * (x - self.mean);
        self.sum += x;
    }

    /// Remove an observation previously added.
    pub fn remove(&mut self, x: f64) {
        if self.n <= 1 {
            *self = Self::default();
            return;
        }
        self.n -= 1;
        let d = x - self.mean;
        self.mean -= d / self.n as f64;
        self.m2 = (self.m2 - d * (x - self.mean)).max(0.0);
        self.sum -= x;
    }

    /// Observations held.
    pub fn count(&self) -> usize {
        self.n
    }

    /// Mean (`None` when empty).
    pub fn mean(&self) -> Option<f64> {
        (self.n > 0).then_some(self.mean)
    }

    /// Sum.
    pub fn sum(&self) -> f64 {
        self.sum
    }

    /// Population (`ddof = 0`) standard deviation — PromQL's `stddev_over_time` and the
    /// z-score convention (`None` when empty).
    pub fn population_std(&self) -> Option<f64> {
        (self.n > 0).then(|| (self.m2 / self.n as f64).sqrt())
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

/// Bivariate Welford co-moments over a sliding window, plus the weighted sums a
/// weighted mean needs.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CoMoments {
    n: usize,
    mx: f64,
    my: f64,
    cxy: f64,
    m2x: f64,
    m2y: f64,
    sum_xy: f64,
    sum_y: f64,
}

impl CoMoments {
    /// Add a pair.
    pub fn add(&mut self, x: f64, y: f64) {
        self.n += 1;
        let n = self.n as f64;
        let dx = x - self.mx;
        let dy = y - self.my;
        self.mx += dx / n;
        self.my += dy / n;
        self.cxy += dx * (y - self.my);
        self.m2x += dx * (x - self.mx);
        self.m2y += dy * (y - self.my);
        self.sum_xy += x * y;
        self.sum_y += y;
    }

    /// Remove a pair previously added.
    pub fn remove(&mut self, x: f64, y: f64) {
        if self.n <= 1 {
            *self = Self::default();
            return;
        }
        self.n -= 1;
        let n = self.n as f64;
        let dx = x - self.mx;
        let dy = y - self.my;
        self.mx -= dx / n;
        self.my -= dy / n;
        self.cxy -= dx * (y - self.my);
        self.m2x = (self.m2x - dx * (x - self.mx)).max(0.0);
        self.m2y = (self.m2y - dy * (y - self.my)).max(0.0);
        self.sum_xy -= x * y;
        self.sum_y -= y;
    }

    /// Pearson correlation (`None` when either side is constant).
    pub fn corr(&self) -> Option<f64> {
        let denom = (self.m2x * self.m2y).sqrt();
        (self.n > 1 && denom > STD_FLOOR).then(|| self.cxy / denom)
    }

    /// `Σ x·y / Σ y` — `x` weighted by `y` (`None` when the weights sum to zero).
    pub fn weighted_mean(&self) -> Option<f64> {
        (self.sum_y.abs() > STD_FLOOR).then(|| self.sum_xy / self.sum_y)
    }
}

/// Pearson correlation of two equal-length slices (`None` when either is constant).
pub fn pearson(xs: &[f64], ys: &[f64]) -> Option<f64> {
    let mut m = CoMoments::default();
    xs.iter().zip(ys).for_each(|(&x, &y)| m.add(x, y));
    m.corr()
}
