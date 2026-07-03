//! Reductions & order statistics over 1-D `f64` arrays — the numpy
//! `sum/mean/std/var/min/max/prod/argmin/argmax/argsort/percentile/cumsum/cumprod`
//! surface (§4 of the plan). Pure Rust, ndarray-backed; parity-tested vs numpy.

use crate::error::{NumericError, Result};
use ndarray::{Array1, ArrayView1};

pub fn sum(a: ArrayView1<f64>) -> f64 {
    a.sum()
}

pub fn prod(a: ArrayView1<f64>) -> f64 {
    a.iter().product()
}

pub fn mean(a: ArrayView1<f64>) -> f64 {
    if a.is_empty() {
        return f64::NAN; // numpy: mean of empty → nan (+ RuntimeWarning)
    }
    a.sum() / a.len() as f64
}

/// Variance with `ddof` (0 = population, 1 = sample) — matches `np.var(a, ddof=)`.
pub fn var(a: ArrayView1<f64>, ddof: usize) -> f64 {
    let n = a.len();
    if n == 0 || n <= ddof {
        return f64::NAN;
    }
    let m = a.sum() / n as f64;
    let ss: f64 = a.iter().map(|&x| (x - m) * (x - m)).sum();
    ss / (n - ddof) as f64
}

pub fn std(a: ArrayView1<f64>, ddof: usize) -> f64 {
    var(a, ddof).sqrt()
}

pub fn min(a: ArrayView1<f64>) -> Result<f64> {
    if a.is_empty() {
        return Err(NumericError::shape("min of empty array"));
    }
    // numpy propagates NaN: if any element is NaN the result is NaN.
    Ok(a.iter().copied().fold(f64::INFINITY, |acc, x| {
        if acc.is_nan() || x.is_nan() {
            f64::NAN
        } else {
            acc.min(x)
        }
    }))
}

pub fn max(a: ArrayView1<f64>) -> Result<f64> {
    if a.is_empty() {
        return Err(NumericError::shape("max of empty array"));
    }
    Ok(a.iter().copied().fold(f64::NEG_INFINITY, |acc, x| {
        if acc.is_nan() || x.is_nan() {
            f64::NAN
        } else {
            acc.max(x)
        }
    }))
}

/// numpy `argmin`: index of the first minimum. NaN, if present, is treated as the
/// smallest (numpy returns the first NaN index).
pub fn argmin(a: ArrayView1<f64>) -> Result<usize> {
    if a.is_empty() {
        return Err(NumericError::shape("argmin of empty array"));
    }
    if let Some((i, _)) = a.iter().enumerate().find(|(_, x)| x.is_nan()) {
        return Ok(i);
    }
    let mut best = 0usize;
    let mut bv = f64::INFINITY;
    for (i, &x) in a.iter().enumerate() {
        if x < bv {
            bv = x;
            best = i;
        }
    }
    Ok(best)
}

pub fn argmax(a: ArrayView1<f64>) -> Result<usize> {
    if a.is_empty() {
        return Err(NumericError::shape("argmax of empty array"));
    }
    if let Some((i, _)) = a.iter().enumerate().find(|(_, x)| x.is_nan()) {
        return Ok(i);
    }
    let mut best = 0usize;
    let mut bv = f64::NEG_INFINITY;
    for (i, &x) in a.iter().enumerate() {
        if x > bv {
            bv = x;
            best = i;
        }
    }
    Ok(best)
}

/// numpy `argsort` (kind='quicksort' → not stable, but for parity we use a stable
/// sort which matches numpy's 'stable'/'mergesort' and is a valid argsort).
pub fn argsort(a: ArrayView1<f64>) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..a.len()).collect();
    idx.sort_by(|&i, &j| {
        // numpy sorts NaN to the end.
        let (x, y) = (a[i], a[j]);
        match (x.is_nan(), y.is_nan()) {
            (true, true) => i.cmp(&j),
            (true, false) => std::cmp::Ordering::Greater,
            (false, true) => std::cmp::Ordering::Less,
            (false, false) => x.partial_cmp(&y).unwrap().then(i.cmp(&j)),
        }
    });
    idx
}

pub fn cumsum(a: ArrayView1<f64>) -> Array1<f64> {
    let mut acc = 0.0;
    Array1::from_iter(a.iter().map(|&x| {
        acc += x;
        acc
    }))
}

pub fn cumprod(a: ArrayView1<f64>) -> Array1<f64> {
    let mut acc = 1.0;
    Array1::from_iter(a.iter().map(|&x| {
        acc *= x;
        acc
    }))
}

/// numpy `percentile` with the default `linear` interpolation. `q` in [0, 100].
pub fn percentile(a: ArrayView1<f64>, q: f64) -> Result<f64> {
    quantile(a, q / 100.0)
}

/// numpy `quantile` (`q` in [0, 1], `linear` interpolation).
pub fn quantile(a: ArrayView1<f64>, q: f64) -> Result<f64> {
    if a.is_empty() {
        return Err(NumericError::shape("percentile of empty array"));
    }
    if !(0.0..=1.0).contains(&q) {
        return Err(NumericError::shape(
            "q must be in [0, 1] (or [0, 100] for percentile)",
        ));
    }
    let mut v: Vec<f64> = a.to_vec();
    v.sort_by(|x, y| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal));
    let n = v.len();
    if n == 1 {
        return Ok(v[0]);
    }
    let pos = q * (n as f64 - 1.0);
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    let frac = pos - lo as f64;
    Ok(v[lo] + (v[hi] - v[lo]) * frac)
}
