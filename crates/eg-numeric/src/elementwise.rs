//! Element-wise ops over 1-D `f64` arrays — the numpy
//! `sqrt/log/exp/abs/tanh/clip/where/maximum/minimum/nan_to_num/isnan` surface.
//! Pure Rust, ndarray-backed; parity-tested vs numpy.

use crate::error::{NumericError, Result};
use ndarray::{Array1, ArrayView1};

pub fn sqrt(a: ArrayView1<f64>) -> Array1<f64> {
    a.mapv(f64::sqrt)
}

pub fn log(a: ArrayView1<f64>) -> Array1<f64> {
    a.mapv(f64::ln)
}

pub fn exp(a: ArrayView1<f64>) -> Array1<f64> {
    a.mapv(f64::exp)
}

pub fn abs(a: ArrayView1<f64>) -> Array1<f64> {
    a.mapv(f64::abs)
}

pub fn tanh(a: ArrayView1<f64>) -> Array1<f64> {
    a.mapv(f64::tanh)
}

/// numpy `clip(a, a_min, a_max)`; either bound may be `None` (`f64::NEG/INFINITY`).
pub fn clip(a: ArrayView1<f64>, lo: f64, hi: f64) -> Array1<f64> {
    a.mapv(|x| {
        let x = if x < lo { lo } else { x };
        if x > hi {
            hi
        } else {
            x
        }
    })
}

/// numpy `nan_to_num(a, nan=, posinf=, neginf=)`.
pub fn nan_to_num(a: ArrayView1<f64>, nan: f64, posinf: f64, neginf: f64) -> Array1<f64> {
    a.mapv(|x| {
        if x.is_nan() {
            nan
        } else if x == f64::INFINITY {
            posinf
        } else if x == f64::NEG_INFINITY {
            neginf
        } else {
            x
        }
    })
}

pub fn isnan(a: ArrayView1<f64>) -> Vec<bool> {
    a.iter().map(|x| x.is_nan()).collect()
}

/// numpy `maximum(a, b)` — element-wise (NaN propagates).
pub fn maximum(a: ArrayView1<f64>, b: ArrayView1<f64>) -> Result<Array1<f64>> {
    if a.len() != b.len() {
        return Err(NumericError::shape("maximum: shape mismatch"));
    }
    Ok(Array1::from_iter(a.iter().zip(b.iter()).map(|(&x, &y)| {
        if x.is_nan() || y.is_nan() {
            f64::NAN
        } else {
            x.max(y)
        }
    })))
}

pub fn minimum(a: ArrayView1<f64>, b: ArrayView1<f64>) -> Result<Array1<f64>> {
    if a.len() != b.len() {
        return Err(NumericError::shape("minimum: shape mismatch"));
    }
    Ok(Array1::from_iter(a.iter().zip(b.iter()).map(|(&x, &y)| {
        if x.is_nan() || y.is_nan() {
            f64::NAN
        } else {
            x.min(y)
        }
    })))
}

/// numpy `where(cond, a, b)` element-wise select.
pub fn where_(cond: &[bool], a: ArrayView1<f64>, b: ArrayView1<f64>) -> Result<Array1<f64>> {
    if cond.len() != a.len() || a.len() != b.len() {
        return Err(NumericError::shape("where: shape mismatch"));
    }
    Ok(Array1::from_iter(
        cond.iter()
            .zip(a.iter().zip(b.iter()))
            .map(|(&c, (&x, &y))| if c { x } else { y }),
    ))
}
