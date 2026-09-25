//! Sliding dot products by FFT (EH-529): `QT[i] = Σ_k q[k]·t[i+k]` for every placement
//! of a query `q` along a series `t`, in O(n log n) — the core of MASS.
//!
//! The transform is the `rustfft` crate (coordinator ruling 2026-09-24). Its planner picks
//! the AVX / SSE / NEON kernels at RUNTIME when the CPU has them and the scalar kernels
//! otherwise, so one binary serves AVX2 and non-AVX2 nodes. The two paths may differ in the
//! last bits; every distance a caller REPORTS is re-derived from a direct dot product
//! (see `mass`), so FFT rounding only ever orders candidates.

use std::sync::Arc;

use rustfft::num_complex::Complex;
use rustfft::{Fft, FftPlanner};

/// A series prepared for repeated sliding dot products with queries of length `m`.
pub struct Correlator {
    m: usize,
    placements: usize,
    spectrum: Vec<Complex<f64>>,
    forward: Arc<dyn Fft<f64>>,
    inverse: Arc<dyn Fft<f64>>,
}

impl Correlator {
    /// Transform `series` once for queries of length `m` (`1 ≤ m ≤ series.len()`).
    pub fn new(series: &[f64], m: usize) -> Self {
        let len = series.len() + m;
        let mut planner = FftPlanner::new();
        let forward = planner.plan_fft_forward(len);
        let inverse = planner.plan_fft_inverse(len);
        let mut spectrum = padded(series.iter().copied(), len);
        forward.process(&mut spectrum);
        Self {
            m,
            placements: series.len() + 1 - m,
            spectrum,
            forward,
            inverse,
        }
    }

    /// `QT[i]` for every placement `i` of `query` (length `m`).
    pub fn dots(&self, query: &[f64]) -> Vec<f64> {
        let len = self.spectrum.len();
        let mut product = padded(query.iter().rev().copied(), len);
        self.forward.process(&mut product);
        for (p, s) in product.iter_mut().zip(&self.spectrum) {
            *p *= *s;
        }
        self.inverse.process(&mut product);
        let scale = 1.0 / len as f64;
        product[self.m - 1..self.m - 1 + self.placements]
            .iter()
            .map(|c| c.re * scale)
            .collect()
    }
}

fn padded(values: impl Iterator<Item = f64>, len: usize) -> Vec<Complex<f64>> {
    let mut out: Vec<Complex<f64>> = values.map(|v| Complex::new(v, 0.0)).collect();
    out.resize(len, Complex::new(0.0, 0.0));
    out
}
