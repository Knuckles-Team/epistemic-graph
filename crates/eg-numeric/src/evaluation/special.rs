//! Special functions and moments the evaluation kernels share (EH-530, re-homed from
//! the finance quant module's private `sf`): the standard-normal quantile and sample
//! skewness / excess kurtosis. The CDF and survival are the ONE tail-accurate
//! [`crate::detkernel::kernels::normal_cdf`]/[`normal_sf`](crate::detkernel::kernels::normal_sf).
//! Transcendentals go through the pinned [`crate::detkernel::math`], so results are
//! bit-identical on every release target.

use crate::detkernel::math;

/// Inverse standard-normal CDF (Acklam's rational approximation, relative error
/// < 1.2e-9). `p <= 0` is `−∞`, `p >= 1` is `+∞`.
pub fn norm_ppf(p: f64) -> f64 {
    const PLOW: f64 = 0.02425;
    if p <= 0.0 {
        return f64::NEG_INFINITY;
    }
    if p >= 1.0 {
        return f64::INFINITY;
    }
    if p < PLOW {
        return tail(p);
    }
    if p > 1.0 - PLOW {
        return -tail(1.0 - p);
    }
    central(p)
}

fn central(p: f64) -> f64 {
    const A: [f64; 6] = [
        -3.969683028665376e+01,
        2.209460984245205e+02,
        -2.759285104469687e+02,
        1.383_577_518_672_69e2,
        -3.066479806614716e+01,
        2.506628277459239e+00,
    ];
    const B: [f64; 5] = [
        -5.447609879822406e+01,
        1.615858368580409e+02,
        -1.556989798598866e+02,
        6.680131188771972e+01,
        -1.328068155288572e+01,
    ];
    let q = p - 0.5;
    let r = q * q;
    (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
        / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
}

/// The lower tail; the upper tail is its mirror.
fn tail(p: f64) -> f64 {
    const C: [f64; 6] = [
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e+00,
        -2.549732539343734e+00,
        4.374664141464968e+00,
        2.938163982698783e+00,
    ];
    const D: [f64; 4] = [
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996e+00,
        3.754408661907416e+00,
    ];
    let q = (-2.0 * math::ln(p)).sqrt();
    (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
        / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
}

/// Central moments `(m2, m3, m4)` (population, ÷n) of a non-empty slice.
fn central_moments(d: &[f64]) -> (f64, f64, f64) {
    let n = d.len() as f64;
    let mean = d.iter().sum::<f64>() / n;
    let (mut m2, mut m3, mut m4) = (0.0, 0.0, 0.0);
    for x in d {
        let e = x - mean;
        let e2 = e * e;
        m2 += e2;
        m3 += e2 * e;
        m4 += e2 * e2;
    }
    (m2 / n, m3 / n, m4 / n)
}

/// Sample skewness (Fisher-Pearson, `m3 / m2^1.5`); `0` under three points or for a
/// flat sample.
pub fn skew(d: &[f64]) -> f64 {
    if d.len() < 3 {
        return 0.0;
    }
    let (m2, m3, _) = central_moments(d);
    if m2 <= 1e-18 {
        return 0.0;
    }
    m3 / (m2 * m2.sqrt())
}

/// Excess kurtosis (Fisher, `m4 / m2² − 3`); `0` under four points or for a flat sample.
pub fn excess_kurtosis(d: &[f64]) -> f64 {
    if d.len() < 4 {
        return 0.0;
    }
    let (m2, _, m4) = central_moments(d);
    if m2 <= 1e-18 {
        return 0.0;
    }
    m4 / (m2 * m2) - 3.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::detkernel::kernels::normal_cdf;

    #[test]
    fn the_normal_cdf_and_quantile_invert_each_other() {
        assert!((normal_cdf(1.959_963_984_540_054) - 0.975).abs() < 1e-15);
        for p in [1e-6, 0.01, 0.2, 0.5, 0.9, 0.999] {
            assert!(
                (normal_cdf(norm_ppf(p)) - p).abs() < 1e-8 * p.max(1e-3),
                "p = {p}"
            );
        }
        assert_eq!(norm_ppf(0.0), f64::NEG_INFINITY);
    }

    #[test]
    fn moments_of_a_symmetric_and_a_flat_sample() {
        let symmetric = [-2.0, -1.0, 0.0, 1.0, 2.0];
        assert_eq!(skew(&symmetric), 0.0);
        assert!((excess_kurtosis(&symmetric) - (-1.3)).abs() < 1e-12);
        assert_eq!(skew(&[3.0; 6]), 0.0);
        assert_eq!(excess_kurtosis(&[3.0; 6]), 0.0);
    }
}
