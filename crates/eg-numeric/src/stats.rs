//! scipy.stats-parity ops (CONCEPT:EG-KG.compute.numeric-stats/EG-358) — the STOP-list scipy functions
//! `agent_utilities` needs with no existing kernel equivalent:
//!
//! * [`spearmanr`] — Spearman rank correlation (rank-transform + Pearson) + a
//!   two-sided p-value from the Student-t approximation (`scipy.stats.spearmanr`).
//! * [`ks_2samp`] — the two-sample Kolmogorov–Smirnov statistic `D` + the
//!   asymptotic p-value (`scipy.stats.ks_2samp(method="asymp")`).
//! * [`norm_ppf`] / [`norm_pdf`] — the normal distribution inverse-CDF (quantile)
//!   and density (`scipy.stats.norm.ppf` / `.pdf`).
//!
//! The distribution CDFs/quantiles come from `statrs` (Student-t, Normal) rather
//! than hand-rolled `erfinv`; the Kolmogorov survival series is the *definition*
//! of the KS distribution (a standard, stable, convergent series — not unstable
//! numerics). Those `statrs`-backed functions live in [`scipy`], gated behind the
//! `analytics` feature (pulled by `python`) so an engine `pi`/`default` build never
//! links `statrs`.
//!
//! The standard-normal CDF [`norm_cdf`] (and its stable logarithm
//! [`norm_log_cdf`]) is ungated: it is the pinned-libm `erfc`, so its bits are
//! the same on every release target, and the barrier/first-passage kernels
//! (`crate::risk::barrier`) and the finance validation kernels share it.

use crate::detkernel::math;
use ndarray::ArrayView1;

#[cfg(feature = "analytics")]
mod scipy;
#[cfg(feature = "analytics")]
pub use scipy::{ks_2samp, norm_pdf, norm_ppf, spearmanr};

/// numpy/scipy `rankdata` with the default `average` tie-handling: ties share the
/// mean of the ranks they span (1-based ranks, matching scipy).
pub fn rankdata(a: ArrayView1<f64>) -> Vec<f64> {
    let n = a.len();
    let mut idx: Vec<usize> = (0..n).collect();
    idx.sort_by(|&i, &j| a[i].partial_cmp(&a[j]).unwrap_or(std::cmp::Ordering::Equal));
    let mut ranks = vec![0.0f64; n];
    let mut i = 0usize;
    while i < n {
        let mut j = i;
        while j + 1 < n && a[idx[j + 1]] == a[idx[i]] {
            j += 1;
        }
        // average of 1-based ranks (i+1 .. j+1)
        let avg = ((i + 1 + j + 1) as f64) / 2.0;
        for &t in &idx[i..=j] {
            ranks[t] = avg;
        }
        i = j + 1;
    }
    ranks
}

/// The Kolmogorov distribution survival function
/// `Q(x) = 2·Σ_{k≥1} (-1)^{k-1} e^{-2 k² x²}` — i.e. `scipy.stats.kstwobign.sf(x)`.
/// The series converges geometrically; 100 terms is far past machine precision for
/// any `x > 0`.
pub fn kolmogorov_sf(x: f64) -> f64 {
    if x <= 0.0 {
        return 1.0;
    }
    let mut sum = 0.0f64;
    let mut sign = 1.0f64;
    for k in 1..=100 {
        let kf = k as f64;
        let term = sign * math::exp(-2.0 * kf * kf * x * x);
        sum += term;
        sign = -sign;
        if term.abs() < 1e-15 {
            break;
        }
    }
    (2.0 * sum).clamp(0.0, 1.0)
}

/// `ln(sqrt(2*pi))`.
const LN_SQRT_2PI: f64 = 0.918_938_533_204_672_8;

/// Below this `x`, `erfc(-x / sqrt 2)` underflows and [`norm_log_cdf`] uses
/// the asymptotic tail series instead.
const LOG_CDF_TAIL: f64 = -37.0;

/// The standard-normal CDF `Phi(x)` and survival `1 - Phi(x)`: the one
/// implementation, `detkernel::kernels::{normal_cdf, normal_sf}` (pinned `erfc`,
/// relative accuracy in both tails), under the scipy-style names.
pub use crate::detkernel::kernels::{normal_cdf as norm_cdf, normal_sf as norm_sf};

/// `ln Phi(x)`, finite far into the lower tail: below [`LOG_CDF_TAIL`] it is
/// the Mills-ratio series `-x^2/2 - ln(-x) - ln sqrt(2 pi) + ln(1 - 1/x^2 + 3/x^4)`,
/// whose relative error there is below `1e-8`.
pub fn norm_log_cdf(x: f64) -> f64 {
    if x >= LOG_CDF_TAIL {
        return math::ln(norm_cdf(x));
    }
    let inv2 = 1.0 / (x * x);
    -0.5 * x * x - math::ln(-x) - LN_SQRT_2PI + math::ln_1p(-inv2 + 3.0 * inv2 * inv2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn norm_cdf_anchors() {
        // scipy.stats.norm.cdf: 0 -> 0.5, 1.96 -> 0.9750021048517795,
        // -3 -> 0.0013498980316300946.
        assert_eq!(norm_cdf(0.0), 0.5);
        assert!((norm_cdf(1.96) - 0.975_002_104_851_779_5).abs() < 1e-15);
        assert!((norm_cdf(-3.0) - 0.001_349_898_031_630_094_6).abs() < 1e-17);
        assert!((norm_cdf(2.5) + norm_cdf(-2.5) - 1.0).abs() < 1e-15);
    }

    #[test]
    fn norm_log_cdf_is_continuous_across_the_tail_switch() {
        // scipy.stats.norm.logcdf(-40) = -804.6084420137538
        assert!((norm_log_cdf(-40.0) - (-804.608_442_013_753_8)).abs() < 1e-6);
        let inside = norm_log_cdf(LOG_CDF_TAIL);
        let outside = norm_log_cdf(LOG_CDF_TAIL - 1e-9);
        assert!((inside - outside).abs() / inside.abs() < 1e-8);
        assert!((norm_log_cdf(0.0) - math::ln(0.5)).abs() < 1e-15);
    }

    #[test]
    fn kolmogorov_sf_bounds() {
        assert_eq!(kolmogorov_sf(0.0), 1.0);
        assert!(kolmogorov_sf(3.0) < 1e-6);
    }
}
