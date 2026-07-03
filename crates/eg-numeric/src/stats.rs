//! scipy.stats-parity ops (CONCEPT:EG-357/EG-358) — the STOP-list scipy functions
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
//! numerics). This module is gated behind the `analytics` feature (pulled by
//! `python`) so an engine `pi`/`default` build never links `statrs`.

use crate::error::{NumericError, Result};
use ndarray::ArrayView1;
use statrs::distribution::{Continuous, ContinuousCDF, Normal, StudentsT};

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

/// Pearson product-moment correlation of two equal-length slices.
fn pearson(x: &[f64], y: &[f64]) -> f64 {
    let n = x.len() as f64;
    let mx = x.iter().sum::<f64>() / n;
    let my = y.iter().sum::<f64>() / n;
    let (mut sxy, mut sxx, mut syy) = (0.0f64, 0.0f64, 0.0f64);
    for (&xi, &yi) in x.iter().zip(y.iter()) {
        let (dx, dy) = (xi - mx, yi - my);
        sxy += dx * dy;
        sxx += dx * dx;
        syy += dy * dy;
    }
    if sxx == 0.0 || syy == 0.0 {
        return f64::NAN;
    }
    sxy / (sxx.sqrt() * syy.sqrt())
}

/// `scipy.stats.spearmanr(x, y)` → `(rho, pvalue)`. `rho` is Pearson on the
/// average-ranked data; the two-sided p-value uses the Student-t transform
/// `t = rho·√((n-2)/(1-rho²))`, `df = n-2` (scipy's default). `p` is `NaN` when
/// `n < 3` (undefined), and `0.0` for a perfect ±1 correlation.
pub fn spearmanr(x: ArrayView1<f64>, y: ArrayView1<f64>) -> Result<(f64, f64)> {
    if x.len() != y.len() {
        return Err(NumericError::shape("spearmanr: x and y length mismatch"));
    }
    let n = x.len();
    if n < 2 {
        return Err(NumericError::shape(
            "spearmanr: need at least 2 observations",
        ));
    }
    let rx = rankdata(x);
    let ry = rankdata(y);
    let rho = pearson(&rx, &ry);
    if n < 3 || rho.is_nan() {
        return Ok((rho, f64::NAN));
    }
    let denom = (1.0 - rho) * (1.0 + rho);
    let p = if denom <= 0.0 {
        0.0 // perfect ±1 correlation → t = ∞ → p = 0
    } else {
        let df = (n - 2) as f64;
        let t = rho * (df / denom).sqrt();
        let dist = StudentsT::new(0.0, 1.0, df)
            .map_err(|e| NumericError::linalg(format!("spearmanr: t-dist: {e}")))?;
        (2.0 * (1.0 - dist.cdf(t.abs()))).clamp(0.0, 1.0)
    };
    Ok((rho, p))
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
        let term = sign * (-2.0 * kf * kf * x * x).exp();
        sum += term;
        sign = -sign;
        if term.abs() < 1e-15 {
            break;
        }
    }
    (2.0 * sum).clamp(0.0, 1.0)
}

/// Two-sample Kolmogorov–Smirnov test → `(statistic, pvalue)`. The statistic
/// `D = max|F₁(x) − F₂(x)|` is **exact** (empirical-CDF merge walk) and matches
/// `scipy.stats.ks_2samp` bit-for-bit. The p-value is the **classic asymptotic**
/// Kolmogorov survival `Q(√(n₁n₂/(n₁+n₂))·D)` — i.e. exactly
/// `scipy.stats.kstwobign.sf(√(n₁n₂/(n₁+n₂))·D)`, the stable large-sample limit.
///
/// NOTE ON PARITY: scipy 1.11+ evaluates the **exact** `kstwo` distribution even
/// in its `method="asymp"` branch, so its default p-value carries a small-sample
/// continuity correction and differs from this pure-asymptotic value by ~1e-3 at
/// modest n (the two converge as n grows). We deliberately keep the classic
/// asymptotic here rather than re-implementing scipy's exact Marsaglia–Tsang–Wang
/// `kstwo` CDF (which would be exactly the "hand-rolled unstable numerics" the
/// kernel avoids). `D` is exact; the p-value is the textbook asymptotic.
pub fn ks_2samp(a: ArrayView1<f64>, b: ArrayView1<f64>) -> Result<(f64, f64)> {
    let (n1, n2) = (a.len(), b.len());
    if n1 == 0 || n2 == 0 {
        return Err(NumericError::shape("ks_2samp: empty sample"));
    }
    let mut xa: Vec<f64> = a.to_vec();
    let mut xb: Vec<f64> = b.to_vec();
    xa.sort_by(|p, q| p.partial_cmp(q).unwrap_or(std::cmp::Ordering::Equal));
    xb.sort_by(|p, q| p.partial_cmp(q).unwrap_or(std::cmp::Ordering::Equal));
    let (fn1, fn2) = (n1 as f64, n2 as f64);
    let (mut i, mut j) = (0usize, 0usize);
    let mut d = 0.0f64;
    // Pool the two sorted samples; at each distinct value advance BOTH empirical
    // CDFs past every tie of that value, then measure the gap — the standard
    // two-sample KS walk (advancing only one pointer on equal values would invent
    // a spurious gap for identical samples).
    while i < n1 && j < n2 {
        let v = xa[i].min(xb[j]);
        while i < n1 && xa[i] == v {
            i += 1;
        }
        while j < n2 && xb[j] == v {
            j += 1;
        }
        let gap = (i as f64 / fn1) - (j as f64 / fn2);
        d = d.max(gap.abs());
    }
    let en = (fn1 * fn2 / (fn1 + fn2)).sqrt();
    let p = kolmogorov_sf(en * d);
    Ok((d, p))
}

/// `scipy.stats.norm.ppf(q, loc, scale)` — the inverse CDF (quantile). `q ∈ (0,1)`.
pub fn norm_ppf(q: f64, loc: f64, scale: f64) -> Result<f64> {
    if !(0.0..=1.0).contains(&q) {
        return Err(NumericError::shape("norm.ppf: q must be in [0, 1]"));
    }
    let dist =
        Normal::new(loc, scale).map_err(|e| NumericError::linalg(format!("norm.ppf: {e}")))?;
    Ok(dist.inverse_cdf(q))
}

/// `scipy.stats.norm.pdf(x, loc, scale)` — the probability density.
pub fn norm_pdf(x: f64, loc: f64, scale: f64) -> Result<f64> {
    let dist =
        Normal::new(loc, scale).map_err(|e| NumericError::linalg(format!("norm.pdf: {e}")))?;
    Ok(dist.pdf(x))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::array;

    // Parity anchors computed with scipy 1.x (CONCEPT:EG-357/EG-358).
    #[test]
    fn spearman_monotonic_is_one() {
        let x = array![1.0, 2.0, 3.0, 4.0, 5.0];
        let y = array![2.0, 4.0, 6.0, 8.0, 10.0];
        let (rho, p) = spearmanr(x.view(), y.view()).unwrap();
        assert!((rho - 1.0).abs() < 1e-12);
        assert!(p.abs() < 1e-12);
    }

    #[test]
    fn spearman_matches_scipy_value() {
        // scipy.stats.spearmanr([1,2,3,4,5],[5,6,7,8,7]) → rho≈0.82078, p≈0.088587
        let x = array![1.0, 2.0, 3.0, 4.0, 5.0];
        let y = array![5.0, 6.0, 7.0, 8.0, 7.0];
        let (rho, p) = spearmanr(x.view(), y.view()).unwrap();
        assert!((rho - 0.8207826816681233).abs() < 1e-9, "rho={rho}");
        assert!((p - 0.08858700531354384).abs() < 1e-9, "p={p}");
    }

    #[test]
    fn ks_identical_samples_zero() {
        let a = array![1.0, 2.0, 3.0, 4.0];
        let b = array![1.0, 2.0, 3.0, 4.0];
        let (d, p) = ks_2samp(a.view(), b.view()).unwrap();
        assert!(d.abs() < 1e-12);
        assert!((p - 1.0).abs() < 1e-9);
    }

    #[test]
    fn ks_disjoint_samples_one() {
        let a = array![1.0, 2.0, 3.0];
        let b = array![10.0, 11.0, 12.0];
        let (d, _p) = ks_2samp(a.view(), b.view()).unwrap();
        assert!((d - 1.0).abs() < 1e-12);
    }

    #[test]
    fn norm_ppf_pdf_anchors() {
        // scipy.stats.norm.ppf(0.975) → 1.959963984540054
        assert!((norm_ppf(0.975, 0.0, 1.0).unwrap() - 1.959963984540054).abs() < 1e-9);
        // norm.ppf(0.5) = loc
        assert!((norm_ppf(0.5, 3.0, 2.0).unwrap() - 3.0).abs() < 1e-12);
        // scipy.stats.norm.pdf(0) → 0.3989422804014327
        assert!((norm_pdf(0.0, 0.0, 1.0).unwrap() - 0.3989422804014327).abs() < 1e-12);
    }
}
