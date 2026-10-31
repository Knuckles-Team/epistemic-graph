//! Multi-factor regression attribution: `y_t = α + Σ_k β_k x_kt + ε_t` by OLS, with
//! Newey–West (HAC) standard errors and the TRUE residual series.
//!
//! The deleted AU `profit_attribution` reported `total - αn - βΣx` as a "residual
//! return". With an intercept, OLS residuals sum to zero, so that number was zero by
//! construction and never carried information. Here the residual SERIES is returned
//! (its autocorrelation is what the HAC errors correct for), and `residual_sum` is
//! reported only so a caller can see that it is zero.

use ndarray::{Array1, Array2, ArrayView1};

use super::{invalid, AttributionCode, AttributionError, AttributionResult};
use crate::detkernel::math;
use crate::detkernel::reduce::serial_sum;
use crate::linalg;

/// How many autocovariance lags the Newey–West estimator uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HacLags {
    /// `floor(4 (n / 100)^(2/9))` (Newey & West 1994).
    Auto,
    /// A fixed lag count (`0` is White's heteroskedasticity-robust estimator).
    Fixed(usize),
}

impl HacLags {
    fn resolve(self, n: usize) -> usize {
        match self {
            Self::Auto => (4.0 * math::pow(n as f64 / 100.0, 2.0 / 9.0)).floor() as usize,
            Self::Fixed(lags) => lags,
        }
        .min(n.saturating_sub(1))
    }
}

/// A fitted factor attribution.
#[derive(Debug, Clone, PartialEq)]
pub struct FactorAttribution {
    pub alpha: f64,
    /// One loading per factor, in factor order.
    pub betas: Vec<f64>,
    /// HAC standard error of `alpha`.
    pub alpha_se: f64,
    /// HAC standard errors of the loadings.
    pub beta_se: Vec<f64>,
    pub r_squared: f64,
    /// `ε_t = y_t - ŷ_t`, in time order.
    pub residuals: Vec<f64>,
    /// The Newey–West lag count used.
    pub lags: usize,
    /// `α · n`: the part of `Σ y` the intercept earns.
    pub alpha_contribution: f64,
    /// `β_k Σ_t x_kt` per factor.
    pub factor_contributions: Vec<f64>,
    /// `Σ ε_t` — zero up to rounding whenever an intercept is fitted.
    pub residual_sum: f64,
}

fn design(y: &[f64], factors: &[Vec<f64>]) -> AttributionResult<Array2<f64>> {
    let n = y.len();
    let p = factors.len() + 1;
    if n <= p {
        return Err(invalid(format!(
            "{n} observations cannot fit {p} coefficients"
        )));
    }
    if factors.iter().any(|f| f.len() != n) {
        return Err(invalid(
            "every factor series must have one value per observation",
        ));
    }
    let finite = y
        .iter()
        .chain(factors.iter().flatten())
        .all(|v| v.is_finite());
    if !finite {
        return Err(invalid("the regression inputs must be finite"));
    }
    Ok(Array2::from_shape_fn((n, p), |(t, j)| {
        if j == 0 {
            1.0
        } else {
            factors[j - 1][t]
        }
    }))
}

/// Reciprocal condition number below which a Gram matrix counts as singular.
const RCOND_FLOOR: f64 = 1e-10;

/// `gram⁻¹` for a symmetric positive semi-definite Gram matrix, or a typed refusal when
/// it is rank-deficient (judged by its singular values, not by an LU pivot happening to
/// be exactly zero). `what` names the design in the refusal.
pub(super) fn checked_inverse(gram: &Array2<f64>, what: &str) -> AttributionResult<Array2<f64>> {
    let singular = |detail: String| {
        AttributionError::new(AttributionCode::Singular, format!("{what}: {detail}"))
    };
    let values = linalg::svdvals(gram.view());
    let largest = values.iter().copied().fold(0.0f64, f64::max);
    let smallest = values.iter().copied().fold(f64::INFINITY, f64::min);
    if !(largest > 0.0 && smallest / largest > RCOND_FLOOR) {
        return Err(singular("rank-deficient".into()));
    }
    linalg::inverse(gram.view()).map_err(|e| singular(e.to_string()))
}

/// `(XᵀX)⁻¹`, or a typed refusal for a rank-deficient design.
fn bread(x: &Array2<f64>) -> AttributionResult<Array2<f64>> {
    let gram = linalg::matmul(x.t(), x.view()).map_err(|e| invalid(e.to_string()))?;
    checked_inverse(&gram, "XᵀX is singular")
}

/// The Newey–West "meat" `Σ_l w_l Σ_t e_t e_{t-l} (x_t x_{t-l}ᵀ + x_{t-l} x_tᵀ)` (`l = 0`
/// counted once), with Bartlett weights `w_l = 1 - l / (L + 1)`.
fn meat(x: &Array2<f64>, residuals: &[f64], lags: usize) -> Array2<f64> {
    let p = x.ncols();
    let mut s = Array2::<f64>::zeros((p, p));
    for lag in 0..=lags {
        let weight = 1.0 - lag as f64 / (lags as f64 + 1.0);
        for t in lag..residuals.len() {
            let scale = weight * residuals[t] * residuals[t - lag];
            add_cross(&mut s, x.row(t), x.row(t - lag), scale, lag == 0);
        }
    }
    s
}

/// `s += scale (a bᵀ + b aᵀ)`, or `s += scale a aᵀ` for the lag-0 term.
fn add_cross(
    s: &mut Array2<f64>,
    a: ArrayView1<f64>,
    b: ArrayView1<f64>,
    scale: f64,
    diagonal: bool,
) {
    let p = a.len();
    for i in 0..p {
        for j in 0..p {
            let pair = if diagonal {
                a[i] * a[j]
            } else {
                a[i] * b[j] + b[i] * a[j]
            };
            s[[i, j]] += scale * pair;
        }
    }
}

fn r_squared(y: &[f64], residuals: &[f64]) -> f64 {
    let mean = serial_sum(y) / y.len() as f64;
    let total: Vec<f64> = y.iter().map(|v| (v - mean) * (v - mean)).collect();
    let error: Vec<f64> = residuals.iter().map(|e| e * e).collect();
    let total = serial_sum(&total);
    if total == 0.0 {
        return 0.0;
    }
    1.0 - serial_sum(&error) / total
}

/// Fit `y` on `factors` (each a series aligned with `y`) with an intercept.
pub fn factor_ols(
    y: &[f64],
    factors: &[Vec<f64>],
    lags: HacLags,
) -> AttributionResult<FactorAttribution> {
    let x = design(y, factors)?;
    let inverse_gram = bread(&x)?;
    let target = Array1::from(y.to_vec());
    let xty = x.t().dot(&target);
    let coefficients = inverse_gram.dot(&xty);
    let fitted = x.dot(&coefficients);
    let residuals: Vec<f64> = y.iter().zip(fitted.iter()).map(|(a, b)| a - b).collect();
    let lags = lags.resolve(y.len());
    let covariance = inverse_gram
        .dot(&meat(&x, &residuals, lags))
        .dot(&inverse_gram);
    let se: Vec<f64> = (0..x.ncols())
        .map(|j| covariance[[j, j]].max(0.0).sqrt())
        .collect();
    let factor_contributions = factors
        .iter()
        .enumerate()
        .map(|(k, series)| coefficients[k + 1] * serial_sum(series))
        .collect();
    Ok(FactorAttribution {
        alpha: coefficients[0],
        betas: coefficients.iter().skip(1).copied().collect(),
        alpha_se: se[0],
        beta_se: se[1..].to_vec(),
        r_squared: r_squared(y, &residuals),
        alpha_contribution: coefficients[0] * y.len() as f64,
        factor_contributions,
        residual_sum: serial_sum(&residuals),
        residuals,
        lags,
    })
}
