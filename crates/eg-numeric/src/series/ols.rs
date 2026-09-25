//! Shared least-squares core for ADF and OU series models.

use nalgebra::{DMatrix, DVector};

/// Ordinary least squares returning coefficients, their standard errors, and the
/// residual variance. `x` rows are observations, columns are regressors (caller
/// supplies the intercept column explicitly if wanted).
pub(super) fn ols_with_se(x: &[Vec<f64>], y: &[f64]) -> Option<(Vec<f64>, Vec<f64>, f64)> {
    let n = x.len();
    if n == 0 {
        return None;
    }
    let k = x[0].len();
    if n <= k {
        return None;
    }
    let xm = DMatrix::from_fn(n, k, |i, j| x[i][j]);
    let yv = DVector::from_fn(n, |i, _| y[i]);
    let xtx = xm.transpose() * &xm;
    let xtx_inv = xtx.try_inverse()?;
    let beta = &xtx_inv * xm.transpose() * &yv;
    let resid = &yv - &xm * &beta;
    let rss: f64 = resid.iter().map(|e| e * e).sum();
    let dof = (n - k) as f64;
    let sigma2 = rss / dof;
    let ses: Vec<f64> = (0..k).map(|j| (sigma2 * xtx_inv[(j, j)]).sqrt()).collect();
    Some((beta.iter().copied().collect(), ses, sigma2))
}
