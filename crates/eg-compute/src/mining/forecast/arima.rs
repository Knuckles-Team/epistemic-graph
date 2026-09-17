//! ARIMA forecasting and its least-squares fitting kernels.

use super::{
    difference, flat_forecast, integrate_forecast, mean, residual_std, widening_bounds, Forecast,
};

// ─────────────────────────── ARIMA ───────────────────────────

/// ARIMA(p,d,q) (CONCEPT:EG-KG.mining.arima): difference `d` times to
/// stationarity, fit AR(p)/MA(q) via Hannan-Rissanen, forecast forward in the
/// differenced domain (future innovations ⇒ their expectation, 0), then
/// integrate back `d` times to the original scale.
pub fn arima(
    series: &[f64],
    p: usize,
    d: usize,
    q: usize,
    horizon: usize,
    confidence: f64,
) -> Forecast {
    let y = difference(series, d);
    if y.len() <= p.max(1) {
        // Not enough data after differencing — fall back to a flat forecast at
        // the last observed value (never panic on a short series).
        let last = *series.last().unwrap();
        return flat_forecast(last, horizon, 0.0, confidence);
    }
    let (c, phis, thetas, resid) = fit_arma(&y, p, q);

    // Forecast forward in the differenced domain.
    let mut y_ext = y.clone();
    let mut resid_ext = resid.clone();
    let mut diff_forecast = Vec::with_capacity(horizon);
    for _ in 0..horizon {
        let t = y_ext.len();
        let mut pred = c;
        for i in 1..=p {
            if t >= i {
                pred += phis[i - 1] * y_ext[t - i];
            }
        }
        for j in 1..=q {
            let val = if t >= j { resid_ext[t - j] } else { 0.0 };
            pred += thetas[j - 1] * val;
        }
        y_ext.push(pred);
        resid_ext.push(0.0); // E[future innovation] = 0
        diff_forecast.push(pred);
    }

    let point = integrate_forecast(&diff_forecast, series, d);
    let (lower, upper) = widening_bounds(&point, residual_std(&resid, p.max(q)), confidence);
    Forecast {
        values: point,
        lower,
        upper,
        trend: Vec::new(),
        seasonal: Vec::new(),
        residual: Vec::new(),
    }
}

/// Fit AR(p)/MA(q) coefficients over the (already-differenced) series `y` via
/// the Hannan-Rissanen two-stage method (CONCEPT:EG-KG.mining.arima): pure AR(p)
/// (`q == 0`) is a single OLS regression; otherwise a long auxiliary AR gives a
/// residual proxy `e`, and `y_t` is regressed on `[y_{t-1..t-p}, e_{t-1..t-q}]`.
/// Returns `(intercept, ar_coeffs, ma_coeffs, in-sample residuals)`.
fn fit_arma(y: &[f64], p: usize, q: usize) -> (f64, Vec<f64>, Vec<f64>, Vec<f64>) {
    let n = y.len();
    if q == 0 {
        let (c, phis, resid) = pure_ar_fit(y, p);
        return (c, phis, Vec::new(), resid);
    }

    // Stage 1: a long auxiliary AR gives a residual proxy `e`.
    let (long_order, e) = auxiliary_residuals(y, p, q);

    // Stage 2: joint AR+MA regression using the proxy residuals as MA regressors.
    let start = p.max(long_order + q).max(1);
    if start >= n {
        // Fixture too short for stage 2 — fall back to the pure-AR fit.
        let (c, phis, resid) = pure_ar_fit(y, p);
        return (c, phis, vec![0.0; q], resid);
    }
    let (c, phis, thetas) = joint_coefficients(y, &e, p, q, start);
    let resid = conditional_residuals(y, p, q, c, &phis, &thetas);
    (c, phis, thetas, resid)
}

fn pure_ar_fit(y: &[f64], p: usize) -> (f64, Vec<f64>, Vec<f64>) {
    let (c, phis) = ols_ar(y, p);
    let resid = ar_residuals(y, p, c, &phis);
    (c, phis, resid)
}

fn ar_residuals(y: &[f64], p: usize, c: f64, phis: &[f64]) -> Vec<f64> {
    let mut resid = vec![0.0; y.len()];
    for t in p..y.len() {
        let mut pred = c;
        for i in 1..=p {
            pred += phis[i - 1] * y[t - i];
        }
        resid[t] = y[t] - pred;
    }
    resid
}

fn auxiliary_residuals(y: &[f64], p: usize, q: usize) -> (usize, Vec<f64>) {
    let n = y.len();
    let long_order = (p + q + 5).min(n.saturating_sub(1)).max(1);
    let (long_c, long_phis) = ols_ar(y, long_order);
    let mut residuals = vec![0.0; n];
    for t in long_order..n {
        let mut pred = long_c;
        for i in 1..=long_order {
            pred += long_phis[i - 1] * y[t - i];
        }
        residuals[t] = y[t] - pred;
    }
    (long_order, residuals)
}

fn joint_coefficients(
    y: &[f64],
    residuals: &[f64],
    p: usize,
    q: usize,
    start: usize,
) -> (f64, Vec<f64>, Vec<f64>) {
    let mut rows: Vec<Vec<f64>> = Vec::with_capacity(y.len() - start);
    let mut targets: Vec<f64> = Vec::with_capacity(y.len() - start);
    for t in start..y.len() {
        let mut row = vec![1.0];
        for i in 1..=p {
            row.push(y[t - i]);
        }
        for j in 1..=q {
            row.push(residuals[t - j]);
        }
        rows.push(row);
        targets.push(y[t]);
    }
    let coeffs = ols_fit(&rows, &targets);
    let c = coeffs[0];
    let phis = coeffs[1..=p].to_vec();
    let thetas = coeffs[p + 1..p + 1 + q].to_vec();
    (c, phis, thetas)
}

fn conditional_residuals(
    y: &[f64],
    p: usize,
    q: usize,
    c: f64,
    phis: &[f64],
    thetas: &[f64],
) -> Vec<f64> {
    // Final residuals: the standard conditional-sum-of-squares recursion (each
    // residual uses the model's OWN previously computed residuals for the MA
    // terms, warm-started at 0).
    let mut resid = vec![0.0; y.len()];
    for t in 0..y.len() {
        let mut pred = c;
        for i in 1..=p {
            if t >= i {
                pred += phis[i - 1] * y[t - i];
            }
        }
        for j in 1..=q {
            if t >= j {
                pred += thetas[j - 1] * resid[t - j];
            }
        }
        resid[t] = y[t] - pred;
    }
    resid
}

/// Fit AR(p) by OLS: `y_t = c + sum_i phi_i*y_{t-i} + e_t`. Returns
/// `(intercept, phis)`; `phis` has length `p`.
fn ols_ar(y: &[f64], p: usize) -> (f64, Vec<f64>) {
    let n = y.len();
    if p == 0 || n <= p {
        return (mean(y), vec![0.0; p]);
    }
    let mut rows: Vec<Vec<f64>> = Vec::with_capacity(n - p);
    let mut targets: Vec<f64> = Vec::with_capacity(n - p);
    for t in p..n {
        let mut row = vec![1.0];
        for i in 1..=p {
            row.push(y[t - i]);
        }
        rows.push(row);
        targets.push(y[t]);
    }
    let coeffs = ols_fit(&rows, &targets);
    (coeffs[0], coeffs[1..].to_vec())
}

// ─────────────────────────── linear algebra ───────────────────────────

/// Ordinary least squares: solve `beta` minimizing `||X*beta - y||^2` via the
/// normal equations `(XᵀX)*beta = Xᵀy`, Gaussian elimination with partial
/// pivoting (a tiny ridge term keeps a near-singular system solvable). `x` rows
/// share one width (the intercept column, if wanted, is the caller's `1.0`
/// entry).
fn ols_fit(x: &[Vec<f64>], y: &[f64]) -> Vec<f64> {
    let k = x[0].len();
    let mut xtx = vec![vec![0.0; k]; k];
    let mut xty = vec![0.0; k];
    for (row, &yt) in x.iter().zip(y.iter()) {
        for i in 0..k {
            xty[i] += row[i] * yt;
            for j in 0..k {
                xtx[i][j] += row[i] * row[j];
            }
        }
    }
    for i in 0..k {
        xtx[i][i] += 1e-8;
    }
    solve_linear(xtx, xty)
}

/// Solve `a*x = b` via Gaussian elimination with partial pivoting. A
/// numerically singular pivot leaves that coefficient at 0 rather than
/// panicking or producing `NaN`.
fn solve_linear(mut a: Vec<Vec<f64>>, mut b: Vec<f64>) -> Vec<f64> {
    let n = b.len();
    for col in 0..n {
        let piv = pivot_row(&a, col);
        a.swap(col, piv);
        b.swap(col, piv);
        let d = a[col][col];
        if d.abs() < 1e-12 {
            continue;
        }
        eliminate_column(&mut a, &mut b, col, d);
    }
    diagonal_solution(&a, &b)
}

fn pivot_row(a: &[Vec<f64>], col: usize) -> usize {
    let mut pivot = col;
    for row in (col + 1)..a.len() {
        if a[row][col].abs() > a[pivot][col].abs() {
            pivot = row;
        }
    }
    pivot
}

fn eliminate_column(a: &mut [Vec<f64>], b: &mut [f64], col: usize, diagonal: f64) {
    for row in 0..a.len() {
        if row == col {
            continue;
        }
        let factor = a[row][col] / diagonal;
        if factor == 0.0 {
            continue;
        }
        for column in col..a.len() {
            a[row][column] -= factor * a[col][column];
        }
        b[row] -= factor * b[col];
    }
}

fn diagonal_solution(a: &[Vec<f64>], b: &[f64]) -> Vec<f64> {
    let n = b.len();
    (0..n)
        .map(|i| {
            if a[i][i].abs() > 1e-12 {
                b[i] / a[i][i]
            } else {
                0.0
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{arima, fit_arma, solve_linear};

    #[test]
    fn solve_linear_returns_zero_for_singular_pivots() {
        let solution = solve_linear(vec![vec![0.0, 0.0], vec![0.0, 0.0]], vec![1.0, 2.0]);
        assert_eq!(solution, vec![0.0, 0.0]);
    }

    #[test]
    fn arima_ma_branch_matches_independent_nonzero_theta_oracle() {
        let series = vec![
            1.0, 1.8, 1.2, 2.4, 1.6, 2.9, 2.1, 3.2, 2.8, 3.7, 3.1, 4.4, 3.9, 5.0, 4.5, 5.6,
        ];
        // Independently calculated from the normal-equation fit (ridge 1e-8)
        // and conditional-sum-of-squares recursion for this hand-counted series.
        let expected_theta = -0.2535505359397986;
        let expected_residuals = [
            -0.7400551110019349,
            -0.7515309207017735,
            -1.85350817014576,
            -0.5586144285275609,
            -1.778894753971549,
            -0.2892299327404153,
            -1.5225093905310012,
            -0.23614050627309924,
            -0.9961988699015705,
            -0.03937829947297056,
            -0.9582269267548553,
            0.48309917515846124,
            -0.462436590821802,
            0.7097395288038104,
            -0.17927247149762504,
            1.0072352792091896,
        ];
        let (_, _, theta, residuals) = fit_arma(&series, 1, 1);
        assert!((theta[0] - expected_theta).abs() < 1e-8);
        for (actual, expected) in residuals.iter().zip(expected_residuals) {
            assert!((actual - expected).abs() < 1e-8);
        }

        // The independently calculated q=1 continuation catches a zeroed or
        // incorrectly lagged MA residual even when shape checks still pass.
        let expected_forecasts = [4.978142928350715, 4.845592115358106, 4.762902353205693];
        let output = arima(&series, 1, 0, 1, 3, 0.95);
        for (actual, expected) in output.values.iter().zip(expected_forecasts) {
            assert!((actual - expected).abs() < 1e-8);
        }
    }
}
