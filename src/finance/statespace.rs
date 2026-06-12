// CONCEPT:KG-2.20h — State-Space & Statistical-Arbitrage Kernels
//
// Real-time hidden-state estimation (Kalman filters) and cross-market
// statistical arbitrage (cointegration / Ornstein-Uhlenbeck) for the finance
// domain. Batched, stateless, served over the Tokio MessagePack protocol.
//
// Primary sources:
//   - Kalman (1960) "A New Approach to Linear Filtering and Prediction Problems"
//   - Dynamic-beta & log-variance volatility state-space models (every quant desk)
//   - Engle-Granger / Augmented Dickey-Fuller cointegration testing
//   - Ornstein-Uhlenbeck mean reversion; MFPT-optimal entry/exit thresholds

use nalgebra::{DMatrix, DVector};
use serde::{Deserialize, Serialize};

// ════════════════════════════════════════════════════════════════════════
//  OLS with standard errors (shared helper for ADF / OU)
// ════════════════════════════════════════════════════════════════════════

/// Ordinary least squares returning coefficients, their standard errors, and the
/// residual variance. `x` rows are observations, columns are regressors (caller
/// supplies the intercept column explicitly if wanted).
fn ols_with_se(x: &[Vec<f64>], y: &[f64]) -> Option<(Vec<f64>, Vec<f64>, f64)> {
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

// ════════════════════════════════════════════════════════════════════════
//  Kalman filters
// ════════════════════════════════════════════════════════════════════════

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KalmanState {
    pub states: Vec<f64>,
    pub variances: Vec<f64>,
}

/// Scalar Kalman filter with constant matrices: x_t = F x_{t-1} + w (Q);
/// z_t = H x_t + v (R). Returns the filtered state + variance at each step.
pub fn kalman_filter_1d(
    observations: &[f64],
    f: f64,
    q: f64,
    h: f64,
    r: f64,
    x0: f64,
    p0: f64,
) -> KalmanState {
    let n = observations.len();
    let mut states = vec![0.0; n];
    let mut variances = vec![0.0; n];
    let (mut x, mut p) = (x0, p0);
    for t in 0..n {
        // predict
        x *= f;
        p = f * p * f + q;
        // update
        let y = observations[t] - h * x;
        let s = h * p * h + r;
        let k = if s.abs() > 1e-18 { p * h / s } else { 0.0 };
        x += k * y;
        p *= 1.0 - k * h;
        states[t] = x;
        variances[t] = p;
    }
    KalmanState { states, variances }
}

/// Dynamic beta via Kalman filter: hidden state β follows a random walk
/// (process noise q), measurement is r_asset = β · r_market + v (noise r), so the
/// measurement matrix is time-varying H_t = r_market,t. Returns β + variance series.
pub fn kalman_beta(
    market_returns: &[f64],
    asset_returns: &[f64],
    q: f64,
    r: f64,
    beta0: f64,
    p0: f64,
) -> KalmanState {
    let n = market_returns.len().min(asset_returns.len());
    let mut betas = vec![0.0; n];
    let mut variances = vec![0.0; n];
    let (mut beta, mut p) = (beta0, p0);
    for t in 0..n {
        // predict (random walk: F = 1)
        p += q;
        // update with time-varying H = market return
        let h = market_returns[t];
        let y = asset_returns[t] - h * beta;
        let s = h * p * h + r;
        let k = if s.abs() > 1e-18 { p * h / s } else { 0.0 };
        beta += k * y;
        p *= 1.0 - k * h;
        betas[t] = beta;
        variances[t] = p;
    }
    KalmanState {
        states: betas,
        variances,
    }
}

/// Kalman volatility tracker. Hidden state is log-variance (random walk, noise q);
/// measurement is log(r_t²) = log σ²_t + η. Returns the ANNUALISED volatility
/// series (√(σ²·252)). `log_var0=None` seeds from the first ≤60 observations.
pub fn kalman_volatility(
    returns: &[f64],
    q: f64,
    r: f64,
    log_var0: Option<f64>,
    p0: f64,
    annualization: f64,
) -> Vec<f64> {
    let n = returns.len();
    if n == 0 {
        return vec![];
    }
    let log_sq: Vec<f64> = returns.iter().map(|x| (x * x).max(1e-12).ln()).collect();
    let seed = log_var0.unwrap_or_else(|| {
        let m = n.min(60);
        log_sq[..m].iter().sum::<f64>() / m as f64
    });
    let (mut log_var, mut p) = (seed, p0);
    let mut out = vec![0.0; n];
    for t in 0..n {
        p += q;
        let y = log_sq[t] - log_var;
        let s = p + r;
        let k = if s.abs() > 1e-18 { p / s } else { 0.0 };
        log_var += k * y;
        p *= 1.0 - k;
        out[t] = (log_var.exp() * annualization).sqrt();
    }
    out
}

// ════════════════════════════════════════════════════════════════════════
//  Cointegration: Augmented Dickey-Fuller
// ════════════════════════════════════════════════════════════════════════

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AdfResult {
    pub statistic: f64,
    pub used_lag: usize,
    pub n_obs: usize,
    /// Finite-sample-interpolated MacKinnon critical values (constant, no trend).
    pub crit_1pct: f64,
    pub crit_5pct: f64,
    pub crit_10pct: f64,
    /// Approximate p-value (monotone interpolation across the critical points).
    pub p_value_approx: f64,
    pub stationary_1pct: bool,
    pub stationary_5pct: bool,
    pub stationary_10pct: bool,
}

/// Finite-sample MacKinnon critical value for the ADF "constant, no trend" case:
/// CV(T) = β∞ + β1/T + β2/T² (MacKinnon 1991 response-surface coefficients).
fn mackinnon_crit(level: u8, t: f64) -> f64 {
    // (β∞, β1, β2) per significance level for the τ_c (constant) case.
    let (b0, b1, b2) = match level {
        1 => (-3.43035, -6.5393, -16.786),
        5 => (-2.86154, -2.8903, -4.234),
        _ => (-2.56677, -1.5384, -2.809), // 10%
    };
    b0 + b1 / t + b2 / (t * t)
}

/// Monotone approximate p-value from the ADF statistic and the three interpolated
/// critical values. Piecewise-linear in the statistic; clearly an approximation
/// (the exact value needs MacKinnon's full surface), but correct in ordering and
/// bracket — more useful than a single fixed cutoff.
fn adf_pvalue(stat: f64, c1: f64, c5: f64, c10: f64) -> f64 {
    // anchors: (stat, p). More-negative stat ⇒ smaller p.
    if stat <= c1 {
        return (0.01 * (stat / c1).clamp(0.0, 1.0)).clamp(1e-4, 0.01);
    }
    let interp = |s: f64, lo_s: f64, hi_s: f64, lo_p: f64, hi_p: f64| {
        lo_p + (s - lo_s) / (hi_s - lo_s) * (hi_p - lo_p)
    };
    if stat <= c5 {
        return interp(stat, c1, c5, 0.01, 0.05);
    }
    if stat <= c10 {
        return interp(stat, c5, c10, 0.05, 0.10);
    }
    // above 10% critical: p rises toward ~1 as stat → 0+
    interp(stat, c10, 0.0, 0.10, 0.90).clamp(0.10, 0.999)
}

/// Augmented Dickey-Fuller test (constant, no trend). Regresses
/// Δy_t = α + γ·y_{t-1} + Σ δ_i Δy_{t-i} + ε; the ADF statistic is the t-stat on γ.
pub fn adf_test(series: &[f64], max_lag: usize) -> AdfResult {
    let n = series.len();
    // need enough points after differencing + lags
    if n < max_lag + 4 {
        return AdfResult {
            statistic: 0.0,
            used_lag: max_lag,
            n_obs: 0,
            crit_1pct: -3.43,
            crit_5pct: -2.86,
            crit_10pct: -2.57,
            p_value_approx: 1.0,
            stationary_1pct: false,
            stationary_5pct: false,
            stationary_10pct: false,
        };
    }
    let dy: Vec<f64> = (1..n).map(|i| series[i] - series[i - 1]).collect();
    // build design: rows from t=max_lag .. dy.len()-1
    let start = max_lag;
    let mut xrows: Vec<Vec<f64>> = vec![];
    let mut yvec: Vec<f64> = vec![];
    for t in start..dy.len() {
        let mut row = vec![1.0, series[t]]; // intercept, y_{t-1} (series index t aligns with dy[t]=series[t+1]-series[t])
        for i in 1..=max_lag {
            row.push(dy[t - i]);
        }
        xrows.push(row);
        yvec.push(dy[t]);
    }
    let (stat, n_obs) = match ols_with_se(&xrows, &yvec) {
        Some((coefs, ses, _)) => {
            // coefs[1] is γ on y_{t-1}; t-stat = γ / se(γ)
            let gamma = coefs[1];
            let se = ses[1];
            let t = if se.abs() > 1e-18 { gamma / se } else { 0.0 };
            (t, yvec.len())
        }
        None => (0.0, 0),
    };
    let t = n_obs.max(1) as f64;
    let c1 = mackinnon_crit(1, t);
    let c5 = mackinnon_crit(5, t);
    let c10 = mackinnon_crit(10, t);
    AdfResult {
        statistic: stat,
        used_lag: max_lag,
        n_obs,
        crit_1pct: c1,
        crit_5pct: c5,
        crit_10pct: c10,
        p_value_approx: adf_pvalue(stat, c1, c5, c10),
        stationary_1pct: stat < c1,
        stationary_5pct: stat < c5,
        stationary_10pct: stat < c10,
    }
}

// ════════════════════════════════════════════════════════════════════════
//  Ornstein-Uhlenbeck: calibration + MFPT-optimal thresholds
// ════════════════════════════════════════════════════════════════════════

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OuParams {
    pub theta: f64,     // mean-reversion rate
    pub mu: f64,        // long-run mean
    pub sigma: f64,     // instantaneous volatility
    pub half_life: f64, // ln(2)/theta
    pub sigma_eq: f64,  // equilibrium std σ/√(2θ)
}

/// Calibrate an OU process dS = θ(μ−S)dt + σ dW from a discretely-sampled spread
/// via the exact AR(1) discretisation S_t = a + b·S_{t-1} + ε (Euler-Maruyama /
/// MLE-equivalent). `dt` is the sampling interval.
pub fn ou_calibrate(spread: &[f64], dt: f64) -> OuParams {
    let n = spread.len();
    if n < 3 {
        return OuParams {
            theta: 0.0,
            mu: spread.iter().sum::<f64>() / n.max(1) as f64,
            sigma: 0.0,
            half_life: f64::INFINITY,
            sigma_eq: 0.0,
        };
    }
    let x: Vec<Vec<f64>> = (0..n - 1).map(|i| vec![1.0, spread[i]]).collect();
    let y: Vec<f64> = (1..n).map(|i| spread[i]).collect();
    let (a, b, resid_var) = match ols_with_se(&x, &y) {
        Some((coefs, _, sigma2)) => (coefs[0], coefs[1].clamp(-0.999_999, 0.999_999), sigma2),
        None => (0.0, 0.0, 0.0),
    };
    let theta = if b > 0.0 { -b.ln() / dt } else { 1.0 / dt };
    let mu = if (1.0 - b).abs() > 1e-9 {
        a / (1.0 - b)
    } else {
        y.iter().sum::<f64>() / y.len() as f64
    };
    // σ from residual variance: Var(ε) = σ²(1−e^{-2θΔt})/(2θ)
    let denom = 1.0 - (-2.0 * theta * dt).exp();
    let sigma = if denom > 1e-12 {
        (resid_var * 2.0 * theta / denom).sqrt()
    } else {
        resid_var.sqrt()
    };
    let sigma_eq = if theta > 1e-12 {
        sigma / (2.0 * theta).sqrt()
    } else {
        sigma
    };
    OuParams {
        theta,
        mu,
        sigma,
        half_life: if theta > 1e-12 {
            std::f64::consts::LN_2 / theta
        } else {
            f64::INFINITY
        },
        sigma_eq,
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OuThresholds {
    pub entry_long: f64,  // enter long below this (μ − z·σ_eq)
    pub entry_short: f64, // enter short above this (μ + z·σ_eq)
    pub exit: f64,        // exit at the mean
    pub z: f64,           // optimal entry deviation in σ_eq units
    pub expected_return_per_unit_time: f64,
}

/// Expected first-passage time (in σ_eq units) of a normalised OU from deviation
/// `b` back to the mean (0), solved from the backward-Kolmogorov MFPT ODE
/// u'' − z·u' = −1/θ on [0, b] with u(0)=0 (exit at mean) and u'(b)=0 (reflecting
/// at entry), via a tridiagonal finite-difference solve.
fn ou_mfpt(theta: f64, b: f64, n: usize) -> f64 {
    if b <= 0.0 || theta <= 0.0 {
        return f64::INFINITY;
    }
    let h = b / n as f64;
    // unknowns u_1..u_n (u_0 = 0). Node i at z_i = i*h.
    // interior i=1..n-1: (u_{i+1}-2u_i+u_{i-1})/h² − z_i(u_{i+1}-u_{i-1})/(2h) = −1/θ
    // boundary i=n: reflecting u'(b)=0 → ghost u_{n+1}=u_{n-1}; gives
    //   (2u_{n-1}-2u_n)/h² = −1/θ
    let m = n; // unknown count (indices 1..=n)
    let mut lower = vec![0.0; m];
    let mut diag = vec![0.0; m];
    let mut upper = vec![0.0; m];
    let mut rhs = vec![0.0; m];
    let inv_h2 = 1.0 / (h * h);
    for i in 1..=n {
        let z = i as f64 * h;
        let row = i - 1;
        if i < n {
            let a_low = inv_h2 + z / (2.0 * h);
            let a_diag = -2.0 * inv_h2;
            let a_up = inv_h2 - z / (2.0 * h);
            lower[row] = a_low;
            diag[row] = a_diag;
            upper[row] = a_up;
            rhs[row] = -1.0 / theta;
            if i == 1 {
                // u_0 = 0 → drop lower contribution
                lower[row] = 0.0;
            }
        } else {
            // reflecting boundary at i=n
            lower[row] = 2.0 * inv_h2;
            diag[row] = -2.0 * inv_h2;
            upper[row] = 0.0;
            rhs[row] = -1.0 / theta;
        }
    }
    // Thomas algorithm
    for i in 1..m {
        let w = lower[i] / diag[i - 1];
        diag[i] -= w * upper[i - 1];
        rhs[i] -= w * rhs[i - 1];
    }
    let mut u = vec![0.0; m];
    u[m - 1] = rhs[m - 1] / diag[m - 1];
    for i in (0..m - 1).rev() {
        u[i] = (rhs[i] - upper[i] * u[i + 1]) / diag[i];
    }
    u[m - 1] // MFPT from entry (z=b) to mean
}

/// MFPT-optimal OU entry/exit band. Grid-searches the entry deviation z (in σ_eq
/// units) that maximises expected profit per unit time
/// J(z) = (z·σ_eq − cost) / MFPT(z), capturing the move from entry back to the mean.
pub fn ou_optimal_thresholds(params: &OuParams, cost: f64) -> OuThresholds {
    let s = params.sigma_eq;
    let theta = params.theta;
    let mut best_z = 1.0;
    let mut best_j = f64::NEG_INFINITY;
    if s > 1e-12 && theta > 1e-12 {
        let steps = 60;
        for i in 1..=steps {
            let z = 0.05 * i as f64; // up to 3.0 σ_eq
            let profit = z * s - cost;
            if profit <= 0.0 {
                continue;
            }
            let t = ou_mfpt(theta, z, 50);
            if !t.is_finite() || t <= 0.0 {
                continue;
            }
            let j = profit / t;
            if j > best_j {
                best_j = j;
                best_z = z;
            }
        }
    }
    OuThresholds {
        entry_long: params.mu - best_z * s,
        entry_short: params.mu + best_z * s,
        exit: params.mu,
        z: best_z,
        expected_return_per_unit_time: if best_j.is_finite() { best_j } else { 0.0 },
    }
}

// ════════════════════════════════════════════════════════════════════════
//  Markov transition matrix (cross-venue regime / lead-lag)
// ════════════════════════════════════════════════════════════════════════

/// Estimate an `n_states`×`n_states` row-stochastic transition matrix from a
/// sequence of integer states (Laplace-smoothed). Used for cross-venue lead-lag
/// ("does an imbalance shift on A predict a book clear on B?").
pub fn markov_transition_matrix(states: &[usize], n_states: usize) -> Vec<Vec<f64>> {
    if n_states == 0 {
        return vec![];
    }
    let mut counts = vec![vec![1.0_f64; n_states]; n_states]; // Laplace prior
    for w in states.windows(2) {
        let (a, b) = (w[0], w[1]);
        if a < n_states && b < n_states {
            counts[a][b] += 1.0;
        }
    }
    counts
        .into_iter()
        .map(|row| {
            let total: f64 = row.iter().sum();
            row.into_iter().map(|c| c / total).collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kalman_filter_1d_tracks_constant() {
        // noisy observations of a constant 5.0; filter should converge near 5
        let obs: Vec<f64> = (0..100)
            .map(|i| 5.0 + 0.01 * ((i % 5) as f64 - 2.0))
            .collect();
        let out = kalman_filter_1d(&obs, 1.0, 1e-5, 1.0, 1e-2, 0.0, 1.0);
        assert!((out.states.last().unwrap() - 5.0).abs() < 0.1);
    }

    #[test]
    fn test_kalman_beta_recovers_known_beta() {
        // r_asset = 1.5 * r_market + small noise; filter should land near 1.5
        let rm: Vec<f64> = (0..300).map(|i| ((i as f64 * 0.1).sin()) * 0.01).collect();
        let ra: Vec<f64> = rm
            .iter()
            .enumerate()
            .map(|(i, m)| 1.5 * m + 1e-5 * ((i % 3) as f64 - 1.0))
            .collect();
        let out = kalman_beta(&rm, &ra, 1e-6, 1e-4, 1.0, 1.0);
        assert!(
            (out.states.last().unwrap() - 1.5).abs() < 0.2,
            "beta={}",
            out.states.last().unwrap()
        );
    }

    #[test]
    fn test_kalman_volatility_positive_and_reasonable() {
        let rets: Vec<f64> = (0..250).map(|i| 0.01 * ((i as f64 * 0.3).sin())).collect();
        let vol = kalman_volatility(&rets, 0.1, 1.0, None, 1.0, 252.0);
        assert_eq!(vol.len(), 250);
        assert!(vol.iter().all(|v| *v >= 0.0 && v.is_finite()));
    }

    #[test]
    fn test_adf_stationary_vs_random_walk() {
        // Deterministic LCG noise so partial sums genuinely wander (a periodic
        // increment would make the "random walk" bounded ⇒ falsely stationary).
        let mut seed = 12_345_u64;
        let mut noise = || {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            ((seed >> 33) as f64 / (1u64 << 31) as f64) - 1.0
        };
        // stationary AR(1): x_t = 0.2 x_{t-1} + e
        let mut x = vec![0.0];
        for i in 1..400 {
            x.push(0.2 * x[i - 1] + noise());
        }
        // genuine random walk: y_t = y_{t-1} + e (unit root)
        let mut y = vec![0.0];
        for _ in 1..400 {
            y.push(y.last().unwrap() + noise());
        }
        let stat = adf_test(&x, 1);
        let rw = adf_test(&y, 1);
        // stationary series rejects the unit root (very negative ADF stat); the
        // random walk does not — so the stationary stat is more negative.
        assert!(
            stat.statistic < rw.statistic,
            "stat={} rw={}",
            stat.statistic,
            rw.statistic
        );
        assert!(
            stat.stationary_5pct,
            "AR(0.2) should be stationary: {}",
            stat.statistic
        );
        assert!(
            !rw.stationary_5pct,
            "random walk should NOT be stationary: {}",
            rw.statistic
        );
        // interpolated criticals are ordered 1% < 5% < 10% (more negative = stricter)
        assert!(stat.crit_1pct < stat.crit_5pct && stat.crit_5pct < stat.crit_10pct);
        // p-value is in [0,1] and the strongly-stationary series has the smaller p
        assert!((0.0..=1.0).contains(&stat.p_value_approx));
        assert!((0.0..=1.0).contains(&rw.p_value_approx));
        assert!(
            stat.p_value_approx < rw.p_value_approx,
            "stationary p {} should be < RW p {}",
            stat.p_value_approx,
            rw.p_value_approx
        );
    }

    #[test]
    fn test_ou_calibrate_recovers_reversion() {
        // simulate OU around mu=0.5 with strong reversion
        let mut s = vec![0.5];
        for i in 1..500 {
            let prev = s[i - 1];
            let drift = 0.3 * (0.5 - prev);
            s.push(prev + drift + 0.01 * ((i % 11) as f64 - 5.0));
        }
        let p = ou_calibrate(&s, 1.0);
        assert!(p.theta > 0.0, "theta={}", p.theta);
        assert!((p.mu - 0.5).abs() < 0.2, "mu={}", p.mu);
        assert!(p.half_life > 0.0 && p.half_life.is_finite());
        assert!(p.sigma_eq >= 0.0);
    }

    #[test]
    fn test_ou_optimal_thresholds_band_brackets_mean() {
        let p = OuParams {
            theta: 0.5,
            mu: 0.0,
            sigma: 0.1,
            half_life: 1.386,
            sigma_eq: 0.1,
        };
        let th = ou_optimal_thresholds(&p, 0.001);
        assert!(th.entry_long < th.exit && th.exit < th.entry_short);
        assert!(th.z > 0.0);
        assert!(th.expected_return_per_unit_time >= 0.0);
    }

    #[test]
    fn test_markov_transition_rows_sum_to_one() {
        let states = vec![0, 1, 1, 2, 0, 1, 2, 2, 0];
        let m = markov_transition_matrix(&states, 3);
        assert_eq!(m.len(), 3);
        for row in &m {
            let s: f64 = row.iter().sum();
            assert!((s - 1.0).abs() < 1e-9);
        }
    }
}
