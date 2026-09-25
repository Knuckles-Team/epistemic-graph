// CONCEPT:EG-KG.domains.state-space-statistical-arbitrage — State-Space & Statistical-Arbitrage Kernels
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

// ════════════════════════════════════════════════════════════════════════
//  Kalman filters
// ════════════════════════════════════════════════════════════════════════

pub use eg_types::compute_result::finance::KalmanState;
// The scalar filter is the series kernel's (EH-530): one recurrence for the finance
// Methods and UQL `DERIVE kalman(…)` / `kbeta(…)`.
use eg_numeric::series::kalman::Kalman;
use eg_numeric::series::KalmanNoise;

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
    let mut filter = Kalman::new(f, KalmanNoise { q, r }, x0, p0);
    let (states, variances) = observations.iter().map(|&z| filter.observe(z, h)).unzip();
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
    // Random-walk β (F = 1) observed through the time-varying H = the market return.
    let mut filter = Kalman::new(1.0, KalmanNoise { q, r }, beta0, p0);
    let (states, variances) = market_returns
        .iter()
        .zip(asset_returns)
        .map(|(&h, &z)| filter.observe(z, h))
        .unzip();
    KalmanState { states, variances }
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

pub use eg_types::compute_result::finance::AdfResult;

/// Run the generic ADF series kernel, preserving the finance wire result.
pub fn adf_test(series: &[f64], max_lag: usize) -> AdfResult {
    let estimate = eg_numeric::series::adf::test(series, max_lag);
    AdfResult {
        statistic: estimate.statistic,
        used_lag: estimate.used_lag,
        n_obs: estimate.n_obs,
        crit_1pct: estimate.crit_1pct,
        crit_5pct: estimate.crit_5pct,
        crit_10pct: estimate.crit_10pct,
        p_value_approx: estimate.p_value_approx,
        stationary_1pct: estimate.stationary_1pct,
        stationary_5pct: estimate.stationary_5pct,
        stationary_10pct: estimate.stationary_10pct,
    }
}

// ════════════════════════════════════════════════════════════════════════
//  Ornstein-Uhlenbeck: calibration + MFPT-optimal thresholds
// ════════════════════════════════════════════════════════════════════════

pub use eg_types::compute_result::finance::OuParams;

/// Calibrate the generic OU series model, preserving the finance wire result.
pub fn ou_calibrate(spread: &[f64], dt: f64) -> OuParams {
    let estimate = eg_numeric::series::ou::calibrate(spread, dt);
    OuParams {
        theta: estimate.theta,
        mu: estimate.mu,
        sigma: estimate.sigma,
        half_life: estimate.half_life,
        sigma_eq: estimate.sigma_eq,
    }
}

pub use eg_types::compute_result::finance::OuThresholds;

/// Compute first-passage bands from the shared OU series model.
pub fn ou_optimal_thresholds(params: &OuParams, cost: f64) -> OuThresholds {
    let estimate = eg_numeric::series::ou::OuEstimate {
        theta: params.theta,
        mu: params.mu,
        sigma: params.sigma,
        half_life: params.half_life,
        sigma_eq: params.sigma_eq,
    };
    let bands = eg_numeric::series::ou::optimal_thresholds(&estimate, cost);
    OuThresholds {
        entry_long: bands.entry_long,
        entry_short: bands.entry_short,
        exit: bands.exit,
        z: bands.z,
        expected_return_per_unit_time: bands.expected_return_per_unit_time,
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
