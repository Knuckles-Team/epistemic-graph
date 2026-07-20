// CONCEPT:EG-KG.compute.risk-analytics — Risk Analytics Engine
//
// Value-at-Risk, CVaR/Expected Shortfall, stress testing, and drawdown computation.
// Replaces Python scipy.stats-based risk calculations.

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RiskMetrics {
    pub var_95: f64,
    pub var_99: f64,
    pub cvar_95: f64,
    pub cvar_99: f64,
    pub max_drawdown: f64,
    pub volatility: f64,
    pub downside_deviation: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,
}

/// Compute parametric VaR assuming normal distribution.
///
/// VaR(α) = μ - z_α * σ
fn parametric_var(mean: f64, std: f64, confidence: f64) -> f64 {
    // Approximate inverse normal CDF (Beasley-Springer-Moro algorithm)
    let z = inv_norm(confidence);
    -(mean - z * std)
}

/// Approximate inverse normal CDF using rational approximation.
fn inv_norm(p: f64) -> f64 {
    // Abramowitz & Stegun approximation 26.2.23
    let p = if p > 0.5 { 1.0 - p } else { p };
    let t = (-2.0 * p.ln()).sqrt();

    let c0 = 2.515517;
    let c1 = 0.802853;
    let c2 = 0.010328;
    let d1 = 1.432788;
    let d2 = 0.189269;
    let d3 = 0.001308;

    t - (c0 + c1 * t + c2 * t * t) / (1.0 + d1 * t + d2 * t * t + d3 * t * t * t)
}

/// Historical VaR — sort returns and pick the percentile.
pub fn historical_var(returns: &[f64], confidence: f64) -> f64 {
    if returns.is_empty() {
        return 0.0;
    }
    let mut sorted = returns.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((1.0 - confidence) * sorted.len() as f64).floor() as usize;
    let idx = idx.min(sorted.len() - 1);
    -sorted[idx]
}

/// Historical CVaR — average of returns beyond VaR threshold.
pub fn historical_cvar(returns: &[f64], confidence: f64) -> f64 {
    if returns.is_empty() {
        return 0.0;
    }
    let mut sorted = returns.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let cutoff = ((1.0 - confidence) * sorted.len() as f64).floor() as usize;
    let cutoff = cutoff.max(1).min(sorted.len());
    let tail: f64 = sorted[..cutoff].iter().sum();
    -(tail / cutoff as f64)
}

/// Compute maximum drawdown from a returns series.
pub fn max_drawdown(returns: &[f64]) -> f64 {
    if returns.is_empty() {
        return 0.0;
    }
    let mut peak = 1.0;
    let mut cumulative = 1.0;
    let mut max_dd = 0.0;

    for &r in returns {
        cumulative *= 1.0 + r;
        if cumulative > peak {
            peak = cumulative;
        }
        let dd = (peak - cumulative) / peak;
        if dd > max_dd {
            max_dd = dd;
        }
    }
    max_dd
}

/// Compute drawdown series (running drawdown at each time step).
pub fn drawdown_series(returns: &[f64]) -> Vec<f64> {
    let mut peak = 1.0;
    let mut cumulative = 1.0;
    let mut dd_series = Vec::with_capacity(returns.len());

    for &r in returns {
        cumulative *= 1.0 + r;
        if cumulative > peak {
            peak = cumulative;
        }
        dd_series.push((peak - cumulative) / peak);
    }
    dd_series
}

/// Downside deviation — std of returns below target.
pub fn downside_deviation(returns: &[f64], target: f64) -> f64 {
    if returns.is_empty() {
        return 0.0;
    }
    let below: Vec<f64> = returns
        .iter()
        .filter(|&&r| r < target)
        .map(|&r| (r - target).powi(2))
        .collect();
    if below.is_empty() {
        return 0.0;
    }
    (below.iter().sum::<f64>() / below.len() as f64).sqrt()
}

/// Compute comprehensive risk metrics from a returns series.
pub fn compute_risk_metrics(returns: &[f64], risk_free_rate: f64) -> RiskMetrics {
    if returns.is_empty() {
        return RiskMetrics {
            var_95: 0.0,
            var_99: 0.0,
            cvar_95: 0.0,
            cvar_99: 0.0,
            max_drawdown: 0.0,
            volatility: 0.0,
            downside_deviation: 0.0,
            sortino_ratio: 0.0,
            calmar_ratio: 0.0,
        };
    }

    let n = returns.len() as f64;
    let mean: f64 = returns.iter().sum::<f64>() / n;
    let variance: f64 =
        returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0).max(1.0);
    let vol = variance.sqrt();
    let dd = max_drawdown(returns);
    let dsd = downside_deviation(returns, risk_free_rate / 252.0);

    let annualized_return = mean * 252.0;
    let annualized_vol = vol * (252.0_f64).sqrt();
    let excess = annualized_return - risk_free_rate;

    let sortino = if dsd > 0.0 {
        excess / (dsd * (252.0_f64).sqrt())
    } else {
        0.0
    };

    let calmar = if dd > 0.0 {
        annualized_return / dd
    } else {
        0.0
    };

    RiskMetrics {
        var_95: historical_var(returns, 0.95),
        var_99: historical_var(returns, 0.99),
        cvar_95: historical_cvar(returns, 0.95),
        cvar_99: historical_cvar(returns, 0.99),
        max_drawdown: dd,
        volatility: annualized_vol,
        downside_deviation: dsd,
        sortino_ratio: sortino,
        calmar_ratio: calmar,
    }
}

/// Monte Carlo VaR — simulate returns from mean/std and compute VaR.
pub fn monte_carlo_var(mean: f64, std: f64, n_simulations: usize, confidence: f64) -> f64 {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut simulated: Vec<f64> = (0..n_simulations)
        .map(|_| {
            // Box-Muller transform for normal distribution
            let u1: f64 = rng.gen();
            let u2: f64 = rng.gen();
            let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
            mean + std * z
        })
        .collect();
    simulated.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((1.0 - confidence) * n_simulations as f64).floor() as usize;
    let idx = idx.min(n_simulations - 1);
    -simulated[idx]
}

/// Stress test — apply a vector of shock factors to portfolio weights and covariance.
pub fn stress_test(
    weights: &[f64],
    expected_returns: &[f64],
    cov_matrix: &[Vec<f64>],
    shock_factors: &[f64],
) -> Vec<f64> {
    // Apply shocks to expected returns and compute stressed portfolio metrics
    let n = weights.len();
    shock_factors
        .iter()
        .map(|&shock| {
            let stressed_returns: Vec<f64> = expected_returns
                .iter()
                .map(|&r| r * (1.0 + shock))
                .collect();
            let mut port_ret = 0.0;
            for i in 0..n {
                port_ret += weights[i] * stressed_returns[i];
            }
            let mut port_var = 0.0;
            for i in 0..n {
                for j in 0..n {
                    // Stressed covariance scales quadratically with shock
                    port_var += weights[i] * weights[j] * cov_matrix[i][j] * (1.0 + shock.abs());
                }
            }
            port_ret - port_var.sqrt() * 1.65 // 95% stress loss
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_historical_var() {
        let returns = vec![
            -0.05, -0.03, -0.01, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07,
        ];
        let var95 = historical_var(&returns, 0.95);
        assert!(var95 > 0.0, "VaR should be positive for this sample");
    }

    #[test]
    fn test_max_drawdown() {
        let returns = vec![0.1, 0.05, -0.2, -0.1, 0.15, 0.1];
        let dd = max_drawdown(&returns);
        assert!(dd > 0.0 && dd < 1.0);
    }

    #[test]
    fn test_risk_metrics() {
        let returns = vec![
            0.01, -0.02, 0.015, -0.005, 0.02, -0.01, 0.005, 0.01, -0.015, 0.02,
        ];
        let metrics = compute_risk_metrics(&returns, 0.02);
        assert!(metrics.volatility > 0.0);
        assert!(metrics.max_drawdown >= 0.0);
    }

    #[test]
    fn test_monte_carlo_var() {
        let var = monte_carlo_var(0.001, 0.02, 10000, 0.95);
        assert!(var > 0.0);
    }

    #[test]
    fn test_empty_returns() {
        let metrics = compute_risk_metrics(&[], 0.02);
        assert_eq!(metrics.volatility, 0.0);
    }
}
