// CONCEPT:KG-2.20a — Mean-Variance Optimization Engine
//
// Full MVO implementation with multiple optimization strategies:
// - Maximum Sharpe ratio
// - Minimum variance
// - Risk parity
// - Black-Litterman
// - Efficient frontier sampling

use nalgebra::{DMatrix, DVector};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OptimizationResult {
    pub weights: Vec<f64>,
    pub expected_return: f64,
    pub expected_volatility: f64,
    pub sharpe_ratio: f64,
    pub method: String,
}

/// Compute portfolio return given weights and expected returns.
fn portfolio_return(weights: &[f64], returns: &[f64]) -> f64 {
    weights.iter().zip(returns.iter()).map(|(w, r)| w * r).sum()
}

/// Compute portfolio variance given weights and covariance matrix.
fn portfolio_variance(weights: &[f64], cov: &[Vec<f64>]) -> f64 {
    let n = weights.len();
    let mut var = 0.0;
    for i in 0..n {
        for j in 0..n {
            var += weights[i] * weights[j] * cov[i][j];
        }
    }
    var
}

/// Project weights onto the simplex (sum=1, all >= 0).
fn project_simplex(v: &[f64]) -> Vec<f64> {
    let n = v.len();
    let mut u: Vec<f64> = v.to_vec();
    u.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));

    let mut cumsum = 0.0;
    let mut rho = 0;
    for i in 0..n {
        cumsum += u[i];
        if u[i] - (cumsum - 1.0) / ((i + 1) as f64) > 0.0 {
            rho = i + 1;
        }
    }
    let theta = (u[..rho].iter().sum::<f64>() - 1.0) / (rho as f64);
    v.iter().map(|&vi| (vi - theta).max(0.0)).collect()
}

/// Project weights onto the simplex with box constraints using a heuristic.
fn project_simplex_with_bounds(v: &[f64], min_w: f64, max_w: f64) -> Vec<f64> {
    let n = v.len();
    let mut u = v.to_vec();
    for _ in 0..100 {
        let mut sum = 0.0;
        for &w in &u {
            sum += w;
        }
        let diff = (sum - 1.0) / (n as f64);
        for w in &mut u {
            *w = (*w - diff).clamp(min_w, max_w);
        }
    }
    let sum: f64 = u.iter().sum();
    if sum > 0.0 {
        for w in &mut u {
            *w /= sum;
        }
    }
    u
}

/// Mean-variance optimization using projected gradient descent.
///
/// Maximizes Sharpe ratio: (w'μ - rf) / sqrt(w'Σw)
pub fn mean_variance_optimization(
    expected_returns: &[f64],
    cov_matrix: &[Vec<f64>],
    risk_free_rate: f64,
    min_weight: Option<f64>,
    max_weight: Option<f64>,
) -> OptimizationResult {
    let n = expected_returns.len();
    if n == 0 {
        return OptimizationResult {
            weights: vec![],
            expected_return: 0.0,
            expected_volatility: 0.0,
            sharpe_ratio: 0.0,
            method: "mvo".to_string(),
        };
    }

    // Initialize with equal weights
    let mut weights: Vec<f64> = vec![1.0 / n as f64; n];
    let learning_rate = 0.01;
    let iterations = 500;

    let min_w = min_weight.unwrap_or(0.0);
    let max_w = max_weight.unwrap_or(1.0);

    for _ in 0..iterations {
        let port_ret = portfolio_return(&weights, expected_returns);
        let port_var = portfolio_variance(&weights, cov_matrix);
        let port_vol = port_var.sqrt().max(1e-12);
        let excess = port_ret - risk_free_rate;

        // Gradient of negative Sharpe ratio
        let mut grad = vec![0.0; n];
        for i in 0..n {
            let d_ret = expected_returns[i];
            let mut d_var = 0.0;
            for j in 0..n {
                d_var += 2.0 * weights[j] * cov_matrix[i][j];
            }
            // d(Sharpe)/dw_i = (μ_i * vol - excess * dvar/(2*vol)) / vol^2
            grad[i] =
                -(d_ret * port_vol - excess * d_var / (2.0 * port_vol)) / (port_vol * port_vol);
        }

        // Gradient step
        for i in 0..n {
            weights[i] -= learning_rate * grad[i];
        }
        // Project back onto simplex with bounds
        weights = project_simplex_with_bounds(&weights, min_w, max_w);
    }

    let port_ret = portfolio_return(&weights, expected_returns);
    let port_var = portfolio_variance(&weights, cov_matrix);
    let port_vol = port_var.sqrt();
    let sharpe = if port_vol > 0.0 {
        (port_ret - risk_free_rate) / port_vol
    } else {
        0.0
    };

    OptimizationResult {
        weights,
        expected_return: port_ret,
        expected_volatility: port_vol,
        sharpe_ratio: sharpe,
        method: "mvo_gradient_descent".to_string(),
    }
}

/// Minimum variance portfolio — ignores expected returns entirely.
pub fn minimum_variance(cov_matrix: &[Vec<f64>]) -> OptimizationResult {
    let n = cov_matrix.len();
    if n == 0 {
        return OptimizationResult {
            weights: vec![],
            expected_return: 0.0,
            expected_volatility: 0.0,
            sharpe_ratio: 0.0,
            method: "min_variance".to_string(),
        };
    }

    let mut weights = vec![1.0 / n as f64; n];
    let learning_rate = 0.01;

    for _ in 0..500 {
        let mut grad = vec![0.0; n];
        for i in 0..n {
            for j in 0..n {
                grad[i] += 2.0 * weights[j] * cov_matrix[i][j];
            }
        }
        for i in 0..n {
            weights[i] -= learning_rate * grad[i];
        }
        weights = project_simplex(&weights);
    }

    let port_var = portfolio_variance(&weights, cov_matrix);
    OptimizationResult {
        weights,
        expected_return: 0.0,
        expected_volatility: port_var.sqrt(),
        sharpe_ratio: 0.0,
        method: "min_variance".to_string(),
    }
}

/// Risk parity — each asset contributes equally to portfolio risk.
pub fn risk_parity(cov_matrix: &[Vec<f64>]) -> OptimizationResult {
    let n = cov_matrix.len();
    if n == 0 {
        return OptimizationResult {
            weights: vec![],
            expected_return: 0.0,
            expected_volatility: 0.0,
            sharpe_ratio: 0.0,
            method: "risk_parity".to_string(),
        };
    }

    // Initialize with inverse volatility
    let mut weights: Vec<f64> = (0..n)
        .map(|i| {
            let vol = cov_matrix[i][i].sqrt().max(1e-12);
            1.0 / vol
        })
        .collect();
    let sum: f64 = weights.iter().sum();
    for w in &mut weights {
        *w /= sum;
    }

    let learning_rate = 0.001;
    for _ in 0..1000 {
        let port_var = portfolio_variance(&weights, cov_matrix);
        let port_vol = port_var.sqrt().max(1e-12);
        let target_rc = port_vol / n as f64;

        let mut grad = vec![0.0; n];
        for i in 0..n {
            let mut marginal_risk = 0.0;
            for j in 0..n {
                marginal_risk += weights[j] * cov_matrix[i][j];
            }
            let risk_contribution = weights[i] * marginal_risk / port_vol;
            grad[i] = risk_contribution - target_rc;
        }

        for i in 0..n {
            weights[i] -= learning_rate * grad[i];
            weights[i] = weights[i].max(1e-8);
        }
        let sum: f64 = weights.iter().sum();
        for w in &mut weights {
            *w /= sum;
        }
    }

    let port_var = portfolio_variance(&weights, cov_matrix);
    OptimizationResult {
        weights,
        expected_return: 0.0,
        expected_volatility: port_var.sqrt(),
        sharpe_ratio: 0.0,
        method: "risk_parity".to_string(),
    }
}

/// Efficient frontier — sample N portfolios along the risk-return frontier.
pub fn efficient_frontier(
    expected_returns: &[f64],
    cov_matrix: &[Vec<f64>],
    risk_free_rate: f64,
    n_points: usize,
) -> Vec<OptimizationResult> {
    let n = expected_returns.len();
    if n == 0 || n_points == 0 {
        return vec![];
    }

    let min_ret = expected_returns
        .iter()
        .cloned()
        .fold(f64::INFINITY, f64::min);
    let max_ret = expected_returns
        .iter()
        .cloned()
        .fold(f64::NEG_INFINITY, f64::max);
    let step = if n_points > 1 {
        (max_ret - min_ret) / (n_points as f64 - 1.0)
    } else {
        0.0
    };

    let mut frontier = Vec::with_capacity(n_points);
    for k in 0..n_points {
        let target_return = min_ret + step * k as f64;

        // Minimize variance subject to target return via penalized gradient descent
        let mut weights = vec![1.0 / n as f64; n];
        let lr = 0.005;
        let penalty = 100.0;

        for _ in 0..300 {
            let port_ret = portfolio_return(&weights, expected_returns);
            let mut grad = vec![0.0; n];
            for i in 0..n {
                for j in 0..n {
                    grad[i] += 2.0 * weights[j] * cov_matrix[i][j];
                }
                // Penalty for deviating from target return
                grad[i] += penalty * 2.0 * (port_ret - target_return) * expected_returns[i];
            }
            for i in 0..n {
                weights[i] -= lr * grad[i];
            }
            weights = project_simplex(&weights);
        }

        let port_ret = portfolio_return(&weights, expected_returns);
        let port_var = portfolio_variance(&weights, cov_matrix);
        let port_vol = port_var.sqrt();
        let sharpe = if port_vol > 0.0 {
            (port_ret - risk_free_rate) / port_vol
        } else {
            0.0
        };

        frontier.push(OptimizationResult {
            weights,
            expected_return: port_ret,
            expected_volatility: port_vol,
            sharpe_ratio: sharpe,
            method: format!("efficient_frontier_point_{}", k),
        });
    }

    frontier
}

/// Invert a matrix, falling back to a tiny ridge regulariser if it is singular.
fn invert_with_ridge(m: &DMatrix<f64>) -> DMatrix<f64> {
    if let Some(inv) = m.clone().try_inverse() {
        return inv;
    }
    let n = m.nrows();
    let ridged = m + DMatrix::<f64>::identity(n, n) * 1e-8;
    ridged
        .try_inverse()
        .unwrap_or_else(|| DMatrix::<f64>::identity(n, n))
}

/// Black-Litterman optimization incorporating market equilibrium and investor views.
///
/// 1. Implied equilibrium excess returns: Π = δ Σ w_mkt
/// 2. Posterior returns blending the prior with k views (P, Q):
///    E[R] = [(τΣ)⁻¹ + Pᵀ Ω⁻¹ P]⁻¹ [(τΣ)⁻¹ Π + Pᵀ Ω⁻¹ Q]
///    with He–Litterman view uncertainty Ω = diag(P (τΣ) Pᵀ).
/// 3. Mean-variance optimize on the posterior to get the final weights.
pub fn black_litterman(
    market_weights: &[f64],
    cov_matrix: &[Vec<f64>],
    views: &[f64],
    pick_matrix: &[Vec<f64>],
    tau: f64,
    risk_aversion: f64,
) -> OptimizationResult {
    let n = market_weights.len();
    if n == 0 {
        return OptimizationResult {
            weights: vec![],
            expected_return: 0.0,
            expected_volatility: 0.0,
            sharpe_ratio: 0.0,
            method: "black_litterman".to_string(),
        };
    }

    let sigma = DMatrix::from_fn(n, n, |i, j| cov_matrix[i][j]);
    let w_mkt = DVector::from_fn(n, |i, _| market_weights[i]);

    // Implied equilibrium excess returns: Π = δ Σ w_mkt
    let pi = (&sigma * &w_mkt) * risk_aversion;

    let tau_sigma = &sigma * tau;
    let tau_sigma_inv = invert_with_ridge(&tau_sigma);

    let k = views.len();
    let posterior_returns: DVector<f64> = if k == 0 || pick_matrix.is_empty() {
        // No views — posterior collapses to the equilibrium prior.
        pi.clone()
    } else {
        let p = DMatrix::from_fn(k, n, |i, j| pick_matrix[i][j]);
        let q = DVector::from_fn(k, |i, _| views[i]);

        // Ω = diag(P (τΣ) Pᵀ): each view's uncertainty scales with its variance.
        let p_tausigma_pt = &p * &tau_sigma * p.transpose();
        let mut omega = DMatrix::<f64>::zeros(k, k);
        for i in 0..k {
            omega[(i, i)] = p_tausigma_pt[(i, i)].abs().max(1e-8);
        }
        let omega_inv = invert_with_ridge(&omega);

        let a = &tau_sigma_inv + p.transpose() * &omega_inv * &p;
        let a_inv = invert_with_ridge(&a);
        let b = &tau_sigma_inv * &pi + p.transpose() * &omega_inv * &q;
        a_inv * b
    };

    let posterior_vec: Vec<f64> = posterior_returns.iter().copied().collect();

    // Mean-variance optimize on the posterior returns (long-only simplex).
    let mut result = mean_variance_optimization(&posterior_vec, cov_matrix, 0.0, None, None);
    result.method = "black_litterman".to_string();
    result
}

/// Minimum variance for a specific target return
pub fn efficient_frontier_target(
    expected_returns: &[f64],
    cov_matrix: &[Vec<f64>],
    target_return: f64,
) -> OptimizationResult {
    let n = expected_returns.len();
    if n == 0 {
        return OptimizationResult {
            weights: vec![],
            expected_return: 0.0,
            expected_volatility: 0.0,
            sharpe_ratio: 0.0,
            method: "efficient_frontier_target".to_string(),
        };
    }

    let mut weights = vec![1.0 / n as f64; n];
    let lr = 0.005;
    let penalty = 100.0;

    for _ in 0..500 {
        let port_ret = portfolio_return(&weights, expected_returns);
        let mut grad = vec![0.0; n];
        for i in 0..n {
            for j in 0..n {
                grad[i] += 2.0 * weights[j] * cov_matrix[i][j];
            }
            grad[i] += penalty * 2.0 * (port_ret - target_return) * expected_returns[i];
        }
        for i in 0..n {
            weights[i] -= lr * grad[i];
        }
        weights = project_simplex(&weights);
    }

    let port_ret = portfolio_return(&weights, expected_returns);
    let port_var = portfolio_variance(&weights, cov_matrix);

    OptimizationResult {
        weights,
        expected_return: port_ret,
        expected_volatility: port_var.sqrt(),
        sharpe_ratio: 0.0,
        method: "efficient_frontier_target".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mvo_basic() {
        let returns = vec![0.10, 0.15, 0.12];
        let cov = vec![
            vec![0.04, 0.006, 0.010],
            vec![0.006, 0.09, 0.015],
            vec![0.010, 0.015, 0.0625],
        ];
        let result = mean_variance_optimization(&returns, &cov, 0.02, None, None);
        assert_eq!(result.weights.len(), 3);
        let sum: f64 = result.weights.iter().sum();
        assert!(
            (sum - 1.0).abs() < 0.01,
            "Weights should sum to 1.0, got {}",
            sum
        );
        assert!(result.sharpe_ratio > 0.0);
    }

    #[test]
    fn test_min_variance() {
        let cov = vec![vec![0.04, 0.006], vec![0.006, 0.09]];
        let result = minimum_variance(&cov);
        assert_eq!(result.weights.len(), 2);
        // Lower-vol asset should have higher weight
        assert!(result.weights[0] > result.weights[1]);
    }

    #[test]
    fn test_risk_parity() {
        let cov = vec![vec![0.04, 0.0], vec![0.0, 0.09]];
        let result = risk_parity(&cov);
        assert_eq!(result.weights.len(), 2);
        let sum: f64 = result.weights.iter().sum();
        assert!((sum - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_efficient_frontier() {
        let returns = vec![0.08, 0.12];
        let cov = vec![vec![0.04, 0.01], vec![0.01, 0.09]];
        let frontier = efficient_frontier(&returns, &cov, 0.02, 5);
        assert_eq!(frontier.len(), 5);
    }

    #[test]
    fn test_empty() {
        let result = mean_variance_optimization(&[], &[], 0.02, None, None);
        assert!(result.weights.is_empty());
    }

    #[test]
    fn test_black_litterman_consumes_views() {
        // Two uncorrelated assets, equal market weights. A bullish view on
        // asset 0 must tilt weight toward asset 0 vs. a bullish view on asset 1.
        // (The old stub ignored views entirely, so both would be identical.)
        let w_mkt = vec![0.5, 0.5];
        let cov = vec![vec![0.04, 0.0], vec![0.0, 0.04]];
        let p = vec![vec![1.0, 0.0]]; // view targets asset 0
        let q = vec![0.10]; // expect +10% on the picked asset

        let bull_0 = black_litterman(&w_mkt, &cov, &q, &p, 0.05, 2.5);

        let p2 = vec![vec![0.0, 1.0]]; // same view magnitude, asset 1
        let bull_1 = black_litterman(&w_mkt, &cov, &q, &p2, 0.05, 2.5);

        assert_eq!(bull_0.weights.len(), 2);
        assert!(
            bull_0.weights[0] > bull_1.weights[0] + 1e-6,
            "view on asset 0 should raise its weight: {:?} vs {:?}",
            bull_0.weights,
            bull_1.weights
        );
        let sum: f64 = bull_0.weights.iter().sum();
        assert!((sum - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_black_litterman_no_views_is_equilibrium() {
        // With no views the posterior is the equilibrium prior; result is a
        // valid long-only portfolio (sums to 1, no NaNs).
        let w_mkt = vec![0.6, 0.4];
        let cov = vec![vec![0.04, 0.01], vec![0.01, 0.09]];
        let result = black_litterman(&w_mkt, &cov, &[], &[], 0.05, 2.5);
        assert_eq!(result.weights.len(), 2);
        let sum: f64 = result.weights.iter().sum();
        assert!((sum - 1.0).abs() < 0.01);
        assert!(result.weights.iter().all(|w| w.is_finite()));
    }
}
