use super::sf;
use eg_numeric::detkernel::kernels::{normal_cdf, normal_sf};

// ════════════════════════════════════════════════════════════════════════
//  Position sizing: Kelly & Bayesian Kelly
// ════════════════════════════════════════════════════════════════════════

/// Fractional Kelly for a YES contract priced at c with true-prob estimate q:
///   f* = (q − c) / (1 − c), scaled by `fraction` and floored at 0.
pub fn kelly_fraction(q: f64, c: f64, fraction: f64) -> f64 {
    if c >= q || c <= 0.0 || c >= 1.0 {
        return 0.0;
    }
    let f_star = (q - c) / (1.0 - c);
    (f_star * fraction).clamp(0.0, 1.0)
}

/// Bayesian Kelly under a Beta(α,β) posterior over the true probability.
/// Maximises E_q[U(f)] via Gauss-Legendre quadrature over q and a grid on f.
pub fn bayesian_kelly_fraction(alpha: f64, beta: f64, c: f64, n_quadrature: usize) -> f64 {
    if c <= 0.0 || c >= 1.0 {
        return 0.0;
    }
    let (nodes, weights) = sf::leggauss(n_quadrature.max(8));
    // map [-1,1] -> [0,1]
    let q_grid: Vec<f64> = nodes.iter().map(|&z| 0.5 * (z + 1.0)).collect();
    let q_w: Vec<f64> = weights
        .iter()
        .zip(&q_grid)
        .map(|(&w, &q)| 0.5 * w * sf::beta_pdf(q, alpha, beta))
        .collect();
    let b = (1.0 - c) / c;
    let neg_eu = |f: f64| -> f64 {
        if f <= 0.0 || f >= 1.0 {
            return 1e10;
        }
        let mut acc = 0.0;
        for (i, &q) in q_grid.iter().enumerate() {
            let u = (1.0 - q) * (1.0 - f).ln() + q * (1.0 + f * b).ln();
            acc += q_w[i] * u;
        }
        -acc
    };
    let mut best_f = 0.0;
    let mut best_v = f64::INFINITY;
    let steps = 200;
    for i in 1..steps {
        let f = i as f64 / steps as f64;
        let v = neg_eu(f);
        if v < best_v {
            best_v = v;
            best_f = f;
        }
    }
    best_f.max(0.0)
}

/// Equal-tailed credible interval for q ~ Beta(α,β); use the lower bound as a
/// conservative Kelly input.
pub fn posterior_credible_interval(alpha: f64, beta: f64, level: f64) -> (f64, f64) {
    (
        sf::beta_ppf(level / 2.0, alpha, beta),
        sf::beta_ppf(1.0 - level / 2.0, alpha, beta),
    )
}

// ════════════════════════════════════════════════════════════════════════
//  Backtest validation: purged CPCV, Deflated Sharpe, PBO, Diebold-Mariano
// ════════════════════════════════════════════════════════════════════════

/// The Deflated Sharpe Ratio and the Probability of Backtest Overfit are the
/// evaluation kernels' (EH-530); the finance Methods and `BacktestRun` call them here.
pub use eg_numeric::evaluation::backtest::{
    deflated_sharpe_ratio, probability_of_backtest_overfit,
};
pub use eg_types::compute_result::finance::{CvSplit, DieboldMariano};

use eg_numeric::evaluation::backtest as eval;

/// Purged combinatorial CV splits (López de Prado) — the evaluation kernel, as the
/// finance wire type.
pub fn purged_cpcv_splits(
    n_samples: usize,
    n_groups: usize,
    n_test_groups: usize,
    purge_window: usize,
    embargo: usize,
) -> Vec<CvSplit> {
    eval::purged_cpcv_splits(n_samples, n_groups, n_test_groups, purge_window, embargo)
        .into_iter()
        .map(|s| CvSplit {
            train: s.train,
            test: s.test,
        })
        .collect()
}

/// Diebold-Mariano test of equal predictive accuracy — the evaluation kernel, as the
/// finance wire type.
pub fn diebold_mariano(losses_a: &[f64], losses_b: &[f64], h: usize) -> DieboldMariano {
    let t = eval::diebold_mariano(losses_a, losses_b, h);
    DieboldMariano {
        statistic: t.statistic,
        p_value: t.p_value,
        a_better: t.a_better,
    }
}
