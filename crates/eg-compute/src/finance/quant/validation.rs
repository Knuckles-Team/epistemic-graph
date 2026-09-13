use super::sf;

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

pub use eg_types::compute_result::finance::CvSplit;

/// Purged combinatorial CV splits with purge window + embargo (López de Prado).
pub fn purged_cpcv_splits(
    n_samples: usize,
    n_groups: usize,
    n_test_groups: usize,
    purge_window: usize,
    embargo: usize,
) -> Vec<CvSplit> {
    if n_groups == 0 || n_test_groups == 0 || n_test_groups > n_groups || n_samples == 0 {
        return vec![];
    }
    let group_size = n_samples / n_groups;
    let ranges: Vec<(usize, usize)> = (0..n_groups)
        .map(|i| {
            let lo = i * group_size;
            let hi = if i == n_groups - 1 {
                n_samples
            } else {
                (i + 1) * group_size
            };
            (lo, hi)
        })
        .collect();

    // combinations of n_test_groups out of n_groups
    let mut combos: Vec<Vec<usize>> = vec![];
    let mut combo = vec![0usize; n_test_groups];
    fn rec(
        start: usize,
        depth: usize,
        n_groups: usize,
        combo: &mut Vec<usize>,
        out: &mut Vec<Vec<usize>>,
    ) {
        if depth == combo.len() {
            out.push(combo.clone());
            return;
        }
        for g in start..n_groups {
            combo[depth] = g;
            rec(g + 1, depth + 1, n_groups, combo, out);
        }
    }
    rec(0, 0, n_groups, &mut combo, &mut combos);

    let mut splits = vec![];
    for test_combo in combos {
        let mut test_idx: Vec<usize> = vec![];
        let mut forbidden: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for &g in &test_combo {
            let (lo, hi) = ranges[g];
            for i in lo..hi {
                test_idx.push(i);
                forbidden.insert(i);
            }
            for i in lo.saturating_sub(purge_window)..lo {
                forbidden.insert(i);
            }
            for i in hi..(hi + embargo).min(n_samples) {
                forbidden.insert(i);
            }
        }
        let train_idx: Vec<usize> = (0..n_samples).filter(|i| !forbidden.contains(i)).collect();
        splits.push(CvSplit {
            train: train_idx,
            test: test_idx,
        });
    }
    splits
}

/// Deflated Sharpe Ratio (Bailey & López de Prado 2014). Returns the probability
/// the observed SR exceeds zero after correcting for trials count and non-normality.
pub fn deflated_sharpe_ratio(observed_sr: f64, n_trials: usize, sr_returns: &[f64]) -> f64 {
    let t = sr_returns.len();
    if t < 4 || n_trials < 1 {
        return 0.0;
    }
    let g3 = sf::skew(sr_returns);
    let g4 = sf::excess_kurtosis(sr_returns);
    let euler = 0.5772156649;
    let nt = n_trials as f64;
    let e_max_sr = (1.0 - euler) * sf::norm_ppf(1.0 - 1.0 / nt)
        + euler * sf::norm_ppf(1.0 - 1.0 / (nt * std::f64::consts::E));
    let sr_var =
        (1.0 - g3 * observed_sr + (g4 / 4.0) * observed_sr * observed_sr) / (t as f64 - 1.0);
    if sr_var <= 0.0 {
        return 0.0;
    }
    let z = (observed_sr - e_max_sr) / sr_var.sqrt();
    sf::norm_cdf(z)
}

/// Probability of Backtest Overfit (López de Prado). Rows = CV splits,
/// columns = strategies. Returns fraction of splits where the IS-best strategy
/// landed below the OOS median. PBO < 0.3 robust; > 0.5 pure overfit.
pub fn probability_of_backtest_overfit(insample: &[Vec<f64>], oos: &[Vec<f64>]) -> f64 {
    let n_splits = insample.len();
    if n_splits == 0 || oos.len() != n_splits {
        return 0.0;
    }
    let n_strat = insample[0].len();
    if n_strat == 0 {
        return 0.0;
    }
    let median_rank = (n_strat as f64 - 1.0) / 2.0;
    let mut below = 0.0;
    for s in 0..n_splits {
        if is_split_below_median_rank(&insample[s], &oos[s], n_strat, median_rank) {
            below += 1.0;
        }
    }
    below / n_splits as f64
}

/// Whether the OOS rank of one split's IS-best strategy falls below the
/// median rank. Split out of `probability_of_backtest_overfit`
/// (extract-method, cx/wD8) — same terms, same order as before.
fn is_split_below_median_rank(
    insample_row: &[f64],
    oos_row: &[f64],
    n_strat: usize,
    median_rank: f64,
) -> bool {
    // argmax IS
    let mut is_best = 0;
    for j in 1..n_strat {
        if insample_row[j] > insample_row[is_best] {
            is_best = j;
        }
    }
    // OOS rank (0 = worst) of is_best
    let mut rank = 0usize;
    for j in 0..n_strat {
        if oos_row[j] < oos_row[is_best] {
            rank += 1;
        }
    }
    (rank as f64) < median_rank
}

pub use eg_types::compute_result::finance::DieboldMariano;

/// Diebold-Mariano test of equal predictive accuracy (Newey-West HAC for h>1).
pub fn diebold_mariano(losses_a: &[f64], losses_b: &[f64], h: usize) -> DieboldMariano {
    let n = losses_a.len().min(losses_b.len());
    if n < 10 {
        return DieboldMariano {
            statistic: 0.0,
            p_value: 1.0,
            a_better: false,
        };
    }
    let d: Vec<f64> = (0..n).map(|i| losses_a[i] - losses_b[i]).collect();
    let d_mean = d.iter().sum::<f64>() / n as f64;
    let gamma = |k: usize| -> f64 {
        let mut s = 0.0;
        for i in k..n {
            s += (d[i] - d_mean) * (d[i - k] - d_mean);
        }
        s / n as f64
    };
    let mut lrv = gamma(0);
    if h > 1 {
        for k in 1..h {
            lrv += 2.0 * (1.0 - k as f64 / h as f64) * gamma(k);
        }
    }
    let d_var = lrv / n as f64;
    if d_var <= 0.0 {
        return DieboldMariano {
            statistic: 0.0,
            p_value: 1.0,
            a_better: d_mean < 0.0,
        };
    }
    let stat = d_mean / d_var.sqrt();
    let p_value = 2.0 * (1.0 - sf::norm_cdf(stat.abs()));
    DieboldMariano {
        statistic: stat,
        p_value,
        a_better: stat < 0.0,
    }
}
