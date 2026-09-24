//! Backtest validation (EH-530, re-homed from the finance quant module so every policy
//! evaluation shares them): purged combinatorial cross-validation, the Deflated Sharpe
//! Ratio, the Probability of Backtest Overfit and the Diebold-Mariano test.

use std::collections::BTreeSet;

use super::special::{excess_kurtosis, norm_cdf, norm_ppf, skew};

/// One cross-validation split: training and test sample indices.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CvSplit {
    pub train: Vec<usize>,
    pub test: Vec<usize>,
}

/// Purged combinatorial CV splits with a purge window before and an embargo after
/// every test group (López de Prado). Empty for a degenerate request.
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
    let ranges = group_ranges(n_samples, n_groups);
    combinations(n_groups, n_test_groups)
        .into_iter()
        .map(|combo| {
            let chosen: Vec<(usize, usize)> = combo.iter().map(|&g| ranges[g]).collect();
            split_for(&chosen, n_samples, purge_window, embargo)
        })
        .collect()
}

/// `n_groups` contiguous index ranges; the last absorbs the remainder.
fn group_ranges(n_samples: usize, n_groups: usize) -> Vec<(usize, usize)> {
    let size = n_samples / n_groups;
    (0..n_groups)
        .map(|i| {
            let hi = if i == n_groups - 1 {
                n_samples
            } else {
                (i + 1) * size
            };
            (i * size, hi)
        })
        .collect()
}

/// Every `k`-subset of `0..n`, in lexicographic order.
fn combinations(n: usize, k: usize) -> Vec<Vec<usize>> {
    let mut out = Vec::new();
    let mut combo: Vec<usize> = (0..k).collect();
    loop {
        out.push(combo.clone());
        let Some(i) = (0..k).rev().find(|&i| combo[i] < n - k + i) else {
            return out;
        };
        combo[i] += 1;
        for j in i + 1..k {
            combo[j] = combo[j - 1] + 1;
        }
    }
}

/// The split whose test set is the chosen groups; training excludes them, the purge
/// window before each and the embargo after each.
fn split_for(chosen: &[(usize, usize)], n_samples: usize, purge: usize, embargo: usize) -> CvSplit {
    let mut test = Vec::new();
    let mut forbidden = BTreeSet::new();
    for &(lo, hi) in chosen {
        test.extend(lo..hi);
        forbidden.extend(lo.saturating_sub(purge)..(hi + embargo).min(n_samples));
    }
    let train = (0..n_samples).filter(|i| !forbidden.contains(i)).collect();
    CvSplit { train, test }
}

/// Deflated Sharpe Ratio (Bailey & López de Prado 2014): the probability the observed
/// Sharpe exceeds the expected maximum of `n_trials` null trials, corrected for the
/// returns' skew and kurtosis. `0` with under four returns or no trial.
pub fn deflated_sharpe_ratio(observed_sr: f64, n_trials: usize, sr_returns: &[f64]) -> f64 {
    let t = sr_returns.len();
    if t < 4 || n_trials < 1 {
        return 0.0;
    }
    let g3 = skew(sr_returns);
    let g4 = excess_kurtosis(sr_returns);
    let euler = 0.577_215_664_9;
    let nt = n_trials as f64;
    let e_max_sr = (1.0 - euler) * norm_ppf(1.0 - 1.0 / nt)
        + euler * norm_ppf(1.0 - 1.0 / (nt * std::f64::consts::E));
    let sr_var =
        (1.0 - g3 * observed_sr + (g4 / 4.0) * observed_sr * observed_sr) / (t as f64 - 1.0);
    if sr_var <= 0.0 {
        return 0.0;
    }
    norm_cdf((observed_sr - e_max_sr) / sr_var.sqrt())
}

/// Probability of Backtest Overfit (López de Prado): rows are CV splits, columns
/// strategies; the share of splits where the in-sample best lands below the
/// out-of-sample median rank. `0` for a degenerate input.
pub fn probability_of_backtest_overfit(insample: &[Vec<f64>], oos: &[Vec<f64>]) -> f64 {
    let n_splits = insample.len();
    if n_splits == 0 || oos.len() != n_splits || insample[0].is_empty() {
        return 0.0;
    }
    let median_rank = (insample[0].len() as f64 - 1.0) / 2.0;
    let below = insample
        .iter()
        .zip(oos)
        .filter(|(is_row, oos_row)| oos_rank_of_is_best(is_row, oos_row) < median_rank)
        .count();
    below as f64 / n_splits as f64
}

/// The out-of-sample rank (0 = worst) of the in-sample best strategy (first on ties).
fn oos_rank_of_is_best(insample_row: &[f64], oos_row: &[f64]) -> f64 {
    let best = (1..insample_row.len()).fold(0, |best, j| {
        if insample_row[j] > insample_row[best] {
            j
        } else {
            best
        }
    });
    oos_row.iter().filter(|&&v| v < oos_row[best]).count() as f64
}

/// A Diebold-Mariano test result.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DieboldMariano {
    pub statistic: f64,
    pub p_value: f64,
    /// Whether the first forecast's losses are lower.
    pub a_better: bool,
}

/// Diebold-Mariano test of equal predictive accuracy of two loss series, with the
/// Newey-West long-run variance for horizon `h > 1`. Inconclusive (statistic 0,
/// p-value 1) under ten pairs or for a zero variance.
pub fn diebold_mariano(losses_a: &[f64], losses_b: &[f64], h: usize) -> DieboldMariano {
    let n = losses_a.len().min(losses_b.len());
    let inconclusive = |a_better| DieboldMariano {
        statistic: 0.0,
        p_value: 1.0,
        a_better,
    };
    if n < 10 {
        return inconclusive(false);
    }
    let d: Vec<f64> = (0..n).map(|i| losses_a[i] - losses_b[i]).collect();
    let d_mean = d.iter().sum::<f64>() / n as f64;
    let d_var = long_run_variance(&d, d_mean, h) / n as f64;
    if d_var <= 0.0 {
        return inconclusive(d_mean < 0.0);
    }
    let statistic = d_mean / d_var.sqrt();
    DieboldMariano {
        statistic,
        p_value: 2.0 * (1.0 - norm_cdf(statistic.abs())),
        a_better: statistic < 0.0,
    }
}

/// Newey-West (Bartlett) long-run variance of `d` up to lag `h − 1`.
fn long_run_variance(d: &[f64], mean: f64, h: usize) -> f64 {
    let n = d.len();
    let gamma = |k: usize| -> f64 {
        (k..n)
            .map(|i| (d[i] - mean) * (d[i - k] - mean))
            .sum::<f64>()
            / n as f64
    };
    let tail: f64 = (1..h.max(1))
        .map(|k| 2.0 * (1.0 - k as f64 / h as f64) * gamma(k))
        .sum();
    gamma(0) + tail
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpcv_purges_and_embargoes_around_every_test_group() {
        let splits = purged_cpcv_splits(120, 6, 2, 5, 5);
        assert_eq!(splits.len(), 15, "C(6, 2) splits");
        let first = &splits[0];
        assert_eq!(first.test, (0..40).collect::<Vec<_>>());
        assert!(!first.train.contains(&44) && first.train.contains(&45));
        assert!(purged_cpcv_splits(10, 0, 1, 0, 0).is_empty());
    }

    #[test]
    fn dsr_pbo_and_dm_behave_at_the_edges() {
        let rets: Vec<f64> = (0..200).map(|i| 0.001 + f64::from(i % 7) * 1e-4).collect();
        let dsr = deflated_sharpe_ratio(1.5, 10, &rets);
        assert!((0.0..=1.0).contains(&dsr));
        assert!(deflated_sharpe_ratio(1.5, 100, &rets) < deflated_sharpe_ratio(1.5, 2, &rets));
        let is = vec![vec![1.0, 2.0, 3.0]; 4];
        assert_eq!(
            probability_of_backtest_overfit(&is, &vec![vec![3.0, 2.0, 1.0]; 4]),
            1.0
        );
        assert_eq!(probability_of_backtest_overfit(&is, &is), 0.0);
        let a: Vec<f64> = (0..50).map(|i| f64::from(i % 3) * 0.1).collect();
        let b: Vec<f64> = a.iter().map(|x| x + 0.5).collect();
        assert!(diebold_mariano(&a, &b, 1).a_better);
        assert_eq!(diebold_mariano(&a[..5], &b[..5], 1).p_value, 1.0);
    }
}
