//! Backtest validation (EH-530, re-homed from the finance quant module so every policy
//! evaluation shares them): purged combinatorial cross-validation, the Deflated Sharpe
//! Ratio, the Probability of Backtest Overfit and the Diebold-Mariano test.

use std::collections::BTreeSet;

use super::special::{excess_kurtosis, norm_ppf, skew};
use crate::detkernel::kernels::{normal_cdf, normal_sf};
use crate::error::{NumericError, Result};

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
    // Keep this 10-digit literal, not `std::f64::consts::EULER_GAMMA`: the sealed
    // backtest run digest (`a_sealed_backtest_run_digest_is_pinned_across_hosts`)
    // pins the value this produces.
    #[allow(clippy::approx_constant)]
    let euler = 0.577_215_664_9;
    let nt = n_trials as f64;
    let e_max_sr = (1.0 - euler) * norm_ppf(1.0 - 1.0 / nt)
        + euler * norm_ppf(1.0 - 1.0 / (nt * std::f64::consts::E));
    let sr_var =
        (1.0 - g3 * observed_sr + (g4 / 4.0) * observed_sr * observed_sr) / (t as f64 - 1.0);
    if sr_var <= 0.0 {
        return 0.0;
    }
    normal_cdf((observed_sr - e_max_sr) / sr_var.sqrt())
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
        p_value: 2.0 * normal_sf(statistic.abs()),
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

/// [`purged_cpcv_splits`] with typed refusals for a degenerate request: no groups, no
/// test groups, more test groups than groups, or fewer samples than groups.
pub fn purged_cpcv_splits_checked(
    n_samples: usize,
    n_groups: usize,
    n_test_groups: usize,
    purge_window: usize,
    embargo: usize,
) -> Result<Vec<CvSplit>> {
    if n_groups == 0 || n_test_groups == 0 || n_test_groups > n_groups {
        return Err(NumericError::bounds(
            "cross-validation needs 0 < test groups <= groups",
        ));
    }
    if n_samples < n_groups {
        return Err(NumericError::bounds(
            "cross-validation needs at least one sample per group",
        ));
    }
    Ok(purged_cpcv_splits(
        n_samples,
        n_groups,
        n_test_groups,
        purge_window,
        embargo,
    ))
}

/// [`deflated_sharpe_ratio`] with typed refusals: a non-finite Sharpe or return, no
/// trial, or under four returns is an error rather than a silent `0`.
pub fn deflated_sharpe_ratio_checked(
    observed_sr: f64,
    n_trials: usize,
    sr_returns: &[f64],
) -> Result<f64> {
    if !observed_sr.is_finite() || sr_returns.iter().any(|r| !r.is_finite()) {
        return Err(NumericError::bounds("a Sharpe or return is non-finite"));
    }
    if n_trials < 1 {
        return Err(NumericError::bounds("a deflated Sharpe needs one trial"));
    }
    if sr_returns.len() < 4 {
        return Err(NumericError::bounds(
            "a deflated Sharpe needs at least four returns",
        ));
    }
    Ok(deflated_sharpe_ratio(observed_sr, n_trials, sr_returns))
}

/// [`probability_of_backtest_overfit`] with typed refusals: no splits, in-sample and
/// out-of-sample row counts that differ, ragged rows, under two strategies, or a
/// non-finite performance is an error rather than a silent `0`.
pub fn probability_of_backtest_overfit_checked(
    insample: &[Vec<f64>],
    oos: &[Vec<f64>],
) -> Result<f64> {
    if insample.is_empty() || insample.len() != oos.len() {
        return Err(NumericError::shape(
            "one in-sample and one out-of-sample row per split",
        ));
    }
    let width = insample[0].len();
    if width < 2 {
        return Err(NumericError::bounds(
            "overfit needs at least two strategies",
        ));
    }
    if insample.iter().chain(oos).any(|row| row.len() != width) {
        return Err(NumericError::shape("the split rows differ in width"));
    }
    if insample.iter().chain(oos).flatten().any(|v| !v.is_finite()) {
        return Err(NumericError::bounds("a performance is non-finite"));
    }
    Ok(probability_of_backtest_overfit(insample, oos))
}

/// [`diebold_mariano`] with typed refusals: unequal lengths, under ten pairs, a
/// zero horizon, or a non-finite loss is an error rather than an inconclusive result.
pub fn diebold_mariano_checked(
    losses_a: &[f64],
    losses_b: &[f64],
    h: usize,
) -> Result<DieboldMariano> {
    if losses_a.len() != losses_b.len() {
        return Err(NumericError::shape("the loss series differ in length"));
    }
    if losses_a.len() < 10 {
        return Err(NumericError::bounds(
            "Diebold-Mariano needs at least ten pairs",
        ));
    }
    if h == 0 {
        return Err(NumericError::bounds("the forecast horizon is at least one"));
    }
    if losses_a.iter().chain(losses_b).any(|v| !v.is_finite()) {
        return Err(NumericError::bounds("a loss is non-finite"));
    }
    Ok(diebold_mariano(losses_a, losses_b, h))
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-FINANCE-PRIMITIVES-R002
    #[test]
    fn cpcv_purges_and_embargoes_around_every_test_group() {
        let splits = purged_cpcv_splits(120, 6, 2, 5, 5);
        assert_eq!(splits.len(), 15, "C(6, 2) splits");
        let first = &splits[0];
        assert_eq!(first.test, (0..40).collect::<Vec<_>>());
        assert!(!first.train.contains(&44) && first.train.contains(&45));
        assert!(purged_cpcv_splits(10, 0, 1, 0, 0).is_empty());
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R002
    #[test]
    fn dsr_pbo_and_dm_behave_at_the_edges() {
        let rets: Vec<f64> = (0..200).map(|i| 0.001 + f64::from(i % 7) * 1e-4).collect();
        let dsr = deflated_sharpe_ratio(1.5, 10, &rets);
        assert!((0.0..=1.0).contains(&dsr));
        assert!(deflated_sharpe_ratio(1.5, 100, &rets) < deflated_sharpe_ratio(1.5, 2, &rets));
        let rows = |r: [f64; 3]| (0..4).map(|_| r.to_vec()).collect::<Vec<_>>();
        let is = rows([1.0, 2.0, 3.0]);
        assert_eq!(
            probability_of_backtest_overfit(&is, &rows([3.0, 2.0, 1.0])),
            1.0
        );
        assert_eq!(probability_of_backtest_overfit(&is, &is), 0.0);
        let a: Vec<f64> = (0..50).map(|i| f64::from(i % 3) * 0.1).collect();
        let b: Vec<f64> = a.iter().map(|x| x + 0.5).collect();
        assert!(diebold_mariano(&a, &b, 1).a_better);
        assert_eq!(diebold_mariano(&a[..5], &b[..5], 1).p_value, 1.0);
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R002.1.6
    #[test]
    fn diebold_mariano_independent_vectors() {
        // Losses and statistics worked independently with the textbook formula
        // `mean(d) / sqrt(gamma_0 / n)` for h = 1, and the Bartlett-weighted
        // long-run variance for h = 2.
        let a = [1.0, 2.0, 3.0, 4.0, 5.0, 2.0, 3.0, 1.0, 2.0, 4.0];
        let b = [2.0, 2.5, 3.5, 5.0, 5.5, 3.5, 3.0, 2.0, 3.5, 5.0];
        let h1 = diebold_mariano_checked(&a, &b, 1).unwrap();
        assert!((h1.statistic + 5.973_191_135_873_606).abs() < 1e-9);
        assert!((h1.p_value / 2.326_570_491_817_392_4e-9 - 1.0).abs() < 1e-4);
        assert!(h1.a_better);
        let h2 = diebold_mariano_checked(&a, &b, 2).unwrap();
        assert!((h2.statistic + 7.520_357_238_464_75).abs() < 1e-9);
        // Swapping the series flips the sign and the winner.
        let swapped = diebold_mariano_checked(&b, &a, 1).unwrap();
        assert!((swapped.statistic - 5.973_191_135_873_606).abs() < 1e-9);
        assert!(!swapped.a_better);
        // Short, mismatched, zero-horizon and non-finite inputs refuse.
        assert!(diebold_mariano_checked(&a[..9], &b[..9], 1).is_err());
        assert!(diebold_mariano_checked(&a, &b[..9], 1).is_err());
        assert!(diebold_mariano_checked(&a, &b, 0).is_err());
        let mut bad = a;
        bad[3] = f64::NAN;
        assert!(diebold_mariano_checked(&bad, &b, 1).is_err());
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R002.1.7
    #[test]
    fn deflated_sharpe_independent_vectors() {
        // Computed independently from the Bailey-Lopez de Prado formula:
        // skew -0.21208785, excess kurtosis -0.85325559, E[max SR] 1.5745983 at ten
        // trials, variance 0.15038792, so DSR = Phi((0.5 - 1.5745983) / 0.38780...).
        let returns = [0.01, -0.02, 0.03, 0.0, 0.015, -0.01, 0.02, 0.005];
        let dsr = deflated_sharpe_ratio_checked(0.5, 10, &returns).unwrap();
        assert!((dsr - 0.002_794_049_147_484_878_3).abs() < 1e-6);
        // More trials deflate the same Sharpe further.
        let more = deflated_sharpe_ratio_checked(0.5, 100, &returns).unwrap();
        assert!(more < dsr);
        // Short, trial-less and non-finite inputs refuse.
        assert!(deflated_sharpe_ratio_checked(0.5, 10, &returns[..3]).is_err());
        assert!(deflated_sharpe_ratio_checked(0.5, 0, &returns).is_err());
        assert!(deflated_sharpe_ratio_checked(f64::NAN, 10, &returns).is_err());
        let mut bad = returns;
        bad[2] = f64::INFINITY;
        assert!(deflated_sharpe_ratio_checked(0.5, 10, &bad).is_err());
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R002.1.8
    #[test]
    fn purged_cv_independent_vectors() {
        // Six samples in three groups of two, one test group each, hand derived.
        let plain = purged_cpcv_splits_checked(6, 3, 1, 0, 0).unwrap();
        let shape: Vec<(Vec<usize>, Vec<usize>)> = plain
            .iter()
            .map(|s| (s.test.clone(), s.train.clone()))
            .collect();
        assert_eq!(
            shape,
            vec![
                (vec![0, 1], vec![2, 3, 4, 5]),
                (vec![2, 3], vec![0, 1, 4, 5]),
                (vec![4, 5], vec![0, 1, 2, 3]),
            ]
        );
        // One sample purged before and one embargoed after each test group.
        let purged = purged_cpcv_splits_checked(6, 3, 1, 1, 1).unwrap();
        let train: Vec<Vec<usize>> = purged.iter().map(|s| s.train.clone()).collect();
        assert_eq!(train, vec![vec![3, 4, 5], vec![0, 5], vec![0, 1, 2]]);
        // C(4, 2) combinations, lexicographic, with the last group's test set last.
        let pairs = purged_cpcv_splits_checked(8, 4, 2, 0, 0).unwrap();
        assert_eq!(pairs.len(), 6);
        assert_eq!(pairs[0].test, vec![0, 1, 2, 3]);
        assert_eq!(pairs[5].test, vec![4, 5, 6, 7]);
        // Degenerate requests refuse.
        assert!(purged_cpcv_splits_checked(6, 0, 1, 0, 0).is_err());
        assert!(purged_cpcv_splits_checked(6, 3, 0, 0, 0).is_err());
        assert!(purged_cpcv_splits_checked(6, 3, 4, 0, 0).is_err());
        assert!(purged_cpcv_splits_checked(2, 3, 1, 0, 0).is_err());
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R002.1.9
    #[test]
    fn pbo_independent_vectors() {
        // Three strategies, the in-sample best is always the third. Its
        // out-of-sample rank (0 = worst) against the median rank 1:
        //   [3,2,1] -> rank 0 below; [1,2,3] -> rank 2; [1,3,2] -> rank 1, equal to
        //   the median so not overfit; [2,3,1] -> rank 0 below. PBO = 2 / 4.
        let is = vec![vec![1.0, 2.0, 3.0]; 4];
        let oos = vec![
            vec![3.0, 2.0, 1.0],
            vec![1.0, 2.0, 3.0],
            vec![1.0, 3.0, 2.0],
            vec![2.0, 3.0, 1.0],
        ];
        assert_eq!(
            probability_of_backtest_overfit_checked(&is, &oos).unwrap(),
            0.5
        );
        // A skilled strategy never lands below the median.
        assert_eq!(
            probability_of_backtest_overfit_checked(&is, &is).unwrap(),
            0.0
        );
        // Degenerate, ragged and non-finite inputs refuse.
        assert!(probability_of_backtest_overfit_checked(&[], &[]).is_err());
        assert!(probability_of_backtest_overfit_checked(&is, &oos[..3]).is_err());
        assert!(probability_of_backtest_overfit_checked(&[vec![1.0]], &[vec![1.0]]).is_err());
        let ragged = vec![vec![1.0, 2.0, 3.0], vec![1.0, 2.0]];
        assert!(probability_of_backtest_overfit_checked(&ragged, &ragged).is_err());
        let nan = vec![vec![1.0, f64::NAN, 3.0]];
        assert!(probability_of_backtest_overfit_checked(&nan, &nan).is_err());
    }
}
