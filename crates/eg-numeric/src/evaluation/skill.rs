//! Predictive skill of a feature (EH-522 FeatureSkill, EH-530 re-home): the information
//! coefficient, the information ratio, the effective number of independent signals,
//! and the per-horizon skill report — rolling rank IC, its decay over horizons, ICIR
//! and a moving-block bootstrap interval — that Decide feature admission reads.

use ndarray::Array2;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::error::{NumericError, Result};
use crate::linalg::eigh;
use crate::series::{apply_pair, PairStat, Spec};

/// Pearson correlation of a signal with forward returns over their finite pairs; `0`
/// under two pairs or for a constant side.
pub fn information_coefficient(signal: &[f64], forward_returns: &[f64]) -> f64 {
    let pairs: Vec<(f64, f64)> = signal
        .iter()
        .zip(forward_returns)
        .filter(|(s, r)| s.is_finite() && r.is_finite())
        .map(|(&s, &r)| (s, r))
        .collect();
    if pairs.len() < 2 {
        return 0.0;
    }
    let (xs, ys): (Vec<f64>, Vec<f64>) = pairs.into_iter().unzip();
    crate::series::window::pearson(&xs, &ys).unwrap_or(0.0)
}

/// [`information_coefficient`] with typed refusals: a length mismatch, fewer than two
/// finite pairs, or a constant side is an error rather than a silent `0`.
pub fn information_coefficient_checked(signal: &[f64], forward_returns: &[f64]) -> Result<f64> {
    if signal.len() != forward_returns.len() {
        return Err(NumericError::shape(
            "signal and forward returns differ in length",
        ));
    }
    let (xs, ys): (Vec<f64>, Vec<f64>) = signal
        .iter()
        .zip(forward_returns)
        .filter(|(s, r)| s.is_finite() && r.is_finite())
        .map(|(&s, &r)| (s, r))
        .unzip();
    if xs.len() < 2 {
        return Err(NumericError::bounds(
            "an information coefficient needs two finite pairs",
        ));
    }
    crate::series::window::pearson(&xs, &ys)
        .ok_or_else(|| NumericError::bounds("a constant side has no correlation"))
}

/// The fundamental law of active management: `IR = IC · √N_independent`.
pub fn information_ratio(ic: f64, n_independent: f64) -> f64 {
    ic * n_independent.max(0.0).sqrt()
}

/// [`information_ratio`] with typed refusals: a non-finite coefficient, or a breadth
/// that is non-finite or negative, is an error rather than a clamped value.
pub fn information_ratio_checked(ic: f64, n_independent: f64) -> Result<f64> {
    if !ic.is_finite() || !(-1.0..=1.0).contains(&ic) {
        return Err(NumericError::bounds(
            "an information coefficient is finite and within [-1, 1]",
        ));
    }
    if !n_independent.is_finite() || n_independent < 0.0 {
        return Err(NumericError::bounds("breadth is finite and not negative"));
    }
    Ok(information_ratio(ic, n_independent))
}

/// The effective number of independent signals of a matrix (rows = signals, columns =
/// time): the participation ratio `(Σλ)² / Σλ²` of the eigenvalues of the rows'
/// correlation matrix. A constant row is uncorrelated with the rest.
pub fn effective_independent_n(rows: &[Vec<f64>]) -> f64 {
    let k = rows.len();
    let t = rows.first().map_or(0, Vec::len);
    if k == 0 {
        return 0.0;
    }
    if t < 2 {
        return k as f64;
    }
    let Ok((eigenvalues, _)) = eigh(correlation(rows, t).view()) else {
        return k as f64;
    };
    let sum: f64 = eigenvalues.iter().sum();
    let sum_sq: f64 = eigenvalues.iter().map(|l| l * l).sum();
    if sum_sq > 1e-12 {
        sum * sum / sum_sq
    } else {
        k as f64
    }
}

/// [`effective_independent_n`] with typed refusals: no rows, rows of unequal length,
/// fewer than two columns, or a non-finite value is an error.
pub fn effective_independent_n_checked(rows: &[Vec<f64>]) -> Result<f64> {
    let t = rows.first().map_or(0, Vec::len);
    if rows.is_empty() || t < 2 {
        return Err(NumericError::bounds(
            "effective breadth needs one row of at least two columns",
        ));
    }
    if rows.iter().any(|row| row.len() != t) {
        return Err(NumericError::shape("the rows differ in length"));
    }
    if rows.iter().flatten().any(|v| !v.is_finite()) {
        return Err(NumericError::bounds("a row holds a non-finite value"));
    }
    Ok(effective_independent_n(rows))
}

/// The rows' correlation matrix over their first `t` columns.
fn correlation(rows: &[Vec<f64>], t: usize) -> Array2<f64> {
    let k = rows.len();
    Array2::from_shape_fn((k, k), |(i, j)| {
        if i == j {
            return 1.0;
        }
        crate::series::window::pearson(
            &rows[i][..t.min(rows[i].len())],
            &rows[j][..t.min(rows[j].len())],
        )
        .unwrap_or(0.0)
    })
}

/// What a skill evaluation computes.
#[derive(Clone, Debug, PartialEq)]
pub struct SkillSpec {
    /// Forward horizons (observations): the feature at `t` against the outcome at `t + h`.
    pub horizons: Vec<usize>,
    /// The rolling rank-IC window.
    pub window: usize,
    /// Bootstrap resamples for the interval on the mean IC (`0`: no interval).
    pub resamples: usize,
    pub seed: u64,
}

/// A feature's skill at one horizon.
#[derive(Clone, Debug, PartialEq)]
pub struct HorizonSkill {
    pub horizon: usize,
    /// Rolling IC values the summary is over.
    pub n: usize,
    pub mean_ic: f64,
    /// Sample (`ddof = 1`) standard deviation of the rolling IC.
    pub ic_std: f64,
    /// `mean_ic / ic_std` (`0` for a constant IC).
    pub icir: f64,
    /// A 95% moving-block bootstrap interval on `mean_ic` (blocks of `window`, since
    /// overlapping windows make consecutive ICs dependent).
    pub ci_lo: f64,
    pub ci_hi: f64,
}

/// The whole report: the IC decay curve over horizons, and the breadth across them.
#[derive(Clone, Debug, PartialEq)]
pub struct FeatureSkill {
    pub horizons: Vec<HorizonSkill>,
    /// Effective number of independent horizons ([`effective_independent_n`] of the
    /// rolling-IC series).
    pub n_eff: f64,
    /// [`information_ratio`] of the first horizon's mean IC at `n_eff`.
    pub ir: f64,
}

/// Evaluate `feature` against a later `outcome` (equal-length, aligned series).
pub fn feature_skill(feature: &[f64], outcome: &[f64], spec: &SkillSpec) -> Result<FeatureSkill> {
    if feature.len() != outcome.len() {
        return Err(NumericError::shape("feature and outcome differ in length"));
    }
    if spec.horizons.is_empty() {
        return Err(NumericError::bounds("at least one horizon is needed"));
    }
    let series = spec
        .horizons
        .iter()
        .map(|&h| rolling_ic(feature, outcome, h, spec.window))
        .collect::<Result<Vec<_>>>()?;
    let horizons = spec
        .horizons
        .iter()
        .zip(&series)
        .map(|(&h, ic)| summarise(h, ic, spec))
        .collect::<Vec<_>>();
    let common = series.iter().map(Vec::len).min().unwrap_or(0);
    let rows: Vec<Vec<f64>> = series.iter().map(|s| s[..common].to_vec()).collect();
    let n_eff = effective_independent_n(&rows);
    let ir = information_ratio(horizons[0].mean_ic, n_eff);
    Ok(FeatureSkill {
        horizons,
        n_eff,
        ir,
    })
}

/// The rolling Spearman IC of `feature[t]` against `outcome[t + h]` (the one series
/// kernel, `ic`), defined values only.
fn rolling_ic(feature: &[f64], outcome: &[f64], h: usize, window: usize) -> Result<Vec<f64>> {
    let n = feature.len().saturating_sub(h);
    let ic = apply_pair(
        Spec::Pair(PairStat::RankCorr, window),
        &feature[..n],
        &outcome[h..h + n],
    )?;
    Ok(ic.into_iter().flatten().collect())
}

fn summarise(horizon: usize, ic: &[f64], spec: &SkillSpec) -> HorizonSkill {
    let n = ic.len();
    let mean_ic = mean(ic);
    let ic_std = sample_std(ic, mean_ic);
    let icir = if ic_std > 1e-12 {
        mean_ic / ic_std
    } else {
        0.0
    };
    let (ci_lo, ci_hi) = block_bootstrap(ic, spec.window.max(1), spec.resamples, spec.seed)
        .unwrap_or((mean_ic, mean_ic));
    HorizonSkill {
        horizon,
        n,
        mean_ic,
        ic_std,
        icir,
        ci_lo,
        ci_hi,
    }
}

fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    xs.iter().sum::<f64>() / xs.len() as f64
}

fn sample_std(xs: &[f64], mean: f64) -> f64 {
    if xs.len() < 2 {
        return 0.0;
    }
    let ss: f64 = xs.iter().map(|x| (x - mean) * (x - mean)).sum();
    (ss / (xs.len() - 1) as f64).sqrt()
}

/// A 95% moving-block bootstrap interval on the mean of `xs`, seeded (reproducible).
fn block_bootstrap(xs: &[f64], block: usize, resamples: usize, seed: u64) -> Option<(f64, f64)> {
    if resamples == 0 || xs.len() < 2 {
        return None;
    }
    let block = block.min(xs.len());
    let starts = xs.len() - block + 1;
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut means: Vec<f64> = (0..resamples)
        .map(|_| {
            let mut sample = Vec::with_capacity(xs.len() + block);
            while sample.len() < xs.len() {
                let s = rng.gen_range(0..starts);
                sample.extend_from_slice(&xs[s..s + block]);
            }
            mean(&sample[..xs.len()])
        })
        .collect();
    means.sort_by(f64::total_cmp);
    let at = |q: f64| means[((q * (resamples - 1) as f64).round() as usize).min(resamples - 1)];
    Some((at(0.025), at(0.975)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A feature that leads the outcome by two steps (a triangle wave, no platform `sin`),
    /// plus noise.
    fn leading(n: usize) -> (Vec<f64>, Vec<f64>) {
        let noise = |i: usize| ((i * 7919) % 13) as f64 / 13.0 - 0.5;
        let driver: Vec<f64> = (0..n + 2)
            .map(|i| ((i % 20) as f64 - 10.0).abs() / 10.0 + noise(i) * 0.1)
            .collect();
        (
            driver[..n].to_vec(),
            driver[..n]
                .iter()
                .enumerate()
                .map(|(i, _)| driver[i.saturating_sub(2)])
                .collect(),
        )
    }

    // spec: EG-FINANCE-PRIMITIVES-R002
    #[test]
    fn skill_peaks_at_the_true_lead_and_the_interval_is_reproducible() {
        let (feature, outcome) = leading(400);
        let spec = SkillSpec {
            horizons: vec![1, 2, 5],
            window: 30,
            resamples: 200,
            seed: 7,
        };
        let skill = feature_skill(&feature, &outcome, &spec).unwrap();
        let by_h: Vec<f64> = skill.horizons.iter().map(|h| h.mean_ic).collect();
        assert!(
            by_h[1] > by_h[0] && by_h[1] > by_h[2],
            "IC decay peaks at h = 2: {by_h:?}"
        );
        assert!(by_h[1] > 0.9);
        let two = &skill.horizons[1];
        assert!(two.ci_lo <= two.mean_ic && two.mean_ic <= two.ci_hi);
        assert_eq!(
            feature_skill(&feature, &outcome, &spec).unwrap(),
            skill,
            "seeded"
        );
        assert!(skill.n_eff >= 1.0 && skill.n_eff <= 3.0);
    }

    #[test]
    fn ic_ir_and_breadth_edge_cases() {
        assert_eq!(information_coefficient(&[1.0, f64::NAN], &[2.0, 3.0]), 0.0);
        assert!((information_coefficient(&[1.0, 2.0, 3.0], &[2.0, 4.0, 7.0]) - 0.993).abs() < 1e-3);
        assert!((information_ratio(0.05, 50.0) - 0.353_553).abs() < 1e-4);
        let s: Vec<f64> = (0..50).map(f64::from).collect();
        assert!((effective_independent_n(&[s.clone(), s.clone()]) - 1.0).abs() < 1e-9);
        let noise: Vec<f64> = (0..50).map(|i| f64::from((i * 37) % 11)).collect();
        assert!(effective_independent_n(&[s, noise]) > 1.5);
        assert!(feature_skill(
            &[1.0],
            &[1.0, 2.0],
            &SkillSpec {
                horizons: vec![1],
                window: 3,
                resamples: 0,
                seed: 0
            }
        )
        .is_err());
    }

    // spec: EG-FINANCE-PRIMITIVES-R002.1.3
    #[test]
    fn information_coefficient_independent_vectors() {
        // x = 1..5, y = 2,4,5,4,5: Sxy = 6, Sxx = 10, Syy = 6, r = 6/sqrt(60).
        let ic =
            information_coefficient_checked(&[1.0, 2.0, 3.0, 4.0, 5.0], &[2.0, 4.0, 5.0, 4.0, 5.0])
                .unwrap();
        assert!((ic - 0.774_596_669_241_483_4).abs() < 1e-12);
        assert_eq!(
            ic,
            information_coefficient(&[1.0, 2.0, 3.0, 4.0, 5.0], &[2.0, 4.0, 5.0, 4.0, 5.0])
        );
        // A perfect negative relation is exactly -1.
        let anti = information_coefficient_checked(&[1.0, 2.0, 3.0], &[3.0, 2.0, 1.0]).unwrap();
        assert!((anti + 1.0).abs() < 1e-12);
        // A non-finite pair is dropped before the correlation.
        let dropped =
            information_coefficient_checked(&[1.0, f64::NAN, 2.0, 3.0], &[3.0, 9.0, 2.0, 1.0])
                .unwrap();
        assert!((dropped + 1.0).abs() < 1e-12);
        // Malformed and short inputs refuse.
        assert!(information_coefficient_checked(&[1.0, 2.0], &[1.0]).is_err());
        assert!(information_coefficient_checked(&[], &[]).is_err());
        assert!(information_coefficient_checked(&[1.0], &[2.0]).is_err());
        assert!(information_coefficient_checked(&[1.0, f64::NAN], &[2.0, 3.0]).is_err());
        assert!(information_coefficient_checked(&[2.0, 2.0, 2.0], &[1.0, 2.0, 3.0]).is_err());
    }

    // spec: EG-FINANCE-PRIMITIVES-R002.1.4
    #[test]
    fn information_ratio_independent_vectors() {
        // IR = IC * sqrt(N): 0.5 * sqrt(16) = 2; 0.25 * sqrt(4) = 0.5; zero breadth is zero.
        assert_eq!(information_ratio_checked(0.5, 16.0).unwrap(), 2.0);
        assert_eq!(information_ratio_checked(0.25, 4.0).unwrap(), 0.5);
        assert_eq!(information_ratio_checked(-0.5, 16.0).unwrap(), -2.0);
        assert_eq!(information_ratio_checked(0.5, 0.0).unwrap(), 0.0);
        assert_eq!(
            information_ratio_checked(0.5, 16.0).unwrap(),
            information_ratio(0.5, 16.0)
        );
        // Malformed inputs refuse.
        for (ic, n) in [
            (f64::NAN, 4.0),
            (f64::INFINITY, 4.0),
            (1.5, 4.0),
            (0.5, -1.0),
            (0.5, f64::NAN),
            (0.5, f64::INFINITY),
        ] {
            assert!(information_ratio_checked(ic, n).is_err(), "{ic} {n}");
        }
    }

    // spec: EG-FINANCE-PRIMITIVES-R002.1.5
    #[test]
    fn effective_sample_size_independent_vectors() {
        // Identical rows are perfectly correlated: eigenvalues (2, 0), (2)^2 / 4 = 1.
        let row = vec![1.0, 2.0, 3.0, 4.0];
        let same = effective_independent_n_checked(&[row.clone(), row.clone()]).unwrap();
        assert!((same - 1.0).abs() < 1e-9);
        // Orthogonal +-1 rows have an identity correlation matrix: (2)^2 / 2 = 2.
        let a = vec![1.0, -1.0, 1.0, -1.0];
        let b = vec![1.0, 1.0, -1.0, -1.0];
        let orthogonal = effective_independent_n_checked(&[a.clone(), b]).unwrap();
        assert!((orthogonal - 2.0).abs() < 1e-9);
        // One row is one independent signal.
        assert!((effective_independent_n_checked(&[a]).unwrap() - 1.0).abs() < 1e-9);
        // Malformed and short inputs refuse.
        assert!(effective_independent_n_checked(&[]).is_err());
        assert!(effective_independent_n_checked(&[vec![1.0]]).is_err());
        assert!(effective_independent_n_checked(&[vec![1.0, 2.0], vec![1.0]]).is_err());
        assert!(effective_independent_n_checked(&[vec![1.0, f64::NAN]]).is_err());
    }

    // spec: EG-FINANCE-PRIMITIVES-R002.1.10
    #[test]
    fn brier_independent_vectors() {
        use crate::calibration::metrics::binary_brier_score;
        // (0.9-1)^2 + (0.8-1)^2 + (0.3-0)^2 + (0.2-0)^2 = 0.01 + 0.04 + 0.09 + 0.04.
        let brier = binary_brier_score(&[0.9, 0.8, 0.3, 0.2], &[1.0, 1.0, 0.0, 0.0]).unwrap();
        assert!((brier - 0.045).abs() < 1e-12);
        // A perfect forecaster scores zero; a maximally wrong one scores one.
        assert_eq!(binary_brier_score(&[1.0, 0.0], &[1.0, 0.0]).unwrap(), 0.0);
        assert_eq!(binary_brier_score(&[0.0, 1.0], &[1.0, 0.0]).unwrap(), 1.0);
        // An uninformative 0.5 forecast scores 0.25.
        assert_eq!(binary_brier_score(&[0.5, 0.5], &[1.0, 0.0]).unwrap(), 0.25);
        // Mismatched, empty and non-finite inputs refuse.
        assert!(binary_brier_score(&[0.5, 0.5], &[1.0]).is_err());
        assert!(binary_brier_score(&[], &[]).is_err());
        assert!(binary_brier_score(&[f64::NAN], &[1.0]).is_err());
    }
}
