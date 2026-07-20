//! Embedding-set drift detection (CONCEPT:EG-KG.sharding.semantic-embedding-store-backed depth: vector drift).
//!
//! Answers "has the DISTRIBUTION of stored embeddings shifted?" — the question that
//! matters right after a model swap ([`crate::version`] tags WHICH vectors came
//! from which model; this crate module tells you whether that swap actually moved
//! the distribution enough to matter, and by how much).
//!
//! The metric is the **Population Stability Index (PSI)**, the standard drift
//! score (originally a credit-risk-model monitoring metric, now the common
//! embedding/feature-drift score): bucket a REFERENCE sample into equal-frequency
//! bins, bucket a CURRENT sample into the SAME bin edges, and sum
//! `(cur% - ref%) * ln(cur% / ref%)` over bins. `PSI ≈ 0` ⇒ no shift; the
//! conventional thresholds are `< 0.1` none, `0.1..0.25` moderate, `>= 0.25`
//! significant (a re-embed / reindex is warranted).
//!
//! Embeddings are high-dimensional, so PSI is computed over a SCALAR projection of
//! each vector, two of which this module provides: the L2 **norm** (dimension-
//! agnostic — works even if `reference`/`current` differ in `dim`, e.g. comparing
//! across a model upgrade that changed dimensionality) and **per-dimension** value
//! distributions (requires equal `dim`, and pinpoints WHICH axes moved — useful
//! once norm drift flags something is wrong).
//!
//! Bin-edge quantiles reuse [`eg_numeric::reductions::percentile`] — the shared
//! numeric kernel, not a hand-rolled quantile.

use eg_numeric::reductions::percentile;
use ndarray::Array1;

/// Bucketed drift severity — the conventional PSI thresholds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DriftLevel {
    /// PSI < 0.1 — no meaningful shift.
    None,
    /// 0.1 <= PSI < 0.25 — a moderate shift; worth watching.
    Moderate,
    /// PSI >= 0.25 — a significant shift; the two samples are meaningfully
    /// different distributions (re-embed / reindex territory).
    Significant,
}

impl DriftLevel {
    fn from_psi(psi: f64) -> Self {
        if psi >= 0.25 {
            DriftLevel::Significant
        } else if psi >= 0.1 {
            DriftLevel::Moderate
        } else {
            DriftLevel::None
        }
    }
}

/// One drift measurement: the PSI score, its qualitative level, and the sample
/// sizes it was computed over (a PSI near a threshold is much less trustworthy at
/// n=20 than n=20,000 — callers should weigh `n_reference`/`n_current`).
#[derive(Clone, Debug, PartialEq)]
pub struct DriftReport {
    pub psi: f64,
    pub level: DriftLevel,
    pub n_reference: usize,
    pub n_current: usize,
}

/// A tiny floor so an empty bin never produces `ln(0)` / `ln(x/0)` — the standard
/// PSI stabilizer (Siddiqi 2006's credit-scoring monitoring practice).
const EPS: f64 = 1e-4;

/// PSI of `current` against `reference`, bucketed into `bins` equal-frequency bins
/// derived from `reference`'s own quantiles. `bins < 2` or either sample empty ⇒
/// `psi = 0.0` (degenerate — nothing to compare).
pub fn population_stability_index(reference: &[f64], current: &[f64], bins: usize) -> DriftReport {
    let (n_ref, n_cur) = (reference.len(), current.len());
    if bins < 2 || n_ref == 0 || n_cur == 0 {
        return DriftReport {
            psi: 0.0,
            level: DriftLevel::None,
            n_reference: n_ref,
            n_current: n_cur,
        };
    }

    // Equal-frequency bin edges from the REFERENCE distribution's own quantiles —
    // by construction every reference bin holds ~1/bins of the reference mass.
    let ref_arr = Array1::from_vec(reference.to_vec());
    let mut edges: Vec<f64> = (1..bins)
        .map(|i| {
            let q = i as f64 / bins as f64;
            percentile(ref_arr.view(), q * 100.0).unwrap_or(f64::NAN)
        })
        .collect();
    edges.retain(|e| !e.is_nan());
    edges.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    edges.dedup_by(|a, b| (*a - *b).abs() < f64::EPSILON);
    let n_bins = edges.len() + 1;
    if n_bins < 2 {
        // The reference distribution is (near-)constant — no informative bins.
        return DriftReport {
            psi: 0.0,
            level: DriftLevel::None,
            n_reference: n_ref,
            n_current: n_cur,
        };
    }

    let bucket_of = |x: f64| -> usize { edges.iter().filter(|&&e| x > e).count() };

    let mut ref_counts = vec![0usize; n_bins];
    for &x in reference {
        ref_counts[bucket_of(x)] += 1;
    }
    let mut cur_counts = vec![0usize; n_bins];
    for &x in current {
        cur_counts[bucket_of(x)] += 1;
    }

    let mut psi = 0.0f64;
    for b in 0..n_bins {
        let p_ref = (ref_counts[b] as f64 / n_ref as f64).max(EPS);
        let p_cur = (cur_counts[b] as f64 / n_cur as f64).max(EPS);
        psi += (p_cur - p_ref) * (p_cur / p_ref).ln();
    }

    DriftReport {
        psi,
        level: DriftLevel::from_psi(psi),
        n_reference: n_ref,
        n_current: n_cur,
    }
}

/// L2 norm of one embedding.
fn norm(v: &[f32]) -> f64 {
    v.iter()
        .map(|&x| (x as f64) * (x as f64))
        .sum::<f64>()
        .sqrt()
}

/// Drift over the embedding-NORM distribution — dimension-agnostic, so it works
/// even when `reference`/`current` embeddings came from models of different
/// dimensionality (a full model swap, not just a re-embed).
pub fn embedding_norm_drift(
    reference: &[Vec<f32>],
    current: &[Vec<f32>],
    bins: usize,
) -> DriftReport {
    let r: Vec<f64> = reference.iter().map(|v| norm(v)).collect();
    let c: Vec<f64> = current.iter().map(|v| norm(v)).collect();
    population_stability_index(&r, &c, bins)
}

/// Per-dimension drift: one [`DriftReport`] per embedding dimension, comparing
/// `reference[*][d]` vs `current[*][d]`'s distribution. Requires every embedding in
/// BOTH sets to share the same `dim` (returns `None` if they don't, or if either
/// set is empty) — unlike [`embedding_norm_drift`], per-dimension comparison is
/// only meaningful within one fixed vector space. Pinpoints WHICH axes moved once
/// the norm-level check has already flagged drift.
pub fn embedding_dimension_drift(
    reference: &[Vec<f32>],
    current: &[Vec<f32>],
    bins: usize,
) -> Option<Vec<DriftReport>> {
    let dim = reference.first()?.len();
    if dim == 0
        || reference.iter().any(|v| v.len() != dim)
        || current.iter().any(|v| v.len() != dim)
    {
        return None;
    }
    Some(
        (0..dim)
            .map(|d| {
                let r: Vec<f64> = reference.iter().map(|v| v[d] as f64).collect();
                let c: Vec<f64> = current.iter().map(|v| v[d] as f64).collect();
                population_stability_index(&r, &c, bins)
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    fn gaussian_sample(n: usize, mean: f64, std: f64, seed: u64) -> Vec<f64> {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        (0..n)
            .map(|_| {
                // Box-Muller — good enough for a synthetic drift test, no new dep.
                let u1: f64 = rng.gen_range(1e-9..1.0);
                let u2: f64 = rng.gen_range(0.0..1.0);
                mean + std * (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
            })
            .collect()
    }

    #[test]
    fn identical_distribution_has_near_zero_psi() {
        let reference = gaussian_sample(5000, 0.0, 1.0, 1);
        let current = gaussian_sample(5000, 0.0, 1.0, 2); // same mean/std, different draw
        let report = population_stability_index(&reference, &current, 10);
        assert!(
            report.psi < 0.05,
            "same distribution should have low PSI, got {}",
            report.psi
        );
        assert_eq!(report.level, DriftLevel::None);
    }

    #[test]
    fn shifted_distribution_has_high_psi() {
        let reference = gaussian_sample(5000, 0.0, 1.0, 1);
        let current = gaussian_sample(5000, 3.0, 1.0, 2); // shifted 3 std devs
        let report = population_stability_index(&reference, &current, 10);
        assert!(
            report.psi >= 0.25,
            "a 3-sigma shift must register as significant drift, got {}",
            report.psi
        );
        assert_eq!(report.level, DriftLevel::Significant);
    }

    #[test]
    fn embedding_norm_drift_detects_scale_shift() {
        // Same direction distribution, but "current" vectors are scaled up 5x —
        // simulating a model swap that changed the embedding norm convention.
        let mut rng = ChaCha8Rng::seed_from_u64(11);
        let dim = 32;
        let reference: Vec<Vec<f32>> = (0..1000)
            .map(|_| (0..dim).map(|_| rng.gen::<f32>() - 0.5).collect())
            .collect();
        let current: Vec<Vec<f32>> = reference
            .iter()
            .map(|v| v.iter().map(|&x| x * 5.0).collect())
            .collect();
        let report = embedding_norm_drift(&reference, &current, 10);
        assert_eq!(report.level, DriftLevel::Significant);
    }

    #[test]
    fn embedding_dimension_drift_pinpoints_shifted_axis() {
        let mut rng = ChaCha8Rng::seed_from_u64(21);
        let dim = 4;
        let reference: Vec<Vec<f32>> = (0..2000)
            .map(|_| (0..dim).map(|_| rng.gen::<f32>() - 0.5).collect())
            .collect();
        // Shift ONLY dimension 2 in current.
        let current: Vec<Vec<f32>> = reference
            .iter()
            .map(|v| {
                let mut w = v.clone();
                w[2] += 10.0;
                w
            })
            .collect();
        let reports = embedding_dimension_drift(&reference, &current, 10).unwrap();
        assert_eq!(reports.len(), 4);
        assert_eq!(
            reports[2].level,
            DriftLevel::Significant,
            "dim 2 was shifted"
        );
        assert_eq!(reports[0].level, DriftLevel::None, "dim 0 was untouched");
    }

    #[test]
    fn dimension_drift_none_on_mismatched_dims() {
        let reference = vec![vec![0.0f32, 1.0], vec![0.5, 0.2]];
        let current = vec![vec![0.0f32, 1.0, 2.0]]; // different dim
        assert!(embedding_dimension_drift(&reference, &current, 5).is_none());
    }

    #[test]
    fn empty_samples_degrade_to_zero_not_panic() {
        let report = population_stability_index(&[], &[1.0, 2.0], 10);
        assert_eq!(report.psi, 0.0);
        assert_eq!(report.level, DriftLevel::None);
    }
}
