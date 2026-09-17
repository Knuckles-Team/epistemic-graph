//! Nonconformity scores and prediction sets for classification.
//!
//! Every method here defines a per-option score `s(x, k)`; a set is
//! `{k : s(x, k) <= threshold}`. Calibrating the threshold on the scores of the
//! true (or best acceptable) options gives the coverage guarantee.
//!
//! * LAC: `s(x, k) = 1 - p_k`.
//! * APS: classes are ranked by probability (descending, lowest class first on
//!   ties); `s(x, k)` is the mass ranked strictly above `k` plus `u * p_k`, with
//!   `u = 1` for the non-randomised variant or a caller-supplied seeded uniform.
//! * RAPS adds `lambda * max(0, rank_k - k_reg)` with 1-based `rank_k`.
//! * Acceptability sets: an item's score is the minimum over its acceptable
//!   options, so the guarantee is `P(C ∩ A != ∅) >= 1 - alpha`.

use super::quantile::Threshold;
use crate::calibration::ProbabilityMatrix;
use crate::detkernel::reduce::order_descending;
use crate::detkernel::{validate, StatResult};

/// The options admitted by a conformal set, in ascending option order.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct PredictionSet {
    options: Vec<usize>,
}

impl PredictionSet {
    /// `{k : scores[k] admitted by threshold}`.
    pub fn from_scores(scores: &[f64], threshold: Threshold) -> Self {
        Self::from_admitted(scores.iter().map(|&score| threshold.admits(score)))
    }

    pub(crate) fn from_admitted(admitted: impl Iterator<Item = bool>) -> Self {
        Self {
            options: admitted
                .enumerate()
                .filter_map(|(k, keep)| keep.then_some(k))
                .collect(),
        }
    }

    /// Admitted options.
    pub fn options(&self) -> &[usize] {
        &self.options
    }

    /// Number of admitted options.
    pub fn len(&self) -> usize {
        self.options.len()
    }

    /// `true` when nothing is admitted.
    pub fn is_empty(&self) -> bool {
        self.options.is_empty()
    }

    /// `true` when `option` is admitted.
    pub fn contains(&self, option: usize) -> bool {
        self.options.binary_search(&option).is_ok()
    }

    /// `true` when the set meets the acceptability set.
    pub fn meets(&self, acceptable: &[usize]) -> bool {
        acceptable.iter().any(|&option| self.contains(option))
    }
}

/// RAPS regularisation `lambda * max(0, rank - k_reg)`; `lambda = 0` is APS.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RapsPenalty {
    lambda: f64,
    k_reg: usize,
}

impl RapsPenalty {
    /// Plain APS.
    pub fn none() -> Self {
        Self {
            lambda: 0.0,
            k_reg: 0,
        }
    }

    /// `lambda >= 0` finite.
    pub fn new(lambda: f64, k_reg: usize) -> StatResult<Self> {
        validate::parameter(
            lambda.is_finite() && lambda >= 0.0,
            "lambda",
            "finite and >= 0",
        )?;
        Ok(Self { lambda, k_reg })
    }

    fn at_rank(self, rank: usize) -> f64 {
        self.lambda * rank.saturating_sub(self.k_reg) as f64
    }
}

/// Least-ambiguous-classifier scores `1 - p_k`.
pub fn lac_scores(probabilities: &[f64]) -> StatResult<Vec<f64>> {
    validate::probability_vector(probabilities, "class probabilities")?;
    Ok(probabilities.iter().map(|p| 1.0 - p).collect())
}

/// APS / RAPS scores for every class of one item. `u` is in `[0, 1]`.
pub fn aps_scores(probabilities: &[f64], u: f64, penalty: RapsPenalty) -> StatResult<Vec<f64>> {
    validate::probability_vector(probabilities, "class probabilities")?;
    validate::all_unit_interval(&[u], "aps randomisation")?;
    let mut scores = vec![0.0; probabilities.len()];
    let mut above = 0.0;
    for (position, class) in order_descending(probabilities).into_iter().enumerate() {
        let p = probabilities[class];
        scores[class] = above + u * p + penalty.at_rank(position + 1);
        above += p;
    }
    Ok(scores)
}

/// The score of the best (smallest-scored) acceptable option.
pub fn acceptability_score(scores: &[f64], acceptable: &[usize]) -> StatResult<f64> {
    validate::non_empty(acceptable, "acceptable options")?;
    validate::all_finite(scores, "option scores")?;
    validate::labels_below(acceptable, scores.len())?;
    Ok(acceptable
        .iter()
        .map(|&option| scores[option])
        .fold(f64::INFINITY, f64::min))
}

/// Calibration scores of the labelled class for every row: APS/RAPS with
/// per-row `u` values (all 1 for the non-randomised variant).
pub fn aps_label_scores(
    probabilities: &ProbabilityMatrix,
    labels: &[usize],
    u: &[f64],
    penalty: RapsPenalty,
) -> StatResult<Vec<f64>> {
    let matrix = probabilities.labelled(labels)?;
    validate::same_len(matrix.row_count(), u.len(), "aps randomisation")?;
    matrix
        .rows()
        .zip(labels)
        .zip(u)
        .map(|((row, &label), &ui)| Ok(aps_scores(row, ui, penalty)?[label]))
        .collect()
}

/// Refuse a class-score row whose width is not `classes` or that is not finite.
pub(crate) fn class_row(scores: &[f64], classes: usize) -> StatResult<()> {
    validate::same_len(classes, scores.len(), "class scores")?;
    validate::all_finite(scores, "class scores")
}
