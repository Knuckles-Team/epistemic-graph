//! Class-conditional (Mondrian) conformal prediction and its binary form.
//!
//! Each class gets its own split-conformal threshold from the calibration items
//! labelled with that class, so coverage holds per class, not just on average.
//! A class with fewer than `n_min` calibration items gets no threshold and no
//! claim: it is admitted to every set (the conservative choice), and
//! [`MondrianConformal::fully_calibrated`] reports that a claim is unavailable.

use super::quantile::{split_conformal, ConformalQuantile};
use super::sets::{class_row, PredictionSet};
use crate::detkernel::{validate, Level, StatResult};
use crate::risk::SampleGate;

/// One class's threshold.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ClassThreshold {
    /// Calibrated on enough items.
    Calibrated(ConformalQuantile),
    /// Too few items: always admitted, no claim.
    BelowMinimum { n: u64, n_min: u64 },
}

impl ClassThreshold {
    fn admits(&self, score: f64) -> bool {
        match self {
            ClassThreshold::Calibrated(quantile) => quantile.threshold().admits(score),
            ClassThreshold::BelowMinimum { .. } => true,
        }
    }
}

/// Per-class thresholds with their level and gate.
#[derive(Debug, Clone, PartialEq)]
pub struct MondrianConformal {
    alpha: Level,
    gate: SampleGate,
    n_calibration: u64,
    thresholds: Vec<ClassThreshold>,
}

impl MondrianConformal {
    /// The miscoverage level.
    pub fn alpha(&self) -> Level {
        self.alpha
    }

    /// The minimum-sample gate.
    pub fn gate(&self) -> SampleGate {
        self.gate
    }

    /// Calibration items over all classes.
    pub fn n_calibration(&self) -> u64 {
        self.n_calibration
    }

    /// Per-class thresholds in class order.
    pub fn thresholds(&self) -> &[ClassThreshold] {
        &self.thresholds
    }

    /// `true` when every class cleared the gate, so a per-class coverage claim
    /// may be made for every class.
    pub fn fully_calibrated(&self) -> bool {
        self.thresholds
            .iter()
            .all(|t| matches!(t, ClassThreshold::Calibrated(_)))
    }

    /// The set for one item's per-class scores.
    pub fn prediction_set(&self, class_scores: &[f64]) -> StatResult<PredictionSet> {
        class_row(class_scores, self.thresholds.len())?;
        Ok(PredictionSet::from_admitted(
            self.thresholds
                .iter()
                .zip(class_scores)
                .map(|(threshold, &score)| threshold.admits(score)),
        ))
    }
}

/// Calibrate per-class thresholds from the labelled-class scores.
pub fn mondrian_conformal(
    label_scores: &[f64],
    labels: &[usize],
    classes: usize,
    alpha: Level,
    gate: SampleGate,
) -> StatResult<MondrianConformal> {
    validate::non_empty(label_scores, "conformal scores")?;
    validate::same_len(label_scores.len(), labels.len(), "labels")?;
    validate::parameter(classes >= 2, "classes", "at least two classes")?;
    validate::labels_below(labels, classes)?;
    let thresholds = (0..classes)
        .map(|class| class_threshold(label_scores, labels, class, alpha, gate))
        .collect::<StatResult<Vec<_>>>()?;
    Ok(MondrianConformal {
        alpha,
        gate,
        n_calibration: label_scores.len() as u64,
        thresholds,
    })
}

fn class_threshold(
    scores: &[f64],
    labels: &[usize],
    class: usize,
    alpha: Level,
    gate: SampleGate,
) -> StatResult<ClassThreshold> {
    let own: Vec<f64> = scores
        .iter()
        .zip(labels)
        .filter_map(|(&score, &label)| (label == class).then_some(score))
        .collect();
    let n = own.len() as u64;
    if !gate.admits(n) {
        return Ok(ClassThreshold::BelowMinimum {
            n,
            n_min: gate.n_min(),
        });
    }
    split_conformal(&own, alpha).map(ClassThreshold::Calibrated)
}

/// A binary conformal set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinarySet {
    /// Neither label.
    Empty,
    /// Only label 0.
    Negative,
    /// Only label 1.
    Positive,
    /// Both labels (abstain).
    Both,
}

/// Mondrian conformal for a binary score `p = P(label = 1)`, with scores
/// `1 - p` for label 1 and `p` for label 0.
#[derive(Debug, Clone, PartialEq)]
pub struct BinaryConformal(MondrianConformal);

fn binary_scores(p: f64) -> [f64; 2] {
    [p, 1.0 - p]
}

impl BinaryConformal {
    /// The underlying per-label thresholds (index 0 = negative, 1 = positive).
    pub fn mondrian(&self) -> &MondrianConformal {
        &self.0
    }

    /// The set for a score `p` in `[0, 1]`.
    pub fn predict(&self, p: f64) -> StatResult<BinarySet> {
        validate::all_unit_interval(&[p], "binary score")?;
        let set = self.0.prediction_set(&binary_scores(p))?;
        Ok(match (set.contains(0), set.contains(1)) {
            (false, false) => BinarySet::Empty,
            (true, false) => BinarySet::Negative,
            (false, true) => BinarySet::Positive,
            (true, true) => BinarySet::Both,
        })
    }
}

/// Calibrate binary conformal from positive-class scores and outcomes.
pub fn binary_conformal(
    positive_scores: &[f64],
    outcomes: &[bool],
    alpha: Level,
    gate: SampleGate,
) -> StatResult<BinaryConformal> {
    validate::same_len(positive_scores.len(), outcomes.len(), "binary outcomes")?;
    validate::all_unit_interval(positive_scores, "binary scores")?;
    let labels: Vec<usize> = outcomes.iter().map(|&o| usize::from(o)).collect();
    let label_scores: Vec<f64> = positive_scores
        .iter()
        .zip(&labels)
        .map(|(&p, &label)| binary_scores(p)[label])
        .collect();
    mondrian_conformal(&label_scores, &labels, 2, alpha, gate).map(BinaryConformal)
}
