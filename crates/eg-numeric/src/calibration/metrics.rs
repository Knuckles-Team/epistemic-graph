//! Calibration metrics: expected calibration error with fixed equal-width bins,
//! reliability tables, Brier score and log loss.
//!
//! A confidence `p` falls in bin `min(floor(p * B), B - 1)`, so bins are
//! `[i/B, (i+1)/B)` with the last bin closed at 1. Bin sums are serial in input
//! order.

use super::scores::ProbabilityMatrix;
use crate::detkernel::math;
use crate::detkernel::reduce::{argmax_first, serial_mean, serial_sum};
use crate::detkernel::{validate, StatResult};

/// A number of equal-width bins, 1..=10_000.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BinCount(usize);

impl BinCount {
    /// Validate a bin count.
    pub fn new(bins: usize) -> StatResult<Self> {
        validate::parameter((1..=10_000).contains(&bins), "bins", "1..=10000")?;
        Ok(Self(bins))
    }

    /// The count.
    pub fn get(self) -> usize {
        self.0
    }

    fn index_of(self, confidence: f64) -> usize {
        ((confidence * self.0 as f64) as usize).min(self.0 - 1)
    }
}

/// One row of a reliability table.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ReliabilityBin {
    /// Inclusive lower edge.
    pub lower: f64,
    /// Upper edge (exclusive except for the last bin).
    pub upper: f64,
    /// Items in the bin.
    pub count: u64,
    /// Mean confidence of the items (0 when empty).
    pub mean_confidence: f64,
    /// Fraction of the items that were correct (0 when empty).
    pub accuracy: f64,
}

/// A reliability table with its summary errors.
#[derive(Debug, Clone, PartialEq)]
pub struct CalibrationReport {
    /// Per-bin rows, all bins included.
    pub bins: Vec<ReliabilityBin>,
    /// `sum_b (n_b / n) |accuracy_b - confidence_b|`.
    pub expected_calibration_error: f64,
    /// `max_b |accuracy_b - confidence_b|` over non-empty bins.
    pub maximum_calibration_error: f64,
    /// Items scored.
    pub n: u64,
}

/// Reliability table for confidences in `[0, 1]` and their correctness.
pub fn reliability(
    confidences: &[f64],
    correct: &[bool],
    bins: BinCount,
) -> StatResult<CalibrationReport> {
    validate::non_empty(confidences, "confidences")?;
    validate::same_len(confidences.len(), correct.len(), "correctness")?;
    validate::all_unit_interval(confidences, "confidences")?;
    let mut sums = vec![(0u64, 0.0f64, 0u64); bins.get()];
    for (&p, &ok) in confidences.iter().zip(correct) {
        let slot = &mut sums[bins.index_of(p)];
        slot.0 += 1;
        slot.1 += p;
        slot.2 += u64::from(ok);
    }
    Ok(summarise(&sums, bins, confidences.len() as u64))
}

fn summarise(sums: &[(u64, f64, u64)], bins: BinCount, n: u64) -> CalibrationReport {
    let width = 1.0 / bins.get() as f64;
    let mut ece = 0.0;
    let mut mce: f64 = 0.0;
    let mut rows = Vec::with_capacity(sums.len());
    for (i, &(count, confidence_sum, hits)) in sums.iter().enumerate() {
        let row = bin_row(i, width, count, confidence_sum, hits);
        let gap = (row.accuracy - row.mean_confidence).abs();
        ece += count as f64 / n as f64 * gap;
        mce = mce.max(gap);
        rows.push(row);
    }
    CalibrationReport {
        bins: rows,
        expected_calibration_error: ece,
        maximum_calibration_error: mce,
        n,
    }
}

fn bin_row(i: usize, width: f64, count: u64, confidence_sum: f64, hits: u64) -> ReliabilityBin {
    let (mean_confidence, accuracy) = if count == 0 {
        (0.0, 0.0)
    } else {
        (confidence_sum / count as f64, hits as f64 / count as f64)
    };
    ReliabilityBin {
        lower: i as f64 * width,
        upper: (i + 1) as f64 * width,
        count,
        mean_confidence,
        accuracy,
    }
}

/// Top-label reliability: confidence is the largest class probability (lowest
/// class wins ties) and an item is correct when that class is the label.
pub fn top_label_reliability(
    probabilities: &ProbabilityMatrix,
    labels: &[usize],
    bins: BinCount,
) -> StatResult<CalibrationReport> {
    let (confidences, correct) = top_label(probabilities, labels)?;
    reliability(&confidences, &correct, bins)
}

fn top_label(
    probabilities: &ProbabilityMatrix,
    labels: &[usize],
) -> StatResult<(Vec<f64>, Vec<bool>)> {
    let matrix = probabilities.labelled(labels)?;
    let mut confidences = Vec::with_capacity(labels.len());
    let mut correct = Vec::with_capacity(labels.len());
    for (row, &label) in matrix.rows().zip(labels) {
        let top = argmax_first(row, "probability row")?;
        confidences.push(row[top]);
        correct.push(top == label);
    }
    Ok((confidences, correct))
}

/// Mean over classes of the one-vs-rest ECE of `p_k` against `label == k`.
pub fn classwise_ece(
    probabilities: &ProbabilityMatrix,
    labels: &[usize],
    bins: BinCount,
) -> StatResult<f64> {
    let matrix = probabilities.labelled(labels)?;
    let mut per_class = Vec::with_capacity(matrix.classes());
    for class in 0..matrix.classes() {
        let confidences: Vec<f64> = matrix.rows().map(|row| row[class]).collect();
        let hits: Vec<bool> = labels.iter().map(|&label| label == class).collect();
        per_class.push(reliability(&confidences, &hits, bins)?.expected_calibration_error);
    }
    serial_mean(&per_class, "classes")
}

/// Multi-class Brier score `mean_i sum_k (p_ik - [y_i = k])^2`.
pub fn brier_score(probabilities: &ProbabilityMatrix, labels: &[usize]) -> StatResult<f64> {
    let terms = per_item(probabilities, labels, |row, label| {
        let squares: Vec<f64> = row
            .iter()
            .enumerate()
            .map(|(k, &p)| {
                let gap = p - if k == label { 1.0 } else { 0.0 };
                gap * gap
            })
            .collect();
        serial_sum(&squares)
    })?;
    serial_mean(&terms, "brier items")
}

/// A probability floor for log loss, strictly inside `(0, 0.5)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProbabilityFloor(f64);

impl ProbabilityFloor {
    /// Validate a floor.
    pub fn new(floor: f64) -> StatResult<Self> {
        validate::parameter(floor > 0.0 && floor < 0.5, "floor", "0 < floor < 0.5")?;
        Ok(Self(floor))
    }
}

/// Mean log loss `-ln max(p_{i, y_i}, floor)`.
pub fn log_loss(
    probabilities: &ProbabilityMatrix,
    labels: &[usize],
    floor: ProbabilityFloor,
) -> StatResult<f64> {
    let terms = per_item(probabilities, labels, |row, label| {
        -math::ln(row[label].max(floor.0))
    })?;
    serial_mean(&terms, "log-loss items")
}

fn per_item(
    probabilities: &ProbabilityMatrix,
    labels: &[usize],
    term: impl Fn(&[f64], usize) -> f64,
) -> StatResult<Vec<f64>> {
    let matrix = probabilities.labelled(labels)?;
    Ok(matrix
        .rows()
        .zip(labels)
        .map(|(row, &label)| term(row, label))
        .collect())
}
