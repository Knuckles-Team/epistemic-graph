//! Reading a decision head over a feature matrix (EH-027, EH-291).
//!
//! Standardise, check each value against the range the head was fitted on
//! (drift: out-of-distribution abstains, §6.3), take one linear logit per
//! option, and -- for a listwise head -- turn the logits into a distribution
//! with a softmax at the calibrated inverse temperature. Every reduction is a
//! serial loop in feature order. An `OptionAttention` head is read by the
//! resident scorer instead ([`super::scorer::forward`]), in fixed point; its
//! outputs are exact dyadic values, so everything downstream compares them
//! exactly.
//!
//! The act rule and the calibration statement are answered here, once, for
//! the ladder and for the promotion evaluation alike.

use eg_types::contract::BoundedVec;
use eg_types::decision::statistical::head::{DecisionHeadBody, HeadCalibration, HeadKind};
use eg_types::decision::statistical::{
    CalibrationMethod, CalibrationStatement, LinearExplanation, StatisticalErrorCode,
};
use eg_types::decision::QuantScaleTag;

use super::features::FeatureMatrix;
use super::quant::{q32, raw_value, value_of};
use super::refusal::{Refusal, RefusalResult};
use super::scorer::fixed::to_f64;
use super::scorer::forward::{macs, read as read_scorer, Scoring};
use super::scorer::legal::LegalSet;
use crate::detkernel::kernels::softmax;

/// A head read over every candidate.
#[derive(Debug, Clone, PartialEq)]
pub struct Evaluated {
    pub standardised: Vec<Vec<f64>>,
    pub logits: Vec<f64>,
    /// `Some` for a listwise head: the (calibrated when the head is) softmax.
    pub probabilities: Option<Vec<f64>>,
}

/// The head's reading, or the first out-of-distribution cell.
#[derive(Debug, Clone, PartialEq)]
pub enum HeadReading {
    InDistribution(Evaluated),
    OutOfDistribution {
        component_id: String,
        feature: String,
    },
}

/// Refuse a head whose shape does not match the matrix it is asked to read.
pub fn check_compatible(
    head: &DecisionHeadBody,
    schema_digest: &str,
    matrix: &FeatureMatrix,
) -> RefusalResult<()> {
    if head.feature_schema_digest != schema_digest {
        return Err(Refusal::new(
            StatisticalErrorCode::HeadInvalid,
            "the head was fitted on a different feature schema",
        ));
    }
    if head.weights.len() != matrix.feature_names.len() {
        return Err(Refusal::new(
            StatisticalErrorCode::HeadInvalid,
            "the head's weight count does not match the feature schema",
        ));
    }
    Ok(())
}

/// Standardise one row; `Err(feature index)` when a value is out of range.
pub fn standardise_row(head: &DecisionHeadBody, row: &[i64]) -> Result<Vec<f64>, usize> {
    let mut out = Vec::with_capacity(row.len());
    for (index, (&raw, spec)) in row.iter().zip(head.standardisation.iter()).enumerate() {
        let value = raw_value(raw, QuantScaleTag::Q32);
        if value < value_of(spec.lower) || value > value_of(spec.upper) {
            return Err(index);
        }
        out.push((value - value_of(spec.center)) / value_of(spec.scale));
    }
    Ok(out)
}

/// The linear logit `w . x`, summed serially.
pub fn logit(weights: &[f64], standardised: &[f64]) -> f64 {
    let mut total = 0.0;
    for (w, x) in weights.iter().zip(standardised) {
        total += w * x;
    }
    total
}

/// The head's weights as working values.
pub fn weights_of(head: &DecisionHeadBody) -> Vec<f64> {
    head.weights.iter().map(|w| value_of(*w)).collect()
}

/// The softmax of `inverse_temperature x logits`.
pub fn scaled_softmax(logits: &[f64], inverse_temperature: f64) -> RefusalResult<Vec<f64>> {
    let scaled: Vec<f64> = logits.iter().map(|z| z * inverse_temperature).collect();
    Ok(softmax(&scaled)?)
}

fn inverse_temperature(head: &DecisionHeadBody) -> f64 {
    head.calibration
        .as_ref()
        .map_or(1.0, |c| value_of(c.inverse_temperature))
}

/// A head read over a list of rows: the reading, or the first
/// out-of-distribution `(row, feature)`.
#[derive(Debug, Clone, PartialEq)]
pub enum RowsReading {
    InDistribution(Evaluated),
    OutOfDistribution { row: usize, feature: usize },
}

fn read_linear(head: &DecisionHeadBody, rows: &[&[i64]]) -> RefusalResult<RowsReading> {
    let weights = weights_of(head);
    let mut standardised = Vec::with_capacity(rows.len());
    for (row, values) in rows.iter().enumerate() {
        match standardise_row(head, values) {
            Ok(x) => standardised.push(x),
            Err(feature) => return Ok(RowsReading::OutOfDistribution { row, feature }),
        }
    }
    let logits: Vec<f64> = standardised.iter().map(|x| logit(&weights, x)).collect();
    let probabilities = if head.kind.is_listwise() {
        Some(scaled_softmax(&logits, inverse_temperature(head))?)
    } else {
        None
    };
    Ok(RowsReading::InDistribution(Evaluated {
        standardised,
        logits,
        probabilities,
    }))
}

fn evaluated_of(scoring: Scoring) -> Evaluated {
    let working = |values: Vec<i64>| values.into_iter().map(to_f64).collect::<Vec<f64>>();
    Evaluated {
        standardised: scoring.standardised.into_iter().map(working).collect(),
        logits: working(scoring.logits),
        probabilities: Some(working(scoring.probabilities)),
    }
}

fn read_attention(head: &DecisionHeadBody, rows: &[&[i64]]) -> RefusalResult<RowsReading> {
    let legal = LegalSet::derive(rows.len(), &[]);
    Ok(match read_scorer(head, rows, &legal)? {
        Ok(scoring) => RowsReading::InDistribution(evaluated_of(scoring)),
        Err(out) => RowsReading::OutOfDistribution {
            row: out.option,
            feature: out.feature,
        },
    })
}

/// Read `head` over `rows` (one `Q32` row per option, in option order).
pub fn read_rows(head: &DecisionHeadBody, rows: &[&[i64]]) -> RefusalResult<RowsReading> {
    match head.kind {
        HeadKind::WeightedFeatures | HeadKind::ListwiseLogistic => read_linear(head, rows),
        HeadKind::OptionAttention => read_attention(head, rows),
    }
}

/// Read `head` over every candidate of `matrix`.
pub fn read_head(head: &DecisionHeadBody, matrix: &FeatureMatrix) -> RefusalResult<HeadReading> {
    let rows: Vec<&[i64]> = (0..matrix.candidate_ids.len())
        .map(|i| matrix.row(i))
        .collect();
    Ok(match read_rows(head, &rows)? {
        RowsReading::InDistribution(evaluated) => HeadReading::InDistribution(evaluated),
        RowsReading::OutOfDistribution { row, feature } => HeadReading::OutOfDistribution {
            component_id: matrix.candidate_ids[row].clone(),
            feature: matrix.feature_names[feature].clone(),
        },
    })
}

/// Options whose nonconformity `1 - p` is within `threshold`, in option order.
pub fn prediction_set(probabilities: &[f64], threshold: f64) -> Vec<usize> {
    (0..probabilities.len())
        .filter(|&i| 1.0 - probabilities[i] <= threshold)
        .collect()
}

/// The first index of the largest value: ties break by option order.
pub fn top_index(values: &[f64]) -> Option<usize> {
    let mut best: Option<usize> = None;
    for (index, value) in values.iter().enumerate() {
        if best.is_none_or(|b| *value > values[b]) {
            best = Some(index);
        }
    }
    best
}

/// The exact per-feature logit contributions of one option -- for a linear
/// head only. The scorer's attention term has no exact per-feature split,
/// so an `OptionAttention` head records no explanation rather than a partial
/// one labelled exact.
pub fn explanation(
    head: &DecisionHeadBody,
    option_id: &str,
    standardised: &[f64],
) -> RefusalResult<Option<LinearExplanation>> {
    match head.kind {
        HeadKind::OptionAttention => Ok(None),
        HeadKind::WeightedFeatures | HeadKind::ListwiseLogistic => {
            linear_explanation(head, option_id, standardised).map(Some)
        }
    }
}

fn linear_explanation(
    head: &DecisionHeadBody,
    option_id: &str,
    standardised: &[f64],
) -> RefusalResult<LinearExplanation> {
    let weights = weights_of(head);
    let contributions = weights
        .iter()
        .zip(standardised)
        .map(|(w, x)| q32(w * x))
        .collect::<RefusalResult<Vec<_>>>()?;
    Ok(LinearExplanation {
        option_id: option_id.to_string(),
        contributions: BoundedVec::new(contributions)
            .map_err(|detail| Refusal::new(StatisticalErrorCode::HeadInvalid, detail))?,
    })
}

/// Whether the calibration makes a non-trivial conformal claim: a finite
/// set threshold below one. Otherwise temperature scaling is all the head has
/// (the fallback), it makes no coverage claim, and it may not act (EH-293).
pub fn conformal_claim(calibration: &HeadCalibration) -> bool {
    let threshold = value_of(calibration.set_threshold);
    (0.0..1.0).contains(&threshold)
}

/// The calibration statement a head's calibration supports.
pub fn calibration_statement(calibration: &HeadCalibration) -> CalibrationStatement {
    let method = if conformal_claim(calibration) {
        CalibrationMethod::Conformal
    } else {
        CalibrationMethod::Temperature
    };
    CalibrationStatement {
        method,
        alpha: Some(calibration.alpha),
        coverage_lower: Some(calibration.coverage_lower),
        coverage_upper: Some(calibration.coverage_upper),
        n_calibration: calibration.n_calibration,
        synthetic: calibration.synthetic,
    }
}

/// What the act rule concludes about one distribution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActRule {
    pub top: Option<usize>,
    pub prediction_set: Vec<usize>,
    pub acts: bool,
}

/// The act rule: act on the top option only when the calibration makes a
/// conformal claim, the top probability reaches the certified threshold, and
/// the top option is inside the conformal prediction set. A set that is
/// empty or misses the top option means the calibrated coverage bound does
/// not hold for this state, and the decision abstains.
pub fn act_rule(probabilities: &[f64], calibration: &HeadCalibration) -> ActRule {
    let top = top_index(probabilities);
    let set = prediction_set(probabilities, value_of(calibration.set_threshold));
    let acts = match (top, calibration.act_threshold.map(value_of)) {
        (Some(t), Some(lambda)) => {
            conformal_claim(calibration) && probabilities[t] >= lambda && set.contains(&t)
        }
        _ => false,
    };
    ActRule {
        top,
        prediction_set: set,
        acts,
    }
}

/// Multiply-accumulates one decision over `options` options costs: the
/// scorer's count, or `options x features` for a linear head.
pub fn cost_macs(head: &DecisionHeadBody, options: usize) -> u64 {
    let features = head.weights.len();
    match head.scorer.as_deref() {
        Some(params) => macs(
            features,
            usize::from(params.width),
            options,
            options.min(usize::from(params.shortlist)),
        ),
        None => (options * features) as u64,
    }
}
