//! Reading a decision head over a feature matrix (EH-027).
//!
//! Standardise, check each value against the range the head was fitted on
//! (drift: out-of-distribution abstains, §6.3), take one linear logit per
//! option, and -- for a listwise head -- turn the logits into a distribution
//! with a softmax at the calibrated inverse temperature. Every reduction is a
//! serial loop in feature order.

use eg_types::contract::BoundedVec;
use eg_types::decision::statistical::head::{DecisionHeadBody, HeadKind};
use eg_types::decision::statistical::{LinearExplanation, StatisticalErrorCode};
use eg_types::decision::QuantScaleTag;

use super::features::FeatureMatrix;
use super::quant::{q32, raw_value, value_of};
use super::refusal::{Refusal, RefusalResult};
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

/// Read `head` over every candidate of `matrix`.
pub fn read_head(head: &DecisionHeadBody, matrix: &FeatureMatrix) -> RefusalResult<HeadReading> {
    let weights = weights_of(head);
    let mut standardised = Vec::with_capacity(matrix.candidate_ids.len());
    for (index, id) in matrix.candidate_ids.iter().enumerate() {
        match standardise_row(head, matrix.row(index)) {
            Ok(row) => standardised.push(row),
            Err(feature) => {
                return Ok(HeadReading::OutOfDistribution {
                    component_id: id.clone(),
                    feature: matrix.feature_names[feature].clone(),
                })
            }
        }
    }
    let logits: Vec<f64> = standardised.iter().map(|x| logit(&weights, x)).collect();
    let probabilities = match head.kind {
        HeadKind::ListwiseLogistic => Some(scaled_softmax(&logits, inverse_temperature(head))?),
        HeadKind::WeightedFeatures => None,
    };
    Ok(HeadReading::InDistribution(Evaluated {
        standardised,
        logits,
        probabilities,
    }))
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

/// The exact per-feature logit contributions of one option.
pub fn explanation(
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
