//! Fitting a decision head with a deterministic optimiser (EH-027, EH-062).
//!
//! The loss is the listwise cross-entropy against a target distribution per
//! item -- uniform over the acceptability set for a full-label item, the
//! executed option for a successful bandit item weighted by its clipped
//! inverse propensity -- plus a small ridge term, times the item's audit
//! inverse-probability weight. It is convex in the weights, and it is
//! minimised by cyclic coordinate descent, each coordinate by the pinned
//! bisection line search in [`crate::detkernel::optimise`]. No randomness, no
//! parallelism, fixed sweep order: the same items produce the same bits.

use eg_types::contract::BoundedVec;
use eg_types::decision::digest::digest_text;
use eg_types::decision::jobs::OptimiserSpec;
use eg_types::decision::statistical::body::content_digest_of;
use eg_types::decision::statistical::dataset::{ItemLabel, LabelledDataset, LabelledItem};
use eg_types::decision::statistical::head::{
    DecisionHeadBody, FeatureStandardisation, FittedRegime, HeadKind, DECISION_HEAD_SCHEMA_VERSION,
};
use eg_types::decision::statistical::StatisticalErrorCode;
use eg_types::decision::StatisticalPolicy;

use super::admission::Regime;
use super::fit_calibrate::calibrate;
use super::quant::{q32, raw_value, value_of};
use super::refusal::{Refusal, RefusalResult};
use crate::detkernel::kernels::softmax;
use crate::detkernel::optimise::minimise_convex_unbounded;

/// Most coordinate-descent sweeps a fit may run, whatever it asks for.
pub const MAX_SWEEPS: u32 = 200;
/// Bisection steps per coordinate line search.
pub const LINE_SEARCH_STEPS: u32 = 64;
/// Ridge strength.
pub const RIDGE: f64 = 1e-3;
/// Largest inverse-propensity weight a bandit item contributes.
pub const IPW_CLIP: f64 = 20.0;
/// Every `CALIBRATION_STRIDE`-th item (by item-id digest) is held out.
pub const CALIBRATION_STRIDE: usize = 4;
/// Domain of the admitted-items digest.
pub const TRAINING_ITEMS_DOMAIN: &str = "eg/decision-training-items/v1";

/// One training example: standardised rows, target distribution, weight.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Example {
    pub(crate) rows: Vec<Vec<f64>>,
    pub(crate) targets: Vec<f64>,
    pub(crate) weight: f64,
}

/// What one fit is asked to produce.
#[derive(Debug, Clone, Copy)]
pub struct FitSpec<'a> {
    pub head_kind: HeadKind,
    pub regime: Regime,
    pub optimiser: OptimiserSpec,
    pub feature_schema_digest: &'a str,
    pub statistical: &'a StatisticalPolicy,
}

fn raw_rows(dataset: &LabelledDataset, item: &LabelledItem) -> Vec<Vec<f64>> {
    let width = dataset.feature_names.len();
    item.features
        .as_slice()
        .chunks(width)
        .map(|row| row.iter().map(|&v| raw_value(v, dataset.scale)).collect())
        .collect()
}

fn standardisation(
    dataset: &LabelledDataset,
    items: &[&LabelledItem],
) -> RefusalResult<Vec<FeatureStandardisation>> {
    let width = dataset.feature_names.len();
    let rows: Vec<Vec<f64>> = items
        .iter()
        .flat_map(|item| raw_rows(dataset, item))
        .collect();
    (0..width)
        .map(|k| {
            let column: Vec<f64> = rows.iter().map(|row| row[k]).collect();
            column_standardisation(&column)
        })
        .collect()
}

fn column_standardisation(column: &[f64]) -> RefusalResult<FeatureStandardisation> {
    let n = column.len().max(1) as f64;
    let mut sum = 0.0;
    let (mut lower, mut upper) = (f64::INFINITY, f64::NEG_INFINITY);
    for &value in column {
        sum += value;
        lower = lower.min(value);
        upper = upper.max(value);
    }
    let mean = sum / n;
    let mut squares = 0.0;
    for &value in column {
        squares += (value - mean) * (value - mean);
    }
    let deviation = (squares / n).sqrt();
    let scale = if deviation > 0.0 { deviation } else { 1.0 };
    Ok(FeatureStandardisation {
        center: q32(mean)?,
        scale: q32(scale)?,
        lower: q32(if lower.is_finite() { lower } else { 0.0 })?,
        upper: q32(if upper.is_finite() { upper } else { 0.0 })?,
    })
}

fn standardised(rows: Vec<Vec<f64>>, spec: &[FeatureStandardisation]) -> Vec<Vec<f64>> {
    rows.into_iter()
        .map(|row| {
            row.iter()
                .zip(spec)
                .map(|(x, s)| (x - value_of(s.center)) / value_of(s.scale))
                .collect()
        })
        .collect()
}

fn audit_weight(item: &LabelledItem) -> f64 {
    item.audit_inclusion
        .filter(|p| p.numerator() > 0)
        .map_or(1.0, |p| p.denominator() as f64 / p.numerator() as f64)
}

fn targets(item: &LabelledItem) -> Option<(Vec<f64>, f64)> {
    let n = item.candidate_ids.len();
    match &item.label {
        ItemLabel::Gold { acceptable, .. } => {
            let share = 1.0 / acceptable.len().max(1) as f64;
            let t = (0..n)
                .map(|i| {
                    if acceptable
                        .iter()
                        .any(|a| a == &item.candidate_ids.as_slice()[i])
                    {
                        share
                    } else {
                        0.0
                    }
                })
                .collect();
            Some((t, 1.0))
        }
        ItemLabel::Logged(logged) => {
            let executed = item.index_of(&logged.executed)?;
            if logged.evaluation.success != Some(true) {
                return None;
            }
            let p = logged.logging_propensities.as_slice()[executed];
            let weight = (p.denominator() as f64 / p.numerator().max(1) as f64).min(IPW_CLIP);
            let t = (0..n)
                .map(|i| if i == executed { 1.0 } else { 0.0 })
                .collect();
            Some((t, weight))
        }
    }
}

/// Examples of the admitted items under a fixed standardisation. Items that
/// carry no positive signal (failed or censored bandit outcomes) yield none.
pub(crate) fn examples(
    dataset: &LabelledDataset,
    items: &[&LabelledItem],
    spec: &[FeatureStandardisation],
) -> Vec<Example> {
    items
        .iter()
        .filter_map(|item| {
            let (t, weight) = targets(item)?;
            Some(Example {
                rows: standardised(raw_rows(dataset, item), spec),
                targets: t,
                weight: weight * audit_weight(item),
            })
        })
        .collect()
}

/// `d loss / d w_k` with every logit shifted by `shift * x_k`.
fn coordinate_slope(examples: &[Example], logits: &[Vec<f64>], k: usize, shift: f64) -> f64 {
    let mut total = 0.0;
    for (example, z) in examples.iter().zip(logits) {
        let shifted: Vec<f64> = z
            .iter()
            .zip(&example.rows)
            .map(|(zj, row)| zj + shift * row[k])
            .collect();
        let Ok(p) = softmax(&shifted) else { continue };
        let mut inner = 0.0;
        for ((pj, tj), row) in p.iter().zip(&example.targets).zip(&example.rows) {
            inner += (pj - tj) * row[k];
        }
        total += example.weight * inner;
    }
    total
}

fn initial_logits(examples: &[Example]) -> Vec<Vec<f64>> {
    examples.iter().map(|e| vec![0.0; e.rows.len()]).collect()
}

/// Minimise the regularised listwise loss by cyclic coordinate descent.
pub(crate) fn fit_weights(
    examples: &[Example],
    dim: usize,
    optimiser: OptimiserSpec,
) -> RefusalResult<Vec<f64>> {
    let mut weights = vec![0.0; dim];
    let mut logits = initial_logits(examples);
    let tolerance = value_of(optimiser.tolerance).abs();
    for _ in 0..optimiser.max_iterations.min(MAX_SWEEPS) {
        let mut largest: f64 = 0.0;
        for k in 0..dim {
            let current = weights[k];
            let slope =
                |w: f64| coordinate_slope(examples, &logits, k, w - current) + 2.0 * RIDGE * w;
            let next = minimise_convex_unbounded(slope, current, 1.0, LINE_SEARCH_STEPS)?;
            let delta = next - current;
            for (z, example) in logits.iter_mut().zip(examples) {
                for (zj, row) in z.iter_mut().zip(&example.rows) {
                    *zj += delta * row[k];
                }
            }
            weights[k] = next;
            largest = largest.max(delta.abs());
        }
        if largest <= tolerance {
            break;
        }
    }
    Ok(weights)
}

/// Deterministic calibration hold-out: every `CALIBRATION_STRIDE`-th item in
/// item-id digest order. Returns `(training, calibration)`.
fn split<'a>(items: &[&'a LabelledItem]) -> (Vec<&'a LabelledItem>, Vec<&'a LabelledItem>) {
    let mut ordered: Vec<(String, &LabelledItem)> = items
        .iter()
        .map(|item| (content_digest_of(item.item_id.as_bytes()), *item))
        .collect();
    ordered.sort_by(|a, b| a.0.cmp(&b.0));
    let mut training = Vec::new();
    let mut calibration = Vec::new();
    for (rank, (_, item)) in ordered.into_iter().enumerate() {
        if rank % CALIBRATION_STRIDE == CALIBRATION_STRIDE - 1 {
            calibration.push(item);
        } else {
            training.push(item);
        }
    }
    (training, calibration)
}

fn calibrates(spec: &FitSpec) -> bool {
    spec.head_kind == HeadKind::ListwiseLogistic && spec.regime == Regime::FullLabel
}

fn regime_tag(regime: Regime) -> FittedRegime {
    match regime {
        Regime::FullLabel => FittedRegime::FullLabel,
        Regime::BanditLabel => FittedRegime::BanditLabel,
    }
}

fn bounded<T, const N: usize>(values: Vec<T>) -> RefusalResult<BoundedVec<T, N>> {
    BoundedVec::new(values)
        .map_err(|detail| Refusal::new(StatisticalErrorCode::DatasetInvalid, detail))
}

/// The digest of the admitted items, in the order they were admitted.
pub fn training_records_digest(items: &[&LabelledItem]) -> String {
    let ids: Vec<&str> = items.iter().map(|item| item.item_id.as_str()).collect();
    digest_text(TRAINING_ITEMS_DOMAIN, &ids)
}

/// Fit a head over the admitted items of `dataset`.
pub fn fit(
    dataset: &LabelledDataset,
    items: &[&LabelledItem],
    spec: &FitSpec,
) -> RefusalResult<DecisionHeadBody> {
    let (training, held_out) = if calibrates(spec) {
        split(items)
    } else {
        (items.to_vec(), Vec::new())
    };
    let standards = standardisation(dataset, items)?;
    let train = examples(dataset, &training, &standards);
    if train.is_empty() {
        return Err(Refusal::new(
            StatisticalErrorCode::NoAdmissibleLabels,
            "no admitted item carries a positive training signal",
        ));
    }
    let weights = fit_weights(&train, standards.len(), spec.optimiser)?;
    let quantised = weights
        .iter()
        .map(|w| q32(*w))
        .collect::<RefusalResult<Vec<_>>>()?;
    let working: Vec<f64> = quantised.iter().map(|w| value_of(*w)).collect();
    let calibration = if calibrates(spec) {
        calibrate(
            &examples(dataset, &held_out, &standards),
            &working,
            spec.statistical,
            dataset.synthetic,
        )?
    } else {
        None
    };
    DecisionHeadBody {
        schema_version: DECISION_HEAD_SCHEMA_VERSION,
        kind: spec.head_kind,
        regime: regime_tag(spec.regime),
        feature_schema_digest: spec.feature_schema_digest.to_string(),
        standardisation: bounded(standards)?,
        weights: bounded(quantised)?,
        calibration,
        training_records_digest: training_records_digest(items),
        n_training: train.len() as u64,
        synthetic: dataset.synthetic,
    }
    .checked()
    .map_err(|detail| Refusal::new(StatisticalErrorCode::HeadInvalid, detail))
}
