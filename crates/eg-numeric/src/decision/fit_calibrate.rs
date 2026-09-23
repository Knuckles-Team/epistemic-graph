//! Calibrating a full-label listwise head on its held-out items (EH-024,
//! EH-005).
//!
//! Three steps, each on the held-out items only: temperature scaling (one
//! inverse temperature minimising the held-out listwise loss), an
//! acceptability-set split-conformal threshold at the policy's alpha -- the
//! weighted variant when any item carries an audit inclusion weight -- and a
//! Learn-then-Test act threshold certifying `P(wrong | act) <= epsilon` with
//! probability at least `1 - delta`. A step whose sample is too small makes
//! no claim: no act threshold is certified, and the head never acts.
//!
//! The conformal and act thresholds are computed on the probabilities the
//! head SERVES: a linear head's float softmax, or -- for the resident scorer
//! -- its fixed-point softmax over the scored options (EH-293). One core,
//! [`calibrate_logits`], serves both.

use eg_types::decision::statistical::dataset::{LabelledDataset, LabelledItem};
use eg_types::decision::statistical::head::{DecisionHeadBody, HeadCalibration};
use eg_types::decision::statistical::{RiskMethod, RiskStatement};
use eg_types::decision::{QuantisedValue, StatisticalPolicy};

use super::fit::Example;
use super::head_eval::{logit, scaled_softmax, top_index};
use super::quant::{item_rows, level, q32, unit_wire, value_of};
use super::refusal::RefusalResult;
use super::scorer::fixed::{to_f64, to_q32};
use super::scorer::forward::{read, served_probabilities, EXCLUDED_LOGIT};
use super::scorer::legal::LegalSet;
use super::targets::{audit_weight, targets};
use crate::conformal::{
    acceptability_score, realised_coverage_interval, split_conformal, weighted_split_conformal,
    Threshold,
};
use crate::detkernel::optimise::{minimise_convex_bounded, DEFAULT_BISECTION_STEPS};
use crate::risk::{calibrate_selective_risk, RiskTarget, SampleGate};

/// Inverse-temperature search bracket.
pub const INVERSE_TEMPERATURE_RANGE: (f64, f64) = (1e-3, 1e3);
/// `2^32` as a working value.
const Q32_ONE: f64 = 4_294_967_296.0;
/// Number of act thresholds on the fixed grid `0.995, 0.985, ..., 0.505`.
pub const ACT_GRID_POINTS: u32 = 50;

fn logits_of(examples: &[Example], weights: &[f64]) -> Vec<Vec<f64>> {
    examples
        .iter()
        .map(|e| e.rows.iter().map(|row| logit(weights, row)).collect())
        .collect()
}

/// How a head turns logits into the probabilities it serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reader {
    /// Float softmax over every option (the linear heads).
    Linear,
    /// Fixed-point softmax over the scored options (the resident scorer).
    Fixed,
}

/// The logit of an option the scorer did not score, as a working value.
fn excluded(z: f64) -> bool {
    z <= to_f64(EXCLUDED_LOGIT)
}

fn served(reader: Reader, logits: &[f64], beta: QuantisedValue) -> RefusalResult<Vec<f64>> {
    match reader {
        Reader::Linear => scaled_softmax(logits, value_of(beta)),
        Reader::Fixed => {
            let fixed: Vec<i64> = logits.iter().map(|&z| fixed_logit(z)).collect();
            Ok(served_probabilities(&fixed, to_q32(beta))
                .into_iter()
                .map(to_f64)
                .collect())
        }
    }
}

/// A working logit back on Q32. Scorer logits are Q32 values read exactly,
/// so this is exact.
fn fixed_logit(z: f64) -> i64 {
    if excluded(z) {
        EXCLUDED_LOGIT
    } else {
        (z * Q32_ONE) as i64
    }
}

/// `d loss / d beta`; options the scorer did not score carry no gradient.
fn temperature_slope(examples: &[Example], logits: &[Vec<f64>], beta: f64) -> f64 {
    let mut total = 0.0;
    for (example, z) in examples.iter().zip(logits) {
        let Ok(p) = scaled_softmax(z, beta) else {
            continue;
        };
        let mut inner = 0.0;
        for ((pj, tj), zj) in p.iter().zip(&example.targets).zip(z) {
            if !excluded(*zj) {
                inner += (pj - tj) * zj;
            }
        }
        total += example.weight * inner;
    }
    total
}

fn acceptable(example: &Example) -> Vec<usize> {
    (0..example.targets.len())
        .filter(|&i| example.targets[i] > 0.0)
        .collect()
}

fn set_threshold(threshold: Threshold) -> f64 {
    match threshold {
        Threshold::Empty => -1.0,
        Threshold::Finite(cut) => cut,
        Threshold::Infinite => 1.0,
    }
}

fn act_grid() -> Vec<f64> {
    (0..ACT_GRID_POINTS)
        .map(|i| 0.995 - 0.01 * f64::from(i))
        .collect()
}

struct Held {
    probabilities: Vec<Vec<f64>>,
    scores: Vec<f64>,
    weights: Vec<f64>,
    top: Vec<f64>,
    wrong: Vec<bool>,
}

fn held_out(
    examples: &[Example],
    logits: &[Vec<f64>],
    reader: Reader,
    beta: QuantisedValue,
) -> RefusalResult<Held> {
    let mut held = Held {
        probabilities: Vec::new(),
        scores: Vec::new(),
        weights: Vec::new(),
        top: Vec::new(),
        wrong: Vec::new(),
    };
    for (example, z) in examples.iter().zip(logits) {
        let p = served(reader, z, beta)?;
        let nonconformity: Vec<f64> = p.iter().map(|pj| 1.0 - pj).collect();
        let good = acceptable(example);
        held.scores
            .push(acceptability_score(&nonconformity, &good)?);
        let top = top_index(&p).unwrap_or(0);
        held.top.push(p[top]);
        held.wrong.push(!good.contains(&top));
        held.weights.push(example.weight);
        held.probabilities.push(p);
    }
    Ok(held)
}

fn conformal_threshold(held: &Held, policy: &StatisticalPolicy) -> RefusalResult<f64> {
    let alpha = level(policy.alpha)?;
    let weighted = held.weights.iter().any(|w| *w != 1.0);
    let quantile = if weighted {
        weighted_split_conformal(&held.scores, &held.weights, 1.0, alpha)?
    } else {
        split_conformal(&held.scores, alpha)?
    };
    Ok(set_threshold(quantile.threshold()))
}

fn certified_act(
    held: &Held,
    policy: &StatisticalPolicy,
    n: u64,
) -> RefusalResult<Option<(f64, RiskStatement)>> {
    let target = RiskTarget {
        epsilon: level(policy.epsilon)?,
        delta: level(policy.delta)?,
        gate: SampleGate::new(policy.n_min.max(1))?,
    };
    let certificate = calibrate_selective_risk(&held.top, &held.wrong, &act_grid(), target)?;
    Ok(certificate.certified.map(|c| {
        (
            c.threshold,
            RiskStatement {
                method: RiskMethod::LearnThenTest,
                epsilon: policy.epsilon,
                delta: policy.delta,
                n_calibration: n,
            },
        )
    }))
}

/// Calibrate a fitted linear listwise head on its held-out examples.
/// `None` when there are no held-out examples to calibrate on.
pub(crate) fn calibrate(
    examples: &[Example],
    weights: &[f64],
    policy: &StatisticalPolicy,
    synthetic: bool,
) -> RefusalResult<Option<HeadCalibration>> {
    let logits = logits_of(examples, weights);
    calibrate_logits(examples, &logits, Reader::Linear, policy, synthetic)
}

/// Calibrate the resident scorer on its own served outputs over the held-out
/// items: each item's targets and weight, and its fixed-point logits.
pub(crate) fn calibrate_scorer(
    head: &DecisionHeadBody,
    dataset: &LabelledDataset,
    held_out: &[&LabelledItem],
    policy: &StatisticalPolicy,
) -> RefusalResult<Option<HeadCalibration>> {
    let mut examples = Vec::new();
    let mut logits = Vec::new();
    for item in held_out {
        let Some((targets, weight)) = targets(item) else {
            continue;
        };
        let rows = item_rows(dataset, item)?;
        let views: Vec<&[i64]> = rows.iter().map(Vec::as_slice).collect();
        let Ok(scoring) = read(head, &views, &LegalSet::derive(views.len(), &[]))? else {
            continue;
        };
        logits.push(scoring.logits.into_iter().map(to_f64).collect());
        examples.push(Example {
            rows: Vec::new(),
            targets,
            weight: weight * audit_weight(item),
        });
    }
    calibrate_logits(&examples, &logits, Reader::Fixed, policy, dataset.synthetic)
}

/// The calibration core over logits served through `reader`. `None` when
/// there are no held-out examples to calibrate on.
pub(crate) fn calibrate_logits(
    examples: &[Example],
    logits: &[Vec<f64>],
    reader: Reader,
    policy: &StatisticalPolicy,
    synthetic: bool,
) -> RefusalResult<Option<HeadCalibration>> {
    if examples.is_empty() {
        return Ok(None);
    }
    let (low, high) = INVERSE_TEMPERATURE_RANGE;
    let beta = minimise_convex_bounded(
        |b| temperature_slope(examples, logits, b),
        low,
        high,
        DEFAULT_BISECTION_STEPS,
    )?;
    let beta_q = q32(beta)?;
    let held = held_out(examples, logits, reader, beta_q)?;
    let n = examples.len() as u64;
    let threshold = conformal_threshold(&held, policy)?;
    let (lower, upper) = realised_coverage_interval(level(policy.alpha)?, n, level(policy.delta)?)?
        .unwrap_or((0.0, 1.0));
    let act = certified_act(&held, policy, n)?;
    Ok(Some(HeadCalibration {
        inverse_temperature: beta_q,
        alpha: policy.alpha,
        set_threshold: q32(threshold)?,
        coverage_lower: unit_wire(lower)?,
        coverage_upper: unit_wire(upper)?,
        act_threshold: act.as_ref().map(|(t, _)| q32(*t)).transpose()?,
        risk: act.map(|(_, risk)| risk),
        n_calibration: n,
        synthetic,
    }))
}
