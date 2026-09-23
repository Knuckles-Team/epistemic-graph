//! Evaluating a candidate head before it may be published (EH-062, EH-040).
//!
//! The evaluation reads only admitted items. Full-label items yield top-1,
//! log loss, Brier, expected calibration error, empirical coverage of the
//! prediction set with a Clopper-Pearson interval, the act rate and an upper
//! bound on the act risk, plus the promotion protocol's further measurements
//! ([`super::promotion`]). Bandit items yield off-policy estimates inside the
//! logging support only ([`super::evaluate_bandit`]). Every promotion gate that
//! fails is named in the receipt; the receipt passes exactly when none did.

use eg_types::decision::digest::digest_text;
use eg_types::decision::jobs::{
    DecisionEvalReceipt, FullLabelMetrics, LabelExclusions, OpeEstimatorKind, PromotionMetrics,
};
use eg_types::decision::statistical::dataset::{ItemLabel, LabelledDataset, LabelledItem};
use eg_types::decision::statistical::head::DecisionHeadBody;
use eg_types::decision::StatisticalPolicy;

use super::admission::Regime;
use super::evaluate_bandit::{bandit_gates, BanditReport};
use super::head_eval::{
    act_rule, calibration_statement, cost_macs, read_rows, scaled_softmax, top_index, RowsReading,
};
use super::promotion::{Observation, Protocol};
use super::quant::{item_rows, q32, unit_wire, value_of};
use super::refusal::{bounded, RefusalResult};
use crate::calibration::metrics::{reliability, BinCount};
use crate::detkernel::math;
use crate::risk::{clopper_pearson, BinomialCounts, IntervalSide};

/// Domain of an evaluation receipt digest.
pub const EVAL_RECEIPT_DOMAIN: &str = "eg/decision-eval-receipt/v1";
/// Reliability bins of the expected calibration error.
pub const ECE_BINS: usize = 10;
/// Probability floor inside the log loss.
pub const LOG_LOSS_FLOOR: f64 = 1e-15;

/// What one evaluation is asked to do.
#[derive(Debug, Clone, Copy)]
pub struct EvalSpec<'a> {
    pub regime: Regime,
    pub statistical: &'a StatisticalPolicy,
    pub estimators: &'a [OpeEstimatorKind],
    /// `sha256:<hex>` of the head body evaluated.
    pub head_digest: &'a str,
    pub policy_digest: &'a str,
}

/// One item read by the head: probabilities, or `None` out of distribution.
pub(crate) struct ItemReading {
    pub(crate) probabilities: Option<Vec<f64>>,
}

pub(crate) fn read_item(
    head: &DecisionHeadBody,
    dataset: &LabelledDataset,
    item: &LabelledItem,
) -> RefusalResult<ItemReading> {
    let rows = item_rows(dataset, item)?;
    let views: Vec<&[i64]> = rows.iter().map(Vec::as_slice).collect();
    let RowsReading::InDistribution(evaluated) = read_rows(head, &views)? else {
        return Ok(ItemReading {
            probabilities: None,
        });
    };
    let probabilities = match evaluated.probabilities {
        Some(p) => p,
        None => {
            let beta = head
                .calibration
                .as_ref()
                .map_or(1.0, |c| value_of(c.inverse_temperature));
            scaled_softmax(&evaluated.logits, beta)?
        }
    };
    Ok(ItemReading {
        probabilities: Some(probabilities),
    })
}

#[derive(Default)]
struct Tally {
    n: u64,
    top1: u64,
    covered: u64,
    acted: u64,
    acted_wrong: u64,
    set_total: u64,
    log_loss: f64,
    brier: f64,
    confidences: Vec<f64>,
    correct: Vec<bool>,
}

fn acceptable_indices(item: &LabelledItem) -> Vec<usize> {
    match &item.label {
        ItemLabel::Gold { acceptable, .. } => acceptable
            .iter()
            .filter_map(|id| item.index_of(id))
            .collect(),
        ItemLabel::Logged(_) => Vec::new(),
    }
}

/// What the act rule and the prediction set concluded about one item.
#[derive(Debug, Clone, Copy, Default)]
struct Judged {
    covered: bool,
    answered: bool,
}

fn tally_item(tally: &mut Tally, p: &[f64], good: &[usize], head: &DecisionHeadBody) -> Judged {
    let top = top_index(p).unwrap_or(0);
    let hit = good.contains(&top);
    let mass: f64 = good.iter().map(|&i| p[i]).sum();
    let share = 1.0 / good.len().max(1) as f64;
    let mut brier = 0.0;
    for (i, pi) in p.iter().enumerate() {
        let t = if good.contains(&i) { share } else { 0.0 };
        brier += (pi - t) * (pi - t);
    }
    tally.top1 += u64::from(hit);
    tally.log_loss += -math::ln(mass.max(LOG_LOSS_FLOOR));
    tally.brier += brier;
    tally.confidences.push(p[top].clamp(0.0, 1.0));
    tally.correct.push(hit);
    let Some(calibration) = head.calibration.as_ref() else {
        return Judged::default();
    };
    let rule = act_rule(p, calibration);
    let covered = rule.prediction_set.iter().any(|i| good.contains(i));
    tally.covered += u64::from(covered);
    tally.set_total += rule.prediction_set.len() as u64;
    if rule.acts {
        tally.acted += 1;
        tally.acted_wrong += u64::from(!hit);
    }
    Judged {
        covered,
        answered: rule.acts,
    }
}

fn rate_interval(
    hits: u64,
    n: u64,
    statistical: &StatisticalPolicy,
    side: IntervalSide,
) -> RefusalResult<(f64, f64)> {
    if n == 0 {
        return Ok((0.0, 1.0));
    }
    let delta = super::quant::level(statistical.delta)?;
    let interval = clopper_pearson(BinomialCounts::new(hits, n)?, delta, side)?;
    Ok((interval.lower, interval.upper))
}

fn metrics(tally: &Tally, statistical: &StatisticalPolicy) -> RefusalResult<FullLabelMetrics> {
    let n = tally.n.max(1) as f64;
    let ece = if tally.confidences.is_empty() {
        0.0
    } else {
        reliability(&tally.confidences, &tally.correct, BinCount::new(ECE_BINS)?)?
            .expected_calibration_error
    };
    let (coverage_lower, coverage_upper) =
        rate_interval(tally.covered, tally.n, statistical, IntervalSide::TwoSided)?;
    let (_, act_upper) = rate_interval(
        tally.acted_wrong,
        tally.acted,
        statistical,
        IntervalSide::Upper,
    )?;
    Ok(FullLabelMetrics {
        n_items: tally.n,
        top1_hits: tally.top1,
        log_loss: q32(tally.log_loss / n)?,
        brier: q32(tally.brier / n)?,
        expected_calibration_error: q32(ece)?,
        covered: tally.covered,
        coverage_lower: unit_wire(coverage_lower)?,
        coverage_upper: unit_wire(coverage_upper)?,
        acted: tally.acted,
        acted_wrong: tally.acted_wrong,
        act_risk_upper: unit_wire(act_upper)?,
        set_size_total: tally.set_total,
    })
}

fn full_label_gates(
    m: &FullLabelMetrics,
    head: &DecisionHeadBody,
    statistical: &StatisticalPolicy,
) -> Vec<String> {
    let mut failed = Vec::new();
    if m.n_items < statistical.n_min {
        failed.push("n_min".to_string());
    }
    let (num, den) = (
        u128::from(statistical.alpha.numerator()),
        u128::from(statistical.alpha.denominator()),
    );
    let covers = u128::from(m.covered) * den >= u128::from(m.n_items) * (den - num);
    if head.calibration.is_some() && !covers {
        failed.push("coverage".to_string());
    }
    let (e_num, e_den) = (
        u128::from(statistical.epsilon.numerator()),
        u128::from(statistical.epsilon.denominator()),
    );
    let risk_ok = u128::from(m.act_risk_upper.numerator()) * e_den
        <= e_num * u128::from(m.act_risk_upper.denominator());
    if m.acted > 0 && !risk_ok {
        failed.push("act_risk".to_string());
    }
    failed
}

/// One full-label evaluation's outputs.
type FullLabel = (FullLabelMetrics, PromotionMetrics, Vec<String>);

fn full_label(
    head: &DecisionHeadBody,
    dataset: &LabelledDataset,
    items: &[&LabelledItem],
    statistical: &StatisticalPolicy,
) -> RefusalResult<FullLabel> {
    let mut tally = Tally::default();
    let mut protocol = Protocol::default();
    for item in items {
        tally.n += 1;
        let good = acceptable_indices(item);
        let reading = read_item(head, dataset, item)?.probabilities;
        let again = read_item(head, dataset, item)?.probabilities;
        let judged = reading
            .as_deref()
            .map(|p| tally_item(&mut tally, p, &good, head))
            .unwrap_or_default();
        protocol.record(&Observation {
            class_key: &item.class_key,
            recorded_at_ms: item.recorded_at_ms,
            probabilities: reading.as_deref(),
            acceptable: &good,
            covered: judged.covered,
            answered: judged.answered,
            macs: cost_macs(head, item.candidate_ids.len()),
            stable: again == reading,
        });
    }
    let m = metrics(&tally, statistical)?;
    let mut gates = full_label_gates(&m, head, statistical);
    let (promotion, failed) =
        protocol.finish(m.top1_hits, head.calibration.is_some(), statistical)?;
    gates.extend(failed);
    Ok((m, promotion, gates))
}

/// Evaluate `head` over the admitted items and seal the receipt.
pub fn evaluate(
    head: &DecisionHeadBody,
    dataset: &LabelledDataset,
    items: &[&LabelledItem],
    exclusions: LabelExclusions,
    spec: &EvalSpec,
) -> RefusalResult<DecisionEvalReceipt> {
    let (metrics_value, promotion, mut failed, report) = match spec.regime {
        Regime::FullLabel => {
            let (m, promotion, gates) = full_label(head, dataset, items, spec.statistical)?;
            (
                Some(m),
                Some(Box::new(promotion)),
                gates,
                BanditReport::default(),
            )
        }
        Regime::BanditLabel => {
            let report = bandit_gates(head, dataset, items, spec)?;
            (None, None, report.failed.clone(), report)
        }
    };
    if items.is_empty() {
        failed.push("no_admitted_items".to_string());
    }
    let mut receipt = DecisionEvalReceipt {
        receipt_digest: String::new(),
        head_digest: spec.head_digest.to_string(),
        policy_digest: spec.policy_digest.to_string(),
        n_records: items.len() as u64,
        estimates: bounded(report.estimates)?,
        calibration: head.calibration.as_ref().map(calibration_statement),
        metrics: metrics_value,
        promotion,
        exclusions,
        pooled: bounded(report.pooled)?,
        failed_gates: bounded(failed.clone())?,
        passed: failed.is_empty(),
        synthetic: dataset.synthetic,
    };
    receipt.receipt_digest = digest_text(EVAL_RECEIPT_DOMAIN, &receipt);
    Ok(receipt)
}
