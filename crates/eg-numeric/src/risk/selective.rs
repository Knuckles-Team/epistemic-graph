//! Selective risk control: calibrate the act threshold so that
//! `P(wrong | act) <= epsilon` holds with probability at least `1 - delta`.
//!
//! This is Learn-then-Test with fixed-sequence testing. The caller fixes a
//! strictly decreasing grid of candidate thresholds before seeing labels (most
//! conservative first). For threshold `lambda` the rule acts on items with
//! `score >= lambda`; with `n` acted items of which `k` were wrong, the null
//! "risk above epsilon" has the exact binomial p-value `P(Bin(n, epsilon) <= k)`.
//! Hypotheses are tested in grid order and testing stops at the first p-value
//! above `delta`; the certified threshold is the last rejected one, and the
//! family-wise error of all rejections is at most `delta`.
//!
//! A threshold that acts on fewer than `n_min` items is skipped without a test
//! and without a claim. The skip depends only on scores, never on labels, so it
//! does not change the sequence's error guarantee.

use super::binomial::{binomial_cdf, clopper_pearson, BinomialCounts, IntervalSide};
use super::sample_gate::SampleGate;
use crate::detkernel::reduce::order_descending;
use crate::detkernel::{validate, Level, StatResult};

/// Risk target, confidence and minimum sample size.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RiskTarget {
    /// Tolerated error rate among acted items.
    pub epsilon: Level,
    /// Allowed probability that the certificate is wrong.
    pub delta: Level,
    /// Minimum acted items for a threshold to be tested.
    pub gate: SampleGate,
}

/// What happened to one candidate threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TestOutcome {
    /// Acted on fewer than `n_min` items: not tested, no claim.
    SkippedBelowMinimum,
    /// Risk above epsilon was rejected at level delta.
    Certified,
    /// Not rejected; testing stopped here.
    NotRejected,
    /// After the stopping point; not tested.
    NotReached,
}

/// One candidate threshold's evidence.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ThresholdTest {
    /// The candidate threshold.
    pub threshold: f64,
    /// Items with `score >= threshold`.
    pub acted: u64,
    /// Wrong items among them.
    pub wrong: u64,
    /// `P(Bin(acted, epsilon) <= wrong)`, when tested.
    pub p_value: Option<f64>,
    /// Outcome.
    pub outcome: TestOutcome,
}

/// The certified threshold with its one-sided Clopper–Pearson risk bound.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CertifiedThreshold {
    /// The lowest certified threshold.
    pub threshold: f64,
    /// Items acted on at it.
    pub acted: u64,
    /// Wrong items among them.
    pub wrong: u64,
    /// Upper `1 - delta` Clopper–Pearson bound on the acted error rate.
    pub risk_upper_bound: f64,
}

/// The calibration result; `certified` is `None` when no threshold can act.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectiveRiskCertificate {
    /// The target it was calibrated for.
    pub target: RiskTarget,
    /// Calibration items.
    pub n_calibration: u64,
    /// The certified threshold, if any.
    pub certified: Option<CertifiedThreshold>,
    /// Every candidate in grid order.
    pub tests: Vec<ThresholdTest>,
}

struct Acted {
    descending_scores: Vec<f64>,
    wrong_prefix: Vec<u64>,
}

impl Acted {
    fn new(scores: &[f64], wrong: &[bool]) -> Self {
        let order = order_descending(scores);
        let mut wrong_prefix = Vec::with_capacity(order.len() + 1);
        wrong_prefix.push(0);
        for &index in &order {
            let last = wrong_prefix[wrong_prefix.len() - 1];
            wrong_prefix.push(last + u64::from(wrong[index]));
        }
        Self {
            descending_scores: order.iter().map(|&i| scores[i]).collect(),
            wrong_prefix,
        }
    }

    fn at(&self, threshold: f64) -> (u64, u64) {
        let acted = self.descending_scores.partition_point(|&s| s >= threshold);
        (acted as u64, self.wrong_prefix[acted])
    }
}

fn validate_inputs(scores: &[f64], wrong: &[bool], thresholds: &[f64]) -> StatResult<()> {
    validate::non_empty(scores, "risk scores")?;
    validate::same_len(scores.len(), wrong.len(), "risk outcomes")?;
    validate::all_finite(scores, "risk scores")?;
    validate::non_empty(thresholds, "risk thresholds")?;
    validate::all_finite(thresholds, "risk thresholds")?;
    let decreasing = thresholds.windows(2).all(|pair| pair[0] > pair[1]);
    validate::parameter(decreasing, "thresholds", "strictly decreasing")
}

fn test_threshold(threshold: f64, acted: &Acted, target: &RiskTarget) -> StatResult<ThresholdTest> {
    let (n, k) = acted.at(threshold);
    if !target.gate.admits(n) {
        return Ok(ThresholdTest {
            threshold,
            acted: n,
            wrong: k,
            p_value: None,
            outcome: TestOutcome::SkippedBelowMinimum,
        });
    }
    let p_value = binomial_cdf(k, n, target.epsilon.to_f64())?;
    let outcome = if p_value <= target.delta.to_f64() {
        TestOutcome::Certified
    } else {
        TestOutcome::NotRejected
    };
    Ok(ThresholdTest {
        threshold,
        acted: n,
        wrong: k,
        p_value: Some(p_value),
        outcome,
    })
}

fn not_reached(threshold: f64, acted: &Acted) -> ThresholdTest {
    let (n, k) = acted.at(threshold);
    ThresholdTest {
        threshold,
        acted: n,
        wrong: k,
        p_value: None,
        outcome: TestOutcome::NotReached,
    }
}

fn certify(test: &ThresholdTest, delta: Level) -> StatResult<CertifiedThreshold> {
    let counts = BinomialCounts::new(test.wrong, test.acted)?;
    let bound = clopper_pearson(counts, delta, IntervalSide::Upper)?;
    Ok(CertifiedThreshold {
        threshold: test.threshold,
        acted: test.acted,
        wrong: test.wrong,
        risk_upper_bound: bound.upper,
    })
}

/// Calibrate the act threshold over a fixed, strictly decreasing grid.
pub fn calibrate_selective_risk(
    scores: &[f64],
    wrong: &[bool],
    thresholds: &[f64],
    target: RiskTarget,
) -> StatResult<SelectiveRiskCertificate> {
    validate_inputs(scores, wrong, thresholds)?;
    let acted = Acted::new(scores, wrong);
    let mut tests = Vec::with_capacity(thresholds.len());
    let mut certified = None;
    let mut stopped = false;
    for &threshold in thresholds {
        if stopped {
            tests.push(not_reached(threshold, &acted));
            continue;
        }
        let test = test_threshold(threshold, &acted, &target)?;
        match test.outcome {
            TestOutcome::Certified => certified = Some(certify(&test, target.delta)?),
            TestOutcome::NotRejected => stopped = true,
            TestOutcome::SkippedBelowMinimum | TestOutcome::NotReached => {}
        }
        tests.push(test);
    }
    Ok(SelectiveRiskCertificate {
        target,
        n_calibration: scores.len() as u64,
        certified,
        tests,
    })
}
