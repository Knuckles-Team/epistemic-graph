//! Step 4 of the resolution ladder: the statistical act/abstain rule.
//!
//! A head may make the engine ACT only when all of this holds: it is a
//! full-label listwise head (linear or the resident scorer), it carries a
//! non-synthetic calibration with a certified act threshold and risk statement, its
//! calibration sample reaches the policy's `n_min`, its alpha, epsilon and
//! delta are no looser than the policy's, and -- for the state at hand -- the
//! act rule holds ([`act_rule`]: a conformal claim, the top probability at the
//! threshold, the top option inside the prediction set).
//! Anything less is at most an advisory score labelled uncalibrated -- and
//! only when the policy's cold-start mode allows advisory output at all.
//! Propensities are the executed policy's, never the head's mass.

use eg_types::contract::BoundedVec;
use eg_types::decision::statistical::head::{DecisionHeadBody, FittedRegime, HeadCalibration};
use eg_types::decision::statistical::{
    AuditDraw, CalibrationStatement, RiskStatement, ScoredOption, StatisticalErrorCode,
    StatisticalOutcome,
};
use eg_types::decision::{
    AbstainReason, ColdStart, DecisionPolicy, StatisticalPolicy, UnitRationalWire,
};

use super::exploration::{audit, logging_vector, plan, ExplorationPermit, ExplorationPlan};
use super::head_eval::{act_rule, calibration_statement, top_index, Evaluated};
use super::quant::{exact_wire, q32};
use super::refusal::{Refusal, RefusalResult};

/// What the rule reads.
#[derive(Debug, Clone, Copy)]
pub struct LadderInputs<'a> {
    /// Sorted by id; every index below refers to this order.
    pub candidate_ids: &'a [String],
    pub head: Option<&'a DecisionHeadBody>,
    pub reading: Option<&'a Evaluated>,
    pub policy: &'a DecisionPolicy,
    pub statistical: &'a StatisticalPolicy,
    pub permit: ExplorationPermit,
    pub seed: &'a [u8; 32],
}

/// What the rule concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LadderResult {
    pub outcome: StatisticalOutcome,
    pub calibration: Option<CalibrationStatement>,
    /// The option whose logit contributions the record explains.
    pub explained: Option<usize>,
    pub audit: Option<AuditDraw>,
    /// The executed policy's distribution (see [`logging_vector`]); empty when
    /// nothing was executed.
    pub logging: Vec<UnitRationalWire>,
}

fn budget(permit: ExplorationPermit) -> Option<UnitRationalWire> {
    match permit {
        ExplorationPermit::Off => None,
        ExplorationPermit::Budget(fraction) => Some(fraction),
    }
}

fn no_looser(candidate: UnitRationalWire, bound: UnitRationalWire) -> bool {
    u128::from(candidate.numerator()) * u128::from(bound.denominator())
        <= u128::from(bound.numerator()) * u128::from(candidate.denominator())
}

fn acting_head_eligible(head: &DecisionHeadBody) -> bool {
    head.kind.is_listwise() && head.regime == FittedRegime::FullLabel && !head.synthetic
}

fn calibration_permits_acting(
    calibration: &HeadCalibration,
    risk: RiskStatement,
    policy: &StatisticalPolicy,
) -> bool {
    !calibration.synthetic
        && calibration.n_calibration >= policy.n_min
        && no_looser(calibration.alpha, policy.alpha)
        && no_looser(risk.epsilon, policy.epsilon)
        && no_looser(risk.delta, policy.delta)
}

/// The calibration that licenses acting, if the head has one the policy accepts.
fn acting_calibration<'a>(
    head: &'a DecisionHeadBody,
    policy: &StatisticalPolicy,
) -> Option<(&'a HeadCalibration, RiskStatement)> {
    if !acting_head_eligible(head) {
        return None;
    }
    let calibration = head.calibration.as_ref()?;
    let risk = calibration.risk?;
    calibration.act_threshold?;
    calibration_permits_acting(calibration, risk, policy).then_some((calibration, risk))
}

fn abstained() -> StatisticalOutcome {
    StatisticalOutcome::Abstained {
        reasons: BoundedVec::new(vec![AbstainReason::InsufficientConfidence])
            .expect("one reason is inside the bound"),
    }
}

fn ids(inputs: &LadderInputs, indices: &[usize]) -> RefusalResult<BoundedVec<String, 64>> {
    BoundedVec::new(
        indices
            .iter()
            .map(|&i| inputs.candidate_ids[i].clone())
            .collect(),
    )
    .map_err(|detail| Refusal::new(StatisticalErrorCode::CandidateSetTooLarge, detail))
}

fn advisory(inputs: &LadderInputs, reading: &Evaluated) -> RefusalResult<StatisticalOutcome> {
    let scores = inputs
        .candidate_ids
        .iter()
        .zip(&reading.logits)
        .map(|(id, logit)| {
            Ok(ScoredOption {
                option_id: id.clone(),
                score: q32(*logit)?,
                probability: None,
            })
        })
        .collect::<RefusalResult<Vec<_>>>()?;
    Ok(StatisticalOutcome::Advisory {
        scores: BoundedVec::new(scores)
            .map_err(|detail| Refusal::new(StatisticalErrorCode::CandidateSetTooLarge, detail))?,
        calibrated: false,
    })
}

fn greedy(reading: Option<&Evaluated>) -> Option<usize> {
    let reading = reading?;
    top_index(reading.probabilities.as_deref().unwrap_or(&reading.logits))
}

fn explored(
    inputs: &LadderInputs,
    drawn: &ExplorationPlan,
    greedy_choice: Option<usize>,
) -> RefusalResult<LadderResult> {
    Ok(LadderResult {
        outcome: StatisticalOutcome::Explored {
            option_id: inputs.candidate_ids[drawn.chosen].clone(),
            propensity: drawn.propensity,
        },
        calibration: None,
        explained: None,
        audit: None,
        logging: logging_vector(
            inputs.candidate_ids.len(),
            greedy_choice,
            budget(inputs.permit),
        )?,
    })
}

fn exploration(
    inputs: &LadderInputs,
    greedy_choice: Option<usize>,
) -> RefusalResult<Option<ExplorationPlan>> {
    match inputs.permit {
        ExplorationPermit::Off => Ok(None),
        ExplorationPermit::Budget(fraction) => plan(
            inputs.seed,
            fraction,
            inputs.candidate_ids.len(),
            greedy_choice,
        ),
    }
}

fn act_or_abstain(
    inputs: &LadderInputs,
    reading: &Evaluated,
    calibration: &HeadCalibration,
    risk: RiskStatement,
    propensity: UnitRationalWire,
) -> RefusalResult<LadderResult> {
    let probabilities = reading.probabilities.as_deref().unwrap_or(&reading.logits);
    let rule = act_rule(probabilities, calibration);
    let (acts, top) = (rule.acts, rule.top);
    let logging = match (acts, top) {
        (true, Some(top)) => {
            logging_vector(inputs.candidate_ids.len(), Some(top), budget(inputs.permit))?
        }
        _ => Vec::new(),
    };
    let (outcome, explained, audit_draw) = match (acts, top) {
        (true, Some(top)) => (
            StatisticalOutcome::Acted {
                option_id: inputs.candidate_ids[top].clone(),
                propensity,
                prediction_set: ids(inputs, &rule.prediction_set)?,
                risk,
            },
            Some(top),
            Some(audit(inputs.seed, inputs.statistical.audit_sample)),
        ),
        _ => (abstained(), top, None),
    };
    Ok(LadderResult {
        outcome,
        calibration: Some(calibration_statement(calibration)),
        explained,
        audit: audit_draw,
        logging,
    })
}

fn uncalibrated(inputs: &LadderInputs, reading: Option<&Evaluated>) -> RefusalResult<LadderResult> {
    let advisory_allowed = !matches!(inputs.policy.cold_start, ColdStart::DeterministicOnly);
    let outcome = match (advisory_allowed, reading) {
        (true, Some(reading)) => advisory(inputs, reading)?,
        _ => abstained(),
    };
    Ok(LadderResult {
        outcome,
        calibration: None,
        explained: greedy(reading),
        audit: None,
        logging: Vec::new(),
    })
}

/// Run the act/abstain rule.
pub fn decide(inputs: &LadderInputs) -> RefusalResult<LadderResult> {
    let acting = inputs
        .head
        .and_then(|head| acting_calibration(head, inputs.statistical));
    let greedy_choice = greedy(inputs.reading);
    let drawn = exploration(inputs, greedy_choice)?;
    if let Some(drawn) = drawn.filter(|d| d.explored) {
        return explored(inputs, &drawn, greedy_choice);
    }
    let propensity = match drawn {
        Some(d) => d.propensity,
        None => exact_wire(1, 1)?,
    };
    match (acting, inputs.reading) {
        (Some((calibration, risk)), Some(reading)) => {
            act_or_abstain(inputs, reading, calibration, risk, propensity)
        }
        _ => uncalibrated(inputs, inputs.reading),
    }
}
