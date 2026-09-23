//! Exploration and audit sampling with exact propensities (EH-020, EH-026).
//!
//! Exploration is OFF unless the policy declares a budget that names the
//! question, and it is REFUSED -- not silently skipped -- for a security,
//! policy, write-back or irreversible question the budget names. Randomisation
//! happens only inside the legal set the earlier steps left, every draw comes
//! from the keyed seed, and the executed option's probability under the
//! randomised policy is recorded as an exact rational: `(1-f) + f/n` for the
//! greedy option, `f/n` for any other, with `f = a/b` the budget fraction.

use eg_types::decision::statistical::keyed::uniform_draw;
use eg_types::decision::statistical::{AuditDraw, QuestionSafety, StatisticalQuestion};
use eg_types::decision::{ColdStart, DecisionErrorCode, DecisionPolicy, UnitRationalWire};

use super::quant::exact_wire;
use super::refusal::{Refusal, RefusalResult};

/// Whether this decision may explore, and how much.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplorationPermit {
    Off,
    Budget(UnitRationalWire),
}

/// The exploration the policy permits for `question`.
pub fn permit(
    policy: &DecisionPolicy,
    question: &StatisticalQuestion,
) -> RefusalResult<ExplorationPermit> {
    let ColdStart::Explore { budget } = &policy.cold_start else {
        return Ok(ExplorationPermit::Off);
    };
    if !budget.questions.iter().any(|q| q == &question.question_id) {
        return Ok(ExplorationPermit::Off);
    }
    if question.safety != QuestionSafety::Ordinary {
        return Err(Refusal::decision(
            DecisionErrorCode::ExplorationForbidden,
            format!(
                "question {} is {:?}: exploration is never permitted for security, policy, \
                 write-back or irreversible questions",
                question.question_id, question.safety
            ),
        ));
    }
    if budget.fraction.numerator() == 0 {
        return Ok(ExplorationPermit::Off);
    }
    Ok(ExplorationPermit::Budget(budget.fraction))
}

/// What the randomised policy executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExplorationPlan {
    /// True when the exploration branch chose, not the greedy one.
    pub explored: bool,
    pub chosen: usize,
    /// The executed policy's exact probability of `chosen`.
    pub propensity: UnitRationalWire,
}

fn reduced(numerator: u128, denominator: u128) -> RefusalResult<UnitRationalWire> {
    let (mut a, mut b) = (numerator, denominator);
    while b != 0 {
        (a, b) = (b, a % b);
    }
    let divisor = a.max(1);
    let (n, d) = (numerator / divisor, denominator / divisor);
    let too_fine = || {
        Refusal::decision(
            DecisionErrorCode::PolicyLoosening,
            "the exploration fraction is too fine to record an exact propensity",
        )
    };
    exact_wire(
        u64::try_from(n).map_err(|_| too_fine())?,
        u64::try_from(d).map_err(|_| too_fine())?,
    )
}

/// Draw the exploration branch for a legal set of `set_len` options whose
/// greedy choice is `greedy` (`None` when there is no score to be greedy on).
/// `None` means the policy did not act: no exploration, and nothing greedy.
pub fn plan(
    seed: &[u8; 32],
    fraction: UnitRationalWire,
    set_len: usize,
    greedy: Option<usize>,
) -> RefusalResult<Option<ExplorationPlan>> {
    if set_len == 0 {
        return Ok(None);
    }
    let (a, b, n) = (
        u128::from(fraction.numerator()),
        u128::from(fraction.denominator()),
        set_len as u128,
    );
    let explored = u128::from(uniform_draw(seed, "explore", fraction.denominator())) < a;
    let chosen = match (explored, greedy) {
        (true, _) => uniform_draw(seed, "option", set_len as u64) as usize,
        (false, Some(g)) => g,
        (false, None) => return Ok(None),
    };
    let numerator = if Some(chosen) == greedy {
        n * (b - a) + a
    } else {
        a
    };
    Ok(Some(ExplorationPlan {
        explored,
        chosen,
        propensity: reduced(numerator, n * b)?,
    }))
}

/// The audit-sampling draw of an acted decision at the policy's rate.
pub fn audit(seed: &[u8; 32], rate: UnitRationalWire) -> AuditDraw {
    let draw = uniform_draw(seed, "audit", rate.denominator());
    AuditDraw {
        inclusion_probability: rate,
        sampled: draw < rate.numerator(),
    }
}
