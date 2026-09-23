//! Which labelled items may train or evaluate a head (EH-016, EH-022).
//!
//! The training invariant (§6.4, invariant 8) is enforced here, item by item,
//! and every refusal is counted by reason so the receipt says what was left
//! out. A bandit label is admitted only when its outcome was evaluated
//! independently -- an observation or better, produced by neither the
//! selected agent nor its lease holder, at or above the fidelity floor, not
//! censored -- its propensity is the executed policy's and non-zero, it was
//! not pinned, and its commit principal is approved. A full-label item is
//! admitted unless an LLM resolved it. The two regimes never mix.

use eg_types::decision::jobs::LabelExclusions;
use eg_types::decision::statistical::dataset::{
    ItemLabel, LabelSource, LabelledDataset, LabelledItem, LoggedOutcome, OutcomeFidelity,
    PropensitySource,
};
use eg_types::decision::{EvidenceClass, RecordWindow, TraceFidelityLevel};

/// Which regime a job reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Regime {
    FullLabel,
    BanditLabel,
}

/// The admission rules of one job.
#[derive(Debug, Clone, Copy)]
pub struct AdmissionRules<'a> {
    pub regime: Regime,
    pub window: RecordWindow,
    pub fidelity_floor: TraceFidelityLevel,
    pub approved_principals: &'a [String],
}

/// Why one item was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exclusion {
    OutsideWindow,
    WrongRegime,
    LlmResolved,
    SelfReported,
    NotObservation,
    Censored,
    BelowFidelityFloor,
    PropensityNotExecutedPolicy,
    Pinned,
    UnapprovedPrincipal,
    ZeroExecutedPropensity,
}

/// The admitted items, in dataset order, and the refusals by reason.
#[derive(Debug, Clone)]
pub struct Admitted<'a> {
    pub items: Vec<&'a LabelledItem>,
    pub exclusions: LabelExclusions,
}

fn fidelity_rank(fidelity: OutcomeFidelity) -> Option<u8> {
    match fidelity {
        OutcomeFidelity::FullStep => Some(0),
        OutcomeFidelity::ToolCalls => Some(1),
        OutcomeFidelity::FinalOutput => Some(2),
        OutcomeFidelity::TraceIncomplete
        | OutcomeFidelity::OutcomeUncertain
        | OutcomeFidelity::Cancelled => None,
    }
}

fn floor_rank(floor: TraceFidelityLevel) -> u8 {
    match floor {
        TraceFidelityLevel::FullStep => 0,
        TraceFidelityLevel::ToolCalls => 1,
        TraceFidelityLevel::FinalOutput => 2,
    }
}

type LoggedCheck = fn(&LabelledItem, &LoggedOutcome, &AdmissionRules) -> Option<Exclusion>;

fn self_reported(
    _: &LabelledItem,
    logged: &LoggedOutcome,
    _: &AdmissionRules,
) -> Option<Exclusion> {
    let producer = &logged.evaluation.producer;
    (producer == &logged.evaluation.selected_agent || producer == &logged.evaluation.lease_holder)
        .then_some(Exclusion::SelfReported)
}

fn not_observation(
    _: &LabelledItem,
    logged: &LoggedOutcome,
    _: &AdmissionRules,
) -> Option<Exclusion> {
    (logged.evaluation.class == EvidenceClass::Claim).then_some(Exclusion::NotObservation)
}

fn censored(_: &LabelledItem, logged: &LoggedOutcome, _: &AdmissionRules) -> Option<Exclusion> {
    let untraced = fidelity_rank(logged.evaluation.fidelity).is_none();
    (untraced || logged.evaluation.success.is_none()).then_some(Exclusion::Censored)
}

fn below_floor(
    _: &LabelledItem,
    logged: &LoggedOutcome,
    rules: &AdmissionRules,
) -> Option<Exclusion> {
    let rank = fidelity_rank(logged.evaluation.fidelity)?;
    (rank > floor_rank(rules.fidelity_floor)).then_some(Exclusion::BelowFidelityFloor)
}

fn head_mass(_: &LabelledItem, logged: &LoggedOutcome, _: &AdmissionRules) -> Option<Exclusion> {
    (logged.propensity_source != PropensitySource::ExecutedPolicy)
        .then_some(Exclusion::PropensityNotExecutedPolicy)
}

fn pinned(_: &LabelledItem, logged: &LoggedOutcome, _: &AdmissionRules) -> Option<Exclusion> {
    logged.pinned.then_some(Exclusion::Pinned)
}

fn unapproved(
    _: &LabelledItem,
    logged: &LoggedOutcome,
    rules: &AdmissionRules,
) -> Option<Exclusion> {
    let approved = rules
        .approved_principals
        .iter()
        .any(|p| p == &logged.commit_principal);
    (!approved).then_some(Exclusion::UnapprovedPrincipal)
}

fn zero_propensity(
    item: &LabelledItem,
    logged: &LoggedOutcome,
    _: &AdmissionRules,
) -> Option<Exclusion> {
    let executed = item.index_of(&logged.executed);
    let zero = executed.is_none_or(|i| logged.logging_propensities.as_slice()[i].numerator() == 0);
    zero.then_some(Exclusion::ZeroExecutedPropensity)
}

/// Every bandit-label rule, in the order refusals are attributed.
const LOGGED_CHECKS: &[LoggedCheck] = &[
    self_reported,
    not_observation,
    censored,
    below_floor,
    head_mass,
    pinned,
    unapproved,
    zero_propensity,
];

/// Why `item` is refused under `rules`, or `None` when it is admitted.
pub fn exclusion(item: &LabelledItem, rules: &AdmissionRules) -> Option<Exclusion> {
    if item.recorded_at_ms < rules.window.from_ms || item.recorded_at_ms > rules.window.to_ms {
        return Some(Exclusion::OutsideWindow);
    }
    match (&item.label, rules.regime) {
        (ItemLabel::Gold { source, .. }, Regime::FullLabel) => {
            (*source == LabelSource::LlmResolved).then_some(Exclusion::LlmResolved)
        }
        (ItemLabel::Logged(logged), Regime::BanditLabel) => LOGGED_CHECKS
            .iter()
            .find_map(|check| check(item, logged, rules)),
        (ItemLabel::Gold { .. }, Regime::BanditLabel)
        | (ItemLabel::Logged(_), Regime::FullLabel) => Some(Exclusion::WrongRegime),
    }
}

fn count(exclusions: &mut LabelExclusions, reason: Exclusion) {
    let slot = match reason {
        Exclusion::OutsideWindow => &mut exclusions.outside_window,
        Exclusion::WrongRegime => &mut exclusions.wrong_regime,
        Exclusion::LlmResolved => &mut exclusions.llm_resolved,
        Exclusion::SelfReported => &mut exclusions.self_reported,
        Exclusion::NotObservation => &mut exclusions.not_observation,
        Exclusion::Censored => &mut exclusions.censored,
        Exclusion::BelowFidelityFloor => &mut exclusions.below_fidelity_floor,
        Exclusion::PropensityNotExecutedPolicy => &mut exclusions.propensity_not_executed_policy,
        Exclusion::Pinned => &mut exclusions.pinned,
        Exclusion::UnapprovedPrincipal => &mut exclusions.unapproved_principal,
        Exclusion::ZeroExecutedPropensity => &mut exclusions.zero_executed_propensity,
    };
    *slot += 1;
}

/// Admit the items of `dataset` that may be used under `rules`.
pub fn admit<'a>(dataset: &'a LabelledDataset, rules: &AdmissionRules) -> Admitted<'a> {
    let mut exclusions = LabelExclusions::default();
    let mut items = Vec::new();
    for item in &dataset.items {
        match exclusion(item, rules) {
            Some(reason) => count(&mut exclusions, reason),
            None => items.push(item),
        }
    }
    Admitted { items, exclusions }
}
