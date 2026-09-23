//! Template enumeration (DECIDE-LAYER-DESIGN §7.3 "Topology").
//!
//! Only operator-published graph templates are enumerated -- at most eight,
//! each with at most six slots (its `Agent` nodes); nothing generative. Every
//! template is decided on its own with the same model, solver, certificate
//! and no-good loop as the one-agent graph, and the winner is the template
//! whose optimal objective is lexicographically smallest, level by level
//! (unknown tier first, then the known value). Ties go to the template the
//! request listed first. Each losing template is recorded as a why-not entry
//! carrying its own optimum, or the fact that it had no feasible answer, so
//! the record says why this topology and not another.
//!
//! Only the winner's certificate is stored. That is enough: a commit re-runs
//! the whole enumeration from the stored inputs and compares bytes, so every
//! losing template's optimum is re-derived rather than trusted.

use std::cmp::Ordering;

use eg_types::decision::{AbstainReason, DecisionOutcome, Violation, WhyNot};
use eg_types::solve::{LevelValue, ObjectiveValue};

use super::conclude::{decide_one, Decided, Ladder};
use super::AssembleError;

/// Most why-not entries one record carries in total.
const MAX_WHY_NOT: usize = 64;

/// A template's optimal objective, when it produced an answer.
fn optimum(decided: &Decided) -> Option<ObjectiveValue> {
    let DecisionOutcome::Solved { certificate, .. } = &decided.outcome else {
        return None;
    };
    certificate
        .incumbent
        .as_ref()
        .map(|incumbent| incumbent.objective.clone())
}

fn level_key(level: &LevelValue) -> (u32, i128) {
    (level.unknown_selected, level.known.get())
}

/// Lexicographic comparison of two objectives over the same policy levels.
fn compare(left: &ObjectiveValue, right: &ObjectiveValue) -> Ordering {
    left.levels
        .iter()
        .map(level_key)
        .cmp(right.levels.iter().map(level_key))
}

fn why_not_template(graph_id: &str, objective: Option<ObjectiveValue>) -> WhyNot {
    let violation = objective
        .is_none()
        .then_some(Violation::InfeasibleWhenForced);
    WhyNot {
        component_id: graph_id.to_string(),
        slot: "template".to_string(),
        forced_objective: objective,
        violation,
    }
}

fn reasons_of(decided: &Decided) -> Vec<AbstainReason> {
    match &decided.outcome {
        DecisionOutcome::Abstained { reasons } => reasons.as_slice().to_vec(),
        DecisionOutcome::Solved { .. } => Vec::new(),
    }
}

/// Decide every template and keep the best answer.
pub(super) fn enumerate(ladder: &Ladder<'_>) -> Result<Decided, AssembleError> {
    let mut outcomes: Vec<(usize, Decided)> = Vec::new();
    for (index, template) in ladder.inputs.templates.iter().enumerate() {
        outcomes.push((index, decide_one(ladder, Some(template))?));
    }
    let winner = outcomes
        .iter()
        .filter_map(|(index, decided)| optimum(decided).map(|objective| (*index, objective)))
        .min_by(|a, b| compare(&a.1, &b.1).then(a.0.cmp(&b.0)))
        .map(|(index, _)| index);
    let Some(winner) = winner else {
        return Ok(merged_abstention(outcomes));
    };
    let templates = ladder.inputs.templates.as_slice();
    let mut losers = Vec::new();
    let mut chosen = None;
    for (index, decided) in outcomes {
        if index == winner {
            chosen = Some(decided);
        } else {
            losers.push(why_not_template(
                &templates[index].graph_id,
                optimum(&decided),
            ));
        }
    }
    let mut decided = chosen.expect("the winner is one of the outcomes");
    decided
        .why_not
        .truncate(MAX_WHY_NOT.saturating_sub(losers.len()));
    decided.why_not.extend(losers);
    Ok(decided)
}

/// No template produced an answer: abstain with every template's reasons, the
/// first template's premises and derivations standing for the shared inputs.
fn merged_abstention(outcomes: Vec<(usize, Decided)>) -> Decided {
    let mut reasons: Vec<AbstainReason> = Vec::new();
    for (_, decided) in &outcomes {
        for reason in reasons_of(decided) {
            if !reasons.contains(&reason) {
                reasons.push(reason);
            }
        }
    }
    reasons.truncate(64);
    let (_, mut first) = outcomes
        .into_iter()
        .next()
        .expect("a template request names at least one template");
    first.outcome = DecisionOutcome::Abstained {
        reasons: eg_types::contract::BoundedVec::new(reasons).expect("truncated to the bound"),
    };
    first
}
