//! The policy's objective as integer levels over the legal remainder.
//!
//! `Uncovered` is a HARD row in this engine (every required capability is a
//! covering constraint), so it contributes no level: an assembly that leaves a
//! requirement uncovered is not a worse answer, it is no answer. Every other
//! level is an exact integer term per variable, and an undeclared fact is
//! `Coefficient::Unknown` -- ranked in its own tier below every known value,
//! never read as zero (§7.3).

use eg_types::decision::{DecisionErrorCode, DecisionInputs, ObjectiveLevelKind, ObjectiveOrder};
use eg_types::solve::{Coefficient, ObjectiveLevelSpec, ObjectiveTerm, VarId};

use super::facts::{self, Declared};
use super::AssembleError;
use crate::solve::model::MAX_ABS_COEFFICIENT;

/// One level's value for one candidate.
fn level_value(
    kind: ObjectiveLevelKind,
    candidate: &eg_types::decision::CandidateFacts,
    currency: Option<&str>,
) -> Declared {
    match kind {
        ObjectiveLevelKind::Uncovered => Declared::None,
        ObjectiveLevelKind::Components => Declared::Known(1),
        ObjectiveLevelKind::DeclaredCost => facts::per_call_cost(candidate, currency),
        ObjectiveLevelKind::DeclaredP95Latency => facts::p95_latency(candidate),
    }
}

fn coefficient(value: Declared) -> Option<Coefficient> {
    match value {
        Declared::Known(0) | Declared::None => None,
        Declared::Known(value) => Some(Coefficient::Known(value)),
        Declared::Unknown => Some(Coefficient::Unknown),
    }
}

pub(super) fn levels(
    inputs: &DecisionInputs,
    vars: &[usize],
    currency: Option<&str>,
) -> Result<Vec<ObjectiveLevelSpec>, AssembleError> {
    let candidates = inputs.candidates.as_slice();
    match &inputs.policy.objective {
        ObjectiveOrder::Lexicographic { levels } => Ok(levels
            .iter()
            .filter(|kind| **kind != ObjectiveLevelKind::Uncovered)
            .map(|&kind| ObjectiveLevelSpec {
                label: level_label(kind).to_string(),
                terms: terms(vars, |index| {
                    coefficient(level_value(kind, &candidates[index], currency))
                }),
            })
            .collect()),
        ObjectiveOrder::Weighted { weights } => {
            let mut overflow = false;
            let terms = terms(vars, |index| {
                let weighted = weighted_value(weights.as_slice(), &candidates[index], currency);
                overflow |= weighted.is_err();
                weighted.ok().flatten()
            });
            if overflow {
                return Err(AssembleError::new(
                    DecisionErrorCode::AssemblyModelInvalid,
                    "a weighted objective coefficient exceeds the solver's coefficient range",
                ));
            }
            Ok(vec![ObjectiveLevelSpec {
                label: "weighted".to_string(),
                terms,
            }])
        }
    }
}

fn terms(
    vars: &[usize],
    mut value: impl FnMut(usize) -> Option<Coefficient>,
) -> Vec<ObjectiveTerm> {
    vars.iter()
        .enumerate()
        .filter_map(|(position, &index)| {
            value(index).map(|coefficient| ObjectiveTerm {
                var: VarId(position as u32),
                coefficient,
            })
        })
        .collect()
}

/// `Σ weight · value` for one candidate: unknown when any weighted value is,
/// an error when the exact sum leaves the solver's coefficient range.
fn weighted_value(
    weights: &[eg_types::decision::WeightedLevel],
    candidate: &eg_types::decision::CandidateFacts,
    currency: Option<&str>,
) -> Result<Option<Coefficient>, ()> {
    let mut total: i64 = 0;
    for level in weights.iter().filter(|level| level.weight > 0) {
        match level_value(level.level, candidate, currency) {
            Declared::None => {}
            Declared::Unknown => return Ok(Some(Coefficient::Unknown)),
            Declared::Known(value) => {
                let weight = i64::try_from(level.weight).map_err(|_| ())?;
                let term = value.checked_mul(weight).ok_or(())?;
                total = total.checked_add(term).ok_or(())?;
            }
        }
    }
    if total > MAX_ABS_COEFFICIENT {
        return Err(());
    }
    Ok(coefficient(Declared::Known(total)))
}

fn level_label(kind: ObjectiveLevelKind) -> &'static str {
    match kind {
        ObjectiveLevelKind::Uncovered => "uncovered",
        ObjectiveLevelKind::Components => "components",
        ObjectiveLevelKind::DeclaredCost => "declared_cost",
        ObjectiveLevelKind::DeclaredP95Latency => "declared_p95_latency",
    }
}
