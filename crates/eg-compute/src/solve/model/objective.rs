//! Exact scalarisation of the lexicographic objective.
//!
//! Each objective level becomes one or two *sublevels*: when the level holds
//! any `Unknown` coefficient, a first sublevel counts the selected
//! unknown-cost variables (the unknown tier), and a second sums the known
//! coefficients. A sublevel's value lies in an interval of width
//! `span = Σ|c| + 1`, so giving sublevel `s` the multiplier
//! `Π_{t after s} span_t` makes the weighted sum order assignments exactly
//! lexicographically: any change in an earlier sublevel outweighs every
//! possible change in all later ones together. The arithmetic is checked
//! `i128`; a model whose multipliers do not fit is refused.

use serde::{Deserialize, Serialize};

use super::error::ModelError;
use super::spec::{Coefficient, ObjectiveLevelSpec};
use crate::solve::scalar::Scalar;

/// Largest total `Σ|w|` of the scalar objective. Leaves headroom in `i128`
/// for the bound denominator and dual multipliers.
pub const MAX_SCALAR_MAGNITUDE: i128 = 1 << 100;

/// Value of one objective level under an assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LevelValue {
    /// Sum of the known coefficients of the selected variables.
    pub known: Scalar,
    /// How many selected variables have an unknown coefficient at this level.
    pub unknown_selected: u32,
}

/// Every level's value plus the exact scalarised objective.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveValue {
    pub levels: Vec<LevelValue>,
    pub scalar: Scalar,
}

/// Scalar objective weight of every variable.
pub(super) fn scalar_weights(
    levels: &[ObjectiveLevelSpec],
    variables: usize,
) -> Result<Vec<i128>, ModelError> {
    let mut weights = vec![0i128; variables];
    let mut multiplier: i128 = 1;
    for sublevel in sublevels(levels).iter().rev() {
        let mut span: i128 = 1;
        for &(var, coefficient) in sublevel {
            let scaled = multiplier.checked_mul(i128::from(coefficient));
            let slot = &mut weights[var];
            *slot = scaled
                .and_then(|value| slot.checked_add(value))
                .ok_or(ModelError::ObjectiveRangeOverflow)?;
            span = span
                .checked_add(i128::from(coefficient.unsigned_abs()))
                .ok_or(ModelError::ObjectiveRangeOverflow)?;
        }
        multiplier = multiplier
            .checked_mul(span)
            .ok_or(ModelError::ObjectiveRangeOverflow)?;
    }
    check_magnitude(&weights)?;
    Ok(weights)
}

fn check_magnitude(weights: &[i128]) -> Result<(), ModelError> {
    let total = weights
        .iter()
        .try_fold(0i128, |sum, weight| sum.checked_add(weight.checked_abs()?));
    match total {
        Some(value) if value <= MAX_SCALAR_MAGNITUDE => Ok(()),
        _ => Err(ModelError::ObjectiveRangeOverflow),
    }
}

/// The sublevels of `levels`, most significant first, as `(var, coefficient)`.
fn sublevels(levels: &[ObjectiveLevelSpec]) -> Vec<Vec<(usize, i64)>> {
    let mut out = Vec::new();
    for level in levels {
        let unknown: Vec<(usize, i64)> = level
            .terms
            .iter()
            .filter(|term| term.coefficient == Coefficient::Unknown)
            .map(|term| (term.var.index(), 1))
            .collect();
        if !unknown.is_empty() {
            out.push(unknown);
        }
        out.push(
            level
                .terms
                .iter()
                .filter_map(|term| match term.coefficient {
                    Coefficient::Known(value) => Some((term.var.index(), value)),
                    Coefficient::Unknown => None,
                })
                .collect(),
        );
    }
    out
}

/// Evaluate every level and the scalar objective of a full assignment.
/// `selected` must have one entry per variable (checked by the caller).
pub(super) fn evaluate(
    levels: &[ObjectiveLevelSpec],
    weights: &[i128],
    selected: &[bool],
) -> ObjectiveValue {
    let level_values = levels
        .iter()
        .map(|level| level_value(level, selected))
        .collect();
    let scalar = weights
        .iter()
        .zip(selected)
        .filter(|(_, &on)| on)
        .map(|(weight, _)| *weight)
        .sum();
    ObjectiveValue {
        levels: level_values,
        scalar: Scalar::new(scalar),
    }
}

fn level_value(level: &ObjectiveLevelSpec, selected: &[bool]) -> LevelValue {
    let mut known: i128 = 0;
    let mut unknown_selected: u32 = 0;
    for term in level.terms.iter().filter(|term| selected[term.var.index()]) {
        match term.coefficient {
            Coefficient::Known(value) => known += i128::from(value),
            Coefficient::Unknown => unknown_selected += 1,
        }
    }
    LevelValue {
        known: Scalar::new(known),
        unknown_selected,
    }
}
