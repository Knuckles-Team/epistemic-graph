//! Exact evaluation of a Lagrangian dual at a partial assignment.
//!
//! Row by row, the multipliers are applied to the right-hand sides and
//! subtracted from the scaled objective, giving every variable's reduced cost
//! `ρ_j`; the variables then contribute `ρ_j` when fixed to 1, nothing when
//! fixed to 0 and `min(0, ρ_j)` when free. Every operation is checked.

use super::error::VerifyError;
use crate::solve::certificate::{LagrangeDual, MAX_BOUND_DENOMINATOR, MAX_MULTIPLIER};
use crate::solve::model::{Model, Relation};

/// Which objective the dual is evaluated against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Target {
    /// The model's scalar objective (a lower-bound claim).
    Objective,
    /// The zero objective (an infeasibility claim).
    Feasibility,
}

/// `D·L(λ, path)` for the dual's denominator `D`.
pub(super) fn scaled_lagrangian(
    model: &Model,
    dual: &LagrangeDual,
    path: &[Option<bool>],
    (target, node): (Target, usize),
) -> Result<i128, VerifyError> {
    check_shape(model, dual, node)?;
    let overflow = VerifyError::ArithmeticOverflow { node };
    let scale = i128::from(dual.denominator);
    let mut reduced: Vec<i128> = match target {
        Target::Objective => model
            .weights()
            .iter()
            .map(|w| w.checked_mul(scale))
            .collect::<Option<_>>(),
        Target::Feasibility => Some(vec![0; model.variable_count()]),
    }
    .ok_or(overflow)?;
    let mut scaled: i128 = 0;
    for entry in &dual.entries {
        let row = &model.rows()[entry.row.index()];
        let multiplier = entry.numerator.get();
        let applied = multiplier
            .checked_mul(i128::from(row.rhs()))
            .ok_or(overflow)?;
        scaled = scaled.checked_add(applied).ok_or(overflow)?;
        for term in row.terms() {
            let slot = &mut reduced[term.var.index()];
            let delta = multiplier
                .checked_mul(i128::from(term.coefficient))
                .ok_or(overflow)?;
            *slot = slot.checked_sub(delta).ok_or(overflow)?;
        }
    }
    reduced
        .iter()
        .zip(path)
        .map(|(&cost, assigned)| match assigned {
            Some(true) => cost,
            Some(false) => 0,
            None => cost.min(0),
        })
        .try_fold(scaled, i128::checked_add)
        .ok_or(overflow)
}

/// The integer lower bound `⌈scaled / D⌉` a bound dual certifies.
pub(super) fn integer_bound(scaled: i128, denominator: u64) -> i128 {
    let divisor = i128::from(denominator);
    let quotient = scaled.div_euclid(divisor);
    if scaled.rem_euclid(divisor) == 0 {
        quotient
    } else {
        quotient + 1
    }
}

/// Denominator in range; entries strictly ascending by row, in range, with
/// a sign the row's relation allows and a magnitude within the cap.
fn check_shape(model: &Model, dual: &LagrangeDual, node: usize) -> Result<(), VerifyError> {
    let malformed = VerifyError::DualMalformed { node };
    if dual.denominator == 0 || dual.denominator > MAX_BOUND_DENOMINATOR {
        return Err(malformed);
    }
    let ascending = dual
        .entries
        .windows(2)
        .all(|pair| pair[0].row < pair[1].row);
    if !ascending {
        return Err(malformed);
    }
    for entry in &dual.entries {
        let row = model
            .rows()
            .get(entry.row.index())
            .ok_or(VerifyError::RowOutOfRange {
                node,
                row: entry.row,
            })?;
        let multiplier = entry.numerator.get();
        let signed = match row.relation() {
            Relation::GreaterEqual => multiplier > 0,
            Relation::LessEqual => multiplier < 0,
            Relation::Equal => multiplier != 0,
        };
        if !signed || multiplier.unsigned_abs() > MAX_MULTIPLIER.unsigned_abs() {
            return Err(malformed);
        }
    }
    Ok(())
}
