//! Row checks over explicit assignments, recomputed term by term.

use super::error::VerifyError;
use crate::solve::model::{Model, Relation, Row, RowId};

/// Check a full selection against every row and the claimed objective.
pub(super) fn check_selection(model: &Model, selected: &[bool]) -> Result<(), VerifyError> {
    if selected.len() != model.variable_count() {
        let (expected, found) = (model.variable_count(), selected.len());
        return Err(VerifyError::AssignmentLength { expected, found });
    }
    for (index, row) in model.rows().iter().enumerate() {
        let lhs: i128 = row
            .terms()
            .iter()
            .filter(|term| selected[term.var.index()])
            .map(|term| i128::from(term.coefficient))
            .sum();
        if !holds(row.relation(), lhs, i128::from(row.rhs())) {
            return Err(VerifyError::RowViolated {
                row: RowId(index as u32),
            });
        }
    }
    Ok(())
}

fn holds(relation: Relation, lhs: i128, rhs: i128) -> bool {
    match relation {
        Relation::LessEqual => lhs <= rhs,
        Relation::GreaterEqual => lhs >= rhs,
        Relation::Equal => lhs == rhs,
    }
}

/// `true` when no completion of `path` satisfies `row`.
pub(super) fn unsatisfiable(row: &Row, path: &[Option<bool>]) -> bool {
    let mut reachable_min: i128 = 0;
    let mut reachable_max: i128 = 0;
    for term in row.terms() {
        let coefficient = i128::from(term.coefficient);
        match path[term.var.index()] {
            Some(true) => {
                reachable_min += coefficient;
                reachable_max += coefficient;
            }
            Some(false) => {}
            None if coefficient < 0 => reachable_min += coefficient,
            None => reachable_max += coefficient,
        }
    }
    let rhs = i128::from(row.rhs());
    match row.relation() {
        Relation::LessEqual => reachable_min > rhs,
        Relation::GreaterEqual => reachable_max < rhs,
        Relation::Equal => reachable_min > rhs || reachable_max < rhs,
    }
}
