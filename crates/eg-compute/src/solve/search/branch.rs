//! Branching choice and the unconstrained completion.
//!
//! The branching row is the most constrained row not yet guaranteed to hold
//! (fewest free terms, then lowest index). Within it the free term whose
//! helpful value is cheapest per unit of coefficient is branched on, helpful
//! value first; ties break by variable index. When every row is guaranteed,
//! the node's optimum is the completion that selects exactly the free
//! variables with negative weight.

use std::cmp::Ordering;

use super::propagate::guarantees;
use super::state::SearchState;
use crate::solve::model::Relation;

/// What to do at a propagated, feasible node.
pub(super) enum Choice {
    /// Every row holds for any completion: close with the best completion.
    Complete,
    /// Branch on `var`, exploring `first` before its negation.
    Branch { var: usize, first: bool },
}

pub(super) fn choose(state: &SearchState<'_>) -> Choice {
    let Some(row) = most_constrained_row(state) else {
        return Choice::Complete;
    };
    let (low, _) = state.activity(row);
    let relation = state.model().rows()[row].relation();
    let raise =
        relation != Relation::LessEqual && low < i128::from(state.model().rows()[row].rhs());
    let weights = state.model().weights();
    let best = state
        .free_terms(row)
        .map(|(var, coefficient)| {
            let helpful = (coefficient > 0) == raise;
            let cost = if helpful { weights[var] } else { -weights[var] };
            (var, helpful, cost, i128::from(coefficient.unsigned_abs()))
        })
        .min_by(|a, b| cheaper(a.2, a.3, b.2, b.3).then(a.0.cmp(&b.0)));
    match best {
        Some((var, helpful, _, _)) => Choice::Branch {
            var,
            first: helpful,
        },
        None => Choice::Complete,
    }
}

fn most_constrained_row(state: &SearchState<'_>) -> Option<usize> {
    let rows = state.model().rows();
    (0..rows.len())
        .filter(|&row| {
            let (low, high) = state.activity(row);
            !guarantees(rows[row].relation(), i128::from(rows[row].rhs()), low, high)
        })
        .min_by_key(|&row| (state.free_count(row), row))
}

/// Order `cost_a / size_a` against `cost_b / size_b` exactly; sizes are
/// positive. Falls back to equality when a cross product overflows.
fn cheaper(cost_a: i128, size_a: i128, cost_b: i128, size_b: i128) -> Ordering {
    match (cost_a.checked_mul(size_b), cost_b.checked_mul(size_a)) {
        (Some(left), Some(right)) => left.cmp(&right),
        _ => Ordering::Equal,
    }
}

/// The best completion's value selection and its scalar objective.
pub(super) fn completion(state: &SearchState<'_>) -> (Vec<bool>, i128) {
    let weights = state.model().weights();
    let selected: Vec<bool> = (0..weights.len())
        .map(|var| state.value(var).unwrap_or(weights[var] < 0))
        .collect();
    let value = state
        .model()
        .objective_value(&selected)
        .expect("a completion has one entry per variable")
        .scalar
        .get();
    (selected, value)
}
