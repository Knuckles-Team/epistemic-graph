//! Greedy incumbent with propagation as repair.
//!
//! While some `≥`/`=` row still lacks activity, select the free variable that
//! helps the most such rows per unit of weight (the H(m)-approximate greedy
//! rule on the covering part; ties by index). Then finish with the branching
//! rule's helpful values. Propagation runs after every fix and repairs what it
//! can; a dead end simply yields no incumbent, so the search starts without one.

use super::branch::{choose, completion, Choice};
use super::propagate::{fix_and_queue, propagate, RowQueue};
use super::state::SearchState;
use crate::solve::model::{Model, Relation};

/// A feasible selection and its scalar objective, if the greedy dive finds one.
pub(super) fn greedy(model: &Model) -> Option<(Vec<bool>, i128)> {
    let mut state = SearchState::new(model);
    let mut queue = RowQueue::new(model.rows().len());
    let mut ignore = |_: usize, _: bool, _: usize| {};
    queue.push_all();
    propagate(&mut state, &mut queue, &mut ignore).ok()?;
    while let Some(var) = best_cover(&state) {
        fix_and_queue(&mut state, &mut queue, var, true);
        propagate(&mut state, &mut queue, &mut ignore).ok()?;
    }
    loop {
        let Choice::Branch { var, first } = choose(&state) else {
            return Some(completion(&state));
        };
        fix_and_queue(&mut state, &mut queue, var, first);
        propagate(&mut state, &mut queue, &mut ignore).ok()?;
    }
}

/// The free variable helping the most unmet demand rows per unit of weight.
fn best_cover(state: &SearchState<'_>) -> Option<usize> {
    let model = state.model();
    let mut helps = vec![0i128; model.variable_count()];
    for (row, spec) in model.rows().iter().enumerate() {
        let (low, _) = state.activity(row);
        if spec.relation() == Relation::LessEqual || low >= i128::from(spec.rhs()) {
            continue;
        }
        for (var, coefficient) in state.free_terms(row) {
            helps[var] += i128::from(coefficient > 0);
        }
    }
    let weights = model.weights();
    (0..helps.len())
        .filter(|&var| helps[var] > 0)
        .min_by(|&a, &b| {
            let left = helps[b] * weights[a].max(1);
            let right = helps[a] * weights[b].max(1);
            left.cmp(&right).then(a.cmp(&b))
        })
}
