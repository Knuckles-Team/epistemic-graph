//! Interval propagation over rows touched by recent fixes.
//!
//! A row whose attainable activity interval excludes its right-hand side is
//! infeasible. A free variable whose opposite value would make a row
//! infeasible is forced; every forcing is reported so the certificate can
//! carry it as a checkable `Forced` node.

use super::state::SearchState;
use crate::solve::model::Relation;

/// Rows waiting to be re-examined, without repeats.
pub(super) struct RowQueue {
    rows: Vec<usize>,
    queued: Vec<bool>,
}

impl RowQueue {
    pub(super) fn new(rows: usize) -> Self {
        Self {
            rows: Vec::new(),
            queued: vec![false; rows],
        }
    }

    pub(super) fn push(&mut self, row: usize) {
        if !self.queued[row] {
            self.queued[row] = true;
            self.rows.push(row);
        }
    }

    pub(super) fn push_all(&mut self) {
        for row in 0..self.queued.len() {
            self.push(row);
        }
    }

    fn pop(&mut self) -> Option<usize> {
        let row = self.rows.pop()?;
        self.queued[row] = false;
        Some(row)
    }

    fn clear(&mut self) {
        while self.pop().is_some() {}
    }
}

/// Fix `var` and queue the rows it appears in.
pub(super) fn fix_and_queue(
    state: &mut SearchState<'_>,
    queue: &mut RowQueue,
    var: usize,
    value: bool,
) {
    state.fix(var, value);
    for &(row, _) in state.column(var) {
        queue.push(row);
    }
}

/// `true` when no activity in `[low, high]` satisfies `relation rhs`.
pub(super) fn excludes(relation: Relation, rhs: i128, low: i128, high: i128) -> bool {
    match relation {
        Relation::GreaterEqual => high < rhs,
        Relation::LessEqual => low > rhs,
        Relation::Equal => high < rhs || low > rhs,
    }
}

/// `true` when every activity in `[low, high]` satisfies `relation rhs`.
pub(super) fn guarantees(relation: Relation, rhs: i128, low: i128, high: i128) -> bool {
    match relation {
        Relation::GreaterEqual => low >= rhs,
        Relation::LessEqual => high <= rhs,
        Relation::Equal => low == rhs && high == rhs,
    }
}

/// Propagate until the queue is empty. Returns the first infeasible row, or
/// reports each forced `(var, value, row)` through `forced`.
pub(super) fn propagate(
    state: &mut SearchState<'_>,
    queue: &mut RowQueue,
    forced: &mut dyn FnMut(usize, bool, usize),
) -> Result<(), usize> {
    while let Some(row) = queue.pop() {
        if let Err(row) = propagate_row(state, queue, row, forced) {
            queue.clear();
            return Err(row);
        }
    }
    Ok(())
}

fn propagate_row(
    state: &mut SearchState<'_>,
    queue: &mut RowQueue,
    row: usize,
    forced: &mut dyn FnMut(usize, bool, usize),
) -> Result<(), usize> {
    let relation = state.model().rows()[row].relation();
    let rhs = i128::from(state.model().rows()[row].rhs());
    let (low, high) = state.activity(row);
    if excludes(relation, rhs, low, high) {
        return Err(row);
    }
    let candidates: Vec<(usize, i64)> = state.free_terms(row).collect();
    for (var, coefficient) in candidates {
        let (low, high) = state.activity(row);
        if let Some(value) = forced_value(relation, rhs, (low, high), coefficient) {
            forced(var, value, row);
            fix_and_queue(state, queue, var, value);
        }
    }
    let (low, high) = state.activity(row);
    if excludes(relation, rhs, low, high) {
        return Err(row);
    }
    Ok(())
}

/// The value a free term must take, when its other value makes the row
/// infeasible.
fn forced_value(
    relation: Relation,
    rhs: i128,
    range: (i128, i128),
    coefficient: i64,
) -> Option<bool> {
    [true, false].into_iter().find(|&value| {
        let (low, high) = narrowed(range, coefficient, !value);
        excludes(relation, rhs, low, high)
    })
}

/// The activity interval after fixing a free term with `coefficient` to `value`.
pub(super) fn narrowed(range: (i128, i128), coefficient: i64, value: bool) -> (i128, i128) {
    let coefficient = i128::from(coefficient);
    let chosen = if value { coefficient } else { 0 };
    (
        range.0 - coefficient.min(0) + chosen,
        range.1 - coefficient.max(0) + chosen,
    )
}
