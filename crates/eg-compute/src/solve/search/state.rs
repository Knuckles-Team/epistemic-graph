//! The partial assignment of a search node, with per-row activity caches.

use crate::solve::model::{Model, RowId, VarId};

/// The partial assignment plus, per row, the fixed activity and the extreme
/// contributions still available from free variables. Fixes are undone in
/// reverse through the trail.
pub(super) struct SearchState<'m> {
    model: &'m Model,
    /// Per variable: `(row, coefficient)` for every row it appears in.
    columns: Vec<Vec<(usize, i64)>>,
    value: Vec<Option<bool>>,
    trail: Vec<usize>,
    fixed: Vec<i128>,
    free_positive: Vec<i128>,
    free_negative: Vec<i128>,
    free_count: Vec<i64>,
}

impl<'m> SearchState<'m> {
    pub(super) fn new(model: &'m Model) -> Self {
        let rows = model.rows().len();
        let mut columns = vec![Vec::new(); model.variable_count()];
        let mut free_positive = vec![0i128; rows];
        let mut free_negative = vec![0i128; rows];
        let mut free_count = vec![0i64; rows];
        for (index, row) in model.rows().iter().enumerate() {
            for term in row.terms() {
                columns[term.var.index()].push((index, term.coefficient));
                let coefficient = i128::from(term.coefficient);
                free_positive[index] += coefficient.max(0);
                free_negative[index] += coefficient.min(0);
                free_count[index] += 1;
            }
        }
        Self {
            model,
            columns,
            value: vec![None; model.variable_count()],
            trail: Vec::new(),
            fixed: vec![0; rows],
            free_positive,
            free_negative,
            free_count,
        }
    }

    pub(super) fn model(&self) -> &'m Model {
        self.model
    }

    pub(super) fn value(&self, var: usize) -> Option<bool> {
        self.value[var]
    }

    /// `(row, coefficient)` for every row `var` appears in.
    pub(super) fn column(&self, var: usize) -> &[(usize, i64)] {
        &self.columns[var]
    }

    pub(super) fn trail_len(&self) -> usize {
        self.trail.len()
    }

    /// Fix a free variable.
    pub(super) fn fix(&mut self, var: usize, value: bool) {
        self.value[var] = Some(value);
        self.trail.push(var);
        self.shift(var, value, 1);
    }

    /// Undo fixes until only the first `len` remain.
    pub(super) fn undo_to(&mut self, len: usize) {
        while self.trail.len() > len {
            let Some(var) = self.trail.pop() else { return };
            let value = self.value[var].take() == Some(true);
            self.shift(var, value, -1);
        }
    }

    /// Move `var` out of (`direction = 1`) or back into (`-1`) the free
    /// contributions of its rows.
    fn shift(&mut self, var: usize, value: bool, direction: i128) {
        for position in 0..self.columns[var].len() {
            let (row, coefficient) = self.columns[var][position];
            let coefficient = i128::from(coefficient);
            self.free_positive[row] -= direction * coefficient.max(0);
            self.free_negative[row] -= direction * coefficient.min(0);
            self.free_count[row] -= direction as i64;
            if value {
                self.fixed[row] += direction * coefficient;
            }
        }
    }

    /// Smallest and largest activity row `row` can still reach.
    pub(super) fn activity(&self, row: usize) -> (i128, i128) {
        let fixed = self.fixed[row];
        (
            fixed + self.free_negative[row],
            fixed + self.free_positive[row],
        )
    }

    /// Activity already contributed by fixed variables.
    pub(super) fn fixed_activity(&self, row: usize) -> i128 {
        self.fixed[row]
    }

    pub(super) fn free_count(&self, row: usize) -> i64 {
        self.free_count[row]
    }

    /// Free `(var, coefficient)` terms of `row`.
    pub(super) fn free_terms(&self, row: usize) -> impl Iterator<Item = (usize, i64)> + '_ {
        self.model.rows()[row]
            .terms()
            .iter()
            .map(|term| (term.var.index(), term.coefficient))
            .filter(|&(var, _)| self.value[var].is_none())
    }
}

/// Typed ids for record nodes.
pub(super) fn var_id(var: usize) -> VarId {
    VarId(var as u32)
}

pub(super) fn row_id(row: usize) -> RowId {
    RowId(row as u32)
}
