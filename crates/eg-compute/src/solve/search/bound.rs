//! Node lower bound by integer dual ascent on the Lagrangian relaxation.
//!
//! Starting from the scaled objective `D·w` as reduced costs, the rows that
//! still demand activity (`≥` and `=` rows with a positive residual) are taken
//! in order of fewest positive free terms, twice:
//!
//! 1. *fair share* — a row's multiplier rises to the smallest
//!    `ρ_j / (a_j · k_j)` over its positive free terms, where `k_j` counts the
//!    demand rows not yet visited that could still use `j`. On a covering model
//!    this spreads each column's cost evenly over the elements it covers, which
//!    is exactly the optimal dual when the optimum is a cost-proportional
//!    partition;
//! 2. *greedy* — each row then takes whatever reduced cost is left
//!    (`min ρ_j / a_j`).
//!
//! Reduced costs of positive terms never go negative, so each raise adds
//! `μ·residual` to the bound. On a pure covering model this is the dual-feasible
//! covering bound `Σ y_e` with `Σ_{e∈S} y_e ≤ c_S`. Multipliers are capped at
//! [`MAX_MULTIPLIER`] so an independent evaluation of the dual cannot overflow
//! `i128`; a capped or skipped raise only weakens the bound.

use super::state::{row_id, SearchState};
use crate::solve::certificate::{DualEntry, LagrangeDual, MAX_MULTIPLIER};
use crate::solve::model::Relation;
use crate::solve::scalar::Scalar;

/// A dual and the bound value it certifies at the node that produced it.
pub(super) struct NodeBound {
    pub(super) dual: LagrangeDual,
    pub(super) value: i128,
}

/// Mutable ascent state: reduced costs, the running scaled bound and each
/// demand row's accumulated multiplier.
struct Ascent {
    reduced: Vec<i128>,
    total: i128,
    multipliers: Vec<i128>,
}

/// Dual ascent at the current node of `state`.
pub(super) fn dual_ascent(state: &SearchState<'_>, denominator: u64) -> NodeBound {
    let scale = i128::from(denominator);
    let model = state.model();
    let reduced: Vec<i128> = model.weights().iter().map(|w| w * scale).collect();
    let total = (0..model.variable_count())
        .filter(|&var| state.value(var) == Some(true))
        .map(|var| reduced[var])
        .sum();
    let rows = demand_rows(state);
    let mut ascent = Ascent {
        reduced,
        total,
        multipliers: vec![0; rows.len()],
    };
    let mut shares = share_counts(state, &rows, model.variable_count());
    for (position, &row) in rows.iter().enumerate() {
        ascent.raise(state, (position, row), Some(&shares));
        for (var, _) in state.free_terms(row).filter(|&(_, c)| c > 0) {
            shares[var] -= 1;
        }
    }
    for (position, &row) in rows.iter().enumerate() {
        ascent.raise(state, (position, row), None);
    }
    ascent.finish(state, &rows, denominator)
}

/// `≥`/`=` rows with positive residual demand, fewest positive free terms first.
fn demand_rows(state: &SearchState<'_>) -> Vec<usize> {
    let model = state.model();
    let mut keyed: Vec<(usize, usize)> = (0..model.rows().len())
        .filter(|&row| model.rows()[row].relation() != Relation::LessEqual)
        .filter(|&row| residual(state, row) > 0)
        .map(|row| (state.free_terms(row).filter(|&(_, c)| c > 0).count(), row))
        .collect();
    keyed.sort_unstable();
    keyed.into_iter().map(|(_, row)| row).collect()
}

fn residual(state: &SearchState<'_>, row: usize) -> i128 {
    i128::from(state.model().rows()[row].rhs()) - state.fixed_activity(row)
}

/// For every variable, how many of `rows` hold it as a positive free term.
fn share_counts(state: &SearchState<'_>, rows: &[usize], variables: usize) -> Vec<i128> {
    let mut shares = vec![0i128; variables];
    for &row in rows {
        for (var, _) in state.free_terms(row).filter(|&(_, c)| c > 0) {
            shares[var] += 1;
        }
    }
    shares
}

impl Ascent {
    /// Raise one row's multiplier. With `shares`, each positive term's reduced
    /// cost is divided by its remaining share count as well as its coefficient.
    fn raise(
        &mut self,
        state: &SearchState<'_>,
        (position, row): (usize, usize),
        shares: Option<&[i128]>,
    ) {
        let terms: Vec<(usize, i128)> = state
            .free_terms(row)
            .map(|(var, coefficient)| (var, i128::from(coefficient)))
            .collect();
        let limit = terms
            .iter()
            .filter(|&&(_, coefficient)| coefficient > 0)
            .map(|&(var, coefficient)| {
                let divisor = coefficient * shares.map_or(1, |counts| counts[var].max(1));
                self.reduced[var].div_euclid(divisor)
            })
            .min();
        let current = self.multipliers[position];
        let Some(step) = limit.map(|limit| limit.min(MAX_MULTIPLIER - current)) else {
            return;
        };
        if step > 0 {
            self.apply(&terms, (position, step), residual(state, row));
        }
    }

    /// Commit a raise of `step` when every product and sum fits.
    fn apply(&mut self, terms: &[(usize, i128)], (position, step): (usize, i128), residual: i128) {
        let updated: Option<Vec<i128>> = terms
            .iter()
            .map(|&(var, coefficient)| {
                self.reduced[var].checked_sub(step.checked_mul(coefficient)?)
            })
            .collect();
        let total = step
            .checked_mul(residual)
            .and_then(|gain| gain.checked_add(self.total));
        let (Some(updated), Some(total)) = (updated, total) else {
            return;
        };
        for (&(var, _), value) in terms.iter().zip(updated) {
            self.reduced[var] = value;
        }
        self.total = total;
        self.multipliers[position] += step;
    }

    fn finish(self, state: &SearchState<'_>, rows: &[usize], denominator: u64) -> NodeBound {
        let free_part: i128 = (0..self.reduced.len())
            .filter(|&var| state.value(var).is_none())
            .map(|var| self.reduced[var].min(0))
            .sum();
        let mut entries: Vec<DualEntry> = rows
            .iter()
            .zip(&self.multipliers)
            .filter(|(_, &multiplier)| multiplier > 0)
            .map(|(&row, &multiplier)| DualEntry {
                row: row_id(row),
                numerator: Scalar::new(multiplier),
            })
            .collect();
        entries.sort_by_key(|entry| entry.row);
        NodeBound {
            dual: LagrangeDual {
                denominator,
                entries,
            },
            value: ceil_div(self.total + free_part, i128::from(denominator)),
        }
    }
}

/// `⌈numerator / denominator⌉` for a positive denominator.
fn ceil_div(numerator: i128, denominator: i128) -> i128 {
    -((-numerator).div_euclid(denominator))
}
