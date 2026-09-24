//! Execution budgets (UQL-09): the resource bounds every plan runs under.
//!
//! A plan that would exceed a budget fails with a typed `UQL_BUDGET_EXCEEDED` error that
//! names the budget and the remedy — never a silent truncation, never an unbounded walk.
//! Defaults are deliberately generous for interactive use; a server binds its own via
//! [`crate::exec::PlanCtx::with_budget`].

/// The error-code prefix of every budget refusal.
pub const BUDGET_EXCEEDED: &str = "UQL_BUDGET_EXCEEDED";

/// Resource bounds for one plan execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    /// Most rows the plan may return (checked on the final RowSet).
    pub max_result_rows: usize,
    /// Most nodes one `TRAVERSE` may visit before it is refused (fan-out bound).
    pub max_traversal_visits: usize,
}

impl Budget {
    /// Default result-row bound.
    pub const DEFAULT_MAX_RESULT_ROWS: usize = 100_000;
    /// Default per-traversal visited-node bound.
    pub const DEFAULT_MAX_TRAVERSAL_VISITS: usize = 1_000_000;

    /// Refuse a final result larger than the bound.
    pub fn check_result(&self, rows: usize) -> Result<(), String> {
        if rows <= self.max_result_rows {
            return Ok(());
        }
        Err(format!(
            "{BUDGET_EXCEEDED}: the result has {rows} rows, above max_result_rows = {}; \
             add `LIMIT <k>` or narrow the query",
            self.max_result_rows
        ))
    }

    /// Refuse a traversal that has visited more nodes than the bound.
    pub fn check_traversal(&self, visited: usize) -> Result<(), String> {
        if visited <= self.max_traversal_visits {
            return Ok(());
        }
        Err(format!(
            "{BUDGET_EXCEEDED}: a TRAVERSE visited more than max_traversal_visits = {} \
             nodes; lower its hop range, filter the seed set, or add edge predicates",
            self.max_traversal_visits
        ))
    }
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            max_result_rows: Self::DEFAULT_MAX_RESULT_ROWS,
            max_traversal_visits: Self::DEFAULT_MAX_TRAVERSAL_VISITS,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budgets_refuse_with_a_typed_code() {
        let b = Budget {
            max_result_rows: 2,
            max_traversal_visits: 3,
        };
        assert!(b.check_result(2).is_ok());
        assert!(b.check_result(3).unwrap_err().starts_with(BUDGET_EXCEEDED));
        assert!(b
            .check_traversal(4)
            .unwrap_err()
            .contains("max_traversal_visits"));
    }
}
