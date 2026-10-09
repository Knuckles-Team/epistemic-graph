//! EG-UNIFIED-DATA-PLANE-R004 — deterministic schema inference over an attached catalog. This is
//! the `.1` typed-model slice: the bounded, policy-gated column-sampling budget the requirement
//! names, and its refusal of a zero or unbounded budget. Dependency discovery and fixed-seed
//! Leiden grouping are later children.

use std::fmt;

/// The ceiling this requirement's "bounded" language enforces: no sampling budget may ask for
/// more rows than this, regardless of policy.
pub const MAX_ALLOWED_SAMPLE_ROWS: u32 = 10_000_000;

/// A per-column sampling budget for JSON-shape profiling: bounded row and byte limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnSamplingBudget {
    pub max_rows: u32,
    pub max_bytes: u64,
}

/// Why a declared sampling budget was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvalidSamplingBudget {
    ZeroRows,
    ZeroBytes,
    RowsOverCeiling { requested: u32 },
}

impl fmt::Display for InvalidSamplingBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroRows => write!(f, "sampling budget names zero rows"),
            Self::ZeroBytes => write!(f, "sampling budget names zero bytes"),
            Self::RowsOverCeiling { requested } => write!(
                f,
                "sampling budget requests {requested} rows, over the {MAX_ALLOWED_SAMPLE_ROWS} ceiling"
            ),
        }
    }
}

impl std::error::Error for InvalidSamplingBudget {}

impl ColumnSamplingBudget {
    /// Refuses an unbounded-in-effect budget (zero rows, zero bytes, or over the fixed ceiling):
    /// a "bounded, policy-gated" sampling budget cannot be zero or unlimited.
    pub fn validate(&self) -> Result<(), InvalidSamplingBudget> {
        if self.max_rows == 0 {
            return Err(InvalidSamplingBudget::ZeroRows);
        }
        if self.max_bytes == 0 {
            return Err(InvalidSamplingBudget::ZeroBytes);
        }
        if self.max_rows > MAX_ALLOWED_SAMPLE_ROWS {
            return Err(InvalidSamplingBudget::RowsOverCeiling {
                requested: self.max_rows,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_budget_within_bounds_validates() {
        let budget = ColumnSamplingBudget {
            max_rows: 1_000,
            max_bytes: 1_048_576,
        };
        budget.validate().unwrap();
    }

    #[test]
    fn a_zero_row_budget_is_refused() {
        let budget = ColumnSamplingBudget {
            max_rows: 0,
            max_bytes: 1_048_576,
        };
        assert_eq!(budget.validate().unwrap_err(), InvalidSamplingBudget::ZeroRows);
    }

    #[test]
    fn a_zero_byte_budget_is_refused() {
        let budget = ColumnSamplingBudget {
            max_rows: 1_000,
            max_bytes: 0,
        };
        assert_eq!(budget.validate().unwrap_err(), InvalidSamplingBudget::ZeroBytes);
    }

    #[test]
    fn a_budget_over_the_ceiling_is_refused() {
        let budget = ColumnSamplingBudget {
            max_rows: MAX_ALLOWED_SAMPLE_ROWS + 1,
            max_bytes: 1_048_576,
        };
        assert_eq!(
            budget.validate().unwrap_err(),
            InvalidSamplingBudget::RowsOverCeiling {
                requested: MAX_ALLOWED_SAMPLE_ROWS + 1
            }
        );
    }
}
