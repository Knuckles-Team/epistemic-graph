//! The per-table acceleration policy EG-UNIFIED-DATA-PLANE-R010 requires: whether a
//! table's cached virtual graph / columnar replica in `eg-lake` is kept accelerated at
//! all, and if so, the lag objective a restart/failover reconvergence test is measured
//! against. This is the typed-model slice (`.1`): the policy type and its validation,
//! refusing a declaration that the requirement's own verification could never actually
//! check. Wiring a real refresh loop driven by change capture, and the cached virtual
//! graph / columnar replica itself, are later children.

use std::fmt;

/// A declared acceleration policy for one attached table (CONCEPT:EG-317's `eg-lake`
/// tier). `enabled` gates whether the table is accelerated at all; `lag_objective_ms`
/// is the declared staleness bound a restart/failover reconvergence test is measured
/// against when it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccelerationPolicy {
    /// The attached table this policy governs. Must name a real table, so this is
    /// never an orphaned policy nothing can be checked against.
    pub table: String,
    /// Whether the table's accelerated copy is maintained at all. `false` means the
    /// table is served live from the attached source with no cached replica.
    pub enabled: bool,
    /// The declared staleness bound, in milliseconds, the accelerated copy must
    /// reconverge within after a restart or failover. Only meaningful while
    /// `enabled`; a disabled policy carries no such obligation.
    pub lag_objective_ms: u64,
}

impl AccelerationPolicy {
    /// Refuse a policy that the requirement's own verification could never check:
    /// one naming no table, or one that is `enabled` with no declared lag objective
    /// (`lag_objective_ms == 0`) — "reconverges within its declared lag objective"
    /// is vacuous with nothing declared. A disabled policy may carry
    /// `lag_objective_ms == 0`; it is not accelerating, so there is nothing to
    /// measure.
    pub fn validate(&self) -> Result<(), InvalidAccelerationPolicy> {
        if self.table.trim().is_empty() {
            return Err(InvalidAccelerationPolicy::EmptyTableName);
        }
        if self.enabled && self.lag_objective_ms == 0 {
            return Err(InvalidAccelerationPolicy::MissingLagObjective);
        }
        Ok(())
    }
}

/// Why an [`AccelerationPolicy`] declaration was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvalidAccelerationPolicy {
    EmptyTableName,
    MissingLagObjective,
}

impl fmt::Display for InvalidAccelerationPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self {
            Self::EmptyTableName => "names no table",
            Self::MissingLagObjective => {
                "is enabled but declares no lag objective (lag_objective_ms == 0)"
            }
        };
        write!(f, "acceleration policy {reason}")
    }
}

impl std::error::Error for InvalidAccelerationPolicy {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_enabled_policy_with_a_positive_lag_objective_validates() {
        let policy = AccelerationPolicy {
            table: "orders".to_string(),
            enabled: true,
            lag_objective_ms: 5_000,
        };
        policy.validate().unwrap();
    }

    #[test]
    fn a_disabled_policy_with_no_lag_objective_validates() {
        let policy = AccelerationPolicy {
            table: "orders".to_string(),
            enabled: false,
            lag_objective_ms: 0,
        };
        policy.validate().unwrap();
    }

    #[test]
    fn an_empty_table_name_is_refused_regardless_of_enabled() {
        for enabled in [true, false] {
            let policy = AccelerationPolicy {
                table: "  ".to_string(),
                enabled,
                lag_objective_ms: 5_000,
            };
            assert_eq!(
                policy.validate().unwrap_err(),
                InvalidAccelerationPolicy::EmptyTableName
            );
        }
    }

    #[test]
    fn an_enabled_policy_with_no_lag_objective_is_refused() {
        let policy = AccelerationPolicy {
            table: "orders".to_string(),
            enabled: true,
            lag_objective_ms: 0,
        };
        assert_eq!(
            policy.validate().unwrap_err(),
            InvalidAccelerationPolicy::MissingLagObjective
        );
    }
}
