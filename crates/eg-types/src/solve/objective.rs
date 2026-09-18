//! The value an assignment gives the lexicographic objective.
//!
//! Each objective level becomes one or two *sublevels*: when the level holds
//! any `Unknown` coefficient, a first sublevel counts the selected
//! unknown-cost variables (the unknown tier) and a second sums the known
//! coefficients. The exact scalarisation that turns those sublevels into
//! [`ObjectiveValue::scalar`] is an algorithm and lives in `eg_compute::solve`;
//! only its result crosses the wire.

use serde::{Deserialize, Serialize};

use super::scalar::Scalar;

/// Value of one objective level under an assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LevelValue {
    /// Sum of the known coefficients of the selected variables.
    pub known: Scalar,
    /// How many selected variables have an unknown coefficient at this level.
    pub unknown_selected: u32,
}

/// Every level's value plus the exact scalarised objective.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ObjectiveValue {
    pub levels: Vec<LevelValue>,
    pub scalar: Scalar,
}
