//! Typed optimistic-write conflict data.
//!
//! This module contains only the wire model. Conflict resolution and the
//! served mutation path live in the crates above `eg-types` in the workspace
//! DAG.

use serde::{Deserialize, Serialize};

/// The policy applied when a writer's expected version is stale.
///
/// This enum is intentionally closed: adding a strategy changes the durable
/// conflict contract and must be reviewed with its resolution semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategy {
    /// Reject the write without changing the stored state.
    Refuse,
    /// Prefer the incoming value when the scope policy allows it.
    PreferNewer,
    /// Merge non-overlapping fields under the scope policy.
    FieldUnion,
}

impl Default for MergeStrategy {
    fn default() -> Self {
        Self::Refuse
    }
}

/// The version mismatch a durable writer must resolve before committing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteConflict {
    /// Version observed by the losing writer when it prepared its mutation.
    pub expected: u64,
    /// Version currently authoritative for the scope.
    pub actual: u64,
    /// Stable logical scope name used to select the merge policy.
    pub scope: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-DURABLE-KERNEL-R084.1
    #[test]
    fn conflict_and_strategies_round_trip_with_refuse_as_default() {
        let conflict = WriteConflict {
            expected: 7,
            actual: 8,
            scope: "tenant-a/graph-main".to_owned(),
        };
        let encoded = serde_json::to_string(&conflict).expect("conflict encodes");
        assert_eq!(
            serde_json::from_str::<WriteConflict>(&encoded).expect("conflict decodes"),
            conflict
        );

        for strategy in [
            MergeStrategy::Refuse,
            MergeStrategy::PreferNewer,
            MergeStrategy::FieldUnion,
        ] {
            let encoded = serde_json::to_string(&strategy).expect("strategy encodes");
            assert_eq!(
                serde_json::from_str::<MergeStrategy>(&encoded).expect("strategy decodes"),
                strategy
            );
        }
        assert_eq!(MergeStrategy::default(), MergeStrategy::Refuse);
    }
}
