//! Cross-store atomicity scope (EG-DURABLE-KERNEL-R034): proving traffic
//! confined to a single store never enters the cross-store commit path. This
//! is the typed model slice (`.1`): a transaction-scope classifier and a
//! pure, total commit-path mapping. The real crash-consistency mechanism and
//! the architecture decision record are later children.

use std::fmt;

use serde::{Deserialize, Serialize};

/// How many distinct stores a transaction touches, classified.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionScope {
    SingleStore,
    CrossStore,
}

/// A transaction touching zero stores is not a valid scope to classify.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmptyTransaction;

impl fmt::Display for EmptyTransaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a transaction touching zero stores has no scope")
    }
}

impl std::error::Error for EmptyTransaction {}

/// Classify a transaction's scope by the number of distinct stores it
/// touches. Refuses a zero-store transaction.
pub fn classify_scope(stores_touched: usize) -> Result<TransactionScope, EmptyTransaction> {
    match stores_touched {
        0 => Err(EmptyTransaction),
        1 => Ok(TransactionScope::SingleStore),
        _ => Ok(TransactionScope::CrossStore),
    }
}

/// The commit path a scope is routed through.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitPath {
    Direct,
    CrossStoreCoordinated,
}

/// The structural invariant this requirement is about: a single-store scope
/// always maps to the direct path, a cross-store scope always maps to the
/// coordinated path. Pure and total -- this can never vary at runtime, so a
/// single-store transaction provably can never reach the cross-store commit
/// path.
pub const fn commit_path_for(scope: TransactionScope) -> CommitPath {
    match scope {
        TransactionScope::SingleStore => CommitPath::Direct,
        TransactionScope::CrossStore => CommitPath::CrossStoreCoordinated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-DURABLE-KERNEL-R034.1
    #[test]
    fn scopes_round_trip_through_their_wire_name() {
        for (scope, name) in [
            (TransactionScope::SingleStore, "singlestore"),
            (TransactionScope::CrossStore, "crossstore"),
        ] {
            let wire = serde_json::to_string(&scope).unwrap();
            assert_eq!(wire, format!("\"{name}\""));
            let back: TransactionScope = serde_json::from_str(&wire).unwrap();
            assert_eq!(back, scope);
        }
    }

    // spec: EG-DURABLE-KERNEL-R034.1
    #[test]
    fn classifying_zero_stores_is_refused() {
        assert_eq!(classify_scope(0), Err(EmptyTransaction));
    }

    // spec: EG-DURABLE-KERNEL-R034.1
    #[test]
    fn one_store_is_single_store_and_more_is_cross_store() {
        assert_eq!(classify_scope(1), Ok(TransactionScope::SingleStore));
        for n in 2..=50 {
            assert_eq!(classify_scope(n), Ok(TransactionScope::CrossStore));
        }
    }

    #[test]
    fn commit_path_mapping_is_deterministic_per_scope() {
        for _ in 0..10 {
            assert_eq!(
                commit_path_for(TransactionScope::SingleStore),
                CommitPath::Direct
            );
            assert_eq!(
                commit_path_for(TransactionScope::CrossStore),
                CommitPath::CrossStoreCoordinated
            );
        }
    }
}
