//! `Method::DecisionCommit`: make one assembly record durable.
//!
//! Owned after S1 by the decision-commit package, which replaces the stub body
//! with the version check, the re-derivation, the certificate verification,
//! the candidate-fact comparison against the pinned revisions and the catalog
//! compare-and-set.

use crate::server::contract_wave::contract_wave_stub;

contract_wave_stub! {
    /// Commit one `DecisionRecord` as a component revision, idempotently.
    handle_decision_commit(eg_types::decision::DecisionCommitRequest)
        refuses "DecisionCommit", tested by commit_stub_tests
}
