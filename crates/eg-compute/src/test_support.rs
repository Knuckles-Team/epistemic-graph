//! Shared repository enrichment fixtures for parser and Raft tests.
//!
//! This module is compiled only for eg-compute's own tests or the explicit
//! `test-support` feature consumed by downstream dev dependencies.

use crate::parser::enrichment_snapshot::{EligibleSnapshot, EligibleUnit};

/// A source with the same immutable revision/digest fields across tests.
/// Tenant, graph, budget, and eligible units remain explicit per scenario.
pub fn repository_enrichment_snapshot(
    tenant: &str,
    graph: &str,
    budget_units: u64,
    units: Vec<EligibleUnit>,
) -> EligibleSnapshot {
    EligibleSnapshot {
        schema_version: 1,
        tenant_id: tenant.into(),
        graph: graph.into(),
        repository_id: "repo".into(),
        source_envelope: "source-one".into(),
        source_commit_ref: "source-one".into(),
        policy_digest: "a".repeat(64),
        catalog_digest: "b".repeat(64),
        model_digest: "c".repeat(64),
        budget_units,
        units,
    }
}
