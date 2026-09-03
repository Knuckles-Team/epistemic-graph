//! Domain-owned method-policy registry.

pub(crate) type PolicyRow = (&'static str, super::MethodPolicy, &'static str);

pub(crate) struct PolicyFlags {
    pub(crate) idempotent: bool,
    pub(crate) audited: bool,
    pub(crate) emits_cdc: bool,
}

pub(crate) const fn make_policy(
    mutates: bool,
    durability_domain: super::DurabilityDomain,
    authz_action: &'static str,
    flags: PolicyFlags,
    txn_participation: super::TxnParticipation,
) -> super::MethodPolicy {
    super::MethodPolicy {
        mutates,
        durability_domain,
        authz_action,
        idempotent: flags.idempotent,
        audited: flags.audited,
        emits_cdc: flags.emits_cdc,
        txn_participation,
    }
}

pub(crate) mod cluster;
pub(crate) mod compute;
pub(crate) mod coordination;
pub(crate) mod graph;
pub(crate) mod ingestion;
pub(crate) mod messaging;
pub(crate) mod query;
pub(crate) mod reasoning;
pub(crate) mod security;
pub(crate) mod storage;
pub(crate) mod transactions;

/// The sole ordered registry of method-policy declarations.
///
/// Domain order and row order are declaration order. Adding or moving a method therefore
/// changes every generated view through this one registry instead of requiring a second,
/// independently ordered projection.
const REGISTRY: &[(&str, &[PolicyRow])] = &[
    ("cluster", cluster::ROWS),
    ("compute", compute::ROWS),
    ("coordination", coordination::ROWS),
    ("graph", graph::ROWS),
    ("ingestion", ingestion::ROWS),
    ("messaging", messaging::ROWS),
    ("query", query::ROWS),
    ("reasoning", reasoning::ROWS),
    ("security", security::ROWS),
    ("storage", storage::ROWS),
    ("transactions", transactions::ROWS),
];

pub(crate) fn rows() -> impl Iterator<Item = &'static PolicyRow> {
    REGISTRY.iter().flat_map(|(_, rows)| rows.iter())
}
