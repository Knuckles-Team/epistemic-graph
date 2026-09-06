//! Typed ledger row handles.
//!
//! These name the same fourteen durable ledger tables the storage kernel
//! declares, censuses and validates. They carry no authority: opening one still
//! requires a kernel-issued read or write transaction. `ledger_table_names` is
//! cross-checked against [`eg_storage::declared_table_names`] so a divergent
//! redeclaration here fails a test rather than silently forking the manifest
//! contract.
//!
//! # Row schema (ledger format v2)
//!
//! Every table below is keyed, in its first key position, by the one
//! [`eg_storage::ledger_scope_key`] -- the scope *binding* digest, which is also
//! what `mutation_scope_bindings_v1` and `mutation_versions_v1` use. Every row
//! carries the exact [`eg_types::MutationScopeIdentity`] it was written under,
//! either directly ([`BATCHES`], [`OUTBOX`], [`FENCES`] via
//! [`eg_storage::ScopeFence`], [`REPLAY_OPERATIONS`] via
//! [`eg_storage::OperationReplayRow`]) or through the receipt its key resolves
//! to ([`IDEMPOTENCY`], [`PRIVATE_PAYLOADS`], [`REPLAY_NONCES`]).
//!
//! That stamp is what makes recovery validation possible: a row resolves its
//! own scope binding by its own key, then the stamped identity must equal the
//! bound identity. Format v1 keyed these tables by the scope *identity* digest
//! while bindings were keyed by the *binding* digest, so no committed row could
//! ever resolve its binding.

use redb::TableDefinition;

pub(crate) const BATCHES: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("mutation_batches_v1");
pub(crate) const IDEMPOTENCY: TableDefinition<'static, (&str, &str), &str> =
    TableDefinition::new("mutation_idempotency_v1");
pub(crate) const VERSIONS: TableDefinition<'static, &str, u64> =
    TableDefinition::new("mutation_versions_v1");
pub(crate) const FENCES: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("mutation_fences_v1");
pub(crate) const OUTBOX: TableDefinition<'static, (&str, &str, u32), &[u8]> =
    TableDefinition::new("mutation_outbox_v1");
pub(crate) const PRIVATE_PAYLOADS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("mutation_private_payloads_v1");
pub(crate) const SCOPE_BINDINGS: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("mutation_scope_bindings_v1");
pub(crate) const REPLAY_NONCES: TableDefinition<'static, (&str, &str), &str> =
    TableDefinition::new("mutation_replay_nonces_v1");
pub(crate) const REPLAY_OPERATIONS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("mutation_replay_operations_v1");

/// Every ledger table name this crate reads or writes, plus the six declared
/// outbox-delivery tables it owns but does not yet write (see
/// [`crate::OutboxCursor`]).
pub(crate) fn ledger_table_names() -> [&'static str; 14] {
    [
        "mutation_batches_v1",
        "mutation_idempotency_v1",
        "mutation_versions_v1",
        "mutation_fences_v1",
        "mutation_outbox_v1",
        "mutation_private_payloads_v1",
        "mutation_outbox_topic_index_v1",
        "mutation_outbox_consumers_v1",
        "mutation_outbox_deliveries_v1",
        "mutation_outbox_cursors_v1",
        "mutation_outbox_claim_cursors_v1",
        "mutation_outbox_fairness_v1",
        "mutation_replay_nonces_v1",
        "mutation_replay_operations_v1",
    ]
}
