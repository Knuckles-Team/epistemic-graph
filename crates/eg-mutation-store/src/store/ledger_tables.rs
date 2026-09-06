//! Typed ledger row handles.
//!
//! These name the same twelve durable tables the storage kernel declares,
//! censuses and validates. They carry no authority: opening one still requires a
//! kernel-issued read or write transaction. `ledger_table_names` is cross-checked
//! against `eg_storage::declared_table_names` so a divergent redeclaration here
//! fails a test rather than silently forking the manifest contract.

use redb::TableDefinition;
use serde::{Deserialize, Serialize};

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

/// Durable route fence for one serving scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Fence {
    pub(crate) placement_epoch: u64,
    pub(crate) fencing_token: u64,
}

/// Every table name this crate reads or writes.
#[cfg(test)]
pub(crate) fn ledger_table_names() -> [&'static str; 12] {
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
    ]
}
