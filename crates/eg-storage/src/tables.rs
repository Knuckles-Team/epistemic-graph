//! The closed physical table set every storage-kernel owner file carries.
//!
//! Three tables are the physical identity of the file itself; the remaining
//! twelve are the durable mutation ledger. The kernel opens, censuses, hashes,
//! copies and validates all fifteen. Row-level ledger reads and writes belong
//! to the mutation owner, which declares its own typed handles for the same
//! names and is cross-checked against [`crate::declared_table_names`].

use redb::TableDefinition;
use serde::{Deserialize, Serialize};

pub(crate) const STORE_ROOT: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("mutation_store_root_v1");
pub(crate) const SCOPE_BINDINGS: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("mutation_scope_bindings_v1");
pub(crate) const OWNER_MANIFEST: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("mutation_owner_manifest_v1");
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
pub(crate) const OUTBOX_TOPIC_INDEX: TableDefinition<
    'static,
    (&str, &str, u64, u64, &str, u32),
    (),
> = TableDefinition::new("mutation_outbox_topic_index_v1");
pub(crate) const PRIVATE_PAYLOADS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("mutation_private_payloads_v1");
pub(crate) const OUTBOX_CONSUMERS: TableDefinition<'static, (&str, &str), &str> =
    TableDefinition::new("mutation_outbox_consumers_v1");
pub(crate) const OUTBOX_DELIVERIES: TableDefinition<'static, (&str, &str, &str, u32), &[u8]> =
    TableDefinition::new("mutation_outbox_deliveries_v1");
pub(crate) const OUTBOX_CURSORS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("mutation_outbox_cursors_v1");
pub(crate) const OUTBOX_CLAIM_CURSORS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("mutation_outbox_claim_cursors_v1");
pub(crate) const OUTBOX_FAIRNESS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("mutation_outbox_fairness_v1");

/// Durable route fence for one serving scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Fence {
    pub(crate) placement_epoch: u64,
    pub(crate) fencing_token: u64,
}

/// Open every declared physical and ledger table of one owner file.
pub(crate) fn open_declared_ledger_tables(
    wtx: &redb::WriteTransaction,
) -> Result<(), String> {
    ensure_table(wtx, STORE_ROOT)?;
    ensure_table(wtx, SCOPE_BINDINGS)?;
    ensure_table(wtx, OWNER_MANIFEST)?;
    ensure_table(wtx, BATCHES)?;
    ensure_table(wtx, IDEMPOTENCY)?;
    ensure_table(wtx, VERSIONS)?;
    ensure_table(wtx, FENCES)?;
    ensure_table(wtx, OUTBOX)?;
    ensure_table(wtx, PRIVATE_PAYLOADS)?;
    ensure_table(wtx, OUTBOX_TOPIC_INDEX)?;
    ensure_table(wtx, OUTBOX_CONSUMERS)?;
    ensure_table(wtx, OUTBOX_DELIVERIES)?;
    ensure_table(wtx, OUTBOX_CURSORS)?;
    ensure_table(wtx, OUTBOX_CLAIM_CURSORS)?;
    ensure_table(wtx, OUTBOX_FAIRNESS)
}

fn ensure_table<K, V>(
    wtx: &redb::WriteTransaction,
    definition: TableDefinition<'static, K, V>,
) -> Result<(), String>
where
    K: redb::Key + 'static,
    V: redb::Value + 'static,
{
    wtx.open_table(definition)
        .map(|_| ())
        .map_err(|error| error.to_string())
}
