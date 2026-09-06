//! The closed physical table set every storage-kernel owner file carries.
//!
//! Three tables are the physical identity of the file itself; the remaining
//! fourteen are the durable mutation ledger. The kernel opens, censuses,
//! hashes, copies and validates all seventeen. Row-level ledger reads and
//! writes belong to the mutation owner, which declares its own typed handles
//! for the same names and is cross-checked against
//! [`crate::declared_table_names`].
//!
//! # Ledger row schema (format v2)
//!
//! Every ledger table is keyed, in its first key position, by the one
//! [`crate::ledger_scope_key`] -- the scope *binding* digest, the same key
//! `mutation_scope_bindings_v1` and `mutation_versions_v1` use. Every ledger
//! row additionally *carries* the exact
//! [`eg_types::MutationScopeIdentity`] it was written under, either directly
//! (`mutation_batches_v1`, `mutation_outbox_v1`, `mutation_fences_v1` via
//! [`ScopeFence`], `mutation_replay_operations_v1`) or through the receipt its
//! key points at (`mutation_idempotency_v1`,
//! `mutation_private_payloads_v1`, `mutation_replay_nonces_v1`).
//!
//! Recovery validation therefore resolves a row's binding by the row's own key
//! and then requires the stamped identity to equal the bound identity. Under
//! format v1 the ledger tables were keyed by the scope *identity* digest while
//! bindings were keyed by the *binding* digest, so the lookup could never
//! succeed and any store with one committed batch failed validation.

use eg_types::contract::Digest256V1;
use eg_types::mutation::MutationReceiptV1;
use eg_types::MutationScopeIdentity;
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
pub(crate) const REPLAY_NONCES: TableDefinition<'static, (&str, &str), &str> =
    TableDefinition::new("mutation_replay_nonces_v1");
pub(crate) const REPLAY_OPERATIONS: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("mutation_replay_operations_v1");

/// Durable route fence for one serving scope, stamped with the exact scope
/// identity it was written under.
///
/// The stamp is what lets recovery validation bind a fence row to its scope
/// binding: the row's key is the binding digest, and `identity` proves which
/// lifecycle generation of that logical owner wrote it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeFence {
    pub identity: MutationScopeIdentity,
    pub placement_epoch: u64,
    pub fencing_token: u64,
}

/// One durable replay decision for one stable operation identity.
///
/// Keyed by `(ledger_scope_key, idempotency_key)` so that a *changed* payload,
/// scope, policy or method under the same idempotency key is a conflict rather
/// than a second row: the stored `operation_replay_digest` is compared against
/// the digest of the proposed [`eg_types::authority::OperationReplayIdentityV1`].
/// `mutation_replay_nonces_v1` maps one attempt nonce digest to the idempotency
/// key it consumed, so the same nonce is always rejected while a fresh nonce
/// over the same stable identity replays `receipt`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationReplayRow {
    pub identity: MutationScopeIdentity,
    pub idempotency_key: String,
    pub operation_replay_digest: Digest256V1,
    pub nonce_replay_digest: Digest256V1,
    pub receipt: MutationReceiptV1,
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
    ensure_table(wtx, OUTBOX_FAIRNESS)?;
    ensure_table(wtx, REPLAY_NONCES)?;
    ensure_table(wtx, REPLAY_OPERATIONS)
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
