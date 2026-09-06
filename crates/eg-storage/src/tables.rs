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
use redb::{TableDefinition, TableHandle};
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
pub(crate) const CLASSES: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("mutation_classes_v1");

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

/// The class of one admitted mutation.
///
/// RF-RULING-004 makes `MutationKernelV1` the only committer of owner rows, so
/// an owner write that carries no caller identity -- compaction, retention,
/// index initialization, a content-addressed definition insert -- cannot be an
/// un-ledgered second authority. It is admitted, ledgered and version-bumping
/// like any other mutation, and labelled here so replay can treat it correctly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationClass {
    /// A caller-originated operation. It may carry an
    /// [`eg_types::authority::OperationReplayIdentityV1`] and participates in
    /// operation-replay resolution.
    Operation,
    /// An owner-maintenance write with no caller idempotency identity. It is
    /// durable and ledgered, but it can never consume an attempt nonce or
    /// record an operation receipt, so it is outside operation-replay conflict
    /// semantics entirely.
    Maintenance,
}

/// The durable class label of one committed batch. Exactly one row exists per
/// `mutation_batches_v1` row, so a batch's class is explicit rather than
/// inferred from the absence of evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationClassRow {
    pub identity: MutationScopeIdentity,
    pub batch_id: String,
    pub class: MutationClass,
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

/// The one authoritative ledger-table list.
///
/// Every sweep over the ledger -- census, open, hash, copy, fingerprint,
/// validation and purge -- expands one of these three macros. There is no
/// hand-maintained per-function list anywhere: adding a table here adds it to
/// all of them at once, which is exactly what format v2's two replay tables
/// were missing when they were added to the census alone.
macro_rules! visit_ledger_tables {
    ($visit:ident) => {{
        $visit!($crate::tables::STORE_ROOT);
        $visit!($crate::tables::SCOPE_BINDINGS);
        $visit!($crate::tables::OWNER_MANIFEST);
        $visit!($crate::tables::BATCHES);
        $visit!($crate::tables::IDEMPOTENCY);
        $visit!($crate::tables::VERSIONS);
        $visit!($crate::tables::FENCES);
        $visit!($crate::tables::OUTBOX);
        $visit!($crate::tables::PRIVATE_PAYLOADS);
        $visit!($crate::tables::OUTBOX_TOPIC_INDEX);
        $visit!($crate::tables::OUTBOX_CONSUMERS);
        $visit!($crate::tables::OUTBOX_DELIVERIES);
        $visit!($crate::tables::OUTBOX_CURSORS);
        $visit!($crate::tables::OUTBOX_CLAIM_CURSORS);
        $visit!($crate::tables::OUTBOX_FAIRNESS);
        $visit!($crate::tables::REPLAY_NONCES);
        $visit!($crate::tables::REPLAY_OPERATIONS);
        $visit!($crate::tables::CLASSES);
    }};
}

/// The same list minus the three physical-identity tables, which a backup
/// re-anchors to the destination incarnation instead of copying byte for byte.
macro_rules! visit_ledger_content_tables {
    ($visit:ident) => {{
        $visit!($crate::tables::BATCHES);
        $visit!($crate::tables::IDEMPOTENCY);
        $visit!($crate::tables::VERSIONS);
        $visit!($crate::tables::FENCES);
        $visit!($crate::tables::OUTBOX);
        $visit!($crate::tables::PRIVATE_PAYLOADS);
        $visit!($crate::tables::OUTBOX_TOPIC_INDEX);
        $visit!($crate::tables::OUTBOX_CONSUMERS);
        $visit!($crate::tables::OUTBOX_DELIVERIES);
        $visit!($crate::tables::OUTBOX_CURSORS);
        $visit!($crate::tables::OUTBOX_CLAIM_CURSORS);
        $visit!($crate::tables::OUTBOX_FAIRNESS);
        $visit!($crate::tables::REPLAY_NONCES);
        $visit!($crate::tables::REPLAY_OPERATIONS);
        $visit!($crate::tables::CLASSES);
    }};
}

/// Every ledger table whose first key component is the
/// [`crate::ledger_scope_key`]. Only `mutation_store_root_v1` and
/// `mutation_owner_manifest_v1` are outside it, because they are the file's
/// identity rather than one scope's rows.
macro_rules! visit_scoped_ledger_tables {
    ($visit:ident) => {{
        $visit!($crate::tables::SCOPE_BINDINGS);
        $visit!($crate::tables::VERSIONS);
        $visit!($crate::tables::FENCES);
        $visit!($crate::tables::BATCHES);
        $visit!($crate::tables::IDEMPOTENCY);
        $visit!($crate::tables::PRIVATE_PAYLOADS);
        $visit!($crate::tables::OUTBOX);
        $visit!($crate::tables::OUTBOX_TOPIC_INDEX);
        $visit!($crate::tables::OUTBOX_CONSUMERS);
        $visit!($crate::tables::OUTBOX_DELIVERIES);
        $visit!($crate::tables::OUTBOX_CURSORS);
        $visit!($crate::tables::OUTBOX_CLAIM_CURSORS);
        $visit!($crate::tables::OUTBOX_FAIRNESS);
        $visit!($crate::tables::REPLAY_NONCES);
        $visit!($crate::tables::REPLAY_OPERATIONS);
        $visit!($crate::tables::CLASSES);
    }};
}

pub(crate) use {visit_ledger_content_tables, visit_ledger_tables, visit_scoped_ledger_tables};

/// Exact declared names of the ledger tables, in manifest order.
pub(crate) fn ledger_table_names() -> Vec<&'static str> {
    let mut names = Vec::new();
    macro_rules! push {
        ($table:expr) => {{
            names.push($table.name());
        }};
    }
    visit_ledger_tables!(push);
    names
}

/// The scope key carried in a ledger row's key, whatever the key's arity.
///
/// Implemented only for the redb key shapes the closed ledger census declares,
/// so a table cannot join a scoped sweep without a matching key shape.
pub trait LedgerRowScope {
    fn ledger_scope(&self) -> &str;
}

impl LedgerRowScope for &str {
    fn ledger_scope(&self) -> &str {
        self
    }
}

impl LedgerRowScope for (&str, &str) {
    fn ledger_scope(&self) -> &str {
        self.0
    }
}

impl LedgerRowScope for (&str, &str, u32) {
    fn ledger_scope(&self) -> &str {
        self.0
    }
}

impl LedgerRowScope for (&str, &str, &str, u32) {
    fn ledger_scope(&self) -> &str {
        self.0
    }
}

impl LedgerRowScope for (&str, &str, u64, u64, &str, u32) {
    fn ledger_scope(&self) -> &str {
        self.0
    }
}

/// Remove every row of one ledger table belonging to `scope_key`.
///
/// Generic over the key shape so scope retirement is driven by the
/// authoritative table list rather than by a per-table deletion routine.
pub(crate) fn purge_scoped_rows<K, V>(
    wtx: &redb::WriteTransaction,
    definition: TableDefinition<'static, K, V>,
    scope_key: &str,
) -> Result<(), String>
where
    K: redb::Key + 'static,
    for<'a> K::SelfType<'a>: LedgerRowScope,
    V: redb::Value + 'static,
{
    wtx.open_table(definition)
        .map_err(|error| error.to_string())?
        .retain(|key, _| key.ledger_scope() != scope_key)
        .map_err(|error| error.to_string())
}

/// Open every declared physical and ledger table of one owner file.
pub(crate) fn open_declared_ledger_tables(wtx: &redb::WriteTransaction) -> Result<(), String> {
    macro_rules! open {
        ($table:expr) => {{
            ensure_table(wtx, $table)?;
        }};
    }
    visit_ledger_tables!(open);
    Ok(())
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
