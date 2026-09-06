//! The one recovery-invariant content check for a physical owner file.

use crate::codec::{decode_batch_record, decode_ledger_record, decode_outbox_record};
use crate::payload::recovery_plan_digest;
use crate::physical::binding::{ledger_scope_key, ScopeBinding};
use crate::physical::incarnation::{
    require_persisted_root, StoreIncarnation, STORAGE_KERNEL_SCHEMA_VERSION,
};
use crate::physical::read_only::ReadOnlyStore;
use crate::physical::root::{reject_prototype_names, PhysicalStore};
use crate::tables::{
    visit_scoped_ledger_tables, LedgerRowScope, MutationClassRow, OperationReplayRow, ScopeFence,
    BATCHES, CLASSES, FENCES, IDEMPOTENCY, OUTBOX, PRIVATE_PAYLOADS, REPLAY_NONCES,
    REPLAY_OPERATIONS, SCOPE_BINDINGS, STORE_ROOT, VERSIONS,
};
use crate::StorageKernelV1;
use eg_types::{MutationBatchRecord, MutationBatchStatus};
use redb::{ReadTransaction, ReadableDatabase, ReadableTable, TableHandle};

type PrivateAuthenticator<'a> = dyn Fn(&[u8], &str) -> Result<(), String> + 'a;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryStoreCounts {
    pub store_roots: u64,
    pub scope_bindings: u64,
    pub batches: u64,
    pub prepared: u64,
    pub committed: u64,
    pub aborted: u64,
    pub idempotency: u64,
    pub versions: u64,
    pub fences: u64,
    pub outbox: u64,
    pub encrypted_private_payloads: u64,
    pub replay_nonces: u64,
    pub replay_operations: u64,
    pub maintenance: u64,
}

/// Validate a LIVE, read-write-capable owner file: proves the physical file has
/// not been substituted since it was opened, then runs the same content checks
/// as [`validate_recovery_store_read_only`].
pub fn validate_recovery_store(kernel: &StorageKernelV1) -> Result<RecoveryStoreCounts, String> {
    validate_live_recovery_store(kernel.store())
}

pub(crate) fn validate_live_recovery_store(
    store: &PhysicalStore,
) -> Result<RecoveryStoreCounts, String> {
    store.validate_physical_root()?;
    let rtx = store
        .database()
        .begin_read()
        .map_err(|error| error.to_string())?;
    let authenticate = |sealed: &[u8], digest: &str| store.authenticate_private(sealed, digest);
    validate_recovery_content(store.incarnation(), &rtx, &authenticate)
}

/// Validate a bundle file opened READ-ONLY (via
/// [`crate::open_read_only`]) without ever opening a write transaction
/// against it -- so validating a backup can never change the bytes a
/// manifest's digests were computed over. Same physical-root check and the
/// same content checks as [`validate_recovery_store`]; the only difference
/// is how the caller reached a readable handle on the file.
pub fn validate_recovery_store_read_only(
    store: &ReadOnlyStore,
) -> Result<RecoveryStoreCounts, String> {
    store.validate_physical_root()?;
    let rtx = store
        .database()
        .begin_read()
        .map_err(|error| error.to_string())?;
    let authenticate = |sealed: &[u8], digest: &str| store.authenticate_private(sealed, digest);
    validate_recovery_content(store.incarnation(), &rtx, &authenticate)
}

/// The one recovery-invariant content check, shared by every caller that can
/// produce `(expected incarnation, read transaction, private-payload
/// authenticator)` -- a live [`MutationStore`], a [`ReadOnlyStore`],
/// or [`crate::adopt_restored_store`]'s pre-rewrite proof that a copied
/// bundle's bytes are self-consistent under the incarnation they were
/// stamped with. Never opens a write transaction and never touches physical
/// identity itself -- callers that need the physical-root check run it
/// separately (see the two wrappers above), and `adopt_restored_store`
/// deliberately skips it, since proving *that* is precisely what makes an
/// adoption necessary.
pub(crate) fn validate_recovery_content(
    expected: &StoreIncarnation,
    rtx: &ReadTransaction,
    authenticate_private: &PrivateAuthenticator<'_>,
) -> Result<RecoveryStoreCounts, String> {
    let root = validate_root(expected, rtx)?;
    let mut counts = RecoveryStoreCounts {
        store_roots: 1,
        ..RecoveryStoreCounts::default()
    };
    validate_bindings(rtx, &root, &mut counts)?;
    validate_versions(rtx, &root, &mut counts)?;
    validate_batches(rtx, &root, &mut counts)?;
    validate_idempotency(rtx, &mut counts)?;
    validate_fences(rtx, &root, &mut counts)?;
    validate_outbox(rtx, &mut counts)?;
    validate_private(authenticate_private, rtx, &mut counts)?;
    validate_replay(rtx, &root, &mut counts)?;
    validate_classes(rtx, &root, &mut counts)?;
    validate_every_scoped_row(rtx, &root)?;
    Ok(counts)
}

fn validate_root(
    expected: &StoreIncarnation,
    rtx: &ReadTransaction,
) -> Result<StoreIncarnation, String> {
    reject_prototype_names(
        rtx.list_tables()
            .map_err(|error| error.to_string())?
            .map(|table| table.name().to_string()),
    )?;
    let table = rtx
        .open_table(STORE_ROOT)
        .map_err(|error| error.to_string())?;
    let root = require_persisted_root(&table)?;
    if &root != expected {
        return Err(
            "mutation store persisted root does not match its physical database".to_string(),
        );
    }
    Ok(root)
}

fn validate_bindings(
    rtx: &ReadTransaction,
    root: &StoreIncarnation,
    counts: &mut RecoveryStoreCounts,
) -> Result<(), String> {
    let table = rtx
        .open_table(SCOPE_BINDINGS)
        .map_err(|error| error.to_string())?;
    let versions = rtx
        .open_table(VERSIONS)
        .map_err(|error| error.to_string())?;
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let binding: ScopeBinding = decode_ledger_record(value.value())?;
        binding.identity.validate_digest()?;
        let expected_key = binding.identity.binding_digest().to_hex();
        if binding.schema_version != STORAGE_KERNEL_SCHEMA_VERSION
            || binding.store_identity_digest != root.identity_digest()
            || key.value() != expected_key
        {
            return Err(
                "mutation scope binding is malformed or attached to another root".to_string(),
            );
        }
        if versions
            .get(key.value())
            .map_err(|error| error.to_string())?
            .is_none()
        {
            return Err("mutation scope binding is missing its version row".to_string());
        }
        increment(&mut counts.scope_bindings, "scope binding count")?;
    }
    Ok(())
}

fn validate_versions(
    rtx: &ReadTransaction,
    root: &StoreIncarnation,
    counts: &mut RecoveryStoreCounts,
) -> Result<(), String> {
    let table = rtx
        .open_table(VERSIONS)
        .map_err(|error| error.to_string())?;
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, _) = row.map_err(|error| error.to_string())?;
        read_binding(rtx, root, key.value())?;
        increment(&mut counts.versions, "version count")?;
    }
    Ok(())
}

fn validate_batches(
    rtx: &ReadTransaction,
    root: &StoreIncarnation,
    counts: &mut RecoveryStoreCounts,
) -> Result<(), String> {
    let table = rtx.open_table(BATCHES).map_err(|error| error.to_string())?;
    let idempotency = rtx
        .open_table(IDEMPOTENCY)
        .map_err(|error| error.to_string())?;
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (identity_key, batch_id) = key.value();
        let record = decode_batch_record(value.value())?;
        let binding = read_binding(rtx, root, identity_key)?;
        if record.identity != binding.identity || record.batch.batch_id != batch_id {
            return Err("mutation batch key does not bind its receipt identity".to_string());
        }
        let linked = idempotency
            .get((identity_key, record.batch.idempotency_key.as_str()))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "mutation receipt is missing its idempotency row".to_string())?;
        if linked.value() != batch_id {
            return Err("mutation receipt idempotency row points elsewhere".to_string());
        }
        if read_class(rtx, identity_key, batch_id)?.identity != binding.identity {
            return Err("mutation batch class row is not bound to its receipt".to_string());
        }
        match record.status {
            MutationBatchStatus::Prepared => increment(&mut counts.prepared, "prepared count")?,
            MutationBatchStatus::Committed => increment(&mut counts.committed, "committed count")?,
            MutationBatchStatus::Aborted => increment(&mut counts.aborted, "aborted count")?,
        }
        increment(&mut counts.batches, "batch count")?;
    }
    Ok(())
}

fn validate_idempotency(
    rtx: &ReadTransaction,
    counts: &mut RecoveryStoreCounts,
) -> Result<(), String> {
    let table = rtx
        .open_table(IDEMPOTENCY)
        .map_err(|error| error.to_string())?;
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (identity_key, idempotency_key) = key.value();
        let batch_id = value.value();
        let record = read_batch(rtx, identity_key, batch_id)?;
        if record.batch.idempotency_key != idempotency_key {
            return Err("mutation idempotency row does not bind exactly one receipt".to_string());
        }
        increment(&mut counts.idempotency, "idempotency count")?;
    }
    Ok(())
}

fn validate_fences(
    rtx: &ReadTransaction,
    root: &StoreIncarnation,
    counts: &mut RecoveryStoreCounts,
) -> Result<(), String> {
    let table = rtx.open_table(FENCES).map_err(|error| error.to_string())?;
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let binding = read_binding(rtx, root, key.value())?;
        let fence = decode_ledger_record::<ScopeFence>(value.value())?;
        fence.identity.validate_digest()?;
        if fence.identity != binding.identity {
            return Err("mutation fence row is not stamped with its bound identity".to_string());
        }
        increment(&mut counts.fences, "fence count")?;
    }
    Ok(())
}

/// Replay evidence: a nonce row names the idempotency key it consumed, and that
/// key must resolve to an operation row stamped with the same bound identity.
fn validate_replay(
    rtx: &ReadTransaction,
    root: &StoreIncarnation,
    counts: &mut RecoveryStoreCounts,
) -> Result<(), String> {
    let operations = rtx
        .open_table(REPLAY_OPERATIONS)
        .map_err(|error| error.to_string())?;
    for row in operations.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (scope_key, idempotency_key) = key.value();
        let binding = read_binding(rtx, root, scope_key)?;
        let record: OperationReplayRow = decode_ledger_record(value.value())?;
        record.identity.validate_digest()?;
        if record.identity != binding.identity || record.idempotency_key != idempotency_key {
            return Err("mutation replay row is not bound to its exact scope".to_string());
        }
        increment(&mut counts.replay_operations, "replay operation count")?;
    }
    let nonces = rtx
        .open_table(REPLAY_NONCES)
        .map_err(|error| error.to_string())?;
    for row in nonces.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (scope_key, _) = key.value();
        read_binding(rtx, root, scope_key)?;
        if operations
            .get((scope_key, value.value()))
            .map_err(|error| error.to_string())?
            .is_none()
        {
            return Err("mutation replay nonce points to a missing operation row".to_string());
        }
        increment(&mut counts.replay_nonces, "replay nonce count")?;
    }
    Ok(())
}

fn validate_outbox(rtx: &ReadTransaction, counts: &mut RecoveryStoreCounts) -> Result<(), String> {
    let table = rtx.open_table(OUTBOX).map_err(|error| error.to_string())?;
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (identity_key, batch_id, ordinal) = key.value();
        let receipt = decode_outbox_record(value.value())?;
        let parent = read_batch(rtx, identity_key, batch_id)?;
        let bound = parent.status == MutationBatchStatus::Committed
            && receipt.identity == parent.identity
            && receipt.batch_id == batch_id
            && receipt.ordinal == ordinal
            && receipt.committed_version == parent.committed_version
            && parent.batch.outbox.get(ordinal as usize) == Some(&receipt.intent);
        if !bound {
            return Err("mutation outbox row is not exactly bound to its parent".to_string());
        }
        increment(&mut counts.outbox, "outbox count")?;
    }
    Ok(())
}

fn validate_private(
    authenticate_private: &PrivateAuthenticator<'_>,
    rtx: &ReadTransaction,
    counts: &mut RecoveryStoreCounts,
) -> Result<(), String> {
    let table = rtx
        .open_table(PRIVATE_PAYLOADS)
        .map_err(|error| error.to_string())?;
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (identity_key, batch_id) = key.value();
        let record = read_batch(rtx, identity_key, batch_id)?;
        let digest = recovery_plan_digest(&record)
            .filter(|_| record.status == MutationBatchStatus::Prepared)
            .ok_or_else(|| {
                "private recovery plan has no authenticated prepared parent".to_string()
            })?;
        authenticate_private(value.value(), digest)?;
        increment(
            &mut counts.encrypted_private_payloads,
            "private payload count",
        )?;
    }
    let records = rtx.open_table(BATCHES).map_err(|error| error.to_string())?;
    for row in records.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let record = decode_batch_record(value.value())?;
        if record.status == MutationBatchStatus::Prepared
            && recovery_plan_digest(&record).is_some()
            && table
                .get(key.value())
                .map_err(|error| error.to_string())?
                .is_none()
        {
            return Err(
                "prepared transaction parent is missing encrypted recovery state".to_string(),
            );
        }
    }
    Ok(())
}

/// Every batch carries exactly one class row, and every class row names a batch
/// that exists in the same scope. A maintenance batch is therefore explicit in
/// the ledger rather than inferred from missing replay evidence.
fn validate_classes(
    rtx: &ReadTransaction,
    root: &StoreIncarnation,
    counts: &mut RecoveryStoreCounts,
) -> Result<(), String> {
    let table = rtx.open_table(CLASSES).map_err(|error| error.to_string())?;
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (identity_key, batch_id) = key.value();
        let binding = read_binding(rtx, root, identity_key)?;
        let class: MutationClassRow = decode_ledger_record(value.value())?;
        class.identity.validate_digest()?;
        if class.identity != binding.identity || class.batch_id != batch_id {
            return Err("mutation class row is not bound to its exact batch".to_string());
        }
        read_batch(rtx, identity_key, batch_id)?;
        if class.class == crate::tables::MutationClass::Maintenance {
            increment(&mut counts.maintenance, "maintenance count")?;
        }
    }
    Ok(())
}

fn read_class(
    rtx: &ReadTransaction,
    identity_key: &str,
    batch_id: &str,
) -> Result<MutationClassRow, String> {
    let table = rtx.open_table(CLASSES).map_err(|error| error.to_string())?;
    let bytes = table
        .get((identity_key, batch_id))
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "mutation receipt is missing its class row".to_string())?;
    decode_ledger_record(bytes.value())
}

/// Every row of every scoped ledger table must resolve its own scope binding.
///
/// The typed checks above cover the eleven tables that carry a content model.
/// This sweep is driven by the authoritative list, so the six outbox-delivery
/// tables -- declared, unwritten today, and previously unvalidated -- cannot
/// carry a row belonging to no bound scope, whether planted in place or
/// inherited from an adopted foreign image.
fn validate_every_scoped_row(rtx: &ReadTransaction, root: &StoreIncarnation) -> Result<(), String> {
    macro_rules! sweep {
        ($table:expr) => {{
            validate_scoped_table(rtx, root, $table)?;
        }};
    }
    visit_scoped_ledger_tables!(sweep);
    Ok(())
}

fn validate_scoped_table<K, V>(
    rtx: &ReadTransaction,
    root: &StoreIncarnation,
    definition: redb::TableDefinition<'static, K, V>,
) -> Result<(), String>
where
    K: redb::Key + 'static,
    for<'a> K::SelfType<'a>: LedgerRowScope,
    V: redb::Value + 'static,
{
    let table = rtx
        .open_table(definition)
        .map_err(|error| error.to_string())?;
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, _) = row.map_err(|error| error.to_string())?;
        read_binding(rtx, root, key.value().ledger_scope())?;
    }
    Ok(())
}

fn read_binding(
    rtx: &ReadTransaction,
    root: &StoreIncarnation,
    key: &str,
) -> Result<ScopeBinding, String> {
    let table = rtx
        .open_table(SCOPE_BINDINGS)
        .map_err(|error| error.to_string())?;
    let bytes = table
        .get(key)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "mutation row references an unbound identity".to_string())?;
    let binding: ScopeBinding = decode_ledger_record(bytes.value())?;
    binding.identity.validate_digest()?;
    if binding.schema_version != STORAGE_KERNEL_SCHEMA_VERSION
        || binding.store_identity_digest != root.identity_digest()
        || binding.identity.binding_digest().to_hex() != key
    {
        return Err("mutation row references a malformed scope binding".to_string());
    }
    Ok(binding)
}

fn read_batch(
    rtx: &ReadTransaction,
    identity_key: &str,
    batch_id: &str,
) -> Result<MutationBatchRecord, String> {
    let table = rtx.open_table(BATCHES).map_err(|error| error.to_string())?;
    let bytes = table
        .get((identity_key, batch_id))
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "mutation row points to a missing receipt".to_string())?;
    let record = decode_batch_record(bytes.value())?;
    if ledger_scope_key(&record.identity) != identity_key || record.batch.batch_id != batch_id {
        return Err("mutation receipt does not match its table key".to_string());
    }
    Ok(record)
}

fn increment(value: &mut u64, label: &str) -> Result<(), String> {
    *value = value
        .checked_add(1)
        .ok_or_else(|| format!("{label} overflow"))?;
    Ok(())
}
