use super::*;
use redb::{ReadTransaction, TableHandle};

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
}

/// Validate a LIVE, read-write-capable store: proves the physical file has
/// not been substituted since it was opened, then runs the same content
/// checks as [`validate_recovery_store_read_only`].
pub fn validate_recovery_store(store: &MutationStore) -> Result<RecoveryStoreCounts, String> {
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
    store: &ReadOnlyMutationStore,
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
/// authenticator)` -- a live [`MutationStore`], a [`ReadOnlyMutationStore`],
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
    let root = super::identity::require_persisted_root(&table)?;
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
        let binding: ScopeBinding = decode_record(value.value())?;
        binding.identity.validate_digest()?;
        let expected_key = binding.identity.binding_digest().to_hex();
        if binding.schema_version != MUTATION_STORE_SCHEMA_VERSION
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
        read_binding(rtx, root, key.value())?;
        decode_record::<Fence>(value.value())?;
        increment(&mut counts.fences, "fence count")?;
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
    let binding: ScopeBinding = decode_record(bytes.value())?;
    binding.identity.validate_digest()?;
    if binding.schema_version != MUTATION_STORE_SCHEMA_VERSION
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
    if scope_identity_key(&record.identity) != identity_key || record.batch.batch_id != batch_id {
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

pub fn version(store: &MutationStore, identity: &MutationScopeIdentity) -> Result<u64, String> {
    let rtx = store
        .database()
        .begin_read()
        .map_err(|error| error.to_string())?;
    binding_for_read(store, &rtx, identity)?;
    let key = identity.binding_digest().to_hex();
    let table = rtx
        .open_table(VERSIONS)
        .map_err(|error| error.to_string())?;
    table
        .get(key.as_str())
        .map_err(|error| error.to_string())?
        .map(|value| value.value())
        .ok_or_else(|| "mutation scope binding is missing its version row".to_string())
}

pub fn read_record(
    store: &MutationStore,
    identity: &MutationScopeIdentity,
    batch_id: &str,
) -> Result<Option<MutationBatchRecord>, String> {
    let rtx = store
        .database()
        .begin_read()
        .map_err(|error| error.to_string())?;
    binding_for_read(store, &rtx, identity)?;
    let key = scope_identity_key(identity);
    let table = rtx.open_table(BATCHES).map_err(|error| error.to_string())?;
    table
        .get((key.as_str(), batch_id))
        .map_err(|error| error.to_string())?
        .map(|value| decode_batch_record(value.value()))
        .transpose()
}

pub fn read_outbox(
    store: &MutationStore,
    identity: &MutationScopeIdentity,
    batch_id: &str,
) -> Result<Vec<MutationOutboxRecord>, String> {
    let rtx = store
        .database()
        .begin_read()
        .map_err(|error| error.to_string())?;
    binding_for_read(store, &rtx, identity)?;
    let identity_key = scope_identity_key(identity);
    let table = rtx.open_table(OUTBOX).map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    let mut budget = CollectionBudget::default();
    for row in table
        .range((identity_key.as_str(), batch_id, 0)..=(identity_key.as_str(), batch_id, u32::MAX))
        .map_err(|error| error.to_string())?
    {
        let (_, value) = row.map_err(|error| error.to_string())?;
        budget.account(value.value().len())?;
        rows.push(decode_outbox_record(value.value())?);
    }
    Ok(rows)
}

pub fn read_private_payload(
    store: &MutationStore,
    identity: &MutationScopeIdentity,
    batch_id: &str,
) -> Result<Option<Vec<u8>>, String> {
    let rtx = store
        .database()
        .begin_read()
        .map_err(|error| error.to_string())?;
    binding_for_read(store, &rtx, identity)?;
    let identity_key = scope_identity_key(identity);
    let record = rtx
        .open_table(BATCHES)
        .map_err(|error| error.to_string())?
        .get((identity_key.as_str(), batch_id))
        .map_err(|error| error.to_string())?
        .map(|value| decode_batch_record(value.value()))
        .transpose()?
        .ok_or_else(|| "private recovery plan has no parent receipt".to_string())?;
    let table = rtx
        .open_table(PRIVATE_PAYLOADS)
        .map_err(|error| error.to_string())?;
    let sealed = table
        .get((identity_key.as_str(), batch_id))
        .map_err(|error| error.to_string())?
        .map(|value| value.value().to_vec());
    if let Some(bytes) = &sealed {
        let digest = private_payload_digest(&record)
            .ok_or_else(|| "private recovery payload has no digest-bound parent".to_string())?;
        store.authenticate_private(bytes, digest)?;
    }
    Ok(sealed)
}
