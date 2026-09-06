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
    let mut budget = CollectionBudget::default();
    let bindings = validate_bindings(rtx, &root, &mut counts, &mut budget)?;
    validate_versions(rtx, &bindings, &mut counts)?;
    let records = validate_batches(rtx, &bindings, &mut counts, &mut budget)?;
    validate_idempotency(rtx, &records, &mut counts, &mut budget)?;
    validate_fences(rtx, &bindings, &mut counts)?;
    validate_outbox(rtx, &records, &mut counts, &mut budget)?;
    validate_private(
        authenticate_private,
        rtx,
        &records,
        &mut counts,
        &mut budget,
    )?;
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
    budget: &mut CollectionBudget,
) -> Result<std::collections::BTreeMap<String, ScopeBinding>, String> {
    let table = rtx
        .open_table(SCOPE_BINDINGS)
        .map_err(|error| error.to_string())?;
    let mut bindings = std::collections::BTreeMap::new();
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        budget.account(key.value().len() + value.value().len())?;
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
        let identity_key = scope_identity_key(&binding.identity);
        if bindings.insert(identity_key, binding).is_some() {
            return Err("mutation store contains duplicate logical identity bindings".to_string());
        }
        counts.scope_bindings = counts.scope_bindings.saturating_add(1);
    }
    Ok(bindings)
}

fn validate_versions(
    rtx: &ReadTransaction,
    bindings: &std::collections::BTreeMap<String, ScopeBinding>,
    counts: &mut RecoveryStoreCounts,
) -> Result<(), String> {
    let expected = bindings
        .values()
        .map(|binding| binding.identity.binding_digest().to_hex())
        .collect::<std::collections::BTreeSet<_>>();
    let table = rtx
        .open_table(VERSIONS)
        .map_err(|error| error.to_string())?;
    let actual = table
        .iter()
        .map_err(|error| error.to_string())?
        .map(|row| {
            row.map(|(key, _)| key.value().to_string())
                .map_err(|error| error.to_string())
        })
        .take(MAX_MUTATION_COLLECTION_ROWS + 1)
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    if actual.len() > MAX_MUTATION_COLLECTION_ROWS || actual != expected {
        return Err("mutation version rows do not exactly match scope bindings".to_string());
    }
    counts.versions = actual.len() as u64;
    Ok(())
}

fn validate_batches(
    rtx: &ReadTransaction,
    bindings: &std::collections::BTreeMap<String, ScopeBinding>,
    counts: &mut RecoveryStoreCounts,
    budget: &mut CollectionBudget,
) -> Result<std::collections::BTreeMap<(String, String), MutationBatchRecord>, String> {
    let table = rtx.open_table(BATCHES).map_err(|error| error.to_string())?;
    let mut records = std::collections::BTreeMap::new();
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (identity_key, batch_id) = key.value();
        budget.account(identity_key.len() + batch_id.len() + value.value().len())?;
        let record = decode_batch_record(value.value())?;
        let binding = bindings
            .get(identity_key)
            .ok_or_else(|| "mutation batch references an unbound identity".to_string())?;
        if record.identity != binding.identity || record.batch.batch_id != batch_id {
            return Err("mutation batch key does not bind its receipt identity".to_string());
        }
        match record.status {
            MutationBatchStatus::Prepared => counts.prepared = counts.prepared.saturating_add(1),
            MutationBatchStatus::Committed => counts.committed = counts.committed.saturating_add(1),
            MutationBatchStatus::Aborted => counts.aborted = counts.aborted.saturating_add(1),
        }
        if records
            .insert((identity_key.to_string(), batch_id.to_string()), record)
            .is_some()
        {
            return Err("duplicate mutation receipt key".to_string());
        }
        counts.batches = counts.batches.saturating_add(1);
    }
    Ok(records)
}

fn validate_idempotency(
    rtx: &ReadTransaction,
    records: &std::collections::BTreeMap<(String, String), MutationBatchRecord>,
    counts: &mut RecoveryStoreCounts,
    budget: &mut CollectionBudget,
) -> Result<(), String> {
    let table = rtx
        .open_table(IDEMPOTENCY)
        .map_err(|error| error.to_string())?;
    let mut linked = std::collections::BTreeSet::new();
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (identity_key, idempotency_key) = key.value();
        let batch_id = value.value();
        budget.account(identity_key.len() + idempotency_key.len() + batch_id.len())?;
        let record = records
            .get(&(identity_key.to_string(), batch_id.to_string()))
            .ok_or_else(|| "mutation idempotency row points to a missing receipt".to_string())?;
        if record.batch.idempotency_key != idempotency_key
            || !linked.insert((identity_key.to_string(), batch_id.to_string()))
        {
            return Err("mutation idempotency row does not bind exactly one receipt".to_string());
        }
        counts.idempotency = counts.idempotency.saturating_add(1);
    }
    if linked != records.keys().cloned().collect() {
        return Err("mutation receipt is missing its idempotency row".to_string());
    }
    Ok(())
}

fn validate_fences(
    rtx: &ReadTransaction,
    bindings: &std::collections::BTreeMap<String, ScopeBinding>,
    counts: &mut RecoveryStoreCounts,
) -> Result<(), String> {
    let table = rtx.open_table(FENCES).map_err(|error| error.to_string())?;
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        if !bindings.contains_key(key.value()) {
            return Err("mutation fence references an unbound identity".to_string());
        }
        decode_record::<Fence>(value.value())?;
        counts.fences = counts.fences.saturating_add(1);
    }
    Ok(())
}

fn validate_outbox(
    rtx: &ReadTransaction,
    records: &std::collections::BTreeMap<(String, String), MutationBatchRecord>,
    counts: &mut RecoveryStoreCounts,
    budget: &mut CollectionBudget,
) -> Result<(), String> {
    let table = rtx.open_table(OUTBOX).map_err(|error| error.to_string())?;
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (identity_key, batch_id, ordinal) = key.value();
        budget.account(identity_key.len() + batch_id.len() + value.value().len())?;
        let receipt = decode_outbox_record(value.value())?;
        let parent = records
            .get(&(identity_key.to_string(), batch_id.to_string()))
            .ok_or_else(|| "mutation outbox row has no parent receipt".to_string())?;
        let bound = parent.status == MutationBatchStatus::Committed
            && receipt.identity == parent.identity
            && receipt.batch_id == batch_id
            && receipt.ordinal == ordinal
            && receipt.committed_version == parent.committed_version
            && parent.batch.outbox.get(ordinal as usize) == Some(&receipt.intent);
        if !bound {
            return Err("mutation outbox row is not exactly bound to its parent".to_string());
        }
        counts.outbox = counts.outbox.saturating_add(1);
    }
    Ok(())
}

fn validate_private(
    authenticate_private: &PrivateAuthenticator<'_>,
    rtx: &ReadTransaction,
    records: &std::collections::BTreeMap<(String, String), MutationBatchRecord>,
    counts: &mut RecoveryStoreCounts,
    budget: &mut CollectionBudget,
) -> Result<(), String> {
    let table = rtx
        .open_table(PRIVATE_PAYLOADS)
        .map_err(|error| error.to_string())?;
    let mut private = std::collections::BTreeSet::new();
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (identity_key, batch_id) = key.value();
        budget.account(identity_key.len() + batch_id.len() + value.value().len())?;
        let record = records
            .get(&(identity_key.to_string(), batch_id.to_string()))
            .ok_or_else(|| "private recovery plan has no parent receipt".to_string())?;
        let digest = recovery_plan_digest(record)
            .filter(|_| record.status == MutationBatchStatus::Prepared)
            .ok_or_else(|| {
                "private recovery plan has no authenticated prepared parent".to_string()
            })?;
        authenticate_private(value.value(), digest)?;
        private.insert((identity_key.to_string(), batch_id.to_string()));
        counts.encrypted_private_payloads = counts.encrypted_private_payloads.saturating_add(1);
    }
    for (key, record) in records {
        if record.status == MutationBatchStatus::Prepared
            && recovery_plan_digest(record).is_some()
            && !private.contains(key)
        {
            return Err(
                "prepared transaction parent is missing encrypted recovery state".to_string(),
            );
        }
    }
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
