use super::*;
use redb::{Key, ReadTransaction, Value};
use std::path::Path;

pub fn recovery_store_fingerprint(store: &MutationStore) -> Result<[u8; 32], String> {
    validate_recovery_store(store)?;
    let rtx = store
        .database()
        .begin_read()
        .map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    hash_bytes_table(&rtx, &mut hasher, b"root", STORE_ROOT)?;
    hash_bytes_table(&rtx, &mut hasher, b"bindings", SCOPE_BINDINGS)?;
    hash_pair_bytes_table(&rtx, &mut hasher, b"batches", BATCHES)?;
    hash_pair_string_table(&rtx, &mut hasher, b"idempotency", IDEMPOTENCY)?;
    hash_string_u64_table(&rtx, &mut hasher, b"versions", VERSIONS)?;
    hash_bytes_table(&rtx, &mut hasher, b"fences", FENCES)?;
    hash_outbox_table(&rtx, &mut hasher)?;
    hash_pair_bytes_table(&rtx, &mut hasher, b"private", PRIVATE_PAYLOADS)?;
    Ok(hasher.finalize().into())
}

fn hash_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn hash_bytes_table(
    rtx: &ReadTransaction,
    hasher: &mut Sha256,
    tag: &[u8],
    definition: TableDefinition<'static, &str, &[u8]>,
) -> Result<(), String> {
    hash_field(hasher, tag);
    let table = rtx
        .open_table(definition)
        .map_err(|error| error.to_string())?;
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        hash_field(hasher, key.value().as_bytes());
        hash_field(hasher, value.value());
    }
    Ok(())
}

fn hash_pair_bytes_table(
    rtx: &ReadTransaction,
    hasher: &mut Sha256,
    tag: &[u8],
    definition: TableDefinition<'static, (&str, &str), &[u8]>,
) -> Result<(), String> {
    hash_field(hasher, tag);
    let table = rtx
        .open_table(definition)
        .map_err(|error| error.to_string())?;
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        hash_field(hasher, key.value().0.as_bytes());
        hash_field(hasher, key.value().1.as_bytes());
        hash_field(hasher, value.value());
    }
    Ok(())
}

fn hash_pair_string_table(
    rtx: &ReadTransaction,
    hasher: &mut Sha256,
    tag: &[u8],
    definition: TableDefinition<'static, (&str, &str), &str>,
) -> Result<(), String> {
    hash_field(hasher, tag);
    let table = rtx
        .open_table(definition)
        .map_err(|error| error.to_string())?;
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        hash_field(hasher, key.value().0.as_bytes());
        hash_field(hasher, key.value().1.as_bytes());
        hash_field(hasher, value.value().as_bytes());
    }
    Ok(())
}

fn hash_string_u64_table(
    rtx: &ReadTransaction,
    hasher: &mut Sha256,
    tag: &[u8],
    definition: TableDefinition<'static, &str, u64>,
) -> Result<(), String> {
    hash_field(hasher, tag);
    let table = rtx
        .open_table(definition)
        .map_err(|error| error.to_string())?;
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        hash_field(hasher, key.value().as_bytes());
        hash_field(hasher, &value.value().to_be_bytes());
    }
    Ok(())
}

fn hash_outbox_table(rtx: &ReadTransaction, hasher: &mut Sha256) -> Result<(), String> {
    hash_field(hasher, b"outbox");
    let table = rtx.open_table(OUTBOX).map_err(|error| error.to_string())?;
    for row in table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        hash_field(hasher, key.value().0.as_bytes());
        hash_field(hasher, key.value().1.as_bytes());
        hash_field(hasher, &key.value().2.to_be_bytes());
        hash_field(hasher, value.value());
    }
    Ok(())
}

/// Create a physical backup with a newly derived destination root and exact
/// rebinding of every logical scope to that root.
pub fn backup_recovery_store(
    source: &MutationStore,
    destination: &Path,
) -> Result<RecoveryStoreCounts, String> {
    if destination.exists() {
        return Err("coordinator backup destination already exists".to_string());
    }
    validate_recovery_store(source)?;
    let target = MutationStore::empty_physical(destination, source.private_integrity())?;
    let rtx = source
        .database()
        .begin_read()
        .map_err(|error| error.to_string())?;
    let mut wtx = target
        .database()
        .begin_write()
        .map_err(|error| error.to_string())?;
    wtx.set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    copy_bindings(&rtx, &wtx, target.incarnation())?;
    copy_table(&rtx, &wtx, BATCHES)?;
    copy_table(&rtx, &wtx, IDEMPOTENCY)?;
    copy_table(&rtx, &wtx, VERSIONS)?;
    copy_table(&rtx, &wtx, FENCES)?;
    copy_table(&rtx, &wtx, OUTBOX)?;
    copy_table(&rtx, &wtx, PRIVATE_PAYLOADS)?;
    wtx.commit().map_err(|error| error.to_string())?;
    validate_recovery_store(&target)
}

fn copy_bindings(
    source: &ReadTransaction,
    target: &WriteTransaction,
    root: &StoreIncarnation,
) -> Result<(), String> {
    let source_table = source
        .open_table(SCOPE_BINDINGS)
        .map_err(|error| error.to_string())?;
    let mut target_table = target
        .open_table(SCOPE_BINDINGS)
        .map_err(|error| error.to_string())?;
    for row in source_table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let mut binding: ScopeBinding = decode_record(value.value())?;
        binding.store_identity_digest = root.identity_digest();
        let bytes = encode_bounded(&binding, "backup mutation scope binding")?;
        target_table
            .insert(key.value(), bytes.as_slice())
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn copy_table<K, V>(
    source: &ReadTransaction,
    target: &WriteTransaction,
    definition: TableDefinition<'static, K, V>,
) -> Result<(), String>
where
    K: Key + 'static,
    V: Value + 'static,
{
    let source_table = source
        .open_table(definition)
        .map_err(|error| error.to_string())?;
    let mut target_table = target
        .open_table(definition)
        .map_err(|error| error.to_string())?;
    for row in source_table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        target_table
            .insert(key.value(), value.value())
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}
