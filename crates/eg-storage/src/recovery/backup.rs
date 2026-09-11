use crate::codec::{decode_ledger_record, encode_bounded};
use crate::kernel::create_physical;
use crate::owner::registry::copy_declared_owner_tables;
use crate::physical::binding::ScopeBinding;
use crate::physical::incarnation::StoreIncarnation;
use crate::physical::root::PhysicalStore;
use crate::recovery::evidence::{copy_table, HashSnapshot};
use crate::recovery::validate::{validate_live_recovery_store, RecoveryStoreCounts};
use crate::tables::{visit_ledger_content_tables, visit_ledger_tables, SCOPE_BINDINGS};
use crate::StorageKernel;
use redb::{ReadTransaction, ReadableTable, WriteTransaction};
use sha2::{Digest, Sha256};
use std::path::Path;

/// Stable content fingerprint over one owner file's physical and ledger rows.
///
/// Driven by the authoritative ledger-table list, so the fingerprint of two
/// stores can never agree while their rows differ in any declared table --
/// including the replay ledger, which a hand-maintained list omitted.
pub fn recovery_store_fingerprint(kernel: &StorageKernel) -> Result<[u8; 32], String> {
    recovery_store_fingerprint_of(kernel.store())
}

pub(crate) fn recovery_store_fingerprint_of(store: &PhysicalStore) -> Result<[u8; 32], String> {
    validate_live_recovery_store(store)?;
    let rtx = store.begin_read()?;
    let snapshot = HashSnapshot::Read(&rtx);
    let mut hasher = Sha256::new();
    macro_rules! hash {
        ($table:expr) => {{
            snapshot.hash_table(&mut hasher, $table)?;
        }};
    }
    visit_ledger_tables!(hash);
    Ok(hasher.finalize().into())
}

/// Create a physical backup with a newly derived destination root and exact
/// rebinding of every logical scope to that root.
pub fn backup_recovery_store(
    source: &StorageKernel,
    destination: &Path,
) -> Result<RecoveryStoreCounts, String> {
    backup_recovery_store_of(source.store(), destination)
}

pub(crate) fn backup_recovery_store_of(
    source: &PhysicalStore,
    destination: &Path,
) -> Result<RecoveryStoreCounts, String> {
    if destination.exists() {
        return Err("coordinator backup destination already exists".to_string());
    }
    validate_live_recovery_store(source)?;
    let manifest = source.manifest();
    let target = create_physical(
        destination,
        manifest.physical_identity.clone(),
        source.private_integrity(),
        manifest.layout,
    )?;
    let rtx = source.begin_read()?;
    let mut wtx = target
        .database()
        .begin_write()
        .map_err(|error| error.to_string())?;
    wtx.set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    copy_bindings(&rtx, &wtx, target.incarnation())?;
    macro_rules! copy {
        ($table:expr) => {{
            copy_table(&rtx, &wtx, $table)?;
        }};
    }
    visit_ledger_content_tables!(copy);
    // The owner tables ARE the domain payload. `create_physical` materialises
    // them empty, so omitting this copied a backup with every domain row
    // missing that then validated as good, because recovery validation walked
    // only the ledger.
    copy_declared_owner_tables(&rtx, &wtx, manifest.layout)?;
    wtx.commit().map_err(|error| error.to_string())?;
    validate_live_recovery_store(&target)
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
        let mut binding: ScopeBinding = decode_ledger_record(value.value())?;
        binding.store_identity_digest = root.identity_digest();
        let bytes = encode_bounded(&binding, "backup mutation scope binding")?;
        target_table
            .insert(key.value(), bytes.as_slice())
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}
