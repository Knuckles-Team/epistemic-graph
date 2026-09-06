use crate::codec::{decode_ledger_record, encode_bounded};
use crate::kernel::{create_physical, open_physical};
use crate::owner::identity::PhysicalStoreIdentity;
use crate::owner::layout::OwnerLayout;
use crate::owner::{
    copy_declared_owner_tables, hash_declared_owner_tables, validate_declared_owner_tables,
    validate_declared_tables_write, validate_manifest_read,
};
use crate::physical::binding::ScopeBinding;
use crate::physical::incarnation::StoreIncarnation;
use crate::physical::manifest::OwnerManifest;
use crate::physical::root::PhysicalStore;
use crate::recovery::validate::validate_recovery_content;
use crate::tables::{
    visit_ledger_content_tables, visit_ledger_tables, OWNER_MANIFEST, SCOPE_BINDINGS,
};
use crate::StorageKernelV1;
use redb::{
    Key, ReadTransaction, ReadableTable, TableDefinition, TableHandle, Value, WriteTransaction,
};
use sha2::{Digest, Sha256};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrictTableEvidence {
    pub table_id: String,
    pub rows: u64,
    pub fingerprint: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrictRecoveryEvidence {
    pub ledger_rows: u64,
    pub owner_rows: u64,
    pub fingerprint: [u8; 32],
    pub tables: Vec<StrictTableEvidence>,
}

/// Per-table row counts and fingerprints over one owner file's whole census.
pub fn strict_recovery_evidence(
    kernel: &StorageKernelV1,
) -> Result<StrictRecoveryEvidence, String> {
    strict_evidence_of(kernel.store())
}

pub(crate) fn strict_evidence_of(
    store: &PhysicalStore,
) -> Result<StrictRecoveryEvidence, String> {
    let rtx = store.begin_read()?;
    let manifest = validated_source_manifest(store, &rtx)?;
    let authenticate = |sealed: &[u8], digest: &str| store.authenticate_private(sealed, digest);
    validate_recovery_content(store.incarnation(), &rtx, &authenticate)?;
    validate_declared_owner_tables(&rtx, manifest.layout)?;
    strict_snapshot_read(&rtx, manifest.layout)
}

/// Copy one owner file to a new physical identity and prove the copy exactly.
pub fn backup_strict_recovery_store(
    source: &StorageKernelV1,
    destination: &Path,
    destination_identity: PhysicalStoreIdentity,
) -> Result<StrictRecoveryEvidence, String> {
    backup_strict_recovery_store_of(source.store(), destination, destination_identity)
}

pub(crate) fn backup_strict_recovery_store_of(
    source: &PhysicalStore,
    destination: &Path,
    destination_identity: PhysicalStoreIdentity,
) -> Result<StrictRecoveryEvidence, String> {
    if destination.exists() {
        return Err("strict backup destination already exists".to_string());
    }
    let rtx = source.begin_read()?;
    let manifest = validated_source_manifest(source, &rtx)?;
    let authenticate = |sealed: &[u8], digest: &str| source.authenticate_private(sealed, digest);
    validate_recovery_content(source.incarnation(), &rtx, &authenticate)?;
    validate_declared_owner_tables(&rtx, manifest.layout)?;
    let source_evidence = strict_snapshot_read(&rtx, manifest.layout)?;
    let target = create_physical(
        destination,
        destination_identity.clone(),
        source.private_integrity(),
        manifest.layout,
    )?;
    let mut wtx = target
        .database()
        .begin_write()
        .map_err(|error| error.to_string())?;
    wtx.set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    let mut target_manifest = manifest.clone();
    target_manifest.physical_identity = destination_identity.clone();
    target_manifest.authority_epoch = target_manifest
        .authority_epoch
        .checked_add(1)
        .ok_or_else(|| "mutation authority epoch exhausted during backup".to_string())?;
    target_manifest.validate()?;
    let manifest_bytes = encode_bounded(&target_manifest, "strict backup owner manifest")?;
    wtx.open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?
        .insert("manifest", manifest_bytes.as_slice())
        .map_err(|error| error.to_string())?;
    let binding_rows = copy_bindings(&rtx, &wtx, target.incarnation())?;
    let ledger_rows = copy_ledger_rows(&rtx, &wtx)?;
    let owner_rows = copy_declared_owner_tables(&rtx, &wtx, manifest.layout)?;
    let target_evidence = strict_snapshot_write(&wtx, manifest.layout)?;
    validate_copied_table_evidence(&source_evidence, &target_evidence)?;
    wtx.commit().map_err(|error| error.to_string())?;
    drop(rtx);
    drop(target);

    let reopened = open_physical(
        destination,
        destination_identity,
        source.private_integrity(),
        manifest.layout,
    )?;
    let evidence = strict_evidence_of(&reopened)?;
    if evidence != target_evidence {
        return Err("strict backup fingerprint changed after reopen".to_string());
    }
    if evidence.ledger_rows != ledger_rows + binding_rows + 2 || evidence.owner_rows != owner_rows {
        return Err("strict backup row counts changed after reopen".to_string());
    }
    Ok(evidence)
}

fn validated_source_manifest(
    store: &PhysicalStore,
    transaction: &ReadTransaction,
) -> Result<OwnerManifest, String> {
    store.validate_physical_root()?;
    let cached = store.manifest();
    let persisted = validate_manifest_read(transaction, &cached.physical_identity, cached.layout)?;
    if persisted != *cached {
        return Err("strict recovery manifest authority changed".to_string());
    }
    Ok(persisted)
}

pub(crate) fn strict_snapshot_read(
    rtx: &ReadTransaction,
    layout: OwnerLayout,
) -> Result<StrictRecoveryEvidence, String> {
    strict_snapshot(HashSnapshot::Read(rtx), layout)
}

pub(crate) fn strict_snapshot_write(
    wtx: &WriteTransaction,
    layout: OwnerLayout,
) -> Result<StrictRecoveryEvidence, String> {
    // Census before any typed open: a write-side open must never recreate a
    // missing table or hide an undeclared normal/multimap table.
    validate_declared_tables_write(wtx, layout)?;
    strict_snapshot(HashSnapshot::Write(wtx), layout)
}

fn strict_snapshot(
    snapshot: HashSnapshot<'_>,
    layout: OwnerLayout,
) -> Result<StrictRecoveryEvidence, String> {
    let mut hasher = Sha256::new();
    let mut tables = Vec::new();
    let ledger_rows = hash_ledger(snapshot, &mut hasher, &mut tables)?;
    let owner_rows = hash_declared_owner_tables(snapshot, layout, &mut hasher, &mut tables)?;
    Ok(StrictRecoveryEvidence {
        ledger_rows,
        owner_rows,
        fingerprint: hasher.finalize().into(),
        tables,
    })
}

/// Hash every ledger table of the authoritative list -- census, evidence and
/// the adoption TOCTOU guard all read this one sweep, so a table added to
/// `visit_ledger_tables!` is covered by all three at once.
fn hash_ledger(
    snapshot: HashSnapshot<'_>,
    hasher: &mut Sha256,
    tables: &mut Vec<StrictTableEvidence>,
) -> Result<u64, String> {
    let mut rows = 0;
    macro_rules! visit {
        ($table:expr) => {{
            let (count, fingerprint) = snapshot.hash_table(hasher, $table)?;
            rows += count;
            tables.push(StrictTableEvidence {
                table_id: $table.name().to_string(),
                rows: count,
                fingerprint,
            });
        }};
    }
    visit_ledger_tables!(visit);
    Ok(rows)
}

/// Copy every ledger row of the authoritative list. The three physical-identity
/// tables are excluded because the backup re-anchors them to the destination
/// incarnation (`copy_bindings`, the manifest write, and `create_physical`).
fn copy_ledger_rows(source: &ReadTransaction, target: &WriteTransaction) -> Result<u64, String> {
    let mut rows = 0;
    macro_rules! copy {
        ($table:expr) => {{
            rows += copy_table(source, target, $table)?;
        }};
    }
    visit_ledger_content_tables!(copy);
    Ok(rows)
}

fn copy_bindings(
    source: &ReadTransaction,
    target: &WriteTransaction,
    root: &StoreIncarnation,
) -> Result<u64, String> {
    let source_table = source
        .open_table(SCOPE_BINDINGS)
        .map_err(|error| error.to_string())?;
    let mut target_table = target
        .open_table(SCOPE_BINDINGS)
        .map_err(|error| error.to_string())?;
    let mut rows = 0;
    for row in source_table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let mut binding: ScopeBinding = decode_ledger_record(value.value())?;
        binding.store_identity_digest = root.identity_digest();
        let bytes = encode_bounded(&binding, "strict backup scope binding")?;
        target_table
            .insert(key.value(), bytes.as_slice())
            .map_err(|error| error.to_string())?;
        rows += 1;
    }
    Ok(rows)
}

pub(crate) fn copy_table<K, V>(
    source: &ReadTransaction,
    target: &WriteTransaction,
    definition: TableDefinition<'static, K, V>,
) -> Result<u64, String>
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
    let mut rows = 0;
    for row in source_table.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        target_table
            .insert(key.value(), value.value())
            .map_err(|error| error.to_string())?;
        rows += 1;
    }
    Ok(rows)
}

#[derive(Clone, Copy)]
pub(crate) enum HashSnapshot<'a> {
    Read(&'a ReadTransaction),
    Write(&'a WriteTransaction),
}

impl HashSnapshot<'_> {
    pub(crate) fn hash_table<K, V>(
        self,
        hasher: &mut Sha256,
        definition: TableDefinition<'static, K, V>,
    ) -> Result<(u64, [u8; 32]), String>
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        let mut table_hasher = Sha256::new();
        hash_tag(&mut table_hasher, definition.name().as_bytes());
        let rows = match self {
            Self::Read(transaction) => {
                let table = transaction
                    .open_table(definition)
                    .map_err(|error| error.to_string())?;
                hash_rows(
                    &mut table_hasher,
                    table.iter().map_err(|error| error.to_string())?,
                )?
            }
            Self::Write(transaction) => {
                let table = transaction
                    .open_table(definition)
                    .map_err(|error| error.to_string())?;
                hash_rows(
                    &mut table_hasher,
                    table.iter().map_err(|error| error.to_string())?,
                )?
            }
        };
        let fingerprint: [u8; 32] = table_hasher.finalize().into();
        hash_tag(hasher, &fingerprint);
        Ok((rows, fingerprint))
    }
}

fn hash_rows<K, V>(hasher: &mut Sha256, rows: redb::Range<'_, K, V>) -> Result<u64, String>
where
    K: Key + 'static,
    V: Value + 'static,
{
    let mut count = 0;
    for row in rows {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let key = key.value();
        let value = value.value();
        hash_tag(hasher, K::as_bytes(&key).as_ref());
        hash_tag(hasher, V::as_bytes(&value).as_ref());
        count += 1;
    }
    Ok(count)
}

fn hash_tag(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn validate_copied_table_evidence(
    source: &StrictRecoveryEvidence,
    target: &StrictRecoveryEvidence,
) -> Result<(), String> {
    if source.tables.len() != target.tables.len() {
        return Err("strict backup table cardinality changed".to_string());
    }
    for (source_table, target_table) in source.tables.iter().zip(&target.tables) {
        if source_table.table_id != target_table.table_id || source_table.rows != target_table.rows
        {
            return Err("strict backup per-table row evidence changed".to_string());
        }
        let reanchored = matches!(
            source_table.table_id.as_str(),
            "mutation_store_root_v1" | "mutation_scope_bindings_v1" | "mutation_owner_manifest_v1"
        );
        if !reanchored && source_table.fingerprint != target_table.fingerprint {
            return Err("strict backup copied-table fingerprint changed".to_string());
        }
    }
    Ok(())
}
