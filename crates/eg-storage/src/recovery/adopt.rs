use crate::owner::identity::PhysicalStoreIdentity;
use crate::owner::layout::OwnerLayout;
use crate::owner::registry::is_mutation_authority_marker;
use crate::owner::{read_current_manifest, validate_declared_owner_tables, validate_manifest_read};
use crate::physical::binding::decode_binding;
use crate::physical::incarnation::{require_persisted_root, StoreIncarnation};
use crate::physical::integrity::{authenticate_with, PrivatePayloadIntegrity};
use crate::physical::manifest::{OwnerManifest, OwnerManifestDigest};
use crate::physical::root::{store_handle, PhysicalStore};
use crate::recovery::authority::{reanchor_staged_store_authority, rewrite_store_authority};
use crate::recovery::evidence::{
    strict_evidence_of, strict_snapshot_read, strict_snapshot_write, StrictRecoveryEvidence,
};
use crate::recovery::validate::{validate_recovery_content, RecoveryStoreCounts};
use crate::tables::{SCOPE_BINDINGS, STORE_ROOT, VERSIONS};
use crate::StorageKernel;
use eg_types::mutation_batch::DurabilityDomain;
use eg_types::{IncarnationId, LogicalName, MutationScopeIdentity, ScopeTenantId};
use redb::{
    Database, ReadTransaction, ReadableDatabase, ReadableTable, TableHandle, WriteTransaction,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Recovery validation token bound to source identity, layout and epoch.
///
/// ```compile_fail
/// # use eg_storage::{adopt_recovery, PhysicalStoreIdentity, ValidatedRecoveryStore};
/// fn cannot_reuse(token: ValidatedRecoveryStore, destination: PhysicalStoreIdentity) {
///     let _ = adopt_recovery(token, destination.clone());
///     let _ = adopt_recovery(token, destination);
/// }
/// ```
pub struct ValidatedRecoveryStore {
    path: PathBuf,
    recorded_root: StoreIncarnation,
    manifest: OwnerManifest,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    evidence: StrictRecoveryEvidence,
}

/// One read-only-validated, exclusive staging image. The token exposes only
/// its dynamic manifest digest and is consumed by adoption.
///
/// ```compile_fail
/// # use eg_storage::{adopt_staged_mutation_store, ValidatedStagedMutationStore};
/// fn cannot_reuse(token: ValidatedStagedMutationStore) {
///     let _ = adopt_staged_mutation_store(token);
///     let _ = adopt_staged_mutation_store(token);
/// }
/// ```
pub struct ValidatedStagedMutationStore {
    path: PathBuf,
    physical_path: PathBuf,
    target_root: StoreIncarnation,
    recorded_root: StoreIncarnation,
    manifest: OwnerManifest,
    manifest_digest: OwnerManifestDigest,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    counts: RecoveryStoreCounts,
    evidence: StrictRecoveryEvidence,
}

impl ValidatedStagedMutationStore {
    pub fn owner_manifest_digest(&self) -> OwnerManifestDigest {
        self.manifest_digest
    }

    pub fn recovery_counts(&self) -> RecoveryStoreCounts {
        self.counts
    }
}

pub enum RecoveryExpectation {
    Plain,
    Mutation {
        physical_identity: PhysicalStoreIdentity,
        layout: OwnerLayout,
    },
}

pub struct ValidatedPlainRecoveryStore {
    path: PathBuf,
}

impl ValidatedPlainRecoveryStore {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub enum ClassifiedRecoveryStore {
    Plain(ValidatedPlainRecoveryStore),
    Mutation(Box<ValidatedRecoveryStore>),
}

/// Classify recovery only against an explicit operator expectation. A plain
/// store cannot be inferred from a failed mutation-store open.
pub fn classify_recovery_store(
    path: &Path,
    expectation: RecoveryExpectation,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
) -> Result<ClassifiedRecoveryStore, String> {
    match expectation {
        RecoveryExpectation::Mutation {
            physical_identity,
            layout,
        } => open_recovery(path, physical_identity, private_integrity, layout)
            .map(|store| ClassifiedRecoveryStore::Mutation(Box::new(store))),
        RecoveryExpectation::Plain => {
            let database = redb::ReadOnlyDatabase::open(path).map_err(|error| error.to_string())?;
            let rtx = database.begin_read().map_err(|error| error.to_string())?;
            let names = rtx
                .list_tables()
                .map_err(|error| error.to_string())?
                .map(|table| table.name().to_string())
                .collect::<std::collections::BTreeSet<_>>();
            let multimap_names = rtx
                .list_multimap_tables()
                .map_err(|error| error.to_string())?
                .map(|table| redb::MultimapTableHandle::name(&table).to_string())
                .collect::<std::collections::BTreeSet<_>>();
            if names.iter().any(|name| is_mutation_authority_marker(name))
                || multimap_names
                    .iter()
                    .any(|name| is_mutation_authority_marker(name))
            {
                return Err("plain recovery expectation encountered mutation authority".to_string());
            }
            Ok(ClassifiedRecoveryStore::Plain(
                ValidatedPlainRecoveryStore {
                    path: path.to_path_buf(),
                },
            ))
        }
    }
}

pub fn open_recovery(
    path: &Path,
    physical_identity: PhysicalStoreIdentity,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    layout: OwnerLayout,
) -> Result<ValidatedRecoveryStore, String> {
    let database = redb::ReadOnlyDatabase::open(path).map_err(|error| error.to_string())?;
    let rtx = database.begin_read().map_err(|error| error.to_string())?;
    let manifest = validate_manifest_read(&rtx, &physical_identity, layout)?;
    let root = rtx
        .open_table(STORE_ROOT)
        .map_err(|error| error.to_string())?;
    let recorded_root = require_persisted_root(&root)?;
    let authenticate = |sealed: &[u8], digest: &str| {
        authenticate_with(private_integrity.as_deref(), sealed, digest)
    };
    validate_recovery_content(&recorded_root, &rtx, &authenticate)?;
    validate_layout_binding_contract(&rtx, layout)?;
    validate_declared_owner_tables(&rtx, layout)?;
    let evidence = strict_snapshot_read(&rtx, layout)?;
    Ok(ValidatedRecoveryStore {
        path: path.to_path_buf(),
        recorded_root,
        manifest,
        private_integrity,
        evidence,
    })
}

/// Inspect one exclusive private staging file containing a complete current
/// mutation-store image without opening it for write. No table, row, or
/// legacy shape is inferred. The expected physical identity and layout bind
/// the image to the calling provider before a token can be issued.
/// `private_integrity` may be absent only when the validated private-payload
/// table is empty; any encrypted recovery row then fails closed.
pub fn inspect_staged_mutation_store(
    path: &Path,
    expected_physical_identity: PhysicalStoreIdentity,
    expected_layout: OwnerLayout,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
) -> Result<ValidatedStagedMutationStore, String> {
    let (target_root, physical_path) = StoreIncarnation::derive(path)?;
    let read_only = redb::ReadOnlyDatabase::open(path).map_err(|error| error.to_string())?;
    let rtx = read_only.begin_read().map_err(|error| error.to_string())?;
    let manifest = read_current_manifest(&rtx)?;
    if manifest.physical_identity != expected_physical_identity
        || manifest.layout != expected_layout
    {
        return Err("staged mutation owner authority does not match provider".to_string());
    }
    let manifest_digest = manifest.digest()?;
    let root = rtx
        .open_table(STORE_ROOT)
        .map_err(|error| error.to_string())?;
    let recorded_root = require_persisted_root(&root)?;
    let authenticate = |sealed: &[u8], digest: &str| {
        authenticate_with(private_integrity.as_deref(), sealed, digest)
    };
    let counts = validate_recovery_content(&recorded_root, &rtx, &authenticate)?;
    validate_layout_binding_contract(&rtx, manifest.layout)?;
    validate_declared_owner_tables(&rtx, manifest.layout)?;
    let evidence = strict_snapshot_read(&rtx, manifest.layout)?;
    Ok(ValidatedStagedMutationStore {
        path: path.to_path_buf(),
        physical_path,
        target_root,
        recorded_root,
        manifest,
        manifest_digest,
        private_integrity,
        counts,
        evidence,
    })
}

fn validate_layout_binding_contract(
    rtx: &ReadTransaction,
    layout: OwnerLayout,
) -> Result<(), String> {
    let bindings = rtx
        .open_table(SCOPE_BINDINGS)
        .map_err(|error| error.to_string())?;
    let versions = rtx
        .open_table(VERSIONS)
        .map_err(|error| error.to_string())?;
    validate_layout_binding_rows(&bindings, &versions, layout)
}

fn validate_layout_binding_contract_write(
    wtx: &WriteTransaction,
    layout: OwnerLayout,
) -> Result<(), String> {
    let bindings = wtx
        .open_table(SCOPE_BINDINGS)
        .map_err(|error| error.to_string())?;
    let versions = wtx
        .open_table(VERSIONS)
        .map_err(|error| error.to_string())?;
    validate_layout_binding_rows(&bindings, &versions, layout)
}

fn validate_layout_binding_rows<B, V>(
    bindings: &B,
    versions: &V,
    layout: OwnerLayout,
) -> Result<(), String>
where
    B: ReadableTable<&'static str, &'static [u8]>,
    V: ReadableTable<&'static str, u64>,
{
    if layout != OwnerLayout::Statechart {
        return Ok(());
    }
    let expected = MutationScopeIdentity::native(
        ScopeTenantId::new("native")?,
        DurabilityDomain::Lifecycle,
        LogicalName::new("statechart-instances")?,
        IncarnationId::new("incarnation:eg-statechart:statechart-instances:1")?,
    )?;
    let expected_key = expected.binding_digest().to_hex();
    let mut binding_rows = bindings.iter().map_err(|error| error.to_string())?;
    let Some(row) = binding_rows.next() else {
        return Err("statechart recovery requires its fixed serving binding".to_string());
    };
    let (key, value) = row.map_err(|error| error.to_string())?;
    let binding = decode_binding(value.value())?;
    if key.value() != expected_key || binding.identity != expected || binding_rows.next().is_some()
    {
        return Err("statechart recovery binding set differs from its fixed authority".to_string());
    }
    let mut version_rows = versions.iter().map_err(|error| error.to_string())?;
    let Some(row) = version_rows.next() else {
        return Err("statechart recovery requires its fixed scope version".to_string());
    };
    let (key, _) = row.map_err(|error| error.to_string())?;
    if key.value() != expected_key || version_rows.next().is_some() {
        return Err("statechart recovery version set differs from its fixed authority".to_string());
    }
    Ok(())
}

/// Consume a validated staging token and atomically reanchor only its root and
/// serving bindings. The complete source image and inode are rechecked under
/// the final write lock before any change.
pub fn adopt_staged_mutation_store(
    token: ValidatedStagedMutationStore,
) -> Result<StorageKernel, String> {
    let database = Database::open(&token.path).map_err(|error| error.to_string())?;
    let (adopted, physical_path) = StoreIncarnation::derive(&token.path)?;
    if adopted != token.target_root || physical_path != token.physical_path {
        return Err("staged mutation physical identity changed after validation".to_string());
    }
    let mut wtx = database.begin_write().map_err(|error| error.to_string())?;
    wtx.set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    if strict_snapshot_write(&wtx, token.manifest.layout)? != token.evidence {
        return Err("staged mutation image changed before atomic adoption".to_string());
    }
    reanchor_staged_store_authority(&wtx, &token.recorded_root, &token.manifest, &adopted)?;
    validate_layout_binding_contract_write(&wtx, token.manifest.layout)?;
    wtx.commit().map_err(|error| error.to_string())?;

    let manifest_digest = token.manifest.digest()?;
    let store = PhysicalStore::from_parts(
        database,
        store_handle(adopted),
        physical_path,
        token.private_integrity,
        token.manifest,
    );
    let adopted_evidence = strict_evidence_of(&store)?;
    validate_staged_reanchor(&token.evidence, &adopted_evidence)?;
    if manifest_digest != token.manifest_digest {
        return Err("staged mutation owner manifest changed during adoption".to_string());
    }
    Ok(StorageKernel::from_store(store))
}

fn validate_staged_reanchor(
    source: &StrictRecoveryEvidence,
    adopted: &StrictRecoveryEvidence,
) -> Result<(), String> {
    if source.tables.len() != adopted.tables.len()
        || source.ledger_rows != adopted.ledger_rows
        || source.owner_rows != adopted.owner_rows
    {
        return Err("staged mutation image cardinality changed during adoption".to_string());
    }
    for (before, after) in source.tables.iter().zip(&adopted.tables) {
        if before.table_id != after.table_id || before.rows != after.rows {
            return Err("staged mutation table identity changed during adoption".to_string());
        }
        if !matches!(
            before.table_id.as_str(),
            "mutation_store_root" | "mutation_scope_bindings"
        ) && before.fingerprint != after.fingerprint
        {
            return Err("staged mutation content changed during adoption".to_string());
        }
    }
    Ok(())
}

pub fn adopt_recovery(
    token: ValidatedRecoveryStore,
    destination_identity: PhysicalStoreIdentity,
) -> Result<StorageKernel, String> {
    let database = Database::open(&token.path).map_err(|error| error.to_string())?;
    let rtx = database.begin_read().map_err(|error| error.to_string())?;
    let current = validate_manifest_read(
        &rtx,
        &token.manifest.physical_identity,
        token.manifest.layout,
    )?;
    if current != token.manifest {
        return Err("recovery authority changed after validation".to_string());
    }
    let root = rtx
        .open_table(STORE_ROOT)
        .map_err(|error| error.to_string())?;
    if require_persisted_root(&root)? != token.recorded_root {
        return Err("recovery root changed after validation".to_string());
    }
    let authenticate = |sealed: &[u8], digest: &str| {
        authenticate_with(token.private_integrity.as_deref(), sealed, digest)
    };
    validate_recovery_content(&token.recorded_root, &rtx, &authenticate)?;
    validate_declared_owner_tables(&rtx, token.manifest.layout)?;
    if strict_snapshot_read(&rtx, token.manifest.layout)? != token.evidence {
        return Err("recovery snapshot changed after validation".to_string());
    }
    drop(root);
    drop(rtx);

    let (adopted, physical_path) = StoreIncarnation::derive(&token.path)?;
    let source_manifest = token.manifest.clone();
    let mut manifest = source_manifest.clone();
    manifest.physical_identity = destination_identity;
    manifest.authority_epoch = manifest
        .authority_epoch
        .checked_add(1)
        .ok_or_else(|| "mutation authority epoch exhausted".to_string())?;
    manifest.validate()?;
    commit_adoption(&database, &token, &source_manifest, &adopted, &manifest)?;
    Ok(StorageKernel::from_store(PhysicalStore::from_parts(
        database,
        store_handle(adopted),
        physical_path,
        token.private_integrity,
        manifest,
    )))
}

fn commit_adoption(
    database: &Database,
    token: &ValidatedRecoveryStore,
    source_manifest: &OwnerManifest,
    adopted: &StoreIncarnation,
    manifest: &OwnerManifest,
) -> Result<(), String> {
    let mut wtx = database.begin_write().map_err(|error| error.to_string())?;
    wtx.set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    if strict_snapshot_write(&wtx, source_manifest.layout)? != token.evidence {
        return Err("recovery snapshot changed before atomic adoption".to_string());
    }
    rewrite_store_authority(
        &wtx,
        &token.recorded_root,
        source_manifest,
        adopted,
        manifest,
    )?;
    validate_layout_binding_contract_write(&wtx, manifest.layout)?;
    wtx.commit().map_err(|error| error.to_string())
}
