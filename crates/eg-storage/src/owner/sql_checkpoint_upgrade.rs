//! Explicit offline creation of the SQL provider-checkpoint table.
//!
//! Ordinary opens and recovery adoption remain current-format-only. This
//! one-time migration accepts exactly one frozen predecessor, changes only
//! its owner manifest and empty checkpoint table, and never reanchors a copy.

use super::domain::SqlOwner;
use super::identity::PhysicalStoreIdentity;
use super::layout::OwnerLayout;
use super::manifest_io::read_manifest_slot;
use super::registry::{sql_pre_checkpoint_evidence, SQL_SOURCE_CHECKPOINTS};
use super::validate_declared_tables_write;
use crate::codec::encode_bounded;
use crate::physical::incarnation::StoreIncarnation;
use crate::physical::integrity::{authenticate_with, PrivatePayloadIntegrity};
use crate::physical::manifest::OwnerManifest;
use crate::physical::root::{store_handle, validate_handle_write, validate_incarnation_read};
use crate::recovery::evidence::{HashSnapshot, StrictRecoveryEvidence};
use crate::recovery::validate::validate_recovery_content;
use crate::tables::OWNER_MANIFEST;
use crate::{StorageKernel, StoreOpenOptions};
use redb::{
    Database, MultimapTableHandle, ReadTransaction, ReadableDatabase, TableHandle, WriteTransaction,
};
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod contract;
mod preview;

// Both inspection and the exclusive upgrade have an explicit small page cache.
const UPGRADE_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// Explicit private LOCAL staging and disk budget for offline SQL inspection.
/// The budget bounds both source bytes and the recovered preview, not memory.
pub struct SqlSourceCheckpointInspectionOptions {
    staging_root: PathBuf,
    max_bytes: u64,
}

impl SqlSourceCheckpointInspectionOptions {
    pub fn new(staging_root: PathBuf, max_bytes: u64) -> Result<Self, String> {
        preview::validate_options(&staging_root, max_bytes)?;
        Ok(Self {
            staging_root,
            max_bytes,
        })
    }
}

/// Read-only evidence for one exact physical SQL predecessor. It is consumed
/// by the exclusive upgrade and cannot be cloned, serialized or forged.
///
/// ```compile_fail
/// # use eg_storage::ValidatedSqlSourceCheckpointUpgrade;
/// fn duplicate(token: ValidatedSqlSourceCheckpointUpgrade) {
///     let _second = token.clone();
/// }
/// ```
pub struct ValidatedSqlSourceCheckpointUpgrade {
    path: PathBuf,
    physical_path: PathBuf,
    pinned_file: File,
    incarnation: StoreIncarnation,
    manifest: OwnerManifest,
    evidence: StrictRecoveryEvidence,
    source_fingerprint: [u8; 32],
    source_bytes: u64,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
}

/// Authority transition only: no source rows, replay identities or SQL source
/// epochs are rewritten by this metadata migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlSourceCheckpointUpgradeReport {
    pub previous_layout_digest: [u8; 32],
    pub current_layout_digest: [u8; 32],
    pub previous_authority_digest: [u8; 32],
    pub current_authority_digest: [u8; 32],
    pub previous_authority_epoch: u64,
    pub current_authority_epoch: u64,
}

/// Inspect an offline SQL file without write authority. The expected physical
/// identity is mandatory; a copied file whose recorded root belongs to another
/// inode is refused. Sealed private payloads require their real authenticator.
pub fn inspect_sql_source_checkpoint_upgrade(
    path: &Path,
    expected_physical_identity: PhysicalStoreIdentity,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    options: SqlSourceCheckpointInspectionOptions,
) -> Result<ValidatedSqlSourceCheckpointUpgrade, String> {
    preview::validate_options(&options.staging_root, options.max_bytes)?;
    let pinned_file = File::open(path).map_err(|error| error.to_string())?;
    pinned_file
        .try_lock_shared()
        .map_err(|error| error.to_string())?;
    let (incarnation, physical_path) = StoreIncarnation::derive(path)?;
    validate_descriptor_path(&pinned_file, path)?;
    let (manifest, evidence, source_fingerprint) = preview::inspect(
        &pinned_file,
        &incarnation,
        &expected_physical_identity,
        private_integrity.as_deref(),
        &options,
    )?;
    let source_bytes = pinned_file
        .metadata()
        .map_err(|error| error.to_string())?
        .len();
    // Check overflow before granting the upgrade token, too.
    contract::successor(&manifest)?;
    validate_descriptor_path(&pinned_file, path)?;
    if StoreIncarnation::derive(path)? != (incarnation.clone(), physical_path.clone()) {
        return Err("SQL checkpoint physical file changed during inspection".to_string());
    }
    // Retain the descriptor identity, not a shared lock that would obstruct the
    // later exclusive open. Intervening changes are caught by its fingerprint.
    pinned_file.unlock().map_err(|error| error.to_string())?;
    Ok(ValidatedSqlSourceCheckpointUpgrade {
        path: path.to_path_buf(),
        physical_path,
        pinned_file,
        incarnation,
        manifest,
        evidence,
        source_fingerprint,
        source_bytes,
        private_integrity,
    })
}

/// Consume an inspected predecessor and perform one Immediate durable commit.
/// The exclusive write rechecks all token evidence before creating any table.
/// This API is never called by normal SQL opens or the server startup path.
pub fn upgrade_sql_source_checkpoints(
    token: ValidatedSqlSourceCheckpointUpgrade,
) -> Result<(StorageKernel, SqlSourceCheckpointUpgradeReport), String> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&token.path)
        .map_err(|error| error.to_string())?;
    upgrade_opened_predecessor(token, file)
}

/// Bind the exact descriptor before redb can initialize or repair any bytes.
/// Keeping the clone lets the exclusive transaction recheck its actual file.
fn upgrade_opened_predecessor(
    token: ValidatedSqlSourceCheckpointUpgrade,
    file: File,
) -> Result<(StorageKernel, SqlSourceCheckpointUpgradeReport), String> {
    validate_upgrade_descriptor(&token, &file)?;
    let (backend, opened_file) = preview::exclusive_backend(file, &token.path)?;
    validate_upgrade_descriptor(&token, &opened_file)?;
    preview::validate_fingerprint(&opened_file, token.source_bytes, token.source_fingerprint)?;
    let database = upgrade_builder()
        .create_with_backend(backend)
        .map_err(|error| error.to_string())?;
    validate_upgrade_descriptor(&token, &opened_file)?;
    {
        let read = database.begin_read().map_err(|error| error.to_string())?;
        let (manifest, _) = inspect_predecessor(
            &read,
            &token.incarnation,
            &token.manifest.physical_identity,
            token.private_integrity.as_deref(),
        )?;
        if manifest != token.manifest {
            return Err("SQL checkpoint predecessor manifest changed after inspection".to_string());
        }
    }
    let current = commit_checkpoint_layout(&database, &token, &opened_file)?;
    let report = upgrade_report(&token, &current);
    drop(database);
    drop(opened_file);
    let options = StoreOpenOptions::default().with_cache_bytes(UPGRADE_CACHE_BYTES)?;
    let kernel = StorageKernel::open_owner_with::<SqlOwner>(
        &token.path,
        current.physical_identity,
        token.private_integrity,
        options,
    )?;
    Ok((kernel, report))
}

fn upgrade_builder() -> redb::Builder {
    let mut builder = Database::builder();
    builder.set_cache_size(UPGRADE_CACHE_BYTES);
    builder
}

fn inspect_predecessor(
    read: &ReadTransaction,
    incarnation: &StoreIncarnation,
    physical_identity: &PhysicalStoreIdentity,
    private_integrity: Option<&dyn PrivatePayloadIntegrity>,
) -> Result<(OwnerManifest, StrictRecoveryEvidence), String> {
    validate_incarnation_read(read, incarnation)?;
    let table = read
        .open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?;
    let manifest = read_manifest_slot(&table)?;
    contract::validate_predecessor(&manifest)?;
    if manifest.physical_identity != *physical_identity {
        return Err(
            "SQL checkpoint predecessor physical owner does not match expectation".to_string(),
        );
    }
    validate_predecessor_census(
        read.list_tables().map_err(|error| error.to_string())?,
        read.list_multimap_tables()
            .map_err(|error| error.to_string())?,
    )?;
    // Typed strict hashing is also the old-table key/value codec validation.
    let evidence = sql_pre_checkpoint_evidence(HashSnapshot::Read(read))?;
    let authenticate =
        |sealed: &[u8], digest: &str| authenticate_with(private_integrity, sealed, digest);
    validate_recovery_content(incarnation, read, &authenticate)?;
    Ok((manifest, evidence))
}

fn validate_predecessor_census<N, M>(normal: N, multimap: M) -> Result<(), String>
where
    N: IntoIterator,
    N::Item: TableHandle,
    M: IntoIterator,
    M::Item: MultimapTableHandle,
{
    let expected: BTreeSet<_> = contract::predecessor_contracts()?
        .into_iter()
        .map(|contract| contract.table_id)
        .collect();
    let actual: BTreeSet<_> = normal
        .into_iter()
        .map(|table| table.name().to_string())
        .collect();
    if actual != expected || multimap.into_iter().next().is_some() {
        return Err("SQL checkpoint predecessor table census is not exact".to_string());
    }
    Ok(())
}

fn validate_token_physical_root(token: &ValidatedSqlSourceCheckpointUpgrade) -> Result<(), String> {
    let (incarnation, physical_path) = StoreIncarnation::derive(&token.path)?;
    if incarnation != token.incarnation || physical_path != token.physical_path {
        return Err("SQL checkpoint physical file changed after inspection".to_string());
    }
    Ok(())
}

/// Descriptor identity is compared with the token's pinned file, independently
/// of the path. An ABA rename cannot lend the original inode's authority to a
/// copied file that redb happened to open.
fn validate_upgrade_descriptor(
    token: &ValidatedSqlSourceCheckpointUpgrade,
    opened_file: &File,
) -> Result<(), String> {
    validate_same_descriptor(&token.pinned_file, opened_file)?;
    validate_descriptor_path(opened_file, &token.path)?;
    validate_token_physical_root(token)
}

fn validate_descriptor_path(file: &File, path: &Path) -> Result<(), String> {
    let current = File::open(path).map_err(|error| error.to_string())?;
    validate_same_descriptor(file, &current)
}

#[cfg(unix)]
fn validate_same_descriptor(expected: &File, actual: &File) -> Result<(), String> {
    let expected = expected.metadata().map_err(|error| error.to_string())?;
    let actual = actual.metadata().map_err(|error| error.to_string())?;
    if !expected.is_file()
        || !actual.is_file()
        || expected.dev() != actual.dev()
        || expected.ino() != actual.ino()
        || actual.len() == 0
    {
        return Err(
            "SQL checkpoint opened file is not the pinned physical predecessor".to_string(),
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_same_descriptor(_expected: &File, _actual: &File) -> Result<(), String> {
    Err("SQL checkpoint descriptor identity is unsupported on this platform".to_string())
}

fn commit_checkpoint_layout(
    database: &Database,
    token: &ValidatedSqlSourceCheckpointUpgrade,
    opened_file: &File,
) -> Result<OwnerManifest, String> {
    let write = begin_checkpoint_write(database, token, opened_file)?;
    let old = unchanged_predecessor(&write, token)?;
    let current = contract::successor(&old)?;
    stage_checkpoint_layout(&write, &current)?;
    // Recheck both the actual database descriptor and its published path after
    // all staging, immediately before the only durable commit.
    validate_upgrade_descriptor(token, opened_file)?;
    write.commit().map_err(|error| error.to_string())?;
    #[cfg(test)]
    tests::crash_at("after_commit");
    Ok(current)
}

/// Open the one immediate-durability write and prove, before any typed open,
/// that it is the validated descriptor, handle and predecessor table census.
fn begin_checkpoint_write(
    database: &Database,
    token: &ValidatedSqlSourceCheckpointUpgrade,
    opened_file: &File,
) -> Result<WriteTransaction, String> {
    let mut write = database.begin_write().map_err(|error| error.to_string())?;
    write
        .set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    validate_upgrade_descriptor(token, opened_file)?;
    validate_handle_write(&store_handle(token.incarnation.clone()), &write)?;
    // The actual census must precede every write-side typed open.
    validate_predecessor_census(
        write.list_tables().map_err(|error| error.to_string())?,
        write
            .list_multimap_tables()
            .map_err(|error| error.to_string())?,
    )?;
    Ok(write)
}

/// The predecessor manifest and SQL evidence inside the write must still be
/// exactly the ones the inspection token validated.
fn unchanged_predecessor(
    write: &WriteTransaction,
    token: &ValidatedSqlSourceCheckpointUpgrade,
) -> Result<OwnerManifest, String> {
    let old = {
        let table = write
            .open_table(OWNER_MANIFEST)
            .map_err(|error| error.to_string())?;
        read_manifest_slot(&table)?
    };
    contract::validate_predecessor(&old)?;
    if old != token.manifest
        || sql_pre_checkpoint_evidence(HashSnapshot::Write(write))? != token.evidence
    {
        return Err("SQL checkpoint predecessor changed before atomic upgrade".to_string());
    }
    Ok(old)
}

/// Create the empty checkpoint table and publish the successor manifest, then
/// validate the staged manifest and declared tables.
fn stage_checkpoint_layout(
    write: &WriteTransaction,
    current: &OwnerManifest,
) -> Result<(), String> {
    #[cfg(test)]
    tests::crash_at("before_table");
    write
        .open_table(SQL_SOURCE_CHECKPOINTS)
        .map_err(|error| error.to_string())?;
    #[cfg(test)]
    tests::crash_at("after_table");
    let encoded = encode_bounded(current, "SQL checkpoint upgraded owner manifest")?;
    write
        .open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?
        .insert("manifest", encoded.as_slice())
        .map_err(|error| error.to_string())?;
    #[cfg(test)]
    tests::crash_at("after_manifest");
    super::validate_manifest_write(write, &current.physical_identity, OwnerLayout::Sql)?;
    validate_declared_tables_write(write, OwnerLayout::Sql)
}

fn upgrade_report(
    token: &ValidatedSqlSourceCheckpointUpgrade,
    current: &OwnerManifest,
) -> SqlSourceCheckpointUpgradeReport {
    SqlSourceCheckpointUpgradeReport {
        previous_layout_digest: token.manifest.layout_digest,
        current_layout_digest: current.layout_digest,
        previous_authority_digest: token.manifest.authority_digest(&token.incarnation),
        current_authority_digest: current.authority_digest(&token.incarnation),
        previous_authority_epoch: token.manifest.authority_epoch,
        current_authority_epoch: current.authority_epoch,
    }
}

#[cfg(test)]
mod tests;
