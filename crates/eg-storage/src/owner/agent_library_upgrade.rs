//! Explicit offline, data-preserving owner-table transitions.
//!
//! Normal opens never run this migration. Inspection uses the same bounded,
//! anonymous native-recovery preview as the SQL owner upgrader; the exclusive
//! writer rechecks the pinned descriptor, byte fingerprint, table census and
//! strict typed evidence before its one immediate durable commit.

use super::domain::{AgentLibraryOwner, GraphShardOwner};
use super::graph_shard::{
    REPOSITORY_ENRICHMENT_BUDGETS, REPOSITORY_ENRICHMENT_PARKS, REPOSITORY_ENRICHMENT_SUPERSESSIONS,
};
use super::identity::PhysicalStoreIdentity;
use super::layout::{layout_digest_over, OwnerLayout};
use super::lineage::{AGENT_LIBRARY_BEFORE_MCP_CATALOG, GRAPH_SHARD_BEFORE_ENRICHMENT};
use super::manifest_io::read_manifest_slot;
use super::persisted_layout::LayoutPredecessor;
use super::registry::{predecessor_evidence, MCP_CATALOG_CONFIGS, MCP_CATALOG_SCOPES};
use super::sql_checkpoint_upgrade::{
    preview, upgrade_builder, validate_descriptor_path, validate_same_descriptor,
    SqlSourceCheckpointInspectionOptions, UPGRADE_CACHE_BYTES,
};
use super::validate_declared_tables_write;
use crate::codec::encode_bounded;
use crate::physical::incarnation::{StoreIncarnation, STORAGE_KERNEL_SCHEMA_VERSION};
use crate::physical::integrity::{authenticate_with, PrivatePayloadIntegrity};
use crate::physical::manifest::{OwnerManifest, TableContract, TableOwnership};
use crate::physical::root::{store_handle, validate_handle_write, validate_incarnation_read};
use crate::recovery::evidence::{HashSnapshot, StrictRecoveryEvidence};
use crate::recovery::validate::validate_recovery_content;
use crate::tables::OWNER_MANIFEST;
use crate::{StorageKernel, StoreOpenOptions};
use redb::{Database, MultimapTableHandle, ReadTransaction, ReadableDatabase, TableHandle};
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub type AgentLibraryInspectionOptions = SqlSourceCheckpointInspectionOptions;
pub type GraphShardInspectionOptions = SqlSourceCheckpointInspectionOptions;

#[derive(Clone, Copy, PartialEq, Eq)]
enum UpgradeTarget {
    AgentLibrary,
    GraphShard,
}

impl UpgradeTarget {
    fn layout(self) -> OwnerLayout {
        match self {
            Self::AgentLibrary => OwnerLayout::AgentLibrary,
            Self::GraphShard => OwnerLayout::GraphShard,
        }
    }

    fn predecessor(self) -> &'static LayoutPredecessor {
        match self {
            Self::AgentLibrary => &AGENT_LIBRARY_BEFORE_MCP_CATALOG,
            Self::GraphShard => &GRAPH_SHARD_BEFORE_ENRICHMENT,
        }
    }

    fn pinned(self) -> [u8; 32] {
        match self {
            Self::AgentLibrary => super::lineage::AGENT_LIBRARY_PRE_MCP_CATALOG_DIGEST,
            Self::GraphShard => super::lineage::GRAPH_SHARD_PRE_ENRICHMENT_DIGEST,
        }
    }
}

/// An exact predecessor inspection, tied to the source descriptor and bytes.
/// This token cannot be cloned or constructed outside the storage kernel.
pub struct ValidatedOwnerLayoutUpgrade {
    target: UpgradeTarget,
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
pub type ValidatedAgentLibraryUpgrade = ValidatedOwnerLayoutUpgrade;
pub type ValidatedGraphShardUpgrade = ValidatedOwnerLayoutUpgrade;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerLayoutUpgradeReport {
    pub previous_layout_digest: [u8; 32],
    pub current_layout_digest: [u8; 32],
    pub previous_authority_epoch: u64,
    pub current_authority_epoch: u64,
}
pub type AgentLibraryUpgradeReport = OwnerLayoutUpgradeReport;
pub type GraphShardUpgradeReport = OwnerLayoutUpgradeReport;

pub fn inspect_agent_library_mcp_catalog_upgrade(
    path: &Path,
    expected_physical_identity: PhysicalStoreIdentity,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    options: AgentLibraryInspectionOptions,
) -> Result<ValidatedAgentLibraryUpgrade, String> {
    inspect_layout_upgrade(
        path,
        expected_physical_identity,
        private_integrity,
        options,
        UpgradeTarget::AgentLibrary,
    )
}

pub fn inspect_graph_shard_enrichment_upgrade(
    path: &Path,
    expected_physical_identity: PhysicalStoreIdentity,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    options: GraphShardInspectionOptions,
) -> Result<ValidatedGraphShardUpgrade, String> {
    inspect_layout_upgrade(
        path,
        expected_physical_identity,
        private_integrity,
        options,
        UpgradeTarget::GraphShard,
    )
}

fn inspect_layout_upgrade(
    path: &Path,
    expected_physical_identity: PhysicalStoreIdentity,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    options: SqlSourceCheckpointInspectionOptions,
    target: UpgradeTarget,
) -> Result<ValidatedOwnerLayoutUpgrade, String> {
    let pinned_file = File::open(path).map_err(|error| error.to_string())?;
    pinned_file
        .try_lock_shared()
        .map_err(|error| error.to_string())?;
    let (incarnation, physical_path) = StoreIncarnation::derive(path)?;
    validate_descriptor_path(&pinned_file, path)?;
    let (manifest, evidence, source_fingerprint) = preview::inspect_with(
        &pinned_file,
        &incarnation,
        &expected_physical_identity,
        private_integrity.as_deref(),
        &options,
        match target {
            UpgradeTarget::AgentLibrary => inspect_agent_preview_read,
            UpgradeTarget::GraphShard => inspect_graph_preview_read,
        },
    )?;
    let source_bytes = pinned_file
        .metadata()
        .map_err(|error| error.to_string())?
        .len();
    successor(&manifest, target)?;
    validate_descriptor_path(&pinned_file, path)?;
    if StoreIncarnation::derive(path)? != (incarnation.clone(), physical_path.clone()) {
        return Err("owner physical file changed during inspection".into());
    }
    pinned_file.unlock().map_err(|error| error.to_string())?;
    Ok(ValidatedOwnerLayoutUpgrade {
        target,
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

pub fn upgrade_agent_library_mcp_catalog(
    token: ValidatedAgentLibraryUpgrade,
) -> Result<(StorageKernel, AgentLibraryUpgradeReport), String> {
    upgrade_layout(token, UpgradeTarget::AgentLibrary)
}

pub fn upgrade_graph_shard_enrichment(
    token: ValidatedGraphShardUpgrade,
) -> Result<(StorageKernel, GraphShardUpgradeReport), String> {
    upgrade_layout(token, UpgradeTarget::GraphShard)
}

fn upgrade_layout(
    token: ValidatedOwnerLayoutUpgrade,
    target: UpgradeTarget,
) -> Result<(StorageKernel, OwnerLayoutUpgradeReport), String> {
    if token.target != target {
        return Err("owner layout upgrade token targets another store".into());
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&token.path)
        .map_err(|error| error.to_string())?;
    validate_descriptor(&token, &file)?;
    let (backend, opened_file) = preview::exclusive_backend(file, &token.path)?;
    validate_descriptor(&token, &opened_file)?;
    preview::validate_fingerprint(&opened_file, token.source_bytes, token.source_fingerprint)?;
    let database = upgrade_builder()
        .create_with_backend(backend)
        .map_err(|error| error.to_string())?;
    validate_descriptor(&token, &opened_file)?;
    {
        let read = database.begin_read().map_err(|error| error.to_string())?;
        let (manifest, _) = inspect_predecessor(
            &read,
            &token.incarnation,
            &token.manifest.physical_identity,
            token.private_integrity.as_deref(),
            target,
        )?;
        if manifest != token.manifest {
            return Err("owner predecessor manifest changed after inspection".into());
        }
    }
    let current = commit_upgrade(&database, &token, &opened_file, target)?;
    let report = OwnerLayoutUpgradeReport {
        previous_layout_digest: token.manifest.layout_digest,
        current_layout_digest: current.layout_digest,
        previous_authority_epoch: token.manifest.authority_epoch,
        current_authority_epoch: current.authority_epoch,
    };
    drop(database);
    drop(opened_file);
    let options = StoreOpenOptions::default().with_cache_bytes(UPGRADE_CACHE_BYTES)?;
    let kernel = match target {
        UpgradeTarget::AgentLibrary => StorageKernel::open_owner_with::<AgentLibraryOwner>(
            &token.path,
            current.physical_identity,
            token.private_integrity,
            options,
        )?,
        UpgradeTarget::GraphShard => StorageKernel::open_owner_with::<GraphShardOwner>(
            &token.path,
            current.physical_identity,
            token.private_integrity,
            options,
        )?,
    };
    Ok((kernel, report))
}

fn contracts_for(
    manifest: &OwnerManifest,
    target: UpgradeTarget,
) -> Result<Vec<TableContract>, String> {
    let contracts: Vec<_> = super::contract::expected_table_contracts(target.layout())
        .into_iter()
        .filter(|contract| {
            contract.ownership == TableOwnership::Ledger
                || target
                    .predecessor()
                    .owner_tables
                    .contains(&contract.table_id.as_str())
        })
        .collect();
    // This is the frozen digest of the actual predecessor format. A file
    // with a self-consistent but fabricated old table contract is not admitted.
    let pinned = target.pinned();
    if manifest.layout_digest != pinned
        || layout_digest_over(target.layout(), &contracts) != pinned
        || manifest.tables != contracts
    {
        return Err("owner manifest is not the pinned layout predecessor".into());
    }
    Ok(contracts)
}

fn validate_predecessor(manifest: &OwnerManifest, target: UpgradeTarget) -> Result<(), String> {
    manifest.physical_identity.validate()?;
    if manifest.schema_version != STORAGE_KERNEL_SCHEMA_VERSION
        || manifest.layout != target.layout()
        || manifest.layout_digest == target.layout().digest()
    {
        return Err("owner manifest is not an exact layout predecessor".into());
    }
    contracts_for(manifest, target)?;
    Ok(())
}

fn successor(manifest: &OwnerManifest, target: UpgradeTarget) -> Result<OwnerManifest, String> {
    let mut next = OwnerManifest::new(manifest.physical_identity.clone(), target.layout())?;
    next.authority_epoch = manifest
        .authority_epoch
        .checked_add(1)
        .ok_or_else(|| "owner layout authority epoch exhausted".to_string())?;
    next.validate()?;
    Ok(next)
}

fn inspect_agent_preview_read(
    database: &Database,
    incarnation: &StoreIncarnation,
    physical: &PhysicalStoreIdentity,
    integrity: Option<&dyn PrivatePayloadIntegrity>,
) -> Result<(OwnerManifest, StrictRecoveryEvidence), String> {
    let read = database.begin_read().map_err(|error| error.to_string())?;
    inspect_predecessor(
        &read,
        incarnation,
        physical,
        integrity,
        UpgradeTarget::AgentLibrary,
    )
}

fn inspect_graph_preview_read(
    database: &Database,
    incarnation: &StoreIncarnation,
    physical: &PhysicalStoreIdentity,
    integrity: Option<&dyn PrivatePayloadIntegrity>,
) -> Result<(OwnerManifest, StrictRecoveryEvidence), String> {
    let read = database.begin_read().map_err(|error| error.to_string())?;
    inspect_predecessor(
        &read,
        incarnation,
        physical,
        integrity,
        UpgradeTarget::GraphShard,
    )
}

fn inspect_predecessor(
    read: &ReadTransaction,
    incarnation: &StoreIncarnation,
    physical: &PhysicalStoreIdentity,
    integrity: Option<&dyn PrivatePayloadIntegrity>,
    target: UpgradeTarget,
) -> Result<(OwnerManifest, StrictRecoveryEvidence), String> {
    validate_incarnation_read(read, incarnation)?;
    let table = read
        .open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?;
    let manifest = read_manifest_slot(&table)?;
    validate_predecessor(&manifest, target)?;
    if manifest.physical_identity != *physical {
        return Err("layout predecessor physical owner mismatch".into());
    }
    validate_census(
        &manifest,
        target,
        read.list_tables().map_err(|error| error.to_string())?,
        read.list_multimap_tables()
            .map_err(|error| error.to_string())?,
    )?;
    let evidence = predecessor_evidence(
        HashSnapshot::Read(read),
        target.layout(),
        target.predecessor().owner_tables,
    )?;
    let authenticate = |sealed: &[u8], digest: &str| authenticate_with(integrity, sealed, digest);
    validate_recovery_content(incarnation, read, &authenticate)?;
    Ok((manifest, evidence))
}

fn validate_census<N, M>(
    manifest: &OwnerManifest,
    target: UpgradeTarget,
    normal: N,
    multimap: M,
) -> Result<(), String>
where
    N: IntoIterator,
    N::Item: TableHandle,
    M: IntoIterator,
    M::Item: MultimapTableHandle,
{
    let expected: BTreeSet<_> = contracts_for(manifest, target)?
        .into_iter()
        .map(|contract| contract.table_id)
        .collect();
    let actual: BTreeSet<_> = normal
        .into_iter()
        .map(|table| table.name().to_string())
        .collect();
    if actual != expected || multimap.into_iter().next().is_some() {
        return Err("layout predecessor table census is not exact".into());
    }
    Ok(())
}

fn validate_descriptor(token: &ValidatedAgentLibraryUpgrade, file: &File) -> Result<(), String> {
    validate_same_descriptor(&token.pinned_file, file)?;
    validate_descriptor_path(file, &token.path)?;
    if StoreIncarnation::derive(&token.path)?
        != (token.incarnation.clone(), token.physical_path.clone())
    {
        return Err("owner physical file changed after inspection".into());
    }
    Ok(())
}

fn commit_upgrade(
    database: &Database,
    token: &ValidatedOwnerLayoutUpgrade,
    file: &File,
    target: UpgradeTarget,
) -> Result<OwnerManifest, String> {
    let mut write = database.begin_write().map_err(|error| error.to_string())?;
    write
        .set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    validate_descriptor(token, file)?;
    validate_handle_write(&store_handle(token.incarnation.clone()), &write)?;
    validate_census(
        &token.manifest,
        target,
        write.list_tables().map_err(|error| error.to_string())?,
        write
            .list_multimap_tables()
            .map_err(|error| error.to_string())?,
    )?;
    let old = {
        let table = write
            .open_table(OWNER_MANIFEST)
            .map_err(|error| error.to_string())?;
        read_manifest_slot(&table)?
    };
    validate_predecessor(&old, target)?;
    if old != token.manifest
        || predecessor_evidence(
            HashSnapshot::Write(&write),
            target.layout(),
            target.predecessor().owner_tables,
        )? != token.evidence
    {
        return Err("owner predecessor changed before atomic upgrade".into());
    }
    let current = successor(&old, target)?;
    match target {
        UpgradeTarget::AgentLibrary => {
            write
                .open_table(MCP_CATALOG_CONFIGS)
                .map_err(|error| error.to_string())?;
            write
                .open_table(MCP_CATALOG_SCOPES)
                .map_err(|error| error.to_string())?;
        }
        UpgradeTarget::GraphShard => {
            write
                .open_table(REPOSITORY_ENRICHMENT_BUDGETS)
                .map_err(|error| error.to_string())?;
            write
                .open_table(REPOSITORY_ENRICHMENT_SUPERSESSIONS)
                .map_err(|error| error.to_string())?;
            write
                .open_table(REPOSITORY_ENRICHMENT_PARKS)
                .map_err(|error| error.to_string())?;
        }
    }
    let encoded = encode_bounded(&current, "upgraded owner manifest")?;
    write
        .open_table(OWNER_MANIFEST)
        .map_err(|error| error.to_string())?
        .insert("manifest", encoded.as_slice())
        .map_err(|error| error.to_string())?;
    super::validate_manifest_write(&write, &current.physical_identity, target.layout())?;
    validate_declared_tables_write(&write, target.layout())?;
    validate_descriptor(token, file)?;
    write.commit().map_err(|error| error.to_string())?;
    Ok(current)
}

#[cfg(test)]
mod tests;
