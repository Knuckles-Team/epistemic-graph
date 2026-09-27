//! Explicit offline, data-preserving owner-table transitions.
//!
//! Normal opens never run this migration. Inspection uses the same bounded,
//! anonymous native-recovery preview as the SQL owner upgrader; the exclusive
//! writer rechecks the pinned descriptor, byte fingerprint, table census and
//! strict typed evidence before its one immediate durable commit.

use super::domain::{AgentLibraryOwner, GraphShardOwner};
use super::identity::PhysicalStoreIdentity;
use super::layout::{layout_digest_over, OwnerLayout};
use super::lineage::{AGENT_LIBRARY_BEFORE_MCP_CATALOG, GRAPH_SHARD_BEFORE_ENRICHMENT};
use super::persisted_layout::LayoutPredecessor;
use super::registry::predecessor_evidence;
use super::sql_checkpoint_upgrade::{
    open_upgrade_writer, open_verified_upgrade_database, pin_upgrade_source, preview,
    read_predecessor_manifest, validate_descriptor_path, validate_exact_table_census,
    validate_pinned_upgrade_file, validate_reopened_predecessor,
    SqlSourceCheckpointInspectionOptions, ValidatedUpgradeSource, UPGRADE_CACHE_BYTES,
};
use crate::physical::incarnation::{StoreIncarnation, STORAGE_KERNEL_SCHEMA_VERSION};
use crate::physical::integrity::{authenticate_with, PrivatePayloadIntegrity};
use crate::physical::manifest::{OwnerManifest, TableContract, TableOwnership};
use crate::recovery::evidence::{HashSnapshot, StrictRecoveryEvidence};
use crate::recovery::validate::validate_recovery_content;
use crate::{StorageKernel, StoreOpenOptions};
use redb::{Database, MultimapTableHandle, ReadTransaction, ReadableDatabase, TableHandle};
use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

pub type AgentLibraryInspectionOptions = SqlSourceCheckpointInspectionOptions;
pub type GraphShardInspectionOptions = SqlSourceCheckpointInspectionOptions;

mod commit;

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
    source: ValidatedUpgradeSource,
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
    let (pinned_file, incarnation, physical_path) = pin_upgrade_source(path)?;
    let (manifest, evidence, source_fingerprint) = preview::inspect_with(
        &pinned_file,
        &incarnation,
        &expected_physical_identity,
        private_integrity.as_deref(),
        &options,
        |database, incarnation, physical, integrity| {
            inspect_preview_read(database, incarnation, physical, integrity, target)
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
        source: ValidatedUpgradeSource::new(
            path,
            (pinned_file, incarnation, physical_path),
            (manifest, evidence, source_fingerprint),
            source_bytes,
            private_integrity,
        ),
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
    let file = open_upgrade_writer(&token.source.path)?;
    validate_descriptor(&token, &file)?;
    let (backend, opened_file) = preview::exclusive_backend(file, &token.source.path)?;
    let database = open_verified_upgrade_database(backend, &opened_file, &token.source, |file| {
        validate_descriptor(&token, file)
    })?;
    validate_reopened_predecessor(
        &database,
        &token.source.manifest,
        "owner predecessor manifest changed after inspection",
        |read| {
            inspect_predecessor(
                read,
                &token.source.incarnation,
                &token.source.manifest.physical_identity,
                token.source.private_integrity.as_deref(),
                target,
            )
            .map(|(manifest, _)| manifest)
        },
    )?;
    let current = commit::commit_upgrade(&database, &token, &opened_file, target)?;
    let report = OwnerLayoutUpgradeReport {
        previous_layout_digest: token.source.manifest.layout_digest,
        current_layout_digest: current.layout_digest,
        previous_authority_epoch: token.source.manifest.authority_epoch,
        current_authority_epoch: current.authority_epoch,
    };
    drop(database);
    drop(opened_file);
    let options = StoreOpenOptions::default().with_cache_bytes(UPGRADE_CACHE_BYTES)?;
    let kernel = match target {
        UpgradeTarget::AgentLibrary => StorageKernel::open_owner_with::<AgentLibraryOwner>(
            &token.source.path,
            current.physical_identity,
            token.source.private_integrity,
            options,
        )?,
        UpgradeTarget::GraphShard => StorageKernel::open_owner_with::<GraphShardOwner>(
            &token.source.path,
            current.physical_identity,
            token.source.private_integrity,
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

fn inspect_preview_read(
    database: &Database,
    incarnation: &StoreIncarnation,
    physical: &PhysicalStoreIdentity,
    integrity: Option<&dyn PrivatePayloadIntegrity>,
    target: UpgradeTarget,
) -> Result<(OwnerManifest, StrictRecoveryEvidence), String> {
    let read = database.begin_read().map_err(|error| error.to_string())?;
    inspect_predecessor(&read, incarnation, physical, integrity, target)
}

fn inspect_predecessor(
    read: &ReadTransaction,
    incarnation: &StoreIncarnation,
    physical: &PhysicalStoreIdentity,
    integrity: Option<&dyn PrivatePayloadIntegrity>,
    target: UpgradeTarget,
) -> Result<(OwnerManifest, StrictRecoveryEvidence), String> {
    let manifest = read_predecessor_manifest(read, incarnation)?;
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
    validate_exact_table_census(
        expected,
        normal,
        multimap,
        "layout predecessor table census is not exact",
    )
}

fn validate_descriptor(token: &ValidatedAgentLibraryUpgrade, file: &File) -> Result<(), String> {
    validate_pinned_upgrade_file(
        &token.source.pinned_file,
        file,
        &token.source.path,
        (
            &token.source.incarnation,
            token.source.physical_path.as_path(),
        ),
        "owner physical file changed after inspection",
    )
}

#[cfg(test)]
mod tests;
