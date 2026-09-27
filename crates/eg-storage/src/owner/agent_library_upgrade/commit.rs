//! The one durable write of an inspected Agent Library or GraphShard layout.

use super::{
    successor, validate_census, validate_descriptor, validate_predecessor, UpgradeTarget,
    ValidatedOwnerLayoutUpgrade,
};
use crate::owner::graph_shard::{
    REPOSITORY_ENRICHMENT_BUDGETS, REPOSITORY_ENRICHMENT_PARKS, REPOSITORY_ENRICHMENT_SUPERSESSIONS,
};
use crate::owner::registry::{predecessor_evidence, MCP_CATALOG_CONFIGS, MCP_CATALOG_SCOPES};
use crate::owner::sql_checkpoint_upgrade::{
    begin_immediate_upgrade_write, read_upgrade_write_manifest, stage_upgrade_manifest,
};
use crate::owner::{validate_declared_tables_write, validate_manifest_write};
use crate::physical::manifest::OwnerManifest;
use crate::physical::root::{store_handle, validate_handle_write};
use crate::recovery::evidence::HashSnapshot;
use redb::{Database, WriteTransaction};
use std::fs::File;

pub(super) fn commit_upgrade(
    database: &Database,
    token: &ValidatedOwnerLayoutUpgrade,
    file: &File,
    target: UpgradeTarget,
) -> Result<OwnerManifest, String> {
    let write = begin_immediate_upgrade_write(database)?;
    validate_descriptor(token, file)?;
    validate_handle_write(&store_handle(token.source.incarnation.clone()), &write)?;
    validate_census(
        &token.source.manifest,
        target,
        write.list_tables().map_err(|error| error.to_string())?,
        write
            .list_multimap_tables()
            .map_err(|error| error.to_string())?,
    )?;
    let old = read_upgrade_write_manifest(&write)?;
    validate_predecessor(&old, target)?;
    if old != token.source.manifest
        || predecessor_evidence(
            HashSnapshot::Write(&write),
            target.layout(),
            target.predecessor().owner_tables,
        )? != token.source.evidence
    {
        return Err("owner predecessor changed before atomic upgrade".into());
    }
    let current = successor(&old, target)?;
    open_successor_tables(&write, target)?;
    stage_upgrade_manifest(&write, &current, "upgraded owner manifest")?;
    validate_manifest_write(&write, &current.physical_identity, target.layout())?;
    validate_declared_tables_write(&write, target.layout())?;
    validate_descriptor(token, file)?;
    write.commit().map_err(|error| error.to_string())?;
    Ok(current)
}

fn open_successor_tables(write: &WriteTransaction, target: UpgradeTarget) -> Result<(), String> {
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
    Ok(())
}
