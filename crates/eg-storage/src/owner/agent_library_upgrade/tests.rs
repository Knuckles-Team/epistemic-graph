use super::*;
use crate::direct_state::private_local_tempdir;
use crate::owner::graph_shard::{
    NODES, REPOSITORY_ENRICHMENT_BUDGETS, REPOSITORY_ENRICHMENT_PARKS,
    REPOSITORY_ENRICHMENT_POLICY_REVISIONS, REPOSITORY_ENRICHMENT_SUPERSESSIONS,
};
use crate::owner::persisted_layout::create_predecessor_owner_file;
use crate::owner::registry::{AGENT_LIBRARY_REVISIONS, MCP_CATALOG_CONFIGS, MCP_CATALOG_SCOPES};
use redb::{ReadableDatabase, ReadableTableMetadata};

fn identity() -> PhysicalStoreIdentity {
    PhysicalStoreIdentity::new("physical:test:agent-library-mcp-upgrade").unwrap()
}

fn graph_identity() -> PhysicalStoreIdentity {
    PhysicalStoreIdentity::new("physical:test:graph-shard-enrichment-upgrade").unwrap()
}

fn options(path: &Path) -> AgentLibraryInspectionOptions {
    AgentLibraryInspectionOptions::new(
        path.parent().unwrap().join("agent-library-inspection"),
        256 * 1024 * 1024,
    )
    .unwrap()
}

fn write_agent_revision(path: &Path, bytes: &[u8]) {
    let database = Database::open(path).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(AGENT_LIBRARY_REVISIONS)
        .unwrap()
        .insert(("tenant-a", "agent-a", 1), bytes)
        .unwrap();
    write.commit().unwrap();
}

#[test]
fn frozen_predecessor_contract_matches_historical_digest() {
    let contracts: Vec<_> =
        super::super::contract::expected_table_contracts(OwnerLayout::AgentLibrary)
            .into_iter()
            .filter(|contract| {
                contract.ownership == TableOwnership::Ledger
                    || AGENT_LIBRARY_BEFORE_MCP_CATALOG
                        .owner_tables
                        .contains(&contract.table_id.as_str())
            })
            .collect();
    assert_eq!(
        layout_digest_over(OwnerLayout::AgentLibrary, &contracts),
        super::super::lineage::AGENT_LIBRARY_PRE_MCP_CATALOG_DIGEST
    );
}

#[test]
fn explicit_upgrade_preserves_existing_agent_rows_and_runs_once() {
    let directory = private_local_tempdir();
    let path = directory.path().join("agent_library.redb");
    create_predecessor_owner_file(&path, identity(), &AGENT_LIBRARY_BEFORE_MCP_CATALOG).unwrap();
    write_agent_revision(&path, b"preserved agent revision");
    assert!(StorageKernel::open_owner::<AgentLibraryOwner>(&path, identity(), None).is_err());
    let token =
        inspect_agent_library_mcp_catalog_upgrade(&path, identity(), None, options(&path)).unwrap();
    let (kernel, report) = upgrade_agent_library_mcp_catalog(token).unwrap();
    assert_eq!(
        report.previous_authority_epoch + 1,
        report.current_authority_epoch
    );
    assert_ne!(report.previous_layout_digest, report.current_layout_digest);
    drop(kernel);
    let database = Database::open(&path).unwrap();
    let read = database.begin_read().unwrap();
    let row = read
        .open_table(AGENT_LIBRARY_REVISIONS)
        .unwrap()
        .get(("tenant-a", "agent-a", 1))
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    assert_eq!(row, b"preserved agent revision");
    assert_eq!(
        read.open_table(MCP_CATALOG_CONFIGS).unwrap().len().unwrap(),
        0
    );
    assert_eq!(
        read.open_table(MCP_CATALOG_SCOPES).unwrap().len().unwrap(),
        0
    );
    drop(read);
    drop(database);
    assert!(
        inspect_agent_library_mcp_catalog_upgrade(&path, identity(), None, options(&path)).is_err()
    );
    StorageKernel::open_owner::<AgentLibraryOwner>(&path, identity(), None).unwrap();
}

#[test]
fn inspected_bytes_are_rechecked_before_any_upgrade_write() {
    let directory = private_local_tempdir();
    let path = directory.path().join("agent_library.redb");
    create_predecessor_owner_file(&path, identity(), &AGENT_LIBRARY_BEFORE_MCP_CATALOG).unwrap();
    let token =
        inspect_agent_library_mcp_catalog_upgrade(&path, identity(), None, options(&path)).unwrap();
    write_agent_revision(&path, b"changed after inspection");
    assert!(upgrade_agent_library_mcp_catalog(token).is_err());
    assert!(StorageKernel::open_owner::<AgentLibraryOwner>(&path, identity(), None).is_err());
}

#[test]
fn earlier_connector_packless_layout_is_not_upgradeable() {
    let directory = private_local_tempdir();
    let path = directory.path().join("agent_library.redb");
    create_predecessor_owner_file(
        &path,
        identity(),
        &super::super::lineage::AGENT_LIBRARY_BEFORE_CONNECTOR_PACKS,
    )
    .unwrap();
    assert!(
        inspect_agent_library_mcp_catalog_upgrade(&path, identity(), None, options(&path)).is_err()
    );
}

#[test]
fn frozen_graph_predecessor_contract_matches_historical_digest() {
    let contracts: Vec<_> =
        super::super::contract::expected_table_contracts(OwnerLayout::GraphShard)
            .into_iter()
            .filter(|contract| {
                contract.ownership == TableOwnership::Ledger
                    || GRAPH_SHARD_BEFORE_ENRICHMENT
                        .owner_tables
                        .contains(&contract.table_id.as_str())
            })
            .collect();
    assert_eq!(
        layout_digest_over(OwnerLayout::GraphShard, &contracts),
        super::super::lineage::GRAPH_SHARD_PRE_ENRICHMENT_DIGEST
    );
}

#[test]
fn graph_upgrade_preserves_existing_nodes_and_runs_once() {
    let directory = private_local_tempdir();
    let path = directory.path().join("graph-0.redb");
    create_predecessor_owner_file(&path, graph_identity(), &GRAPH_SHARD_BEFORE_ENRICHMENT).unwrap();
    let database = Database::open(&path).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(NODES)
        .unwrap()
        .insert(("graph-a", "node-a"), b"preserved node".as_slice())
        .unwrap();
    write.commit().unwrap();
    drop(database);
    assert!(StorageKernel::open_owner::<GraphShardOwner>(&path, graph_identity(), None).is_err());
    let token =
        inspect_graph_shard_enrichment_upgrade(&path, graph_identity(), None, options(&path))
            .unwrap();
    let (kernel, report) = upgrade_graph_shard_enrichment(token).unwrap();
    assert_eq!(
        report.previous_authority_epoch + 1,
        report.current_authority_epoch
    );
    drop(kernel);
    let database = Database::open(&path).unwrap();
    let read = database.begin_read().unwrap();
    assert_eq!(
        read.open_table(NODES)
            .unwrap()
            .get(("graph-a", "node-a"))
            .unwrap()
            .unwrap()
            .value(),
        b"preserved node"
    );
    for name in [
        REPOSITORY_ENRICHMENT_BUDGETS.name(),
        REPOSITORY_ENRICHMENT_POLICY_REVISIONS.name(),
        REPOSITORY_ENRICHMENT_SUPERSESSIONS.name(),
        REPOSITORY_ENRICHMENT_PARKS.name(),
    ] {
        assert!(read
            .list_tables()
            .unwrap()
            .any(|table| table.name() == name));
    }
    drop(read);
    drop(database);
    assert!(
        inspect_graph_shard_enrichment_upgrade(&path, graph_identity(), None, options(&path))
            .is_err()
    );
    StorageKernel::open_owner::<GraphShardOwner>(&path, graph_identity(), None).unwrap();
}
