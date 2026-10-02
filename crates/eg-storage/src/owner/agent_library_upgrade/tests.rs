use super::*;
use crate::direct_state::private_local_tempdir;
use crate::owner::graph_shard::{AUDIT_CHAIN, AUDIT_REQUESTS, NODES};
use crate::owner::lineage_fixtures::{
    recorded_identity, RecordedLayout, BEFORE_AUDIT_REQUESTS, BEFORE_ENRICHMENT,
};
use crate::owner::persisted_layout::create_predecessor_owner_file;
use crate::owner::registry::{AGENT_LIBRARY_REVISIONS, MCP_CATALOG_CONFIGS, MCP_CATALOG_SCOPES};
use redb::{ReadableDatabase, ReadableTableMetadata};

fn identity() -> PhysicalStoreIdentity {
    PhysicalStoreIdentity::new("physical:test:agent-library-mcp-upgrade").unwrap()
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

type InspectGraphShard = fn(
    &Path,
    PhysicalStoreIdentity,
    Option<Arc<dyn PrivatePayloadIntegrity>>,
    GraphShardInspectionOptions,
) -> Result<ValidatedGraphShardUpgrade, String>;
type UpgradeGraphShard =
    fn(ValidatedGraphShardUpgrade) -> Result<(StorageKernel, GraphShardUpgradeReport), String>;

/// One graph-shard generation that has an offline upgrade: the manifest and
/// census its build recorded, how the registry names it, and the upgrade that
/// accepts it.
struct GraphGeneration {
    recorded: &'static str,
    target: UpgradeTarget,
    inspect: InspectGraphShard,
    upgrade: UpgradeGraphShard,
    /// The tables the upgrade must create.
    added: &'static [&'static str],
}

const BEFORE_ENRICHMENT_GENERATION: GraphGeneration = GraphGeneration {
    recorded: BEFORE_ENRICHMENT,
    target: UpgradeTarget::GraphShard(GraphShardSource::BeforeEnrichment),
    inspect: inspect_graph_shard_enrichment_upgrade,
    upgrade: upgrade_graph_shard_enrichment,
    added: &[
        "repository_enrichment_budgets",
        "repository_enrichment_policy_revisions",
        "repository_enrichment_supersessions",
        "repository_enrichment_parks",
        "audit_requests",
    ],
};

const BEFORE_AUDIT_REQUESTS_GENERATION: GraphGeneration = GraphGeneration {
    recorded: BEFORE_AUDIT_REQUESTS,
    target: UpgradeTarget::GraphShard(GraphShardSource::BeforeAuditRequests),
    inspect: inspect_graph_shard_audit_requests_upgrade,
    upgrade: upgrade_graph_shard_audit_requests,
    added: &["audit_requests"],
};

const GRAPH_GENERATIONS: [&GraphGeneration; 2] = [
    &BEFORE_ENRICHMENT_GENERATION,
    &BEFORE_AUDIT_REQUESTS_GENERATION,
];

const NODE_KEY: (&str, &str) = ("graph-a", "node-a");
const AUDIT_KEY: (&str, u64) = ("graph-a", 0);

impl GraphGeneration {
    fn identity(&self) -> PhysicalStoreIdentity {
        recorded_identity(self.recorded)
    }

    /// A file of this generation holding one node and one audit-chain entry,
    /// proven byte-identical in manifest and census to the recorded build
    /// before any row is added.
    fn create_with_rows(&self, path: &Path) {
        create_predecessor_owner_file(path, self.identity(), self.target.predecessor()).unwrap();
        assert_eq!(
            RecordedLayout::read(path),
            RecordedLayout::parse(self.recorded)
        );
        let database = Database::open(path).unwrap();
        let write = database.begin_write().unwrap();
        write
            .open_table(NODES)
            .unwrap()
            .insert(NODE_KEY, b"preserved node".as_slice())
            .unwrap();
        write
            .open_table(AUDIT_CHAIN)
            .unwrap()
            .insert(AUDIT_KEY, b"preserved audit entry".as_slice())
            .unwrap();
        write.commit().unwrap();
    }

    fn inspect(&self, path: &Path) -> Result<ValidatedGraphShardUpgrade, String> {
        (self.inspect)(path, self.identity(), None, options(path))
    }

    fn open(&self, path: &Path) -> Result<StorageKernel, String> {
        StorageKernel::open_owner::<GraphShardOwner>(path, self.identity(), None)
    }
}

fn assert_rows_survived_and_tables_were_added(path: &Path, added: &[&str]) {
    let database = Database::open(path).unwrap();
    let read = database.begin_read().unwrap();
    let nodes = read.open_table(NODES).unwrap();
    assert_eq!(
        nodes.get(NODE_KEY).unwrap().unwrap().value(),
        b"preserved node"
    );
    let audit = read.open_table(AUDIT_CHAIN).unwrap();
    assert_eq!(
        audit.get(AUDIT_KEY).unwrap().unwrap().value(),
        b"preserved audit entry"
    );
    let present: Vec<String> = read
        .list_tables()
        .unwrap()
        .map(|table| table.name().to_string())
        .collect();
    for name in added {
        assert!(present.iter().any(|table| table == name), "{name}");
    }
    assert_eq!(read.open_table(AUDIT_REQUESTS).unwrap().len().unwrap(), 0);
}

/// The table contracts the upgrader accepts for each generation are exactly
/// the ones the recorded build persisted, and hash to its frozen digest.
#[test]
fn frozen_graph_predecessor_contracts_match_the_recorded_manifests() {
    for generation in GRAPH_GENERATIONS {
        let recorded = RecordedLayout::parse(generation.recorded).manifest();
        assert_eq!(recorded.layout_digest, generation.target.pinned());
        let contracts = contracts_for(&recorded, generation.target).unwrap();
        assert_eq!(contracts, recorded.tables);
        assert_eq!(
            layout_digest_over(OwnerLayout::GraphShard, &contracts),
            generation.target.pinned()
        );
    }
}

/// A store at each recorded layout is refused by the ordinary open, upgraded
/// in place by its explicit offline upgrade with every row kept, gains the
/// tables it lacked, and then opens normally. The upgrade does not run twice.
#[test]
fn graph_upgrades_preserve_rows_add_the_missing_tables_and_run_once() {
    for generation in GRAPH_GENERATIONS {
        let directory = private_local_tempdir();
        let path = directory.path().join("graph-0.redb");
        generation.create_with_rows(&path);
        let refusal = generation.open(&path).err().unwrap();
        assert!(
            refusal.starts_with("GRAPH_SHARD_FORMAT_UPGRADE_REQUIRED: "),
            "{refusal}"
        );
        let token = generation.inspect(&path).unwrap();
        let (kernel, report) = (generation.upgrade)(token).unwrap();
        assert_eq!(report.previous_layout_digest, generation.target.pinned());
        assert_eq!(
            report.current_layout_digest,
            OwnerLayout::GraphShard.digest()
        );
        assert_eq!(
            report.previous_authority_epoch + 1,
            report.current_authority_epoch
        );
        drop(kernel);
        assert_rows_survived_and_tables_were_added(&path, generation.added);
        assert!(generation.inspect(&path).is_err());
        generation.open(&path).unwrap();
    }
}

/// Each offline upgrade accepts only its own generation: neither admits the
/// other's file, and neither can be finished with the other's token.
#[test]
fn a_graph_upgrade_refuses_a_file_or_token_of_another_generation() {
    let [older, newer] = GRAPH_GENERATIONS;
    let directory = private_local_tempdir();
    let old_path = directory.path().join("graph-0.redb");
    let new_path = directory.path().join("graph-1.redb");
    older.create_with_rows(&old_path);
    newer.create_with_rows(&new_path);
    assert!((newer.inspect)(&old_path, older.identity(), None, options(&old_path)).is_err());
    assert!((older.inspect)(&new_path, newer.identity(), None, options(&new_path)).is_err());
    let token = newer.inspect(&new_path).unwrap();
    assert!((older.upgrade)(token).is_err());
    assert!(
        newer.open(&new_path).is_err(),
        "a refused token must not upgrade"
    );
}
