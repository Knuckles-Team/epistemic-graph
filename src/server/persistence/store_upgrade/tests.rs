//! The command against genuine predecessor files in disposable directories:
//! what each verb reports, that an upgrade keeps the exact row bytes and runs
//! once, what is refused, and that nothing the command does not upgrade is
//! ever written.

use super::*;
use crate::redb_layout::shard_filename;
use crate::redb_store::shard::SHARD_PHYSICAL_STORE;
use crate::redb_store::NODES;
use crate::server::persistence::agent_library::{
    AgentLibraryStore, AGENT_LIBRARY_FILE, AGENT_LIBRARY_PHYSICAL_STORE,
};
use eg_storage::{
    create_predecessor_owner_file, AgentLibraryOwner, GraphShardOwner, LayoutPredecessor,
    OwnerLayout, StorageKernel, AGENT_LIBRARY_BEFORE_MCP_CATALOG, AGENT_LIBRARY_REVISIONS,
    BLOB_BEFORE_HOLDERS, GRAPH_SHARD_BEFORE_AUDIT_REQUESTS, GRAPH_SHARD_BEFORE_ENRICHMENT,
    OFFLINE_STORE_UPGRADES,
};
use redb::ReadableDatabase;
use sha2::{Digest, Sha256};

/// A key-value file holding only its cold-cache table: no build ever wrote
/// one, so its layout digest is outside every lineage.
const KV_COLD_CACHE_ONLY: LayoutPredecessor = LayoutPredecessor {
    layout: OwnerLayout::Kv,
    label: "key-value file with only the cold-cache table",
    owner_tables: &["eg_kvcache_cold"],
    data_lost: "not applicable: no build wrote this file",
    file_name: "kv.redb",
};
const STRAY: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("stray_table");

/// A private data directory on local disk. Offline inspection refuses a
/// staging root under the system temporary directory, so the directory lives
/// under this package's build output instead.
fn data_dir() -> tempfile::TempDir {
    let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("target");
    std::fs::create_dir_all(&parent).unwrap();
    tempfile::Builder::new()
        .prefix("store-upgrade-test-")
        .tempdir_in(parent)
        .unwrap()
}

fn identity(name: &str) -> PhysicalStoreIdentity {
    PhysicalStoreIdentity::new(name).unwrap()
}

fn file_digest(path: &Path) -> Vec<u8> {
    Sha256::digest(std::fs::read(path).unwrap()).to_vec()
}

fn confirmed() -> ApplyOptions {
    ApplyOptions {
        confirm: true,
        ..ApplyOptions::default()
    }
}

fn put<K: redb::Key + 'static>(
    path: &Path,
    table: redb::TableDefinition<K, &'static [u8]>,
    key: K::SelfType<'_>,
    value: &[u8],
) {
    let database = redb::Database::open(path).unwrap();
    let write = database.begin_write().unwrap();
    write.open_table(table).unwrap().insert(key, value).unwrap();
    write.commit().unwrap();
}

fn get<K: redb::Key + 'static>(
    path: &Path,
    table: redb::TableDefinition<K, &'static [u8]>,
    key: K::SelfType<'_>,
) -> Vec<u8> {
    let database = redb::Database::open(path).unwrap();
    let read = database.begin_read().unwrap();
    let table = read.open_table(table).unwrap();
    let row = table.get(key).unwrap().expect("the row is present");
    row.value().to_vec()
}

/// A predecessor Agent Library with one revision row.
fn old_agent_library(dir: &Path) -> std::path::PathBuf {
    let path = dir.join(AGENT_LIBRARY_FILE);
    create_predecessor_owner_file(
        &path,
        identity(AGENT_LIBRARY_PHYSICAL_STORE),
        &AGENT_LIBRARY_BEFORE_MCP_CATALOG,
    )
    .unwrap();
    put(
        &path,
        AGENT_LIBRARY_REVISIONS,
        ("tenant-a", "agent-a", 1),
        b"kept agent revision",
    );
    path
}

/// A predecessor graph shard, of the generation every existing shard has,
/// with one node row.
fn old_graph_shard(dir: &Path) -> std::path::PathBuf {
    old_graph_shard_of(dir, 0, &GRAPH_SHARD_BEFORE_AUDIT_REQUESTS)
}

fn old_graph_shard_of(
    dir: &Path,
    index: usize,
    predecessor: &LayoutPredecessor,
) -> std::path::PathBuf {
    let path = dir.join(shard_filename(index));
    create_predecessor_owner_file(&path, identity(SHARD_PHYSICAL_STORE), predecessor).unwrap();
    put(&path, NODES, ("graph-a", "node-a"), b"kept node");
    path
}

fn statuses(report: &Report) -> Vec<(&str, StoreStatus)> {
    report
        .stores
        .iter()
        .map(|store| (store.file.as_str(), store.status))
        .collect()
}

fn summary(report: &Report) -> serde_json::Value {
    let rendered = report.render();
    serde_json::from_str(rendered.lines().last().unwrap()).unwrap()
}

#[test]
fn every_registered_upgrade_is_one_the_command_can_run() {
    for upgrade in OFFLINE_STORE_UPGRADES {
        let layout = upgrade.predecessor.layout;
        if layout == OwnerLayout::Sql && !cfg!(feature = "query") {
            continue;
        }
        assert!(
            served_identity(layout).is_some(),
            "{} is registered, but this command knows no physical identity for a {} store",
            upgrade.predecessor.label,
            layout.canonical_name()
        );
    }
}

#[test]
fn inspect_finds_nothing_to_do_on_current_stores_and_writes_nothing() {
    let dir = data_dir();
    let library = dir.path().join(AGENT_LIBRARY_FILE);
    let shard = dir.path().join(shard_filename(0));
    drop(
        StorageKernel::create_owner::<AgentLibraryOwner>(
            &library,
            identity(AGENT_LIBRARY_PHYSICAL_STORE),
            None,
        )
        .unwrap(),
    );
    drop(
        StorageKernel::create_owner::<GraphShardOwner>(
            &shard,
            identity(SHARD_PHYSICAL_STORE),
            None,
        )
        .unwrap(),
    );
    let before = (file_digest(&library), file_digest(&shard));

    let report = inspect(dir.path());

    assert_eq!(report.exit_code, EXIT_NOTHING_TO_DO, "{}", report.render());
    assert_eq!(report.outcome, "nothing_to_do");
    assert_eq!(
        statuses(&report),
        [
            ("graph-0.redb", StoreStatus::Current),
            (AGENT_LIBRARY_FILE, StoreStatus::Current)
        ]
    );
    assert_eq!(before, (file_digest(&library), file_digest(&shard)));
    let entries = std::fs::read_dir(dir.path()).unwrap().count();
    assert_eq!(
        entries, 2,
        "inspect created something in the data directory"
    );
}

#[test]
fn inspect_reports_an_available_upgrade_without_changing_the_store() {
    let dir = data_dir();
    let library = old_agent_library(dir.path());
    let before = file_digest(&library);

    let report = inspect(dir.path());

    assert_eq!(
        report.exit_code,
        EXIT_UPGRADE_AVAILABLE,
        "{}",
        report.render()
    );
    assert_eq!(
        statuses(&report),
        [(AGENT_LIBRARY_FILE, StoreStatus::UpgradeAvailable)]
    );
    assert_eq!(report.stores[0].layout, Some("agent_library"));
    assert!(report.stores[0]
        .detail
        .contains(OFFLINE_UPGRADE_APPLY_COMMAND));
    assert!(report
        .render()
        .contains("  agent_library.redb [agent_library] upgrade_available: "));
    assert_eq!(summary(&report)["stores"][0]["status"], "upgrade_available");
    assert_eq!(summary(&report)["outcome"], "upgrade_available");
    assert_eq!(summary(&report)["exit_code"], EXIT_UPGRADE_AVAILABLE);
    assert_eq!(file_digest(&library), before);
}

#[test]
fn apply_upgrades_each_predecessor_keeps_its_rows_and_runs_once() {
    let dir = data_dir();
    let library = old_agent_library(dir.path());
    let shard = old_graph_shard(dir.path());
    let older_shard = old_graph_shard_of(dir.path(), 1, &GRAPH_SHARD_BEFORE_ENRICHMENT);

    let report = apply(dir.path(), &confirmed());

    assert_eq!(report.exit_code, EXIT_NOTHING_TO_DO, "{}", report.render());
    assert_eq!(
        statuses(&report),
        [
            ("graph-0.redb", StoreStatus::Upgraded),
            ("graph-1.redb", StoreStatus::Upgraded),
            (AGENT_LIBRARY_FILE, StoreStatus::Upgraded)
        ]
    );
    assert!(report.stores[0]
        .detail
        .contains(GRAPH_SHARD_BEFORE_AUDIT_REQUESTS.label));
    assert!(report.stores[1]
        .detail
        .contains(GRAPH_SHARD_BEFORE_ENRICHMENT.label));
    assert_eq!(
        get(&older_shard, NODES, ("graph-a", "node-a")),
        b"kept node"
    );
    let line = summary(&report);
    assert_eq!(line["command"], "store-upgrade");
    assert_eq!(line["verb"], "apply");
    assert_eq!(line["outcome"], "upgraded");
    assert_eq!(line["pre_upgrade_copy"], "none");
    assert_eq!(line["stores"][0]["status"], "upgraded");
    assert_eq!(
        get(
            &library,
            AGENT_LIBRARY_REVISIONS,
            ("tenant-a", "agent-a", 1)
        ),
        b"kept agent revision"
    );
    assert_eq!(get(&shard, NODES, ("graph-a", "node-a")), b"kept node");
    drop(
        StorageKernel::open_owner::<GraphShardOwner>(&shard, identity(SHARD_PHYSICAL_STORE), None)
            .unwrap(),
    );

    let upgraded = (file_digest(&library), file_digest(&shard));
    let again = apply(dir.path(), &confirmed());
    assert_eq!(again.exit_code, EXIT_NOTHING_TO_DO, "{}", again.render());
    assert_eq!(again.outcome, "nothing_to_do");
    assert_eq!(
        statuses(&again),
        [
            ("graph-0.redb", StoreStatus::Current),
            ("graph-1.redb", StoreStatus::Current),
            (AGENT_LIBRARY_FILE, StoreStatus::Current)
        ]
    );
    assert_eq!(upgraded, (file_digest(&library), file_digest(&shard)));
    assert_eq!(inspect(dir.path()).exit_code, EXIT_NOTHING_TO_DO);
}

#[cfg(feature = "query")]
#[test]
fn apply_upgrades_a_sql_catalog_and_keeps_its_rows() {
    const SQL_ROWS: redb::TableDefinition<(&str, u64), &[u8]> =
        redb::TableDefinition::new("__sql_rows__");
    let dir = data_dir();
    let catalogs = dir.path().join(crate::server::sql_tables::SQL_CATALOG_DIR);
    std::fs::create_dir(&catalogs).unwrap();
    let catalog = catalogs.join("tenant-catalog").with_extension("redb");
    create_predecessor_owner_file(
        &catalog,
        identity(discover::SQL_PHYSICAL_STORE),
        &eg_storage::SQL_BEFORE_SOURCE_CHECKPOINTS,
    )
    .unwrap();
    put(&catalog, SQL_ROWS, ("tenant-a:table", 7), b"kept sql row");
    let refusal = refuse_upgradable_stores(dir.path()).unwrap_err();
    assert!(
        refusal.starts_with("SQL_FORMAT_UPGRADE_REQUIRED: "),
        "{refusal}"
    );

    let report = apply(dir.path(), &confirmed());

    assert_eq!(report.exit_code, EXIT_NOTHING_TO_DO, "{}", report.render());
    assert_eq!(report.stores[0].status, StoreStatus::Upgraded);
    assert_eq!(report.stores[0].layout, Some("sql"));
    assert_eq!(
        get(&catalog, SQL_ROWS, ("tenant-a:table", 7)),
        b"kept sql row"
    );
    assert_eq!(refuse_upgradable_stores(dir.path()), Ok(()));
    assert_eq!(apply(dir.path(), &confirmed()).outcome, "nothing_to_do");
    // The table store accepts the upgraded file, so the physical identity
    // this command expects of a SQL catalog is the one that store declares.
    let authority = crate::store_authority::process_authority();
    drop(
        eg_query::TableStore::open_scoped(
            &catalog,
            "tenant-a",
            crate::store_authority::process_verifier(),
            authority.principal(),
            &authority.proof(),
        )
        .unwrap(),
    );
}

#[test]
fn both_verbs_refuse_while_an_engine_holds_the_data_directory() {
    let dir = data_dir();
    let library = old_agent_library(dir.path());
    let before = file_digest(&library);
    let engine = crate::persist_lock::acquire(dir.path().to_str().unwrap()).unwrap();

    let applied = apply(dir.path(), &confirmed());
    assert_eq!(applied.exit_code, EXIT_FAILED, "{}", applied.render());
    assert!(applied.error.as_deref().unwrap().contains("already locked"));
    assert!(applied.stores.is_empty());
    assert_eq!(summary(&applied)["outcome"], "failed");
    let inspected = inspect(dir.path());
    assert_eq!(inspected.exit_code, EXIT_FAILED, "{}", inspected.render());
    assert!(inspected
        .error
        .as_deref()
        .unwrap()
        .contains("engine is running"));
    assert_eq!(file_digest(&library), before);

    drop(engine);
    assert_eq!(inspect(dir.path()).exit_code, EXIT_UPGRADE_AVAILABLE);
}

#[test]
fn apply_without_confirmation_opens_nothing() {
    let dir = data_dir();
    let library = old_agent_library(dir.path());
    let before = file_digest(&library);

    let report = apply(dir.path(), &ApplyOptions::default());

    assert_eq!(report.exit_code, EXIT_FAILED);
    assert!(report.error.as_deref().unwrap().contains("--confirm"));
    assert_eq!(file_digest(&library), before);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[test]
fn a_format_with_no_upgrade_is_reported_and_never_modified() {
    let dir = data_dir();
    let unknown = dir.path().join(KV_COLD_CACHE_ONLY.file_name);
    create_predecessor_owner_file(&unknown, identity("test:kv"), &KV_COLD_CACHE_ONLY).unwrap();
    let retired = dir.path().join(BLOB_BEFORE_HOLDERS.file_name);
    create_predecessor_owner_file(&retired, identity("test:blob"), &BLOB_BEFORE_HOLDERS).unwrap();
    let before = (file_digest(&unknown), file_digest(&retired));
    let expected = [
        (KV_COLD_CACHE_ONLY.file_name, StoreStatus::UnknownFormat),
        (BLOB_BEFORE_HOLDERS.file_name, StoreStatus::NoUpgrade),
    ];

    let inspected = inspect(dir.path());
    assert_eq!(inspected.exit_code, EXIT_BLOCKED, "{}", inspected.render());
    assert_eq!(statuses(&inspected), expected);
    assert!(inspected.stores[1].detail.contains("move blob.redb aside"));
    assert_eq!(refuse_upgradable_stores(dir.path()), Ok(()));

    let applied = apply(dir.path(), &confirmed());
    assert_eq!(applied.exit_code, EXIT_BLOCKED, "{}", applied.render());
    assert_eq!(applied.outcome, "blocked");
    assert_eq!(statuses(&applied), expected);
    assert_eq!(before, (file_digest(&unknown), file_digest(&retired)));
}

#[test]
fn apply_stops_at_the_first_failure_and_touches_no_later_store() {
    let dir = data_dir();
    let shard = old_graph_shard(dir.path());
    put(&shard, STRAY, "planted", b"x");
    let library = old_agent_library(dir.path());
    let before = (file_digest(&shard), file_digest(&library));

    let report = apply(dir.path(), &confirmed());

    assert_eq!(report.exit_code, EXIT_FAILED, "{}", report.render());
    assert_eq!(statuses(&report), [("graph-0.redb", StoreStatus::Failed)]);
    assert_eq!(report.not_processed, 1);
    assert_eq!(summary(&report)["not_processed"], 1);
    assert_eq!(before, (file_digest(&shard), file_digest(&library)));
    // Both files are still exactly the predecessor generation they were.
    assert_eq!(get(&shard, NODES, ("graph-a", "node-a")), b"kept node");
    assert_eq!(
        statuses(&inspect(dir.path())),
        [
            ("graph-0.redb", StoreStatus::UpgradeAvailable),
            (AGENT_LIBRARY_FILE, StoreStatus::UpgradeAvailable)
        ]
    );
}

#[test]
fn the_server_refuses_an_upgradable_store_and_names_the_command() {
    let dir = data_dir();
    let library = old_agent_library(dir.path());
    let before = file_digest(&library);
    let directory = dir.path().to_str().unwrap();

    let refusal = refuse_upgradable_stores(dir.path()).unwrap_err();
    assert!(
        refusal.starts_with("AGENT_LIBRARY_FORMAT_UPGRADE_REQUIRED: "),
        "{refusal}"
    );
    assert!(
        refusal.ends_with(&format!(
            "Run: epistemic-graph-server store-upgrade apply {directory} --confirm"
        )),
        "{refusal}"
    );
    let opened = AgentLibraryStore::open(directory).unwrap_err();
    assert!(
        opened.starts_with("AGENT_LIBRARY_FORMAT_UPGRADE_REQUIRED: "),
        "{opened}"
    );
    assert!(opened.contains(OFFLINE_UPGRADE_APPLY_COMMAND), "{opened}");
    assert_eq!(file_digest(&library), before, "a refusal must not write");

    assert_eq!(
        apply(dir.path(), &confirmed()).exit_code,
        EXIT_NOTHING_TO_DO
    );
    assert_eq!(refuse_upgradable_stores(dir.path()), Ok(()));
    drop(AgentLibraryStore::open(directory).unwrap());
}

#[test]
fn the_graph_store_open_names_the_command_for_an_upgradable_shard() {
    let dir = data_dir();
    old_graph_shard(dir.path());
    let directory = dir.path().to_str().unwrap().to_string();

    let refusal = refuse_upgradable_stores(dir.path()).unwrap_err();
    assert!(
        refusal.starts_with("GRAPH_SHARD_FORMAT_UPGRADE_REQUIRED: "),
        "{refusal}"
    );
    let opened = crate::server::persistence::redb_backend::RedbBackend::open(directory, 16)
        .err()
        .expect("a predecessor shard must not open");
    assert!(
        opened.contains("GRAPH_SHARD_FORMAT_UPGRADE_REQUIRED"),
        "{opened}"
    );
    assert!(opened.contains(OFFLINE_UPGRADE_APPLY_COMMAND), "{opened}");
}

/// Leave `path` as an engine killed mid-flight leaves a store: its last
/// commit is durable, but the database was never closed. The bytes are
/// captured while the database is still open and written back, into the same
/// file, after it has been closed.
fn leave_unclean(path: &Path) {
    let database = redb::Database::open(path).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(AGENT_LIBRARY_REVISIONS)
        .unwrap()
        .insert(
            ("tenant-a", "agent-b", 2),
            b"committed before the kill".as_slice(),
        )
        .unwrap();
    write.commit().unwrap();
    let never_closed = std::fs::read(path).unwrap();
    drop(database);
    std::fs::write(path, never_closed).unwrap();
}

#[test]
fn a_predecessor_left_by_an_unclean_shutdown_is_still_upgraded() {
    let dir = data_dir();
    let library = old_agent_library(dir.path());
    leave_unclean(&library);
    let before = file_digest(&library);

    // Read-only, the manifest of such a file cannot be read at all.
    let inspected = inspect(dir.path());
    assert_eq!(
        statuses(&inspected),
        [(AGENT_LIBRARY_FILE, StoreStatus::NotInspectable)]
    );
    assert_eq!(
        inspected.exit_code,
        EXIT_UNDETERMINED,
        "{}",
        inspected.render()
    );
    assert_eq!(inspected.outcome, "undetermined");
    assert_eq!(file_digest(&library), before);

    let report = apply(dir.path(), &confirmed());
    assert_eq!(
        statuses(&report),
        [(AGENT_LIBRARY_FILE, StoreStatus::Upgraded)]
    );
    assert_eq!(report.exit_code, EXIT_NOTHING_TO_DO, "{}", report.render());
    assert_eq!(
        get(
            &library,
            AGENT_LIBRARY_REVISIONS,
            ("tenant-a", "agent-a", 1)
        ),
        b"kept agent revision"
    );
    assert_eq!(
        get(
            &library,
            AGENT_LIBRARY_REVISIONS,
            ("tenant-a", "agent-b", 2)
        ),
        b"committed before the kill"
    );
}
