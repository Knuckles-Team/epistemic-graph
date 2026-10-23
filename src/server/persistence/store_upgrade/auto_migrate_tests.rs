//! Automatic migration at open (EG-DURABLE-KERNEL-R070.1), against genuine
//! predecessor files in disposable directories: it migrates under the held
//! lock and keeps the rows, it is idempotent, an interrupted run resumes, the
//! switch off keeps the old refusal, and too old or too new formats are
//! refused by name and never written.

use std::ops::ControlFlow;

use super::auto_migrate::{migrate_with, parse_switch, read_journal_states};
use super::*;
use crate::redb_layout::shard_filename;
use crate::redb_store::shard::SHARD_PHYSICAL_STORE;
use crate::redb_store::NODES;
use crate::server::persistence::agent_library::{AGENT_LIBRARY_FILE, AGENT_LIBRARY_PHYSICAL_STORE};
use eg_storage::{
    create_predecessor_owner_file, LayoutPredecessor, OwnerLayout,
    AGENT_LIBRARY_BEFORE_MCP_CATALOG, AGENT_LIBRARY_REVISIONS, BLOB_BEFORE_HOLDERS,
    GRAPH_SHARD_BEFORE_AUDIT_REQUESTS,
};
use redb::ReadableDatabase;
use sha2::{Digest, Sha256};

/// A graph shard whose table set no build ever wrote: its layout digest is
/// outside this build's lineage, as a newer build's shard is.
const SHARD_FROM_A_NEWER_BUILD: LayoutPredecessor = LayoutPredecessor {
    layout: OwnerLayout::GraphShard,
    label: "graph shard with a table no build of this lineage declares",
    owner_tables: &["eg_future_table"],
    data_lost: "not applicable: no build of this lineage wrote this file",
    file_name: "graph-0.redb",
};

/// Offline inspection refuses a staging root under the system temporary
/// directory, so the data directory lives under this package's build output.
fn data_dir() -> tempfile::TempDir {
    let parent = Path::new(env!("CARGO_MANIFEST_DIR")).join("target");
    std::fs::create_dir_all(&parent).unwrap();
    tempfile::Builder::new()
        .prefix("store-auto-migrate-test-")
        .tempdir_in(parent)
        .unwrap()
}

fn lock(dir: &Path) -> crate::persist_lock::PersistDirLock {
    crate::persist_lock::acquire(&dir.to_string_lossy()).unwrap()
}

fn identity(name: &str) -> PhysicalStoreIdentity {
    PhysicalStoreIdentity::new(name).unwrap()
}

fn digest(path: &Path) -> Vec<u8> {
    Sha256::digest(std::fs::read(path).unwrap()).to_vec()
}

fn options() -> ApplyOptions {
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

/// A previous-patch data directory: one graph shard and one Agent Library,
/// each in a registered predecessor generation and holding one row.
fn previous_patch_store(dir: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let shard = dir.join(shard_filename(0));
    create_predecessor_owner_file(
        &shard,
        identity(SHARD_PHYSICAL_STORE),
        &GRAPH_SHARD_BEFORE_AUDIT_REQUESTS,
    )
    .unwrap();
    put(&shard, NODES, ("graph-a", "node-a"), b"kept node");
    let library = dir.join(AGENT_LIBRARY_FILE);
    create_predecessor_owner_file(
        &library,
        identity(AGENT_LIBRARY_PHYSICAL_STORE),
        &AGENT_LIBRARY_BEFORE_MCP_CATALOG,
    )
    .unwrap();
    put(
        &library,
        AGENT_LIBRARY_REVISIONS,
        ("tenant-a", "agent-a", 1),
        b"kept agent revision",
    );
    (shard, library)
}

fn statuses(report: &Report) -> Vec<(&str, StoreStatus)> {
    report
        .stores
        .iter()
        .map(|store| (store.file.as_str(), store.status))
        .collect()
}

fn assert_rows_kept(shard: &Path, library: &Path) {
    assert_eq!(get(shard, NODES, ("graph-a", "node-a")), b"kept node");
    assert_eq!(
        get(library, AGENT_LIBRARY_REVISIONS, ("tenant-a", "agent-a", 1)),
        b"kept agent revision"
    );
}

// spec: EG-DURABLE-KERNEL-R070.1
#[test]
fn open_migrates_a_previous_patch_store_under_the_held_lock_and_keeps_rows() {
    let dir = data_dir();
    let (shard, library) = previous_patch_store(dir.path());
    let held = lock(dir.path());
    assert!(refuse_upgradable_stores(dir.path())
        .unwrap_err()
        .contains("GRAPH_SHARD_FORMAT_UPGRADE_REQUIRED"));

    let migration = migrate_on_open(dir.path(), &held).unwrap();

    assert!(!migration.resumed);
    assert!(migration.refused.is_empty(), "{:?}", migration.refused);
    assert!(migration.startup_refusal().is_none());
    let upgraded: Vec<_> = migration
        .stores
        .iter()
        .map(|store| (store.file.as_str(), store.status))
        .collect();
    assert_eq!(
        upgraded,
        [
            ("graph-0.redb", StoreStatus::Upgraded),
            (AGENT_LIBRARY_FILE, StoreStatus::Upgraded)
        ]
    );
    assert_rows_kept(&shard, &library);
    assert_eq!(refuse_upgradable_stores(dir.path()), Ok(()));
    assert_eq!(read_journal_states(dir.path()), None, "journal is cleared");
    drop(held);
    let inspected = inspect(dir.path());
    assert_eq!(
        inspected.exit_code,
        EXIT_NOTHING_TO_DO,
        "{}",
        inspected.render()
    );
}

// spec: EG-DURABLE-KERNEL-R070.1
#[test]
fn open_is_idempotent_and_writes_nothing_on_a_current_store() {
    let dir = data_dir();
    let (shard, library) = previous_patch_store(dir.path());
    let held = lock(dir.path());
    migrate_on_open(dir.path(), &held).unwrap();
    let before = (digest(&shard), digest(&library));

    let again = migrate_on_open(dir.path(), &held).unwrap();

    assert!(again.stores.is_empty() && again.refused.is_empty() && !again.resumed);
    assert_eq!(before, (digest(&shard), digest(&library)));
    assert_eq!(read_journal_states(dir.path()), None);
}

// spec: EG-DURABLE-KERNEL-R070.1
#[test]
fn an_interrupted_migration_resumes_at_the_next_open() {
    let dir = data_dir();
    let (shard, library) = previous_patch_store(dir.path());
    let held = lock(dir.path());

    // Stop after the first store: the state a crash between two upgrades
    // leaves. The shard is current, the library is not, the journal stays.
    let first = migrate_with(dir.path(), &held, options(), |_| ControlFlow::Break(())).unwrap();
    assert_eq!(first.stores.len(), 1);
    assert_eq!(
        read_journal_states(dir.path()),
        Some(vec![
            ("graph-0.redb".to_string(), "upgraded"),
            (AGENT_LIBRARY_FILE.to_string(), "pending"),
        ])
    );
    drop(held);
    let halfway = inspect(dir.path());
    assert_eq!(
        statuses(&halfway),
        [
            ("graph-0.redb", StoreStatus::Current),
            (AGENT_LIBRARY_FILE, StoreStatus::UpgradeAvailable)
        ]
    );

    let held = lock(dir.path());
    let resumed = migrate_on_open(dir.path(), &held).unwrap();

    assert!(resumed.resumed);
    assert_eq!(resumed.stores.len(), 1);
    assert_eq!(resumed.stores[0].file, AGENT_LIBRARY_FILE);
    assert_eq!(resumed.stores[0].status, StoreStatus::Upgraded);
    assert_eq!(read_journal_states(dir.path()), None);
    assert_rows_kept(&shard, &library);
    drop(held);
    assert_eq!(inspect(dir.path()).exit_code, EXIT_NOTHING_TO_DO);
}

// spec: EG-DURABLE-KERNEL-R070.1
#[test]
fn the_switch_is_on_by_default_and_off_keeps_the_named_refusal() {
    assert_eq!(parse_switch(None), Ok(true));
    for on in ["", "1", "true", "TRUE", "on", "yes"] {
        assert_eq!(parse_switch(Some(on)), Ok(true), "{on}");
    }
    for off in ["0", "false", "False", "off", "no"] {
        assert_eq!(parse_switch(Some(off)), Ok(false), "{off}");
    }
    let invalid = parse_switch(Some("sometimes")).unwrap_err();
    assert_eq!(invalid.code(), "STORE_AUTO_MIGRATE_INVALID_SWITCH");

    // Switch off: the engine migrates nothing, and the startup check refuses
    // with the store's named error and the manual command, as before.
    let dir = data_dir();
    let (shard, library) = previous_patch_store(dir.path());
    let before = (digest(&shard), digest(&library));
    let refusal = refuse_upgradable_stores(dir.path()).unwrap_err();
    assert!(
        refusal.starts_with("GRAPH_SHARD_FORMAT_UPGRADE_REQUIRED: "),
        "{refusal}"
    );
    assert!(refusal.contains("store-upgrade apply"), "{refusal}");
    assert_eq!(before, (digest(&shard), digest(&library)));
}

// spec: EG-DURABLE-KERNEL-R070.1
#[test]
fn a_too_new_shard_is_refused_by_name_and_stops_startup_untouched() {
    let dir = data_dir();
    let shard = dir.path().join(SHARD_FROM_A_NEWER_BUILD.file_name);
    create_predecessor_owner_file(
        &shard,
        identity(SHARD_PHYSICAL_STORE),
        &SHARD_FROM_A_NEWER_BUILD,
    )
    .unwrap();
    let before = digest(&shard);
    let held = lock(dir.path());

    let migration = migrate_on_open(dir.path(), &held).unwrap();

    let refusal = migration
        .startup_refusal()
        .expect("a too-new shard stops startup");
    assert_eq!(refusal.code(), "STORE_FORMAT_TOO_NEW");
    assert!(refusal.to_string().contains("graph-0.redb"), "{refusal}");
    assert!(migration.stores.is_empty());
    assert_eq!(digest(&shard), before);
    assert_eq!(read_journal_states(dir.path()), None);
}

// spec: EG-DURABLE-KERNEL-R070.1
#[test]
fn a_too_old_lazy_store_is_refused_by_name_and_left_to_its_own_open() {
    let dir = data_dir();
    let (shard, library) = previous_patch_store(dir.path());
    let blob = dir.path().join(BLOB_BEFORE_HOLDERS.file_name);
    create_predecessor_owner_file(&blob, identity("test:blob"), &BLOB_BEFORE_HOLDERS).unwrap();
    let before = digest(&blob);
    let held = lock(dir.path());

    let migration = migrate_on_open(dir.path(), &held).unwrap();

    assert_eq!(migration.refused.len(), 1);
    assert_eq!(migration.refused[0].error.code(), "STORE_FORMAT_TOO_OLD");
    assert!(!migration.refused[0].blocks_startup);
    assert!(migration.startup_refusal().is_none());
    assert!(migration.refused[0]
        .error
        .to_string()
        .contains("move blob.redb aside"));
    assert_eq!(digest(&blob), before);
    // The supported stores beside it are still migrated.
    assert_eq!(migration.stores.len(), 2);
    assert_rows_kept(&shard, &library);
}

#[cfg(feature = "metrics")]
// spec: EG-DURABLE-KERNEL-R070.1
#[test]
fn every_step_is_counted_on_the_metrics_registry() {
    let dir = data_dir();
    previous_patch_store(dir.path());
    let held = lock(dir.path());
    migrate_on_open(dir.path(), &held).unwrap();

    let rendered = crate::metrics::render();
    for needle in [
        "epistemic_graph_store_migration_steps_total{outcome=\"written\",step=\"journal\"}",
        "epistemic_graph_store_migration_steps_total{outcome=\"upgraded\",step=\"store\"}",
        "epistemic_graph_store_migration_steps_total{outcome=\"cleared\",step=\"journal\"}",
        "epistemic_graph_store_migration_steps_total{outcome=\"migrated\",step=\"run\"}",
    ] {
        assert!(
            rendered.contains(needle),
            "{needle} missing from\n{rendered}"
        );
    }
}
