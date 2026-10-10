//! Focused compatibility checks for the two SQL layouts actually declared by
//! the Train 4 owner lineage.

use super::*;
use crate::direct_state::private_local_tempdir;
use crate::owner::persisted_layout::create_predecessor_owner_file;
use crate::recovery::evidence::strict_recovery_evidence;
use redb::{ReadableDatabase, TableDefinition};
use sha2::{Digest, Sha256};

/// Serializes the tests here that open redb files. `upgrade_is_atomic_across_each_crash_stage`
/// spawns child processes, and a child holds duplicates of every parent file
/// descriptor until it execs. redb's `flock` lives on the shared open file, so a
/// database another test thread has just dropped can still read as locked
/// ("Database already open") while a child is starting.
static REDB_FILES: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn redb_files() -> std::sync::MutexGuard<'static, ()> {
    REDB_FILES
        .lock()
        .expect("a sibling redb-file test panicked while holding the lock")
}

const USER_ROWS: TableDefinition<(&str, u64), &[u8]> = TableDefinition::new("__sql_rows__");
const STRAY: TableDefinition<&str, &[u8]> = TableDefinition::new("stray_sql_table");

fn identity() -> PhysicalStoreIdentity {
    PhysicalStoreIdentity::new("eg-query:sql-user-tables").unwrap()
}

fn options(path: &Path) -> SqlSourceCheckpointInspectionOptions {
    SqlSourceCheckpointInspectionOptions::new(
        path.parent().unwrap().join("sql-layout-inspection"),
        256 * 1024 * 1024,
    )
    .unwrap()
}

fn predecessor(path: &Path, old: &crate::owner::persisted_layout::LayoutPredecessor) {
    create_predecessor_owner_file(path, identity(), old).unwrap();
    let db = Database::open(path).unwrap();
    let write = db.begin_write().unwrap();
    write
        .open_table(USER_ROWS)
        .unwrap()
        .insert(("tenant-a:source-table", 7), b"preserved row".as_slice())
        .unwrap();
    write.commit().unwrap();
}

#[test]
fn exact_predecessor_contracts_match_the_frozen_historical_digests() {
    for (old, pinned) in [
        (
            SQL_BEFORE_SOURCE_CHECKPOINTS,
            contract::PRE_SOURCE_CHECKPOINTS,
        ),
        (SQL_BEFORE_DURABLE_ANN, contract::PRE_DURABLE_ANN),
    ] {
        let contracts: Vec<_> = crate::owner::contract::expected_table_contracts(OwnerLayout::Sql)
            .into_iter()
            .filter(|contract| {
                contract.ownership == crate::physical::manifest::TableOwnership::Ledger
                    || old.owner_tables.contains(&contract.table_id.as_str())
            })
            .collect();
        assert_eq!(
            crate::owner::layout::layout_digest_over(OwnerLayout::Sql, &contracts),
            pinned,
            "{} no longer matches its frozen layout contract",
            old.label
        );
    }
}

fn row(path: &Path) -> Vec<u8> {
    let db = Database::open(path).unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(USER_ROWS).unwrap();
    table
        .get(("tenant-a:source-table", 7))
        .unwrap()
        .unwrap()
        .value()
        .to_vec()
}

/// Only a subprocess receives the crash stage. Exiting without unwinding
/// exercises redb's committed versus uncommitted upgrade boundary.
pub(super) fn crash_at(stage: &str) {
    if std::env::var("EG_SQL_LAYOUT_UPGRADE_CRASH").ok().as_deref() == Some(stage) {
        std::process::exit(73);
    }
}

#[test]
fn layout_upgrade_crash_child() {
    let Some(path) = std::env::var_os("EG_SQL_LAYOUT_UPGRADE_CHILD_PATH") else {
        return;
    };
    let path = PathBuf::from(path);
    let token =
        inspect_sql_source_checkpoint_upgrade(&path, identity(), None, options(&path)).unwrap();
    upgrade_sql_source_checkpoints(token).unwrap();
    panic!("the child must terminate at the selected durable stage");
}

#[test]
fn upgrade_is_atomic_across_each_crash_stage() {
    let _serial = redb_files();
    for stage in [
        "before_table",
        "after_table",
        "after_manifest",
        "after_commit",
    ] {
        let directory = private_local_tempdir();
        let path = directory.path().join("sql.redb");
        predecessor(&path, &SQL_BEFORE_DURABLE_ANN);
        let before = row(&path);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "owner::sql_checkpoint_upgrade::tests::layout_upgrade_crash_child",
                "--nocapture",
            ])
            .env("EG_SQL_LAYOUT_UPGRADE_CHILD_PATH", &path)
            .env("EG_SQL_LAYOUT_UPGRADE_CRASH", stage)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(73), "{stage}");
        if stage == "after_commit" {
            StorageKernel::open_owner::<SqlOwner>(&path, identity(), None).unwrap();
            assert!(
                inspect_sql_source_checkpoint_upgrade(&path, identity(), None, options(&path))
                    .is_err()
            );
        } else {
            assert!(StorageKernel::open_owner::<SqlOwner>(&path, identity(), None).is_err());
            let token =
                inspect_sql_source_checkpoint_upgrade(&path, identity(), None, options(&path))
                    .unwrap();
            upgrade_sql_source_checkpoints(token).unwrap();
        }
        assert_eq!(row(&path), before, "{stage}");
    }
}

#[test]
fn both_declared_sql_predecessors_upgrade_once_and_preserve_rows() {
    let _serial = redb_files();
    for old in [SQL_BEFORE_SOURCE_CHECKPOINTS, SQL_BEFORE_DURABLE_ANN] {
        let directory = private_local_tempdir();
        let path = directory.path().join("sql.redb");
        predecessor(&path, &old);
        assert!(StorageKernel::open_owner::<SqlOwner>(&path, identity(), None).is_err());
        let before = row(&path);
        let token =
            inspect_sql_source_checkpoint_upgrade(&path, identity(), None, options(&path)).unwrap();
        let (kernel, report) = upgrade_sql_source_checkpoints(token).unwrap();
        assert_ne!(report.previous_layout_digest, report.current_layout_digest);
        assert_eq!(
            report.previous_authority_epoch + 1,
            report.current_authority_epoch
        );
        let evidence = strict_recovery_evidence(&kernel).unwrap();
        for table_name in [
            SQL_SOURCE_CHECKPOINTS.name(),
            SQL_ANN_DIRTY.name(),
            SQL_ANN_GENERATIONS.name(),
            SQL_EDGE_INDEXES.name(),
        ] {
            assert!(evidence
                .tables
                .iter()
                .any(|table| table.table_id == table_name));
        }
        drop(kernel);
        assert_eq!(row(&path), before);
        assert!(
            inspect_sql_source_checkpoint_upgrade(&path, identity(), None, options(&path)).is_err()
        );
    }
}

/// An SQL-layout file whose owner-manifest generation is NOT one of the
/// predecessors the shared owner-manifest lineage declares (a stale/foreign
/// binding) is refused both by the one shared classification that governs
/// every owner's open-refusal and upgrade-routing decision, AND by the SQL
/// offline upgrade's own inspection -- proving the SQL store has no separate
/// admission path beyond what that one shared lineage check declares.
// spec: EG-DURABLE-KERNEL-R045
#[test]
fn an_undeclared_sql_predecessor_is_refused_by_the_shared_lineage_check() {
    let _serial = redb_files();
    const SQL_UNDECLARED_GENERATION: crate::owner::persisted_layout::LayoutPredecessor =
        crate::owner::persisted_layout::LayoutPredecessor {
            layout: OwnerLayout::Sql,
            label: "SQL generation that no lineage declares",
            owner_tables: &["__nonexistent_sql_owner_table__"],
            data_lost: "nothing is known about it",
            file_name: "sql.redb",
        };
    let directory = private_local_tempdir();
    let path = directory.path().join("sql.redb");
    create_predecessor_owner_file(&path, identity(), &SQL_UNDECLARED_GENERATION).unwrap();

    assert!(
        matches!(
            crate::owner::offline_upgrade::classify_owner_store_format(&path),
            Ok(crate::owner::offline_upgrade::OwnerStoreFormat::Unknown(
                OwnerLayout::Sql
            ))
        ),
        "the shared classification must not treat an undeclared generation as upgradable"
    );
    assert!(
        inspect_sql_source_checkpoint_upgrade(&path, identity(), None, options(&path)).is_err(),
        "the SQL offline upgrade must not admit a predecessor the shared lineage never declared"
    );
}

#[test]
fn mismatched_census_is_rejected_without_changing_the_file() {
    let _serial = redb_files();
    let directory = private_local_tempdir();
    let path = directory.path().join("sql.redb");
    predecessor(&path, &SQL_BEFORE_DURABLE_ANN);
    let db = Database::open(&path).unwrap();
    let write = db.begin_write().unwrap();
    write.open_table(STRAY).unwrap();
    write.commit().unwrap();
    drop(db);
    let before: [u8; 32] = Sha256::digest(std::fs::read(&path).unwrap()).into();
    assert!(inspect_sql_ann_generation_upgrade(&path, identity(), None, options(&path)).is_err());
    let after: [u8; 32] = Sha256::digest(std::fs::read(&path).unwrap()).into();
    assert_eq!(before, after);
}
