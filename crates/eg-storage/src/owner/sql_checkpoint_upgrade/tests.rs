//! Focused compatibility checks for the two SQL layouts actually declared by
//! the Train 4 owner lineage.

use super::*;
use crate::direct_state::private_tempdir;
use crate::owner::persisted_layout::create_predecessor_owner_file;
use crate::recovery::evidence::strict_recovery_evidence;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use sha2::{Digest, Sha256};

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

#[test]
fn both_declared_sql_predecessors_upgrade_once_and_preserve_rows() {
    for old in [SQL_BEFORE_SOURCE_CHECKPOINTS, SQL_BEFORE_DURABLE_ANN] {
        let directory = private_tempdir();
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

#[test]
fn mismatched_census_is_rejected_without_changing_the_file() {
    let directory = private_tempdir();
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
