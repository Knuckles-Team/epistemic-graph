//! Characterization tests for the private `apply_schema_migration_in`
//! (CX-EG-01, `crates/eg-query/src/tables/store.rs`), reached only through
//! the public `TableStore::apply_schema_migration`.
//!
//! Covers: a fresh migration application (AddColumn), an idempotent replay
//! of the identical migration (same catalog_version, `replayed: true`), a
//! second distinct migration applied on top (RenameColumn, catalog_version
//! advances again), and a stale-version rejection (a migration whose
//! `expected_schema_version` no longer matches the authoritative version).
//! The DropColumn/AlterColumnType/AddConstraint/DropConstraint operation
//! variants and the migration-identity-conflict / concurrent-claim error
//! paths are not independently exercised here -- flagged in the lane
//! report; each is pinned only by verbatim code motion in the refactor.

#![cfg(feature = "sql")]

use eg_query::{
    Column, ColumnType, MigrationPolicy, SchemaMigration, SchemaMigrationOperation, TableSchema,
    TableStore,
};

fn schema() -> TableSchema {
    TableSchema::new(
        "widgets",
        vec![Column::new("id", ColumnType::BigInt, false, true)],
    )
}

#[test]
fn fresh_migration_applies_and_identical_replay_is_idempotent() {
    let (store, _path) = TableStore::open_temp().expect("temporary table store");
    store.create_table(&schema(), false).expect("create table");
    let current = store.get_schema("widgets").unwrap().unwrap();

    let migration = SchemaMigration::for_schema(
        "widgets-add-name",
        "__legacy_store__",
        0,
        &current,
        vec![SchemaMigrationOperation::AddColumn {
            column: Column::new("name", ColumnType::Text, true, false),
        }],
        MigrationPolicy::default(),
    )
    .expect("sealed migration");

    let first = store
        .apply_schema_migration(&migration)
        .expect("first application");
    assert!(!first.replayed);
    assert_eq!(first.catalog_version, 1);
    assert_eq!(first.schema_version, 1);
    assert_eq!(store.schema_version("widgets").unwrap(), 1);

    let replay = store
        .apply_schema_migration(&migration)
        .expect("idempotent replay");
    assert!(replay.replayed);
    assert_eq!(replay.catalog_version, 1);
    assert_eq!(replay.schema_version, 1);

    let after = store.get_schema("widgets").unwrap().unwrap();
    assert_eq!(after.columns().len(), 2);
    assert_eq!(after.columns()[1].name, "name");
}

#[test]
fn second_distinct_migration_advances_catalog_version() {
    let (store, _path) = TableStore::open_temp().expect("temporary table store");
    store.create_table(&schema(), false).expect("create table");
    let current = store.get_schema("widgets").unwrap().unwrap();
    let add = SchemaMigration::for_schema(
        "widgets-add-name",
        "__legacy_store__",
        0,
        &current,
        vec![SchemaMigrationOperation::AddColumn {
            column: Column::new("name", ColumnType::Text, true, false),
        }],
        MigrationPolicy::default(),
    )
    .unwrap();
    store.apply_schema_migration(&add).unwrap();

    let after_add = store.get_schema("widgets").unwrap().unwrap();
    let rename = SchemaMigration::for_schema(
        "widgets-rename-name",
        "__legacy_store__",
        1,
        &after_add,
        vec![SchemaMigrationOperation::RenameColumn {
            from: "name".into(),
            to: "label".into(),
        }],
        MigrationPolicy::default(),
    )
    .unwrap();
    let applied = store.apply_schema_migration(&rename).unwrap();
    assert!(!applied.replayed);
    assert_eq!(applied.catalog_version, 2);
    assert_eq!(applied.schema_version, 2);

    let after_rename = store.get_schema("widgets").unwrap().unwrap();
    assert_eq!(after_rename.columns()[1].name, "label");
}

#[test]
fn stale_expected_version_is_rejected() {
    let (store, _path) = TableStore::open_temp().expect("temporary table store");
    store.create_table(&schema(), false).expect("create table");
    let current = store.get_schema("widgets").unwrap().unwrap();
    let add = SchemaMigration::for_schema(
        "widgets-add-name",
        "__legacy_store__",
        0,
        &current,
        vec![SchemaMigrationOperation::AddColumn {
            column: Column::new("name", ColumnType::Text, true, false),
        }],
        MigrationPolicy::default(),
    )
    .unwrap();
    store.apply_schema_migration(&add).unwrap();

    // Stale: still declares expected_schema_version 0, but the table is now
    // at version 1.
    let stale = SchemaMigration::for_schema(
        "widgets-add-other",
        "__legacy_store__",
        0,
        &current,
        vec![SchemaMigrationOperation::AddColumn {
            column: Column::new("other", ColumnType::Text, true, false),
        }],
        MigrationPolicy::default(),
    )
    .unwrap();
    let err = store
        .apply_schema_migration(&stale)
        .expect_err("stale migration must be rejected");
    assert!(
        err.contains("STALE_SCHEMA_VERSION"),
        "unexpected error: {err}"
    );
    assert_eq!(store.schema_version("widgets").unwrap(), 1);
}
