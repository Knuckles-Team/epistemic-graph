//! Focused NE-012 fixtures for the durable schema-migration authority.
#![cfg(feature = "sql")]

use eg_query::{
    Column, ColumnType, MigrationPolicy, SchemaMigration, SchemaMigrationOperation, TableSchema,
    TableStore,
};

fn schema() -> TableSchema {
    TableSchema::new(
        "events",
        vec![Column::new("id", ColumnType::BigInt, false, true)],
    )
}

#[test]
fn apply_replay_and_restart_preserve_the_ordered_chain() {
    let (legacy, path) = TableStore::open_temp().expect("temporary table store");
    // Reopen the same file under an explicit owner scope before any writes so
    // the migration's tenant/table binding is checked by the store itself.
    drop(legacy);
    let store = TableStore::open_scoped(&path, "tenant-ne012").expect("scoped store");
    store.create_table(&schema(), false).expect("create table");
    let current = store
        .get_schema("events")
        .expect("schema read")
        .expect("events schema");
    let migration = SchemaMigration::for_schema(
        "events-add-source",
        "tenant-ne012",
        0,
        &current,
        vec![SchemaMigrationOperation::AddColumn {
            column: Column::new("source", ColumnType::Text, true, false),
        }],
        MigrationPolicy::default(),
    )
    .expect("sealed migration");

    let first = store
        .apply_schema_migration(&migration)
        .expect("first application");
    assert!(!first.replayed);
    assert_eq!(first.catalog_version, 1);
    let replay = store
        .apply_schema_migration(&migration)
        .expect("idempotent replay");
    assert!(replay.replayed);
    assert_eq!(replay.catalog_version, 1);
    assert_eq!(store.schema_version("events").unwrap(), 1);
    assert_eq!(store.schema_catalog_version().unwrap(), 1);
    assert_eq!(store.schema_migrations("events").unwrap().len(), 1);
    drop(store);

    let reopened = TableStore::open_scoped(&path, "tenant-ne012").expect("restart verification");
    let snapshot = reopened
        .schema_snapshot("events")
        .unwrap()
        .expect("schema snapshot");
    assert_eq!(snapshot.version, 1);
    assert_eq!(snapshot.tenant_scope, "tenant-ne012");
    assert_eq!(reopened.schema_catalog_version().unwrap(), 1);
}

#[test]
fn stale_readers_and_wrong_tenant_are_rejected_without_mutation() {
    let (store, _path) = TableStore::open_temp().expect("temporary table store");
    store.create_table(&schema(), false).expect("create table");
    let current = store.get_schema("events").unwrap().unwrap();
    let migration = SchemaMigration::for_schema(
        "events-add-source",
        "__legacy_store__",
        0,
        &current,
        vec![SchemaMigrationOperation::AddColumn {
            column: Column::new("source", ColumnType::Text, true, false),
        }],
        MigrationPolicy::default(),
    )
    .unwrap();
    store.apply_schema_migration(&migration).unwrap();
    let stale = SchemaMigration::for_schema(
        "events-add-other",
        "__legacy_store__",
        0,
        &current,
        vec![SchemaMigrationOperation::AddColumn {
            column: Column::new("other", ColumnType::Text, true, false),
        }],
        MigrationPolicy::default(),
    )
    .unwrap();
    assert!(store.apply_schema_migration(&stale).is_err());
    assert_eq!(store.schema_version("events").unwrap(), 1);

    let (scoped, _path) = TableStore::open_temp().expect("temporary table store");
    scoped.create_table(&schema(), false).unwrap();
    let current = scoped.get_schema("events").unwrap().unwrap();
    let wrong_tenant = SchemaMigration::for_schema(
        "events-add-source",
        "other-tenant",
        0,
        &current,
        vec![SchemaMigrationOperation::AddColumn {
            column: Column::new("source", ColumnType::Text, true, false),
        }],
        MigrationPolicy::default(),
    )
    .unwrap();
    assert!(scoped.apply_schema_migration(&wrong_tenant).is_err());
    assert_eq!(scoped.schema_version("events").unwrap(), 0);
}
