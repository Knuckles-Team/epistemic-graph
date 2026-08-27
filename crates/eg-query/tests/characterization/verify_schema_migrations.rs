//! Characterization tests for `TableStore::verify_schema_migrations`
//! (CX-EG-01, `crates/eg-query/src/tables/store.rs`).
//!
//! `verify_schema_migrations` is invoked automatically by `open_scoped` on
//! every open (see store.rs:393), so these tests pin observed behaviour on
//! the reachable-through-the-public-API paths: an empty store, a store with
//! a table but no migrations, a store with an applied migration (both
//! in-memory and after a disk reopen, which re-runs verification against the
//! persisted chain). The function's internal `TableDoesNotExist` fallback
//! branches (for `SCHEMA_MIGRATION_ORDER`/`SCHEMA_MIGRATIONS`/
//! `SCHEMA_CATALOG_VERSIONS`/`SCHEMA_CATALOG_ORDER` missing while
//! `SCHEMA_VERSIONS` exists) model backward compatibility with store files
//! that predate one of those tables' introduction; a store created through
//! the current public API always has all of them together once any
//! migration exists, so those specific branches are not independently
//! exercised here. The refactor is a pure mechanical extraction (no logic
//! change), so preserving them byte-identical is the safety net for that gap.

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
fn empty_store_verifies_ok() {
    let (store, _path) = TableStore::open_temp().expect("temporary table store");
    // Known-bad check: an empty store with no catalog/version tables at all
    // must not spuriously fail. Proven by construction (no tables exist yet)
    // rather than by inverting the assertion, since there is nothing to
    // corrupt through the public API at this point.
    assert!(store.verify_schema_migrations().is_ok());
}

#[test]
fn store_with_table_and_no_migrations_verifies_ok() {
    let (store, _path) = TableStore::open_temp().expect("temporary table store");
    store.create_table(&schema(), false).expect("create table");
    assert!(store.verify_schema_migrations().is_ok());
}

#[test]
fn store_with_applied_migration_verifies_ok_in_memory_and_after_reopen() {
    let (store, path) = TableStore::open_temp().expect("temporary table store");
    drop(store);
    let store = TableStore::open_scoped(&path, "tenant-verify-schema").expect("scoped store");
    store.create_table(&schema(), false).expect("create table");
    let current = store
        .get_schema("events")
        .expect("schema read")
        .expect("events schema");
    let migration = SchemaMigration::for_schema(
        "events-add-source",
        "tenant-verify-schema",
        0,
        &current,
        vec![SchemaMigrationOperation::AddColumn {
            column: Column::new("source", ColumnType::Text, true, false),
        }],
        MigrationPolicy::default(),
    )
    .expect("sealed migration");
    store
        .apply_schema_migration(&migration)
        .expect("apply migration");

    // Explicit direct call, in-memory: exercises the full chain-walk loop
    // (versions/order/records/catalog_versions/catalog_order all present).
    assert!(store.verify_schema_migrations().is_ok());

    // Reopen from disk: open_scoped() re-runs verify_schema_migrations()
    // internally (store.rs:393) against the persisted chain -- open()
    // returning Ok(..) IS the pin that verification passed on real
    // deserialized data, not freshly-constructed in-memory state.
    drop(store);
    let reopened =
        TableStore::open_scoped(&path, "tenant-verify-schema").expect("reopen after migration");
    assert!(reopened.verify_schema_migrations().is_ok());
    assert_eq!(reopened.schema_version("events").unwrap(), 1);
}
