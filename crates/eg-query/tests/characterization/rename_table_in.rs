//! Characterization tests for the private `rename_table_in` (CX-EG-01,
//! `crates/eg-query/src/tables/store.rs`), reached only through the public
//! `TableStore::rename_table`.
//!
//! Covers: rows/sequence/schema-name all move to the new catalog key and a
//! SERIAL insert after the rename gets the NEXT id (not a reused one -- the
//! sequence carried forward), an inbound FOREIGN KEY from another table is
//! retargeted to the new name, and renaming onto an already-existing table
//! name is rejected. Renaming a table with a hypertable plan, and a
//! self-referential (self-FK) table, are not independently exercised here
//! -- flagged in the lane report; each is pinned only by verbatim code
//! motion in the refactor.

#![cfg(feature = "sql")]

use eg_query::tables::schema::{RefAction, TableConstraint};
use eg_query::{Cell, Column, ColumnType, TableSchema, TableStore};

fn widgets_schema() -> TableSchema {
    TableSchema::new(
        "widgets",
        vec![Column {
            serial: true,
            ..Column::new("id", ColumnType::BigInt, false, true)
        }],
    )
}

#[test]
fn rename_moves_rows_schema_name_and_carries_the_sequence_forward() {
    let (store, _path) = TableStore::open_temp().expect("temporary table store");
    store.create_table(&widgets_schema(), false).unwrap();
    store
        .insert_rows("widgets", &[], &[vec![], vec![]])
        .expect("seed two SERIAL rows (ids 1, 2)");

    store.rename_table("widgets", "gadgets").expect("rename");

    // Old name is gone, new name has the moved rows and the renamed schema.
    assert!(store.get_schema("widgets").unwrap().is_none());
    let renamed = store
        .get_schema("gadgets")
        .unwrap()
        .expect("gadgets schema");
    assert_eq!(renamed.name, "gadgets");
    let rows = store.scan("gadgets").expect("scan renamed table");
    assert_eq!(rows.len(), 2);

    // The SERIAL sequence carried forward: the next insert gets id 3, not a
    // reused 1 or 2 (which would mean the old table's sequence was dropped
    // rather than re-keyed).
    let inserted = store
        .insert_rows_returning("gadgets", &[], &[vec![]])
        .expect("third SERIAL row");
    assert_eq!(inserted[0][0], Cell::Int(3));
}

#[test]
fn rename_retargets_an_inbound_foreign_key() {
    let (store, _path) = TableStore::open_temp().expect("temporary table store");
    store.create_table(&widgets_schema(), false).unwrap();
    let children_schema = TableSchema::new(
        "children",
        vec![
            Column::new("id", ColumnType::BigInt, false, true),
            Column::new("widget_id", ColumnType::BigInt, true, false),
        ],
    );
    store.create_table(&children_schema, false).unwrap();
    store
        .add_constraint(
            "children",
            TableConstraint::ForeignKey {
                name: None,
                columns: vec!["widget_id".into()],
                ref_table: "widgets".into(),
                ref_columns: vec!["id".into()],
                on_delete: RefAction::Cascade,
                on_update: RefAction::NoAction,
            },
        )
        .expect("add FK");

    store.rename_table("widgets", "gadgets").expect("rename");

    let children_after = store.get_schema("children").unwrap().unwrap();
    let fk = children_after
        .constraints()
        .iter()
        .find_map(|c| match c {
            TableConstraint::ForeignKey { ref_table, .. } => Some(ref_table.clone()),
            _ => None,
        })
        .expect("FK constraint still present");
    assert_eq!(fk, "gadgets");
}

#[test]
fn rename_onto_an_existing_table_is_rejected() {
    let (store, _path) = TableStore::open_temp().expect("temporary table store");
    store.create_table(&widgets_schema(), false).unwrap();
    store
        .create_table(
            &TableSchema::new(
                "gadgets",
                vec![Column::new("id", ColumnType::BigInt, false, true)],
            ),
            false,
        )
        .unwrap();

    let err = store
        .rename_table("widgets", "gadgets")
        .expect_err("renaming onto an existing table must be rejected");
    assert!(err.contains("already exists"), "unexpected error: {err}");
    // Neither table was touched by the rejected rename.
    assert!(store.get_schema("widgets").unwrap().is_some());
    assert_eq!(store.scan("gadgets").unwrap().len(), 0);
}
