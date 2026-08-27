//! Characterization tests for the private `insert_on_conflict_in` (CX-EG-01,
//! `crates/eg-query/src/tables/store.rs`), reached only through the public
//! `TableStore::insert_rows_on_conflict`.
//!
//! Covers the function's three top-level outcomes per row: no conflict
//! (fresh insert), a conflict with `DO NOTHING` (skip, `affected` gets no
//! entry), and a conflict with `DO UPDATE SET ...` (merge into the existing
//! row). The composite (multi-column) UNIQUE/PK conflict-detection path and
//! the final `validate_uniqueness_in`/CHECK-constraint error paths are not
//! independently exercised here (they are pinned only by verbatim code
//! motion during the refactor, not by a dedicated assertion) -- flagged in
//! the lane report as a coverage gap for a follow-up characterization pass.

#![cfg(feature = "sql")]

use eg_query::{Cell, Column, ColumnType, ConflictAction, TableSchema, TableStore};
use serde_json::Map;

fn schema() -> TableSchema {
    TableSchema::new(
        "widgets",
        vec![
            Column {
                unique: true,
                ..Column::new("id", ColumnType::BigInt, false, false)
            },
            Column::new("name", ColumnType::Text, true, false),
        ],
    )
}

fn open() -> TableStore {
    let (store, _path) = TableStore::open_temp().expect("temporary table store");
    store.create_table(&schema(), false).expect("create table");
    store
        .insert_rows(
            "widgets",
            &["id".into(), "name".into()],
            &[vec![1i64.into(), "a".into()]],
        )
        .expect("seed row");
    store
}

#[test]
fn no_conflict_inserts_a_fresh_row() {
    let store = open();
    let affected = store
        .insert_rows_on_conflict(
            "widgets",
            &["id".into(), "name".into()],
            &[vec![2i64.into(), "b".into()]],
            &ConflictAction::DoNothing,
        )
        .expect("insert with no conflict");
    assert_eq!(affected.len(), 1);
    assert_eq!(affected[0][1], Cell::Text("b".into()));
    let rows = store.scan("widgets").expect("scan");
    assert_eq!(rows.len(), 2);
}

#[test]
fn do_nothing_on_conflict_skips_and_leaves_the_row_unchanged() {
    let store = open();
    let affected = store
        .insert_rows_on_conflict(
            "widgets",
            &["id".into(), "name".into()],
            &[vec![1i64.into(), "conflicting".into()]],
            &ConflictAction::DoNothing,
        )
        .expect("insert with conflict, DO NOTHING");
    assert!(affected.is_empty());
    let rows = store.scan("widgets").expect("scan");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Cell::Text("a".into()));
}

#[test]
fn do_update_on_conflict_merges_the_set_assignments() {
    let store = open();
    let mut set = Map::new();
    set.insert("name".to_string(), "updated".into());
    let affected = store
        .insert_rows_on_conflict(
            "widgets",
            &["id".into(), "name".into()],
            &[vec![1i64.into(), "conflicting".into()]],
            &ConflictAction::DoUpdate(set),
        )
        .expect("insert with conflict, DO UPDATE");
    assert_eq!(affected.len(), 1);
    assert_eq!(affected[0][1], Cell::Text("updated".into()));
    let rows = store.scan("widgets").expect("scan");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Cell::Text("updated".into()));
}
