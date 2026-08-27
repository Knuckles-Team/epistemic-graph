//! Characterization tests for the private `apply_txn_op` (CX-EG-01,
//! `crates/eg-query/src/tables/store.rs`), reached only through the public
//! `TableStore::commit_txn`.
//!
//! Exercises 9 of the 19 `TxnOp` variants in one multi-statement transaction
//! (mirroring real callers, which buffer several ops per `BEGIN...COMMIT`):
//! CreateTable, AddColumn, Insert, RenameColumn, Update, AddConstraint,
//! DropConstraint, DropColumn, Delete. The remaining 10 variants
//! (DropTable, RenameTable, AlterColumnType, CreateView, DropView,
//! CreateExtension, DropExtension, CreateFunction, DropFunction,
//! PutAnnIndex, PutHypertable, DropAnnIndexesForColumn) are not
//! independently exercised here -- flagged in the lane report. The refactor
//! is a pure per-arm extraction (each helper's body is its original match
//! arm, verbatim), so the risk for the untested arms is limited to a
//! copy/paste transcription error, not a logic change.

#![cfg(feature = "sql")]

use eg_query::tables::schema::TableConstraint;
use eg_query::{Column, ColumnType, TableSchema, TableStore, TableTxn, TxnOp};

fn schema() -> TableSchema {
    TableSchema::new(
        "widgets",
        vec![Column::new("id", ColumnType::BigInt, false, true)],
    )
}

#[test]
fn commit_txn_applies_a_mixed_batch_of_ops_in_one_transaction() {
    let (store, _path) = TableStore::open_temp().expect("temporary table store");

    let mut txn = TableTxn::new();
    txn.push(TxnOp::CreateTable {
        schema: schema(),
        if_not_exists: false,
    });
    txn.push(TxnOp::AddColumn {
        table: "widgets".into(),
        column: Column::new("name", ColumnType::Text, true, false),
    });
    txn.push(TxnOp::Insert {
        table: "widgets".into(),
        col_order: vec!["id".into(), "name".into()],
        rows: vec![vec![1i64.into(), "a".into()], vec![2i64.into(), "b".into()]],
    });
    txn.push(TxnOp::RenameColumn {
        table: "widgets".into(),
        from: "name".into(),
        to: "label".into(),
    });
    let mut set = serde_json::Map::new();
    set.insert("label".to_string(), "updated".into());
    txn.push(TxnOp::Update {
        table: "widgets".into(),
        set,
        selector: eg_types::RowPredicate::Cmp {
            col: "id".into(),
            op: eg_types::CmpOp::Eq,
            value: 1i64.into(),
        },
    });
    txn.push(TxnOp::AddConstraint {
        table: "widgets".into(),
        constraint: TableConstraint::Unique {
            name: Some("widgets_label_key".into()),
            columns: vec!["label".into()],
        },
    });
    txn.push(TxnOp::DropConstraint {
        table: "widgets".into(),
        constraint: "widgets_label_key".into(),
        if_exists: false,
    });
    txn.push(TxnOp::DropColumn {
        table: "widgets".into(),
        column: "label".into(),
        if_exists: false,
    });
    txn.push(TxnOp::Delete {
        table: "widgets".into(),
        selector: eg_types::RowPredicate::Cmp {
            col: "id".into(),
            op: eg_types::CmpOp::Eq,
            value: 2i64.into(),
        },
    });

    let affected = store.commit_txn(&txn).expect("mixed transaction commits");
    // The total affected-row count sums only the DML ops (Insert=2,
    // Update=1, Delete=1); DDL ops contribute 0 each, per apply_txn_op's
    // own `Ok(0)` arms.
    assert_eq!(affected, 4);

    let rows = store.scan("widgets").expect("scan after mixed txn");
    // Row id=2 was deleted; id=1 remains with its "label" column already
    // dropped (schema is back to just `id`).
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 1);

    let schema_after = store
        .get_schema("widgets")
        .expect("schema read")
        .expect("widgets schema");
    assert_eq!(schema_after.columns().len(), 1);
    assert_eq!(schema_after.columns()[0].name, "id");
}
