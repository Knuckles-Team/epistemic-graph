//! End-to-end tests for arbitrary user-defined relational tables (CONCEPT:EG-018):
//! the FULL path a `psql`/ORM client drives — `classify` the statement, route DDL/DML
//! to the redb [`TableStore`], then read back through DataFusion via
//! `exec_sql_typed_with_tables`, including a JOIN between a user table and the graph
//! `nodes` table (the unified-engine payoff) and an `INSERT … SELECT`.
//!
//! These mirror exactly what `src/server/pgwire/mod.rs` wires for the wire protocol,
//! but exercised in-process so the increment is provably usable without a socket.

#![cfg(feature = "sql")]

use eg_core::graph::{GraphCore, GraphView};
use eg_query::{
    classify, exec_sql_typed_with_tables, Cell, Column, ColumnType, StatementKind, TableSchema,
    TableStore, TypedQueryResult,
};
use serde_json::{json, Value};

/// Resolve a `classify::ColumnDef` (raw type spelling) into a store `Column`.
fn to_store_column(c: &eg_query::ColumnDef) -> Column {
    Column {
        name: c.name.clone(),
        ty: ColumnType::parse(&c.type_name).expect("known column type"),
        nullable: c.nullable,
        primary_key: c.primary_key,
        unique: c.unique,
        serial: c.serial,
        default: c.default.clone(),
        check: c.check.clone(),
    }
}

/// Drive ONE statement through the same classify → route path the pgwire shim uses.
/// Read statements return their typed result; writes/DDL return `None`.
fn run(store: &TableStore, view: &GraphView, sql: &str) -> Option<TypedQueryResult> {
    match classify(sql).expect("classify") {
        StatementKind::Read => Some(exec_sql_typed_with_tables(view, store, sql).expect("select")),
        StatementKind::CreateTable(plan) => {
            let schema = TableSchema {
                name: plan.name.clone(),
                columns: plan.columns.iter().map(to_store_column).collect(),
            };
            store.create_table(&schema, plan.if_not_exists).unwrap();
            None
        }
        StatementKind::DropTable(plan) => {
            store.drop_table(&plan.name, plan.if_exists).unwrap();
            None
        }
        StatementKind::AlterTable(plan) => {
            store
                .add_column(&plan.name, to_store_column(&plan.add_column))
                .unwrap();
            None
        }
        StatementKind::InsertTable(ins) => {
            store
                .insert_rows(&ins.table, &ins.columns, &ins.rows)
                .unwrap();
            None
        }
        StatementKind::InsertSelect(ins) => {
            // Run the SELECT through DataFusion (graph + user tables), then insert.
            let res = exec_sql_typed_with_tables(view, store, &ins.select_sql).expect("subselect");
            store
                .insert_rows(&ins.table, &ins.columns, &res.rows)
                .unwrap();
            None
        }
        StatementKind::UpdateTable(upd) => {
            store
                .update_where(&upd.table, &upd.set, &upd.selector.pred)
                .unwrap();
            None
        }
        StatementKind::DeleteTable(del) => {
            store.delete_where(&del.table, &del.selector.pred).unwrap();
            None
        }
        other => panic!("unexpected statement kind for user-table test: {other:?}"),
    }
}

/// A graph with two `:Stock` nodes carrying a `symbol` property, for the JOIN test.
fn graph_with_stocks() -> GraphView {
    let core = GraphCore::new();
    for (id, symbol) in [("n1", "AAPL"), ("n2", "MSFT")] {
        core.add_node(
            id.into(),
            rmp_serde::to_vec_named(&json!({"symbol": symbol, "kind": "Stock"})).unwrap(),
        );
    }
    core.analysis_snapshot()
}

#[test]
fn create_insert_select_typed_columns_roundtrip() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();

    run(
        &store,
        &view,
        "CREATE TABLE prices (ts TIMESTAMP NOT NULL, symbol TEXT NOT NULL, \
         price DOUBLE, volume BIGINT, raw BYTEA, labels JSON, ok BOOLEAN)",
    );
    run(
        &store,
        &view,
        "INSERT INTO prices (ts, symbol, price, volume, raw, labels, ok) VALUES \
         (1700000000000000, 'AAPL', 192.5, 1000000, 'abc', '[1,2,3]', true), \
         (1700000000000001, 'MSFT', 401.2, 2000000, 'xyz', '[4,5,6]', false)",
    );

    let res = run(
        &store,
        &view,
        "SELECT ts, symbol, price, volume, ok FROM prices ORDER BY symbol",
    )
    .unwrap();
    assert_eq!(res.rows.len(), 2);
    // Column order + typed round-trip.
    assert_eq!(res.columns[0].name, "ts");
    assert_eq!(res.rows[0][1], Value::String("AAPL".into()));
    assert_eq!(res.rows[0][2], json!(192.5));
    assert_eq!(res.rows[0][3], json!(1000000));
    assert_eq!(res.rows[0][4], Value::Bool(true));
    assert_eq!(res.rows[1][1], Value::String("MSFT".into()));
    // Timestamp round-trips as i64 micros.
    assert_eq!(res.rows[0][0], json!(1700000000000000i64));
}

#[test]
fn join_user_table_with_graph_nodes() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    run(
        &store,
        &view,
        "CREATE TABLE prices (symbol TEXT, price DOUBLE)",
    );
    run(
        &store,
        &view,
        "INSERT INTO prices (symbol, price) VALUES ('AAPL', 192.5), ('MSFT', 401.2)",
    );

    // The unified-engine query: JOIN a user table to the graph projection in ONE plan.
    let res = run(
        &store,
        &view,
        "SELECT n.id, p.price FROM nodes n JOIN prices p ON n.symbol = p.symbol \
         ORDER BY n.id",
    )
    .unwrap();
    assert_eq!(res.rows.len(), 2);
    assert_eq!(res.rows[0][0], Value::String("n1".into())); // AAPL
    assert_eq!(res.rows[0][1], json!(192.5));
    assert_eq!(res.rows[1][0], Value::String("n2".into())); // MSFT
    assert_eq!(res.rows[1][1], json!(401.2));
}

#[test]
fn update_delete_alter_full_dml() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    run(
        &store,
        &view,
        "CREATE TABLE prices (symbol TEXT, price DOUBLE)",
    );
    run(
        &store,
        &view,
        "INSERT INTO prices (symbol, price) VALUES ('AAPL', 1.0), ('MSFT', 2.0), ('AAPL', 3.0)",
    );

    // UPDATE … WHERE.
    run(
        &store,
        &view,
        "UPDATE prices SET price = 9.9 WHERE symbol = 'AAPL'",
    );
    let res = run(
        &store,
        &view,
        "SELECT price FROM prices WHERE symbol = 'AAPL'",
    )
    .unwrap();
    assert_eq!(res.rows.len(), 2);
    assert!(res.rows.iter().all(|r| r[0] == json!(9.9)));

    // DELETE … WHERE.
    run(&store, &view, "DELETE FROM prices WHERE symbol = 'MSFT'");
    let res = run(&store, &view, "SELECT COUNT(*) AS c FROM prices").unwrap();
    assert_eq!(res.rows[0][0], json!(2));

    // ALTER TABLE ADD COLUMN — old rows read NULL.
    run(&store, &view, "ALTER TABLE prices ADD COLUMN currency TEXT");
    let res = run(&store, &view, "SELECT currency FROM prices").unwrap();
    assert_eq!(res.rows.len(), 2);
    assert!(res.rows.iter().all(|r| r[0] == Value::Null));
}

#[test]
fn insert_select_from_join() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    run(
        &store,
        &view,
        "CREATE TABLE prices (symbol TEXT, price DOUBLE)",
    );
    run(
        &store,
        &view,
        "INSERT INTO prices (symbol, price) VALUES ('AAPL', 192.5), ('MSFT', 401.2)",
    );
    // A second table populated by INSERT … SELECT over a user-table ↔ graph JOIN.
    run(
        &store,
        &view,
        "CREATE TABLE enriched (node TEXT, price DOUBLE)",
    );
    run(
        &store,
        &view,
        "INSERT INTO enriched (node, price) SELECT n.id, p.price \
         FROM nodes n JOIN prices p ON n.symbol = p.symbol",
    );
    let res = run(
        &store,
        &view,
        "SELECT node, price FROM enriched ORDER BY node",
    )
    .unwrap();
    assert_eq!(res.rows.len(), 2);
    assert_eq!(res.rows[0][0], Value::String("n1".into()));
    assert_eq!(res.rows[1][1], json!(401.2));
}

#[test]
fn drop_table_then_select_errors() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    run(&store, &view, "CREATE TABLE t (a INT)");
    run(&store, &view, "INSERT INTO t (a) VALUES (1), (2)");
    run(&store, &view, "DROP TABLE t");
    // After DROP the table is gone — a SELECT against it fails to plan.
    assert!(classify("SELECT a FROM t").is_ok());
    assert!(exec_sql_typed_with_tables(&view, &store, "SELECT a FROM t").is_err());
    // And the catalog no longer knows it.
    assert!(store.get_schema("t").unwrap().is_none());
}

#[test]
fn create_table_cannot_shadow_graph_tables() {
    assert!(classify("CREATE TABLE nodes (id TEXT)").is_err());
    assert!(classify("CREATE TABLE edges (src TEXT)").is_err());
}

#[test]
fn create_view_over_nodes_and_select_through_it() {
    // CONCEPT:EG-072 — a view over the graph `nodes` table registers as a read-only
    // relation, and a SELECT through it (even joined to a user table) resolves.
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();

    // CREATE VIEW stocks AS SELECT id, symbol FROM nodes.
    let StatementKind::CreateView(plan) =
        classify("CREATE VIEW stocks AS SELECT id, symbol FROM nodes").expect("classify")
    else {
        panic!("expected CreateView");
    };
    store
        .create_view(&plan.name, &plan.select_sql, plan.or_replace)
        .unwrap();

    // SELECT through the view.
    let res =
        exec_sql_typed_with_tables(&view, &store, "SELECT id, symbol FROM stocks ORDER BY id")
            .expect("select through view");
    assert_eq!(res.rows.len(), 2);
    assert_eq!(res.rows[0][0], Value::String("n1".into()));
    assert_eq!(res.rows[0][1], Value::String("AAPL".into()));

    // A view can be JOINed and filtered like any relation.
    let res2 = exec_sql_typed_with_tables(
        &view,
        &store,
        "SELECT symbol FROM stocks WHERE id = 'n2'",
    )
    .expect("filtered view select");
    assert_eq!(res2.rows.len(), 1);
    assert_eq!(res2.rows[0][0], Value::String("MSFT".into()));

    // DROP VIEW removes it — a subsequent SELECT no longer resolves.
    store.drop_view("stocks", false).unwrap();
    assert!(exec_sql_typed_with_tables(&view, &store, "SELECT id FROM stocks").is_err());
}

/// A direct store-level proof that EVERY supported column type round-trips its cell
/// value (the scan returns the exact `Cell` that was coerced in).
#[test]
fn every_column_type_roundtrips() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let schema = TableSchema {
        name: "typed".into(),
        columns: vec![
            Column::new("i", ColumnType::Int, true, false),
            Column::new("b", ColumnType::BigInt, true, false),
            Column::new("f", ColumnType::Double, true, false),
            Column::new("t", ColumnType::Text, true, false),
            Column::new("o", ColumnType::Bool, true, false),
            Column::new("ts", ColumnType::Timestamp, true, false),
            Column::new("by", ColumnType::Bytes, true, false),
            Column::new("j", ColumnType::Json, true, false),
        ],
    };
    store.create_table(&schema, false).unwrap();
    let cols: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    store
        .insert_rows(
            "typed",
            &cols,
            &[vec![
                json!(7),
                json!(9_000_000_000i64),
                json!(1.5),
                json!("hello"),
                json!(true),
                json!(1700000000000000i64),
                json!("AB"),
                json!({"k": [1, 2]}),
            ]],
        )
        .unwrap();
    let rows = store.scan("typed").unwrap();
    assert_eq!(rows[0][0], Cell::Int(7));
    assert_eq!(rows[0][1], Cell::Int(9_000_000_000));
    assert_eq!(rows[0][2], Cell::Float(1.5));
    assert_eq!(rows[0][3], Cell::Text("hello".into()));
    assert_eq!(rows[0][4], Cell::Bool(true));
    assert_eq!(rows[0][5], Cell::Timestamp(1700000000000000));
    assert_eq!(rows[0][6], Cell::Bytes(b"AB".to_vec()));
    assert_eq!(rows[0][7], Cell::Json(json!({"k": [1, 2]})));
}
