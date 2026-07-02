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
            // CONCEPT:EG-018 ADD COLUMN + CONCEPT:EG-310 the rest — mirror the facade.
            use eg_query::AlterTableAction as A;
            match plan.action {
                A::AddColumn(col) => store.add_column(&plan.name, to_store_column(&col)).unwrap(),
                A::DropColumn { column, if_exists } => {
                    store.drop_column(&plan.name, &column, if_exists).unwrap()
                }
                A::RenameColumn { from, to } => {
                    store.rename_column(&plan.name, &from, &to).unwrap()
                }
                A::RenameTable { new_name } => {
                    store.rename_table(&plan.name, &new_name).unwrap()
                }
                A::AlterColumnType { column, new_type } => store
                    .alter_column_type(&plan.name, &column, ColumnType::parse(&new_type).unwrap())
                    .unwrap(),
                A::DropConstraint {
                    constraint,
                    if_exists,
                } => store
                    .drop_constraint(&plan.name, &constraint, if_exists)
                    .unwrap(),
            }
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
        // CONCEPT:EG-103 — route CREATE VIEW / CREATE FUNCTION to the store so the
        // system-catalog tests can assert views (relkind='v') and functions (pg_proc)
        // are reflected. Mirrors the pgwire shim's routing.
        StatementKind::CreateView(plan) => {
            store
                .create_view(&plan.name, &plan.select_sql, plan.or_replace)
                .unwrap();
            None
        }
        StatementKind::CreateFunction(plan) => {
            store.create_function(&plan.func, plan.or_replace).unwrap();
            None
        }
        // CONCEPT:EG-116/EG-313 — route `CREATE INDEX … USING hnsw|ivfflat` to the
        // durable ANN index catalog so a matching `ORDER BY col <-> $1 LIMIT k` pushes
        // down to a real eg-ann index. Mirrors the pgwire shim's routing.
        StatementKind::CreateAnnIndex(plan) => {
            store.put_ann_index(&plan).unwrap();
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
    let res2 =
        exec_sql_typed_with_tables(&view, &store, "SELECT symbol FROM stocks WHERE id = 'n2'")
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

// ── pgvector vector type + distance operators (CONCEPT:EG-115) ───────────────

/// Create a `vector(3)` table with three orthonormal embeddings, then exercise the
/// `<->` (L2) and `<=>` (cosine) nearest-neighbour path end-to-end: `classify` (a
/// Read), operator desugaring to the `vector_*` UDFs, and brute-force evaluation over
/// the stored `List<Float32>` column. Also confirms the vector round-trips back to the
/// client as a JSON array of numbers.
#[test]
fn vector_column_distance_nearest_neighbour() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();

    run(&store, &view, "CREATE TABLE vecs (id INT, emb vector(3))");
    run(
        &store,
        &view,
        "INSERT INTO vecs (id, emb) VALUES \
         (1, '[1,0,0]'), (2, '[0,1,0]'), (3, '[0,0,1]')",
    );

    // The stored vector round-trips as a JSON array of numbers.
    let got = run(&store, &view, "SELECT emb FROM vecs WHERE id = 1").unwrap();
    assert_eq!(got.rows.len(), 1);
    assert_eq!(got.rows[0][0], json!([1.0, 0.0, 0.0]));

    // L2 nearest: a query near [1,0,0] ranks id 1 first, then 2, then 3.
    let l2 = run(
        &store,
        &view,
        "SELECT id FROM vecs ORDER BY emb <-> '[0.9, 0.1, 0]' LIMIT 2",
    )
    .unwrap();
    assert_eq!(l2.rows.len(), 2);
    assert_eq!(l2.rows[0][0], json!(1));
    assert_eq!(l2.rows[1][0], json!(2));

    // Cosine nearest (`<=>`): the same query is most similar to id 1.
    let cos = run(
        &store,
        &view,
        "SELECT id FROM vecs ORDER BY emb <=> '[0.9, 0.1, 0]' LIMIT 1",
    )
    .unwrap();
    assert_eq!(cos.rows.len(), 1);
    assert_eq!(cos.rows[0][0], json!(1));

    // The distance is projectable and computes the expected L2 to a matching vector.
    let d = run(
        &store,
        &view,
        "SELECT emb <-> '[1,0,0]' AS dist FROM vecs WHERE id = 1",
    )
    .unwrap();
    assert_eq!(d.rows[0][0], json!(0.0));
}

// ── pgvector real ANN top-k pushdown (CONCEPT:EG-313) ────────────────────────

/// Seed a `vecs (id INT, emb vector(4))` table with `n` distinct deterministic
/// embeddings — the shared fixture for the EG-313 pushdown-vs-brute-force tests.
fn seed_vecs(store: &TableStore, view: &GraphView, n: usize) {
    run(store, view, "CREATE TABLE vecs (id INT, emb vector(4))");
    for i in 0..n {
        let v: Vec<f32> = (0..4)
            .map(|j| ((i * 5 + j * 2) % 13) as f32 + (i as f32) * 0.01)
            .collect();
        let lit = format!("[{},{},{},{}]", v[0], v[1], v[2], v[3]);
        run(
            store,
            view,
            &format!("INSERT INTO vecs (id, emb) VALUES ({i}, '{lit}')"),
        );
    }
}

/// The nearest ids the EG-115 brute-force `vector_l2()` scan returns for `query`,
/// LIMIT `k` — the ground-truth reference. Computed on a FRESH store with NO ANN index
/// registered, so the exec path takes the full-scan fallback.
fn brute_nearest_ids(query: &str, k: usize, n: usize, op: &str) -> Vec<Value> {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    seed_vecs(&store, &view, n);
    let sql = format!("SELECT id FROM vecs ORDER BY emb {op} '{query}' LIMIT {k}");
    run(&store, &view, &sql)
        .unwrap()
        .rows
        .into_iter()
        .map(|r| r[0].clone())
        .collect()
}

/// With a `hnsw` index registered, `ORDER BY emb <-> $q LIMIT k` pushes down to a real
/// eg-ann HNSW index and returns the TRUE nearest-k — bit-identical to the brute-force
/// reference (CONCEPT:EG-313).
#[test]
fn eg313_hnsw_pushdown_matches_brute_force_l2() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    let n = 12;
    seed_vecs(&store, &view, n);
    run(
        &store,
        &view,
        "CREATE INDEX ON vecs USING hnsw (emb vector_l2_ops)",
    );
    assert_eq!(store.list_ann_indexes().unwrap().len(), 1);

    let q = "[6.2, 1.1, 8.3, 3.0]";
    for k in [1usize, 3, 5] {
        let pushed = run(
            &store,
            &view,
            &format!("SELECT id FROM vecs ORDER BY emb <-> '{q}' LIMIT {k}"),
        )
        .unwrap();
        let want = brute_nearest_ids(q, k, n, "<->");
        let got: Vec<Value> = pushed.rows.into_iter().map(|r| r[0].clone()).collect();
        assert_eq!(got, want, "EG-313 hnsw pushdown top-{k} must equal brute force");
    }
}

/// With an `ivfflat` index the pushdown consults a real eg-ann IVF-PQ index; the true
/// nearest-k (after exact rerank) still equals the brute-force reference (CONCEPT:EG-313).
#[test]
fn eg313_ivfflat_pushdown_matches_brute_force_l2() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    let n = 16;
    seed_vecs(&store, &view, n);
    run(
        &store,
        &view,
        "CREATE INDEX ON vecs USING ivfflat (emb vector_l2_ops)",
    );

    let q = "[3.0, 9.0, 2.5, 7.0]";
    for k in [1usize, 4] {
        let pushed = run(
            &store,
            &view,
            &format!("SELECT id FROM vecs ORDER BY emb <-> '{q}' LIMIT {k}"),
        )
        .unwrap();
        let want = brute_nearest_ids(q, k, n, "<->");
        let got: Vec<Value> = pushed.rows.into_iter().map(|r| r[0].clone()).collect();
        assert_eq!(got, want, "EG-313 ivfflat pushdown top-{k} must equal brute force");
    }
}

/// A `vector_cosine_ops` (`<=>`) index pushes the cosine nearest-neighbour query down;
/// the result equals the cosine brute-force reference (CONCEPT:EG-313).
#[test]
fn eg313_cosine_pushdown_matches_brute_force() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    let n = 12;
    seed_vecs(&store, &view, n);
    run(
        &store,
        &view,
        "CREATE INDEX ON vecs USING hnsw (emb vector_cosine_ops)",
    );

    let q = "[5.0, 4.0, 6.0, 2.0]";
    let pushed = run(
        &store,
        &view,
        &format!("SELECT id FROM vecs ORDER BY emb <=> '{q}' LIMIT 3"),
    )
    .unwrap();
    let want = brute_nearest_ids(q, 3, n, "<=>");
    let got: Vec<Value> = pushed.rows.into_iter().map(|r| r[0].clone()).collect();
    assert_eq!(got, want, "EG-313 cosine pushdown top-3 must equal brute force");
}

/// No registered index ⇒ the exec path keeps the EG-115 brute-force fallback and still
/// returns the correct nearest-k (CONCEPT:EG-313). A cosine `<=>` query with only an L2
/// index registered ALSO falls back (metric not covered), still correct.
#[test]
fn eg313_no_matching_index_falls_back_to_brute_force() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    let n = 10;
    seed_vecs(&store, &view, n);

    let q = "[7.0, 2.0, 5.0, 8.0]";
    // No index at all → fallback.
    assert!(store.list_ann_indexes().unwrap().is_empty());
    let no_index = run(
        &store,
        &view,
        &format!("SELECT id FROM vecs ORDER BY emb <-> '{q}' LIMIT 3"),
    )
    .unwrap();
    let want = brute_nearest_ids(q, 3, n, "<->");
    let got: Vec<Value> = no_index.rows.into_iter().map(|r| r[0].clone()).collect();
    assert_eq!(got, want, "EG-313 no-index must fall back to brute force");

    // Register an L2-only index, then issue a COSINE query — metric not covered ⇒
    // fallback, still correct against the cosine brute-force reference.
    run(
        &store,
        &view,
        "CREATE INDEX ON vecs USING hnsw (emb vector_l2_ops)",
    );
    let cos = run(
        &store,
        &view,
        &format!("SELECT id FROM vecs ORDER BY emb <=> '{q}' LIMIT 3"),
    )
    .unwrap();
    let want_cos = brute_nearest_ids(q, 3, n, "<=>");
    let got_cos: Vec<Value> = cos.rows.into_iter().map(|r| r[0].clone()).collect();
    assert_eq!(
        got_cos, want_cos,
        "EG-313 uncovered metric must fall back to brute force"
    );
}

/// A `vector(n)` column rejects an embedding whose dimension disagrees with `n`.
#[test]
fn vector_dimension_mismatch_rejected() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    run(&store, &view, "CREATE TABLE v2 (id INT, emb vector(3))");
    // classify accepts the literal INSERT; the store's typed coerce rejects the width.
    let StatementKind::InsertTable(ins) =
        classify("INSERT INTO v2 (id, emb) VALUES (1, '[1,2]')").expect("classify")
    else {
        panic!("expected InsertTable");
    };
    let err = store.insert_rows(&ins.table, &ins.columns, &ins.rows);
    assert!(
        err.is_err(),
        "a 2-d vector into a vector(3) column must be rejected"
    );
}

// ── Postgres system catalogs (CONCEPT:EG-103) ────────────────────────────────
// A real psql/ORM introspects pg_catalog + information_schema over the live schema.
// These build a table + view + function, then assert the synthesized system views
// reflect them with the right relkind/types over the SAME exec path.

/// Set up a store+graph with a user table `prices`, a view `cheap` over it, and a SQL
/// function `add_two` — the three relation/routine kinds the catalogs must reflect.
fn catalog_fixture() -> (TableStore, GraphView) {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    run(
        &store,
        &view,
        "CREATE TABLE prices (symbol TEXT, price DOUBLE, volume BIGINT, ok BOOLEAN)",
    );
    run(
        &store,
        &view,
        "CREATE VIEW cheap AS SELECT symbol, price FROM prices WHERE price < 300",
    );
    run(
        &store,
        &view,
        "CREATE FUNCTION add_two(a int, b int) RETURNS int AS $$ SELECT a + b $$ LANGUAGE sql",
    );
    (store, view)
}

/// `pg_catalog.pg_class` JOIN `pg_namespace` lists tables (`relkind='r'`) AND the view
/// (`relkind='v'`) under `public` — the ORM bootstrap "list relations" probe (EG-103).
#[test]
fn pg_class_lists_tables_and_views_eg103() {
    let (store, view) = catalog_fixture();
    let r = run(
        &store,
        &view,
        "SELECT c.relname, c.relkind FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'public' ORDER BY c.relname",
    )
    .unwrap();
    let got: Vec<(String, String)> = r
        .rows
        .iter()
        .map(|row| {
            (
                row[0].as_str().unwrap().to_string(),
                row[1].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert!(
        got.contains(&("nodes".to_string(), "r".to_string())),
        "{got:?}"
    );
    assert!(
        got.contains(&("edges".to_string(), "r".to_string())),
        "{got:?}"
    );
    assert!(
        got.contains(&("prices".to_string(), "r".to_string())),
        "{got:?}"
    );
    assert!(
        got.contains(&("cheap".to_string(), "v".to_string())),
        "view must be relkind='v': {got:?}"
    );
}

/// `information_schema.columns` lists a table's columns with their SQL `data_type` — the
/// query SQLAlchemy `reflect()` issues to learn a table's shape (EG-103).
#[test]
fn information_schema_columns_lists_table_columns_eg103() {
    let (store, view) = catalog_fixture();
    let r = run(
        &store,
        &view,
        "SELECT column_name, data_type FROM information_schema.columns \
         WHERE table_name = 'prices' ORDER BY ordinal_position",
    )
    .unwrap();
    let got: Vec<(String, String)> = r
        .rows
        .iter()
        .map(|row| {
            (
                row[0].as_str().unwrap().to_string(),
                row[1].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("symbol".to_string(), "text".to_string()),
            ("price".to_string(), "double precision".to_string()),
            ("volume".to_string(), "bigint".to_string()),
            ("ok".to_string(), "boolean".to_string()),
        ]
    );
}

/// `information_schema.tables` lists both the base tables and the view with the correct
/// `table_type` (EG-103).
#[test]
fn information_schema_tables_lists_tables_and_views_eg103() {
    let (store, view) = catalog_fixture();
    let r = run(
        &store,
        &view,
        "SELECT table_name, table_type FROM information_schema.tables \
         WHERE table_name IN ('prices','cheap') ORDER BY table_name",
    )
    .unwrap();
    let got: Vec<(String, String)> = r
        .rows
        .iter()
        .map(|row| {
            (
                row[0].as_str().unwrap().to_string(),
                row[1].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("cheap".to_string(), "VIEW".to_string()),
            ("prices".to_string(), "BASE TABLE".to_string()),
        ]
    );
}

/// The view's own columns are reflected in `information_schema.columns` too — read back
/// from the registered view provider (EG-103).
#[test]
fn information_schema_reflects_view_columns_eg103() {
    let (store, view) = catalog_fixture();
    let r = run(
        &store,
        &view,
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name = 'cheap' ORDER BY ordinal_position",
    )
    .unwrap();
    let cols: Vec<String> = r
        .rows
        .iter()
        .map(|row| row[0].as_str().unwrap().to_string())
        .collect();
    assert_eq!(cols, vec!["symbol".to_string(), "price".to_string()]);
}

/// `pg_catalog.pg_proc` and `information_schema.routines` reflect the EG-118 SQL
/// function (EG-103).
#[test]
fn pg_proc_and_routines_reflect_function_eg103() {
    let (store, view) = catalog_fixture();
    let p = run(
        &store,
        &view,
        "SELECT proname, pronargs FROM pg_catalog.pg_proc WHERE proname = 'add_two'",
    )
    .unwrap();
    assert_eq!(p.rows.len(), 1, "add_two must appear in pg_proc");
    assert_eq!(p.rows[0][0], Value::String("add_two".into()));
    assert_eq!(p.rows[0][1], json!(2));

    let r = run(
        &store,
        &view,
        "SELECT routine_name, routine_type FROM information_schema.routines \
         WHERE routine_name = 'add_two'",
    )
    .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1], Value::String("FUNCTION".into()));
}

/// The catalog scalar functions resolve — BOTH bare and `pg_catalog.`-qualified (as psql
/// `\d` emits): `format_type`, `pg_table_is_visible`, `current_schema` (EG-103).
#[test]
fn catalog_functions_qualified_and_bare_eg103() {
    let (store, view) = catalog_fixture();
    let r = run(
        &store,
        &view,
        "SELECT pg_catalog.format_type(20, -1) AS t, \
                pg_catalog.pg_table_is_visible(16384) AS vis, \
                current_schema() AS sch",
    )
    .unwrap();
    assert_eq!(r.rows[0][0], Value::String("int8".into()));
    assert_eq!(r.rows[0][1], Value::Bool(true));
    assert_eq!(r.rows[0][2], Value::String("public".into()));
}

/// A psql `\d`-style query — JOIN `pg_class`/`pg_namespace`, filtered by
/// `pg_catalog.pg_table_is_visible(c.oid)` and `pg_catalog.format_type` in the select —
/// returns the live relations, exercising qualified table refs AND qualified function
/// calls together (EG-103).
#[test]
fn psql_backslash_d_style_query_eg103() {
    let (store, view) = catalog_fixture();
    let r = run(
        &store,
        &view,
        "SELECT c.relname, c.relkind \
         FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind IN ('r','v') \
           AND n.nspname = 'public' \
           AND pg_catalog.pg_table_is_visible(c.oid) \
         ORDER BY c.relname",
    )
    .unwrap();
    let names: Vec<String> = r
        .rows
        .iter()
        .map(|row| row[0].as_str().unwrap().to_string())
        .collect();
    for must in ["cheap", "edges", "nodes", "prices"] {
        assert!(
            names.contains(&must.to_string()),
            "missing {must}: {names:?}"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// CONCEPT:EG-310 — ALTER TABLE beyond ADD COLUMN (DROP/RENAME COLUMN, RENAME TABLE,
// ALTER COLUMN TYPE with per-row coercion, DROP CONSTRAINT). Each exercises the full
// classify → route → redb store path and reads back through DataFusion.
// ─────────────────────────────────────────────────────────────────────────────

/// CONCEPT:EG-310 — DROP COLUMN removes it from the schema AND drops its cell from
/// every stored row; the surviving columns keep their values.
#[test]
fn eg310_drop_column_removes_schema_and_cells() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    run(&store, &view, "CREATE TABLE prices (symbol TEXT, price DOUBLE, note TEXT)");
    run(
        &store,
        &view,
        "INSERT INTO prices (symbol, price, note) VALUES ('AAPL', 1.5, 'x'), ('MSFT', 2.5, 'y')",
    );

    run(&store, &view, "ALTER TABLE prices DROP COLUMN price");

    // Schema no longer has the column.
    let schema = store.get_schema("prices").unwrap().unwrap();
    assert!(schema.column("price").is_none());
    assert_eq!(schema.columns.len(), 2);
    // Stored rows were migrated: each cell for the dropped column is gone.
    for row in store.scan("prices").unwrap() {
        assert_eq!(row.len(), 2);
    }
    // Surviving columns still read their original values.
    let res = run(&store, &view, "SELECT symbol, note FROM prices ORDER BY symbol").unwrap();
    assert_eq!(res.rows.len(), 2);
    assert_eq!(res.rows[0][0], json!("AAPL"));
    assert_eq!(res.rows[0][1], json!("x"));
    // Selecting the dropped column no longer plans.
    assert!(exec_sql_typed_with_tables(&view, &store, "SELECT price FROM prices").is_err());
}

/// CONCEPT:EG-310 — RENAME COLUMN keeps every stored value (rows are positional); the
/// old name disappears and the new name projects the same data.
#[test]
fn eg310_rename_column_preserves_values() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    run(&store, &view, "CREATE TABLE prices (symbol TEXT, price DOUBLE)");
    run(&store, &view, "INSERT INTO prices (symbol, price) VALUES ('AAPL', 9.9)");

    run(&store, &view, "ALTER TABLE prices RENAME COLUMN price TO amount");

    let schema = store.get_schema("prices").unwrap().unwrap();
    assert!(schema.column("price").is_none());
    assert!(schema.column("amount").is_some());
    let res = run(&store, &view, "SELECT amount FROM prices").unwrap();
    assert_eq!(res.rows[0][0], json!(9.9));
    assert!(exec_sql_typed_with_tables(&view, &store, "SELECT price FROM prices").is_err());
}

/// CONCEPT:EG-310 — RENAME TABLE moves the catalog entry, the sequence, and every stored
/// row; the old name is gone and SERIAL ids continue (never reused).
#[test]
fn eg310_rename_table_moves_rows() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    run(&store, &view, "CREATE TABLE prices (id SERIAL PRIMARY KEY, symbol TEXT)");
    run(&store, &view, "INSERT INTO prices (symbol) VALUES ('AAPL'), ('MSFT')");

    run(&store, &view, "ALTER TABLE prices RENAME TO quotes");

    assert!(store.get_schema("prices").unwrap().is_none());
    assert!(store.get_schema("quotes").unwrap().is_some());
    let res = run(&store, &view, "SELECT id, symbol FROM quotes ORDER BY id").unwrap();
    assert_eq!(res.rows.len(), 2);
    assert_eq!(res.rows[0][0], json!(1));
    assert_eq!(res.rows[1][1], json!("MSFT"));
    // Old name no longer plans.
    assert!(exec_sql_typed_with_tables(&view, &store, "SELECT id FROM prices").is_err());
    // SERIAL sequence carried forward — a new row gets id 3, not 1.
    run(&store, &view, "INSERT INTO quotes (symbol) VALUES ('GOOG')");
    let res = run(&store, &view, "SELECT id FROM quotes ORDER BY id").unwrap();
    assert_eq!(res.rows.last().unwrap()[0], json!(3));
}

/// CONCEPT:EG-310 — ALTER COLUMN TYPE migrates every stored cell best-effort: numeric
/// text → bigint, int → double, and int → text all coerce correctly.
#[test]
fn eg310_alter_column_type_coerces_rows() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    run(&store, &view, "CREATE TABLE t (a TEXT, b INT, c INT)");
    run(&store, &view, "INSERT INTO t (a, b, c) VALUES ('42', 5, 7), ('100', 9, 11)");

    // numeric text → bigint
    run(&store, &view, "ALTER TABLE t ALTER COLUMN a TYPE BIGINT");
    // int → double
    run(&store, &view, "ALTER TABLE t ALTER COLUMN b TYPE DOUBLE PRECISION");
    // int → text
    run(&store, &view, "ALTER TABLE t ALTER COLUMN c TYPE TEXT");

    assert_eq!(store.get_schema("t").unwrap().unwrap().column("a").unwrap().ty, ColumnType::BigInt);
    let res = run(&store, &view, "SELECT a, b, c FROM t ORDER BY a").unwrap();
    assert_eq!(res.rows[0][0], json!(42));   // '42' -> 42 (int)
    assert_eq!(res.rows[0][1], json!(5.0));  // 5 -> 5.0 (double)
    assert_eq!(res.rows[0][2], json!("7"));  // 7 -> "7" (text)
    assert_eq!(res.rows[1][0], json!(100));
}

/// CONCEPT:EG-310 — ALTER COLUMN TYPE rejects an incompatible value and rolls back the
/// WHOLE change: the schema type and every stored cell are left untouched.
#[test]
fn eg310_alter_column_type_rejects_incompatible() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    run(&store, &view, "CREATE TABLE t (a TEXT)");
    run(&store, &view, "INSERT INTO t (a) VALUES ('abc')");

    let err = store.alter_column_type("t", "a", ColumnType::Int);
    assert!(err.is_err(), "expected rejection coercing 'abc' -> int");

    // Rolled back: column still TEXT and the value is intact.
    assert_eq!(store.get_schema("t").unwrap().unwrap().column("a").unwrap().ty, ColumnType::Text);
    let res = run(&store, &view, "SELECT a FROM t").unwrap();
    assert_eq!(res.rows[0][0], json!("abc"));
}

/// CONCEPT:EG-310 — every ALTER rejects a nonexistent column/table; `IF EXISTS` turns the
/// DROP COLUMN into a harmless no-op.
#[test]
fn eg310_reject_nonexistent() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    run(&store, &view, "CREATE TABLE t (a INT)");

    assert!(store.drop_column("t", "nope", false).is_err());
    assert!(store.drop_column("t", "nope", true).is_ok()); // IF EXISTS no-op
    assert!(store.rename_column("t", "nope", "x").is_err());
    assert!(store.rename_column("nope", "a", "x").is_err()); // missing table
    assert!(store.alter_column_type("t", "nope", ColumnType::Text).is_err());
    assert!(store.rename_table("nope", "x").is_err()); // missing source
    // Dropping the only column is refused.
    assert!(store.drop_column("t", "a", false).is_err());
}

/// CONCEPT:EG-310 — RENAME TABLE onto an existing name, and RENAME COLUMN onto an
/// existing column name, are both rejected (no clobber).
#[test]
fn eg310_reject_collisions() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    run(&store, &view, "CREATE TABLE a (x INT, y INT)");
    run(&store, &view, "CREATE TABLE b (z INT)");

    assert!(store.rename_table("a", "b").is_err()); // target exists
    assert!(store.rename_column("a", "x", "y").is_err()); // target column exists
}

/// CONCEPT:EG-310 — DROP CONSTRAINT clears a UNIQUE/PK constraint (matched by Postgres's
/// synthesized name), after which a previously-rejected duplicate insert succeeds.
#[test]
fn eg310_drop_constraint_relaxes_uniqueness() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    run(&store, &view, "CREATE TABLE u (id INT PRIMARY KEY, email TEXT)");
    run(&store, &view, "INSERT INTO u (id, email) VALUES (1, 'a@x')");
    // Duplicate PK rejected while the constraint stands.
    assert!(store.insert_rows("u", &["id".into(), "email".into()], &[vec![json!(1), json!("b@x")]]).is_err());

    // Drop the PK constraint (synthesized name `u_pkey`), then the dup id inserts.
    run(&store, &view, "ALTER TABLE u DROP CONSTRAINT u_pkey");
    run(&store, &view, "INSERT INTO u (id, email) VALUES (1, 'b@x')");
    let res = run(&store, &view, "SELECT COUNT(*) AS c FROM u WHERE id = 1").unwrap();
    assert_eq!(res.rows[0][0], json!(2));

    // A nonexistent constraint is rejected unless IF EXISTS.
    assert!(store.drop_constraint("u", "u_nope", false).is_err());
    assert!(store.drop_constraint("u", "u_nope", true).is_ok());
}

/// CONCEPT:EG-310 — every ALTER persists across a store reopen (redb durability): the
/// migrated schema and rows survive dropping and re-opening the database file.
#[test]
fn eg310_persists_across_reopen() {
    let (store, path) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    run(&store, &view, "CREATE TABLE prices (symbol TEXT, price INT, note TEXT)");
    run(&store, &view, "INSERT INTO prices (symbol, price, note) VALUES ('AAPL', 42, 'x')");
    run(&store, &view, "ALTER TABLE prices DROP COLUMN note");
    run(&store, &view, "ALTER TABLE prices RENAME COLUMN price TO amount");
    run(&store, &view, "ALTER TABLE prices ALTER COLUMN amount TYPE TEXT");
    run(&store, &view, "ALTER TABLE prices RENAME TO quotes");

    // Reopen the SAME redb file (drop every handle first).
    drop(store);
    let store = TableStore::open(&path).unwrap();

    assert!(store.get_schema("prices").unwrap().is_none());
    let schema = store.get_schema("quotes").unwrap().unwrap();
    assert!(schema.column("note").is_none());
    assert_eq!(schema.column("amount").unwrap().ty, ColumnType::Text);
    let res = run(&store, &view, "SELECT symbol, amount FROM quotes").unwrap();
    assert_eq!(res.rows[0][0], json!("AAPL"));
    assert_eq!(res.rows[0][1], json!("42")); // int -> text migration survived
}

/// CONCEPT:EG-310 — a multi-statement `BEGIN … ALTER … COMMIT` applies the ALTER in the
/// SAME redb write txn; an incompatible ALTER COLUMN TYPE aborts and rolls back the LOT.
#[test]
fn eg310_multi_statement_txn_atomic() {
    use eg_query::{TableTxn, TxnOp};
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_stocks();
    run(&store, &view, "CREATE TABLE t (a TEXT, b INT)");
    run(&store, &view, "INSERT INTO t (a, b) VALUES ('abc', 1)");

    // A txn whose second op (ALTER a -> INT over 'abc') is incompatible must roll back
    // the FIRST op (ADD COLUMN c) too.
    let mut txn = TableTxn::new();
    txn.push(TxnOp::AddColumn {
        table: "t".into(),
        column: Column::new("c", ColumnType::Int, true, false),
    });
    txn.push(TxnOp::AlterColumnType {
        table: "t".into(),
        column: "a".into(),
        new_type: ColumnType::Int,
    });
    assert!(store.commit_txn(&txn).is_err());

    // Nothing applied: no `c` column, `a` still TEXT.
    let schema = store.get_schema("t").unwrap().unwrap();
    assert!(schema.column("c").is_none());
    assert_eq!(schema.column("a").unwrap().ty, ColumnType::Text);

    // A well-typed txn commits both ops atomically.
    let mut txn = TableTxn::new();
    txn.push(TxnOp::AddColumn {
        table: "t".into(),
        column: Column::new("c", ColumnType::Int, true, false),
    });
    txn.push(TxnOp::RenameColumn {
        table: "t".into(),
        from: "b".into(),
        to: "qty".into(),
    });
    store.commit_txn(&txn).unwrap();
    let schema = store.get_schema("t").unwrap().unwrap();
    assert!(schema.column("c").is_some());
    assert!(schema.column("qty").is_some());
}
