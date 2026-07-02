//! End-to-end tests for SQL stored functions — `CREATE FUNCTION … LANGUAGE sql`
//! (CONCEPT:EG-118). Exercises the FULL path a `psql`/ORM client drives: `classify`
//! the DDL, persist the function in the redb [`TableStore`] function catalog, then
//! read back through DataFusion via `exec_sql_typed_with_tables`, which EXPANDS the
//! call (scalar → scalar subquery; table → parameterized-view subquery) into the query
//! text so the existing SQL planner executes it — there is no separate evaluator.
//!
//! These mirror what `src/server/wire` / `src/server/handlers/query` wire for the
//! server, but exercised in-process so the increment is provably usable without a socket.

#![cfg(feature = "sql")]

use eg_core::graph::{GraphCore, GraphView};
use eg_query::{
    classify, exec_sql_typed_with_tables, StatementKind, TableStore, TypedQueryResult,
};
use serde_json::{json, Value};

/// Drive ONE statement through the same classify → route path the server uses. A
/// `CREATE/DROP FUNCTION` is persisted in the function catalog; a read is executed
/// (functions expanded); anything else panics (these tests only use those shapes).
fn run(store: &TableStore, view: &GraphView, sql: &str) -> Option<TypedQueryResult> {
    match classify(sql).expect("classify") {
        StatementKind::Read => Some(exec_sql_typed_with_tables(view, store, sql).expect("select")),
        StatementKind::CreateFunction(plan) => {
            store
                .create_function(&plan.func, plan.or_replace)
                .expect("create function");
            None
        }
        StatementKind::DropFunction(plan) => {
            store
                .drop_function(&plan.name, plan.if_exists)
                .expect("drop function");
            None
        }
        other => panic!("unexpected statement kind for function test: {other:?}"),
    }
}

/// A graph with three `:Agent` nodes carrying an integer `score` property, for the
/// table-function expansion test.
fn graph_with_agents() -> GraphView {
    let core = GraphCore::new();
    for (id, score) in [("a1", 10i64), ("a2", 50), ("a3", 90)] {
        core.add_node(
            id.into(),
            rmp_serde::to_vec_named(&json!({"score": score, "kind": "Agent"})).unwrap(),
        );
    }
    core.analysis_snapshot()
}

// ── scalar functions ──────────────────────────────────────────────────────────

#[test]
fn scalar_function_call_computes_value_eg118() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = GraphCore::new().analysis_snapshot();
    run(
        &store,
        &view,
        "CREATE FUNCTION add_two(a int, b int) RETURNS int AS $$ SELECT a + b $$ LANGUAGE sql",
    );
    // `SELECT add_two(1, 2)` resolves to the inlined body value.
    let res = run(&store, &view, "SELECT add_two(1, 2) AS s").unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0], json!(3));
}

#[test]
fn scalar_function_preserves_operator_precedence_eg118() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = GraphCore::new().analysis_snapshot();
    // Body `a * b`; a call `mul(1 + 2, 4)` must compute (1+2)*4 = 12, NOT 1+2*4 = 9 —
    // proving each substituted argument is parenthesized (CONCEPT:EG-118).
    run(
        &store,
        &view,
        "CREATE FUNCTION mul(a int, b int) RETURNS int AS $$ SELECT a * b $$ LANGUAGE sql",
    );
    let res = run(&store, &view, "SELECT mul(1 + 2, 4) AS p").unwrap();
    assert_eq!(res.rows[0][0], json!(12));
}

#[test]
fn scalar_function_nested_calls_eg118() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = GraphCore::new().analysis_snapshot();
    run(
        &store,
        &view,
        "CREATE FUNCTION add_two(a int, b int) RETURNS int AS $$ SELECT a + b $$ LANGUAGE sql",
    );
    // A call nested in another call's argument expands over multiple passes.
    let res = run(&store, &view, "SELECT add_two(add_two(1, 2), 3) AS s").unwrap();
    assert_eq!(res.rows[0][0], json!(6));
}

// ── table functions ───────────────────────────────────────────────────────────

#[test]
fn table_function_expands_in_from_eg118() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_agents();
    run(
        &store,
        &view,
        "CREATE FUNCTION high_scorers(min_score int) RETURNS TABLE(id text) AS $$ \
         SELECT id FROM nodes WHERE score > min_score $$ LANGUAGE sql",
    );
    // `FROM high_scorers(40)` expands to the body SELECT with the arg substituted.
    let res = run(
        &store,
        &view,
        "SELECT id FROM high_scorers(40) ORDER BY id",
    )
    .unwrap();
    let ids: Vec<&Value> = res.rows.iter().map(|r| &r[0]).collect();
    assert_eq!(ids, vec![&json!("a2"), &json!("a3")]);
}

#[test]
fn table_function_setof_expands_eg118() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_agents();
    run(
        &store,
        &view,
        "CREATE FUNCTION all_ids() RETURNS SETOF text AS $$ SELECT id FROM nodes $$ LANGUAGE sql",
    );
    let res = run(&store, &view, "SELECT count(*) AS c FROM all_ids()").unwrap();
    assert_eq!(res.rows[0][0], json!(3));
}

// ── OR REPLACE / DROP / persistence ────────────────────────────────────────────

#[test]
fn create_or_replace_function_eg118() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = GraphCore::new().analysis_snapshot();
    run(
        &store,
        &view,
        "CREATE FUNCTION f(a int) RETURNS int AS $$ SELECT a + 1 $$ LANGUAGE sql",
    );
    assert_eq!(run(&store, &view, "SELECT f(10) AS v").unwrap().rows[0][0], json!(11));
    // Re-defining without OR REPLACE errors at the catalog; WITH it overwrites.
    assert!(store
        .create_function(
            &match classify("CREATE FUNCTION f(a int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql")
                .unwrap()
            {
                StatementKind::CreateFunction(p) => p.func,
                _ => unreachable!(),
            },
            false,
        )
        .is_err());
    run(
        &store,
        &view,
        "CREATE OR REPLACE FUNCTION f(a int) RETURNS int AS $$ SELECT a * 100 $$ LANGUAGE sql",
    );
    assert_eq!(run(&store, &view, "SELECT f(10) AS v").unwrap().rows[0][0], json!(1000));
}

#[test]
fn drop_function_removes_it_eg118() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = GraphCore::new().analysis_snapshot();
    run(
        &store,
        &view,
        "CREATE FUNCTION f(a int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql",
    );
    run(&store, &view, "DROP FUNCTION f");
    // After DROP, the call no longer expands and DataFusion reports the unknown function.
    assert!(exec_sql_typed_with_tables(&view, &store, "SELECT f(1)").is_err());
    // DROP FUNCTION IF EXISTS on an absent function is a no-op.
    run(&store, &view, "DROP FUNCTION IF EXISTS f");
}

#[test]
fn function_persists_in_catalog_across_reopen_eg118() {
    let (store, path) = TableStore::open_temp().unwrap();
    let view = GraphCore::new().analysis_snapshot();
    run(
        &store,
        &view,
        "CREATE FUNCTION triple(a int) RETURNS int AS $$ SELECT a * 3 $$ LANGUAGE sql",
    );
    drop(store);
    // Re-open the durable store: the function is still there and still expands.
    let store2 = TableStore::open(&path).unwrap();
    assert_eq!(store2.list_functions().unwrap().len(), 1);
    let res = exec_sql_typed_with_tables(&view, &store2, "SELECT triple(7) AS v").unwrap();
    assert_eq!(res.rows[0][0], json!(21));
}

// ── classify-level parsing (no execution) ──────────────────────────────────────

#[test]
fn classify_rejects_non_sql_language_eg118() {
    // A procedural PL/pgSQL body is a documented follow-up — rejected with a precise
    // error, never silently accepted (CONCEPT:EG-118).
    let err = classify(
        "CREATE FUNCTION f() RETURNS int AS $$ BEGIN RETURN 1; END $$ LANGUAGE plpgsql",
    )
    .unwrap_err();
    assert!(err.contains("LANGUAGE"), "unexpected error: {err}");
}

#[test]
fn classify_language_before_as_order_eg118() {
    // `LANGUAGE sql` may precede the `AS <body>` clause (CONCEPT:EG-118).
    let k = classify(
        "CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql AS $$ SELECT a $$",
    )
    .unwrap();
    match k {
        StatementKind::CreateFunction(p) => {
            assert_eq!(p.func.name, "f");
            assert_eq!(p.func.args.len(), 1);
            assert_eq!(p.func.body.trim(), "SELECT a");
        }
        other => panic!("expected CreateFunction, got {other:?}"),
    }
}
