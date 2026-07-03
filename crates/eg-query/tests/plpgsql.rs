//! End-to-end tests for PL/pgSQL procedural functions — `CREATE FUNCTION … LANGUAGE
//! plpgsql` (CONCEPT:EG-340 / EG-341). Exercises the FULL path a `psql`/ORM client
//! drives: `classify` the DDL, persist the function in the redb [`TableStore`] function
//! catalog, then call it with a bare `SELECT fn(args)` / `CALL proc(args)` through
//! `exec_sql_typed_with_tables`, which detects the plpgsql function and runs the
//! procedural interpreter — its embedded SQL (expression eval, `SELECT … INTO`) runs back
//! through the SAME read path.
//!
//! Mirrors `tests/functions.rs` (the `LANGUAGE sql` inline-expansion path) so both stored
//! function flavors are proven usable in-process without a socket.

#![cfg(feature = "sql")]

use eg_core::graph::{GraphCore, GraphView};
use eg_query::{classify, exec_sql_typed_with_tables, StatementKind, TableStore, TypedQueryResult};
use serde_json::json;

/// Drive ONE statement through the same classify → route path the server uses. A
/// `CREATE/DROP FUNCTION` is persisted; a `SELECT`/`CALL` is executed. `CALL` classifies
/// as an error in the SQL classifier (no `Statement::Call` route), which the server maps
/// to the read path — so this helper routes a classify error to execution too.
fn run(store: &TableStore, view: &GraphView, sql: &str) -> Option<TypedQueryResult> {
    match classify(sql) {
        Ok(StatementKind::Read) => {
            Some(exec_sql_typed_with_tables(view, store, sql).expect("select"))
        }
        Ok(StatementKind::CreateFunction(plan)) => {
            store
                .create_function(&plan.func, plan.or_replace)
                .expect("create function");
            None
        }
        Ok(StatementKind::DropFunction(plan)) => {
            store
                .drop_function(&plan.name, plan.if_exists)
                .expect("drop function");
            None
        }
        // `CALL proc(...)` — the classifier has no Call route; the server falls through to
        // the read path, so exercise the same here.
        Err(_) => Some(exec_sql_typed_with_tables(view, store, sql).expect("call")),
        Ok(other) => panic!("unexpected statement kind for plpgsql test: {other:?}"),
    }
}

/// A graph with three `:Agent` nodes carrying an integer `score`, for the `SELECT … INTO`
/// test (the embedded SQL reads the graph through the same read path).
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

/// The headline test: a FOR loop accumulating a sum + an IF branch (CONCEPT:EG-341).
#[test]
fn plpgsql_for_loop_sum_with_if_branch_eg341() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = GraphCore::new().analysis_snapshot();
    run(
        &store,
        &view,
        "CREATE FUNCTION sum_to(n int) RETURNS int AS $$ \
         DECLARE total int := 0; \
         BEGIN \
           FOR i IN 1..n LOOP \
             total := total + i; \
           END LOOP; \
           IF total > 40 THEN \
             RETURN total; \
           ELSE \
             RETURN -1; \
           END IF; \
         END $$ LANGUAGE plpgsql",
    );
    // sum(1..10) = 55 > 40 → returns 55.
    let res = run(&store, &view, "SELECT sum_to(10) AS s").unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.columns[0].name, "s");
    assert_eq!(res.rows[0][0], json!(55));
    // sum(1..5) = 15, not > 40 → the ELSE branch returns -1.
    let res = run(&store, &view, "SELECT sum_to(5) AS s").unwrap();
    assert_eq!(res.rows[0][0], json!(-1));
}

/// `SELECT … INTO var` binds an embedded query result to a variable (CONCEPT:EG-341).
#[test]
fn plpgsql_select_into_from_graph_eg341() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = graph_with_agents();
    run(
        &store,
        &view,
        "CREATE FUNCTION count_above(min_score int) RETURNS int AS $$ \
         DECLARE c int; \
         BEGIN \
           SELECT count(*) INTO c FROM nodes WHERE score > min_score; \
           RETURN c; \
         END $$ LANGUAGE plpgsql",
    );
    // Two agents (a2=50, a3=90) score above 40.
    let res = run(&store, &view, "SELECT count_above(40) AS n").unwrap();
    assert_eq!(res.rows[0][0], json!(2));
    // All three score above 0.
    let res = run(&store, &view, "SELECT count_above(0) AS n").unwrap();
    assert_eq!(res.rows[0][0], json!(3));
}

/// WHILE loop + assignment computing a factorial (CONCEPT:EG-341).
#[test]
fn plpgsql_while_loop_factorial_eg341() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = GraphCore::new().analysis_snapshot();
    run(
        &store,
        &view,
        "CREATE FUNCTION fact(n int) RETURNS int AS $$ \
         DECLARE result int := 1; i int := 1; \
         BEGIN \
           WHILE i <= n LOOP \
             result := result * i; \
             i := i + 1; \
           END LOOP; \
           RETURN result; \
         END $$ LANGUAGE plpgsql",
    );
    assert_eq!(
        run(&store, &view, "SELECT fact(5) AS f").unwrap().rows[0][0],
        json!(120)
    );
    assert_eq!(
        run(&store, &view, "SELECT fact(0) AS f").unwrap().rows[0][0],
        json!(1)
    );
}

/// A `LOOP … EXIT WHEN …` with ELSIF ladder (CONCEPT:EG-341).
#[test]
fn plpgsql_loop_exit_when_and_elsif_eg341() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = GraphCore::new().analysis_snapshot();
    run(
        &store,
        &view,
        "CREATE FUNCTION classify_n(n int) RETURNS text AS $$ \
         DECLARE i int := 0; \
         BEGIN \
           LOOP \
             i := i + 1; \
             EXIT WHEN i >= n; \
           END LOOP; \
           IF i < 3 THEN \
             RETURN 'small'; \
           ELSIF i < 10 THEN \
             RETURN 'medium'; \
           ELSE \
             RETURN 'large'; \
           END IF; \
         END $$ LANGUAGE plpgsql",
    );
    assert_eq!(
        run(&store, &view, "SELECT classify_n(2) AS c")
            .unwrap()
            .rows[0][0],
        json!("small")
    );
    assert_eq!(
        run(&store, &view, "SELECT classify_n(7) AS c")
            .unwrap()
            .rows[0][0],
        json!("medium")
    );
    assert_eq!(
        run(&store, &view, "SELECT classify_n(50) AS c")
            .unwrap()
            .rows[0][0],
        json!("large")
    );
}

/// `CALL proc(args)` executes the body (returns no rows). Correctness of the control flow
/// is asserted via a `RAISE EXCEPTION` that fires only on a wrong intermediate sum
/// (CONCEPT:EG-340). A clean `Ok` proves the FOR loop computed the expected value.
#[test]
fn plpgsql_call_procedure_runs_body_eg340() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let view = GraphCore::new().analysis_snapshot();
    run(
        &store,
        &view,
        "CREATE FUNCTION check_sum() RETURNS void AS $$ \
         DECLARE total int := 0; \
         BEGIN \
           FOR i IN 1..4 LOOP \
             total := total + i; \
           END LOOP; \
           IF total <> 10 THEN \
             RAISE EXCEPTION 'bad sum'; \
           END IF; \
         END $$ LANGUAGE plpgsql",
    );
    // 1+2+3+4 = 10 → no exception → CALL returns an empty result set.
    let res = run(&store, &view, "CALL check_sum()").unwrap();
    assert_eq!(res.rows.len(), 0);

    // A proc that RAISEs must surface the message as an error.
    run(
        &store,
        &view,
        "CREATE FUNCTION always_fail() RETURNS void AS $$ \
         BEGIN RAISE EXCEPTION 'boom'; END $$ LANGUAGE plpgsql",
    );
    let err = match exec_sql_typed_with_tables(&view, &store, "CALL always_fail()") {
        Ok(_) => panic!("expected RAISE EXCEPTION to error"),
        Err(e) => e,
    };
    assert!(err.contains("boom"), "unexpected error: {err}");
}

/// A plpgsql function persists across a store reopen and still runs (CONCEPT:EG-340).
#[test]
fn plpgsql_function_persists_across_reopen_eg340() {
    let (store, path) = TableStore::open_temp().unwrap();
    let view = GraphCore::new().analysis_snapshot();
    run(
        &store,
        &view,
        "CREATE FUNCTION dbl(n int) RETURNS int AS $$ BEGIN RETURN n * 2; END $$ LANGUAGE plpgsql",
    );
    drop(store);
    let store2 = TableStore::open(&path).unwrap();
    // The reopened catalog still records it as a plpgsql function.
    assert!(store2.list_functions().unwrap()[0].is_plpgsql());
    let res = exec_sql_typed_with_tables(&view, &store2, "SELECT dbl(21) AS v").unwrap();
    assert_eq!(res.rows[0][0], json!(42));
}
