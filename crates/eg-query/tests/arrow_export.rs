//! Arrow dataset-handle export path (CONCEPT:INT-P2-2): `exec_sql_arrow` must hand
//! back REAL Arrow `RecordBatch`es with typed columns — never per-row JSON — so an
//! external heavy-compute job pulls typed data in bulk instead of marshalling rows
//! through Python. This proves the SQL engine's Arrow leg end-to-end: a query over a
//! graph produces a schema with the expected Arrow types, and the batches' values
//! read back correctly through the native Arrow array API (no JSON decode anywhere
//! in this test).

#![cfg(feature = "sql")]

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::DataType;
use eg_core::graph::GraphCore;
use eg_query::{
    exec_sql, exec_sql_arrow, exec_sql_cached, exec_sql_typed,
    exec_sql_typed_with_tables_cached_cancellable, CancellationToken, PgColType, SqlCache,
    SqlContextCache, TableStore,
};
use serde_json::json;

/// A graph with three `:Agent` nodes carrying a `name` (Utf8) and a `score` (Int64)
/// property, for exercising the Arrow schema/typed-value round trip.
fn graph_with_agents() -> GraphCore {
    let core = GraphCore::new();
    for (id, name, score) in [
        ("a1", "alice", 10i64),
        ("a2", "bob", 50),
        ("a3", "carol", 90),
    ] {
        core.add_node(
            id.into(),
            rmp_serde::to_vec_named(&json!({"kind": "Agent", "name": name, "score": score}))
                .unwrap(),
        );
    }
    core
}

#[test]
fn exec_sql_arrow_returns_typed_record_batches_not_json_rows() {
    let core = graph_with_agents();
    let view = core.analysis_snapshot();

    let (schema, batches) =
        exec_sql_arrow(&view, "SELECT id, name, score FROM nodes ORDER BY id").expect("query");

    // The schema is a REAL Arrow schema with typed columns — not a stringly-typed
    // JSON shape.
    assert_eq!(
        schema.field_with_name("id").unwrap().data_type(),
        &DataType::Utf8
    );
    assert_eq!(
        schema.field_with_name("name").unwrap().data_type(),
        &DataType::Utf8
    );
    assert_eq!(
        schema.field_with_name("score").unwrap().data_type(),
        &DataType::Int64
    );

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "3 agents in, 3 rows out");

    // Read the values back through the native Arrow array API — proves the payload
    // is real typed columnar Arrow, not a JSON cell decode.
    let mut ids = Vec::new();
    let mut names = Vec::new();
    let mut scores = Vec::new();
    for batch in &batches {
        assert_eq!(batch.schema().as_ref(), schema.as_ref());
        let id_col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id column is a real Arrow StringArray");
        let name_col = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column is a real Arrow StringArray");
        let score_col = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("score column is a real Arrow Int64Array");
        for i in 0..batch.num_rows() {
            ids.push(id_col.value(i).to_string());
            names.push(name_col.value(i).to_string());
            scores.push(score_col.value(i));
        }
    }
    assert_eq!(ids, vec!["a1", "a2", "a3"]);
    assert_eq!(names, vec!["alice", "bob", "carol"]);
    assert_eq!(scores, vec![10, 50, 90]);
}

#[test]
fn exec_sql_arrow_empty_result_yields_zero_batches() {
    let core = graph_with_agents();
    let view = core.analysis_snapshot();

    // Matches the same convention `batches_to_typed`/`batches_to_result` already use:
    // when DataFusion's stream produces no batches at all, the schema falls back to
    // empty (there is nothing to derive column types from) rather than fabricating
    // one — the caller still gets an unambiguous "zero rows" result, never an error.
    let (schema, batches) =
        exec_sql_arrow(&view, "SELECT id FROM nodes WHERE id = 'nonexistent'").expect("query");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
    assert!(schema.fields().is_empty() || batches.is_empty());
}

#[test]
fn max_rows_caps_crossing_union_for_arrow_json_and_msgpack() {
    let core = graph_with_agents();
    let view = core.analysis_snapshot();
    let sql = "SELECT value FROM generate_series(1, 49999) \
               UNION ALL \
               SELECT value FROM generate_series(50000, 50001) \
               ORDER BY value";
    let cap = 50_000usize;

    let (schema, batches) = exec_sql_arrow(&view, sql).expect("arrow query");
    let arrow_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(arrow_rows, cap);
    assert!(batches.iter().all(|batch| batch.num_rows() <= cap));
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    let mut values = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column is a real Arrow Int64Array");
        values.extend(column.values().iter().copied());
    }
    assert_eq!(values.first(), Some(&1));
    assert_eq!(values.last(), Some(&(cap as i64)));

    let typed = exec_sql_typed(&view, sql).expect("typed query");
    assert_eq!(typed.rows.len(), cap);
    assert_eq!(typed.rows.first().map(|row| &row[0]), Some(&json!(1)));
    assert_eq!(typed.rows.last().map(|row| &row[0]), Some(&json!(cap)));

    let msgpack = exec_sql(&view, sql, &CancellationToken::new()).expect("msgpack query");
    assert_eq!(msgpack.rows.len(), cap);
    let first: Vec<serde_json::Value> = rmp_serde::from_slice(&msgpack.rows[0]).expect("first");
    let last: Vec<serde_json::Value> = rmp_serde::from_slice(&msgpack.rows[cap - 1]).expect("last");
    assert_eq!(first, vec![json!(1)]);
    assert_eq!(last, vec![json!(cap)]);
}

#[test]
fn cached_and_prepared_crossing_cap_matches_all_result_modes() {
    let core = graph_with_agents();
    // `analysis_snapshot_versioned` is gated by eg-core's optional result-cache
    // feature, which this SQL integration target does not enable. This fixture is
    // read-only, so the ordinary snapshot plus its unchanged OCC version is the
    // equivalent cache key pair here.
    let version = core.version();
    let view = core.analysis_snapshot();
    let sql = "SELECT value FROM generate_series(1, 49999) \
               UNION ALL \
               SELECT value FROM generate_series(50000, 50001) \
               ORDER BY value";
    let cap = 50_000usize;

    // The raw Arrow path is the shape-preserving reference: one real Int64 column,
    // ordered rows, and no batch or aggregate over the transport cap.
    let (schema, batches) = exec_sql_arrow(&view, sql).expect("arrow query");
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "value");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        cap
    );
    assert!(batches.iter().all(|batch| batch.num_rows() <= cap));
    assert!(batches
        .iter()
        .all(|batch| batch.schema().as_ref() == schema.as_ref()));
    let arrow_values: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value is Int64")
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(arrow_values.len(), cap);
    assert_eq!(arrow_values.first(), Some(&1));
    assert_eq!(arrow_values.last(), Some(&(cap as i64)));
    assert!(arrow_values.windows(2).all(|pair| pair[0] + 1 == pair[1]));

    // Typed JSON is the uncached reference for the prepared context path.
    let typed = exec_sql_typed(&view, sql).expect("typed query");
    assert_eq!(typed.columns.len(), 1);
    assert_eq!(typed.columns[0].name, "value");
    assert_eq!(typed.columns[0].ty, PgColType::Int8);
    assert_eq!(typed.rows.len(), cap);
    assert_eq!(typed.rows.first(), Some(&vec![json!(1)]));
    assert_eq!(typed.rows.last(), Some(&vec![json!(cap)]));

    // MessagePack uses the version-keyed table cache. Decode every row and compare
    // it to typed JSON, not merely the row count, so column order/value parity is
    // explicit at the crossing boundary.
    let msgpack =
        exec_sql_cached(&view, version, &SqlCache::new(), sql).expect("cached MessagePack query");
    assert_eq!(msgpack.columns, vec!["value".to_string()]);
    assert_eq!(msgpack.rows.len(), cap);
    let msgpack_rows: Vec<Vec<serde_json::Value>> = msgpack
        .rows
        .iter()
        .map(|row| rmp_serde::from_slice(row).expect("MessagePack row"))
        .collect();
    assert_eq!(msgpack_rows, typed.rows);

    // The whole-SessionContext cache is the prepared/tables-aware path. Call it
    // twice: the first call builds the context and the second is a genuine cache
    // hit, while both must preserve the exact same bounded prefix.
    let (store, _temp_path) = TableStore::open_temp().expect("temporary table store");
    let prepared_cache = SqlContextCache::new();
    let prepared = exec_sql_typed_with_tables_cached_cancellable(
        &view,
        version,
        version,
        "tenant-p07b",
        "graph-p07b",
        "caller-p07b",
        &store,
        &prepared_cache,
        sql,
        &CancellationToken::new(),
    )
    .expect("prepared context query");
    assert_eq!(prepared, typed);
    let prepared_hit = exec_sql_typed_with_tables_cached_cancellable(
        &view,
        version,
        version,
        "tenant-p07b",
        "graph-p07b",
        "caller-p07b",
        &store,
        &prepared_cache,
        sql,
        &CancellationToken::new(),
    )
    .expect("prepared context cache hit");
    assert_eq!(prepared_hit, typed);
    assert_eq!(prepared_cache.stats(), (1, 1));

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let stopped = exec_sql_typed_with_tables_cached_cancellable(
        &view,
        version,
        version,
        "tenant-p07b",
        "graph-p07b",
        "caller-p07b",
        &store,
        &prepared_cache,
        sql,
        &cancelled,
    )
    .expect("prepared cancellation is a successful early stop");
    assert!(stopped.rows.is_empty());
    assert_eq!(prepared_cache.stats(), (2, 1));
}
