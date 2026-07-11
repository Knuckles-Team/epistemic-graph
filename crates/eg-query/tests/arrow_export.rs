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
use eg_query::exec_sql_arrow;
use serde_json::json;

/// A graph with three `:Agent` nodes carrying a `name` (Utf8) and a `score` (Int64)
/// property, for exercising the Arrow schema/typed-value round trip.
fn graph_with_agents() -> GraphCore {
    let core = GraphCore::new();
    for (id, name, score) in [("a1", "alice", 10i64), ("a2", "bob", 50), ("a3", "carol", 90)] {
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
    assert_eq!(schema.field_with_name("id").unwrap().data_type(), &DataType::Utf8);
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
