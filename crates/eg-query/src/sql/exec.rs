//! SQL execution entry (CONCEPT:KG-2.178). Takes a `&GraphView` + a SQL string,
//! builds a SessionContext, registers the `nodes` provider + the `json_get*` UDFs,
//! runs the query, and materializes the result as `QueryResult { columns, rows }`.
//!
//! DataFusion's executor is async and the engine server runs on a multi-thread
//! Tokio reactor. Per the de-risk spike, we drive `collect()` on a dedicated
//! CURRENT-THREAD runtime built inside the call — the handler invokes this from
//! `spawn_blocking` (the `compute_off_lock` idiom), so no DataFusion work ever runs
//! on a reactor worker.

use arrow::array::Array;
use datafusion::prelude::SessionContext;
use eg_core::graph::GraphView;
// The wire DTO lives at the bottom of the DAG (eg-types); the algorithm stays here.
pub use eg_types::protocol::QueryResult;

use super::providers::build_nodes_table;
use super::udfs::{json_get_f64_udf, json_get_i64_udf, json_get_udf};

/// Implicit max rows guarded into the result. Transport is one Response per
/// Request (no streaming), so an unbounded SELECT would buffer the whole graph in
/// one message; we cap and truncate.
const MAX_ROWS: usize = 50_000;

/// Run `sql` over `view` (read-only, single graph). Synchronous: builds and drives
/// its own current-thread runtime, so it is safe to call inside `spawn_blocking`.
pub fn exec_sql(view: &GraphView, sql: &str) -> Result<QueryResult, String> {
    let table = build_nodes_table(view)?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime build: {e}"))?;

    rt.block_on(async move {
        let ctx = SessionContext::new();
        ctx.register_table("nodes", std::sync::Arc::new(table))
            .map_err(|e| format!("register nodes: {e}"))?;
        ctx.register_udf(json_get_udf());
        ctx.register_udf(json_get_f64_udf());
        ctx.register_udf(json_get_i64_udf());

        let df = ctx.sql(sql).await.map_err(|e| format!("sql: {e}"))?;
        let batches = df.collect().await.map_err(|e| format!("collect: {e}"))?;
        batches_to_result(&batches)
    })
}

/// Convert the result RecordBatches into column names + msgpack-per-row, applying
/// the implicit max-rows guard.
fn batches_to_result(batches: &[arrow::record_batch::RecordBatch]) -> Result<QueryResult, String> {
    let columns: Vec<String> = match batches.first() {
        Some(b) => b
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect(),
        None => Vec::new(),
    };

    let mut rows: Vec<Vec<u8>> = Vec::new();
    'outer: for batch in batches {
        for r in 0..batch.num_rows() {
            if rows.len() >= MAX_ROWS {
                break 'outer;
            }
            let mut cells: Vec<serde_json::Value> = Vec::with_capacity(batch.num_columns());
            for c in 0..batch.num_columns() {
                cells.push(cell_to_json(batch.column(c), r)?);
            }
            let blob = rmp_serde::to_vec(&cells).map_err(|e| format!("encode row: {e}"))?;
            rows.push(blob);
        }
    }

    Ok(QueryResult { columns, rows })
}

/// One cell at `(col, row)` to a `serde_json::Value`. Covers the types the `nodes`
/// schema and ordinary SELECT projections produce (the inferred columns + UDF
/// outputs + literals).
fn cell_to_json(col: &dyn Array, row: usize) -> Result<serde_json::Value, String> {
    use arrow::array::{
        BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
    };
    use arrow::datatypes::DataType::*;
    use serde_json::Value;

    if col.is_null(row) {
        return Ok(Value::Null);
    }
    let v = match col.data_type() {
        Utf8 => {
            let a = col.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(a.value(row).to_string())
        }
        Boolean => {
            let a = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(a.value(row))
        }
        Int64 => {
            let a = col.as_any().downcast_ref::<Int64Array>().unwrap();
            Value::Number(a.value(row).into())
        }
        UInt64 => {
            let a = col.as_any().downcast_ref::<UInt64Array>().unwrap();
            Value::Number(a.value(row).into())
        }
        Float64 => {
            let a = col.as_any().downcast_ref::<Float64Array>().unwrap();
            serde_json::Number::from_f64(a.value(row))
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        Binary => {
            // The `props` escape-hatch column: hand back the raw msgpack bytes so a
            // caller selecting `props` still gets the blob (serde_json encodes a
            // byte slice as an array of numbers).
            let a = col.as_any().downcast_ref::<BinaryArray>().unwrap();
            Value::Array(
                a.value(row)
                    .iter()
                    .map(|b| Value::Number((*b).into()))
                    .collect(),
            )
        }
        other => {
            // Any other DataFusion-produced type (e.g. an aggregate) — stringify so
            // the result is never lossy-dropped.
            return Err(format!("unsupported result column type: {other:?}"));
        }
    };
    Ok(v)
}
