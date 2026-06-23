//! SQL execution entry (CONCEPT:KG-2.178). Takes a `&GraphView` + a SQL string,
//! builds a SessionContext, registers the `nodes` provider + the `json_get*` UDFs,
//! runs the query, and materializes the result as `QueryResult { columns, rows }`.
//!
//! DataFusion's executor is async and the engine server runs on a multi-thread
//! Tokio reactor. Per the de-risk spike, we drive `collect()` on a dedicated
//! CURRENT-THREAD runtime built inside the call — the handler invokes this from
//! `spawn_blocking` (the `compute_off_lock` idiom), so no DataFusion work ever runs
//! on a reactor worker.

use std::sync::Arc;

use arrow::array::Array;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use eg_core::graph::GraphView;
// The wire DTO lives at the bottom of the DAG (eg-types); the algorithm stays here.
pub use eg_types::protocol::QueryResult;

use super::providers::{infer_edges, infer_nodes, SqlCache};
use super::tablefuncs::{BetweennessFunc, PagerankFunc};
use super::udfs::{epistemic_decay_udf, json_get_f64_udf, json_get_i64_udf, json_get_udf};

/// Implicit max rows guarded into the result. Transport is one Response per
/// Request (no streaming), so an unbounded SELECT would buffer the whole graph in
/// one message; we cap and truncate.
const MAX_ROWS: usize = 50_000;

/// Run `sql` over `view` (read-only, single graph), with no cache: re-scans the
/// `nodes`/`edges` tables every call. Synchronous — builds and drives its own
/// current-thread runtime, safe to call inside `spawn_blocking`.
pub fn exec_sql(view: &GraphView, sql: &str) -> Result<QueryResult, String> {
    let nodes = infer_nodes(view)?;
    let edges = infer_edges(view)?;
    run(view, nodes, edges, sql)
}

/// A coarse, Postgres-mappable column type derived from the Arrow result schema.
/// The pgwire shim (CONCEPT:KG-2.189) maps each to a wire type OID; the variants
/// cover exactly the Arrow types the `nodes`/`edges` schema-on-read inference and
/// ordinary SELECT projections produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgColType {
    Text,
    Int8,
    Float8,
    Bool,
}

/// One typed result column: its name plus the pg-mappable type inferred from the
/// Arrow result schema.
#[derive(Debug, Clone)]
pub struct TypedColumn {
    pub name: String,
    pub ty: PgColType,
}

/// The SQL result with per-column types and rows as decoded JSON values — the
/// shape the pgwire shim needs to emit a Postgres `RowDescription` (type OIDs) +
/// `DataRow`s. Reuses the SAME DataFusion exec path as [`exec_sql`]; the only
/// difference is it surfaces the Arrow column types and hands back JSON cells
/// instead of MessagePack blobs (no wire-protocol re-encode needed downstream).
pub struct TypedQueryResult {
    pub columns: Vec<TypedColumn>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// Run `sql` over `view` and return a [`TypedQueryResult`] (CONCEPT:KG-2.189).
/// Identical execution to [`exec_sql`] — same providers, UDFs, table functions,
/// off-lock snapshot, and current-thread runtime — but it captures the Arrow
/// result schema so the pgwire shim can describe columns with real type OIDs.
pub fn exec_sql_typed(view: &GraphView, sql: &str) -> Result<TypedQueryResult, String> {
    let nodes = infer_nodes(view)?;
    let edges = infer_edges(view)?;
    run_typed(view, nodes, edges, sql)
}

/// Run `sql` over `view` reusing `cache`'s `(nodes, edges)` tables when they are
/// still valid for `version` (the GraphCore OCC `version()`, CONCEPT:KG-2.184).
/// A different version (any committed write bumped it) rebuilds, so the cache never
/// serves stale tables. `view` must be the snapshot taken at `version`.
pub fn exec_sql_cached(
    view: &GraphView,
    version: u64,
    cache: &SqlCache,
    sql: &str,
) -> Result<QueryResult, String> {
    let tables = cache.tables_at(view, version)?;
    run(view, tables.nodes, tables.edges, sql)
}

/// Shared driver: register the two tables, the scalar/aggregate UDFs, and the
/// graph table functions, then collect the query.
fn run(
    view: &GraphView,
    nodes: (
        arrow::datatypes::SchemaRef,
        arrow::record_batch::RecordBatch,
    ),
    edges: (
        arrow::datatypes::SchemaRef,
        arrow::record_batch::RecordBatch,
    ),
    sql: &str,
) -> Result<QueryResult, String> {
    let nodes_table = MemTable::try_new(nodes.0, vec![vec![nodes.1]])
        .map_err(|e| format!("nodes mem table: {e}"))?;
    let edges_table = MemTable::try_new(edges.0, vec![vec![edges.1]])
        .map_err(|e| format!("edges mem table: {e}"))?;

    // The graph table functions run their kernel over an owned snapshot; clone the
    // topology+ids once (cheap relative to the algorithm) so they don't borrow `view`.
    let snap = Arc::new(view.clone());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime build: {e}"))?;

    rt.block_on(async move {
        let ctx = SessionContext::new();
        ctx.register_table("nodes", Arc::new(nodes_table))
            .map_err(|e| format!("register nodes: {e}"))?;
        ctx.register_table("edges", Arc::new(edges_table))
            .map_err(|e| format!("register edges: {e}"))?;
        ctx.register_udf(json_get_udf());
        ctx.register_udf(json_get_f64_udf());
        ctx.register_udf(json_get_i64_udf());
        ctx.register_udf(epistemic_decay_udf());
        ctx.register_udtf("pagerank", Arc::new(PagerankFunc::new(snap.clone())));
        ctx.register_udtf("betweenness", Arc::new(BetweennessFunc::new(snap.clone())));
        #[cfg(feature = "finance")]
        {
            ctx.register_udaf(super::udfs::var_udaf());
            ctx.register_udaf(super::udfs::cvar_udaf());
        }

        let df = ctx.sql(sql).await.map_err(|e| format!("sql: {e}"))?;
        let batches = df.collect().await.map_err(|e| format!("collect: {e}"))?;
        batches_to_result(&batches)
    })
}

/// Same driver as [`run`] but returns a [`TypedQueryResult`] (column types from
/// the Arrow schema + JSON cells). Shares the providers/UDFs/runtime verbatim so
/// the pgwire read path is the SAME engine path as `Method::Sql`.
fn run_typed(
    view: &GraphView,
    nodes: (
        arrow::datatypes::SchemaRef,
        arrow::record_batch::RecordBatch,
    ),
    edges: (
        arrow::datatypes::SchemaRef,
        arrow::record_batch::RecordBatch,
    ),
    sql: &str,
) -> Result<TypedQueryResult, String> {
    let nodes_table = MemTable::try_new(nodes.0, vec![vec![nodes.1]])
        .map_err(|e| format!("nodes mem table: {e}"))?;
    let edges_table = MemTable::try_new(edges.0, vec![vec![edges.1]])
        .map_err(|e| format!("edges mem table: {e}"))?;
    let snap = Arc::new(view.clone());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime build: {e}"))?;

    rt.block_on(async move {
        let ctx = SessionContext::new();
        ctx.register_table("nodes", Arc::new(nodes_table))
            .map_err(|e| format!("register nodes: {e}"))?;
        ctx.register_table("edges", Arc::new(edges_table))
            .map_err(|e| format!("register edges: {e}"))?;
        ctx.register_udf(json_get_udf());
        ctx.register_udf(json_get_f64_udf());
        ctx.register_udf(json_get_i64_udf());
        ctx.register_udf(epistemic_decay_udf());
        ctx.register_udtf("pagerank", Arc::new(PagerankFunc::new(snap.clone())));
        ctx.register_udtf("betweenness", Arc::new(BetweennessFunc::new(snap.clone())));
        #[cfg(feature = "finance")]
        {
            ctx.register_udaf(super::udfs::var_udaf());
            ctx.register_udaf(super::udfs::cvar_udaf());
        }

        let df = ctx.sql(sql).await.map_err(|e| format!("sql: {e}"))?;
        let batches = df.collect().await.map_err(|e| format!("collect: {e}"))?;
        batches_to_typed(&batches)
    })
}

/// Map an Arrow column `DataType` to a coarse pg-mappable [`PgColType`]. Anything
/// outside the small known set falls back to `Text` (stringified), so the wire
/// surface is never lossy-dropped.
fn pg_col_type(dt: &arrow::datatypes::DataType) -> PgColType {
    use arrow::datatypes::DataType::*;
    match dt {
        Boolean => PgColType::Bool,
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => PgColType::Int8,
        Float16 | Float32 | Float64 => PgColType::Float8,
        _ => PgColType::Text,
    }
}

/// Convert the result RecordBatches into typed columns + JSON-per-cell rows,
/// applying the same implicit max-rows guard as [`batches_to_result`].
fn batches_to_typed(
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<TypedQueryResult, String> {
    let columns: Vec<TypedColumn> = match batches.first() {
        Some(b) => b
            .schema()
            .fields()
            .iter()
            .map(|f| TypedColumn {
                name: f.name().clone(),
                ty: pg_col_type(f.data_type()),
            })
            .collect(),
        None => Vec::new(),
    };

    let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
    'outer: for batch in batches {
        for r in 0..batch.num_rows() {
            if rows.len() >= MAX_ROWS {
                break 'outer;
            }
            let mut cells: Vec<serde_json::Value> = Vec::with_capacity(batch.num_columns());
            for c in 0..batch.num_columns() {
                cells.push(cell_to_json(batch.column(c), r)?);
            }
            rows.push(cells);
        }
    }
    Ok(TypedQueryResult { columns, rows })
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
