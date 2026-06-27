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
use arrow::datatypes::SchemaRef;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use eg_core::graph::GraphView;
// The wire DTO lives at the bottom of the DAG (eg-types); the algorithm stays here.
pub use eg_types::protocol::QueryResult;

use super::catalog::register_pg_catalog;
use super::providers::{infer_edges, infer_nodes, NodesTableProvider, SqlCache};
use super::tablefuncs::{BetweennessFunc, PagerankFunc};
use super::udfs::{epistemic_decay_udf, json_get_f64_udf, json_get_i64_udf, json_get_udf};
use crate::tables::TableStore;

/// One user table materialized for registration into the SQL context: its name plus
/// the Arrow `(schema, batch)` scanned out of the redb store (CONCEPT:EG-018).
type UserTable = (String, SchemaRef, arrow::record_batch::RecordBatch);

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
    run(view, nodes, edges, Vec::new(), sql)
}

/// Materialize EVERY user table in `store` into an Arrow `(name, schema, batch)` so
/// it can be registered alongside `nodes`/`edges` (CONCEPT:EG-018). One redb scan
/// per table; the unified-engine payoff is that the resulting tables join the graph
/// in a single DataFusion plan.
fn materialize_user_tables(store: &TableStore) -> Result<Vec<UserTable>, String> {
    let mut out = Vec::new();
    for name in store.list_tables()? {
        let schema = match store.get_schema(&name)? {
            Some(s) => s,
            None => continue,
        };
        let rows = store.scan(&name)?;
        let (arrow_schema, batch) = crate::tables::provider::materialize(&schema, &rows)?;
        out.push((name, arrow_schema, batch));
    }
    Ok(out)
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
    run_typed(view, nodes, edges, Vec::new(), sql)
}

/// Run `sql` over `view` AND the user tables in `store` (CONCEPT:EG-018). Identical
/// to [`exec_sql_typed`] but every `CREATE TABLE` user table is registered as a
/// DataFusion `TableProvider` alongside `nodes`/`edges`, so a SELECT can read a user
/// table, JOIN it to the graph, or both in ONE plan. This is the read path the pgwire
/// shim calls so `psql`/ORMs see user tables and the graph in the same database.
pub fn exec_sql_typed_with_tables(
    view: &GraphView,
    store: &TableStore,
    sql: &str,
) -> Result<TypedQueryResult, String> {
    let nodes = infer_nodes(view)?;
    let edges = infer_edges(view)?;
    let user = materialize_user_tables(store)?;
    run_typed(view, nodes, edges, user, sql)
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
    run(view, tables.nodes, tables.edges, Vec::new(), sql)
}

/// Build the shared `SessionContext` for a SQL run: register the `nodes`/`edges`
/// tables, the scalar/aggregate UDFs, the graph table functions, the synthetic
/// `pg_catalog` + catalog functions (CONCEPT:KG-2.201), and enable DataFusion's
/// native `information_schema` so a real driver/ORM can introspect on connect.
///
/// `nodes_schema`/`edges_schema` are the inferred Arrow schemas the catalog is
/// derived from (the catalog reports exactly the columns a SELECT returns). The
/// `nodes` table is the index-pushdown provider; `edges` a plain MemTable.
fn build_ctx(
    snap: Arc<GraphView>,
    nodes: (SchemaRef, arrow::record_batch::RecordBatch),
    edges: (SchemaRef, arrow::record_batch::RecordBatch),
    user_tables: Vec<UserTable>,
) -> Result<SessionContext, String> {
    let nodes_schema = nodes.0.clone();
    let edges_schema = edges.0.clone();

    // CONCEPT:KG-2.199: the `nodes` table is a custom provider with secondary-index
    // predicate pushdown — a `WHERE col = 'x'` narrows rows via the index instead of
    // scanning every node. `edges` stays a plain MemTable.
    let nodes_table = NodesTableProvider::new(nodes.0, nodes.1);
    let edges_table = MemTable::try_new(edges.0, vec![vec![edges.1]])
        .map_err(|e| format!("edges mem table: {e}"))?;

    // CONCEPT:KG-2.201: enable DataFusion's native `information_schema` so
    // `information_schema.tables`/`.columns` reflect the registered relations.
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);

    ctx.register_table("nodes", Arc::new(nodes_table))
        .map_err(|e| format!("register nodes: {e}"))?;
    ctx.register_table("edges", Arc::new(edges_table))
        .map_err(|e| format!("register edges: {e}"))?;
    // CONCEPT:EG-018: register each user table (a MemTable over its scanned rows)
    // alongside the graph projection, and remember its schema for the catalog so a
    // reflecting driver sees it in `pg_class`/`information_schema`.
    let mut user_relations: Vec<(String, SchemaRef)> = Vec::with_capacity(user_tables.len());
    for (name, schema, batch) in user_tables {
        let table = MemTable::try_new(schema.clone(), vec![vec![batch]])
            .map_err(|e| format!("user table `{name}` mem table: {e}"))?;
        ctx.register_table(name.as_str(), Arc::new(table))
            .map_err(|e| format!("register user table `{name}`: {e}"))?;
        user_relations.push((name, schema));
    }
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
    // CONCEPT:KG-2.201/EG-018: supplement the pg_catalog DataFusion does not provide,
    // including the user tables so an ORM reflects them.
    register_pg_catalog(&ctx, &nodes_schema, &edges_schema, &user_relations)?;
    Ok(ctx)
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
    user_tables: Vec<UserTable>,
    sql: &str,
) -> Result<QueryResult, String> {
    // The graph table functions run their kernel over an owned snapshot; clone the
    // topology+ids once (cheap relative to the algorithm) so they don't borrow `view`.
    let snap = Arc::new(view.clone());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime build: {e}"))?;

    rt.block_on(async move {
        let ctx = build_ctx(snap, nodes, edges, user_tables)?;
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
    user_tables: Vec<UserTable>,
    sql: &str,
) -> Result<TypedQueryResult, String> {
    let snap = Arc::new(view.clone());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime build: {e}"))?;

    rt.block_on(async move {
        let ctx = build_ctx(snap, nodes, edges, user_tables)?;
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

/// One cell at `(col, row)` to a `serde_json::Value` (CONCEPT:KG-2.196).
///
/// DataFusion 43 fully executes aggregates / GROUP BY / HAVING, window functions,
/// CTEs, subqueries, set ops (UNION/INTERSECT/EXCEPT) and DISTINCT — but their
/// results materialize as a much wider set of Arrow types than the `nodes`/`edges`
/// schema-on-read produces (every Int/UInt/Float width, Decimal128/256, Date/Time/
/// Timestamp, Utf8 variants, List/Struct/Map intermediates). This decoder covers the
/// common numeric/string/binary fast paths natively, then degrades any remaining
/// type to its Arrow display string via [`array_value_to_string`] — so a cell type
/// NEVER hard-errors at result materialization (which would fail a query the compute
/// already succeeded at). Fallback representation choices: Decimal128/256 → lossless
/// decimal string; Date/Time/Timestamp(*)[/tz] → ISO-8601 string; List/Struct/Map →
/// Arrow's textual rendering.
fn cell_to_json(col: &dyn Array, row: usize) -> Result<serde_json::Value, String> {
    use arrow::array::{
        BinaryArray, BooleanArray, Float16Array, Float32Array, Float64Array, Int16Array,
        Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
        StringViewArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow::datatypes::DataType::*;
    use arrow::util::display::array_value_to_string;
    use serde_json::Value;

    if col.is_null(row) {
        return Ok(Value::Null);
    }

    // A binary/blob column → JSON array of byte numbers (the historical `props`
    // escape-hatch encoding: serde_json renders a byte slice as an array of numbers).
    let bytes_to_json = |bytes: &[u8]| -> Value {
        Value::Array(bytes.iter().map(|b| Value::Number((*b).into())).collect())
    };

    let v = match col.data_type() {
        // ── strings ──
        Utf8 => Value::String(
            col.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        LargeUtf8 => Value::String(
            col.as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        Utf8View => Value::String(
            col.as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        // ── bool ──
        Boolean => Value::Bool(
            col.as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row),
        ),
        // ── signed ints (all widths) → JSON number ──
        Int8 => Value::Number(
            col.as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(row)
                .into(),
        ),
        Int16 => Value::Number(
            col.as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row)
                .into(),
        ),
        Int32 => Value::Number(
            col.as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row)
                .into(),
        ),
        Int64 => Value::Number(
            col.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row)
                .into(),
        ),
        // ── unsigned ints (all widths) → JSON number ──
        UInt8 => Value::Number(
            col.as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(row)
                .into(),
        ),
        UInt16 => Value::Number(
            col.as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(row)
                .into(),
        ),
        UInt32 => Value::Number(
            col.as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row)
                .into(),
        ),
        UInt64 => Value::Number(
            col.as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row)
                .into(),
        ),
        // ── floats (all widths) → JSON number; non-finite → null ──
        Float16 => {
            let f = col
                .as_any()
                .downcast_ref::<Float16Array>()
                .unwrap()
                .value(row);
            serde_json::Number::from_f64(f.to_f64())
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        Float32 => {
            let f = col
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row);
            serde_json::Number::from_f64(f as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        Float64 => {
            let f = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            serde_json::Number::from_f64(f)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        // ── binary blobs → array of byte numbers (the `props` escape hatch) ──
        Binary => bytes_to_json(
            col.as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(row),
        ),
        LargeBinary => bytes_to_json(
            col.as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(row),
        ),
        // ── everything else: Decimal128/256 (→ lossless decimal string), Date32/64,
        // Time32/64, Timestamp(*)[/tz] (→ ISO-8601 string), List/LargeList/Struct/Map
        // (→ Arrow's textual rendering), or any unforeseen type — degrade to the Arrow
        // display string. NEVER hard-error on a cell type. ──
        other => {
            // `array_value_to_string` formats with `with_display_error(true)`, so even
            // a type Arrow itself can't render yields a placeholder string rather than
            // an Err — but guard the Result anyway and fall to the type name as a last
            // resort so a cell decode can never fail.
            match array_value_to_string(col, row) {
                Ok(s) => Value::String(s),
                Err(e) => {
                    tracing::debug!(
                        target: "eg_query::sql",
                        "cell display fallback failed for {other:?}: {e}; using type name"
                    );
                    Value::String(format!("{other:?}"))
                }
            }
        }
    };
    Ok(v)
}
