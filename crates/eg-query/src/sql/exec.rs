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

use super::catalog::register_system_catalogs;
use super::providers::{infer_edges, infer_nodes, NodesTableProvider, SqlCache};
use super::tablefuncs::{BetweennessFunc, GenerateSeriesFunc, PagerankFunc};
use super::udfs::{
    bm25_match_udf, bm25_score_udf, bm25_snippet_udf, epistemic_decay_udf, greatest_udf,
    int4range_udf, json_get_f64_udf, json_get_i64_udf, json_get_udf, least_udf,
    range_contained_by_udf, range_contains_range_udf, range_contains_udf, range_overlaps_udf,
    time_bucket_udf, tsrange_udf, vector_cosine_udf, vector_ip_udf, vector_l2_udf,
};
use crate::tables::{StoredFunction, TableStore};

/// One user table materialized for registration into the SQL context: its name plus
/// the Arrow `(schema, batch)` scanned out of the redb store (CONCEPT:EG-018).
type UserTable = (String, SchemaRef, arrow::record_batch::RecordBatch);

/// Register the CONCEPT:EG-104 Postgres common-function surface — `greatest`/`least`,
/// the range constructors (`int4range`/`tsrange`) + predicates (`range_contains`/
/// `range_overlaps`/`range_contains_range`/`range_contained_by`), and the
/// `generate_series` table function — on `ctx`. Shared so the graph exec path and the
/// tables-only obs path expose the identical function set. (DataFusion's native array
/// functions + `unnest` come from the `nested_expressions` feature, registered by
/// `SessionContext::new_with_config`'s default-feature set — no explicit call needed.)
fn register_pg_common(ctx: &SessionContext) {
    ctx.register_udf(greatest_udf());
    ctx.register_udf(least_udf());
    ctx.register_udf(int4range_udf());
    ctx.register_udf(tsrange_udf());
    ctx.register_udf(range_contains_udf());
    ctx.register_udf(range_overlaps_udf());
    ctx.register_udf(range_contains_range_udf());
    ctx.register_udf(range_contained_by_udf());
    ctx.register_udtf("generate_series", Arc::new(GenerateSeriesFunc));
}

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
    run(view, nodes, edges, Vec::new(), Vec::new(), Vec::new(), sql)
}

/// Run read-only `sql` over a set of pre-built in-memory Arrow tables — NO graph
/// (CONCEPT:EG-162). Each `(name, schema, batches)` is registered as a DataFusion
/// `MemTable`, and the `json_get*` scalar UDFs are registered so a JSON-object column
/// (e.g. an observability log record's `attrs`) is reachable schema-on-read. This is
/// the SQL leg the observability log-search surface (`src/server/obs`) drives: it
/// hands the Parquet log segments + hot series it scanned out of the blob CAS as Arrow
/// batches and runs SQL (`SELECT severity, count(*) FROM logs GROUP BY severity`, …)
/// over them. Synchronous — builds and drives its own current-thread runtime, safe to
/// call inside `spawn_blocking` (the `compute_off_lock` idiom).
pub fn exec_sql_over_tables(
    tables: Vec<(String, SchemaRef, Vec<arrow::record_batch::RecordBatch>)>,
    sql: &str,
) -> Result<TypedQueryResult, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime build: {e}"))?;
    let sql = super::classify::desugar_vector_ops(sql);
    rt.block_on(async move {
        let config = SessionConfig::new().with_information_schema(true);
        let ctx = SessionContext::new_with_config(config);
        for (name, schema, batches) in tables {
            // One partition; an empty `batches` registers a valid empty table so a
            // `SELECT … FROM <name>` returns 0 rows rather than "table not found".
            let mem = MemTable::try_new(schema, vec![batches])
                .map_err(|e| format!("mem table `{name}`: {e}"))?;
            ctx.register_table(name.as_str(), Arc::new(mem))
                .map_err(|e| format!("register `{name}`: {e}"))?;
        }
        ctx.register_udf(json_get_udf());
        ctx.register_udf(json_get_f64_udf());
        ctx.register_udf(json_get_i64_udf());
        // CONCEPT:EG-117/EG-119 — time_bucket + BM25 UDFs so the obs log-search SQL leg
        // can time-bucket and lexically filter too.
        ctx.register_udf(time_bucket_udf());
        ctx.register_udf(bm25_match_udf());
        ctx.register_udf(bm25_score_udf());
        ctx.register_udf(bm25_snippet_udf());
        // CONCEPT:EG-104 — greatest/least, range fns, generate_series (obs path too).
        register_pg_common(&ctx);
        let df = ctx.sql(&sql).await.map_err(|e| format!("sql: {e}"))?;
        let batches = df.collect().await.map_err(|e| format!("collect: {e}"))?;
        batches_to_typed(&batches)
    })
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
    /// A pgvector `vector` column (CONCEPT:EG-115) — a `List<Float32>` result column.
    /// The pgwire shim maps it to a stable float-array wire OID and renders each value
    /// as the pgvector text form `[1,2,3]`.
    Vector,
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
    run_typed(view, nodes, edges, Vec::new(), Vec::new(), Vec::new(), sql)
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
    // CONCEPT:EG-072: the durable views, registered as read-only named queries so a
    // SELECT that references a view expands its stored SELECT during context build.
    let views = store.list_views()?;
    // CONCEPT:EG-118: the durable SQL stored functions, expanded into the query text
    // (scalar → scalar subquery; table → parameterized-view subquery) before planning.
    let functions = store.list_functions()?;
    run_typed(view, nodes, edges, user, views, functions, sql)
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
    run(
        view,
        tables.nodes,
        tables.edges,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        sql,
    )
}

/// The materialized `SessionContext` plus the live-relation Arrow schemas the
/// system catalogs are synthesized from (CONCEPT:EG-103). Returned by [`build_ctx`]
/// so the caller can register the `pg_catalog` + `information_schema` system views
/// AFTER the durable views are registered (their columns are read back from the
/// registered view providers).
struct BuiltCtx {
    ctx: SessionContext,
    nodes_schema: SchemaRef,
    edges_schema: SchemaRef,
    user_relations: Vec<(String, SchemaRef)>,
}

/// Build the shared `SessionContext` for a SQL run: register the `nodes`/`edges`
/// tables, the scalar/aggregate UDFs, and the graph table functions. The synthetic
/// Postgres system catalogs (`pg_catalog.*` + a fully-synthesized `information_schema.*`,
/// CONCEPT:EG-103, extending CONCEPT:KG-2.201) are registered SEPARATELY by
/// [`register_system_catalogs`] AFTER the durable views are wired, so views appear as
/// relations (`relkind='v'`) with their real columns. DataFusion's NATIVE
/// `information_schema` is deliberately DISABLED here because the engine synthesizes the
/// whole schema itself (native cannot be extended with `routines`/`key_column_usage`/
/// `table_constraints`); the synthesized `information_schema.tables`/`.columns` stay in
/// sync with the same schema-on-read inference.
///
/// `nodes_schema`/`edges_schema` are the inferred Arrow schemas the catalog is
/// derived from (the catalog reports exactly the columns a SELECT returns). The
/// `nodes` table is the index-pushdown provider; `edges` a plain MemTable.
fn build_ctx(
    snap: Arc<GraphView>,
    nodes: (SchemaRef, arrow::record_batch::RecordBatch),
    edges: (SchemaRef, arrow::record_batch::RecordBatch),
    user_tables: Vec<UserTable>,
) -> Result<BuiltCtx, String> {
    let nodes_schema = nodes.0.clone();
    let edges_schema = edges.0.clone();

    // CONCEPT:KG-2.199: the `nodes` table is a custom provider with secondary-index
    // predicate pushdown — a `WHERE col = 'x'` narrows rows via the index instead of
    // scanning every node. `edges` stays a plain MemTable.
    let nodes_table = NodesTableProvider::new(nodes.0, nodes.1);
    let edges_table = MemTable::try_new(edges.0, vec![vec![edges.1]])
        .map_err(|e| format!("edges mem table: {e}"))?;

    // CONCEPT:EG-103: DataFusion's native `information_schema` is DISABLED — the engine
    // synthesizes the whole `information_schema` (plus `pg_catalog`) itself in
    // `register_system_catalogs`, because native cannot be extended with the
    // `routines`/`key_column_usage`/`table_constraints` views psql/ORMs also probe.
    let config = SessionConfig::new().with_information_schema(false);
    let ctx = SessionContext::new_with_config(config);

    ctx.register_table("nodes", Arc::new(nodes_table))
        .map_err(|e| format!("register nodes: {e}"))?;
    ctx.register_table("edges", Arc::new(edges_table))
        .map_err(|e| format!("register edges: {e}"))?;
    // CONCEPT:EG-018: register each user table (a MemTable over its scanned rows)
    // alongside the graph projection, and remember its schema for the catalog so a
    // reflecting driver sees it in `pg_class`/`information_schema`.
    // CONCEPT:EG-020: register each user table through the SAME secondary-index
    // pushdown provider the `nodes` table uses (`NodesTableProvider` is generic
    // equality-pushdown over an Arrow batch), so a `WHERE col = 'x'` on a user table
    // narrows rows via the index instead of scanning the whole batch.
    let mut user_relations: Vec<(String, SchemaRef)> = Vec::with_capacity(user_tables.len());
    for (name, schema, batch) in user_tables {
        let table = NodesTableProvider::new(schema.clone(), batch);
        ctx.register_table(name.as_str(), Arc::new(table))
            .map_err(|e| format!("register user table `{name}`: {e}"))?;
        user_relations.push((name, schema));
    }
    ctx.register_udf(json_get_udf());
    ctx.register_udf(json_get_f64_udf());
    ctx.register_udf(json_get_i64_udf());
    ctx.register_udf(epistemic_decay_udf());
    // CONCEPT:EG-115 — pgvector distance functions the `<->`/`<=>`/`<#>` operators
    // desugar to (brute-force over a vector column; the eg-ann index pushdown is EG-116).
    ctx.register_udf(vector_l2_udf());
    ctx.register_udf(vector_cosine_udf());
    ctx.register_udf(vector_ip_udf());
    // CONCEPT:EG-117 — TimescaleDB `time_bucket`. CONCEPT:EG-119 — ParadeDB BM25
    // `col @@@ 'q'` / `paradedb.score()`/`snippet()` desugar targets.
    ctx.register_udf(time_bucket_udf());
    ctx.register_udf(bm25_match_udf());
    ctx.register_udf(bm25_score_udf());
    ctx.register_udf(bm25_snippet_udf());
    ctx.register_udtf("pagerank", Arc::new(PagerankFunc::new(snap.clone())));
    ctx.register_udtf("betweenness", Arc::new(BetweennessFunc::new(snap.clone())));
    // CONCEPT:EG-104 — greatest/least, int4range/tsrange + range predicates, and the
    // generate_series table function, rounding out the Postgres common-function surface.
    register_pg_common(&ctx);
    #[cfg(feature = "finance")]
    {
        ctx.register_udaf(super::udfs::var_udaf());
        ctx.register_udaf(super::udfs::cvar_udaf());
    }
    // CONCEPT:EG-103: the `pg_catalog` + `information_schema` system views are registered
    // by the caller via `register_system_catalogs` AFTER `register_views`, so views are
    // synthesized as relations with their real column schemas.
    Ok(BuiltCtx {
        ctx,
        nodes_schema,
        edges_schema,
        user_relations,
    })
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
    views: Vec<(String, String)>,
    functions: Vec<StoredFunction>,
    sql: &str,
) -> Result<QueryResult, String> {
    // The graph table functions run their kernel over an owned snapshot; clone the
    // topology+ids once (cheap relative to the algorithm) so they don't borrow `view`.
    let snap = Arc::new(view.clone());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime build: {e}"))?;

    // CONCEPT:EG-103 — strip the `pg_catalog.` qualifier off catalog FUNCTION calls
    // (psql `\d`/ORMs emit `pg_catalog.format_type(...)`) so the bare-name UDFs resolve;
    // schema-qualified TABLE refs (`pg_catalog.pg_class`) are untouched.
    let sql = super::catalog::strip_pg_catalog_fn_qualifier(sql);
    // CONCEPT:EG-118 — expand SQL stored-function calls into inline SQL (scalar subquery /
    // parameterized-view subquery) BEFORE the pgvector desugar + planning, so an inlined
    // body is itself desugared and planned. A no-op when there are no functions.
    let sql = super::funcs::expand_functions(&sql, &functions)?;
    // CONCEPT:EG-115 — rewrite pgvector distance operators (`<->`/`<=>`/`<#>`) to the
    // registered `vector_*` UDF calls BEFORE DataFusion plans the SQL (it has no
    // operator for them). A no-op when none are present or the SQL doesn't parse.
    let sql = super::classify::desugar_vector_ops(&sql);
    rt.block_on(async move {
        let built = build_ctx(snap, nodes, edges, user_tables)?;
        let ctx = built.ctx;
        register_views(&ctx, &views, &functions).await?;
        // CONCEPT:EG-103 — synthesize `pg_catalog` + `information_schema` from the live
        // relations (nodes/edges/user tables), the now-registered views, and the stored
        // functions, so psql/ORMs can introspect the real schema.
        register_system_catalogs(
            &ctx,
            &built.nodes_schema,
            &built.edges_schema,
            &built.user_relations,
            &views,
            &functions,
        )
        .await?;
        let df = ctx.sql(&sql).await.map_err(|e| format!("sql: {e}"))?;
        let batches = df.collect().await.map_err(|e| format!("collect: {e}"))?;
        batches_to_result(&batches)
    })
}

/// Register each durable view as a DataFusion logical view (CONCEPT:EG-072): plan its
/// stored SELECT against the already-registered `nodes`/`edges`/user tables, then
/// register the resulting `ViewTable` under the view's name so a query that references
/// it expands the SELECT. A view whose SELECT fails to plan (e.g. it referenced a table
/// since dropped) is skipped with a debug log rather than failing every query.
async fn register_views(
    ctx: &SessionContext,
    views: &[(String, String)],
    functions: &[StoredFunction],
) -> Result<(), String> {
    for (name, select_sql) in views {
        // CONCEPT:EG-118 — a view body may itself call a stored function; expand it first.
        let select_sql = super::funcs::expand_functions(select_sql, functions)
            .unwrap_or_else(|_| select_sql.clone());
        // CONCEPT:EG-115 — a view body may itself use the pgvector operators.
        let select_sql = super::classify::desugar_vector_ops(&select_sql);
        match ctx.sql(&select_sql).await {
            Ok(df) => {
                let provider = df.into_view();
                if let Err(e) = ctx.register_table(name.as_str(), provider) {
                    tracing::debug!(
                        target: "eg_query::sql",
                        "skipping view `{name}`: register failed: {e}"
                    );
                }
            }
            Err(e) => tracing::debug!(
                target: "eg_query::sql",
                "skipping view `{name}`: SELECT failed to plan: {e}"
            ),
        }
    }
    Ok(())
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
    views: Vec<(String, String)>,
    functions: Vec<StoredFunction>,
    sql: &str,
) -> Result<TypedQueryResult, String> {
    let snap = Arc::new(view.clone());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime build: {e}"))?;

    // CONCEPT:EG-103 — see `run`: strip `pg_catalog.` off catalog function calls first.
    let sql = super::catalog::strip_pg_catalog_fn_qualifier(sql);
    // CONCEPT:EG-118 — see `run`: expand SQL stored-function calls before desugar/planning.
    let sql = super::funcs::expand_functions(&sql, &functions)?;
    // CONCEPT:EG-115 — see `run`: desugar the pgvector operators before planning.
    let sql = super::classify::desugar_vector_ops(&sql);
    rt.block_on(async move {
        let built = build_ctx(snap, nodes, edges, user_tables)?;
        let ctx = built.ctx;
        register_views(&ctx, &views, &functions).await?;
        // CONCEPT:EG-103 — synthesize `pg_catalog` + `information_schema` (see `run`).
        register_system_catalogs(
            &ctx,
            &built.nodes_schema,
            &built.edges_schema,
            &built.user_relations,
            &views,
            &functions,
        )
        .await?;
        let df = ctx.sql(&sql).await.map_err(|e| format!("sql: {e}"))?;
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
        // CONCEPT:EG-115 — a `List<Float32>` result column is a pgvector `vector`.
        List(field) | FixedSizeList(field, _) if *field.data_type() == Float32 => {
            PgColType::Vector
        }
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
        // ── pgvector `vector` (CONCEPT:EG-115): a `List<Float32>` cell → JSON array of
        // numbers; the pgwire shim renders that array as the pgvector text `[1,2,3]`. ──
        List(field) | FixedSizeList(field, _) if *field.data_type() == Float32 => {
            use arrow::array::{FixedSizeListArray, ListArray};
            let child = if let Some(la) = col.as_any().downcast_ref::<ListArray>() {
                la.value(row)
            } else {
                col.as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap()
                    .value(row)
            };
            let floats = child
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or("vector child is not Float32")?;
            Value::Array(
                (0..floats.len())
                    .map(|i| {
                        serde_json::Number::from_f64(floats.value(i) as f64)
                            .map(Value::Number)
                            .unwrap_or(Value::Null)
                    })
                    .collect(),
            )
        }
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
