//! SQL execution entry (CONCEPT:EG-KG.query.read-only-sql-query). Takes a `&GraphView` + a SQL string,
//! builds a SessionContext, registers the `nodes` provider + the `json_get*` UDFs,
//! runs the query, and materializes the result as `QueryResult { columns, rows }`.
//!
//! DataFusion's executor is async and the engine server runs on a multi-thread
//! Tokio reactor. Per the de-risk spike, we drive `collect()` on a dedicated
//! CURRENT-THREAD runtime built inside the call — the handler invokes this from
//! `spawn_blocking` (the `compute_off_lock` idiom), so no DataFusion work ever runs
//! on a reactor worker.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::Array;
use arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use eg_core::graph::GraphView;
// The wire DTO lives at the bottom of the DAG (eg-types); the algorithm stays here.
pub use eg_types::protocol::QueryResult;

use super::catalog::register_system_catalogs;
use super::pgfamily::{plan_ann_search, AnnIndexPlan};
use super::providers::{infer_nodes, EdgesTableProvider, NodesTableProvider, SqlCache};
use super::tablefuncs::{BetweennessFunc, GenerateSeriesFunc, PagerankFunc};
use super::udfs::{
    base64_decode_udf, base64_encode_udf, bm25_match_udf, bm25_score_udf, bm25_snippet_udf,
    epistemic_decay_udf, greatest_udf, int4range_udf, ipcontains_udf, ipfamily_udf, iphost_udf,
    ipmasklen_udf, json_get_f64_udf, json_get_i64_udf, json_get_udf, least_udf, md5_udf,
    range_contained_by_udf, range_contains_range_udf, range_contains_udf, range_overlaps_udf,
    sha1_udf, sha256_udf, time_bucket_udf, tsrange_udf, vector_cosine_udf, vector_ip_udf,
    vector_l2_udf,
};
use crate::tables::{StoredFunction, TableSchema, TableStore};

/// One user table's registration plan for `build_ctx` (CONCEPT:EG-KG.query.register-user-tables-alongside):
/// [`materialize_user_tables`] chooses PER TABLE between the two variants below.
enum UserTable {
    /// `(name, schema, batch)` pre-materialized out of the redb store —
    /// byte-identical to this crate's original sole behavior. The ONLY mode
    /// [`apply_ann_pushdown`]'s durable-index top-k slice can narrow (it mutates
    /// an already-materialized batch in place), so this variant is used for a
    /// table a durable ANN index actually covers.
    Eager(String, SchemaRef, arrow::record_batch::RecordBatch),
    /// `(name, typed catalog schema, store handle)` — row materialization
    /// deferred to a [`crate::tables::provider::UserTableProvider`], which pushes
    /// a `SERIAL`-column equality down to a redb point-get instead of a full
    /// table scan (see that type's own module doc for exactly what still falls
    /// back to a scan). Used for every OTHER table — the common case, since a
    /// durable ANN index over a user table is a narrow, opt-in feature.
    Lazy(String, TableSchema, TableStore),
}

/// Register the CONCEPT:EG-KG.query.greatest-least-int4range-tsrange Postgres common-function surface — `greatest`/`least`,
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
    // CONCEPT:EG-KG.query.sqlite-compat-scalar-udfs (turso-assimilation rank 14) —
    // crypto hash + base64 + ipaddr helpers turso ships as loadable extensions.
    ctx.register_udf(md5_udf());
    ctx.register_udf(sha1_udf());
    ctx.register_udf(sha256_udf());
    ctx.register_udf(base64_encode_udf());
    ctx.register_udf(base64_decode_udf());
    ctx.register_udf(ipfamily_udf());
    ctx.register_udf(ipmasklen_udf());
    ctx.register_udf(iphost_udf());
    ctx.register_udf(ipcontains_udf());
}

/// Register the CONCEPT:EG-KG.query.surface-b-numeric-operators Analytics-Program Surface-B numeric operators — the
/// `eg-numeric`-backed `cosine_sim`/`l2_normalize`/`zscore` scalar UDFs + the
/// `covariance` UDAF — so analytics run in-engine over resident columns
/// (compute-near-data). Gated behind the `numeric` feature (out of `pi`); a no-numeric
/// build links neither eg-numeric nor nalgebra. Shared by the graph exec path and the
/// tables-only obs path so both expose the identical operator set.
#[cfg(feature = "numeric")]
fn register_numeric(ctx: &SessionContext) {
    use super::numeric::{
        cosine_sim_udf, covariance_udaf, kmeans_udaf, l2_normalize_udf, pca_udaf, svd_udaf,
        zscore_udf,
    };
    ctx.register_udf(cosine_sim_udf());
    ctx.register_udf(l2_normalize_udf());
    ctx.register_udf(zscore_udf());
    ctx.register_udaf(covariance_udaf());
    // CONCEPT:EG-KG.query.svd-eg-pca-column svd / EG-335 pca — column→Array2 marshalling UDAFs (singular values /
    // top-k principal-component directions of the aggregated vector column).
    ctx.register_udaf(svd_udaf());
    ctx.register_udaf(pca_udaf());
    // CONCEPT:EG-KG.query.kmeans-clustering-half-one kmeans — the clustering half; one cluster label per aggregated row.
    ctx.register_udaf(kmeans_udaf());
}

/// Implicit max rows guarded into the result. Transport is one Response per
/// Request (no streaming), so an unbounded SELECT would buffer the whole graph in
/// one message; we cap and truncate.
const MAX_ROWS: usize = 50_000;

// ── streaming / spillable / cancellable SQL collect (CONCEPT:EG-KG.query.streaming-spillable-collect, EG-P1-4) ──
//
// `DataFrame::collect()` materializes the ENTIRE result as one `Vec<RecordBatch>`
// before the caller sees anything — no batch-at-a-time consumption, no way to stop
// a running query early, and no bound on peak memory short of `MAX_ROWS` (which is
// only applied AFTER everything is already resident). `collect_streaming` below is
// the drop-in replacement `run`/`run_typed` use instead: it drives DataFusion's own
// `SendableRecordBatchStream` (the SAME physical plan, just pulled batch-by-batch
// instead of awaited-to-completion), checks a [`CancellationToken`] BETWEEN
// batches, and spills already-buffered batches to a temp Arrow-IPC file once the
// running row count crosses a threshold — bounding resident memory to that
// threshold regardless of total result size.
//
// This module element compiles under `sql` only (arrow-ipc + futures-util are both
// already-resolved transitive deps at that feature, so this adds no new crate to a
// default/non-sql build's tree).

/// Cooperative cancellation flag for a running SQL execution
/// (CONCEPT:EG-KG.query.streaming-spillable-collect). Checked BETWEEN batches — never mid-batch —
/// so cancellation is CHUNK-granular: a cancelled query stops after its current
/// in-flight batch rather than draining the whole stream. `Clone` + `Send + Sync`,
/// so a caller can hold one end (e.g. a connection's drop/abort handler, or a
/// per-request registry keyed by request id) while the query runs on the blocking
/// pool that owns the other end.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl CancellationToken {
    /// A fresh, not-yet-cancelled token.
    pub fn new() -> Self {
        Self::default()
    }

    /// Raise the flag — the next batch boundary a [`collect_streaming`] loop checks
    /// will observe it and stop.
    pub fn cancel(&self) {
        self.0.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Has [`Self::cancel`] been called (on this token or any clone of it)?
    pub fn is_cancelled(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Row-count threshold past which [`collect_streaming`] spills already-buffered
/// batches to a temp Arrow-IPC file instead of holding them resident, bounding the
/// SQL collect path's peak memory regardless of total result size. Overridable via
/// `EPISTEMIC_GRAPH_SQL_SPILL_ROWS`; the default sits comfortably above [`MAX_ROWS`]
/// so an ORDINARY served query (already capped there) essentially never spills in
/// practice — the budget exists for the streaming path's internal accumulation
/// ahead of that cap, not to change the served row cap itself. A caller exercising
/// the spill path directly (e.g. a bulk export or an admin query with a raised row
/// cap) passes a smaller threshold to [`collect_streaming`] explicitly.
pub fn default_spill_rows() -> usize {
    std::env::var("EPISTEMIC_GRAPH_SQL_SPILL_ROWS")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(200_000)
}

/// Summary of how a [`collect_streaming`] run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StreamOutcome {
    /// Stopped because the [`CancellationToken`] fired mid-stream (before the source
    /// was drained or the row cap reached).
    pub cancelled: bool,
    /// At least one spill-to-disk round-trip happened (the running row count
    /// crossed the spill threshold at least once).
    pub spilled: bool,
    /// Total rows actually pulled off the stream (across every batch seen, whether
    /// resident or spilled) before stopping.
    pub rows: usize,
}

/// A temp Arrow-IPC (file format) spill backing [`collect_streaming`] — mirrors
/// `eg_plan::runtime`'s spill-round-trip idiom for the planner's own intermediates,
/// applied here to the SQL collect path's `RecordBatch`es. The writer stays open
/// across repeated [`Self::append`] calls (one per spill trigger); [`Self::read_back`]
/// finishes it and re-reads every batch back in order. Best-effort file removal on
/// drop, so a spilled query never leaks a temp file even on an early return.
struct SpillFile {
    path: std::path::PathBuf,
    writer: Option<arrow::ipc::writer::FileWriter<std::fs::File>>,
}

impl SpillFile {
    fn create(schema: &arrow::datatypes::Schema) -> Result<Self, String> {
        let path = spill_path();
        let file = std::fs::File::create(&path)
            .map_err(|e| format!("spill create {}: {e}", path.display()))?;
        let writer = arrow::ipc::writer::FileWriter::try_new(file, schema)
            .map_err(|e| format!("spill writer: {e}"))?;
        Ok(Self {
            path,
            writer: Some(writer),
        })
    }

    fn append(&mut self, batches: &[arrow::record_batch::RecordBatch]) -> Result<(), String> {
        let writer = self.writer.as_mut().ok_or("spill file already finished")?;
        for b in batches {
            writer.write(b).map_err(|e| format!("spill write: {e}"))?;
        }
        Ok(())
    }

    /// Finish the writer and read every spilled batch back, in order.
    fn read_back(mut self) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
        if let Some(mut w) = self.writer.take() {
            w.finish().map_err(|e| format!("spill finish: {e}"))?;
        }
        let file = std::fs::File::open(&self.path)
            .map_err(|e| format!("spill reopen {}: {e}", self.path.display()))?;
        let reader = arrow::ipc::reader::FileReader::try_new(file, None)
            .map_err(|e| format!("spill reader: {e}"))?;
        let mut out = Vec::new();
        for batch in reader {
            out.push(batch.map_err(|e| format!("spill read: {e}"))?);
        }
        Ok(out)
    }
}

impl Drop for SpillFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A unique temp-file path for one spill (`<tmp>/eg-query-sql-spill-<pid>-<seq>.arrow`):
/// the process id + a monotonic counter make it collision-free across threads and
/// concurrent queries, with no temp-dir crate needed.
fn spill_path() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("eg-query-sql-spill-{pid}-{seq}.arrow"))
}

/// Batch-at-a-time, spillable, cancellable materialization of a batch stream
/// (CONCEPT:EG-KG.query.streaming-spillable-collect, EG-P1-4) — the streaming replacement for
/// `DataFrame::collect()`'s eager whole-result buffering. Pulls ONE batch at a time
/// (never blocking on the full result before the caller can act), checks `cancel`
/// between batches, and spills already-buffered batches to a temp Arrow-IPC file
/// once the running row count crosses `spill_rows` — freeing them from RAM so peak
/// resident memory is bounded by `spill_rows`, not the total result size. Stops
/// early — never over-reading the source — once either `cancel` fires or
/// [`MAX_ROWS`] is reached (the same cap the eager path applies, just enforced
/// during accumulation instead of after). Returns every batch actually produced
/// (spilled-then-recovered ++ resident, in original order) plus an outcome
/// summary. Generic over the stream's item type (`Result<RecordBatch, String>`) so
/// this core loop is directly unit-testable against a synthetic `futures_util::stream::iter`
/// fixture, independent of a running DataFusion physical plan.
async fn collect_streaming<S>(
    mut stream: S,
    cancel: &CancellationToken,
    spill_rows: usize,
) -> Result<(Vec<arrow::record_batch::RecordBatch>, StreamOutcome), String>
where
    S: futures_util::Stream<Item = Result<arrow::record_batch::RecordBatch, String>> + Unpin,
{
    use futures_util::StreamExt;

    let mut resident: Vec<arrow::record_batch::RecordBatch> = Vec::new();
    let mut resident_rows = 0usize;
    let mut total_rows = 0usize;
    let mut spill: Option<SpillFile> = None;
    let mut cancelled = false;

    while let Some(next) = stream.next().await {
        if cancel.is_cancelled() {
            cancelled = true;
            break;
        }
        let batch = next?;
        let n = batch.num_rows();
        resident_rows += n;
        total_rows += n;
        resident.push(batch);

        if resident_rows >= spill_rows.max(1) {
            if spill.is_none() {
                spill = Some(SpillFile::create(&resident[0].schema())?);
            }
            spill
                .as_mut()
                .expect("just constructed above")
                .append(&resident)?;
            resident.clear();
            resident_rows = 0;
        }

        if total_rows >= MAX_ROWS {
            break; // never over-read a source already past the served cap
        }
    }

    let spilled = spill.is_some();
    let mut out = match spill {
        Some(f) => f.read_back()?,
        None => Vec::new(),
    };
    out.extend(resident);

    Ok((
        out,
        StreamOutcome {
            cancelled,
            spilled,
            rows: total_rows,
        },
    ))
}

/// Execute `df` and collect its result via the streaming/spillable path
/// ([`collect_streaming`]), threading `cancel` through so a request-scoped
/// cancellation (CONCEPT:EG-KG.query.streaming-spillable-collect, L36) actually stops the
/// stream at its next batch boundary — the drop-in replacement for `df.collect()` every
/// internal call site below now uses. For any query under the default spill threshold
/// (the overwhelming common case) an uncancelled run is behaviorally identical to the
/// eager path: same batches, same order, the same downstream `MAX_ROWS` truncation —
/// only the ACCUMULATION becomes batch-at-a-time and boundedly resident instead of
/// buffering the whole result up front.
async fn collect_default(
    df: datafusion::dataframe::DataFrame,
    cancel: &CancellationToken,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    use futures_util::StreamExt;
    let stream = df
        .execute_stream()
        .await
        .map_err(|e| format!("execute_stream: {e}"))?;
    let stream = stream.map(|r| r.map_err(|e| format!("stream: {e}")));
    let (batches, _outcome) = collect_streaming(stream, cancel, default_spill_rows()).await?;
    Ok(batches)
}

/// Run `sql` over `view` (read-only, single graph), with no cache: re-scan the
/// `nodes`/`edges` tables every call. Synchronous — builds and drives its own
/// current-thread runtime, safe to call inside `spawn_blocking`. `cancel` is a
/// required part of the current execution contract and is checked at each batch
/// boundary.
pub fn exec_sql(
    view: &GraphView,
    sql: &str,
    cancel: &CancellationToken,
) -> Result<QueryResult, String> {
    let nodes = infer_nodes(view)?;
    run(
        view,
        nodes,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        sql,
        cancel,
    )
}

/// Run read-only `sql` over a set of pre-built in-memory Arrow tables — NO graph
/// (CONCEPT:EG-KG.query.concept-4). Each `(name, schema, batches)` is registered as a DataFusion
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
        // CONCEPT:EG-KG.query.continuous-aggregate-lowering/EG-119 — time_bucket + BM25 UDFs so the obs log-search SQL leg
        // can time-bucket and lexically filter too.
        ctx.register_udf(time_bucket_udf());
        ctx.register_udf(bm25_match_udf());
        ctx.register_udf(bm25_score_udf());
        ctx.register_udf(bm25_snippet_udf());
        // CONCEPT:EG-KG.query.greatest-least-int4range-tsrange — greatest/least, range fns, generate_series (obs path too).
        register_pg_common(&ctx);
        // CONCEPT:EG-KG.query.surface-b-numeric-operators — Surface-B numeric operators over the obs Arrow tables too.
        #[cfg(feature = "numeric")]
        register_numeric(&ctx);
        let df = ctx.sql(&sql).await.map_err(|e| format!("sql: {e}"))?;
        let batches = collect_default(df, &CancellationToken::new()).await?;
        batches_to_typed(&batches)
    })
}

/// Build the registration plan for EVERY user table in `store` so each can be
/// registered alongside `nodes`/`edges` (CONCEPT:EG-KG.query.register-user-tables-alongside). A table's schema is
/// ALWAYS resolved here (one cheap catalog lookup — never a row scan); whether its
/// ROWS are eagerly scanned too depends on `ann_indexes`:
///
/// * a table `ann_indexes` names is [`UserTable::Eager`] — pre-scanned + materialized
///   NOW, exactly as this crate's original sole behavior, because
///   [`apply_ann_pushdown`]'s durable-index top-k slice runs BEFORE `build_ctx` and
///   can only narrow an ALREADY-materialized batch in place.
/// * every other table is [`UserTable::Lazy`] — its row scan is deferred entirely to
///   [`crate::tables::provider::UserTableProvider`], which pushes a `SERIAL`-column
///   equality down to a redb point-get instead of an eager `TableStore::scan` (see
///   that type's own module doc for exactly what still falls back to a scan). Since
///   `ann_indexes` is empty for the overwhelming majority of stores/tables, this is
///   the common case: NO row scan happens here at all for those tables — not even a
///   deferred one — until (and unless) `UserTableProvider::scan` actually needs one.
fn materialize_user_tables(
    store: &TableStore,
    ann_indexes: &[AnnIndexPlan],
) -> Result<Vec<UserTable>, String> {
    let mut out = Vec::new();
    for name in store.list_tables()? {
        let schema = match store.get_schema(&name)? {
            Some(s) => s,
            None => continue,
        };
        let ann_covered = ann_indexes
            .iter()
            .any(|ix| ix.table.eq_ignore_ascii_case(&name));
        if ann_covered {
            let rows = store.scan(&name)?;
            let (arrow_schema, batch) = crate::tables::provider::materialize(&schema, &rows)?;
            out.push(UserTable::Eager(name, arrow_schema, batch));
        } else {
            out.push(UserTable::Lazy(name, schema, store.clone()));
        }
    }
    Ok(out)
}

/// A coarse, Postgres-mappable column type derived from the Arrow result schema.
/// The pgwire shim (CONCEPT:AU-KG.query.raw-python) maps each to a wire type OID; the variants
/// cover exactly the Arrow types the `nodes`/`edges` schema-on-read inference and
/// ordinary SELECT projections produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgColType {
    Text,
    Int8,
    Float8,
    Bool,
    /// A pgvector `vector` column (CONCEPT:EG-KG.query.view-pgvector-operators) — a `List<Float32>` result column.
    /// The pgwire shim maps it to a stable float-array wire OID and renders each value
    /// as the pgvector text form `[1,2,3]`.
    Vector,
}

/// One typed result column: its name plus the pg-mappable type inferred from the
/// Arrow result schema.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedColumn {
    pub name: String,
    pub ty: PgColType,
}

/// The SQL result with per-column types and rows as decoded JSON values — the
/// shape the pgwire shim needs to emit a Postgres `RowDescription` (type OIDs) +
/// `DataRow`s. Reuses the SAME DataFusion exec path as [`exec_sql`]; the only
/// difference is it surfaces the Arrow column types and hands back JSON cells
/// instead of MessagePack blobs (no wire-protocol re-encode needed downstream).
/// `Debug`/`PartialEq` (CONCEPT:EG-KG.query.served-context-cache) let a test assert two runs —
/// e.g. the cached vs. the uncached SQL context path — produced an IDENTICAL result.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedQueryResult {
    pub columns: Vec<TypedColumn>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// Run `sql` over `view` and return a [`TypedQueryResult`] (CONCEPT:AU-KG.query.raw-python).
/// Identical execution to [`exec_sql`] — same providers, UDFs, table functions,
/// off-lock snapshot, and current-thread runtime — but it captures the Arrow
/// result schema so the pgwire shim can describe columns with real type OIDs.
///
/// A fresh, never-cancelled [`CancellationToken`] — see [`exec_sql_typed_cancellable`] for a
/// caller that needs REAL cancellation (L36).
pub fn exec_sql_typed(view: &GraphView, sql: &str) -> Result<TypedQueryResult, String> {
    exec_sql_typed_cancellable(view, sql, &CancellationToken::new())
}

/// As [`exec_sql_typed`], threading `cancel` down to [`collect_streaming`] (L36).
pub fn exec_sql_typed_cancellable(
    view: &GraphView,
    sql: &str,
    cancel: &CancellationToken,
) -> Result<TypedQueryResult, String> {
    let nodes = infer_nodes(view)?;
    run_typed(
        view,
        nodes,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        sql,
        cancel,
    )
}

/// Run `sql` over `view` AND the user tables in `store` (CONCEPT:EG-KG.query.register-user-tables-alongside). Identical
/// to [`exec_sql_typed`] but every `CREATE TABLE` user table is registered as a
/// DataFusion `TableProvider` alongside `nodes`/`edges`, so a SELECT can read a user
/// table, JOIN it to the graph, or both in ONE plan. This is the read path the pgwire
/// shim calls so `psql`/ORMs see user tables and the graph in the same database.
///
/// A fresh, never-cancelled [`CancellationToken`] — see
/// [`exec_sql_typed_with_tables_cancellable`] for a caller that needs REAL cancellation
/// (L36, e.g. the served `Method::Sql` wire handler, which threads a request-scoped token a
/// client cancel / timeout can trip).
pub fn exec_sql_typed_with_tables(
    view: &GraphView,
    store: &TableStore,
    sql: &str,
) -> Result<TypedQueryResult, String> {
    exec_sql_typed_with_tables_cancellable(view, store, sql, &CancellationToken::new())
}

/// As [`exec_sql_typed_with_tables`], threading `cancel` all the way down to
/// [`collect_streaming`] (CONCEPT:EG-KG.query.streaming-spillable-collect, L36) — a caller
/// holding the OTHER end of `cancel` can stop this query mid-stream, at its next batch
/// boundary, instead of the fresh never-cancelled token every non-`_cancellable` entry point
/// above builds internally.
pub fn exec_sql_typed_with_tables_cancellable(
    view: &GraphView,
    store: &TableStore,
    sql: &str,
    cancel: &CancellationToken,
) -> Result<TypedQueryResult, String> {
    // CONCEPT:EG-KG.query.create-drop-function: the durable SQL stored functions, expanded into the query text
    // (scalar → scalar subquery; table → parameterized-view subquery) before planning.
    let functions = store.list_functions()?;
    // CONCEPT:EG-KG.query.eg-validate-procedural-body: a bare top-level `SELECT plfn(args)` / `CALL plproc(args)` naming a
    // `LANGUAGE plpgsql` function runs the procedural interpreter instead of DataFusion.
    // Its embedded SQL (expression eval, `SELECT … INTO`) runs back through THIS read path
    // — the interpreter is synchronous, so each recursive call builds its own runtime with
    // no nesting (we are not inside a reactor here; the handler calls us on `spawn_blocking`).
    // The recursive call reuses the SAME `cancel` token, so a cancelled outer request also
    // stops an in-flight plpgsql-embedded SELECT.
    if functions.iter().any(|f| f.is_plpgsql()) {
        let run_sql = |q: &str| exec_sql_typed_with_tables_cancellable(view, store, q, cancel);
        if let Some(res) = super::plpgsql::try_exec_call(sql, &functions, &run_sql)? {
            return Ok(res);
        }
    }
    let nodes = infer_nodes(view)?;
    // CONCEPT:EG-KG.query.real-ann-top-k/EG-313: the durable pgvector ANN index registrations, consulted to
    // push a matching `ORDER BY col <-> $1 LIMIT k` down to a real eg-ann index —
    // fetched BEFORE `materialize_user_tables` so it can decide, per table, whether
    // an eager pre-materialized batch is required (see that function's own doc).
    let ann_indexes = store.list_ann_indexes()?;
    let user = materialize_user_tables(store, &ann_indexes)?;
    // CONCEPT:EG-KG.query.durable-views: the durable views, registered as read-only named queries so a
    // SELECT that references a view expands its stored SELECT during context build.
    let views = store.list_views()?;
    run_typed(
        view,
        nodes,
        user,
        views,
        functions,
        ann_indexes,
        sql,
        cancel,
    )
}

/// Run `sql` over `view` reusing `cache`'s `(nodes, edges)` tables when they are
/// still valid for `version` (the GraphCore OCC `version()`, CONCEPT:EG-KG.query.version-keyed-cache).
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
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        sql,
        &CancellationToken::new(),
    )
}

// ── whole-`SessionContext` cache for the served SQL read path (CONCEPT:EG-KG.query.served-context-cache) ──
//
// `run`/`run_typed`/`run_arrow` (via `build_ctx` + `register_views` +
// `register_system_catalogs`) rebuild the ENTIRE `SessionContext` from scratch on
// EVERY call: ~20 UDF/UDAF/UDTF registrations, re-parsing + re-planning EVERY
// durable view's SQL, resynthesizing the system catalogs, and (in the callers that
// don't already share one) a fresh `tokio::runtime::Builder::new_current_thread()`
// built + torn down per call. None of that depends on the QUERY TEXT — only on the
// graph's row/topology snapshot, the SQL-domain catalog (tables/views/functions/
// indexes), and the caller's RLS visibility. [`SqlContextCache`] amortizes it: a
// repeat call sharing the same [`SqlContextEpoch`] reuses the already-built,
// already-registered context instead of redoing any of that work.
//
// This is a NEW, served-path-ONLY entry point
// ([`exec_sql_typed_with_tables_cached_cancellable`]) alongside — not replacing —
// [`exec_sql_typed_with_tables_cancellable`]; every existing caller (the embedded
// API, the write-path's internal reads, every pre-existing test) is unchanged.

/// The cache key a served SQL context is safely amortized under. Two calls with an
/// IDENTICAL epoch are guaranteed to see the SAME `nodes`/`edges`/user-table DATA
/// and the SAME view/function/index/extension/hypertable CATALOG — each field below
/// closes one specific mutation surface; see its own doc. Correctness rule: when in
/// doubt about whether something changed, the field must change too (a false MISS
/// only costs a rebuild; a false HIT would serve a stale or cross-agent result).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SqlContextEpoch {
    /// `TableStore` is one durable redb file PER (tenant, actor) (owner-scoped
    /// catalogs — see `server::sql_tables::user_table_store`) — folding tenant into
    /// the key makes a same-shaped key from two DIFFERENT tenants' stores
    /// structurally impossible to collide on, rather than merely unlikely.
    tenant: String,
    /// `GraphCore::version()` is PER-GRAPH, not global (CONCEPT:EG-KG.txn.multi-op-occ-acid) — the bare
    /// version number alone is not a unique key across two different graphs
    /// sitting at the same version, so the graph name itself is part of the key.
    graph: String,
    /// `GraphCore::version()` at snapshot time — bumps on every committed node/edge
    /// mutation (`AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge`/property writes,
    /// INCLUDING a node's `_owner`/`_visibility`/`_grants` RLS tags, since those are
    /// ordinary node properties). Closes: graph row/topology changes.
    graph_version: u64,
    /// [`TableStore::catalog_fingerprint`] — a hash over EVERY durable SQL-domain
    /// OCC counter the store holds, across every scope any served write path uses
    /// (see that method's own doc for exactly why "every scope", not one). Closes:
    /// `CREATE|DROP|ALTER TABLE`, `CREATE|DROP VIEW`, `CREATE|DROP FUNCTION`,
    /// `CREATE|DROP EXTENSION`, `CREATE INDEX` (pgvector ANN registration),
    /// `CREATE HYPERTABLE`, and `INSERT`/`UPDATE`/`DELETE` on a user table —
    /// everything `register_views`, `materialize_user_tables`, and the
    /// system-catalog synthesis read.
    catalog_fingerprint: u64,
    /// The requesting agent id `IsolationLayer::filter_view` filtered `view`
    /// against BEFORE it ever reached this module (every served caller applies RLS
    /// first — see `server::handlers::query`'s `Method::Sql` read arm).
    /// `nodes`/`edges`/the planned views/the `pagerank`/`betweenness` UDTFs are ALL
    /// built from that already-filtered snapshot, so two different callers'
    /// contexts are NEVER interchangeable even at an otherwise-identical
    /// graph/catalog epoch — omitting this field would let one agent's
    /// row-filtered `nodes` table leak to a different agent through a shared cache
    /// entry. Mirrors `server::handlers::query::rls_cache_hash`'s identical
    /// `caller`-salting discipline for the served `UnifiedQuery` result cache (the
    /// one KNOWN, already-accepted residual gap that discipline carries — and this
    /// cache inherits unchanged, not newly — is a caller's EFFECTIVE visibility
    /// changing via `RegisterIdentity`/`RbacAdmin` with NO graph or catalog write in
    /// between; there is no existing isolation-policy epoch counter to close it, and
    /// this module cannot see `IsolationLayer` at all — see the crate-level report
    /// for the full accounting).
    ///
    /// CONCEPT:EG-KG.sharding.row-level-security, P10/W1.7-C — investigated narrowing this to a coarser
    /// RLS-equivalence-CLASS (same visible-row-set ⇒ same class) so N distinct
    /// callers sharing one class would amortize ONE rebuild instead of N. Finding,
    /// worked from `IsolationLayer::can_see_row`'s own rules (`crates/eg-core/src/isolation.rs`):
    /// for an OWNED+PRIVATE row, visibility is `owner == agent_id || grants.contains(agent_id)
    /// || is_manager_of(agent_id, owner)` — checked against the EXACT agent_id
    /// string, with no coarser structural index over ownership/grants anywhere in
    /// eg-core. Two DIFFERENT non-`System` agents can therefore be PROVEN equivalent
    /// ONLY by comparing their filtered views (an O(V) walk — the exact cost this
    /// field exists to amortize) or by building NEW infrastructure (a persisted
    /// owner/grant reverse index) — out of scope here. The ONE class that IS
    /// provably safe with NO scan is `System`: `can_see_row` grants it every row
    /// unconditionally, before any owner/grant/manager check ever runs, so EVERY
    /// `System` caller's filtered view is the SAME unfiltered graph. Exploiting even
    /// that narrow win would still require TWO further changes out of this field's
    /// reach: (1) the facade's owner-scoped registry
    /// (`server::sql_tables::sql_context_cache`) instantiates a SEPARATE
    /// `SqlContextCache` per (tenant, agent_id) for an UNRELATED, stronger reason —
    /// each instance's `BuiltCtx` also embeds that agent's OWN privately-owned
    /// `TableStore`-derived user tables, which must never be shared regardless of
    /// RLS class — so two `System` callers never reach the SAME `SqlContextCache`
    /// instance to begin with; and (2) even if they did, `BuiltCtx` would need
    /// splitting into an RLS-class-shareable graph layer and a strictly per-agent
    /// catalog layer. Both are real, multi-file architectural changes — the RLS/
    /// cache-architecture refactor this field's narrowing is deferred to, not
    /// something this task ships a partial/cosmetic version of. `caller` therefore
    /// stays the raw, per-agent identity: the one key this crate can PROVE, from
    /// `IsolationLayer`'s own semantics, never lets two differently-visible callers
    /// share an entry (proof exercised by
    /// `tests/sql_context_cache_invalidation.rs`'s caller-identity + edges-specific
    /// negative tests).
    caller: String,
}

/// Bound on [`SqlContextCache`]'s resident entries — otherwise a cache holding one
/// entry per distinct (tenant, graph, caller) triple could grow without bound under
/// many-agent traffic. Overridable via `EPISTEMIC_GRAPH_SQL_CONTEXT_CACHE_SIZE`.
fn max_cached_contexts() -> usize {
    std::env::var("EPISTEMIC_GRAPH_SQL_CONTEXT_CACHE_SIZE")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(64)
}

/// `(caller, node_epoch)` — the key of the `nodes`-batch sub-cache (see
/// `SqlContextCache::node_batches`).
type NodeBatchKey = (String, u64);
/// The cached `nodes` table: its schema + the O(V) inferred Arrow batch.
type NodeBatchEntry = (SchemaRef, arrow::record_batch::RecordBatch);

/// Amortized whole-`SessionContext` cache for the served SQL read path. Mirrors
/// [`SqlCache`]'s staleness discipline (version/epoch-keyed, build OUTSIDE the
/// lock, replace inside it, never serve a mismatched key) but does NOT literally
/// compose an [`SqlCache`] instance for the `nodes`/`edges` half — see
/// [`Self::get_or_build`]'s doc for exactly why that specific reuse would be
/// unsafe here, discovered by this cache's own invalidation tests. Caches the
/// fully built + registered [`BuiltCtx`] (UDFs/UDAFs/UDTFs, planned durable views,
/// synthesized system catalogs) keyed by [`SqlContextEpoch`].
pub struct SqlContextCache {
    contexts: Mutex<HashMap<SqlContextEpoch, Arc<BuiltCtx>>>,
    /// The O(V) inferred `nodes` Arrow batch, sub-cached by (CALLER, NODE EPOCH)
    /// (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation, W1.6/P7 site 3). The batch depends
    /// ONLY on node data, so when a full-`SqlContextEpoch` miss is caused by a pure-edge or
    /// catalog-only write (the node epoch is UNCHANGED), the whole `BuiltCtx` is rebuilt but this
    /// expensive scan is REUSED instead of re-run — turning the "every write forces a full O(V+E)
    /// Arrow rebuild" cost into O(E)+catalog on a write that did not touch nodes. The key INCLUDES
    /// the caller because `infer_nodes(view)` runs over the caller's ALREADY RLS-filtered view, so
    /// the node batch is caller-specific — a shared `SqlContextCache` (one per owner file, but a
    /// single test/embedded instance may serve several callers) must NEVER hand one caller's
    /// narrower filtered node projection to another. Mirrors `SqlContextEpoch`'s own `caller` field.
    node_batches: Mutex<HashMap<NodeBatchKey, Arc<NodeBatchEntry>>>,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
    /// Reuse counters for the `nodes`-batch sub-cache (the O(V) scans SKIPPED / RUN). Observability
    /// + the site-3 regression test's proof that a pure-edge write reuses the node table.
    node_reuses: std::sync::atomic::AtomicU64,
    node_builds: std::sync::atomic::AtomicU64,
}

impl Default for SqlContextCache {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlContextCache {
    pub fn new() -> Self {
        Self {
            contexts: Mutex::new(HashMap::new()),
            node_batches: Mutex::new(HashMap::new()),
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
            node_reuses: std::sync::atomic::AtomicU64::new(0),
            node_builds: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// `(node_reuses, node_builds)` since construction (W1.6/P7 site 3): how many times the O(V)
    /// `nodes` Arrow batch was reused across a non-node write vs re-inferred. A pure-edge write
    /// following a warm SQL context should REUSE (reuses grows, builds does not).
    pub fn node_stats(&self) -> (u64, u64) {
        (
            self.node_reuses.load(std::sync::atomic::Ordering::Relaxed),
            self.node_builds.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// `(hits, misses)` since construction — observability, and the exact hook the
    /// invalidation tests below assert against to prove a call actually reused (or
    /// actually rebuilt) the cached context, not just that the RESULT happened to
    /// look right.
    pub fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(std::sync::atomic::Ordering::Relaxed),
            self.misses.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Return the built context for `epoch`, rebuilding (and caching) it when
    /// absent. The build itself — `build_ctx` + `register_views` +
    /// `register_system_catalogs`, all async (a view's SELECT is planned via
    /// `ctx.sql(...).await`) — runs OUTSIDE the lock, the SAME discipline
    /// `SqlCache::tables_at` uses: a concurrent request for a DIFFERENT epoch never
    /// blocks on this one's build. Two concurrent misses for the SAME epoch may
    /// both build (last insert wins); both callers still get a valid, correctly
    /// built context for that epoch — the identical race tolerance
    /// `SqlCache::tables_at` already accepts.
    ///
    /// Deliberately does NOT call [`SqlCache::tables_at`] here (an earlier version
    /// of this cache did, and its OWN invalidation test caught the bug):
    /// `tables_at` is keyed on `version: u64` ALONE, which is only a safe cache key
    /// when a given version always corresponds to the SAME `view` content —
    /// TRUE for `exec_sql_cached`'s callers (never caller/RLS-aware), but FALSE
    /// here, where `view` is ALREADY caller-filtered and two different callers can
    /// legitimately share a `graph_version` while their view CONTENT differs. Two
    /// different callers sharing one `SqlCache` instance at the same version would
    /// silently hand caller B caller A's (already RLS-filtered) `nodes` batch.
    /// Instead, `nodes` is inferred fresh on every genuine EPOCH miss — safe
    /// because the epoch (which DOES include `caller`) is what gates reuse, not a
    /// narrower sub-key. `edges` is never inferred here at all — `build_ctx`
    /// constructs an `EdgesTableProvider` straight from `snap` (see its own doc);
    /// nothing about that changes per-caller, since the provider still only ever
    /// walks the ALREADY-caller-filtered `view`/`snap` it is handed.
    async fn get_or_build(
        &self,
        epoch: SqlContextEpoch,
        node_epoch: u64,
        snap: Arc<GraphView>,
        view: &GraphView,
        store: &TableStore,
    ) -> Result<Arc<BuiltCtx>, String> {
        if let Some(hit) = self.contexts.lock().unwrap().get(&epoch).cloned() {
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(hit);
        }
        self.misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Site 3 (W1.6/P7): reuse the O(V) `nodes` Arrow batch when this full-epoch miss was caused
        // by a write that did NOT touch nodes (same `node_epoch`) — a pure-edge or catalog-only
        // write. `RecordBatch`/`SchemaRef` are Arc-backed, so the clone is cheap; the SKIPPED work
        // is the whole-node-store scan + Arrow column materialization.
        let node_key = (epoch.caller.clone(), node_epoch);
        let nodes = {
            let cached = self.node_batches.lock().unwrap().get(&node_key).cloned();
            match cached {
                Some(batch) => {
                    self.node_reuses
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    (*batch).clone()
                }
                None => {
                    let inferred = infer_nodes(view)?;
                    self.node_builds
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let mut guard = self.node_batches.lock().unwrap();
                    // Same crude-but-correct bound as `contexts`: clear on overflow rather than
                    // tracking recency. An extra miss only re-infers; it never serves wrong data.
                    if guard.len() >= max_cached_contexts() && !guard.contains_key(&node_key) {
                        guard.clear();
                    }
                    guard.insert(node_key, Arc::new(inferred.clone()));
                    inferred
                }
            }
        };
        // See `exec_sql_typed_with_tables_cancellable`'s identical ordering note:
        // `ann_indexes` is fetched BEFORE `materialize_user_tables` so it can pick
        // Eager vs Lazy per table.
        let ann_indexes = store.list_ann_indexes()?;
        let user = materialize_user_tables(store, &ann_indexes)?;
        let views = store.list_views()?;
        let functions = store.list_functions()?;
        let built = build_ctx(snap, nodes, user)?;
        register_views(&built.ctx, &views, &functions).await?;
        register_system_catalogs(
            &built.ctx,
            &built.nodes_schema,
            &built.edges_schema,
            &built.user_relations,
            &views,
            &functions,
        )
        .await?;
        let built = Arc::new(built);

        let mut guard = self.contexts.lock().unwrap();
        // Crude-but-correct bound: clear the WHOLE map on overflow rather than
        // tracking per-entry recency. An extra miss only costs a rebuild; a buggy
        // eviction policy could serve the wrong epoch's context — simplicity here
        // is a correctness choice, not a shortcut.
        if guard.len() >= max_cached_contexts() && !guard.contains_key(&epoch) {
            guard.clear();
        }
        guard.insert(epoch, built.clone());
        Ok(built)
    }
}

thread_local! {
    /// One lazily-built `current_thread` Tokio runtime PER blocking-pool OS thread,
    /// reused across every [`exec_sql_typed_with_tables_cached_cancellable`] call
    /// that happens to land on that thread — instead of building + tearing one down
    /// per call. Safe because: (1) the runtime holds no query-specific state, only
    /// an executor — each call's `block_on` future runs to completion before the
    /// next one starts, so there is nothing for one call to leak into the next; (2)
    /// `thread_local` gives each OS thread its OWN runtime, so there is no
    /// cross-thread sharing/contention to reason about; (3) `.block_on()` is only
    /// ever invoked from the thread that owns this cell, never nested — identical to
    /// the existing per-call `Builder::new_current_thread()...build()` idiom the
    /// REST of this file uses; this only changes WHEN the runtime is constructed,
    /// never who drives it or how.
    static SQL_EXEC_RUNTIME: std::cell::RefCell<Option<tokio::runtime::Runtime>> =
        const { std::cell::RefCell::new(None) };
}

/// Run `f` against this thread's lazily-built, reused current-thread runtime.
fn with_thread_runtime<T>(f: impl FnOnce(&tokio::runtime::Runtime) -> T) -> Result<T, String> {
    SQL_EXEC_RUNTIME.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("runtime build: {e}"))?;
            *slot = Some(rt);
        }
        Ok(f(slot.as_ref().expect("just ensured Some above")))
    })
}

/// Conservative predicate: could `sql` trigger [`apply_ann_pushdown`] against
/// `ann_indexes`? A durable ANN index's top-k pushdown narrows `nodes`/a user table
/// to a slice specific to THIS query's vector + k — that result is per-QUERY, not
/// per-epoch, so a query this returns `true` for must never be served from
/// [`SqlContextCache`]. Deliberately over-approximates (checks only the cheap,
/// side-effect-free PREFIX of `apply_ann_pushdown`'s own conditions — index
/// non-empty, a covering `ORDER BY <->/<=>/<#> ... LIMIT` shape parses, no `WHERE`):
/// a query this flags that `apply_ann_pushdown` would ultimately no-op on anyway (an
/// unresolved bind placeholder, no matching index config, or `topk_slice` declining)
/// just falls back to the slower uncached path for nothing — never a correctness
/// bug. `false` is the one case this predicate must get exactly right.
fn ann_pushdown_may_apply(sql: &str, ann_indexes: &[AnnIndexPlan]) -> bool {
    !ann_indexes.is_empty()
        && plan_ann_search(sql, ann_indexes).is_some()
        && !super::ann::sql_has_where(sql)
}

/// As [`exec_sql_typed_with_tables_cancellable`], but amortizing the WHOLE
/// `SessionContext` build across every call that shares an identical
/// [`SqlContextEpoch`] (CONCEPT:EG-KG.query.served-context-cache — the served-path context cache).
///
/// `graph_version` MUST be the version `view` was snapshotted at
/// (`GraphCore::analysis_snapshot_versioned`, taken atomically with the snapshot so
/// the two can never drift out of sync); `tenant`/`graph` identify the epoch's
/// tenant + graph; `caller` is the agent id `view` was ALREADY `filter_view`d for by
/// the caller of this function (this function does not filter anything itself — it
/// trusts `view` exactly like [`exec_sql_typed_with_tables_cancellable`] already
/// does).
///
/// Falls back to the byte-identical UNCACHED path for the two shapes a cached
/// context cannot safely serve: a bare `plpgsql` call (handled entirely before any
/// context would be built) and a query [`ann_pushdown_may_apply`] to (its top-k
/// slice is per-query, not per-epoch).
///
/// `node_epoch` (W1.6/P7 site 3) is the version of the most recent write that could have changed
/// any NODE (from `GraphCore::dep_clock().node_epoch()`, which folds in the coarse floor so a
/// follower's replicated node write is covered). It gates the O(V) `nodes`-batch sub-cache: a
/// full-epoch miss whose `node_epoch` is unchanged (a pure-edge or catalog-only write) reuses the
/// cached node table instead of re-scanning it. Pass `graph_version` for it where a finer epoch is
/// unavailable (correct, just no reuse — the node batch then keys on the same counter the whole
/// epoch does).
#[allow(clippy::too_many_arguments)]
pub fn exec_sql_typed_with_tables_cached_cancellable(
    view: &GraphView,
    graph_version: u64,
    node_epoch: u64,
    tenant: &str,
    graph: &str,
    caller: &str,
    store: &TableStore,
    cache: &SqlContextCache,
    sql: &str,
    cancel: &CancellationToken,
) -> Result<TypedQueryResult, String> {
    // plpgsql calls run through the procedural interpreter, never through a
    // SessionContext at all — identical short-circuit to the uncached entry point,
    // which this delegates to unchanged.
    let functions = store.list_functions()?;
    if functions.iter().any(|f| f.is_plpgsql()) {
        return exec_sql_typed_with_tables_cancellable(view, store, sql, cancel);
    }
    // A durable ANN index covering this exact query shape narrows `nodes`/a user
    // table to a query-specific top-k slice — per-query, never cacheable. Delegate
    // to the unchanged uncached path.
    let ann_indexes = store.list_ann_indexes()?;
    if ann_pushdown_may_apply(sql, &ann_indexes) {
        return exec_sql_typed_with_tables_cancellable(view, store, sql, cancel);
    }

    let epoch = SqlContextEpoch {
        tenant: tenant.to_string(),
        graph: graph.to_string(),
        graph_version,
        catalog_fingerprint: store.catalog_fingerprint()?,
        caller: caller.to_string(),
    };

    // Same pre-plan SQL-text transforms `run_typed` applies, in the same order —
    // strip the `pg_catalog.` function qualifier, expand stored SQL functions, then
    // desugar the pgvector operators. None of these depend on the epoch.
    let sql_text = super::catalog::strip_pg_catalog_fn_qualifier(sql);
    let sql_text = super::funcs::expand_functions(&sql_text, &functions)?;
    let sql_text = super::classify::desugar_vector_ops(&sql_text);
    let snap = Arc::new(view.clone());

    with_thread_runtime(|rt| {
        rt.block_on(async move {
            let built = cache
                .get_or_build(epoch, node_epoch, snap, view, store)
                .await?;
            let df = built
                .ctx
                .sql(&sql_text)
                .await
                .map_err(|e| format!("sql: {e}"))?;
            let batches = collect_default(df, cancel).await?;
            batches_to_typed(&batches)
        })
    })?
}

/// The materialized `SessionContext` plus the live-relation Arrow schemas the
/// system catalogs are synthesized from (CONCEPT:EG-KG.query.route-create-view-create). Returned by [`build_ctx`]
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
/// CONCEPT:EG-KG.query.route-create-view-create, extending CONCEPT:EG-KG.query.datafusion) are registered SEPARATELY by
/// [`register_system_catalogs`] AFTER the durable views are wired, so views appear as
/// relations (`relkind='v'`) with their real columns. DataFusion's NATIVE
/// `information_schema` is deliberately DISABLED here because the engine synthesizes the
/// whole schema itself (native cannot be extended with `routines`/`key_column_usage`/
/// `table_constraints`); the synthesized `information_schema.tables`/`.columns` stay in
/// sync with the same schema-on-read inference.
///
/// `nodes_schema`/`edges_schema` are the inferred Arrow schemas the catalog is
/// derived from (the catalog reports exactly the columns a SELECT returns). Both
/// `nodes` and `edges` are index-pushdown providers (CONCEPT:EG-KG.query.concept-12): `edges`'s schema is
/// static (no scan needed to know it — see `EdgesTableProvider`'s own doc), so it
/// is computed here with no row materialization at all.
fn build_ctx(
    snap: Arc<GraphView>,
    nodes: (SchemaRef, arrow::record_batch::RecordBatch),
    user_tables: Vec<UserTable>,
) -> Result<BuiltCtx, String> {
    let nodes_schema = nodes.0.clone();

    // CONCEPT:EG-KG.query.concept-12: `nodes` is a custom provider with secondary-index predicate
    // pushdown — a `WHERE col = 'x'` narrows rows via the index instead of scanning
    // every node. `edges` pushes src/dst equality down to an O(deg) adjacency walk
    // over `snap` instead of ever materializing the full edge set (its own module
    // doc has the full design) — constructed straight from the snapshot, with no
    // `(schema, batch)` argument needed at all.
    let nodes_table = NodesTableProvider::new(nodes.0, nodes.1);
    let edges_table = EdgesTableProvider::new(snap.clone());
    let edges_schema = edges_table.schema();

    // CONCEPT:EG-KG.query.route-create-view-create: DataFusion's native `information_schema` is DISABLED — the engine
    // synthesizes the whole `information_schema` (plus `pg_catalog`) itself in
    // `register_system_catalogs`, because native cannot be extended with the
    // `routines`/`key_column_usage`/`table_constraints` views psql/ORMs also probe.
    let config = SessionConfig::new().with_information_schema(false);
    let ctx = SessionContext::new_with_config(config);

    ctx.register_table("nodes", Arc::new(nodes_table))
        .map_err(|e| format!("register nodes: {e}"))?;
    ctx.register_table("edges", Arc::new(edges_table))
        .map_err(|e| format!("register edges: {e}"))?;
    // CONCEPT:EG-KG.query.register-user-tables-alongside: register each user table alongside the graph projection, and
    // remember its schema for the catalog so a reflecting driver sees it in
    // `pg_class`/`information_schema`. CONCEPT:EG-KG.query.register-each-user-table: an `Eager` table (a durable ANN
    // index covers it) goes through the SAME secondary-index pushdown provider
    // `nodes` uses; every other (`Lazy`) table goes through `UserTableProvider`,
    // which additionally pushes a `SERIAL`-column equality to a redb point-get
    // instead of an eager `TableStore::scan` (see `materialize_user_tables`'s doc
    // for the Eager/Lazy choice, and `UserTableProvider`'s own doc for exactly what
    // still falls back to a scan).
    let mut user_relations: Vec<(String, SchemaRef)> = Vec::with_capacity(user_tables.len());
    for entry in user_tables {
        match entry {
            UserTable::Eager(name, schema, batch) => {
                let table = NodesTableProvider::new(schema.clone(), batch);
                ctx.register_table(name.as_str(), Arc::new(table))
                    .map_err(|e| format!("register user table `{name}`: {e}"))?;
                user_relations.push((name, schema));
            }
            UserTable::Lazy(name, schema, store) => {
                let arrow_schema = crate::tables::provider::arrow_schema(&schema);
                let table = crate::tables::provider::UserTableProvider::new(store, schema);
                ctx.register_table(name.as_str(), Arc::new(table))
                    .map_err(|e| format!("register user table `{name}`: {e}"))?;
                user_relations.push((name, arrow_schema));
            }
        }
    }
    ctx.register_udf(json_get_udf());
    ctx.register_udf(json_get_f64_udf());
    ctx.register_udf(json_get_i64_udf());
    ctx.register_udf(epistemic_decay_udf());
    // CONCEPT:EG-KG.query.view-pgvector-operators — pgvector distance functions the `<->`/`<=>`/`<#>` operators
    // desugar to (brute-force over a vector column; the eg-ann index pushdown is EG-116).
    ctx.register_udf(vector_l2_udf());
    ctx.register_udf(vector_cosine_udf());
    ctx.register_udf(vector_ip_udf());
    // CONCEPT:EG-KG.query.continuous-aggregate-lowering — TimescaleDB `time_bucket`. CONCEPT:EG-KG.query.paradedb-bm25 — ParadeDB BM25
    // `col @@@ 'q'` / `paradedb.score()`/`snippet()` desugar targets.
    ctx.register_udf(time_bucket_udf());
    ctx.register_udf(bm25_match_udf());
    ctx.register_udf(bm25_score_udf());
    ctx.register_udf(bm25_snippet_udf());
    ctx.register_udtf("pagerank", Arc::new(PagerankFunc::new(snap.clone())));
    ctx.register_udtf("betweenness", Arc::new(BetweennessFunc::new(snap.clone())));
    // CONCEPT:EG-KG.query.greatest-least-int4range-tsrange — greatest/least, int4range/tsrange + range predicates, and the
    // generate_series table function, rounding out the Postgres common-function surface.
    register_pg_common(&ctx);
    // CONCEPT:EG-KG.query.surface-b-numeric-operators — Surface-B numeric operators over the graph's resident columns.
    #[cfg(feature = "numeric")]
    register_numeric(&ctx);
    #[cfg(feature = "finance")]
    {
        ctx.register_udaf(super::udfs::var_udaf());
        ctx.register_udaf(super::udfs::cvar_udaf());
    }
    // CONCEPT:EG-KG.query.route-create-view-create: the `pg_catalog` + `information_schema` system views are registered
    // by the caller via `register_system_catalogs` AFTER `register_views`, so views are
    // synthesized as relations with their real column schemas.
    Ok(BuiltCtx {
        ctx,
        nodes_schema,
        edges_schema,
        user_relations,
    })
}

/// Real pgvector ANN top-k pushdown (CONCEPT:EG-KG.query.real-pgvector-ann-top). When `sql` is a covered
/// `SELECT … FROM t ORDER BY col <-> $q LIMIT k` (a registered `hnsw`/`ivfflat` index
/// exists for `(t, col, metric)`), narrow the target table's materialized batch to the
/// TRUE nearest-k rows — computed by building/consulting a real [`eg_ann`] index (HNSW
/// or IVF per the index type) over the column's vectors and exact-reranking — BEFORE the
/// query is planned. The subsequent (desugared) brute-force `ORDER BY` then runs over
/// only those k rows, so projection/types/order are preserved while the O(N) scan the
/// EG-115 fallback would do is replaced by the ANN top-k.
///
/// A no-op (⇒ the caller keeps the brute-force full scan) when: no index covers the
/// query, a `WHERE` filter is present (pgvector filters THEN ranks — the pre-selection
/// would change semantics), the query vector is an unresolved bind placeholder, or the
/// target/column is not a materialized `List<Float32>` vector column with ≥ k usable rows.
///
/// Must be called on the PRE-desugar SQL (while the `<->`/`<=>`/`<#>` operators are still
/// intact for [`plan_ann_search`]).
fn apply_ann_pushdown(
    sql: &str,
    ann_indexes: &[AnnIndexPlan],
    nodes: &mut (SchemaRef, arrow::record_batch::RecordBatch),
    user_tables: &mut [UserTable],
) {
    if ann_indexes.is_empty() {
        return;
    }
    let Some(plan) = plan_ann_search(sql, ann_indexes) else {
        return;
    };
    if super::ann::sql_has_where(sql) {
        return;
    }
    let Some(qvec) = super::ann::parse_query_vector(&plan.query) else {
        return;
    };
    // The index method (hnsw/ivfflat) comes from the covering registration; the metric
    // is the query operator's metric (already matched by `plan_ann_search`).
    let Some(ix) = ann_indexes.iter().find(|ix| {
        ix.table.eq_ignore_ascii_case(&plan.table)
            && ix.column.eq_ignore_ascii_case(&plan.column)
            && ix.metric == plan.metric
    }) else {
        return;
    };
    let method = ix.method;

    if plan.table.eq_ignore_ascii_case("nodes") {
        if let Some(sliced) = super::ann::topk_slice(
            &nodes.0,
            &nodes.1,
            &plan.column,
            method,
            plan.metric,
            &qvec,
            plan.k,
        ) {
            nodes.1 = sliced;
        }
        return;
    }
    for entry in user_tables.iter_mut() {
        // A `Lazy` table is, by `materialize_user_tables`'s construction, never one
        // an ANN index covers (that's exactly the condition it uses to pick
        // `Eager`) — so `plan.table` can only ever name an `Eager` entry here. The
        // `let else` still degrades to a no-op rather than panicking if that
        // invariant were ever violated.
        let UserTable::Eager(name, schema, batch) = entry else {
            continue;
        };
        if name.eq_ignore_ascii_case(&plan.table) {
            if let Some(sliced) = super::ann::topk_slice(
                schema,
                batch,
                &plan.column,
                method,
                plan.metric,
                &qvec,
                plan.k,
            ) {
                *batch = sliced;
            }
            return;
        }
    }
}

/// Shared driver: register the two tables, the scalar/aggregate UDFs, and the
/// graph table functions, then collect the query.
// EG-313 adds `ann_indexes` (the 8th arg) beside the existing views/functions catalogs;
// L36 adds `cancel` (the 9th) so the collect leg is REALLY cancellable, not just
// spillable.
#[allow(clippy::too_many_arguments)]
fn run(
    view: &GraphView,
    nodes: (
        arrow::datatypes::SchemaRef,
        arrow::record_batch::RecordBatch,
    ),
    user_tables: Vec<UserTable>,
    views: Vec<(String, String)>,
    functions: Vec<StoredFunction>,
    ann_indexes: Vec<AnnIndexPlan>,
    sql: &str,
    cancel: &CancellationToken,
) -> Result<QueryResult, String> {
    // The graph table functions run their kernel over an owned snapshot; clone the
    // topology+ids once (cheap relative to the algorithm) so they don't borrow `view`.
    // `EdgesTableProvider` reuses this SAME snapshot (built_ctx below), so `edges`
    // needs no separate materialization here at all.
    let snap = Arc::new(view.clone());
    let mut nodes = nodes;
    let mut user_tables = user_tables;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime build: {e}"))?;

    // CONCEPT:EG-KG.query.route-create-view-create — strip the `pg_catalog.` qualifier off catalog FUNCTION calls
    // (psql `\d`/ORMs emit `pg_catalog.format_type(...)`) so the bare-name UDFs resolve;
    // schema-qualified TABLE refs (`pg_catalog.pg_class`) are untouched.
    let sql = super::catalog::strip_pg_catalog_fn_qualifier(sql);
    // CONCEPT:EG-KG.query.create-drop-function — expand SQL stored-function calls into inline SQL (scalar subquery /
    // parameterized-view subquery) BEFORE the pgvector desugar + planning, so an inlined
    // body is itself desugared and planned. A no-op when there are no functions.
    let sql = super::funcs::expand_functions(&sql, &functions)?;
    // CONCEPT:EG-KG.query.real-pgvector-ann-top — real pgvector ANN top-k pushdown on the PRE-desugar SQL (the
    // `<->`/`<=>`/`<#>` operators are still intact for the planner). Narrows the target
    // batch to the true nearest-k via a real eg-ann index when one is registered.
    apply_ann_pushdown(&sql, &ann_indexes, &mut nodes, &mut user_tables);
    // CONCEPT:EG-KG.query.view-pgvector-operators — rewrite pgvector distance operators (`<->`/`<=>`/`<#>`) to the
    // registered `vector_*` UDF calls BEFORE DataFusion plans the SQL (it has no
    // operator for them). A no-op when none are present or the SQL doesn't parse.
    let sql = super::classify::desugar_vector_ops(&sql);
    rt.block_on(async move {
        let built = build_ctx(snap, nodes, user_tables)?;
        let ctx = built.ctx;
        register_views(&ctx, &views, &functions).await?;
        // CONCEPT:EG-KG.query.route-create-view-create — synthesize `pg_catalog` + `information_schema` from the live
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
        let batches = collect_default(df, cancel).await?;
        batches_to_result(&batches)
    })
}

/// Run `sql` over `view` and return the result as real Arrow batches — no per-row
/// JSON/MessagePack decode (CONCEPT:INT-P2-2, the Arrow dataset-handle seam for
/// external heavy compute: engine snapshot/dataset handle → Arrow → external job →
/// signed result artifact → transactional writeback). Shares the EXACT same
/// providers/UDFs/table-functions/off-lock-snapshot/current-thread-runtime plumbing as
/// [`exec_sql_typed`] — the only difference is the caller gets back the
/// [`RecordBatch`](arrow::record_batch::RecordBatch)es DataFusion already produced
/// (plus their shared [`SchemaRef`]) instead of a per-cell JSON decode. This is what an
/// external heavy-compute job (data-science-mcp, a training loop) pulls typed and in
/// bulk instead of marshalling rows through Python/JSON.
///
/// A fresh, never-cancelled [`CancellationToken`] — see [`exec_sql_arrow_cancellable`]
/// for a caller that needs REAL cancellation (L36).
pub fn exec_sql_arrow(
    view: &GraphView,
    sql: &str,
) -> Result<(SchemaRef, Vec<arrow::record_batch::RecordBatch>), String> {
    exec_sql_arrow_cancellable(view, sql, &CancellationToken::new())
}

/// As [`exec_sql_arrow`], threading `cancel` down to [`collect_streaming`] (L36).
pub fn exec_sql_arrow_cancellable(
    view: &GraphView,
    sql: &str,
    cancel: &CancellationToken,
) -> Result<(SchemaRef, Vec<arrow::record_batch::RecordBatch>), String> {
    let nodes = infer_nodes(view)?;
    run_arrow(
        view,
        nodes,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        sql,
        cancel,
    )
}

/// Shared driver identical to [`run`]/[`run_typed`] up through the DataFusion collect,
/// but returns the RAW `RecordBatch`es (plus their shared schema) instead of decoding
/// them into rows — see [`exec_sql_arrow`] above.
#[allow(clippy::too_many_arguments)]
fn run_arrow(
    view: &GraphView,
    nodes: (
        arrow::datatypes::SchemaRef,
        arrow::record_batch::RecordBatch,
    ),
    user_tables: Vec<UserTable>,
    views: Vec<(String, String)>,
    functions: Vec<StoredFunction>,
    ann_indexes: Vec<AnnIndexPlan>,
    sql: &str,
    cancel: &CancellationToken,
) -> Result<(SchemaRef, Vec<arrow::record_batch::RecordBatch>), String> {
    let snap = Arc::new(view.clone());
    let mut nodes = nodes;
    let mut user_tables = user_tables;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime build: {e}"))?;

    let sql = super::catalog::strip_pg_catalog_fn_qualifier(sql);
    let sql = super::funcs::expand_functions(&sql, &functions)?;
    apply_ann_pushdown(&sql, &ann_indexes, &mut nodes, &mut user_tables);
    let sql = super::classify::desugar_vector_ops(&sql);
    rt.block_on(async move {
        let built = build_ctx(snap, nodes, user_tables)?;
        let ctx = built.ctx;
        register_views(&ctx, &views, &functions).await?;
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
        let batches = collect_default(df, cancel).await?;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
        Ok((schema, batches))
    })
}

/// Register each durable view as a DataFusion logical view (CONCEPT:EG-KG.query.durable-views): plan its
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
        // CONCEPT:EG-KG.query.create-drop-function — a view body may itself call a stored function; expand it first.
        let select_sql = super::funcs::expand_functions(select_sql, functions)
            .unwrap_or_else(|_| select_sql.clone());
        // CONCEPT:EG-KG.query.view-pgvector-operators — a view body may itself use the pgvector operators.
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
// EG-313 adds `ann_indexes` (the 8th arg) beside the existing views/functions catalogs;
// L36 adds `cancel` (the 9th) — see `run`'s doc.
#[allow(clippy::too_many_arguments)]
fn run_typed(
    view: &GraphView,
    nodes: (
        arrow::datatypes::SchemaRef,
        arrow::record_batch::RecordBatch,
    ),
    user_tables: Vec<UserTable>,
    views: Vec<(String, String)>,
    functions: Vec<StoredFunction>,
    ann_indexes: Vec<AnnIndexPlan>,
    sql: &str,
    cancel: &CancellationToken,
) -> Result<TypedQueryResult, String> {
    let snap = Arc::new(view.clone());
    let mut nodes = nodes;
    let mut user_tables = user_tables;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("runtime build: {e}"))?;

    // CONCEPT:EG-KG.query.route-create-view-create — see `run`: strip `pg_catalog.` off catalog function calls first.
    let sql = super::catalog::strip_pg_catalog_fn_qualifier(sql);
    // CONCEPT:EG-KG.query.create-drop-function — see `run`: expand SQL stored-function calls before desugar/planning.
    let sql = super::funcs::expand_functions(&sql, &functions)?;
    // CONCEPT:EG-KG.query.real-pgvector-ann-top — see `run`: real pgvector ANN top-k pushdown on the pre-desugar SQL.
    apply_ann_pushdown(&sql, &ann_indexes, &mut nodes, &mut user_tables);
    // CONCEPT:EG-KG.query.view-pgvector-operators — see `run`: desugar the pgvector operators before planning.
    let sql = super::classify::desugar_vector_ops(&sql);
    rt.block_on(async move {
        let built = build_ctx(snap, nodes, user_tables)?;
        let ctx = built.ctx;
        register_views(&ctx, &views, &functions).await?;
        // CONCEPT:EG-KG.query.route-create-view-create — synthesize `pg_catalog` + `information_schema` (see `run`).
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
        let batches = collect_default(df, cancel).await?;
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
        // CONCEPT:EG-KG.query.view-pgvector-operators — a `List<Float32>` result column is a pgvector `vector`.
        List(field) | FixedSizeList(field, _) if *field.data_type() == Float32 => PgColType::Vector,
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

/// One cell at `(col, row)` to a `serde_json::Value` (CONCEPT:EG-KG.query.concept-11).
///
/// DataFusion 54 fully executes aggregates / GROUP BY / HAVING, window functions,
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
        // ── pgvector `vector` (CONCEPT:EG-KG.query.view-pgvector-operators): a `List<Float32>` cell → JSON array of
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
        // ── analytics numeric lists (CONCEPT:EG-KG.query.svd-eg-pca-column svd / EG-335 pca / EG-344 kmeans): a
        // `List<Float64>` (singular values), a nested `List<List<Float64>>` (principal-
        // component vectors), or a `List<Int64>` (kmeans cluster labels) cell → JSON
        // array(s) of numbers, so in-engine linear-algebra/clustering results deserialize
        // STRUCTURALLY for a client rather than degrading to an Arrow text blob. Recurses
        // through `cell_to_json` so each leaf hits the `Float64`/`Int64` number arm. ──
        List(field) if matches!(field.data_type(), Float64 | Int64 | List(_)) => {
            use arrow::array::ListArray;
            let child = col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or("numeric List cell is not a ListArray")?
                .value(row);
            let items = (0..child.len())
                .map(|i| cell_to_json(child.as_ref(), i))
                .collect::<Result<Vec<_>, _>>()?;
            Value::Array(items)
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

// ── streaming / spillable / cancellable collect tests (CONCEPT:EG-KG.query.streaming-spillable-collect, EG-P1-4) ──

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures_util::StreamExt;

    fn batch(vals: &[i32]) -> arrow::record_batch::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let arr = Int32Array::from(vals.to_vec());
        arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    fn total_rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    fn flatten_i32(batches: &[arrow::record_batch::RecordBatch]) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect()
    }

    /// With a spill threshold far above the total row count, `collect_streaming`
    /// pulls every batch, in order, with no spill — the un-cancelled, un-spilled
    /// common case.
    #[tokio::test]
    async fn collect_streaming_pulls_every_batch_in_order_no_spill() {
        let batches = vec![Ok(batch(&[1, 2, 3])), Ok(batch(&[4, 5])), Ok(batch(&[6]))];
        let stream = futures_util::stream::iter(batches);
        let cancel = CancellationToken::new();
        let (out, outcome) = collect_streaming(stream, &cancel, 1_000_000).await.unwrap();
        assert_eq!(flatten_i32(&out), vec![1, 2, 3, 4, 5, 6]);
        assert!(!outcome.cancelled);
        assert!(!outcome.spilled);
        assert_eq!(outcome.rows, 6);
    }

    /// Crossing the spill threshold mid-stream triggers a real spill-to-disk
    /// round-trip, and the recovered result is LOSSLESS and ORDER-PRESERVING —
    /// identical to the no-spill case, just materialized through a temp file.
    #[tokio::test]
    async fn collect_streaming_spills_past_threshold_and_recovers_losslessly() {
        let batches = vec![
            Ok(batch(&[1, 2, 3])),
            Ok(batch(&[4, 5, 6])),
            Ok(batch(&[7, 8, 9])),
        ];
        let stream = futures_util::stream::iter(batches);
        let cancel = CancellationToken::new();
        // Threshold 4: batch 1 (3 rows) stays resident; batch 2 pushes resident to 6
        // rows (>= 4) ⇒ spills batches 1+2; batch 3 (3 more) never re-crosses 4 in
        // this run, so it stays resident and is appended after the recovered spill.
        let (out, outcome) = collect_streaming(stream, &cancel, 4).await.unwrap();
        assert!(
            outcome.spilled,
            "crossing the threshold must trigger a spill"
        );
        assert_eq!(outcome.rows, 9);
        assert_eq!(flatten_i32(&out), vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    /// A token cancelled AS A SIDE EFFECT of consuming the stream's 3rd batch is
    /// observed at the next batch boundary — `collect_streaming` stops SHORT of the
    /// full 5-batch source rather than running to completion, and the outcome
    /// reports the cancellation.
    #[tokio::test]
    async fn collect_streaming_stops_early_when_cancelled() {
        let batches = vec![
            Ok(batch(&[1])),
            Ok(batch(&[2])),
            Ok(batch(&[3])),
            Ok(batch(&[4])),
            Ok(batch(&[5])),
        ];
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        let mut seen = 0usize;
        let stream = futures_util::stream::iter(batches).map(move |b| {
            seen += 1;
            if seen == 3 {
                trigger.cancel();
            }
            b
        });
        let (out, outcome) = collect_streaming(stream, &cancel, 1_000_000).await.unwrap();
        assert!(outcome.cancelled, "outcome must report the cancellation");
        let rows = total_rows(&out);
        assert!(
            rows < 5,
            "a cancelled collect must stop before draining the whole 5-row stream: got {rows}"
        );
        assert!(rows >= 2, "batches consumed before cancellation still land");
    }

    #[test]
    fn default_spill_rows_sits_above_max_rows_so_ordinary_queries_never_spill() {
        assert!(default_spill_rows() > MAX_ROWS);
    }

    #[test]
    fn cancellation_token_clone_shares_the_flag() {
        let tok = CancellationToken::new();
        assert!(!tok.is_cancelled());
        let clone = tok.clone();
        clone.cancel();
        assert!(
            tok.is_cancelled(),
            "cancel on a clone must be observed on every other clone (shared flag)"
        );
    }
}

// ── SqlContextEpoch / ann_pushdown_may_apply unit tests (CONCEPT:EG-KG.query.served-context-cache) ──
//
// The end-to-end invalidation proofs (view add/alter/drop, epoch isolation across
// graph-version AND caller, cached-vs-uncached parity, the cross-scope-commit gap)
// live in `tests/sql_context_cache_invalidation.rs` — a crate-level integration
// test, since they need a real `TableStore` + `MutationBatch` commit path. This
// module covers the one PURE, side-effect-free predicate `SqlContextCache`'s
// caller introduces: `ann_pushdown_may_apply`.
#[cfg(test)]
mod sql_context_cache_unit_tests {
    use super::*;
    use crate::sql::pgfamily::{AnnMethod, VectorMetric};

    fn idx() -> AnnIndexPlan {
        AnnIndexPlan {
            name: None,
            table: "nodes".to_string(),
            column: "embedding".to_string(),
            method: AnnMethod::Hnsw,
            metric: VectorMetric::L2,
            if_not_exists: false,
        }
    }

    /// `false` is the one answer [`ann_pushdown_may_apply`] must get exactly right
    /// (a `true` that turns out unnecessary just costs a slower uncached fallback;
    /// a `false` that should have been `true` would let a query-specific top-k
    /// slice be cached and served to a LATER, differently-shaped query). The
    /// positive ("does flag a genuinely covering query") case is deliberately NOT
    /// asserted here: it depends on `plan_ann_search` itself recognizing the SQL
    /// shape, which is CURRENTLY BROKEN on this branch for reasons unrelated to
    /// this cache — `sql::pgfamily::tests::eg116_ann_pushdown_chooses_ann_when_index_present`
    /// (pre-existing, untouched by this change, using the identical query shape)
    /// already fails the SAME way on a clean checkout. `ann_pushdown_may_apply`
    /// only ever WEAKENS as `plan_ann_search` weakens (fewer bypasses, never more
    /// unsafe caching), so this is a known, cited, pre-existing gap this task did
    /// not introduce and is out of scope to fix — not a silently assumed pass.
    #[test]
    fn ann_pushdown_predicate_is_false_for_every_non_covering_shape() {
        let indexes = vec![idx()];
        assert!(
            !ann_pushdown_may_apply(
                "SELECT id FROM nodes ORDER BY embedding <-> '[1,2,3]' LIMIT 5",
                &[],
            ),
            "no registered ANN index at all must never flag (nothing to push down)"
        );
        assert!(
            !ann_pushdown_may_apply(
                "SELECT id FROM nodes WHERE id = 'x' ORDER BY embedding <-> '[1,2,3]' LIMIT 5",
                &indexes,
            ),
            "a WHERE clause changes pgvector semantics (filter THEN rank) -- never pushed \
             down, so never cache-bypassed either"
        );
        assert!(
            !ann_pushdown_may_apply("SELECT id FROM nodes", &indexes),
            "an ordinary query with no vector ORDER BY at all must never flag"
        );
    }
}
