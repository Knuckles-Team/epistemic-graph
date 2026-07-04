//! Graph-wide kernels as TABLE FUNCTIONS (CONCEPT:EG-KG.query.version-keyed-cache).
//!
//! A scalar UDF sees one row at a time and cannot run a whole-graph algorithm, so
//! `pagerank()` and `betweenness()` are exposed as zero-arg DataFusion **table
//! functions** (`SessionContext::register_udtf`, DataFusion 43's
//! [`TableFunctionImpl`]). Each call runs the eg_compute algorithm over the query's
//! GraphView snapshot ONCE and returns the `(id: Utf8, score: Float64)` rows as a
//! `MemTable`, so a query can JOIN the scores against `nodes`:
//!
//! ```sql
//! SELECT n.id, p.score
//! FROM nodes n JOIN pagerank() p ON n.id = p.id
//! ORDER BY p.score DESC
//! ```
//!
//! The snapshot is captured when the UDTF is registered (it borrows nothing from
//! the live graph), so the scores are consistent with the `nodes`/`edges` tables
//! built from the same snapshot.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;
use eg_core::graph::GraphView;

/// PageRank damping / iteration defaults — match the engine's graph-op handler
/// defaults so the table function and the `PageRank` method agree.
const PAGERANK_DAMPING: f64 = 0.85;
const PAGERANK_ITERATIONS: usize = 100;

/// Shared `(id, score)` schema for both kernels.
fn score_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]))
}

/// Materialize `(id, score)` pairs as a single-batch `MemTable`.
fn scores_table(rows: Vec<(String, f64)>) -> DfResult<Arc<dyn TableProvider>> {
    let schema = score_schema();
    let ids: StringArray = rows.iter().map(|(id, _)| Some(id.as_str())).collect();
    let scores: Float64Array = rows.iter().map(|(_, s)| Some(*s)).collect();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(scores)])
        .map_err(|e| DataFusionError::Execution(format!("scores batch: {e}")))?;
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|e| DataFusionError::Execution(format!("scores mem table: {e}")))?;
    Ok(Arc::new(table))
}

/// `pagerank()` table function over an owned snapshot.
#[derive(Debug)]
pub(crate) struct PagerankFunc {
    view: Arc<GraphView>,
}

impl PagerankFunc {
    pub(crate) fn new(view: Arc<GraphView>) -> Self {
        Self { view }
    }
}

impl TableFunctionImpl for PagerankFunc {
    fn call(&self, _args: &[Expr]) -> DfResult<Arc<dyn TableProvider>> {
        let rows =
            eg_compute::algorithms::pagerank(&self.view, PAGERANK_DAMPING, PAGERANK_ITERATIONS);
        scores_table(rows)
    }
}

/// `betweenness()` table function over an owned snapshot.
#[derive(Debug)]
pub(crate) struct BetweennessFunc {
    view: Arc<GraphView>,
}

impl BetweennessFunc {
    pub(crate) fn new(view: Arc<GraphView>) -> Self {
        Self { view }
    }
}

impl TableFunctionImpl for BetweennessFunc {
    fn call(&self, _args: &[Expr]) -> DfResult<Arc<dyn TableProvider>> {
        let rows = eg_compute::algorithms::betweenness_centrality(&self.view);
        scores_table(rows)
    }
}

// ── generate_series (CONCEPT:EG-KG.query.greatest-least-int4range-tsrange) ────────────────────────────────────────

/// Cap on the number of rows a single `generate_series` call may materialize, so a
/// runaway `generate_series(1, 10^12)` can't OOM the one-shot result buffer.
const GENERATE_SERIES_MAX_ROWS: usize = 10_000_000;

/// `generate_series(start, stop[, step]) -> TABLE(value int8)` (CONCEPT:EG-KG.query.greatest-least-int4range-tsrange) — the
/// Postgres set-returning function ORMs/BI tools emit for numeric series and calendar
/// spines. DataFusion 43's `default-features = false` build registers no `generate_series`
/// table function, so EG-104 provides it: an inclusive integer series from `start` to
/// `stop` stepping by `step` (default `1`; negative counts down). Arguments must be
/// integer literals (the ordinary `SELECT * FROM generate_series(1, 5)` shape). Column is
/// named `value`.
#[derive(Debug)]
pub(crate) struct GenerateSeriesFunc;

/// Pull an i64 out of a literal `Expr` argument (Int64/Int32/UInt64/Float64), else error.
fn literal_i64(e: &Expr, ctx: &str) -> DfResult<i64> {
    if let Expr::Literal(sv) = e {
        match sv {
            ScalarValue::Int64(Some(v)) => return Ok(*v),
            ScalarValue::Int32(Some(v)) => return Ok(*v as i64),
            ScalarValue::Int16(Some(v)) => return Ok(*v as i64),
            ScalarValue::Int8(Some(v)) => return Ok(*v as i64),
            ScalarValue::UInt64(Some(v)) => return Ok(*v as i64),
            ScalarValue::UInt32(Some(v)) => return Ok(*v as i64),
            ScalarValue::Float64(Some(v)) => return Ok(*v as i64),
            _ => {}
        }
    }
    Err(DataFusionError::Execution(format!(
        "generate_series: {ctx} must be an integer literal, got `{e}`"
    )))
}

impl TableFunctionImpl for GenerateSeriesFunc {
    fn call(&self, args: &[Expr]) -> DfResult<Arc<dyn TableProvider>> {
        if args.len() != 2 && args.len() != 3 {
            return Err(DataFusionError::Execution(
                "generate_series expects (start, stop[, step])".into(),
            ));
        }
        let start = literal_i64(&args[0], "start")?;
        let stop = literal_i64(&args[1], "stop")?;
        let step = match args.get(2) {
            Some(e) => literal_i64(e, "step")?,
            None => 1,
        };
        if step == 0 {
            return Err(DataFusionError::Execution(
                "generate_series: step must be non-zero".into(),
            ));
        }
        let mut values: Vec<i64> = Vec::new();
        let mut cur = start;
        // Inclusive of `stop`, ascending or descending by the sign of `step`.
        while (step > 0 && cur <= stop) || (step < 0 && cur >= stop) {
            values.push(cur);
            if values.len() >= GENERATE_SERIES_MAX_ROWS {
                return Err(DataFusionError::Execution(format!(
                    "generate_series: series exceeds {GENERATE_SERIES_MAX_ROWS} rows"
                )));
            }
            match cur.checked_add(step) {
                Some(next) => cur = next,
                None => break,
            }
        }
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let col: Int64Array = values.into_iter().map(Some).collect();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(col)])
            .map_err(|e| DataFusionError::Execution(format!("generate_series batch: {e}")))?;
        let table = MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| DataFusionError::Execution(format!("generate_series table: {e}")))?;
        Ok(Arc::new(table))
    }
}
