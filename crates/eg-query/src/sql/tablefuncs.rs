//! Graph-wide kernels as TABLE FUNCTIONS (CONCEPT:KG-2.184).
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

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::Expr;
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
