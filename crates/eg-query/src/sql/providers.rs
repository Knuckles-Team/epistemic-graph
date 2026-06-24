//! The `nodes` table provider (CONCEPT:KG-2.178). Schema-on-read: scan every
//! node's property MessagePack blob once, decode to `serde_json::Value` (the same
//! path `get_nodes_by_label` uses), infer an Arrow schema as the union of observed
//! keys, and materialize a single RecordBatch wrapped in a DataFusion `MemTable`.
//!
//! Type inference per key (over all nodes that carry it):
//!   bool                    -> Boolean
//!   integer                 -> Int64
//!   float (or int+float mix)-> Float64
//!   anything else / nested / heterogeneous -> Utf8 (JSON-stringified)
//!   missing on a row        -> null
//! An `id: Utf8` column (the node id) and a raw `props: Binary` escape-hatch column
//! (the original msgpack blob, for the `json_get*` UDFs) are ALWAYS emitted.

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanArray, BooleanBuilder, Float64Array, Float64Builder,
    Int64Array, Int64Builder, StringArray, StringBuilder, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use eg_core::graph::GraphView;
use petgraph::visit::{EdgeRef, IntoEdgeReferences};
use serde_json::Value;

/// Widening lattice for an inferred column type. `Null` means "seen only null /
/// not yet seen"; anything wider wins on conflict, collapsing to `Utf8` for
/// heterogeneous or nested values.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Inferred {
    Null,
    Bool,
    Int,
    Float,
    Str,
}

impl Inferred {
    fn widen(self, other: Inferred) -> Inferred {
        use Inferred::*;
        match (self, other) {
            (Null, x) | (x, Null) => x,
            (a, b) if a == b => a,
            // int + float collapse to float; everything else heterogeneous -> str.
            (Int, Float) | (Float, Int) => Float,
            _ => Str,
        }
    }

    fn from_value(v: &Value) -> Inferred {
        match v {
            Value::Null => Inferred::Null,
            Value::Bool(_) => Inferred::Bool,
            Value::Number(n) if n.is_i64() || n.is_u64() => Inferred::Int,
            Value::Number(_) => Inferred::Float,
            Value::String(_) => Inferred::Str,
            // arrays / objects -> JSON-stringified Utf8.
            _ => Inferred::Str,
        }
    }

    fn arrow_type(self) -> DataType {
        match self {
            // A column that was only ever null still needs a concrete type for
            // Arrow; Utf8 (all-null) is the least surprising.
            Inferred::Null | Inferred::Str => DataType::Utf8,
            Inferred::Bool => DataType::Boolean,
            Inferred::Int => DataType::Int64,
            Inferred::Float => DataType::Float64,
        }
    }
}

/// A decoded node: its id plus the raw blob and the decoded JSON object (or `None`
/// if the blob didn't decode to an object — it still appears as an id+props row).
struct DecodedNode<'a> {
    id: &'a str,
    raw: &'a [u8],
    obj: Option<serde_json::Map<String, Value>>,
}

/// Inferred (schema, single batch) for the `nodes` table over `view` — the
/// schema-on-read scan. Split out from MemTable construction so the result can be
/// cached and re-wrapped per query (CONCEPT:KG-2.184 version-keyed cache).
pub(crate) fn infer_nodes(view: &GraphView) -> Result<(SchemaRef, RecordBatch), String> {
    // Pass 1: decode blobs and infer the per-key type union.
    let mut decoded: Vec<DecodedNode> = Vec::with_capacity(view.node_properties.len());
    // BTreeMap keeps a stable, deterministic column order.
    let mut inferred: BTreeMap<String, Inferred> = BTreeMap::new();

    for (id, blob) in view.node_properties.iter() {
        let obj = rmp_serde::from_slice::<Value>(blob.as_slice())
            .ok()
            .and_then(|v| match v {
                Value::Object(m) => Some(m),
                _ => None,
            });
        if let Some(ref m) = obj {
            for (k, v) in m.iter() {
                let kind = Inferred::from_value(v);
                inferred
                    .entry(k.clone())
                    .and_modify(|cur| *cur = cur.widen(kind))
                    .or_insert(kind);
            }
        }
        decoded.push(DecodedNode {
            id,
            raw: blob.as_slice(),
            obj,
        });
    }

    // Schema: id (Utf8, non-null), inferred columns (all nullable), props (Binary).
    let mut fields: Vec<Field> = Vec::with_capacity(inferred.len() + 2);
    fields.push(Field::new("id", DataType::Utf8, false));
    for (name, kind) in inferred.iter() {
        fields.push(Field::new(name, kind.arrow_type(), true));
    }
    fields.push(Field::new("props", DataType::Binary, false));
    let schema: SchemaRef = Arc::new(Schema::new(fields));

    let batch = build_batch(&schema, &inferred, &decoded)?;
    Ok((schema, batch))
}

/// Materialize one RecordBatch column-by-column following `inferred`.
fn build_batch(
    schema: &SchemaRef,
    inferred: &BTreeMap<String, Inferred>,
    decoded: &[DecodedNode],
) -> Result<RecordBatch, String> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    // id column.
    let mut id_b = StringBuilder::new();
    for n in decoded {
        id_b.append_value(n.id);
    }
    columns.push(Arc::new(id_b.finish()));

    // inferred property columns.
    for (name, kind) in inferred.iter() {
        let col: ArrayRef = match kind {
            Inferred::Bool => {
                let mut b = BooleanBuilder::new();
                for n in decoded {
                    match n.obj.as_ref().and_then(|o| o.get(name)) {
                        Some(Value::Bool(x)) => b.append_value(*x),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            Inferred::Int => {
                let mut b = Int64Builder::new();
                for n in decoded {
                    match n
                        .obj
                        .as_ref()
                        .and_then(|o| o.get(name))
                        .and_then(Value::as_i64)
                    {
                        Some(x) => b.append_value(x),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            Inferred::Float => {
                let mut b = Float64Builder::new();
                for n in decoded {
                    match n
                        .obj
                        .as_ref()
                        .and_then(|o| o.get(name))
                        .and_then(Value::as_f64)
                    {
                        Some(x) => b.append_value(x),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            // Str / Null columns: JSON-stringify non-string scalars, pass strings
            // through, null for missing/json-null.
            Inferred::Str | Inferred::Null => {
                let mut b = StringBuilder::new();
                for n in decoded {
                    match n.obj.as_ref().and_then(|o| o.get(name)) {
                        None | Some(Value::Null) => b.append_null(),
                        Some(Value::String(s)) => b.append_value(s),
                        Some(other) => b.append_value(other.to_string()),
                    }
                }
                Arc::new(b.finish())
            }
        };
        columns.push(col);
    }

    // raw props escape-hatch column.
    let mut props_b = BinaryBuilder::new();
    for n in decoded {
        props_b.append_value(n.raw);
    }
    columns.push(Arc::new(props_b.finish()));

    RecordBatch::try_new(schema.clone(), columns).map_err(|e| format!("record batch: {e}"))
}

// ── edges table (CONCEPT:KG-2.184) ────────────────────────────────────────

/// The `edges` table schema: fixed columns over the petgraph topology.
///   src:  Utf8   — source node id (the petgraph source node weight)
///   dst:  Utf8   — target node id (the petgraph target node weight)
///   rel:  Utf8   — the edge weight string (relationship/type), `StableDiGraph`'s
///                  edge weight in `GraphView`
///   props: Binary — the FIRST raw msgpack edge-property blob for `(src,dst)` if
///                   one exists (the same escape hatch `nodes.props` provides),
///                   else null
/// Registered alongside `nodes`, so
///   `SELECT … FROM nodes JOIN edges ON nodes.id = edges.src`
/// joins a node row to its outgoing edges in DataFusion (a hash/merge join over the
/// two MemTables), with `json_get*` reaching into either `props` column.
fn edges_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("src", DataType::Utf8, false),
        Field::new("dst", DataType::Utf8, false),
        Field::new("rel", DataType::Utf8, false),
        Field::new("props", DataType::Binary, true),
    ]))
}

/// Inferred (schema, single batch) for the `edges` table over `view`. One row per
/// petgraph edge; the edge weight string is `rel`. `props` is the first stored
/// edge-property blob for the endpoint pair (msgpack), or null.
pub(crate) fn infer_edges(view: &GraphView) -> Result<(SchemaRef, RecordBatch), String> {
    let schema = edges_schema();

    let edge_count = view.graph.edge_count();
    let mut src_b = StringBuilder::new();
    let mut dst_b = StringBuilder::new();
    let mut rel_b = StringBuilder::new();
    let mut props_b = BinaryBuilder::new();

    for e in view.graph.edge_references() {
        let src = &view.graph[e.source()];
        let dst = &view.graph[e.target()];
        src_b.append_value(src);
        dst_b.append_value(dst);
        rel_b.append_value(e.weight());
        match view
            .edge_properties
            .get(&(src.clone(), dst.clone()))
            .and_then(|blobs| blobs.first().cloned())
        {
            Some(blob) => props_b.append_value(blob.as_slice()),
            None => props_b.append_null(),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(src_b.finish()),
        Arc::new(dst_b.finish()),
        Arc::new(rel_b.finish()),
        Arc::new(props_b.finish()),
    ];
    let _ = edge_count;
    let batch =
        RecordBatch::try_new(schema.clone(), columns).map_err(|e| format!("edges batch: {e}"))?;
    Ok((schema, batch))
}

// ── version-keyed inferred-schema cache (CONCEPT:KG-2.184) ─────────────────

/// One cached, version-stamped inference of the `nodes` and `edges` tables. The
/// schema-on-read scan is O(V) (decode every node blob) + O(E); for a read-mostly
/// graph that cost is wasted on every query because the snapshot is identical
/// between writes. Caching it keyed by the GraphCore OCC `version()` (CONCEPT:KG-2.180)
/// turns repeated queries into a cheap MemTable re-wrap of the already-built batches.
#[derive(Clone)]
pub(crate) struct CachedTables {
    pub nodes: (SchemaRef, RecordBatch),
    pub edges: (SchemaRef, RecordBatch),
}

/// A tiny version-keyed cache: holds the last `(version, CachedTables)`. A query
/// with a matching version reuses the batches; a different version (any committed
/// write bumped the counter) rebuilds and replaces the entry. Correctness rests on
/// the monotonic `version()` — it changes on every committed mutation, so a stale
/// entry can never be served after a write. Read-only, so a single `Mutex<Option<…>>`
/// is enough; contention is a brief swap, never the scan itself.
#[derive(Default)]
pub struct SqlCache {
    inner: std::sync::Mutex<Option<(u64, CachedTables)>>,
}

impl SqlCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the cached `(nodes, edges)` tables for `view` at `version`, rebuilding
    /// (and replacing the cached entry) when the stored version differs or the cache
    /// is cold. The build itself runs outside the lock so a concurrent reader of a
    /// different version doesn't block on a long scan.
    pub(crate) fn tables_at(&self, view: &GraphView, version: u64) -> Result<CachedTables, String> {
        if let Some((v, tables)) = self.inner.lock().unwrap().as_ref() {
            if *v == version {
                return Ok(tables.clone());
            }
        }
        let built = CachedTables {
            nodes: infer_nodes(view)?,
            edges: infer_edges(view)?,
        };
        *self.inner.lock().unwrap() = Some((version, built.clone()));
        Ok(built)
    }
}

// ── the pushdown registry (CONCEPT:KG-2.213) ─────────────────────────────────
//
// ONE registry the `nodes` provider consults for the two pushdown questions —
// "is this predicate pushable?" and "resolve it to row positions" — instead of
// the provider hard-coding the per-column indexability + per-value resolution
// inline. This mirrors eg-core's `IndexManager` seam (CONCEPT:KG-2.213) at the
// relational boundary: a relational predicate (`col = literal`) corresponds to
// eg-core's `Predicate::PropertyEq`; the `id` column / a `type` equality maps to
// the label/property indexes there. The registry keeps the EXACT bounded +
// demand-driven equality semantics (CONCEPT:KG-2.199) so results stay identical —
// it relocates the bespoke checks into one object, it does not change them.
//
// It works on row positions over the already-materialized Arrow batch (the SQL
// surface's data shape) rather than node ids — so it is the relational sibling of
// the eg-core `IndexManager`, not a second consumer of the same cache. Adding a
// new pushable predicate shape (a range index, a text MATCH) extends this one
// registry, not scattered provider methods.

/// Default cap on the number of distinct columns the pushdown registry will index.
/// Mirrors eg-core's property-index bound; env-overridable via
/// `EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES`.
const DEFAULT_MAX_INDEXED_PROPERTIES: usize = 32;

/// A canonicalized equality value used as a secondary-index key. We index Arrow
/// cell values by their canonical string form so a `col = literal` predicate
/// resolves to row positions regardless of the column's inferred Arrow type.
type IndexKey = String;

/// The single secondary-index registry behind the `nodes` provider's pushdown
/// (CONCEPT:KG-2.213, equality policy CONCEPT:KG-2.199). Owns the lazily-built,
/// bounded per-column equality index (`column → value → row positions`) over the
/// materialized batch, and answers:
///   * [`PushdownRegistry::indexable_eq`] — "is this predicate a pushable
///     equality?" (the registry equivalent of eg-core `IndexManager::index_for`);
///   * [`PushdownRegistry::lookup`] — resolve a `(column, value)` equality to row
///     positions, or `None` when the column can't be indexed under the bound
///     (caller full-scans).
///
/// Index policy (bounded + demand-driven, mirroring eg-core CONCEPT:KG-2.199): a
/// column is indexed on its FIRST resolved equality and cached, up to
/// `EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES` columns (default 32); columns named in
/// `EPISTEMIC_GRAPH_INDEXED_PROPERTIES` are pre-seeded on the first build.
#[derive(Debug)]
pub(crate) struct PushdownRegistry {
    schema: SchemaRef,
    batch: RecordBatch,
    /// `column name → (value → row positions)`, built lazily under the bound.
    index: RwLock<HashMap<String, HashMap<IndexKey, Vec<u32>>>>,
}

impl PushdownRegistry {
    fn new(schema: SchemaRef, batch: RecordBatch) -> Self {
        Self {
            schema,
            batch,
            index: RwLock::new(HashMap::new()),
        }
    }

    /// Columns that are equality-indexable: `id` and every scalar property column.
    /// The `props` (Binary) column is the raw-blob escape hatch — never indexed.
    fn is_indexable_column(&self, name: &str) -> bool {
        match self.schema.column_with_name(name) {
            Some((_, f)) => matches!(
                f.data_type(),
                DataType::Utf8 | DataType::Boolean | DataType::Int64 | DataType::Float64
            ),
            None => false,
        }
    }

    fn max_indexed() -> usize {
        std::env::var("EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_MAX_INDEXED_PROPERTIES)
    }

    fn seed_columns() -> Vec<String> {
        std::env::var("EPISTEMIC_GRAPH_INDEXED_PROPERTIES")
            .ok()
            .map(|s| {
                s.split(',')
                    .map(str::trim)
                    .filter(|k| !k.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Canonical string form of an Arrow cell at `(col, row)`, or `None` for null /
    /// a non-scalar column. Matches the literal canonicalization in
    /// [`scalar_to_key`] so a pushed `col = literal` lands on the right bucket.
    fn cell_key(col: &dyn Array, row: usize) -> Option<IndexKey> {
        if col.is_null(row) {
            return None;
        }
        match col.data_type() {
            DataType::Utf8 => col
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|a| a.value(row).to_string()),
            DataType::Boolean => col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(|a| a.value(row).to_string()),
            DataType::Int64 => col
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(row).to_string()),
            DataType::Float64 => col
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(|a| a.value(row).to_string()),
            _ => None,
        }
    }

    /// Build the `value → row positions` map for one column over the materialized
    /// batch. One linear pass; the result is cached.
    fn build_column_index(&self, name: &str) -> HashMap<IndexKey, Vec<u32>> {
        let mut by_value: HashMap<IndexKey, Vec<u32>> = HashMap::new();
        if let Some((idx, _)) = self.schema.column_with_name(name) {
            let col = self.batch.column(idx);
            for row in 0..self.batch.num_rows() {
                if let Some(k) = Self::cell_key(col.as_ref(), row) {
                    by_value.entry(k).or_default().push(row as u32);
                }
            }
        }
        by_value
    }

    /// Ensure `name` is indexed (honoring the cap + the env pre-seed) and return the
    /// row positions matching `value`, or `None` if the column cannot be indexed
    /// under the bound (caller falls back to a full scan for that predicate).
    fn lookup(&self, name: &str, value: &IndexKey) -> Option<Vec<u32>> {
        {
            let guard = self.index.read().unwrap();
            if let Some(by_value) = guard.get(name) {
                return Some(by_value.get(value).cloned().unwrap_or_default());
            }
        }
        let mut guard = self.index.write().unwrap();
        let cap = Self::max_indexed();
        if guard.is_empty() {
            for seed in Self::seed_columns() {
                if guard.len() >= cap {
                    break;
                }
                if !guard.contains_key(&seed) && self.is_indexable_column(&seed) {
                    let m = self.build_column_index(&seed);
                    guard.insert(seed, m);
                }
            }
        }
        if !guard.contains_key(name) {
            if guard.len() >= cap {
                return None; // cap reached — full-scan fallback for this column.
            }
            let m = self.build_column_index(name);
            guard.insert(name.to_string(), m);
        }
        Some(
            guard
                .get(name)
                .and_then(|m| m.get(value).cloned())
                .unwrap_or_default(),
        )
    }

    /// "Is this predicate a pushable equality?" — extract `(column, canonical-value)`
    /// from a `col = literal` / `literal = col` equality on an indexable column;
    /// `None` for anything else. The one place a predicate's pushability is decided.
    fn indexable_eq(&self, expr: &Expr) -> Option<(String, IndexKey)> {
        let Expr::BinaryExpr(be) = expr else {
            return None;
        };
        if be.op != Operator::Eq {
            return None;
        }
        let (col, lit) = match (be.left.as_ref(), be.right.as_ref()) {
            (Expr::Column(c), Expr::Literal(v)) => (c, v),
            (Expr::Literal(v), Expr::Column(c)) => (c, v),
            _ => return None,
        };
        if !self.is_indexable_column(&col.name) {
            return None;
        }
        scalar_to_key(lit).map(|k| (col.name.clone(), k))
    }
}

// ── nodes TableProvider over the pushdown registry (CONCEPT:KG-2.199/2.213) ──

/// `nodes` table provider whose predicate pushdown runs through ONE
/// [`PushdownRegistry`] (CONCEPT:KG-2.213) rather than bespoke per-index checks.
/// Wraps the already-materialized `(schema, batch)` that `infer_nodes` produced (so
/// the rows are byte-identical to the full-scan path); the registry holds the
/// lazily-built, bounded per-column equality index.
///
/// `supports_filters_pushdown` asks the registry whether each filter is a pushable
/// equality (`Inexact` if so, `Unsupported` otherwise); `scan` asks the registry to
/// resolve those equalities to row positions, intersects them, and returns ONLY the
/// matching rows (an `arrow::compute::take` into a fresh in-memory table) instead of
/// decoding/scanning every node. Because the predicates are reported `Inexact`,
/// DataFusion re-applies them as a Filter above the scan, so results are IDENTICAL
/// to the full-scan path — the index is a pure row-reduction optimization. With no
/// pushable filter the provider serves the full batch (the original behavior).
#[derive(Debug)]
pub(crate) struct NodesTableProvider {
    schema: SchemaRef,
    batch: RecordBatch,
    /// The single pushdown registry this provider consults (CONCEPT:KG-2.213).
    registry: PushdownRegistry,
}

impl NodesTableProvider {
    pub(crate) fn new(schema: SchemaRef, batch: RecordBatch) -> Self {
        Self {
            schema: schema.clone(),
            batch: batch.clone(),
            registry: PushdownRegistry::new(schema, batch),
        }
    }
}

/// Canonical string key for a literal `ScalarValue`, matching [`cell_key`] so a
/// pushed `col = 'x'` / `col = 5` finds the indexed bucket. `None` for null / a
/// type we don't index (the predicate then stays unsupported).
fn scalar_to_key(v: &ScalarValue) -> Option<IndexKey> {
    match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        ScalarValue::Boolean(Some(b)) => Some(b.to_string()),
        ScalarValue::Int8(Some(n)) => Some(n.to_string()),
        ScalarValue::Int16(Some(n)) => Some(n.to_string()),
        ScalarValue::Int32(Some(n)) => Some(n.to_string()),
        ScalarValue::Int64(Some(n)) => Some(n.to_string()),
        ScalarValue::UInt8(Some(n)) => Some(n.to_string()),
        ScalarValue::UInt16(Some(n)) => Some(n.to_string()),
        ScalarValue::UInt32(Some(n)) => Some(n.to_string()),
        ScalarValue::UInt64(Some(n)) => Some(n.to_string()),
        ScalarValue::Float32(Some(f)) => Some((*f as f64).to_string()),
        ScalarValue::Float64(Some(f)) => Some(f.to_string()),
        _ => None,
    }
}

#[async_trait]
impl TableProvider for NodesTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// `Inexact` for a `col = literal` equality on an indexable column, `Unsupported`
    /// otherwise. `Inexact` (NOT `Exact`) is deliberate and is what makes correctness
    /// independent of the index: DataFusion pushes the predicate into `scan` (so we
    /// narrow rows via the index) BUT also keeps the equality as a Filter node ABOVE
    /// the scan and re-applies it. So whether the index narrows to the exact set, a
    /// superset (a column that overflowed the bounded cap → full scan), or the
    /// predicate is composite, the final result is ALWAYS identical to the full-scan
    /// path — the index is a pure row-reduction optimization, never a correctness
    /// boundary. The re-filter over the already-narrowed rows is negligible.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if self.registry.indexable_eq(f).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Collect the indexable equality predicates DataFusion pushed down,
        // classified through the ONE pushdown registry (CONCEPT:KG-2.213).
        let preds: Vec<(String, IndexKey)> = filters
            .iter()
            .filter_map(|f| self.registry.indexable_eq(f))
            .collect();

        // Resolve each via the index and intersect. If ANY indexable predicate's
        // column can't be indexed under the cap, fall back to the full batch for
        // that predicate (DataFusion re-applied filters keep correctness — but we
        // reported Exact, so to stay correct we must still serve a SUPERSET; the
        // simplest correct superset is the full scan). We therefore only narrow
        // when EVERY pushed predicate resolved through the index.
        let mut matched: Option<Vec<u32>> = None;
        let mut all_indexed = true;
        for (col, val) in &preds {
            match self.registry.lookup(col, val) {
                Some(rows) => {
                    matched = Some(match matched.take() {
                        None => rows,
                        Some(prev) => intersect_sorted(&prev, &rows),
                    });
                }
                None => {
                    all_indexed = false;
                    break;
                }
            }
        }

        let batch = if preds.is_empty() || !all_indexed {
            // No pushable filter, or a column overflowed the bounded cap → serve the
            // full batch. Correct in every case because the predicates were reported
            // `Inexact`, so DataFusion re-applies them as a Filter above this scan.
            self.batch.clone()
        } else {
            let mut rows = matched.unwrap_or_default();
            rows.sort_unstable();
            let indices = UInt32Array::from(rows);
            arrow::compute::take_record_batch(&self.batch, &indices)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))?
        };

        let mem = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
        // Delegate execution (projection + limit handling) to the inner MemTable over
        // the already-narrowed rows. We pass NO filters down — the index narrowing
        // replaced the Exact predicates; any Unsupported predicate is still a Filter
        // node ABOVE this scan, so it is applied by DataFusion, not lost.
        mem.scan(state, projection, &[], limit).await
    }
}

/// Intersection of two SORTED-ASCENDING `u32` slices (row positions). The index
/// produces ascending positions, so this is a linear merge.
fn intersect_sorted(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod provider_tests {
    use super::*;
    use eg_core::graph::GraphCore;
    use serde_json::json;

    fn provider() -> NodesTableProvider {
        let core = GraphCore::new();
        for (id, team, rank) in [
            ("a", "blue", 1),
            ("b", "red", 2),
            ("c", "blue", 2),
            ("d", "blue", 3),
        ] {
            core.add_node(
                id.into(),
                rmp_serde::to_vec_named(&json!({"team": team, "rank": rank})).unwrap(),
            );
        }
        let snap = core.analysis_snapshot();
        let (schema, batch) = infer_nodes(&snap).unwrap();
        NodesTableProvider::new(schema, batch)
    }

    /// The index lookup returns exactly the row positions whose column value matches
    /// — the proof the pushdown path resolves through the index, not a scan.
    #[test]
    fn lookup_returns_matching_row_positions() {
        let p = provider();
        let reg = &p.registry;
        // `team` is an indexable Utf8 column; resolve "blue" -> the 3 blue rows.
        let rows = reg.lookup("team", &"blue".to_string()).unwrap();
        assert_eq!(rows.len(), 3, "three nodes are blue");
        // Each returned position's `team` cell must actually be "blue".
        let (idx, _) = reg.schema.column_with_name("team").unwrap();
        let col = reg.batch.column(idx);
        for r in rows {
            assert_eq!(
                PushdownRegistry::cell_key(col.as_ref(), r as usize),
                Some("blue".to_string())
            );
        }
        // A value with no rows -> empty, but Some (column IS indexed).
        assert_eq!(reg.lookup("team", &"green".to_string()), Some(Vec::new()));
    }

    /// `supports_filters_pushdown`: equality on an indexable column is `Inexact`
    /// (pushed + re-checked), everything else `Unsupported` (kept as a Filter).
    #[test]
    fn supports_filters_classifies_predicates() {
        use datafusion::logical_expr::{col, lit};
        let p = provider();
        let eq = col("team").eq(lit("blue"));
        let like = col("team").like(lit("b%"));
        let gt = col("rank").gt(lit(2_i64));
        let refs: Vec<&Expr> = vec![&eq, &like, &gt];
        let got = p.supports_filters_pushdown(&refs).unwrap();
        assert_eq!(got[0], TableProviderFilterPushDown::Inexact);
        assert_eq!(got[1], TableProviderFilterPushDown::Unsupported);
        assert_eq!(got[2], TableProviderFilterPushDown::Unsupported);
    }

    /// The bounded cap: with cap=1, a second distinct column overflows and `lookup`
    /// returns `None` (caller serves the full batch — still correct via `Inexact`).
    #[test]
    fn bounded_cap_overflows_to_none() {
        std::env::set_var("EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES", "1");
        std::env::remove_var("EPISTEMIC_GRAPH_INDEXED_PROPERTIES");
        let p = provider();
        let reg = &p.registry;
        assert!(reg.lookup("team", &"blue".to_string()).is_some());
        assert!(
            reg.lookup("rank", &"2".to_string()).is_none(),
            "cap=1 must refuse a second column"
        );
        std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES");
    }
}
