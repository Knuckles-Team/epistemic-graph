//! The `nodes` table provider (CONCEPT:EG-KG.query.read-only-sql-query). Schema-on-read: scan every
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

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanArray, BooleanBuilder, Float64Array, Float64Builder,
    Int64Array, Int64Builder, StringArray, StringBuilder, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::ScalarValue,
    datasource::MemTable,
    error::Result as DfResult,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
};
use eg_core::graph::GraphView;
use petgraph::visit::{EdgeRef, IntoEdgeReferences};
use petgraph::Direction;
use serde_json::Value;

use super::filter_shape::{classify_pushdown, column_eq_literal, ScanPlan};

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

/// The fixed column names `infer_nodes` always emits (`id` at the front, `props` at
/// the back) — reserved so a same-named node PROPERTY never produces a second Arrow
/// `Field` with the same name (DataFusion rejects a schema with a duplicate
/// qualified field name, e.g. `nodes.id`, on every query over the table, not just
/// ones that reference the column).
fn is_reserved_column(name: &str) -> bool {
    name == "id" || name == "props"
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
/// cached and re-wrapped per query (CONCEPT:EG-KG.query.version-keyed-cache version-keyed cache).
pub(crate) fn infer_nodes(view: &GraphView) -> Result<(SchemaRef, RecordBatch), String> {
    // Pass 1: decode blobs and infer the per-key type union.
    let mut decoded: Vec<DecodedNode> = Vec::with_capacity(view.node_properties.len());
    // BTreeMap keeps a stable, deterministic column order.
    let mut inferred: BTreeMap<String, Inferred> = BTreeMap::new();

    for (id, blob) in view.node_properties.iter() {
        let obj = eg_types::msgpack::decode_property_object(blob.as_slice()).ok();
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
    // `id` and `props` are RESERVED column names owned by the fixed columns above/
    // below this loop — if a node's own JSON properties happen to carry a key
    // literally named "id" or "props" (common: many ingested nodes stash their own
    // id as a property), skip it here rather than emitting a second `Field` with the
    // same name. DataFusion's schema validation rejects a duplicate qualified field
    // name (`nodes.id`) on ANY query over the table, even `SELECT COUNT(*)` — so an
    // unfiltered duplicate silently broke every NL/SQL query once a single node
    // anywhere in the graph carried an `id`/`props` property. The reserved fixed
    // column always wins; the duplicate property value is still reachable via the
    // `props` escape-hatch blob.
    let mut fields: Vec<Field> = Vec::with_capacity(inferred.len() + 2);
    fields.push(Field::new("id", DataType::Utf8, false));
    for (name, kind) in inferred.iter() {
        if is_reserved_column(name) {
            continue;
        }
        fields.push(Field::new(name, kind.arrow_type(), true));
    }
    fields.push(Field::new("props", DataType::Binary, false));
    let schema: SchemaRef = Arc::new(Schema::new(fields));

    let batch = build_batch(&schema, &inferred, &decoded)?;
    Ok((schema, batch))
}

/// Build one inferred property column. Looking the property up on every decoded node is
/// the same for each inferred kind; only how the JSON value is appended differs.
fn build_property_column<B: arrow::array::builder::ArrayBuilder>(
    decoded: &[DecodedNode],
    name: &str,
    mut values: B,
    mut append: impl FnMut(&mut B, Option<&Value>),
) -> ArrayRef {
    for n in decoded {
        append(&mut values, n.obj.as_ref().and_then(|o| o.get(name)));
    }
    arrow::array::builder::ArrayBuilder::finish(&mut values)
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

    // inferred property columns. Mirrors the field-list skip above: a property
    // literally named `id`/`props` does not get its own column (the reserved fixed
    // column already occupies that name), keeping the batch's column count aligned
    // 1:1 with the schema built above.
    for (name, kind) in inferred.iter() {
        if is_reserved_column(name) {
            continue;
        }
        let col: ArrayRef = match kind {
            Inferred::Bool => {
                build_property_column(decoded, name, BooleanBuilder::new(), |b, v| {
                    b.append_option(match v {
                        Some(Value::Bool(x)) => Some(*x),
                        _ => None,
                    })
                })
            }
            Inferred::Int => build_property_column(decoded, name, Int64Builder::new(), |b, v| {
                b.append_option(v.and_then(Value::as_i64))
            }),
            Inferred::Float => {
                build_property_column(decoded, name, Float64Builder::new(), |b, v| {
                    b.append_option(v.and_then(Value::as_f64))
                })
            }
            // Str / Null columns: JSON-stringify non-string scalars, pass strings
            // through, null for missing/json-null.
            Inferred::Str | Inferred::Null => {
                build_property_column(decoded, name, StringBuilder::new(), |b, v| match v {
                    None | Some(Value::Null) => b.append_null(),
                    Some(Value::String(s)) => b.append_value(s),
                    Some(other) => b.append_value(other.to_string()),
                })
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

// ── edges table (CONCEPT:EG-KG.query.version-keyed-cache) ────────────────────────────────────────

/// The `edges` table schema: fixed columns over the petgraph topology.
///   src:  Utf8   — source node id (the petgraph source node weight)
///   dst:  Utf8   — target node id (the petgraph target node weight)
///   rel:  Utf8   — the edge weight string (canonical relationship), `StableDiGraph`'s
///                  edge weight in `GraphView`
///   props: Binary — the FIRST raw msgpack edge-property blob for `(src,dst)` if
///                   one exists (the same escape hatch `nodes.props` provides),
///                   else null
/// Registered alongside `nodes`, so
///   `SELECT … FROM nodes JOIN edges ON nodes.id = edges.src`
/// joins a node row to its outgoing edges in DataFusion (a hash/merge join over the
/// two MemTables), with `json_get*` reaching into either `props` column.
pub(crate) fn edges_schema() -> SchemaRef {
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

// ── edges TableProvider: src/dst adjacency pushdown (CONCEPT:EG-KG.query.concept-12) ──
//
// Unlike `nodes` — whose SCHEMA is itself data-dependent (the union of every
// observed property key) and therefore requires an O(V) scan just to know the
// columns — `edges`'s schema is FIXED (`src`/`dst`/`rel`/`props`, see
// `edges_schema`). So unlike [`NodesTableProvider`] (which wraps an ALREADY
// fully-materialized batch — the O(V) scan already happened before the provider
// exists), [`EdgesTableProvider`] needs no batch at construction time at all: it
// holds the graph snapshot itself and defers row materialization to `scan`, where
// the pushed predicate decides how much of the graph is actually touched.
//
// A `src = 'x'` / `dst = 'x'` equality resolves via a direct O(deg(x)) adjacency
// walk over the SAME `GraphView::graph`/`node_map` petgraph topology eg-core's own
// `GraphCore::get_successors`/`get_predecessors` use (`edges_directed`) — the full
// edge set is never touched, so an edge-predicated query no longer pays the O(E)
// `infer_edges` scan at all (not just a narrower post-scan Filter over an
// already-built batch). `rel` (the third fixed column) carries no independent
// information to index on: every edge's weight is deterministically
// `"{src}:{dst}"` (`GraphTxn::add_edge`), so a `rel = 'a:b'` predicate is already
// fully implied by (and no cheaper than) a `src`+`dst` equality — there is no
// eg-core structural index over relationship strings to push it through, so it is
// left as an ordinary post-scan `Filter` (`Unsupported`), the documented
// "fall back to scan" case. `props` is the opaque per-edge blob (reached via
// `json_get*`, never equality-indexed — exactly like `nodes.props`).
//
// With NO recognized src/dst equality (an unfiltered `SELECT * FROM edges`, or a
// predicate over `rel`/`props`/anything else), the provider falls back to the
// full walk — the SAME O(E) cost `infer_edges` always paid — but memoizes it
// (once per provider instance) so a repeat unfiltered query against the SAME
// provider (e.g. `SqlContextCache`'s reused `BuiltCtx`) doesn't re-walk the graph.

mod edge_equality;
use edge_equality::{edge_column_eq, EdgeEquality};

/// `edges` table provider (CONCEPT:EG-KG.query.concept-12) — see the module section
/// doc above for the full pushdown design.
#[derive(Debug)]
pub(crate) struct EdgesTableProvider {
    schema: SchemaRef,
    view: Arc<GraphView>,
    /// The full, unfiltered batch — built at most once per provider instance, and
    /// only when a query has no src/dst equality this provider can push down.
    full: RwLock<Option<RecordBatch>>,
}

impl EdgesTableProvider {
    pub(crate) fn new(view: Arc<GraphView>) -> Self {
        Self {
            schema: edges_schema(),
            view,
            full: RwLock::new(None),
        }
    }

    /// Outgoing adjacency for `src` — O(deg(src)), via the SAME `edges_directed`
    /// primitive `GraphCore::get_successors` uses. `dst`, if ALSO pushed alongside
    /// `src` in the same `WHERE`, is checked per candidate edge (still O(deg(src))
    /// total, never a second pass over the whole graph). An empty batch (not an
    /// error) when `src` names no node — matching the full-scan path, which would
    /// also return zero rows.
    fn scan_by_src(&self, src: &str, dst: Option<&str>) -> Result<RecordBatch, String> {
        let mut rows = Vec::new();
        if let Some(&idx) = self.view.node_map.get(src) {
            for e in self.view.graph.edges_directed(idx, Direction::Outgoing) {
                let d = &self.view.graph[e.target()];
                if dst.is_some_and(|want| want != d.as_str()) {
                    continue;
                }
                rows.push((src.to_string(), d.clone(), e.weight().clone()));
            }
        }
        edges_batch_from_rows(&self.view, &rows)
    }

    /// Incoming adjacency for `dst` — O(deg(dst)), mirroring [`Self::scan_by_src`].
    /// Only reached when NO `src` equality was pushed (see `scan`, which tries
    /// `src` first).
    fn scan_by_dst(&self, dst: &str) -> Result<RecordBatch, String> {
        let mut rows = Vec::new();
        if let Some(&idx) = self.view.node_map.get(dst) {
            for e in self.view.graph.edges_directed(idx, Direction::Incoming) {
                let s = &self.view.graph[e.source()];
                rows.push((s.clone(), dst.to_string(), e.weight().clone()));
            }
        }
        edges_batch_from_rows(&self.view, &rows)
    }

    /// The full, unfiltered table — the ONLY path that pays the O(E) walk
    /// `infer_edges` always paid, and only when the query pushed no src/dst
    /// equality this provider can resolve cheaper. Memoized so a provider shared
    /// across repeat calls (`SqlContextCache`'s reused `BuiltCtx`) walks the graph
    /// at most once for every such query, not once per call.
    fn full_batch(&self) -> Result<RecordBatch, String> {
        if let Some(b) = self.full.read().unwrap().as_ref() {
            return Ok(b.clone());
        }
        let (_, batch) = infer_edges(&self.view)?;
        *self.full.write().unwrap() = Some(batch.clone());
        Ok(batch)
    }
}

#[async_trait]
impl TableProvider for EdgesTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// `Inexact` for a `src`/`dst` equality (pushed to an O(deg) adjacency walk in
    /// `scan`); everything else — including a `rel`/`props` equality, see
    /// `edge_column_eq`'s doc for exactly why `rel` is excluded — `Unsupported`
    /// (an ordinary post-scan `Filter`). `Inexact`, not `Exact`, for the identical
    /// reason `NodesTableProvider` uses it: DataFusion re-applies the equality as
    /// a Filter above the scan regardless, so correctness never depends on the
    /// walk being exhaustive.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(classify_pushdown(filters, |f| edge_column_eq(f).is_some()))
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> ScanPlan {
        let eq = EdgeEquality::from_filters(filters);

        // `src`, when pushed, is tried first (it also absorbs a co-pushed `dst`
        // as an extra per-candidate check — see `scan_by_src`); otherwise `dst`
        // alone; otherwise the full walk.
        let batch = if let Some(src) = eq.src.as_deref() {
            self.scan_by_src(src, eq.dst.as_deref())
        } else if let Some(dst) = eq.dst.as_deref() {
            self.scan_by_dst(dst)
        } else {
            self.full_batch()
        }
        .map_err(datafusion::error::DataFusionError::Execution)?;

        let mem = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
        // No filters passed down: the walk already narrowed by any recognized
        // equality; any OTHER (unsupported) predicate is still a Filter node ABOVE
        // this scan, applied by DataFusion.
        mem.scan(state, projection, &[], limit).await
    }
}

/// Build one `edges` RecordBatch from an explicit, already-selected sequence of
/// `(src, dst, rel)` triples — shared by [`EdgesTableProvider`]'s src/dst-pushed
/// traversals. Looks up each pair's `props` blob the SAME way [`infer_edges`]'s
/// full scan does (the first stored property blob for that endpoint pair, or
/// null), so a narrowed row is byte-identical to the row the full scan would have
/// produced for the same edge.
fn edges_batch_from_rows(
    view: &GraphView,
    rows: &[(String, String, String)],
) -> Result<RecordBatch, String> {
    let mut src_b = StringBuilder::new();
    let mut dst_b = StringBuilder::new();
    let mut rel_b = StringBuilder::new();
    let mut props_b = BinaryBuilder::new();

    for (src, dst, rel) in rows {
        src_b.append_value(src);
        dst_b.append_value(dst);
        rel_b.append_value(rel);
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
    RecordBatch::try_new(edges_schema(), columns).map_err(|e| format!("edges batch: {e}"))
}

// ── version-keyed inferred-schema cache (CONCEPT:EG-KG.query.version-keyed-cache) ─────────────────

/// One cached, version-stamped inference of the `nodes` table. The schema-on-read
/// scan is O(V) (decode every node blob); for a read-mostly graph that cost is
/// wasted on every query because the snapshot is identical between writes.
/// Caching it keyed by the GraphCore OCC `version()` (CONCEPT:EG-KG.txn.multi-op-occ-acid) turns
/// repeated queries into a cheap MemTable re-wrap of the already-built batch.
///
/// `edges` is deliberately NOT cached here: [`EdgesTableProvider`] defers its own
/// row materialization to `scan()` (a src/dst equality resolves via an O(deg)
/// adjacency walk, never a full table scan at all — see its own module doc), so
/// there is no eager `(SchemaRef, RecordBatch)` to cache for it in the first
/// place; [`super::exec::run`]/`run_typed`/`run_arrow` construct an
/// `EdgesTableProvider` directly from the snapshot instead of consulting this
/// cache.
#[derive(Clone)]
pub(crate) struct CachedTables {
    pub nodes: (SchemaRef, RecordBatch),
}

/// A tiny version-keyed cache: holds the last `(version, CachedTables)`. A query
/// with a matching version reuses the batch; a different version (any committed
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

    /// Return the cached `nodes` table for `view` at `version`, rebuilding (and
    /// replacing the cached entry) when the stored version differs or the cache is
    /// cold. The build itself runs outside the lock so a concurrent reader of a
    /// different version doesn't block on a long scan.
    pub(crate) fn tables_at(&self, view: &GraphView, version: u64) -> Result<CachedTables, String> {
        if let Some((v, tables)) = self.inner.lock().unwrap().as_ref() {
            if *v == version {
                return Ok(tables.clone());
            }
        }
        let built = CachedTables {
            nodes: infer_nodes(view)?,
        };
        *self.inner.lock().unwrap() = Some((version, built.clone()));
        Ok(built)
    }
}

// ── the pushdown registry (CONCEPT:AU-KG.retrieval.architecture-report) ─────────────────────────────────
//
// ONE registry the `nodes` provider consults for the two pushdown questions —
// "is this predicate pushable?" and "resolve it to row positions" — instead of
// the provider hard-coding the per-column indexability + per-value resolution
// inline. This mirrors eg-core's `IndexManager` seam (CONCEPT:AU-KG.retrieval.architecture-report) at the
// relational boundary: a relational predicate (`col = literal`) corresponds to
// eg-core's `Predicate::PropertyEq`; the `id` column / a `type` equality maps to
// the label/property indexes there. The registry keeps the EXACT bounded +
// demand-driven equality semantics (CONCEPT:EG-KG.query.concept-12) so results stay identical —
// it relocates the bespoke checks into one object, it does not change them.
//
// It works on row positions over the already-materialized Arrow batch (the SQL
// surface's data shape) rather than node ids — so it is the relational sibling of
// the eg-core `IndexManager`, not a second consumer of the same cache. Adding a
// new pushable predicate shape (a range index, a text MATCH) extends this one
// registry, not scattered provider methods.

/// A canonicalized equality value used as a secondary-index key. We index Arrow
/// cell values by their canonical string form so a `col = literal` predicate
/// resolves to row positions regardless of the column's inferred Arrow type.
type IndexKey = String;

/// The single secondary-index registry behind the `nodes` provider's pushdown
/// (CONCEPT:AU-KG.retrieval.architecture-report, equality policy CONCEPT:EG-KG.query.concept-12). Owns the lazily-built,
/// bounded per-column equality index (`column → value → row positions`) over the
/// materialized batch, and answers:
///   * [`PushdownRegistry::indexable_eq`] — "is this predicate a pushable
///     equality?" (the registry equivalent of eg-core `IndexManager::index_for`);
///   * [`PushdownRegistry::lookup`] — resolve a `(column, value)` equality to row
///     positions, or `None` when the column can't be indexed under the bound
///     (caller full-scans).
///
/// Index policy (bounded + demand-driven, mirroring eg-core CONCEPT:EG-KG.query.concept-12): a
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
        eg_core::graph::GraphCore::max_indexed_properties()
    }

    fn seed_columns() -> Vec<String> {
        eg_core::graph::GraphCore::indexed_properties_from_env()
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
        let (col, lit) = column_eq_literal(expr)?;
        if !self.is_indexable_column(&col.name) {
            return None;
        }
        scalar_to_key(lit).map(|k| (col.name.clone(), k))
    }
}

// ── nodes TableProvider over the pushdown registry (CONCEPT:EG-KG.query.concept-12/2.213) ──

/// `nodes` table provider whose predicate pushdown runs through ONE
/// [`PushdownRegistry`] (CONCEPT:AU-KG.retrieval.architecture-report) rather than bespoke per-index checks.
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
    /// The single pushdown registry this provider consults (CONCEPT:AU-KG.retrieval.architecture-report).
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
    scalar_key::text_or_bool_key(v)
        .or_else(|| scalar_key::integer_key(v))
        .or_else(|| scalar_key::float_key(v))
}

// `scalar_to_key`'s per-type-group arms — split into their own module (kiss
// file-size split, not a rename: this file keeps `scalar_to_key` itself and
// every caller).
mod scalar_key;

#[async_trait]
impl TableProvider for NodesTableProvider {
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
        Ok(classify_pushdown(filters, |f| {
            self.registry.indexable_eq(f).is_some()
        }))
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> ScanPlan {
        // Collect the indexable equality predicates DataFusion pushed down,
        // classified through the ONE pushdown registry (CONCEPT:AU-KG.retrieval.architecture-report).
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
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?
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
mod provider_tests;

/// `EdgesTableProvider` pushdown correctness (P10/W1.7-A). Every test compares an
/// `EXPLAIN`-observable claim (the pushdown narrows what DataFusion touches) or a
/// direct classification check against [`NodesTableProvider`]'s own established
/// pattern, and every query result is cross-checked against what the SAME query
/// returns from a graph with NO src/dst/rel predicate at all (the definition of
/// "the pushdown must never change the answer, only how it's computed").
#[cfg(test)]
mod edges_provider_tests;
