//! Server-layer secondary indexes wired into the per-graph [`IndexManager`] seam
//! (CONCEPT:EG-KG.storage.incremental-text / .incremental-temporal / .incremental-derived-owl).
//!
//! ## Why this module exists — the D3 remainder
//!
//! D1 opened the [`crate::index::SecondaryIndex`] / [`crate::index::IndexManager`]
//! write-maintenance seam and wired the ONE index that lives ON `GraphCore` — the
//! vector `SemanticStore` — so a committed write batch maintains it incrementally
//! (`apply_delta`) instead of drop-and-rebuild. The remaining heavy indexes —
//! **text** (`eg-text` Tantivy `TextIndex`), **temporal** (`eg-tsdb` `SeriesStore`),
//! and **derived-OWL** (`eg-rdf` reasoner) — do NOT live on `GraphCore`: they sit in
//! the server layer, ABOVE `eg-core` in the crate DAG, so `eg-core`'s
//! `GraphCore::maintain_indexes` cannot reach them.
//!
//! This module is the **server-dispatch registration hook**: each store is wrapped as
//! a [`crate::index::SecondaryIndex`] and registered into a graph's `IndexManager`
//! (via [`GraphCore::register_index`], installed per-graph by [`ServerIndexFactory`]).
//! The coalescer's committed batch then threads the [`crate::index::ChangeSet`] —
//! whose `NodeChange` carries the captured node property blob — through their
//! `apply_delta` under the SAME topology write lock as the vector store, so text/
//! temporal/derived-OWL stay coherent with the graph and never torn.
//!
//! Spatial indexes additionally carry an explicit completeness state. Recovery
//! replays graph material before the factory is installed, and paged lazy-open
//! temporarily exposes only a prefix; neither case is planner-visible until a
//! full source-graph backfill completes. Served queries use their existing
//! snapshot fallback while the index is absent or incomplete.
//!
//! ## Deadlock discipline
//!
//! `apply_delta` runs INSIDE the batch's topology write lock. It therefore reads a
//! node's content ONLY from the `ChangeSet`'s captured blob — NEVER by calling back
//! into `core` (`get_node_properties` would re-enter the held topology lock and
//! deadlock). `full_rebuild`, which DOES re-read every live node from `core`, is an
//! explicit OUT-OF-LOCK startup/recovery operation and is never reached from the
//! in-lock commit path.

use crate::graph::GraphCore;
use crate::index::{
    ChangeSet, IndexColumns, IndexDescriptor, IndexError, IndexKind, IndexManifest, Predicate,
    SecondaryIndex, SecondaryIndexFactory,
};

/// Decode a captured `NodeChange.properties_msgpack` blob into a JSON value map. The
/// coalescer captures the FULL property blob for an added node, and the CAS `updates`
/// map for an updated node — both MessagePack maps.
fn decode_props(blob: &[u8]) -> Option<serde_json::Map<String, serde_json::Value>> {
    eg_types::msgpack::decode_property_object(blob).ok()
}

// ── text (CONCEPT:EG-KG.storage.incremental-text) ────────────────────────────────────

/// The conventional node fields a text index derives its document body from, in
/// precedence order (first present wins). Mirrors what a `:Doc` node carries.
#[cfg(feature = "text")]
const TEXT_KEYS: &[&str] = &[
    "text",
    "content",
    "body",
    "description",
    "summary",
    "title",
    "name",
];

/// Extract a node's text body from its property map — `Some` iff the node carries a
/// text field (so an UPDATE that touched no text field leaves the prior entry intact).
#[cfg(feature = "text")]
fn extract_text(props: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    for k in TEXT_KEYS {
        if let Some(serde_json::Value::String(s)) = props.get(*k) {
            return Some(s.clone());
        }
    }
    None
}

/// Server-layer eg-text Tantivy BM25 index over one graph's node text, driven
/// incrementally by the committed write batch (CONCEPT:EG-KG.storage.incremental-text).
#[cfg(feature = "text")]
pub struct GraphTextIndex {
    index: std::sync::Mutex<eg_text::TextIndex>,
    manifest: std::sync::Mutex<IndexManifest>,
}

#[cfg(feature = "text")]
impl GraphTextIndex {
    pub fn new(index: eg_text::TextIndex) -> Self {
        Self {
            index: std::sync::Mutex::new(index),
            manifest: std::sync::Mutex::new(IndexManifest::default()),
        }
    }

    /// BM25 top-`k` search over the graph's node text — the read surface a hybrid
    /// planner (lexical `Rank`) consumes. Reflects the last committed batch.
    pub fn search(&self, query: &str, k: usize) -> Vec<eg_text::TextHit> {
        self.index
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .search(query, k)
    }
}

#[cfg(feature = "text")]
impl SecondaryIndex for GraphTextIndex {
    fn kind(&self) -> IndexKind {
        IndexKind::Text
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn descriptor(&self) -> IndexDescriptor {
        IndexDescriptor {
            kind: IndexKind::Text,
            columns: IndexColumns::NonColumnar,
            serves_lookup: false,
        }
    }
    fn covers(&self, _p: &Predicate) -> bool {
        false
    }
    fn lookup(&self, _core: &GraphCore, _p: &Predicate) -> Option<Vec<String>> {
        None
    }
    fn needs_content(&self) -> bool {
        true
    }
    fn manifest(&self) -> IndexManifest {
        *self.manifest.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn publish_manifest(&self, manifest: IndexManifest) {
        *self.manifest.lock().unwrap_or_else(|e| e.into_inner()) = manifest;
    }
    fn maintains_manifest(&self) -> bool {
        true
    }

    /// Incremental BM25 maintenance for a committed batch. Reads content from the
    /// captured blobs only (never `core`). Infallible: a commit error is logged and
    /// swallowed (the index self-heals on the next commit / `full_rebuild`), so the
    /// in-lock `commit_batch` never falls back to the core-reading `full_rebuild`.
    fn apply_delta(&self, _core: &GraphCore, change: &ChangeSet) -> Result<(), IndexError> {
        let mut ix = self.index.lock().unwrap_or_else(|e| e.into_inner());
        for id in &change.removed_nodes {
            ix.delete(id);
        }
        // An ADD carries the full blob ⇒ authoritative body (possibly empty). An
        // UPDATE (CAS) carries only the changed fields ⇒ re-index ONLY when it touched
        // a text field; otherwise leave the prior doc (its text is unchanged).
        for nc in &change.added_nodes {
            let body = nc
                .properties_msgpack
                .as_deref()
                .and_then(decode_props)
                .and_then(|m| extract_text(&m))
                .unwrap_or_default();
            ix.upsert(&nc.id, &body);
        }
        for nc in &change.updated_nodes {
            if let Some(body) = nc
                .properties_msgpack
                .as_deref()
                .and_then(decode_props)
                .and_then(|m| extract_text(&m))
            {
                ix.upsert(&nc.id, &body);
            }
        }
        if let Err(e) = ix.commit() {
            tracing::warn!("GraphTextIndex commit failed (will self-heal): {e}");
        }
        Ok(())
    }

    /// OUT-OF-LOCK rebuild: re-index every live node from `core` (safe here — no
    /// topology lock is held on the kill-switch / maintenance path). Drops the whole
    /// index first so it reflects exactly the live set.
    fn full_rebuild(&self, core: &GraphCore) -> Result<(), IndexError> {
        let mut ix = self.index.lock().unwrap_or_else(|e| e.into_inner());
        ix.clear()
            .map_err(|e| IndexError::Failed(format!("text rebuild reset: {e}")))?;
        for id in core.node_ids() {
            let body = core
                .get_node_properties(&id)
                .as_deref()
                .and_then(decode_props)
                .and_then(|m| extract_text(&m))
                .unwrap_or_default();
            ix.upsert(&id, &body);
        }
        ix.commit()
            .map_err(|e| IndexError::Failed(format!("text rebuild commit: {e}")))
    }
}

/// Planner pushdown adapter (CONCEPT:EG-KG.query.served-text-index-binding, EG-P1-4): lets a served
/// `RankText`/`FuseRrf` leg search the MAINTAINED per-graph [`GraphTextIndex`] directly
/// instead of `eg-plan`'s prior behavior of building a throwaway BM25 index from the
/// queried snapshot on EVERY request. Cheap to construct per request — it only holds an
/// `Arc<GraphCore>` clone — and each [`eg_plan::TextSource::search`] call re-resolves the
/// registered index through the generic [`crate::index::IndexManager`] (a downcast via
/// [`crate::index::SecondaryIndex::as_any`]) and searches it fresh under a brief internal
/// lock, so it always reflects the LAST COMMITTED batch with no per-query rebuild. `eg-core`
/// cannot expose this directly (the concrete `GraphTextIndex` type lives ABOVE it in the
/// crate DAG — see this module's top docs), so the adapter lives here, at the server layer,
/// where both `eg-core`'s generic registry AND the concrete text index are in scope.
#[cfg(feature = "text")]
pub struct ServedTextIndex {
    core: std::sync::Arc<GraphCore>,
}

#[cfg(feature = "text")]
impl ServedTextIndex {
    pub fn new(core: std::sync::Arc<GraphCore>) -> Self {
        Self { core }
    }

    /// `true` iff this graph has a maintained persistent text index registered — the
    /// caller (the query handler) uses this to choose between pushing a `RankText`/
    /// `FuseRrf` leg down into THIS adapter or falling back to a snapshot-derived index
    /// (a graph created before the factory installed, or a test harness with no
    /// `ServerIndexFactory` wired at all).
    pub fn available(&self) -> bool {
        let source_snapshot_version = self.core.version();
        let nodes = self.core.node_count() as u64;
        let edges = self.core.edge_count() as u64;
        self.core
            .indexes()
            .with_server_index(crate::index::IndexKind::Text, |index| {
                index
                    .manifest()
                    .covers_source(source_snapshot_version, nodes, edges)
            })
            .unwrap_or(false)
    }
}

#[cfg(feature = "text")]
impl eg_plan::TextSource for ServedTextIndex {
    fn search(&self, query: &str, k: usize) -> Vec<eg_text::TextHit> {
        let source_snapshot_version = self.core.version();
        let nodes = self.core.node_count() as u64;
        let edges = self.core.edge_count() as u64;
        self.core
            .indexes()
            .with_server_index(crate::index::IndexKind::Text, |idx| {
                if !idx
                    .manifest()
                    .covers_source(source_snapshot_version, nodes, edges)
                {
                    return Vec::new();
                }
                idx.as_any()
                    .downcast_ref::<GraphTextIndex>()
                    .map(|gti| gti.search(query, k))
                    .unwrap_or_default()
            })
            .unwrap_or_default()
    }
}

// ── temporal (CONCEPT:EG-KG.storage.incremental-temporal) ────────────────────────────

/// 1-hour buckets (ns) for a node's temporal series — a storage-layout constant only;
/// range reads are bucket-agnostic.
#[cfg(feature = "tsdb")]
const TEMPORAL_BUCKET_NS: u64 = 3_600_000_000_000;

/// The per-graph series id for a node's temporal series (NUL-separated so it can't
/// collide across graphs/nodes).
#[cfg(feature = "tsdb")]
fn temporal_series_id(graph: &str, node_id: &str) -> String {
    format!("{graph}\u{0}{node_id}")
}

/// Extract a node's temporal series from its property map — a `measurements` array of
/// `{ts:int, value:number}` objects. `Some` iff the node carries the field (so a CAS
/// that didn't touch it leaves the prior series intact).
#[cfg(feature = "tsdb")]
fn extract_measurements(
    props: &serde_json::Map<String, serde_json::Value>,
) -> Option<Vec<eg_tsdb::point::Point>> {
    let arr = props.get("measurements")?.as_array()?;
    let mut pts = Vec::with_capacity(arr.len());
    for m in arr {
        let ts = m.get("ts").and_then(|v| v.as_i64())?;
        let value = m.get("value").and_then(|v| v.as_f64())?;
        pts.push(eg_tsdb::point::Point::single(ts, value));
    }
    // Deterministic order (append handles out-of-order, but keep rebuild == incremental).
    pts.sort_by_key(|p| p.ts);
    Some(pts)
}

/// Server-layer eg-tsdb temporal index over one graph's node measurement series,
/// driven incrementally by the committed write batch (CONCEPT:EG-KG.storage.incremental-temporal).
/// A node's series is idempotently REPLACED on add/update (delete-then-append) and
/// dropped on removal, so the store never diverges from a full rebuild.
#[cfg(feature = "tsdb")]
pub struct GraphTemporalIndex {
    store: std::sync::Arc<eg_tsdb::store::SeriesStore>,
    graph: String,
    manifest: std::sync::Mutex<IndexManifest>,
}

#[cfg(feature = "tsdb")]
impl GraphTemporalIndex {
    pub fn new(
        store: std::sync::Arc<eg_tsdb::store::SeriesStore>,
        graph: impl Into<String>,
    ) -> Self {
        Self {
            store,
            graph: graph.into(),
            manifest: std::sync::Mutex::new(IndexManifest::default()),
        }
    }

    /// Idempotent replace of a node's series: drop the old one, then append the new
    /// points (empty ⇒ just the drop). Keeps incremental == rebuild.
    fn replace(&self, node_id: &str, points: &[eg_tsdb::point::Point]) {
        let sid = temporal_series_id(&self.graph, node_id);
        if let Err(e) = self.store.delete_series(&sid) {
            tracing::warn!("GraphTemporalIndex delete_series({sid}) failed: {e}");
            return;
        }
        if !points.is_empty() {
            let fields = [String::from("value")];
            if let Err(e) = self
                .store
                .append_batch(&sid, 1, TEMPORAL_BUCKET_NS, &fields, points)
            {
                tracing::warn!("GraphTemporalIndex append_batch({sid}) failed: {e}");
            }
        }
    }
}

#[cfg(feature = "tsdb")]
impl SecondaryIndex for GraphTemporalIndex {
    fn kind(&self) -> IndexKind {
        IndexKind::Temporal
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn descriptor(&self) -> IndexDescriptor {
        IndexDescriptor {
            kind: IndexKind::Temporal,
            columns: IndexColumns::NonColumnar,
            serves_lookup: false,
        }
    }
    fn covers(&self, _p: &Predicate) -> bool {
        false
    }
    fn lookup(&self, _core: &GraphCore, _p: &Predicate) -> Option<Vec<String>> {
        None
    }
    fn needs_content(&self) -> bool {
        true
    }
    fn manifest(&self) -> IndexManifest {
        *self.manifest.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn publish_manifest(&self, manifest: IndexManifest) {
        *self.manifest.lock().unwrap_or_else(|e| e.into_inner()) = manifest;
    }
    fn maintains_manifest(&self) -> bool {
        true
    }

    fn apply_delta(&self, _core: &GraphCore, change: &ChangeSet) -> Result<(), IndexError> {
        for id in &change.removed_nodes {
            let sid = temporal_series_id(&self.graph, id);
            if let Err(e) = self.store.delete_series(&sid) {
                tracing::warn!("GraphTemporalIndex delete_series({sid}) failed: {e}");
            }
        }
        for nc in &change.added_nodes {
            if let Some(pts) = nc
                .properties_msgpack
                .as_deref()
                .and_then(decode_props)
                .and_then(|m| extract_measurements(&m))
            {
                self.replace(&nc.id, &pts);
            }
        }
        for nc in &change.updated_nodes {
            if let Some(pts) = nc
                .properties_msgpack
                .as_deref()
                .and_then(decode_props)
                .and_then(|m| extract_measurements(&m))
            {
                self.replace(&nc.id, &pts);
            }
        }
        Ok(())
    }

    /// OUT-OF-LOCK rebuild from live nodes: drop every node's series, then re-append
    /// from current content. Safe (no topology lock held on this path).
    fn full_rebuild(&self, core: &GraphCore) -> Result<(), IndexError> {
        for id in core.node_ids() {
            let pts = core
                .get_node_properties(&id)
                .as_deref()
                .and_then(decode_props)
                .and_then(|m| extract_measurements(&m))
                .unwrap_or_default();
            self.replace(&id, &pts);
        }
        Ok(())
    }
}

// ── derived-OWL (CONCEPT:EG-KG.storage.incremental-derived-owl) ───────────────────────

/// Server-layer derived-OWL index (CONCEPT:EG-KG.storage.incremental-derived-owl).
///
/// The eg-rdf OWL reasoner ([`eg_rdf::owl::Reasoner`], CONCEPT:EG-KG.ontology.incremental-materialization)
/// is ALREADY a differential materializer: `Reasoner::add_axioms(delta)` resumes from
/// the prior `S`/`R` closure and derives ONLY the new entailments, and it is invoked
/// ON-DEMAND at query time — not rebuilt on each topology write. So the correct wiring
/// here is a **no-op** `apply_delta`: re-running materialization on the write batch
/// would duplicate the reasoner's own incremental pass. The index is registered so it
/// PARTICIPATES in the seam (discoverable via the manager, counted in the batch tally),
/// keeping derived-OWL a first-class member of the one registry, while the actual
/// differential materialization stays owned by the reasoner. The incremental ≡ rebuild
/// equivalence of THAT materializer is proven directly against `Reasoner` in the tests.
#[cfg(feature = "owl")]
pub struct DerivedOwlIndex {
    manifest: std::sync::Mutex<IndexManifest>,
}

#[cfg(feature = "owl")]
impl Default for DerivedOwlIndex {
    fn default() -> Self {
        Self {
            manifest: std::sync::Mutex::new(IndexManifest::default()),
        }
    }
}

#[cfg(feature = "owl")]
impl SecondaryIndex for DerivedOwlIndex {
    fn kind(&self) -> IndexKind {
        IndexKind::DerivedOwl
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn descriptor(&self) -> IndexDescriptor {
        IndexDescriptor {
            kind: IndexKind::DerivedOwl,
            columns: IndexColumns::NonColumnar,
            serves_lookup: false,
        }
    }
    fn covers(&self, _p: &Predicate) -> bool {
        false
    }
    fn lookup(&self, _core: &GraphCore, _p: &Predicate) -> Option<Vec<String>> {
        None
    }
    // No content needed: the reasoner reads the graph's triples/axioms itself on-demand.
    fn needs_content(&self) -> bool {
        false
    }
    fn manifest(&self) -> IndexManifest {
        *self.manifest.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn publish_manifest(&self, manifest: IndexManifest) {
        *self.manifest.lock().unwrap_or_else(|e| e.into_inner()) = manifest;
    }
    fn maintains_manifest(&self) -> bool {
        true
    }
    /// No-op: the reasoner already differentially materialized this delta (see the
    /// struct docs). Returns Ok so the batch never falls back to a rebuild.
    fn apply_delta(&self, _core: &GraphCore, _change: &ChangeSet) -> Result<(), IndexError> {
        Ok(())
    }
    /// No-op: the on-demand reasoner rebuilds its own closure when next invoked.
    fn full_rebuild(&self, _core: &GraphCore) -> Result<(), IndexError> {
        Ok(())
    }
}

// ── spatial (CONCEPT:EG-KG.storage.incremental-spatial, L37) ─────────────────────────

/// The canonical spatial-geometry property key a maintained index derives its bboxes
/// from. Deliberately narrower than eg-plan's ephemeral fallback (`geometry_from_value`,
/// which also accepts the `geom` alias) — the SAME "fixed key list vs the fallback's
/// wider one" asymmetry [`GraphTextIndex`]'s `TEXT_KEYS` has relative to the
/// snapshot-derived text fallback, and for the identical reason: it makes the served
/// pushdown differentially provable (see `served_spatial_scan_pushes_down_into_persistent_index_not_snapshot_fallback`
/// in `tests/served_query_completeness.rs`) — a node whose geometry lives under `geom`
/// matches the ephemeral fallback but is invisible to this maintained index.
#[cfg(feature = "geo")]
const SPATIAL_GEOMETRY_KEY: &str = "geometry";

/// Decode `props`'s `type` (the spatial LAYER, matching eg-plan's `spatial_scan`
/// convention) and its `geometry` WKT into a `(layer, bbox)` pair — `None` when either is
/// absent/unparsable/has no bbox (e.g. an empty `GeometryCollection`).
#[cfg(feature = "geo")]
fn extract_layer(props: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    props.get("type")?.as_str().map(str::to_string)
}

#[cfg(feature = "geo")]
fn extract_bbox(props: &serde_json::Map<String, serde_json::Value>) -> Option<eg_geo::Bbox> {
    let wkt = props.get(SPATIAL_GEOMETRY_KEY)?.as_str()?;
    eg_geo::parse_wkt(wkt).ok()?.bbox()
}

/// One layer's cached packed Hilbert R-tree over `items` — invalidated (removed) by
/// [`SpatialState`] whenever a write touches ANY member of that layer, rebuilt lazily on
/// the NEXT query. `ids[i]` is the original item id backing the tree's positional index
/// `i` (mirrors `eg_geo::RTree::build`'s own `(usize, Bbox)` id convention).
#[cfg(feature = "geo")]
struct LayerTree {
    ids: Vec<String>,
    tree: eg_geo::RTree,
}

/// The maintained item set + per-layer cached trees backing [`GraphSpatialIndex`]
/// (CONCEPT:EG-KG.storage.incremental-spatial). `items` (`id -> (layer, bbox)`) is the SINGLE
/// source of truth, updated incrementally off the committed batch; `built` caches each
/// layer's packed Hilbert R-tree, invalidated (removed) whenever a write touches that
/// layer's item set and rebuilt lazily on the layer's NEXT query — so `query` amortizes
/// the (immutable, batch-built) R-tree's construction cost across every read between two
/// writes, instead of eg-plan's `spatial_scan` v1 fallback, which rebuilds it on EVERY
/// scan.
#[cfg(feature = "geo")]
#[derive(Default)]
struct SpatialState {
    items: std::collections::HashMap<String, (String, eg_geo::Bbox)>,
    built: std::collections::HashMap<String, LayerTree>,
    /// `true` only after a full source-graph backfill completed. A freshly
    /// registered index starts incomplete so recovery/paged lazy-open can never
    /// advertise its initially empty or partial item set to the planner.
    manifest: IndexManifest,
    /// Incremented by every node delta. A full rebuild samples this before and
    /// after reading the graph and retries if a concurrent maintained write
    /// landed, preventing the replacement from overwriting that delta.
    maintenance_revision: u64,
}

#[cfg(feature = "geo")]
impl SpatialState {
    /// Record/replace `id`'s `(layer, bbox)`. Invalidates the cached tree for the OLD
    /// layer too (if `id` moved layers — an unusual but handled case), so neither the
    /// old nor the new layer's next query sees a stale tree.
    fn upsert(&mut self, id: &str, layer: String, bbox: eg_geo::Bbox) {
        if let Some((old_layer, _)) = self.items.get(id) {
            if *old_layer != layer {
                self.built.remove(old_layer);
            }
        }
        self.built.remove(&layer);
        self.items.insert(id.to_string(), (layer, bbox));
    }

    fn remove(&mut self, id: &str) {
        if let Some((layer, _)) = self.items.remove(id) {
            self.built.remove(&layer);
        }
    }

    /// (Re)build `layer`'s tree if its cache is dirty (absent), then query it. The
    /// rebuild filters `items` down to `layer`'s members — O(live nodes) — same as a
    /// `full_rebuild`, but paid at most ONCE per write batch that touched the layer,
    /// never per query.
    fn query(&mut self, layer: &str, bbox: [f64; 4]) -> Vec<String> {
        if !self.built.contains_key(layer) {
            let mut ids: Vec<String> = Vec::new();
            let mut boxes: Vec<(usize, eg_geo::Bbox)> = Vec::new();
            for (id, (l, b)) in self.items.iter() {
                if l == layer {
                    boxes.push((ids.len(), *b));
                    ids.push(id.clone());
                }
            }
            let tree = eg_geo::RTree::build(&boxes);
            self.built
                .insert(layer.to_string(), LayerTree { ids, tree });
        }
        let lt = self
            .built
            .get(layer)
            .expect("just built or already cached above");
        lt.tree
            .query_bbox(&eg_geo::Bbox::from_array(bbox))
            .into_iter()
            .map(|i| lt.ids[i].clone())
            .collect()
    }
}

/// Server-layer maintained spatial index over one graph's geometry-bearing nodes
/// (CONCEPT:EG-KG.storage.incremental-spatial, L37) — the persistent counterpart to
/// eg-plan's `spatial_scan` v1 ephemeral per-query R-tree build. Keyed by `layer` (the
/// node's `type` property, exactly as `Op::SpatialScan`); each layer's item set is
/// maintained incrementally off the committed write batch, and its packed Hilbert
/// R-tree (immutable once built — `eg_geo::RTree` has no incremental insert/remove) is
/// rebuilt lazily on the layer's next query after a write invalidated it, not on every
/// query — see [`SpatialState`]'s docs.
#[cfg(feature = "geo")]
pub struct GraphSpatialIndex {
    state: std::sync::Mutex<SpatialState>,
}

#[cfg(feature = "geo")]
impl Default for GraphSpatialIndex {
    fn default() -> Self {
        Self {
            state: std::sync::Mutex::new(SpatialState::default()),
        }
    }
}

#[cfg(feature = "geo")]
impl GraphSpatialIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// bbox query over `layer`'s maintained item set — the read surface a served
    /// `Op::SpatialScan` pushdown (`ServedSpatialIndex`) consumes. Reflects the last
    /// committed batch.
    pub fn query_bbox(&self, layer: &str, bbox: [f64; 4]) -> Vec<String> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .query(layer, bbox)
    }

    /// Whether the maintained item set covers the graph material from which it
    /// was registered. Planner pushdown is legal only in this state; otherwise
    /// the served path keeps its snapshot-derived fallback.
    pub fn is_complete(&self, source_snapshot_version: u64, nodes: u64, edges: u64) -> bool {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .manifest
            .covers_source(source_snapshot_version, nodes, edges)
    }
}

#[cfg(feature = "geo")]
impl SecondaryIndex for GraphSpatialIndex {
    fn kind(&self) -> IndexKind {
        IndexKind::Spatial
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn descriptor(&self) -> IndexDescriptor {
        IndexDescriptor {
            kind: IndexKind::Spatial,
            columns: IndexColumns::NonColumnar,
            serves_lookup: false,
        }
    }
    fn covers(&self, _p: &Predicate) -> bool {
        false
    }
    fn lookup(&self, _core: &GraphCore, _p: &Predicate) -> Option<Vec<String>> {
        None
    }
    fn needs_content(&self) -> bool {
        true
    }
    fn manifest(&self) -> IndexManifest {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .manifest
    }
    fn publish_manifest(&self, manifest: IndexManifest) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .manifest = manifest;
    }
    fn maintains_manifest(&self) -> bool {
        true
    }

    /// Incremental maintenance for a committed batch. Reads content from the captured
    /// blobs only (never `core`). An ADD carries the FULL blob, so a node with no
    /// `type`/`geometry` simply has neither key (no-op — nothing to index). An UPDATE
    /// (CAS) carries only the touched fields: when it carries a NEW geometry but not
    /// `type` (the common "move this object" write), the node's LAST KNOWN layer (from
    /// `items`) is reused so the entry stays correctly bucketed without requiring every
    /// geometry-only update to also restate `type`. An update that touches NEITHER key
    /// is a no-op, leaving the prior entry (if any) untouched — mirroring the text
    /// index's "no text field touched" no-op.
    fn apply_delta(&self, _core: &GraphCore, change: &ChangeSet) -> Result<(), IndexError> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        for id in &change.removed_nodes {
            st.remove(id);
        }
        for nc in change.added_nodes.iter().chain(change.updated_nodes.iter()) {
            let Some(props) = nc.properties_msgpack.as_deref().and_then(decode_props) else {
                continue;
            };
            let Some(bbox) = extract_bbox(&props) else {
                continue;
            };
            let layer =
                extract_layer(&props).or_else(|| st.items.get(&nc.id).map(|(l, _)| l.clone()));
            if let Some(layer) = layer {
                st.upsert(&nc.id, layer, bbox);
            }
        }
        if !change.added_nodes.is_empty()
            || !change.updated_nodes.is_empty()
            || !change.removed_nodes.is_empty()
        {
            st.maintenance_revision = st.maintenance_revision.wrapping_add(1);
        }
        Ok(())
    }

    /// OUT-OF-LOCK rebuild: re-derive every live node's `(layer, bbox)` from `core`,
    /// dropping any node that no longer carries both. Safe here — no topology lock held
    /// on this path.
    fn full_rebuild(&self, core: &GraphCore) -> Result<(), IndexError> {
        const MAX_STABLE_REBUILD_ATTEMPTS: usize = 8;

        for _ in 0..MAX_STABLE_REBUILD_ATTEMPTS {
            let revision = self
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .maintenance_revision;
            let mut rebuilt = SpatialState::default();
            for id in core.node_ids() {
                let Some(props) = core
                    .get_node_properties(&id)
                    .as_deref()
                    .and_then(decode_props)
                else {
                    continue;
                };
                if let (Some(layer), Some(bbox)) = (extract_layer(&props), extract_bbox(&props)) {
                    rebuilt.upsert(&id, layer, bbox);
                }
            }

            let mut current = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if current.maintenance_revision != revision {
                continue;
            }
            rebuilt.maintenance_revision = revision;
            *current = rebuilt;
            return Ok(());
        }

        Err(IndexError::Failed(
            "spatial index source changed throughout backfill".to_string(),
        ))
    }
}

/// Planner pushdown adapter (CONCEPT:EG-KG.storage.incremental-spatial, L37): lets a served
/// `Op::SpatialScan` search the MAINTAINED per-graph [`GraphSpatialIndex`] directly
/// instead of eg-plan's `spatial_scan`'s prior behavior of building a throwaway packed
/// Hilbert R-tree from the queried snapshot on EVERY request. Cheap to construct per
/// request — it only holds an `Arc<GraphCore>` clone — and each
/// [`eg_plan::SpatialSource::query_bbox`] call re-resolves the registered index through
/// the generic [`crate::index::IndexManager`] (a downcast via
/// [`crate::index::SecondaryIndex::as_any`]) and queries it fresh under a brief internal
/// lock, so it always reflects the LAST COMMITTED batch with no per-query rebuild.
/// Mirrors [`ServedTextIndex`] exactly.
#[cfg(feature = "geo")]
pub struct ServedSpatialIndex {
    core: std::sync::Arc<GraphCore>,
}

#[cfg(feature = "geo")]
impl ServedSpatialIndex {
    pub fn new(core: std::sync::Arc<GraphCore>) -> Self {
        Self { core }
    }

    /// `true` iff this graph has a maintained persistent spatial index registered
    /// AND fully backfilled from its source graph. The caller uses this to choose
    /// between pushdown and the snapshot-derived fallback; merely registering an
    /// empty/incomplete recovery or paged-lazy-open index is never sufficient.
    pub fn available(&self) -> bool {
        let source_snapshot_version = self.core.version();
        let nodes = self.core.node_count() as u64;
        let edges = self.core.edge_count() as u64;
        self.core
            .indexes()
            .with_server_index(crate::index::IndexKind::Spatial, |idx| {
                idx.as_any()
                    .downcast_ref::<GraphSpatialIndex>()
                    .is_some_and(|index| index.is_complete(source_snapshot_version, nodes, edges))
            })
            .unwrap_or(false)
    }
}

#[cfg(feature = "geo")]
impl eg_plan::SpatialSource for ServedSpatialIndex {
    fn query_bbox(&self, layer: &str, bbox: [f64; 4]) -> Vec<String> {
        let source_snapshot_version = self.core.version();
        let nodes = self.core.node_count() as u64;
        let edges = self.core.edge_count() as u64;
        self.core
            .indexes()
            .with_server_index(crate::index::IndexKind::Spatial, |idx| {
                idx.as_any()
                    .downcast_ref::<GraphSpatialIndex>()
                    .filter(|gsi| gsi.is_complete(source_snapshot_version, nodes, edges))
                    .map(|gsi| gsi.query_bbox(layer, bbox))
                    .unwrap_or_default()
            })
            .unwrap_or_default()
    }
}

/// Map a graph/tenant name to a bounded, INJECTIVE, filesystem-safe directory
/// name for the per-graph text index root. Every byte outside `[A-Za-z0-9._-]`
/// — including `~` itself — is escaped as `~XX` (lowercase hex), so distinct
/// inputs can never collide: the old scheme collapsed EVERY such byte to a
/// single `_`, so `"agent:planner"`, `"agent-planner"`, `"agent.planner"`, and
/// `"agent_planner"` (four distinct tenants) all sanitized to the identical
/// `"agent_planner"` directory — a multi-tenant text-index collision in every
/// deployment that sets `GRAPH_SERVICE_PERSIST_DIR` (mandatory in served mode).
/// An escaped name over 200 bytes falls back to a fixed-width SHA-256 hash of
/// the raw name to stay filesystem-safe. Mirrors `crate::redb_store::sanitize`'s
/// collision-free escape scheme exactly, duplicated here (rather than reused)
/// because that module lives behind the `redb` feature and this one must not —
/// see `build_text`'s doc.
#[cfg(feature = "text")]
fn sanitize_text_dir_name(name: &str) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;

    let mut key = String::with_capacity(name.len());
    for &byte in name.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            key.push(char::from(byte));
        } else {
            write!(&mut key, "~{byte:02x}").expect("write to String cannot fail");
        }
    }
    if key.len() <= 200 {
        return key;
    }
    let digest = Sha256::digest(name.as_bytes());
    let mut bounded = String::with_capacity(66);
    bounded.push_str("~h");
    for byte in digest {
        write!(&mut bounded, "{byte:02x}").expect("write to String cannot fail");
    }
    bounded
}

// ── the factory (registered per-graph by the registry) ───────────────────────────

/// Builds the enabled server-layer secondary indexes for each graph and hands them to
/// the [`crate::registry::GraphRegistry`] (CONCEPT:EG-KG.storage.incremental-text /
/// .incremental-temporal / .incremental-derived-owl). Installed once at startup via
/// `registry.set_secondary_index_factory(...)`; the registry then registers the
/// per-graph indexes onto every existing graph and every future `create_graph`.
#[derive(Default)]
pub struct ServerIndexFactory {
    /// The shared `SeriesStore` the `Ts*` handlers use — graph-node series live beside
    /// explicit measurements. `None` ⇒ no temporal index wired.
    #[cfg(feature = "tsdb")]
    series: Option<std::sync::Arc<eg_tsdb::store::SeriesStore>>,
    /// When set, roots each graph's Tantivy index at `<dir>/<sanitized-graph>`; `None`
    /// ⇒ in-memory per-graph indexes.
    #[cfg(feature = "text")]
    text_persist_dir: Option<std::path::PathBuf>,
}

impl ServerIndexFactory {
    /// An empty factory — refine it with the cfg-gated builder methods below, then
    /// `into_arc()` and hand to `registry.set_secondary_index_factory`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach the shared time-series store (temporal index).
    #[cfg(feature = "tsdb")]
    pub fn with_series(mut self, series: std::sync::Arc<eg_tsdb::store::SeriesStore>) -> Self {
        self.series = Some(series);
        self
    }

    /// Root the per-graph text indexes at `dir` (in-memory when `None`).
    #[cfg(feature = "text")]
    pub fn with_text_dir(mut self, dir: Option<std::path::PathBuf>) -> Self {
        self.text_persist_dir = dir;
        self
    }

    /// Wrap into the trait object the registry stores.
    pub fn into_arc(self) -> std::sync::Arc<dyn SecondaryIndexFactory> {
        std::sync::Arc::new(self)
    }

    #[cfg(feature = "text")]
    fn build_text(&self, name: &str) -> Option<Box<dyn SecondaryIndex>> {
        let ix = match &self.text_persist_dir {
            Some(root) => {
                // Injective, filesystem-safe graph/tenant name — dependency-free (no
                // coupling to the redb-gated `redb_store::sanitize`), but the SAME
                // collision-free escape scheme: every byte outside
                // `[A-Za-z0-9._-]` is `~`-hex-escaped rather than collapsed, so two
                // distinct tenant names can never sanitize to the same directory
                // (see `sanitize_text_dir_name`'s doc for why the old scheme could).
                let safe = sanitize_text_dir_name(name);
                let dir = root.join(safe);
                if let Err(e) = std::fs::create_dir_all(&dir) {
                    tracing::warn!(
                        "text index dir {} unavailable ({e}); skipping",
                        dir.display()
                    );
                    return None;
                }
                match eg_text::TextIndex::open(&dir) {
                    Ok(ix) => ix,
                    Err(e) => {
                        tracing::warn!(
                            "open text index at {} failed ({e}); skipping",
                            dir.display()
                        );
                        return None;
                    }
                }
            }
            None => match eg_text::TextIndex::in_memory() {
                Ok(ix) => ix,
                Err(e) => {
                    tracing::warn!("in-memory text index for {name} failed ({e}); skipping");
                    return None;
                }
            },
        };
        Some(Box::new(GraphTextIndex::new(ix)))
    }
}

impl SecondaryIndexFactory for ServerIndexFactory {
    fn for_graph(&self, _name: &str) -> Vec<Box<dyn SecondaryIndex>> {
        let mut out: Vec<Box<dyn SecondaryIndex>> = Vec::new();
        #[cfg(feature = "text")]
        if let Some(ix) = self.build_text(_name) {
            out.push(ix);
        }
        #[cfg(feature = "tsdb")]
        if let Some(series) = &self.series {
            out.push(Box::new(GraphTemporalIndex::new(series.clone(), _name)));
        }
        #[cfg(feature = "owl")]
        out.push(Box::new(DerivedOwlIndex::default()));
        #[cfg(feature = "geo")]
        out.push(Box::new(GraphSpatialIndex::new()));
        out
    }
}

#[cfg(all(test, feature = "text"))]
mod batch_update_tests {
    use super::*;

    fn text_hits(core: &GraphCore, query: &str) -> Vec<String> {
        core.indexes()
            .with_server_index(crate::index::IndexKind::Text, |index| {
                index
                    .as_any()
                    .downcast_ref::<GraphTextIndex>()
                    .expect("graph text index")
                    .search(query, 10)
                    .into_iter()
                    .map(|hit| hit.id)
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn public_batch_keeps_text_manifest_complete_and_tombstones_removed_docs() {
        let core = std::sync::Arc::new(GraphCore::new());
        core.add_node(
            "doc".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({"text": "legacy phrase"})).unwrap(),
        );
        core.register_index(Box::new(GraphTextIndex::new(
            eg_text::TextIndex::in_memory().unwrap(),
        )));
        core.indexes().rebuild_server_indexes(&core);
        assert_eq!(text_hits(&core, "legacy"), vec!["doc"]);

        let update = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "upsert_node", "id": "doc", "properties": {"text": "current phrase"}},
            {"op": "add_embedding", "id": "doc", "embedding": [1.0, 0.0]}
        ]))
        .unwrap();
        crate::algorithms::batch_update(&core, &update).unwrap();
        core.mark_dirty();
        assert!(ServedTextIndex::new(core.clone()).available());
        assert!(text_hits(&core, "legacy").is_empty());
        assert_eq!(text_hits(&core, "current"), vec!["doc"]);

        let remove = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "remove_node", "id": "doc"}
        ]))
        .unwrap();
        crate::algorithms::batch_update(&core, &remove).unwrap();
        core.mark_dirty();
        assert!(text_hits(&core, "current").is_empty());
        assert_eq!(core.semantic_store.read().get_embedding("doc"), None);
        assert!(core
            .indexes()
            .server_manifests()
            .iter()
            .all(|(_, manifest)| {
                manifest.covers_source(
                    core.version(),
                    core.node_count() as u64,
                    core.edge_count() as u64,
                )
            }));
    }
}

#[cfg(all(test, feature = "text"))]
mod sanitize_tenant_dir_tests {
    use super::*;

    /// The exact four-tenant collision the OLD `map(|c| ...).collect()` scheme
    /// produced (every non-alphanumeric byte collapsed to the same `_`):
    /// `"agent:planner"`, `"agent-planner"`, `"agent.planner"`, and
    /// `"agent_planner"` all sanitized to the identical `"agent_planner"`
    /// string, so two distinct tenants' Tantivy text indexes silently shared
    /// one on-disk directory. The injective scheme must map all four to
    /// pairwise-DISTINCT names.
    #[test]
    fn distinct_tenant_names_no_longer_collide() {
        let names = [
            "agent:planner",
            "agent-planner",
            "agent.planner",
            "agent_planner",
        ];
        let sanitized: Vec<String> = names.iter().map(|n| sanitize_text_dir_name(n)).collect();

        let mut unique = sanitized.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(
            unique.len(),
            names.len(),
            "expected {} pairwise-distinct sanitized names, got {:?}",
            names.len(),
            sanitized
        );

        // Confirm this is exactly the collision the old scheme produced — i.e.
        // the fix actually changed the outcome, not just that the new scheme is
        // injective in the abstract.
        let old_scheme = |n: &str| -> String {
            n.chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                .collect()
        };
        let old_sanitized: Vec<String> = names.iter().map(|n| old_scheme(n)).collect();
        let mut old_unique = old_sanitized.clone();
        old_unique.sort();
        old_unique.dedup();
        assert_eq!(
            old_unique.len(),
            1,
            "expected the OLD scheme to collide all four names (regression baseline), got {:?}",
            old_sanitized
        );
    }

    /// End-to-end proof at the level `build_text` actually operates: two
    /// previously-colliding tenant names must root their text indexes at two
    /// different, independently-created directories on disk.
    #[test]
    fn build_text_creates_distinct_directories_for_colliding_tenant_names() {
        let tmp = tempfile::tempdir().unwrap();
        let factory = ServerIndexFactory::new().with_text_dir(Some(tmp.path().to_path_buf()));

        assert!(factory.build_text("agent:planner").is_some());
        assert!(factory.build_text("agent-planner").is_some());
        assert!(factory.build_text("agent.planner").is_some());
        assert!(factory.build_text("agent_planner").is_some());

        let dirs: Vec<std::path::PathBuf> = [
            "agent:planner",
            "agent-planner",
            "agent.planner",
            "agent_planner",
        ]
        .iter()
        .map(|n| tmp.path().join(sanitize_text_dir_name(n)))
        .collect();
        let mut unique = dirs.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(
            unique.len(),
            dirs.len(),
            "expected 4 distinct on-disk directories, got {:?}",
            dirs
        );
        for dir in &dirs {
            assert!(dir.is_dir(), "{} was not created", dir.display());
        }
    }

    #[test]
    fn long_name_falls_back_to_bounded_hash() {
        let long = "x".repeat(5000);
        let sanitized = sanitize_text_dir_name(&long);
        assert!(
            sanitized.len() < 100,
            "expected a bounded hash fallback, got length {}",
            sanitized.len()
        );
        assert!(sanitized.starts_with("~h"));
        // Two different long names must still map to different hashes.
        let other = "y".repeat(5000);
        assert_ne!(
            sanitize_text_dir_name(&long),
            sanitize_text_dir_name(&other)
        );
    }
}
