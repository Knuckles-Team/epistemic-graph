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
//! ## Deadlock discipline
//!
//! `apply_delta` runs INSIDE the batch's topology write lock. It therefore reads a
//! node's content ONLY from the `ChangeSet`'s captured blob — NEVER by calling back
//! into `core` (`get_node_properties` would re-enter the held topology lock and
//! deadlock). `full_rebuild`, which DOES re-read every live node from `core`, is the
//! OUT-OF-LOCK path only (`EPISTEMIC_GRAPH_INCREMENTAL_INDEX=0` kill-switch +
//! explicit maintenance via `IndexManager::rebuild_all`) — never reached from the
//! in-lock `commit_batch` fallback, because these `apply_delta`s are infallible.

use crate::graph::GraphCore;
use crate::index::{
    ChangeSet, IndexColumns, IndexDescriptor, IndexError, IndexKind, Predicate, SecondaryIndex,
    SecondaryIndexFactory,
};

/// Decode a captured `NodeChange.properties_msgpack` blob into a JSON value map. The
/// coalescer captures the FULL property blob for an added node, and the CAS `updates`
/// map for an updated node — both MessagePack maps.
fn decode_props(blob: &[u8]) -> Option<serde_json::Map<String, serde_json::Value>> {
    match rmp_serde::from_slice::<serde_json::Value>(blob) {
        Ok(serde_json::Value::Object(m)) => Some(m),
        _ => None,
    }
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
}

#[cfg(feature = "text")]
impl GraphTextIndex {
    pub fn new(index: eg_text::TextIndex) -> Self {
        Self {
            index: std::sync::Mutex::new(index),
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
        for id in core.node_ids() {
            ix.delete(&id); // clear any stale doc for this id first
        }
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
        self.core
            .indexes()
            .with_server_index(crate::index::IndexKind::Text, |_| ())
            .is_some()
    }
}

#[cfg(feature = "text")]
impl eg_plan::TextSource for ServedTextIndex {
    fn search(&self, query: &str, k: usize) -> Vec<eg_text::TextHit> {
        self.core
            .indexes()
            .with_server_index(crate::index::IndexKind::Text, |idx| {
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
pub struct DerivedOwlIndex;

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
                // Dependency-free filesystem-safe graph name (no coupling to the
                // redb-gated `redb_store::sanitize`).
                let safe: String = name
                    .chars()
                    .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                    .collect();
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
    fn for_graph(&self, name: &str) -> Vec<Box<dyn SecondaryIndex>> {
        let mut out: Vec<Box<dyn SecondaryIndex>> = Vec::new();
        #[cfg(feature = "text")]
        if let Some(ix) = self.build_text(name) {
            out.push(ix);
        }
        #[cfg(feature = "tsdb")]
        if let Some(series) = &self.series {
            out.push(Box::new(GraphTemporalIndex::new(series.clone(), name)));
        }
        #[cfg(feature = "owl")]
        out.push(Box::new(DerivedOwlIndex));
        out
    }
}
