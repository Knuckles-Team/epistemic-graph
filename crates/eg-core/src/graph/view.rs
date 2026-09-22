use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::EdgeRef;

use super::decode_property_value;
use super::GraphSchemaSources;

/// An owned, consistent, UNLOCKED read view of a graph (topology + properties),
/// produced by `GraphCore::*_snapshot`. The read-only graph algorithms operate on
/// a `GraphView` (never on the live, locked `GraphCore`), so a long O(V·E)
/// computation runs entirely off the graph's locks. (Phase C-B)
#[derive(Default)]
pub struct GraphView {
    pub graph: StableDiGraph<String, String>,
    pub node_map: HashMap<String, NodeIndex>,
    pub node_properties: HashMap<String, Arc<Vec<u8>>>,
    /// Snapshot mirror of `GraphCore::edge_properties` — see that field's doc for
    /// the read/write model: multiple entries per pair are deliberate (distinct
    /// typed edges and/or bitemporal history), and any reader selecting among them
    /// must do so explicitly (by `relationship` and/or bitemporal liveness), never
    /// by Vec position.
    pub edge_properties: HashMap<(String, String), Vec<Arc<Vec<u8>>>>,
    /// Point-in-time graph schema authority. Reasoning and validation consume
    /// this exact catalog rather than consulting mutable process-global state.
    pub schema_sources: Arc<GraphSchemaSources>,
    /// Interior-mutable, type-erased memo of expensive per-snapshot DERIVED stats
    /// (CONCEPT:EG-KG.query.column-range-stats) — e.g. the planner's per-column
    /// min/max + histogram catalog (`eg_plan`'s `ColumnStats`). A `GraphView` is an
    /// IMMUTABLE point-in-time snapshot (produced by `*_snapshot` at one OCC
    /// `version()`); its `node_properties` never change after construction, so
    /// anything derived purely from them can be computed ONCE and reused for the life
    /// of this view without ever going stale. A committed write produces a brand-NEW
    /// snapshot — a new `GraphView` whose memo starts empty (see the `Clone`/`Default`
    /// impls) — so fresh data is always recomputed and a stale hit is structurally
    /// impossible. Typed as `dyn Any` so `eg-core` stays free of the higher-tier
    /// `eg-plan` types that populate it; the producing crate downcasts. NOT filled
    /// here — populated lazily on first access via `get_or_init`.
    pub plan_stats_memo: OnceLock<Arc<dyn Any + Send + Sync>>,
    /// Interior-mutable memo of a caller-defined `label → node ids` index over
    /// this snapshot's `node_properties` (CONCEPT:EG-KG.query.column-range-stats sibling of
    /// `plan_stats_memo` above — same per-view `OnceLock` memoization shape, minus
    /// the type erasure since every populator shares one `HashMap<String,
    /// Vec<String>>` value type). A `GraphView` never changes after construction,
    /// so once built the memo is valid for the rest of this view's life — a
    /// labeled scan pays the O(N) decode pass at most ONCE per snapshot no matter
    /// how many label lookups the query makes, instead of once per lookup.
    /// `eg-core` does not decide what a "label" is (a node's `type`/`node_type`/
    /// `label` fields serve `GraphCore.label_index`'s broader write-path
    /// contract, while Cypher's own `(var:Label)` predicate is deliberately
    /// narrower — see `eg-query`'s `node_has_label`); [`Self::label_index`] takes
    /// the indexing rule as a builder closure from its caller instead, exactly
    /// how `plan_stats_memo` is filled by the producing crate rather than by
    /// eg-core itself. `None` until first use.
    pub label_index_memo: OnceLock<HashMap<String, Vec<String>>>,
    /// Interior-mutable, type-erased memo of per-column APPROXIMATE DISTINCT-VALUE sketches
    /// (CONCEPT:EG-KG.query.approx-distinct-cardinality, W4.5/N5) — `eg-plan`'s `DistinctStats`
    /// (an `eg_compute::sketch::HyperLogLog` per top-level property key). Same shape and same
    /// staleness-impossible reasoning as [`Self::plan_stats_memo`] (a THIRD, separate slot rather
    /// than folded into that one: `plan_stats_memo` already has established callers/tests keyed
    /// to holding exactly `eg-plan`'s `ColumnStats` — see that field's doc). `None` until first
    /// use; populated by `eg-plan`, not here.
    pub distinct_stats_memo: OnceLock<Arc<dyn Any + Send + Sync>>,
    /// Graph-level named-projection catalog + dependency-clock handle
    /// (CONCEPT:EG-KG.query.named-graph-projection-catalog, W4.5/N5). Unlike `plan_stats_memo`/
    /// `label_index_memo` above (per-snapshot memos that start cold on every view), this is
    /// GRAPH identity, not snapshot-derived data — see [`ProjectionScope`]'s doc. `None` on any
    /// view not produced by `GraphCore::analysis_snapshot_versioned`.
    #[cfg(feature = "result-cache")]
    pub projection_scope: Option<ProjectionScope>,
    /// Node ids CURRENTLY ontology schema (TBox) as of this snapshot (BUG A3,
    /// 2026-08-12) — derived from `GraphCore::schema_refs`'s live reverse
    /// index at snapshot time, NEVER cached as a property on the node itself.
    /// `IsolationLayer::filter_view` consults this set (not a `_schema`
    /// property key decoded from the blob) to exempt TBox nodes from
    /// row-level default-deny, so deleting the axiom that made a node schema
    /// naturally drops it from the NEXT snapshot's set — no separate "clear
    /// the marker" step exists or is needed. Empty on a `GraphView` with no
    /// node data at all (`topology_snapshot`) since there is nothing to
    /// row-filter there.
    pub schema_node_ids: std::collections::HashSet<String>,
    /// perf/row-visibility-index: each node's DECODED RLS visibility as of this
    /// snapshot, captured from `GraphCore`'s write-time-maintained
    /// `visibility_index` (see [`GraphCore::live_visibility_index`]) so
    /// `IsolationLayer::can_see_node`/`filter_view` (`crate::isolation`) can
    /// consult it as an O(1) map lookup per node instead of running
    /// `row_visibility`'s full bounded msgpack decode for every node on every
    /// cold query — the SAME `RwLock<Option<HashMap<..>>>` + incremental-posting
    /// idiom [`GraphCore`]'s `label_index` already uses, mirrored here as a
    /// per-snapshot copy exactly like `schema_node_ids` immediately above (real
    /// snapshot DATA, not a derived memo — see that field's doc). A node absent
    /// from this map (a `topology_snapshot`, which carries no property blobs at
    /// all, or any hand-built `GraphView` not sourced from `GraphCore`) is not a
    /// correctness gap: `can_see_node` falls back to its original per-blob decode
    /// for any id missing here. Only ever populated under `security` — the only
    /// build where [`crate::isolation::RowVisibility`] exists at all.
    #[cfg(feature = "security")]
    pub visibility_index: HashMap<String, crate::isolation::RowVisibility>,
}

/// Graph-level handles threaded into a [`GraphView`] (CONCEPT:EG-KG.query.named-graph-projection-catalog,
/// W4.5/N5) so code holding only a read-only view — never the live, lock-bearing `GraphCore` —
/// can still reach the two pieces of graph-IDENTITY state a named projection needs: the
/// [`crate::projection_catalog::ProjectionCatalog`] itself and the [`crate::dep_scope::DepClock`]
/// that validates its entries. Deliberately narrow: NOT the whole `GraphCore` (which holds the
/// `RwLock`/`DashMap` state a read-only, off-lock algorithm must never touch — the entire reason
/// `GraphView` snapshots exist, per this file's opening doc comment). Both fields are `Arc`
/// clones of the SAME live objects `GraphCore` owns, not forks of their state, so a projection
/// materialized through this handle is visible to every OTHER query against the same graph, and
/// a write recorded on the live `DepClock` is immediately visible here.
///
/// `None` on any `GraphView` not produced via a `(view, version)` pair —
/// `topology_snapshot`/`analysis_snapshot`/`get_subgraph`/manually-constructed test views — so a
/// `gds.*` procedure without this handle simply falls back to re-projecting from the view on
/// every call (today's behavior; zero regression for any pre-existing caller). Only
/// `GraphCore::analysis_snapshot_versioned` populates it, because a freshly materialized
/// projection must be stamped with the SAME atomically-read `version` that call already
/// captures.
#[derive(Clone)]
#[cfg(feature = "result-cache")]
pub struct ProjectionScope {
    pub catalog: Arc<crate::projection_catalog::ProjectionCatalog>,
    pub dep_clock: Arc<crate::dep_scope::DepClock>,
    /// The graph version this VIEW reflects — what a NEWLY materialized projection is stamped
    /// `computed_at` with, so it validates correctly against `dep_clock` on the next lookup.
    pub version: u64,
}

impl Clone for GraphView {
    /// Clone the immutable graph data but START THE MEMOS COLD. The clone is a
    /// distinct value a caller may in principle mutate through its `pub` fields, so it
    /// must not inherit the source's cached stats (which describe the source's data).
    /// Fresh empty memos make any derived stat recompute on demand for the clone —
    /// staleness is impossible even under a hypothetical clone-then-mutate.
    ///
    /// `projection_scope` is the one exception: it is GRAPH identity (an `Arc` handle to the
    /// source `GraphCore`'s own catalog + clock), not snapshot-derived data, so it is PRESERVED
    /// (an `Arc` clone) rather than reset — a clone of this view must still resolve a named
    /// projection against the same live catalog the original would have.
    fn clone(&self) -> Self {
        GraphView::clone_view_with_cold_memos(self)
    }
}

impl GraphView {
    fn clone_view_with_cold_memos(&self) -> Self {
        Self {
            graph: self.graph.clone(),
            node_map: self.node_map.clone(),
            node_properties: self.node_properties.clone(),
            edge_properties: self.edge_properties.clone(),
            schema_sources: Arc::clone(&self.schema_sources),
            plan_stats_memo: OnceLock::new(),
            label_index_memo: OnceLock::new(),
            distinct_stats_memo: OnceLock::new(),
            #[cfg(feature = "result-cache")]
            projection_scope: self.projection_scope.clone(),
            // BUG A3: real snapshot DATA (this view's point-in-time TBox
            // membership), not a derived cache/memo — preserved like
            // `node_properties`/`edge_properties` above, not reset.
            schema_node_ids: self.schema_node_ids.clone(),
            // perf/row-visibility-index: same category as `schema_node_ids`
            // immediately above — real point-in-time snapshot data, preserved.
            #[cfg(feature = "security")]
            visibility_index: self.visibility_index.clone(),
        }
    }
}

impl std::fmt::Debug for GraphView {
    /// The type-erased `plan_stats_memo` and the `label_index_memo` are derived
    /// caches, not data (and the former isn't `Debug`), so both are omitted —
    /// `Debug` shows exactly the snapshot's graph and property data, unchanged
    /// from the former `#[derive(Debug)]`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphView")
            .field("graph", &self.graph)
            .field("node_map", &self.node_map)
            .field("node_properties", &self.node_properties)
            .field("edge_properties", &self.edge_properties)
            .finish()
    }
}

impl GraphView {
    /// Does a directed edge source→target exist in this view? (Used by VF2
    /// subgraph matching, which runs on a snapshot.)
    pub fn has_edge(&self, source_id: &str, target_id: &str) -> bool {
        if let (Some(&s), Some(&t)) = (self.node_map.get(source_id), self.node_map.get(target_id)) {
            self.graph.find_edge(s, t).is_some()
        } else {
            false
        }
    }

    /// Does this view currently hold a node with `node_id`? (Read-your-own-writes
    /// conflict checks over an overlaid snapshot — CONCEPT:EG-KG.compute.kg-transaction-is-pinned.)
    pub fn has_node(&self, node_id: &str) -> bool {
        self.node_map.contains_key(node_id)
    }

    /// The decoded property object for a node in this view, or `None` when the node
    /// is absent / its blob is not a decodable object (CONCEPT:EG-KG.compute.kg-transaction-is-pinned RETURNING over
    /// an overlaid snapshot).
    pub fn node_row_object(
        &self,
        node_id: &str,
    ) -> Option<serde_json::Map<String, serde_json::Value>> {
        let blob = self.node_properties.get(node_id)?;
        match decode_property_value(blob) {
            Ok(serde_json::Value::Object(o)) => Some(o),
            _ => None,
        }
    }

    /// Lazily build-and-cache a `label → node ids` index over this snapshot's
    /// `node_properties`, using `build` to decide what "label" means (see
    /// `label_index_memo`'s doc). Computed once per view on first call — an O(N)
    /// decode pass — and reused for every later lookup against the same
    /// immutable snapshot, regardless of which label(s) are queried, so a
    /// caller that used to full-scan on every `(var:Label)` lookup now full-scans
    /// at most once per query.
    pub fn label_index(
        &self,
        build: impl FnOnce(&Self) -> HashMap<String, Vec<String>>,
    ) -> &HashMap<String, Vec<String>> {
        self.label_index_memo.get_or_init(|| build(self))
    }

    // ── Read-your-own-writes overlay (CONCEPT:EG-KG.compute.kg-transaction-is-pinned) ────────────────────
    //
    // A pgwire wire transaction buffers its graph-node mutations and only applies
    // them at COMMIT (through `GraphCore::txn`). A SELECT issued INSIDE that open
    // transaction runs against `analysis_snapshot()` — a point-in-time clone that
    // predates the buffered writes — so without an overlay it would NOT see the
    // txn's own uncommitted inserts/updates/deletes. These methods replay a
    // buffered op onto the cloned view (never touching the live `GraphCore`), so a
    // read inside the txn observes its own writes. They mirror the corresponding
    // `GraphTxn` op minus the ledger (a view carries no ledger).

    /// Overlay a buffered node ADD onto this snapshot (mirrors `GraphTxn::add_node`).
    pub fn overlay_add_node(&mut self, node_id: String, properties_msgpack: Vec<u8>) {
        if !self.node_map.contains_key(&node_id) {
            let idx = self.graph.add_node(node_id.clone());
            self.node_map.insert(node_id.clone(), idx);
        }
        self.node_properties
            .insert(node_id, Arc::new(properties_msgpack));
    }

    /// Overlay a buffered node REMOVE onto this snapshot (mirrors
    /// `GraphTxn::remove_node`): drop the node, its properties, and any incident
    /// edge properties.
    pub fn overlay_remove_node(&mut self, node_id: &str) {
        if let Some(idx) = self.node_map.remove(node_id) {
            self.node_properties.remove(node_id);
            self.edge_properties
                .retain(|k, _| k.0 != node_id && k.1 != node_id);
            self.graph.remove_node(idx);
        }
    }

    /// Overlay a buffered compare-and-set onto this snapshot (mirrors
    /// `GraphTxn::compare_and_set_fields`): when every `(field, expected)` in
    /// `conditions` matches the node's current value (a MISSING field reads as
    /// `null`), merge `updates` into the property object. Returns whether it
    /// applied. A missing/undecodable node, or a failed condition, is a no-op
    /// returning `false`.
    pub fn overlay_compare_and_set_fields(
        &mut self,
        node_id: &str,
        conditions: &serde_json::Map<String, serde_json::Value>,
        updates: &serde_json::Map<String, serde_json::Value>,
    ) -> bool {
        let bytes = match self.node_properties.get(node_id) {
            Some(b) => b.clone(),
            None => return false,
        };
        let mut val = match decode_property_value(&bytes) {
            Ok(v) => v,
            Err(_) => return false,
        };
        let obj = match val.as_object_mut() {
            Some(o) => o,
            None => return false,
        };
        for (field, expected) in conditions {
            let current = obj.get(field).unwrap_or(&serde_json::Value::Null);
            if current != expected {
                return false;
            }
        }
        for (field, value) in updates {
            obj.insert(field.clone(), value.clone());
        }
        let reenc = match rmp_serde::to_vec_named(&val) {
            Ok(b) => b,
            Err(_) => return false,
        };
        self.node_properties
            .insert(node_id.to_string(), Arc::new(reenc));
        true
    }

    /// Overlay a buffered edge ADD onto this snapshot (mirrors `GraphTxn::add_edge`,
    /// CONCEPT:EG-KG.query.txn-cross-modal-ryow — in-txn cross-modal RYOW). Adds a petgraph edge between two
    /// nodes that already exist in the view (an edge to a not-yet-present endpoint is
    /// dropped, exactly like `GraphTxn::add_edge` errors on a missing endpoint), and
    /// records its property blob under `edge_properties`. This makes a staged edge
    /// BFS-reachable on the cloned view so the Traverse leg of an in-txn unified query
    /// sees the txn's own uncommitted edges. Never touches the live `GraphCore`.
    /// Returns whether the edge was added (both endpoints present).
    pub fn overlay_add_edge(
        &mut self,
        source_id: String,
        target_id: String,
        properties_msgpack: Vec<u8>,
    ) -> bool {
        let (Some(&s), Some(&t)) = (self.node_map.get(&source_id), self.node_map.get(&target_id))
        else {
            return false;
        };
        self.graph
            .add_edge(s, t, format!("{}:{}", source_id, target_id));
        self.edge_properties
            .entry((source_id, target_id))
            .or_default()
            .push(Arc::new(properties_msgpack));
        true
    }

    /// Overlay a buffered edge REMOVE onto this snapshot (mirrors
    /// `GraphTxn::remove_edge`, CONCEPT:EG-KG.query.txn-cross-modal-ryow): drop every petgraph edge between the
    /// endpoints and forget their edge properties, so a staged deletion is invisible
    /// to the in-txn Traverse leg. A no-op when either endpoint or the edge is absent.
    pub fn overlay_remove_edge(&mut self, source_id: &str, target_id: &str) {
        if let (Some(&s), Some(&t)) = (self.node_map.get(source_id), self.node_map.get(target_id)) {
            // A pair may carry multiple parallel edges. Collect them in one
            // adjacency walk; repeated `find_edge` rescanned that list once per
            // parallel edge.
            let edges: Vec<_> = self.graph.edges_connecting(s, t).map(|e| e.id()).collect();
            for e in edges {
                self.graph.remove_edge(e);
            }
        }
        self.edge_properties
            .remove(&(source_id.to_string(), target_id.to_string()));
    }
}
