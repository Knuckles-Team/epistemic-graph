// CONCEPT:EG-KG.compute.graph-compute-engine - Core Graph Storage Module
//
// Core petgraph DiGraph CRUD operations, node/edge storage,
// serialization, ledger, and repository parsing.

use aho_corasick::{AhoCorasick, MatchKind};
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock, RwLockWriteGuard};
use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::EdgeRef;
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

/// The graph TOPOLOGY — the petgraph structure + the id→index map. Mutated only
/// under `GraphCore::topo` write lock, read under its read lock. Kept separate
/// from properties so that property reads/writes (the common hot path) never
/// contend on the structural lock. (Phase C-B)
#[derive(Debug, Default, Clone)]
pub struct Topology {
    pub graph: StableDiGraph<String, String>,
    pub node_map: HashMap<String, NodeIndex>,
}

/// An owned, consistent, UNLOCKED read view of a graph (topology + properties),
/// produced by `GraphCore::*_snapshot`. The read-only graph algorithms operate on
/// a `GraphView` (never on the live, locked `GraphCore`), so a long O(V·E)
/// computation runs entirely off the graph's locks. (Phase C-B)
#[derive(Default)]
pub struct GraphView {
    pub graph: StableDiGraph<String, String>,
    pub node_map: HashMap<String, NodeIndex>,
    pub node_properties: HashMap<String, Arc<Vec<u8>>>,
    pub edge_properties: HashMap<(String, String), Vec<Arc<Vec<u8>>>>,
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
        Self {
            graph: self.graph.clone(),
            node_map: self.node_map.clone(),
            node_properties: self.node_properties.clone(),
            edge_properties: self.edge_properties.clone(),
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

/// Concurrent graph storage (Phase C-B — enterprise multi-write concurrency).
///
/// The store is split across independent locks so same-graph operations no longer
/// serialize behind one big lock:
/// * `topo` (RwLock) — structural changes (add/remove node/edge) take the write
///   lock; graph-traversal reads take the read lock. Structural edits never dangle
///   edges because the topology mutates atomically under one guard.
/// * `node_properties` / `edge_properties` (DashMap) — property reads and writes
///   are lock-free per key and DO NOT touch `topo`, so they run concurrently with
///   each other AND with topology writers/readers.
/// * `ledger` (Mutex), `semantic_store` (RwLock) — their own locks.
///
/// Mutations go through an explicit [`GraphTxn`] (holds `topo.write()` for its
/// duration), so multi-step atomic operations (a whole `batch_update`, the 3-pass
/// reasoning) hold ONE guard — the atomicity is visible in the code, not implied by
/// an outer lock. Single-op convenience methods open a one-shot txn. Properties are
/// `Arc<Vec<u8>>` (Phase C-A) so they move into/out of the DashMap and snapshots
/// without copying the bytes.
/// One capability term found in a query by the ontology lexical gate
/// (CONCEPT:EG-ORCH.routing.lexical-capability-escalation). `term` is the matched alias/name, `node_type` its capability
/// class (Tool/Skill/MCPServer/…), `label` the owning node's display name,
/// `mcp_server` the owning fleet server (so a caller can bind that server's
/// toolset directly), and `score` the matched term's character length.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OntologyMatch {
    pub term: String,
    pub node_type: String,
    pub label: String,
    pub mcp_server: String,
    pub score: f64,
}

/// A built aho-corasick automaton over capability terms plus the per-pattern
/// metadata (parallel to the automaton's pattern ids). Cached on [`GraphCore`]
/// and reused while `node_count` matches the live store (CONCEPT:EG-ORCH.routing.lexical-capability-escalation).
#[derive(Debug)]
struct OntologyTermIndex {
    node_count: usize,
    ac: AhoCorasick,
    metas: Vec<OntologyMatch>,
}

/// Secondary property index (CONCEPT:EG-KG.query.concept-12): a bounded, demand-driven set of
/// per-key `value → node ids` maps for equality lookups. Built lazily on
/// [`GraphCore`] and invalidated by `mark_dirty()`, mirroring the label index.
///
/// Policy (bounded + opt-in so indexing every key can't blow up memory):
/// * `keys` are added on demand — a property key is indexed the FIRST time
///   `nodes_by_property` is called for it (and then reused), so we only pay for
///   the keys queries actually filter on.
/// * `EPISTEMIC_GRAPH_INDEXED_PROPERTIES` (comma-separated) pre-seeds keys to
///   index eagerly on the first build.
/// * `EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES` (default 32) caps how many distinct
///   keys are ever indexed; once full, a new key is NOT added (the caller falls
///   back to a full scan). The cap is read once per build.
#[derive(Debug, Default)]
struct PropertyIndex {
    /// `key → (value → node ids)`. A value is the canonical string form of the
    /// property (see [`GraphCore::property_value_key`]).
    keys: HashMap<String, HashMap<String, Vec<String>>>,
}

/// Default cap on the number of distinct property keys ever indexed.
const DEFAULT_MAX_INDEXED_PROPERTIES: usize = 32;

/// Version stamps for the four lazy node-derived indexes (CONCEPT:EG-KG.storage.incremental-index-stamp,
/// W1.6/P7). See [`GraphCore::index_stamps`]. Each atomic holds the graph `version()` through
/// which the matching index's CONTENTS are current. A build stamps the version it scanned at; an
/// incremental `maintain_indexes` step stamps the batch's committed version. `mark_dirty` uses
/// the stamp to decide whether an index is stale (drop it) or already current (preserve it).
#[derive(Debug, Default)]
struct IndexStamps {
    label: std::sync::atomic::AtomicU64,
    node_id: std::sync::atomic::AtomicU64,
    property: std::sync::atomic::AtomicU64,
    path: std::sync::atomic::AtomicU64,
    /// perf/row-visibility-index sibling stamp for `GraphCore::visibility_index`,
    /// gated like the index itself — only meaningful under `security`.
    #[cfg(feature = "security")]
    visibility: std::sync::atomic::AtomicU64,
}

/// Inverted JSONPath path-index (CONCEPT:EG-KG.compute.json-deep-indexing — document/JSON deep indexing): for
/// each indexed JSONPath, a `value → node ids` map (equality/`->>`) PLUS the set of
/// ids for which the path resolves to any value (existence/containment selectivity).
/// Built lazily on [`GraphCore`] and invalidated by `mark_dirty()`, mirroring the flat
/// [`PropertyIndex`] — so a `WHERE props->>'k' = 'v'` / `props @> '{"k":"v"}'` filter is
/// index-accelerated (candidate ids) instead of a full node scan.
///
/// Policy (bounded + demand-driven, exactly like [`PropertyIndex`], so a deep document
/// with many paths cannot blow up memory):
/// * a JSONPath is indexed the FIRST time it is queried (and then reused);
/// * `EPISTEMIC_GRAPH_INDEXED_JSON_PATHS` (comma-separated) pre-seeds paths;
/// * `EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS` (default 64) caps the distinct paths ever
///   indexed; once full a new path is refused (the caller full-scans).
#[derive(Debug, Default)]
struct PathIndex {
    /// `jsonpath → (canonical scalar value → node ids)` for equality lookups.
    by_value: HashMap<String, HashMap<String, Vec<String>>>,
    /// `jsonpath → node ids` for which the path resolves to ANY value (existence).
    present: HashMap<String, Vec<String>>,
}

impl PathIndex {
    /// Convert the live (HashMap) index into its durable, deterministic snapshot form
    /// (CONCEPT:EG-KG.storage.path-index-store). `stamp` is the source graph's OCC `version()` at persist time.
    /// The BTreeMap conversion normalizes key order so the persisted bytes are stable.
    fn to_persisted(&self, stamp: u64) -> crate::path_persist::PersistedPathIndex {
        let by_value = self
            .by_value
            .iter()
            .map(|(path, vals)| {
                (
                    path.clone(),
                    vals.iter()
                        .map(|(v, ids)| (v.clone(), ids.clone()))
                        .collect(),
                )
            })
            .collect();
        let present = self
            .present
            .iter()
            .map(|(path, ids)| (path.clone(), ids.clone()))
            .collect();
        crate::path_persist::PersistedPathIndex {
            by_value,
            present,
            stamp,
        }
    }

    /// Rehydrate a live (HashMap) index from its durable snapshot form (CONCEPT:EG-KG.storage.path-index-store).
    /// An empty snapshot yields the cold in-memory default, so a warm-start over an
    /// empty store is byte-for-byte the pre-EG-308 boot state.
    fn from_persisted(snap: &crate::path_persist::PersistedPathIndex) -> Self {
        let by_value = snap
            .by_value
            .iter()
            .map(|(path, vals)| {
                (
                    path.clone(),
                    vals.iter()
                        .map(|(v, ids)| (v.clone(), ids.clone()))
                        .collect(),
                )
            })
            .collect();
        let present = snap
            .present
            .iter()
            .map(|(path, ids)| (path.clone(), ids.clone()))
            .collect();
        PathIndex { by_value, present }
    }
}

/// Default cap on the number of distinct JSONPaths ever indexed (CONCEPT:EG-KG.compute.json-deep-indexing).
const DEFAULT_MAX_INDEXED_JSON_PATHS: usize = 64;

/// Capability node types whose names/synonyms form the lexical gate vocabulary.
const CAPABILITY_NODE_TYPES: &[&str] = &[
    "Tool",
    "NativeTool",
    "Skill",
    "MCPServer",
    "Server",
    "BusinessCapability",
    "Resource",
];

/// A change notification (CONCEPT:EG-KG.compute.cdc-event-emit — GraphQL real subscriptions via CDC).
/// Emitted by [`GraphCore::mark_dirty`] (and the remote-change path) AFTER a
/// committed write, carrying the graph's post-write OCC `version`. A subscriber
/// re-resolves its live query when it observes a bump — the foundation for a push
/// GraphQL subscription (server-layer carrier) instead of poll-only.
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    /// The graph this change belongs to (set on the notifier via
    /// [`ChangeNotifier::set_graph`]; empty when the core is unnamed, e.g. a fork).
    pub graph: String,
    /// The post-write OCC version ([`GraphCore::version`]), monotonic per core.
    pub version: u64,
}

/// A sink the change stream pushes [`ChangeEvent`]s to. The server implements this
/// over a Tokio channel (watch/mpsc); eg-core itself stays runtime-free — NO tokio
/// dep is pulled here, so the default/Pi build is unaffected (Pi contract). An
/// implementation MUST NOT block (it runs inline on the write path): do only a
/// non-blocking notify (e.g. `watch::Sender::send`).
pub trait ChangeSink: Send + Sync {
    fn on_change(&self, event: &ChangeEvent);
}

/// Dependency-light change-notification fan-out (CONCEPT:EG-KG.compute.cdc-event-emit). A
/// `parking_lot`-guarded list of `Weak` sinks — NO new dependency (`parking_lot` is
/// already an eg-core dep) and NO async runtime, so eg-core's default build links
/// nothing extra and the tokio carrier lives in the server layer. The no-subscriber
/// path is a single relaxed atomic load, so `emit` stays OFF the write hot path
/// until something actually subscribes. Dropping the subscriber's `Arc`
/// unsubscribes (the notifier holds only a `Weak`, pruned on the next `emit`).
#[derive(Default)]
pub struct ChangeNotifier {
    graph: RwLock<String>,
    sinks: Mutex<Vec<std::sync::Weak<dyn ChangeSink>>>,
    active: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for ChangeNotifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeNotifier")
            .field("subscribers", &self.sinks.lock().len())
            .finish()
    }
}

impl ChangeNotifier {
    /// Name the graph these events belong to (set once when the core is registered).
    pub fn set_graph(&self, name: impl Into<String>) {
        *self.graph.write() = name.into();
    }

    /// Register a sink. The notifier keeps only a `Weak`, so the CALLER must retain
    /// the `Arc` for as long as it wants notifications — dropping it unsubscribes.
    pub fn subscribe(&self, sink: &std::sync::Arc<dyn ChangeSink>) {
        self.sinks.lock().push(std::sync::Arc::downgrade(sink));
        self.active
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Are there any live subscribers? (Cheap relaxed load.)
    pub fn has_subscribers(&self) -> bool {
        self.active.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Emit a change at `version` to every live sink, pruning dead `Weak`s. A no-op
    /// (single atomic load) when nothing has subscribed.
    pub fn emit(&self, version: u64) {
        if !self.active.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        let event = ChangeEvent {
            graph: self.graph.read().clone(),
            version,
        };
        // Upgrade/prune under the subscriber mutex, then invoke callbacks after
        // releasing it. A slow sink no longer serializes subscribe/debug calls,
        // and a sink may safely subscribe another sink from its callback without
        // recursively deadlocking this notifier.
        let live = {
            let mut sinks = self.sinks.lock();
            let live: Vec<_> = sinks.iter().filter_map(std::sync::Weak::upgrade).collect();
            sinks.retain(|sink| sink.strong_count() > 0);
            if sinks.is_empty() {
                self.active
                    .store(false, std::sync::atomic::Ordering::Release);
            }
            live
        };
        for sink in live {
            sink.on_change(&event);
        }
    }
}

#[derive(Debug)]
pub struct GraphCore {
    pub topo: RwLock<Topology>,
    pub node_properties: DashMap<String, Arc<Vec<u8>>>,
    pub edge_properties: DashMap<(String, String), Vec<Arc<Vec<u8>>>>,
    pub ledger: Mutex<Vec<String>>,
    pub semantic_store: RwLock<crate::compute::semantic::SemanticStore>,
    /// Authoritative closed-world integrity policy for this graph. The policy is
    /// part of every graph snapshot and mutation delta; it is never process-global
    /// derived state, so rollback, recovery, and Raft snapshot installation cannot
    /// diverge from the data image it governs.
    integrity_policy: RwLock<Option<IntegrityPolicy>>,
    /// Has this serving projection changed since its last observer pass? Starts
    /// `true` so a freshly created or loaded graph is observed once; authoritative
    /// durability is handled separately at each mutation commit.
    pub dirty: std::sync::atomic::AtomicBool,
    /// Monotonic write-version counter for optimistic concurrency control
    /// (CONCEPT:EG-KG.txn.occ-graph-core — OCC ACID transactions). Bumped once per COMMITTED write
    /// (every single-op/coalesced write via `mark_dirty`, and once per multi-op
    /// txn commit). A staged OCC transaction snapshots this at begin and re-checks
    /// it (plus the read-set node versions) under the commit lock; a concurrent
    /// inline/coalesced write that bumped it forces the txn to re-validate. Read
    /// cheaply via `version()`; never gates a read path.
    pub version: std::sync::atomic::AtomicU64,
    /// Change-notification fan-out (CONCEPT:EG-KG.compute.cdc-event-emit). `mark_dirty` (and the
    /// remote-change path) emit a [`ChangeEvent`] carrying the bumped `version`; a
    /// server-layer GraphQL subscription carrier subscribes to turn a poll-only
    /// subscription into a real push (re-resolve-on-change). Dep-light + off the hot
    /// path when there are no subscribers, so the default/Pi build is unaffected.
    changes: ChangeNotifier,
    /// Cached aho-corasick index of capability-node terms for the lexical
    /// classification gate (CONCEPT:EG-ORCH.routing.lexical-capability-escalation). Built lazily and reused while the
    /// node count is unchanged, so `match_ontology_terms` is ~µs per query
    /// instead of a full node scan. `None` until first use / after invalidation.
    ontology_index: RwLock<Option<OntologyTermIndex>>,
    /// Cached secondary label index (CONCEPT:EG-KG.compute.consult-lazy): `label → node ids` so
    /// `get_nodes_by_label` is an O(1) map lookup instead of a full DashMap scan
    /// that deserializes every node's properties. Built lazily on first label
    /// lookup and invalidated when a successful node write can affect labels — a property update can change a
    /// node's label without changing `node_count`, so this index must NOT key its
    /// validity on node count the way the ontology index does. `None` until first
    /// use / after invalidation. A node appears under every label it carries
    /// across `type`/`node_type`/`label`/`labels` (mirrors `get_nodes_by_label`).
    label_index: RwLock<Option<HashMap<String, Vec<String>>>>,
    /// Sorted node ids for the unlabeled keyset scan. Without this derived cache,
    /// every `MATCH (n) ... LIMIT k` page copied and sorted all N ids before
    /// returning k rows. It is built lazily from topology and invalidated with the
    /// other node-derived caches after a committed write, making warm pages
    /// O(log N + k) instead of O(N log N). Property bytes are never retained here.
    node_id_index: RwLock<Option<Vec<String>>>,
    /// Sorted `(source, target)` edge-key pairs for the keyset-paginated edge scan
    /// (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation — the edge sibling of `node_id_index`'s unlabeled
    /// keyset scan). Without this derived cache, every `get_edges_page` call would
    /// re-collect and re-sort every edge key before returning one page. Built
    /// lazily on first `get_edges_page` call. Invalidated UNCONDITIONALLY on every
    /// committed write (both `mark_dirty` and `mark_dirty_preserving_indexes`,
    /// unlike the node-derived caches above) because a "pure edge batch" — the one
    /// case `mark_dirty_preserving_indexes` exists for — is exactly the case that
    /// changes this index; it cannot ride that node-only skip. Property bytes are
    /// never retained here — only the lightweight key pairs.
    edge_key_index: RwLock<Option<Vec<(String, String)>>>,
    /// Cached secondary PROPERTY index (CONCEPT:EG-KG.query.concept-12): for each indexed
    /// property key, a `value → node ids` map so `nodes_by_property(key, value)`
    /// is an O(1) map lookup instead of a full DashMap scan that deserializes
    /// every node's properties (the perf win behind SQL `WHERE prop = 'x'`
    /// predicate pushdown in eg-query). The set of indexed keys is BOUNDED and
    /// demand-driven: a key is indexed on its FIRST `nodes_by_property` call and
    /// then cached, up to `EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES` keys (default
    /// 32); keys named in `EPISTEMIC_GRAPH_INDEXED_PROPERTIES` (comma-separated)
    /// are pre-seeded on first use. Indexing every key would be unbounded memory,
    /// hence the cap. Invalidated when a successful node write touches a covered
    /// key (or has unknown scope), so it never serves a
    /// stale view across a mutation (a property write can change an indexed value
    /// without changing `node_count`, so validity must NOT key on node count).
    /// `None` until first use / after invalidation; the inner map only ever holds
    /// the keys demanded so far.
    property_index: RwLock<Option<PropertyIndex>>,
    /// Cached inverted JSONPath path-index (CONCEPT:EG-KG.compute.json-deep-indexing — document/JSON deep
    /// indexing): `jsonpath → value → ids` (equality/`->>`) + `jsonpath → ids`
    /// (existence/`@>` selectivity), so a deep JSON filter is index-accelerated
    /// instead of a full node scan. Bounded + demand-driven exactly like
    /// `property_index`, and invalidated when a node write can affect a covered path —
    /// a JSON write can change a nested value without changing `node_count`, so
    /// validity must NOT key on node count. `None` until first use / after
    /// invalidation.
    path_index: RwLock<Option<PathIndex>>,
    /// Durable persistence for the JSONPath path-index (CONCEPT:EG-KG.storage.path-index-store). When a store
    /// is attached (a persist dir is configured), the demand-driven `path_index` is
    /// written through on each (re)build and rehydrated at boot via
    /// [`GraphCore::rehydrate_path_index`], so a restart skips the full node rescan the
    /// EG-084 build would otherwise pay on the first JSON filter after a cold start.
    /// The seam is a dep-free trait object (the redb impl is feature-gated), so the
    /// default/Pi build links nothing extra. `None` (the default) ⇒ the path-index is
    /// fully in-memory and every write-through is a no-op — the pre-EG-308 behavior.
    path_index_store: RwLock<Option<Arc<dyn crate::path_persist::PathIndexPersistence>>>,
    /// The unified secondary-index registry/seam (CONCEPT:AU-KG.retrieval.architecture-report). Owns the
    /// `SecondaryIndex` descriptors (label, property, + discoverable vector /
    /// ontology) so a planner consults ONE registry — `index_for(predicate)` /
    /// `descriptors_for_column(col)` — instead of bespoke per-index checks. The
    /// label/property CACHES still live in the fields above (lazy + selectively
    /// invalidated); the manager only routes, so their behavior is unchanged. The
    /// registry is fixed for the graph's lifetime ⇒ no interior locking needed.
    index_manager: crate::index::IndexManager,
    /// Read-through into the durable tier on a RAM MISS (CONCEPT:EG-KG.storage.read-through-seam-exercised). Set
    /// only under redb-AUTHORITATIVE mode, where a node may have been evicted from
    /// RAM once it is durable in redb. On a node-property miss the read path
    /// consults this to serve the evicted node's stored blob, so eviction can bound
    /// memory WITHOUT making the node unreadable. `None` (the default, and always
    /// off authoritative mode) means a miss is a genuine absence — behavior is then
    /// byte-for-byte unchanged. See `crate::read_through`.
    read_through: RwLock<Option<Arc<dyn crate::read_through::ReadThrough>>>,
    /// Bloom-filter guard on the `read_through` NEGATIVE-lookup path
    /// (CONCEPT:EG-KG.storage.bloom-negative-lookup-guard): populated on every
    /// `AddNode` and on every full/complete node-set (re)materialization, so a
    /// RAM miss for a node id that was NEVER seen skips the blocking redb
    /// point-read entirely. `RwLock` because the filter is replaced wholesale
    /// (resized) on a full re-materialization (`replace_snapshot`); ordinary
    /// inserts only need the read guard (bit words are `AtomicU64`). See
    /// `crate::bloom`.
    node_bloom: RwLock<crate::bloom::NodeBloomFilter>,
    /// Whether `node_bloom` currently reflects the COMPLETE known durable node-id
    /// set for this graph (as opposed to only the ids added so far in a
    /// paged-lazy-open still in progress — CONCEPT:EG-KG.sharding.paged-lazy-open).
    /// While `false`, `read_through_get` never consults the filter (falls
    /// through to the original always-read behavior), so a not-yet-paged-in
    /// node can never be mistaken for a genuine absence. Set by the registry on
    /// a full/eager load and on the final page of a paged-lazy open; also set by
    /// `replace_snapshot`, which always rebuilds the filter from a COMPLETE
    /// node set.
    bloom_complete: std::sync::atomic::AtomicBool,
    /// Version stamps for the lazy node-derived indexes (CONCEPT:EG-KG.storage.incremental-index-stamp,
    /// W1.6/P7). Each records the graph `version()` through which its index's CONTENTS have been
    /// maintained (built or incrementally updated by `maintain_indexes`). `mark_dirty` drops an
    /// index only when its stamp is STALE relative to the write's new version — so a write whose
    /// maintenance step already brought the index current (an ADD/REMOVE incrementally applied to
    /// the postings, rather than a full rebuild) is PRESERVED, while a write that bypassed
    /// maintenance (leaving a stale stamp) still invalidates. Stamps are read + written under
    /// each index's own `RwLock`, so the stamp and its index contents move atomically.
    index_stamps: IndexStamps,
    /// Count of FULL rebuilds of the node-derived label / property / JSONPath indexes
    /// (CONCEPT:EG-KG.storage.incremental-index-stamp, W1.6/P7 — the "rebuild count under continuous
    /// ingest ~0" metric). Incremented once per `build_label_index` / `build_property_value_map` /
    /// `build_json_path_maps` — the O(V) scans W1.6 replaced with per-write incremental posting
    /// maintenance. Under continuous ingest this stays ~1 (the initial warm build) instead of
    /// growing one-per-write. Observability only; never on a read hot path.
    index_rebuilds: std::sync::atomic::AtomicU64,
    /// Dependency clock for the result cache's dependency-scoped invalidation
    /// (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation, W1.6/P7, feature `result-cache`).
    /// A write bumps only the label / key / node / edge dimensions it touched; a cached result
    /// survives every write disjoint from the query's dependency set. See `crate::dep_scope`.
    /// `Arc`-wrapped (not a bare value) so [`Self::analysis_snapshot_versioned`] can hand a
    /// SHARED, live handle to it into a [`ProjectionScope`] riding along on a [`GraphView`] —
    /// the same continuously-fed clock instance, not a fork of its state — letting `eg-query`'s
    /// GDS procedures (which see only `&GraphView`, never the lock-bearing `GraphCore`) validate
    /// a named graph projection against it (CONCEPT:EG-KG.query.named-graph-projection-catalog,
    /// W4.5/N5). Every existing accessor/call site is unaffected: `&DepClock` methods take
    /// `&self` (interior mutability throughout), so they resolve through the `Arc` via ordinary
    /// auto-deref with no call-site changes.
    #[cfg(feature = "result-cache")]
    dep_clock: Arc<crate::dep_scope::DepClock>,
    /// Version-keyed query-RESULT cache (CONCEPT:EG-KG.coordination.distributed-cache-coherence, feature `result-cache`).
    /// Caches the serialized bytes of a read query (`Sql`/`Cypher`/`Sparql`/
    /// `UnifiedQuery`) keyed by `(query-hash, version())`. A repeated identical query
    /// on an UNCHANGED graph hits; any write bumps `version` so the next lookup keys
    /// on a new version and misses (recompute) — staleness is impossible by
    /// construction. Bounded LRU, pure-Rust, so it folds into the lean Pi tier.
    #[cfg(feature = "result-cache")]
    result_cache: crate::result_cache::ResultCache,
    /// Named graph-projection catalog (CONCEPT:EG-KG.query.named-graph-projection-catalog, W4.5 /
    /// N5, feature `result-cache`) — the `gds.graph.project`-equivalent: a materialized
    /// projection an algorithm re-runs against without re-scanning the graph, invalidated via
    /// [`Self::dep_clock`] (reused, not duplicated). `Arc`-wrapped for the SAME reason as
    /// `dep_clock`: shared into a [`GraphView`]'s [`ProjectionScope`] so `eg-query`'s `gds.*`
    /// procedures can populate/consult it. See `crate::projection_catalog`.
    #[cfg(feature = "result-cache")]
    graph_projections: Arc<crate::projection_catalog::ProjectionCatalog>,
    /// D-OP-1 / D-OB-20 — bounded per-actor cache of `GraphReadAuthority::project_core`'s
    /// RLS projection, invalidated by `version` advancing. See
    /// `crate::rls_projection_cache` for the full rationale (mirrors the
    /// invalidate-on-version-change idiom `ontology_index`/`label_index` already use,
    /// extended to be per-actor). Only ever populated/consulted from the
    /// `security`-gated half of `project_core` (`src/server/access.rs`).
    #[cfg(feature = "security")]
    rls_projection_cache: crate::rls_projection_cache::ProjectionCache,
    /// perf/cold-query-floor-analysis (UNCOMPILED proposal, not yet wired into any
    /// caller) — bounded per-actor cache of the RLS-FILTERED `GraphView`
    /// `Method::CypherQuery`'s cache-miss path would build, sibling of
    /// `rls_projection_cache` immediately above (same invalidate-on-version-change +
    /// whole-image `generation` idiom) but holding the lighter-weight `GraphView`
    /// instead of a second whole `GraphCore`. See `crate::rls_view_cache`.
    #[cfg(feature = "security")]
    rls_view_cache: crate::rls_view_cache::FilteredViewCache,
    /// perf/row-visibility-index: cached `node_id -> RowVisibility` for every
    /// currently-live node, mirroring `label_index`'s exact shape and idiom
    /// (`RwLock<Option<HashMap<..>>>`, built lazily via [`Self::live_visibility_index`]
    /// / [`Self::build_visibility_index`], stamped via `index_stamps.visibility`,
    /// dropped by [`Self::invalidate_indexes`] / [`Self::invalidate_node_indexes_if_stale`]
    /// exactly like the other node-derived caches) — EXCEPT this one is also
    /// incrementally MAINTAINED on every add/remove/RLS-key-touching update
    /// (`Self::visibility_index_set`/`Self::visibility_index_remove`, called from
    /// `Self::invalidate_indexes_for_change`), so once warm it never needs a full
    /// rebuild again under continuous ingest — only the ONE node a write actually
    /// touched is re-decoded. This is what makes the FIRST query after a write-driven
    /// `FilteredViewCache`/`rls_projection_cache` invalidation (`crate::rls_view_cache`,
    /// `crate::rls_projection_cache` — both amortize the FILTERED result across
    /// queries, not the per-node decode itself) cheap too: `IsolationLayer::can_see_node`
    /// consults a per-snapshot copy of this map (`GraphView::visibility_index`,
    /// captured by `Self::live_visibility_index` at snapshot time) as an O(1) lookup
    /// instead of re-running `crate::isolation::row_visibility`'s full bounded
    /// msgpack decode over every node in the graph. Every entry is produced by
    /// calling that SAME function on the node's raw blob — this index never
    /// reimplements or approximates the RLS decode, only memoizes it — so a warm
    /// lookup is byte-for-byte identical to what a cold decode of the same blob
    /// would have produced. Gated by `security` (the only build where
    /// `crate::isolation::RowVisibility` exists at all).
    #[cfg(feature = "security")]
    visibility_index: RwLock<Option<HashMap<String, crate::isolation::RowVisibility>>>,
    /// perf/row-visibility-index: count of FULL rebuilds of `visibility_index`
    /// (the row-visibility sibling of [`Self::index_rebuilds`], kept separate so
    /// this new index's own "rebuild count under continuous ingest ~1" signal is
    /// never conflated with the pre-existing label/property/path counter). Never on
    /// a read hot path — observability only.
    #[cfg(feature = "security")]
    visibility_index_rebuilds: std::sync::atomic::AtomicU64,
    /// A18 TBox/ABox RLS distinction (BUG A3, 2026-08-12): reverse index counting
    /// LIVE ontology schema-defining triples per node id — `iri -> count of
    /// live schema-defining triples`. A node is TBox (schema-exempt from
    /// row-level default-deny, `eg_core::isolation::RLS_SCHEMA_KEY`'s
    /// reasoning) iff its count here is `> 0`, DERIVED fresh on every read via
    /// [`Self::is_schema_node`] — never cached as a settable property flag on
    /// the node itself. `eg-rdf`'s SPARQL UPDATE insert/delete path
    /// ([`Self::mark_schema_ref`]/[`Self::unmark_schema_ref`]) maintains this
    /// in the SAME mutation as the triple write/delete, so an axiom's deletion
    /// makes its IRI revert to ordinary ABox default-deny automatically — there
    /// is no separate "clear the marker" step to forget. Previously this crate
    /// stamped a `_schema: true` PROPERTY on the node at insert time and never
    /// cleared it on delete: an IRI once used as a schema term and later
    /// repurposed as an ABox individual stayed permanently schema-visible.
    /// Bounded by the number of LIVE schema triples (ontology size), not graph
    /// size — cheap even on a large ABox.
    ///
    /// **KNOWN GAP (2026-08-12), found during A3 end-to-end verification:**
    /// this field is IN-MEMORY ONLY — it is NOT one of [`GraphSnapshot`]'s
    /// fields, unlike `ledger` (its closest analogue: also
    /// process-observability, also carried in full on every snapshot). The
    /// mutation gateway's staged-commit pipeline
    /// (`server::mutation::commit_mutation_body`) executes a write against an
    /// ISOLATED `GraphCore` built fresh via [`Self::from_snapshot`], then
    /// durably commits and publishes the result back to the LIVE core via
    /// [`Self::prepare_snapshot_publish`]/[`Self::replace_snapshot`] — a
    /// wholesale `GraphSnapshot` replace. Because `schema_refs` is absent
    /// from that struct, `mark_schema_ref`/`unmark_schema_ref` calls made
    /// against the STAGED image (e.g. `eg_rdf::mapping::load_triples`/
    /// `eg_rdf::update::remove_triples` invoked via the native
    /// `Method::AddTriples`/`Method::RemoveTriples` wire methods, which ARE
    /// gateway-routed this way) never reach the live, servable core — a
    /// silent no-op, confirmed via direct instrumentation of
    /// `access::build_projection` while writing this fix's regression test.
    /// The mechanism (mark/unmark/`is_schema_node`/`filter_view`
    /// consultation) IS correct in isolation and IS correctly wired for the
    /// pgwire `SPARQL UPDATE INSERT DATA` cross-modal seam specifically,
    /// because THAT commit path
    /// (`server::wire::WireSession::commit_txn_state` →
    /// `handlers::txn::commit_cross_modal_txn`) mutates the live core
    /// directly rather than staging through a throwaway `GraphCore` +
    /// `GraphSnapshot` round-trip. Closing this gap for the gateway-routed
    /// paths needs `schema_refs` added to `GraphSnapshot` (a
    /// `#[serde(deny_unknown_fields)]`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`-
    /// gated durable format this crate's own docs call "strict... the
    /// mandatory version and unknown-field rejection prevent a partial or
    /// differently shaped image from being accepted as current") plus the
    /// matching capture/restore in [`Self::snapshot`]/[`Self::replace_snapshot`]
    /// — a durable-schema migration, deliberately NOT attempted here: it is
    /// a materially larger, higher-blast-radius change (the durable format
    /// used by every existing redb-backed deployment) than this fix's scope,
    /// and belongs in its own reviewed change.
    schema_refs: DashMap<String, u64>,
    /// BUG A1 follow-up (2026-08-12): count of ledger entries permanently
    /// dropped from the FRONT of [`Self::ledger`] over this `GraphCore`
    /// instance's lifetime, by [`Self::push_ledger`]'s drop-oldest-half cap.
    /// `ledger` is an in-memory, ephemeral, 100k-entry ring — NOT a durable
    /// change log — so exceeding the cap, a cold-tenant idle offload/
    /// hibernate/rehydrate cycle, an eviction, or a process restart can all
    /// empty or truncate it while the underlying mutations stay durably
    /// committed in redb. This counter is the 0-based sequence of the OLDEST
    /// entry the ledger can currently vouch for (`Self::ledger_watermark`) —
    /// exposed on every `GetLedger` response so a caller can DETECT
    /// truncation across reads (watermark increased) instead of inferring
    /// completeness from a merely-nonzero read.
    ledger_dropped_total: std::sync::atomic::AtomicU64,
}

impl Default for GraphCore {
    fn default() -> Self {
        Self::new()
    }
}

/// Write transaction over a [`GraphCore`]: holds the topology write lock for its
/// lifetime and borrows the property maps + ledger. All mutations run through it,
/// so a sequence of mutations under one `txn()` is atomic w.r.t. other topology
/// writers (and excludes graph-traversal readers) for the transaction's duration.
/// Property writes still go through the DashMap (lock-free per key) but are ordered
/// by the held topology guard for structural consistency. (Phase C-B)
pub struct GraphTxn<'a> {
    pub topo: RwLockWriteGuard<'a, Topology>,
    node_properties: &'a DashMap<String, Arc<Vec<u8>>>,
    edge_properties: &'a DashMap<(String, String), Vec<Arc<Vec<u8>>>>,
    ledger: &'a Mutex<Vec<String>>,
    /// Read-guard access to `GraphCore::node_bloom` (CONCEPT:EG-KG.storage.bloom-negative-lookup-guard) —
    /// `add_node` records every id it inserts, lock-free (`fetch_or` on `AtomicU64`
    /// words) under a shared read guard.
    node_bloom: &'a RwLock<crate::bloom::NodeBloomFilter>,
    /// Shared with `GraphCore::ledger_dropped_total` (BUG A1 follow-up) — every
    /// PRODUCTION ledger write goes through `Self::push_ledger`, which applies
    /// the cap + drop-accounting policy via `push_ledger_impl`.
    ledger_dropped_total: &'a std::sync::atomic::AtomicU64,
}

/// Result of an owner-fenced delivery-tag nack transition.
#[cfg(feature = "broker")]
#[derive(Debug, Clone, PartialEq)]
pub enum BrokerNackTransition {
    /// The lookup/message was missing, stale, or owned by another consumer.
    Absent,
    /// The current delivery was atomically returned to the pending pool.
    Requeued,
    /// The current delivery was atomically removed and may now be dead-lettered.
    Terminal {
        node_id: String,
        queue: String,
        properties: serde_json::Value,
    },
}

/// Owned, serializable graph state used for isolated mutation staging and portable
/// transfer (CONCEPT:EG-KG.storage.nonblocking-checkpoint). Two properties matter:
///
/// * **Short lock scope:** producing it clones node/edge/ledger/semantic data so
///   encoding and isolated execution happen after the topology lock is released.
/// * **Direct serialization (A3):** encoded straight via `rmp_serde`. Node/edge
///   properties are already MessagePack byte blobs, so the current snapshot schema
///   never detours through `serde_json::Value` or allocates a second property image.
/// * **Strict persisted schema:** the mandatory version and unknown-field rejection
///   prevent a partial or differently shaped image from being accepted as current.
pub const GRAPH_SNAPSHOT_SCHEMA_VERSION: u16 = 2;

/// Current graph-scoped integrity policy. Only the enforcing posture exists;
/// storing the validated source document keeps `eg-core` independent of SHACL
/// while allowing the server layer to compile it into its native guard.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrityPolicy {
    pub shapes_ttl: String,
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    <Option<T> as serde::Deserialize>::deserialize(deserializer)
}

/// KNOWN GAP (2026-08-12): does NOT carry `GraphCore::schema_refs` (the A18
/// TBox/ABox live reverse index) — see that field's own doc for the full
/// consequence (the mutation gateway's staged-commit pipeline silently drops
/// schema marks for gateway-routed native writes) and why fixing it is a
/// deliberately deferred, separately-scoped durable-schema migration.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphSnapshot {
    pub schema_version: u16,
    /// Required current-schema field. `None` is an explicit fail-closed,
    /// not-yet-provisioned policy state; omission is rejected by serde.
    #[serde(deserialize_with = "deserialize_required_option")]
    pub integrity_policy: Option<IntegrityPolicy>,
    // Arc-valued (Phase C-A): building a snapshot clones Arc pointers, not the
    // property bytes. The serialized current schema remains an owned byte array.
    pub nodes: Vec<(String, Arc<Vec<u8>>)>,
    pub edges: Vec<(String, String, Arc<Vec<u8>>)>,
    pub ledger: Vec<String>,
    pub semantic_store: crate::compute::semantic::SemanticStore,
}

// Snapshots may be substantially larger than one RPC frame, but they still cross
// a restore boundary and must be structurally bounded before serde can honor any
// attacker-controlled collection length hint.
const MAX_GRAPH_SNAPSHOT_BYTES: usize = 1024 * 1024 * 1024;
const MAX_GRAPH_SNAPSHOT_ITEMS: usize = 16_000_000;
fn decode_property_value(
    bytes: &[u8],
) -> Result<serde_json::Value, eg_types::msgpack::MsgpackValidationError> {
    eg_types::msgpack::decode_property_value(bytes)
}

/// The label set a node carries, read from EXACTLY the fields `build_label_index` /
/// `get_nodes_by_label` match — `type` / `node_type` / `label` (scalar) plus every entry of the
/// multi-valued `labels` array — so the incremental label-index maintainers (W1.6/P7,
/// CONCEPT:EG-KG.storage.incremental-index-stamp) file/unfile a node under the SAME labels the full
/// scan would. Duplicates are possible (a node repeating a value across fields) and are collapsed
/// by the sorted-set posting invariant `insert_sorted` keeps.
fn labels_of(val: &serde_json::Value) -> Vec<String> {
    let mut out = Vec::new();
    for key in ["type", "node_type", "label"] {
        if let Some(label) = val.get(key).and_then(|v| v.as_str()) {
            out.push(label.to_string());
        }
    }
    if let Some(arr) = val.get("labels").and_then(|v| v.as_array()) {
        for x in arr {
            if let Some(label) = x.as_str() {
                out.push(label.to_string());
            }
        }
    }
    out
}

/// Insert `id` into a SORTED, deduplicated posting Vec in place (W1.6/P7 incremental index
/// maintenance), preserving the sort + no-duplicate invariant every posting build establishes (so
/// `collect_by_label`'s keyset paging and `nodes_by_properties`' two-pointer intersection stay
/// correct). A no-op when `id` is already present.
fn insert_sorted(ids: &mut Vec<String>, id: &str) {
    if let Err(pos) = ids.binary_search_by(|x| x.as_str().cmp(id)) {
        ids.insert(pos, id.to_string());
    }
}

/// Remove `id` from a SORTED posting Vec in place (W1.6/P7). A no-op when `id` is absent.
fn remove_sorted(ids: &mut Vec<String>, id: &str) {
    if let Ok(pos) = ids.binary_search_by(|x| x.as_str().cmp(id)) {
        ids.remove(pos);
    }
}

/// Accumulate a node's labels + top-level property keys into a write footprint (W1.6/P7,
/// feature `result-cache`) so the dependency clock bumps exactly the dimensions this node's
/// add/remove/update touched.
#[cfg(feature = "result-cache")]
fn collect_dep_footprint(fp: &mut crate::dep_scope::WriteFootprint, val: &serde_json::Value) {
    fp.labels.extend(labels_of(val));
    if let Some(obj) = val.as_object() {
        fp.keys.extend(obj.keys().cloned());
    }
}

impl GraphSnapshot {
    /// Serialize this snapshot to MessagePack (called OFF the graph lock).
    pub fn to_msgpack(&self) -> Result<Vec<u8>, String> {
        self.validate_schema()?;
        rmp_serde::to_vec_named(self).map_err(|e| e.to_string())
    }

    fn validate_schema(&self) -> Result<(), String> {
        if self.schema_version != GRAPH_SNAPSHOT_SCHEMA_VERSION {
            return Err(format!(
                "unsupported graph snapshot schema version {}; expected {}",
                self.schema_version, GRAPH_SNAPSHOT_SCHEMA_VERSION
            ));
        }
        Ok(())
    }
}

/// The error `add_edge`/`add_edge_no_ledger` return when an endpoint id is
/// absent from THIS graph's own `node_map`.
///
/// A `GraphCore`/`GraphView` has no notion of "which graph am I" — it is a
/// bare topology keyed only by node id, with no graph name or cross-graph
/// reference of any kind. Every request is dispatched to exactly one graph's
/// core (resolved by graph name at the registry, one layer above this
/// module), and each graph keeps its own independent node-id namespace, so
/// there is no code path by which an id genuinely belonging to a DIFFERENT
/// graph could ever be looked up here. "Not found" therefore always means
/// one of: a typo/stale id, a node not yet added on this write path, OR —
/// the case this message exists to head off — a caller trying to link two
/// endpoints that live in different graphs, which this API structurally
/// cannot do (an edge cannot span graphs; add both endpoints to the same
/// graph, or use a cross-graph query/reference mechanism instead of a native
/// edge). Without this framing the bare "node not found" reads as a missing-
/// data bug and sends readers hunting for a deleted/never-written node,
/// exactly the confusion this wording is meant to prevent.
fn edge_endpoint_not_found(role: &str, id: &str) -> String {
    format!(
        "{role} node '{id}' not found in this graph. Edges cannot span \
         graphs — each graph has its own independent node namespace, so \
         `add_edge` only ever looks up '{id}' within the one graph this \
         call is targeting. If '{id}' exists, it is either a typo/stale id \
         or it belongs to a DIFFERENT graph than the one you're writing to; \
         add both edge endpoints to the same graph, or use a cross-graph \
         query/reference instead of a native edge.",
    )
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

/// Streams a byte slice as lowercase hex DIRECTLY into a formatter (CONCEPT:AU-KG.backend.b-auto-size).
///
/// `format!("…|{}", hex::encode(&blob))` allocated the 2·N-byte hex String TWICE — once
/// for `hex::encode`'s return, then again as `format!` copied it into the final buffer —
/// all while the topology write guard is held. For a large property blob that transient
/// double-allocation is a leading Pi memory + lock-hold driver. Using this `Display`
/// adapter, `format!` writes the hex digits straight into its single output buffer with
/// no intermediate String, so the ledger line is built with ONE allocation. The emitted
/// text is byte-identical to `hex::encode` (lowercase, 2 chars/byte), so the on-disk
/// ledger format and every consumer (audit mirror, redb `ledger` table, snapshot replay)
/// are unchanged.
struct HexLedger<'a>(&'a [u8]);

impl std::fmt::Display for HexLedger<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for &b in self.0 {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

/// Cap on `GraphCore::ledger`'s length (BUG A1 follow-up, 2026-08-12): the
/// in-memory mutation ledger is an ephemeral, bounded ring, NOT a durable
/// change log. Exceeding this drops the oldest half — see
/// `GraphTxn::push_ledger`'s doc for the full reasoning and
/// `GraphCore::ledger_watermark` for how a caller detects it happened.
const LEDGER_CAP: usize = 100_000;
/// How many of the oldest entries `GraphTxn::push_ledger` drops in one trim,
/// once `LEDGER_CAP` is exceeded.
const LEDGER_TRIM: usize = 50_000;

/// Shared implementation behind `GraphTxn::push_ledger` — the staged-
/// transaction writer every production mutation actually goes through
/// (`add_node`/`remove_node`/`add_edge`/`remove_edge`/...). Split out as its
/// own free function (rather than inlined into that one method) so a future
/// second caller can share the exact same drop-oldest-half + watermark-
/// accounting policy without duplicating it. See
/// `GraphCore::ledger_dropped_total`'s field doc for the full BUG A1
/// follow-up reasoning.
fn push_ledger_impl(
    ledger: &Mutex<Vec<String>>,
    dropped_total: &std::sync::atomic::AtomicU64,
    entry: String,
) {
    let mut ledger = ledger.lock();
    ledger.push(entry);
    if ledger.len() > LEDGER_CAP {
        let dropped = ledger.drain(0..LEDGER_TRIM).count() as u64;
        let total_dropped =
            dropped_total.fetch_add(dropped, std::sync::atomic::Ordering::Relaxed) + dropped;
        let retained = ledger.len();
        drop(ledger);
        tracing::warn!(
            dropped,
            total_dropped,
            retained,
            "graph mutation ledger exceeded its {LEDGER_CAP}-entry cap and dropped its \
             oldest {LEDGER_TRIM} entries -- GetLedger watermark advanced to {total_dropped}; \
             durable state is unaffected, only the in-memory audit ledger lost this history"
        );
    }
}

impl<'a> GraphTxn<'a> {
    /// Push one entry onto the ledger, applying `LEDGER_CAP`'s drop-oldest-
    /// half policy (BUG A1 follow-up, 2026-08-12). Every production mutation
    /// (`add_node`/`remove_node`/`add_edge`/`remove_edge`/... below) records
    /// through here via `push_ledger_impl`, so the cap + drop-accounting
    /// policy lives in exactly one place.
    fn push_ledger(&self, entry: String) {
        push_ledger_impl(self.ledger, self.ledger_dropped_total, entry);
    }

    /// Test node membership through the already-held topology guard. Compound
    /// mutations use this to validate their complete state transition before the
    /// first row is changed, without a second (recursive) graph lock.
    pub fn has_node(&self, node_id: &str) -> bool {
        self.topo.node_map.contains_key(node_id)
    }

    /// Clone one resident node property blob without reacquiring the topology lock.
    /// Compound mutations use this accessor while this transaction already holds
    /// the graph's write guard.
    pub fn get_node_properties(&self, node_id: &str) -> Option<Vec<u8>> {
        self.node_properties
            .get(node_id)
            .map(|properties| (**properties).clone())
    }

    /// Node count from the already-held topology guard. Index maintenance uses
    /// this instead of recursively acquiring `GraphCore`'s topology lock.
    pub fn node_count(&self) -> usize {
        self.topo.graph.node_count()
    }

    /// Edge count from the already-held topology guard. See [`Self::node_count`].
    pub fn edge_count(&self) -> usize {
        self.topo.graph.edge_count()
    }

    // ── Node CRUD (under the held topology write guard) ──────────────────

    pub fn add_node(&mut self, node_id: String, properties_msgpack: Vec<u8>) {
        if !self.topo.node_map.contains_key(&node_id) {
            let new_idx = self.topo.graph.add_node(node_id.clone());
            self.topo.node_map.insert(node_id.clone(), new_idx);
        }
        let log = format!("ADD_NODE|{}|{}", node_id, HexLedger(&properties_msgpack));
        self.node_properties
            .insert(node_id.clone(), Arc::new(properties_msgpack));
        // CONCEPT:EG-KG.storage.bloom-negative-lookup-guard — record every id ever
        // added so a later durable-read-through miss can trust a bloom "no".
        self.node_bloom.read().insert(&node_id);
        self.push_ledger(log);
    }

    /// Insert a node only when its id is absent from the current topology.
    ///
    /// Membership testing and insertion share this transaction's topology write
    /// guard, so two writers racing on the same id cannot both observe absence.
    /// The losing writer leaves both the properties and ledger untouched.
    pub fn create_node_if_absent(&mut self, node_id: String, properties_msgpack: Vec<u8>) -> bool {
        if self.topo.node_map.contains_key(&node_id) {
            return false;
        }
        self.add_node(node_id, properties_msgpack);
        true
    }

    pub fn remove_node(&mut self, node_id: String) {
        if let Some(idx) = self.topo.node_map.get(&node_id).copied() {
            // Derive the exact endpoint pairs from petgraph's adjacency lists before
            // removing the node.  The old `DashMap::retain` walked EVERY edge pair
            // for every single-node delete (O(E)); adjacency makes the work
            // proportional to this node's degree.  A set collapses parallel edges
            // and the incoming/outgoing duplicate of a self-loop.
            let mut incident = std::collections::HashSet::new();
            for edge in self
                .topo
                .graph
                .edges_directed(idx, petgraph::Direction::Incoming)
                .chain(
                    self.topo
                        .graph
                        .edges_directed(idx, petgraph::Direction::Outgoing),
                )
            {
                incident.insert((
                    self.topo.graph[edge.source()].clone(),
                    self.topo.graph[edge.target()].clone(),
                ));
            }
            // Properties first, then topology: a crash mid-remove can never leave
            // a live node index whose properties already vanished (which on reload
            // would resurrect a half-deleted node). Topology is the source of truth.
            self.node_properties.remove(&node_id);
            for key in incident {
                self.edge_properties.remove(&key);
            }
            self.topo.node_map.remove(&node_id);
            self.topo.graph.remove_node(idx);
            self.push_ledger(format!("REMOVE_NODE|{}", node_id));
        }
    }

    /// Drop a set of nodes from the resident projection without recording logical
    /// delete mutations. This is the memory-pressure counterpart of `remove_node`:
    /// authoritative rows remain in the durable store and can be read through or
    /// re-materialized later. Incident edge properties are filtered once for the
    /// whole batch. Endpoint pairs come from the selected nodes' adjacency lists,
    /// avoiding both the old O(evictions × edges) repeated-delete behavior and
    /// an O(all edges) projection scan when only a small resident set is evicted.
    pub fn evict_resident_nodes(&mut self, node_ids: &[String]) -> usize {
        if node_ids.is_empty() {
            return 0;
        }
        let selected: std::collections::HashSet<&str> =
            node_ids.iter().map(String::as_str).collect();
        let selected_nodes: Vec<(&str, NodeIndex)> = selected
            .into_iter()
            .filter_map(|node_id| {
                self.topo
                    .node_map
                    .get(node_id)
                    .copied()
                    .map(|index| (node_id, index))
            })
            .collect();
        let mut incident = std::collections::HashSet::new();
        for (_, index) in &selected_nodes {
            for edge in self
                .topo
                .graph
                .edges_directed(*index, petgraph::Direction::Incoming)
                .chain(
                    self.topo
                        .graph
                        .edges_directed(*index, petgraph::Direction::Outgoing),
                )
            {
                incident.insert((
                    self.topo.graph[edge.source()].clone(),
                    self.topo.graph[edge.target()].clone(),
                ));
            }
        }
        for (node_id, index) in &selected_nodes {
            self.node_properties.remove(*node_id);
            self.topo.node_map.remove(*node_id);
            self.topo.graph.remove_node(*index);
        }
        for key in incident {
            self.edge_properties.remove(&key);
        }
        selected_nodes.len()
    }

    /// Serializable gated remove (CONCEPT:EG-KG.txn.serializable-mutation-gate). Decodes the node's CURRENT
    /// property blob to a row map UNDER the held write guard, re-evaluates
    /// `predicate`, and removes the node only if it still matches — so a compound
    /// `DELETE … WHERE <predicate>` cannot delete a row that a concurrent writer
    /// changed out from under the candidate-id scan. Returns whether it removed.
    /// A missing/undecodable node, or a predicate that no longer holds, is a no-op
    /// returning `false`.
    pub fn remove_node_if(&mut self, node_id: &str, predicate: &eg_types::RowPredicate) -> bool {
        let map = match self.node_row_map(node_id) {
            Some(m) => m,
            None => return false,
        };
        if !predicate.eval(&map) {
            return false;
        }
        self.remove_node(node_id.to_string());
        true
    }

    /// Decode a node's stored property blob into a `col -> value` row map for
    /// predicate evaluation (CONCEPT:EG-KG.txn.serializable-mutation-gate). The synthetic `id` column is injected
    /// (the blob stores only properties, not the node id) so a predicate may
    /// reference `id` alongside property columns. `None` if absent/undecodable.
    fn node_row_map(&self, node_id: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
        let bytes = self.node_properties.get(node_id)?.value().clone();
        let val = decode_property_value(&bytes).ok()?;
        let mut map = match val {
            serde_json::Value::Object(o) => o,
            _ => return None,
        };
        map.entry("id".to_string())
            .or_insert_with(|| serde_json::Value::String(node_id.to_string()));
        Some(map)
    }

    // ── Edge CRUD (under the held topology write guard) ──────────────────

    pub fn add_edge(
        &mut self,
        source_id: String,
        target_id: String,
        properties_msgpack: Vec<u8>,
    ) -> Result<(), String> {
        let source_idx = match self.topo.node_map.get(&source_id) {
            Some(&idx) => idx,
            None => return Err(edge_endpoint_not_found("Source", &source_id)),
        };
        let target_idx = match self.topo.node_map.get(&target_id) {
            Some(&idx) => idx,
            None => return Err(edge_endpoint_not_found("Target", &target_id)),
        };
        self.topo.graph.add_edge(
            source_idx,
            target_idx,
            format!("{}:{}", source_id, target_id),
        );
        let log = format!(
            "ADD_EDGE|{}|{}|{}",
            source_id,
            target_id,
            HexLedger(&properties_msgpack)
        );
        self.edge_properties
            .entry((source_id.clone(), target_id.clone()))
            .or_default()
            .push(Arc::new(properties_msgpack));
        self.push_ledger(log);
        Ok(())
    }

    pub fn remove_edge(&mut self, source_id: String, target_id: String) {
        if let (Some(&src_idx), Some(&tgt_idx)) = (
            self.topo.node_map.get(&source_id),
            self.topo.node_map.get(&target_id),
        ) {
            // Edge properties are stored per endpoint pair, so removing only one
            // topology edge while dropping the whole property vector left hidden
            // parallel edges behind. Pair removal (and `upsert_edge`) replaces all.
            let edge_indices: Vec<_> = self
                .topo
                .graph
                .edges_connecting(src_idx, tgt_idx)
                .map(|edge| edge.id())
                .collect();
            for edge_idx in edge_indices {
                self.topo.graph.remove_edge(edge_idx);
            }
            self.edge_properties
                .remove(&(source_id.clone(), target_id.clone()));
            self.push_ledger(format!("REMOVE_EDGE|{}|{}", source_id, target_id));
        }
    }

    /// Atomic compare-and-set on a node's property blob (CONCEPT:EG-KG.compute.backend backend-
    /// agnostic atomic claim). Runs entirely under the held topology write guard
    /// (decode → check → merge → re-encode → write), so the read-modify-write is
    /// atomic w.r.t. other writers. For every `(field, expected)` in `conditions`
    /// the node's current value must equal `expected`, treating a MISSING field as
    /// `null` (so a condition of `null` means "absent or null"). If ALL conditions
    /// hold, every `(field, value)` from `updates` is merged into the object and
    /// `true` is returned. If the node is absent, any condition fails, or the
    /// current blob fails to decode, the node is left untouched and `false` is
    /// returned.
    pub fn compare_and_set_fields(
        &mut self,
        node_id: &str,
        conditions: &serde_json::Map<String, serde_json::Value>,
        updates: &serde_json::Map<String, serde_json::Value>,
    ) -> bool {
        let bytes = match self.node_properties.get(node_id) {
            Some(b) => b.value().clone(),
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
        // Check every condition against the current value (missing reads as null).
        for (field, expected) in conditions {
            let current = obj.get(field).unwrap_or(&serde_json::Value::Null);
            if current != expected {
                return false;
            }
        }
        // All conditions held — merge updates and write the blob back.
        for (field, value) in updates {
            obj.insert(field.clone(), value.clone());
        }
        let reenc = match rmp_serde::to_vec_named(&val) {
            Ok(b) => b,
            Err(_) => return false,
        };
        let log = format!("CAS_NODE|{}|{}", node_id, HexLedger(&reenc));
        self.node_properties
            .insert(node_id.to_string(), Arc::new(reenc));
        self.push_ledger(log);
        true
    }

    /// Serializable gated compare-and-set (CONCEPT:EG-KG.txn.serializable-mutation-gate). Like
    /// [`GraphTxn::compare_and_set_fields`] but FIRST re-evaluates `predicate`
    /// against the node's CURRENT row (decoded under the held write guard, with the
    /// synthetic `id` column injected). If the predicate no longer holds the node is
    /// left untouched and `false` is returned — this is the serializable re-check for
    /// a compound `UPDATE … WHERE <predicate>` whose candidate ids were resolved by an
    /// earlier (lock-free) read. When the predicate holds, the usual `conditions`
    /// check + `updates` merge run atomically under the same guard.
    pub fn compare_and_set_fields_if(
        &mut self,
        node_id: &str,
        predicate: &eg_types::RowPredicate,
        conditions: &serde_json::Map<String, serde_json::Value>,
        updates: &serde_json::Map<String, serde_json::Value>,
    ) -> bool {
        match self.node_row_map(node_id) {
            Some(map) if predicate.eval(&map) => {}
            _ => return false,
        }
        self.compare_and_set_fields(node_id, conditions, updates)
    }

    /// Non-destructively CLOSE the temporal windows of a contradicted edge
    /// (CONCEPT:AU-KG.ingest.list-durable-media). Sets the matching edge's `valid_until = invalid_at`
    /// (event-time close) and `tx_to = tx_now` (belief retracted) — it does NOT
    /// remove the edge, so an `AS OF` before `invalid_at` still sees the fact and the
    /// `AS OF TX` history of what-we-believed is preserved. Matches the edge(s)
    /// between `(source_id, target_id)` whose canonical `relationship` field equals
    /// `relationship`
    /// and that are not already closed at or before `invalid_at`. Returns how many
    /// edge blobs were updated. Deterministic in its args, so it replays identically
    /// from the WAL / on a Raft follower.
    pub fn invalidate_edge(
        &mut self,
        source_id: &str,
        target_id: &str,
        relationship: &str,
        invalid_at: u64,
        tx_now: u64,
    ) -> usize {
        let key = (source_id.to_string(), target_id.to_string());
        let mut entry = match self.edge_properties.get_mut(&key) {
            Some(e) => e,
            None => return 0,
        };
        let mut updated = 0usize;
        for blob in entry.value_mut().iter_mut() {
            let Ok(mut val) = decode_property_value(blob.as_slice()) else {
                continue;
            };
            let Some(obj) = val.as_object_mut() else {
                continue;
            };
            let rel_matches =
                obj.get("relationship").and_then(|v| v.as_str()) == Some(relationship);
            if !rel_matches {
                continue;
            }
            // Skip an edge already closed at or before this instant (idempotent).
            let already_closed = obj
                .get("valid_until")
                .and_then(|v| v.as_u64())
                .is_some_and(|vu| vu <= invalid_at);
            if already_closed {
                continue;
            }
            obj.insert("valid_until".into(), serde_json::json!(invalid_at));
            obj.insert("tx_to".into(), serde_json::json!(tx_now));
            if let Ok(reenc) = rmp_serde::to_vec_named(&val) {
                *blob = Arc::new(reenc);
                updated += 1;
            }
        }
        if updated > 0 {
            self.push_ledger(format!(
                "INVALIDATE_EDGE|{}|{}|{}|{}|{}",
                source_id, target_id, relationship, invalid_at, tx_now
            ));
        }
        updated
    }

    /// Atomically SUPERSEDE a prior edge with a new one (CONCEPT:AU-KG.ingest.list-durable-media) under the
    /// single held write guard: close the prior edge's validity window
    /// (`valid_until = valid_at`, `tx_to = tx_now`) and insert the new edge — never
    /// deleting the prior, so the full history survives. The new edge's blob is
    /// supplied fully-formed by the caller (it should carry `valid_from = valid_at`
    /// and a `supersedes` provenance pointer). Returns `Ok(())` once the new edge is
    /// added (endpoints must exist), after invalidating the prior.
    // The nine args are three irreducible groups of distinct primitives — the new
    // edge (source, target, properties), the prior edge to close (source, target,
    // relationship) and the two bitemporal timestamps (valid_at, tx_now). Bundling
    // them into a struct would add ceremony at every call site without making any
    // group clearer, so a scoped allow is the right call here.
    #[allow(clippy::too_many_arguments)]
    pub fn supersede_edge(
        &mut self,
        new_source: String,
        new_target: String,
        new_properties_msgpack: Vec<u8>,
        prior_source: &str,
        prior_target: &str,
        prior_relationship: &str,
        valid_at: u64,
        tx_now: u64,
    ) -> Result<(), String> {
        self.invalidate_edge(
            prior_source,
            prior_target,
            prior_relationship,
            valid_at,
            tx_now,
        );
        self.add_edge(new_source, new_target, new_properties_msgpack)
    }

    // ── Agent-native memory primitives (CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg / EG-221) ─────────────
    //
    // The DETERMINISTIC engine substrate for the agent-native memory tier from the
    // "Are We Ready For An Agent-Native Memory System?" survey (arXiv 2606.24775).
    // The memory LIFECYCLE — deciding *when* to summarize/consolidate, and the
    // LLM-produced summary TEXT — lives in agent-utilities; the engine only stores
    // + links what AU hands it. No clock/RNG is read internally: any node id is
    // derived deterministically from the (sorted) inputs, and any timestamp is
    // taken from the caller-supplied property blobs. Every method here runs under
    // the single held topology write guard, so it is atomic w.r.t. other writers
    // and replays identically from the WAL / on a Raft follower.

    /// Decode a node's stored property blob into its raw property object (no
    /// synthetic `id` injection, unlike [`GraphTxn::node_row_map`]). `None` if the
    /// node is absent or its blob is not a decodable object. Read under the held
    /// write guard so the read-modify-write stays atomic (CONCEPT:EG-KG.compute.consolidate-cluster).
    fn node_object(&self, node_id: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
        let bytes = self.node_properties.get(node_id)?.value().clone();
        match decode_property_value(&bytes) {
            Ok(serde_json::Value::Object(o)) => Some(o),
            _ => None,
        }
    }

    /// Unconditionally merge `updates` into a node's property object under the held
    /// write guard (CONCEPT:EG-KG.compute.consolidate-cluster). Like [`GraphTxn::compare_and_set_fields`] with
    /// no conditions. A missing/undecodable node is a no-op returning `false`.
    fn merge_fields(
        &mut self,
        node_id: &str,
        updates: &serde_json::Map<String, serde_json::Value>,
    ) -> bool {
        let empty = serde_json::Map::new();
        self.compare_and_set_fields(node_id, &empty, updates)
    }

    /// Does a `source → target` edge carrying `relationship` already exist under
    /// the held guard? Used to keep provenance-edge creation idempotent so a
    /// re-run of `create_summary_node` / `consolidate` does not add parallel edges.
    fn has_relationship_edge(&self, source_id: &str, target_id: &str, relationship: &str) -> bool {
        self.edge_properties
            .get(&(source_id.to_string(), target_id.to_string()))
            .is_some_and(|blobs| {
                blobs.iter().any(|b| {
                    decode_property_value(b)
                        .ok()
                        .and_then(|v| {
                            v.as_object()
                                .and_then(|o| o.get("relationship"))
                                .and_then(|r| r.as_str())
                                .map(|s| s == relationship)
                        })
                        .unwrap_or(false)
                })
            })
    }

    /// Deterministic id for a summary node over `(level, sorted child ids)`
    /// (CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg). Same inputs ⇒ same id, so re-summarizing a cluster at a
    /// level UPSERTS the same node rather than spawning a duplicate. No RNG.
    fn derive_summary_id(level: u32, sorted_children: &[String]) -> String {
        use std::hash::{Hash, Hasher};
        // `DefaultHasher::new()` is SipHash with FIXED (zero) keys — deterministic
        // across processes (unlike `RandomState`), so the id replays identically.
        let mut h = std::collections::hash_map::DefaultHasher::new();
        level.hash(&mut h);
        for c in sorted_children {
            c.hash(&mut h);
        }
        format!("summary:L{}:{:016x}", level, h.finish())
    }

    /// Deterministic id for a consolidated semantic node over its `(sorted episodic
    /// ids)` cluster (CONCEPT:EG-KG.compute.consolidate-cluster). No RNG.
    fn derive_semantic_id(sorted_cluster: &[String]) -> String {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        for c in sorted_cluster {
            c.hash(&mut h);
        }
        format!("semantic:{:016x}", h.finish())
    }

    /// CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg — create (or UPSERT) a hierarchical summary node at abstraction
    /// `level`, linked to each of `child_ids` via a `SUMMARIZES` provenance edge, so a
    /// multi-level abstraction ladder can be built (a level-2 summary's children may
    /// themselves be level-1 summary nodes). The LLM-produced summary TEXT is passed
    /// in `props` by the caller (AU); the engine stores it verbatim and only injects
    /// the structural markers `type = "SummaryNode"`, `summary_level`, and
    /// `summary_child_count`. If `props` carries an `id` string it is honoured (and
    /// stripped from the stored blob, since a node id is not a property); otherwise a
    /// deterministic id over `(level, sorted children)` is used. Returns the id.
    ///
    /// Deterministic + idempotent: re-running with the same inputs upserts the same
    /// node and does NOT duplicate the `SUMMARIZES` edges. Children that do not exist
    /// are skipped (no dangling edges). Runs under the held write guard.
    pub fn create_summary_node(
        &mut self,
        level: u32,
        child_ids: &[String],
        props: serde_json::Map<String, serde_json::Value>,
    ) -> String {
        // Canonicalize children (sorted + deduped) for deterministic id + ledger.
        let mut children: Vec<String> = child_ids.to_vec();
        children.sort();
        children.dedup();

        let id = match props.get("id").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => Self::derive_summary_id(level, &children),
        };

        // Merge onto any existing node object (upsert) then apply caller props +
        // structural markers.
        let mut obj = self.node_object(&id).unwrap_or_default();
        for (k, v) in props {
            if k == "id" {
                continue; // the node id, not a stored property
            }
            obj.insert(k, v);
        }
        obj.entry("type".to_string())
            .or_insert_with(|| serde_json::json!("SummaryNode"));
        obj.insert("summary_level".to_string(), serde_json::json!(level));
        obj.insert(
            "summary_child_count".to_string(),
            serde_json::json!(children.len()),
        );
        if let Ok(blob) = rmp_serde::to_vec_named(&serde_json::Value::Object(obj)) {
            self.add_node(id.clone(), blob);
        }

        // Link to existing children via idempotent `SUMMARIZES` provenance edges.
        for child in &children {
            if child == &id {
                continue; // never summarize self
            }
            if !self.topo.node_map.contains_key(child) {
                continue; // skip absent child — no dangling edge
            }
            if self.has_relationship_edge(&id, child, "SUMMARIZES") {
                continue; // already linked (idempotent re-run)
            }
            if let Ok(eprops) =
                rmp_serde::to_vec_named(&serde_json::json!({"relationship": "SUMMARIZES"}))
            {
                let _ = self.add_edge(id.clone(), child.clone(), eprops);
            }
        }
        id
    }

    /// CONCEPT:EG-KG.compute.consolidate-cluster — consolidate a cluster of `episodic_ids` memory nodes into ONE
    /// semantic node (episodic → semantic consolidation), the paper's key
    /// LOCALIZED-maintenance finding: nothing outside the cluster + its immediate
    /// neighbours is touched, and there is NO global reindex.
    ///
    /// What it does, all atomically under the held write guard:
    /// * Create/UPDATE the semantic node with the caller-merged `semantic_props`
    ///   (AU/LLM produce the merged content; the engine stores it). `type` defaults
    ///   to `"SemanticMemory"`; `consolidated_from_count` is recorded. If
    ///   `semantic_props` carries an `id` it is honoured, else a deterministic id
    ///   over the sorted cluster is used.
    /// * Preserve BITEMPORAL validity: the consolidated node's `tx_from` spans the
    ///   MIN of the children's `tx_from`, and `tx_to` the MAX of the children's
    ///   `tx_to` — UNLESS any child is still open (no `tx_to`), in which case the
    ///   consolidated node is left open too. Caller-supplied `tx_from`/`tx_to`
    ///   override the computed span.
    /// * COPY each episodic's EXTERNAL edges (endpoint outside the cluster) onto the
    ///   semantic node, re-pointed to it — so the semantic node inherits the
    ///   cluster's connectivity. Intra-cluster edges are subsumed (skipped). The
    ///   originals are preserved on the episodics (provenance/history intact).
    /// * Add a `CONSOLIDATES` provenance edge semantic → each episodic.
    /// * Mark each episodic `consolidated = true` + `consolidated_into = <id>`
    ///   WITHOUT deleting it (bitemporal history preserved).
    ///
    /// Deterministic (inputs sorted; copied edges collected + sorted before apply)
    /// and idempotent (provenance edges guarded). Returns the semantic node id.
    pub fn consolidate(
        &mut self,
        episodic_ids: &[String],
        semantic_props: serde_json::Map<String, serde_json::Value>,
    ) -> String {
        // Canonical cluster (sorted + deduped) for a deterministic id + ledger.
        let mut cluster: Vec<String> = episodic_ids.to_vec();
        cluster.sort();
        cluster.dedup();
        let cluster_set: std::collections::HashSet<&str> =
            cluster.iter().map(|s| s.as_str()).collect();

        let semantic_id = match semantic_props.get("id").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => Self::derive_semantic_id(&cluster),
        };

        // Bitemporal span over the children (CONCEPT:EG-KG.compute.preserved/2.250 preserved).
        let mut tx_from_min: Option<u64> = None;
        let mut tx_to_max: Option<u64> = None;
        let mut any_open = false;
        for epi in &cluster {
            if let Some(obj) = self.node_object(epi) {
                if let Some(f) = obj.get("tx_from").and_then(|v| v.as_u64()) {
                    tx_from_min = Some(tx_from_min.map_or(f, |m| m.min(f)));
                }
                match obj.get("tx_to").and_then(|v| v.as_u64()) {
                    Some(t) => tx_to_max = Some(tx_to_max.map_or(t, |m| m.max(t))),
                    None => any_open = true, // an open child ⇒ the span stays open
                }
            }
        }

        // Build the semantic node object: merge onto any existing node (upsert),
        // then caller props, then computed markers (caller values win).
        let mut obj = self.node_object(&semantic_id).unwrap_or_default();
        for (k, v) in semantic_props {
            if k == "id" {
                continue;
            }
            obj.insert(k, v);
        }
        obj.entry("type".to_string())
            .or_insert_with(|| serde_json::json!("SemanticMemory"));
        obj.insert(
            "consolidated_from_count".to_string(),
            serde_json::json!(cluster.len()),
        );
        if !obj.contains_key("tx_from") {
            if let Some(f) = tx_from_min {
                obj.insert("tx_from".to_string(), serde_json::json!(f));
            }
        }
        if !obj.contains_key("tx_to") && !any_open {
            if let Some(t) = tx_to_max {
                obj.insert("tx_to".to_string(), serde_json::json!(t));
            }
        }
        if let Ok(blob) = rmp_serde::to_vec_named(&serde_json::Value::Object(obj)) {
            self.add_node(semantic_id.clone(), blob);
        }

        // Collect EXTERNAL edges to copy onto the semantic node. Gather first (no
        // mutation during the DashMap scan), then sort + dedup for a deterministic
        // ledger, then apply after the iterator guard drops.
        let mut to_add: Vec<(String, String, Vec<u8>)> = Vec::new();
        for entry in self.edge_properties.iter() {
            let (src, tgt) = entry.key();
            let src_in = cluster_set.contains(src.as_str());
            let tgt_in = cluster_set.contains(tgt.as_str());
            if src_in == tgt_in {
                // both in-cluster (subsumed) or both external (unrelated) — skip.
                continue;
            }
            for blob in entry.value() {
                if src_in {
                    if tgt != &semantic_id {
                        to_add.push((semantic_id.clone(), tgt.clone(), (**blob).clone()));
                    }
                } else if src != &semantic_id {
                    to_add.push((src.clone(), semantic_id.clone(), (**blob).clone()));
                }
            }
        }
        to_add.sort();
        to_add.dedup();
        for (src, tgt, blob) in to_add {
            // Skip if an identical-relationship edge already connects these (keeps a
            // re-run from stacking duplicate redirected edges).
            let rel = decode_property_value(&blob).ok().and_then(|v| {
                v.as_object()
                    .and_then(|o| o.get("relationship"))
                    .and_then(|r| r.as_str())
                    .map(|s| s.to_string())
            });
            if let Some(r) = &rel {
                if self.has_relationship_edge(&src, &tgt, r) {
                    continue;
                }
            }
            let _ = self.add_edge(src, tgt, blob);
        }

        // Provenance + mark episodics consolidated (localized — cluster only).
        for epi in &cluster {
            if epi == &semantic_id || !self.topo.node_map.contains_key(epi) {
                continue;
            }
            if !self.has_relationship_edge(&semantic_id, epi, "CONSOLIDATES") {
                if let Ok(eprops) =
                    rmp_serde::to_vec_named(&serde_json::json!({"relationship": "CONSOLIDATES"}))
                {
                    let _ = self.add_edge(semantic_id.clone(), epi.clone(), eprops);
                }
            }
            let mut mark = serde_json::Map::new();
            mark.insert("consolidated".to_string(), serde_json::json!(true));
            mark.insert(
                "consolidated_into".to_string(),
                serde_json::json!(semantic_id),
            );
            self.merge_fields(epi, &mark);
        }
        semantic_id
    }

    // ── Memory maintenance — decay + reinforcement (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive) ────────────
    //
    // Module 4 of the agent-native memory tier ("Are We Ready For An Agent-Native
    // Memory System?", arXiv 2606.24775): the paper's finding that "localized
    // maintenance is more cost-efficient than global reorganization". The caller (the
    // AU maintenance loop) picks a WORKING SET of memory ids; the engine only touches
    // those nodes — there is NO global scan or reindex. No clock/RNG is read
    // internally: `now_ms`, `half_life_ms`, `weight`, and `threshold` are all supplied
    // by the caller, so a run replays identically from the WAL / on a Raft follower.
    // Every method runs under the single held topology write guard, so it is atomic
    // w.r.t. other writers.
    //
    // A memory node carries these memory-value fields in its property blob:
    //   * `importance`     (f64) — retrieval-priority weight; decays over time and is
    //                              boosted on access.
    //   * `access_count`   (u64) — how many times it has been reinforced (retrieved).
    //   * `last_access_ms` (u64) — wall-clock of the last reinforcement (recency).
    //   * `last_decay_ms`  (u64) — decay's OWN clock, so a maintenance sweep never
    //                              masquerades as an access.
    //   * `forgotten`      (bool)— set true when evicted via the mark path (provenance
    //                              preserved; the node + its edges + bitemporal history
    //                              stay intact).
    // Every memory participating in maintenance carries an explicit finite
    // `importance`. Missing or non-numeric values are not interpreted as an older
    // schema: that node is outside the maintenance operation and must be migrated by
    // its writer before reinforcement, decay, or eviction.

    /// Read the mandatory current-schema `importance` value.
    fn memory_importance(obj: &serde_json::Map<String, serde_json::Value>) -> Option<f64> {
        obj.get("importance")
            .and_then(|v| v.as_f64())
            .filter(|importance| importance.is_finite())
    }

    /// CONCEPT:EG-KG.maintenance.combined-maintenance-primitive — REINFORCE a memory on retrieval: bump `access_count`, refresh
    /// `last_access_ms` to `now_ms` (recency), and raise `importance` by `weight`
    /// (retrieval strengthens a memory). The current memory schema requires an explicit
    /// finite `importance`; a node without it is rejected from this operation. A memory
    /// previously marked `forgotten` is REVIVED (`forgotten = false`) — a
    /// re-accessed memory is live again.
    ///
    /// Deterministic (no clock/RNG — `now_ms`/`weight` are caller-supplied). This is an
    /// accumulator, so it is intentionally NOT idempotent: each call models a distinct
    /// retrieval event. A missing/undecodable node is a no-op returning `false`. Runs
    /// under the held write guard.
    pub fn reinforce(&mut self, id: &str, now_ms: u64, weight: f64) -> bool {
        let Some(obj) = self.node_object(id) else {
            return false;
        };
        let Some(current_importance) = Self::memory_importance(&obj) else {
            return false;
        };
        let importance = current_importance + weight;
        let access_count = obj
            .get("access_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
            + 1;
        let mut updates = serde_json::Map::new();
        updates.insert("importance".to_string(), serde_json::json!(importance));
        updates.insert("access_count".to_string(), serde_json::json!(access_count));
        updates.insert("last_access_ms".to_string(), serde_json::json!(now_ms));
        if obj.get("forgotten").and_then(|v| v.as_bool()) == Some(true) {
            updates.insert("forgotten".to_string(), serde_json::json!(false));
        }
        self.merge_fields(id, &updates)
    }

    /// CONCEPT:EG-KG.maintenance.combined-maintenance-primitive — apply time-based EXPONENTIAL decay to a memory's `importance`
    /// from the time elapsed since it was last touched: `importance *= 0.5 ^ (elapsed /
    /// half_life_ms)`, i.e. importance halves every `half_life_ms` of inactivity. The
    /// elapsed clock is measured from `last_decay_ms` if present, else `last_access_ms`
    /// (recency); the method advances `last_decay_ms` — NOT `last_access_ms` — so a
    /// maintenance sweep never masquerades as an access.
    ///
    /// A node carrying no current-schema `importance` field is rejected from the
    /// maintenance operation (returns `false`). A node with importance but no decay reference just stamps `last_decay_ms = now_ms` (a
    /// baseline; no decay this pass). Deterministic (no clock/RNG) and IDEMPOTENT at a
    /// fixed `now_ms`: because it advances `last_decay_ms` to `now_ms`, a second call
    /// with the same `now_ms` sees zero elapsed and leaves importance unchanged; and
    /// successive passes compose exactly (`0.5^(Δ₁/h)·0.5^(Δ₂/h) = 0.5^((Δ₁+Δ₂)/h)`).
    /// `half_life_ms == 0` is treated as no-decay (only the baseline stamp). Runs under
    /// the held write guard.
    pub fn decay_node(&mut self, id: &str, now_ms: u64, half_life_ms: u64) -> bool {
        let Some(obj) = self.node_object(id) else {
            return false;
        };
        // Untouched: no memory-value `importance` ⇒ not a decayable memory node.
        let Some(importance) = obj.get("importance").and_then(|v| v.as_f64()) else {
            return false;
        };
        let reference = obj
            .get("last_decay_ms")
            .and_then(|v| v.as_u64())
            .or_else(|| obj.get("last_access_ms").and_then(|v| v.as_u64()));
        let mut updates = serde_json::Map::new();
        if let Some(ref_ms) = reference {
            let elapsed = now_ms.saturating_sub(ref_ms);
            if elapsed > 0 && half_life_ms > 0 {
                let factor = 0.5_f64.powf(elapsed as f64 / half_life_ms as f64);
                updates.insert(
                    "importance".to_string(),
                    serde_json::json!(importance * factor),
                );
            }
        }
        // Advance decay's own clock — makes the pass idempotent + composable.
        updates.insert("last_decay_ms".to_string(), serde_json::json!(now_ms));
        self.merge_fields(id, &updates)
    }

    /// CONCEPT:EG-KG.maintenance.combined-maintenance-primitive — batch [`GraphTxn::decay_node`] over a caller-supplied working
    /// set `ids` (LOCALIZED — the caller picks the set; the engine never scans the
    /// whole store). Returns the number of nodes actually decayed/stamped. The effect
    /// is order-independent (each node is decayed against its own clock). Runs under
    /// the held write guard.
    pub fn decay_memories(&mut self, now_ms: u64, half_life_ms: u64, ids: &[String]) -> usize {
        let mut n = 0;
        for id in ids {
            if self.decay_node(id, now_ms, half_life_ms) {
                n += 1;
            }
        }
        n
    }

    /// CONCEPT:EG-KG.maintenance.combined-maintenance-primitive — FORGET a single memory `id` locally. With `delete == false`
    /// (the default, provenance-preserving path) the node is MARKED `forgotten = true`
    /// — its bitemporal history + edges stay intact for audit / recall-on-reinforce.
    /// With `delete == true` the node (and its edges) are hard-removed. A missing node
    /// is a no-op returning `false`. Deterministic; marking is idempotent. Runs under
    /// the held write guard.
    pub fn forget(&mut self, id: &str, delete: bool) -> bool {
        if delete {
            if !self.topo.node_map.contains_key(id) {
                return false;
            }
            self.remove_node(id.to_string());
            true
        } else {
            let mut updates = serde_json::Map::new();
            updates.insert("forgotten".to_string(), serde_json::json!(true));
            self.merge_fields(id, &updates)
        }
    }

    /// CONCEPT:EG-KG.maintenance.combined-maintenance-primitive — EVICT every memory in the working set `ids` whose (already-
    /// decayed) `importance` has fallen strictly BELOW `threshold`, pruning it via
    /// [`GraphTxn::forget`] (mark when `delete == false`, hard-remove when `true`). A
    /// node carrying no current-schema `importance` is skipped rather than assigned a
    /// synthetic score. A node already marked `forgotten = true` is skipped (not re-pruned). LOCALIZED (only
    /// `ids` are considered — no full scan) and deterministic; returns the pruned ids,
    /// sorted + deduped. Callers typically [`GraphTxn::decay_memories`] first. Runs
    /// under the held write guard.
    pub fn evict_below(&mut self, ids: &[String], threshold: f64, delete: bool) -> Vec<String> {
        let mut pruned: Vec<String> = Vec::new();
        for id in ids {
            let Some(obj) = self.node_object(id) else {
                continue; // absent/undecodable — nothing to evict.
            };
            if obj.get("forgotten").and_then(|v| v.as_bool()) == Some(true) {
                continue; // already forgotten — idempotent skip.
            }
            if Self::memory_importance(&obj).is_some_and(|importance| importance < threshold)
                && self.forget(id, delete)
            {
                pruned.push(id.clone());
            }
        }
        pruned.sort();
        pruned.dedup();
        pruned
    }

    /// CONCEPT:EG-KG.maintenance.combined-maintenance-primitive — the combined maintenance primitive the AU maintenance loop
    /// schedules over a working set: DECAY every node in `ids` (against its own clock),
    /// then EVICT those that fell below `evict_threshold`. LOCALIZED (only `ids`),
    /// atomic (one held write guard), deterministic + idempotent at a fixed `now_ms`.
    /// `delete` selects mark (`false`, the default, provenance-preserving) vs
    /// hard-remove (`true`) for eviction. Returns `(decayed_count, pruned_ids)`. Runs
    /// under the held write guard.
    pub fn maintain(
        &mut self,
        ids: &[String],
        now_ms: u64,
        half_life_ms: u64,
        evict_threshold: f64,
        delete: bool,
    ) -> (usize, Vec<String>) {
        let decayed = self.decay_memories(now_ms, half_life_ms, ids);
        let pruned = self.evict_below(ids, evict_threshold, delete);
        (decayed, pruned)
    }

    // ── Scene-graph / 3D world model (CONCEPT:EG-KG.compute.scene-graph-primitives) ─────────────────────────
    //
    // A `:SceneObject` node carries a local `pose` (translation + rotation quaternion
    // + scale) in its property blob; a parent/child transform hierarchy is expressed
    // with reciprocal typed edges — child →`CHILD_OF`→ parent and parent →`HAS_CHILD`
    // → child — so the world transform is the composition of local poses up the
    // parent chain. Optional spatial relations (`ON`/`IN`/`NEAR`/`SUPPORTS`) and an
    // axis-aligned bounding volume attach with the same edge/property conventions the
    // memory primitives use. Deterministic (ids derived from graph state + inputs, no
    // clock/RNG) and atomic under the held write guard, so it replays identically.

    /// Deterministic id for a new scene object over `(sequence, parent, pose)`
    /// (CONCEPT:EG-KG.compute.scene-graph-primitives). `sequence` is the live node count at insertion time, which
    /// is monotonic under replay, so identical WAL replay yields the identical id
    /// while distinct inserts never collide. No RNG/clock.
    fn derive_scene_id(sequence: usize, parent: Option<&str>, pose: &crate::scene::Pose) -> String {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        sequence.hash(&mut h);
        parent.unwrap_or("").hash(&mut h);
        // Hash the canonical pose JSON string so the seed depends on the pose too.
        pose.to_json().to_string().hash(&mut h);
        format!("scene:{:016x}", h.finish())
    }

    /// Write the reciprocal parent/child transform edges (child `CHILD_OF` parent +
    /// parent `HAS_CHILD` child) idempotently, skipping an absent parent so no
    /// dangling edge is created. Runs under the held write guard.
    fn link_parent(&mut self, child: &str, parent: &str) {
        if child == parent || !self.topo.node_map.contains_key(parent) {
            return;
        }
        if !self.has_relationship_edge(child, parent, "CHILD_OF") {
            if let Ok(e) = rmp_serde::to_vec_named(&serde_json::json!({"relationship": "CHILD_OF"}))
            {
                let _ = self.add_edge(child.to_string(), parent.to_string(), e);
            }
        }
        if !self.has_relationship_edge(parent, child, "HAS_CHILD") {
            if let Ok(e) =
                rmp_serde::to_vec_named(&serde_json::json!({"relationship": "HAS_CHILD"}))
            {
                let _ = self.add_edge(parent.to_string(), child.to_string(), e);
            }
        }
    }

    /// The current transform parent of `child`: the target of its outgoing `CHILD_OF`
    /// edge, if any. Read under the held write guard.
    fn transform_parent(&self, child: &str) -> Option<String> {
        let &idx = self.topo.node_map.get(child)?;
        let targets: Vec<String> = self
            .topo
            .graph
            .edges_directed(idx, petgraph::Direction::Outgoing)
            .map(|e| self.topo.graph[e.target()].clone())
            .collect();
        targets
            .into_iter()
            .find(|t| self.has_relationship_edge(child, t, "CHILD_OF"))
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — create a `:SceneObject` node with local `pose`, optionally
    /// parented under `parent` via reciprocal `CHILD_OF`/`HAS_CHILD` edges (an absent
    /// parent is skipped — no dangling edge). Returns the new object's deterministic
    /// id. Runs under the held write guard.
    pub fn add_scene_object(&mut self, pose: &crate::scene::Pose, parent: Option<&str>) -> String {
        let id = Self::derive_scene_id(self.topo.node_map.len(), parent, pose);
        let obj = serde_json::json!({ "type": "SceneObject", "pose": pose.to_json() });
        if let Ok(blob) = rmp_serde::to_vec_named(&obj) {
            self.add_node(id.clone(), blob);
        }
        if let Some(p) = parent {
            self.link_parent(&id, p);
        }
        id
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — replace the local `pose` of scene object `id`. No-op
    /// returning `false` if the node is absent. Runs under the held write guard.
    pub fn set_pose(&mut self, id: &str, pose: &crate::scene::Pose) -> bool {
        let mut update = serde_json::Map::new();
        update.insert("pose".to_string(), pose.to_json());
        self.merge_fields(id, &update)
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — reparent scene object `id` under `new_parent` (or detach to a
    /// root with `None`): drops the existing reciprocal transform edges to the old
    /// parent and links the new one, so `world_transform` recomposes down the new
    /// chain. No-op returning `false` if `id` is absent or `new_parent` is missing.
    /// Runs under the held write guard.
    pub fn reparent(&mut self, id: &str, new_parent: Option<&str>) -> bool {
        if !self.topo.node_map.contains_key(id) {
            return false;
        }
        if let Some(np) = new_parent {
            if np == id || !self.topo.node_map.contains_key(np) {
                return false;
            }
        }
        if let Some(old) = self.transform_parent(id) {
            self.remove_edge(id.to_string(), old.clone());
            self.remove_edge(old, id.to_string());
        }
        if let Some(np) = new_parent {
            self.link_parent(id, np);
        }
        true
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — add a spatial relationship edge `from →<rel>→ to` (e.g.
    /// `ON`/`IN`/`NEAR`/`SUPPORTS`), idempotently. Returns `false` if either endpoint
    /// is absent or the same edge already exists. Runs under the held write guard.
    pub fn add_spatial_relation(&mut self, from: &str, to: &str, rel: &str) -> bool {
        if from == to
            || !self.topo.node_map.contains_key(from)
            || !self.topo.node_map.contains_key(to)
            || self.has_relationship_edge(from, to, rel)
        {
            return false;
        }
        match rmp_serde::to_vec_named(&serde_json::json!({"relationship": rel})) {
            Ok(e) => self.add_edge(from.to_string(), to.to_string(), e).is_ok(),
            Err(_) => false,
        }
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — attach (or replace) the axis-aligned bounding volume of scene
    /// object `id`. No-op returning `false` if the node is absent. Runs under the
    /// held write guard.
    pub fn set_bounding_volume(&mut self, id: &str, aabb: &crate::scene::Aabb) -> bool {
        let mut update = serde_json::Map::new();
        update.insert("aabb".to_string(), aabb.to_json());
        self.merge_fields(id, &update)
    }

    // ── Action / policy / trajectory memory (CONCEPT:EG-KG.compute.discounted-return) ──────────────────
    //
    // Episodic TRAJECTORY memory for agents / robotics (the Phase-P counterpart to
    // the EG-220/221/222 memory tier and the EG-087 scene model). A `:Trajectory`
    // node heads an ordered chain of `:Step` nodes, each carrying `action`, `reward`,
    // `t` (the caller's timestep), and optional `state_ref` / `next_state_ref` that
    // reference EG-087 scene-object / state nodes. Steps are stitched into a temporal
    // chain with `NEXT_STEP` edges (tail → new), tied back to their trajectory with a
    // `BELONGS_TO` edge (step → trajectory), and made cheaply enumerable with a
    // reciprocal `HAS_STEP` edge (trajectory → step) — exactly the reciprocal-edge
    // convention EG-087 uses for `CHILD_OF`/`HAS_CHILD`. The trajectory node tracks
    // `step_count`, `head_step`, and `tail_step` so an append is O(1).
    //
    // Deterministic + atomic like the rest of the tier: the caller supplies every
    // number (`reward`, `t`, and the analysis `gamma`) — the engine reads NO clock and
    // NO RNG, so a run replays identically from the WAL / on a Raft follower. Ids are
    // derived from graph state + inputs. Everything below runs under the single held
    // topology write guard, so a `start_trajectory` / `append_step` is atomic w.r.t.
    // other writers. Backward-compatible: a `:Step`/`:Trajectory` is an ordinary node,
    // and the analysis helpers read only nodes reachable from the given trajectory.

    /// Deterministic id for a new trajectory over `(sequence, props)`
    /// (CONCEPT:EG-KG.compute.discounted-return). `sequence` is the live node count at insertion time, which is
    /// monotonic under replay, so identical WAL replay yields the identical id while
    /// distinct inserts never collide. No RNG/clock.
    fn derive_trajectory_id(
        sequence: usize,
        props: &serde_json::Map<String, serde_json::Value>,
    ) -> String {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        sequence.hash(&mut h);
        serde_json::Value::Object(props.clone())
            .to_string()
            .hash(&mut h);
        format!("trajectory:{:016x}", h.finish())
    }

    /// Deterministic id for the `step_index`-th step of trajectory `traj_id`
    /// (CONCEPT:EG-KG.compute.discounted-return): `"<traj_id>:step:<index>"`. Fully replayable (a function of
    /// the trajectory id + the monotonic step ordinal), unique within a trajectory,
    /// and it sorts lexicographically in append order. No RNG/clock.
    fn derive_step_id(traj_id: &str, step_index: u64) -> String {
        format!("{}:step:{:08}", traj_id, step_index)
    }

    /// CONCEPT:EG-KG.compute.discounted-return — START a new `:Trajectory` (episode) and return its id. The
    /// caller-supplied `props` are stored verbatim; the engine injects the structural
    /// markers `type = "Trajectory"` and initializes `step_count = 0` (only when
    /// absent, so a re-run UPSERTS without resetting an in-progress episode). If
    /// `props` carries an `id` string it is honoured (and stripped from the stored
    /// blob); otherwise a deterministic id over `(live node count, props)` is used.
    /// Runs under the held write guard.
    pub fn start_trajectory(
        &mut self,
        props: serde_json::Map<String, serde_json::Value>,
    ) -> String {
        let id = match props.get("id").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => Self::derive_trajectory_id(self.topo.node_map.len(), &props),
        };
        let mut obj = self.node_object(&id).unwrap_or_default();
        for (k, v) in props {
            if k == "id" {
                continue; // the node id, not a stored property
            }
            obj.insert(k, v);
        }
        obj.entry("type".to_string())
            .or_insert_with(|| serde_json::json!("Trajectory"));
        obj.entry("step_count".to_string())
            .or_insert_with(|| serde_json::json!(0));
        if let Ok(blob) = rmp_serde::to_vec_named(&serde_json::Value::Object(obj)) {
            self.add_node(id.clone(), blob);
        }
        id
    }

    /// CONCEPT:EG-KG.compute.discounted-return — APPEND a `:Step{ action, reward, t, state_ref, next_state_ref }`
    /// to trajectory `traj_id`, extending its ordered temporal chain. The new step is:
    /// * created with the caller's `action` (stored verbatim — a string or a
    ///   structured JSON action), `reward`, `t` (the caller's timestep), its ordinal
    ///   `step_index`, and any `state_ref` / `next_state_ref` (references to EG-087
    ///   scene / state nodes; omitted when `None`);
    /// * linked to its trajectory with a `BELONGS_TO` edge (step → trajectory) plus a
    ///   reciprocal `HAS_STEP` edge (trajectory → step) for O(matches) enumeration;
    /// * stitched onto the chain tail with a `NEXT_STEP` edge (previous tail → new
    ///   step); the first step also stamps the trajectory's `head_step`.
    ///
    /// The trajectory's `tail_step` and `step_count` are advanced. Returns the new
    /// step id, or `None` if `traj_id` is absent/undecodable (no partial write).
    /// Deterministic (no clock/RNG — `reward`/`t` are caller-supplied) and atomic under
    /// the held write guard. It is an accumulator (each call is a distinct step), so it
    /// is intentionally NOT idempotent.
    #[allow(clippy::too_many_arguments)]
    pub fn append_step(
        &mut self,
        traj_id: &str,
        action: serde_json::Value,
        reward: f64,
        state_ref: Option<&str>,
        next_state_ref: Option<&str>,
        t: u64,
    ) -> Option<String> {
        let traj = self.node_object(traj_id)?;
        let step_index = traj.get("step_count").and_then(|v| v.as_u64()).unwrap_or(0);
        let tail = traj
            .get("tail_step")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let step_id = Self::derive_step_id(traj_id, step_index);

        // Build + write the step node.
        let mut step = serde_json::Map::new();
        step.insert("type".to_string(), serde_json::json!("Step"));
        step.insert("action".to_string(), action);
        step.insert("reward".to_string(), serde_json::json!(reward));
        step.insert("t".to_string(), serde_json::json!(t));
        step.insert("step_index".to_string(), serde_json::json!(step_index));
        if let Some(s) = state_ref {
            step.insert("state_ref".to_string(), serde_json::json!(s));
        }
        if let Some(s) = next_state_ref {
            step.insert("next_state_ref".to_string(), serde_json::json!(s));
        }
        if let Ok(blob) = rmp_serde::to_vec_named(&serde_json::Value::Object(step)) {
            self.add_node(step_id.clone(), blob);
        }

        // BELONGS_TO (step → trajectory) + reciprocal HAS_STEP (trajectory → step).
        if let Ok(e) = rmp_serde::to_vec_named(&serde_json::json!({"relationship": "BELONGS_TO"})) {
            let _ = self.add_edge(step_id.clone(), traj_id.to_string(), e);
        }
        if let Ok(e) = rmp_serde::to_vec_named(&serde_json::json!({"relationship": "HAS_STEP"})) {
            let _ = self.add_edge(traj_id.to_string(), step_id.clone(), e);
        }

        // NEXT_STEP (previous tail → new step) — the temporal chain link.
        if let Some(prev) = &tail {
            if let Ok(e) =
                rmp_serde::to_vec_named(&serde_json::json!({"relationship": "NEXT_STEP"}))
            {
                let _ = self.add_edge(prev.clone(), step_id.clone(), e);
            }
        }

        // Advance the trajectory bookkeeping (head on the first step, always tail).
        let mut upd = serde_json::Map::new();
        upd.insert("tail_step".to_string(), serde_json::json!(step_id));
        upd.insert("step_count".to_string(), serde_json::json!(step_index + 1));
        if tail.is_none() {
            upd.insert("head_step".to_string(), serde_json::json!(step_id));
        }
        self.merge_fields(traj_id, &upd);
        Some(step_id)
    }
}

impl GraphCore {
    pub fn new() -> Self {
        GraphCore {
            topo: RwLock::new(Topology::default()),
            node_properties: DashMap::new(),
            edge_properties: DashMap::new(),
            ledger: Mutex::new(Vec::new()),
            semantic_store: RwLock::new(crate::compute::semantic::SemanticStore::new()),
            integrity_policy: RwLock::new(None),
            dirty: std::sync::atomic::AtomicBool::new(true),
            version: std::sync::atomic::AtomicU64::new(0),
            changes: ChangeNotifier::default(),
            ontology_index: RwLock::new(None),
            label_index: RwLock::new(None),
            node_id_index: RwLock::new(None),
            edge_key_index: RwLock::new(None),
            property_index: RwLock::new(None),
            // CONCEPT:EG-KG.compute.json-deep-indexing — cold JSONPath path-index; built lazily on first use.
            path_index: RwLock::new(None),
            // CONCEPT:EG-KG.storage.path-index-store — no durable path-index store until one is attached.
            path_index_store: RwLock::new(None),
            index_manager: crate::index::IndexManager::with_default_indexes(),
            read_through: RwLock::new(None),
            node_bloom: RwLock::new(crate::bloom::NodeBloomFilter::default()),
            bloom_complete: std::sync::atomic::AtomicBool::new(false),
            index_stamps: IndexStamps::default(),
            index_rebuilds: std::sync::atomic::AtomicU64::new(0),
            #[cfg(feature = "result-cache")]
            dep_clock: Arc::new(crate::dep_scope::DepClock::new()),
            #[cfg(feature = "result-cache")]
            result_cache: crate::result_cache::ResultCache::new(),
            #[cfg(feature = "result-cache")]
            graph_projections: Arc::new(crate::projection_catalog::ProjectionCatalog::new()),
            #[cfg(feature = "security")]
            rls_projection_cache: crate::rls_projection_cache::ProjectionCache::default(),
            #[cfg(feature = "security")]
            rls_view_cache: crate::rls_view_cache::FilteredViewCache::default(),
            // perf/row-visibility-index — cold, like the other lazy node-derived
            // indexes above; built on first use.
            #[cfg(feature = "security")]
            visibility_index: RwLock::new(None),
            #[cfg(feature = "security")]
            visibility_index_rebuilds: std::sync::atomic::AtomicU64::new(0),
            schema_refs: DashMap::new(),
            ledger_dropped_total: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// The version-keyed query-result cache (CONCEPT:EG-KG.coordination.distributed-cache-coherence). The query handlers
    /// consult it before executing a read and populate it after a miss; correctness
    /// rests on `version()` keying (a write bumps the version, retiring every prior
    /// result). See `crate::result_cache`.
    #[cfg(feature = "result-cache")]
    pub fn result_cache(&self) -> &crate::result_cache::ResultCache {
        &self.result_cache
    }

    /// The per-graph dependency clock backing dependency-scoped result-cache invalidation
    /// (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation, W1.6/P7). The query handlers
    /// pass a query's [`crate::dep_scope::DepSet`] to `result_cache().get_dep`/`put_dep`, which
    /// consult this clock to serve a cached result across any write DISJOINT from that dependency
    /// set. Write paths feed it via `maintain_indexes` (fine dimensions) and `mark_dirty` (the
    /// coarse floor). See `crate::dep_scope`.
    #[cfg(feature = "result-cache")]
    pub fn dep_clock(&self) -> &crate::dep_scope::DepClock {
        &self.dep_clock
    }

    /// The named graph-projection catalog (CONCEPT:EG-KG.query.named-graph-projection-catalog,
    /// W4.5/N5) — the `gds.graph.project`-equivalent materialized-projection cache. `eg-query`'s
    /// `gds.graph.project`/`gds.graph.drop`/`gds.graph.list`/`gds.graph.exists` procedures and
    /// every named-projection-aware `gds.*` algorithm procedure reach it via the
    /// [`ProjectionScope`] riding on [`Self::analysis_snapshot_versioned`]'s [`GraphView`], not
    /// through this accessor directly (they only ever hold a view) — this accessor is for
    /// `GraphCore`-holding callers (tests, admin/introspection surfaces). See
    /// `crate::projection_catalog`.
    #[cfg(feature = "result-cache")]
    pub fn graph_projections(&self) -> &crate::projection_catalog::ProjectionCatalog {
        &self.graph_projections
    }

    /// Count of FULL node-derived index rebuilds since construction (W1.6/P7, the "rebuild count
    /// under continuous ingest ~0" metric — CONCEPT:EG-KG.storage.incremental-index-stamp). A warm index
    /// maintained incrementally through a write stream does NOT rebuild, so this stays ~1 (the
    /// initial cold build) rather than growing one-per-write. Read by the index-rebuild regression
    /// test and available as an observability counter.
    pub fn index_rebuilds(&self) -> u64 {
        self.index_rebuilds
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The unified secondary-index registry/seam (CONCEPT:AU-KG.retrieval.architecture-report). A planner
    /// (eg-query's pushdown, eg-plan's Filter leg) consults this ONE registry to
    /// ask "which index covers this predicate?" / "what indexes cover column X?"
    /// instead of hard-coding per-index checks. The label/property caches it routes
    /// to still live on this `GraphCore`, so their lazy + `mark_dirty` semantics
    /// are unchanged.
    pub fn indexes(&self) -> &crate::index::IndexManager {
        &self.index_manager
    }

    /// Return the authoritative integrity policy attached to this graph image.
    pub fn integrity_policy(&self) -> Option<IntegrityPolicy> {
        self.integrity_policy.read().clone()
    }

    /// Replace the authoritative integrity policy. Callers validate the SHACL
    /// source before staging this transition; the topology writer makes the
    /// control-state update serialize with snapshots and graph mutations.
    pub fn set_integrity_policy(&self, policy: IntegrityPolicy) {
        self.install_integrity_policy(Some(policy));
    }

    /// Install the exact policy state recovered from durable authority. This is
    /// public for persistence adapters; ordinary mutation surfaces should use
    /// [`set_integrity_policy`](Self::set_integrity_policy) after validation.
    #[doc(hidden)]
    pub fn install_integrity_policy(&self, policy: Option<IntegrityPolicy>) {
        let _topology = self.topo.write();
        *self.integrity_policy.write() = policy;
    }

    /// Register a SERVER-LAYER secondary index (text/temporal/derived-OWL,
    /// CONCEPT:EG-KG.storage.incremental-text etc.) onto this graph AFTER construction.
    /// Called by the [`crate::index::SecondaryIndexFactory`] the registry installs, so
    /// a committed write batch drives the index's incremental
    /// [`crate::index::SecondaryIndex::apply_delta`] through
    /// [`maintain_indexes`](Self::maintain_indexes) with no per-write wiring. Mirrors
    /// [`set_read_through`](Self::set_read_through): a per-graph capability injected by
    /// the server layer via `&self` interior mutability.
    pub fn register_index(&self, index: Box<dyn crate::index::SecondaryIndex>) {
        self.index_manager.register_server_index(index);
    }

    /// Does a registered server index derive from node content, so the write
    /// coalescer must capture property blobs into the [`crate::index::ChangeSet`]
    /// (CONCEPT:EG-KG.storage.incremental-text / .incremental-temporal)? A single
    /// relaxed atomic load — `false` (no content index) keeps the hot path zero-clone.
    #[inline]
    pub fn wants_change_content(&self) -> bool {
        self.index_manager.wants_change_content()
    }

    /// Invalidate the lazy secondary caches — label (CONCEPT:EG-KG.compute.consult-lazy), property
    /// equality (CONCEPT:EG-KG.query.concept-12), and JSONPath path (CONCEPT:EG-KG.compute.json-deep-indexing) —
    /// so they rebuild on the next read. Carved out of `mark_dirty` (behavior-identical)
    /// so the write-coalescer's index-maintenance seam (CONCEPT:EG-KG.storage.index-manager-seam) can
    /// reuse the SAME invalidation inside its batch topology lock.
    ///
    /// RECONCILE-WITH-LANE-0: Lane 0 (feat/lane0-foundation) owns this carve. If it
    /// lands first, drop this copy and keep the identical Lane-0 body — the two are
    /// byte-for-byte the same three cache nulls.
    pub fn invalidate_indexes(&self) {
        *self.label_index.write() = None;
        *self.node_id_index.write() = None;
        *self.property_index.write() = None;
        *self.path_index.write() = None;
        // perf/row-visibility-index: a wholesale content replacement (e.g.
        // `replace_snapshot`) invalidates the visibility index exactly like the
        // other node-derived caches above — the warm map would otherwise keep
        // serving decisions for a now-superseded image.
        #[cfg(feature = "security")]
        {
            *self.visibility_index.write() = None;
        }
    }

    /// Invalidate the edge-key keyset-scan cache (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation). Kept
    /// separate from [`Self::invalidate_indexes`] (the NODE-derived caches) because
    /// it must fire on EVERY committed write that touches edges, including the
    /// "pure edge batch" path that deliberately skips the node-derived caches via
    /// [`Self::mark_dirty_preserving_indexes`].
    fn invalidate_edge_key_index(&self) {
        *self.edge_key_index.write() = None;
    }

    /// Incrementally maintain the lazy node-derived caches for a committed write batch
    /// (CONCEPT:EG-KG.storage.incremental-index-stamp, W1.6/P7) and — under `result-cache` — record
    /// the batch's dependency footprint on the [`crate::dep_scope::DepClock`] that backs
    /// dependency-scoped result-cache invalidation.
    ///
    /// Where the previous behavior DROPPED the entire label / property index on any node add or
    /// remove (forcing a full O(V) rebuild on the next label/property query — pathological under
    /// continuous ingest), this updates the affected postings in place:
    ///   * ADD — file the new id under every label it carries and every indexed property key it has;
    ///   * REMOVE — unfile the id (from its captured labels/keys, or by scanning the warm postings);
    ///   * UPDATE (CAS) — re-file the id for exactly the changed label / property fields.
    ///
    /// Only a WARM (already-built) cache is touched; a cold cache stays cold and builds on demand.
    /// An un-attributable change — an update with unknown `changed_fields`, or an add whose blob
    /// will not decode — still DROPS the affected cache (the sound conservative fallback) and
    /// coarsely floors the dependency clock. Each cache left warm is STAMPED with `target_version`
    /// so `mark_dirty` recognizes it as already-current and preserves it instead of nuking it. The
    /// JSONPath path-index is not incrementally maintained (arbitrary-path resolution); a node add
    /// or remove drops any warm path-index so the next JSON filter rebuilds it — its pre-W1.6
    /// behavior, and the documented coarse-fallback shape for that cache.
    fn invalidate_indexes_for_change(&self, change: &crate::index::ChangeSet, target_version: u64) {
        #[cfg(feature = "result-cache")]
        let mut footprint = crate::dep_scope::WriteFootprint::default();
        #[cfg(feature = "result-cache")]
        if !change.added_edges.is_empty() || !change.removed_edges.is_empty() {
            // The node-derived caches are untouched by a pure edge change, but a traversal query
            // depends on the edge set, so the clock must learn the edge dimension moved.
            footprint.edge_changed = true;
        }

        if change.has_node_changes() {
            self.maintain_node_id_index(change, target_version);
        }

        // ── ADDS: file the new id into the warm postings it belongs to. ──
        for nc in &change.added_nodes {
            #[cfg(feature = "result-cache")]
            {
                footprint.node_changed = true;
            }
            // perf/row-visibility-index: unlike the label/property postings below,
            // `row_visibility` tolerates an undecodable blob internally (it returns
            // `RowVisibility::default_public()`, the SAME value `can_see_node` would
            // have produced for that node before this index existed) — so there is
            // no "cannot target its postings" failure mode here and no need to drop
            // the whole index on a decode failure. File unconditionally.
            #[cfg(feature = "security")]
            self.visibility_index_set(&nc.id, nc.properties_msgpack.as_deref(), target_version);
            match self.node_props_value(&nc.id, nc.properties_msgpack.as_deref()) {
                Some(val) => {
                    self.label_index_add(&nc.id, &val, target_version);
                    self.property_index_add(&nc.id, &val, target_version);
                    #[cfg(feature = "result-cache")]
                    collect_dep_footprint(&mut footprint, &val);
                }
                None => {
                    // Cannot read the added node's content ⇒ cannot target its postings; drop.
                    *self.label_index.write() = None;
                    *self.property_index.write() = None;
                    #[cfg(feature = "result-cache")]
                    {
                        footprint.coarse_node = true;
                    }
                }
            }
            *self.path_index.write() = None;
        }

        // ── REMOVES: unfile the id from the warm postings. ──
        for id in &change.removed_nodes {
            #[cfg(feature = "result-cache")]
            {
                footprint.node_changed = true;
            }
            // perf/row-visibility-index: a flat `id -> RowVisibility` map needs no
            // captured value to unfile — unlike the label/property postings (which
            // need to know WHICH postings to remove `id` from), removal here is a
            // single O(1) delete regardless of what the node's blob contained.
            #[cfg(feature = "security")]
            self.visibility_index_remove(id, target_version);
            let captured = change
                .removed_node_props
                .get(id)
                .and_then(|blob| decode_property_value(blob).ok());
            match &captured {
                Some(val) => {
                    self.label_index_remove(id, Some(val), target_version);
                    self.property_index_remove(id, Some(val), target_version);
                    #[cfg(feature = "result-cache")]
                    collect_dep_footprint(&mut footprint, val);
                }
                None => {
                    // Uncaptured removal: scan the warm postings for the id (sound; no blob decode)
                    // and coarsely floor the clock (its labels/keys are unknown).
                    self.label_index_remove(id, None, target_version);
                    self.property_index_remove(id, None, target_version);
                    #[cfg(feature = "result-cache")]
                    {
                        footprint.coarse_node = true;
                    }
                }
            }
            *self.path_index.write() = None;
        }

        // ── UPDATES (CAS): re-file the id for the changed label / property fields only. ──
        for nc in &change.updated_nodes {
            #[cfg(feature = "result-cache")]
            {
                footprint.node_changed = true;
            }
            let Some(fields) = nc.changed_fields.as_ref() else {
                // Unknown-scope update ⇒ drop the node-derived caches (fallback) + floor.
                *self.label_index.write() = None;
                *self.property_index.write() = None;
                *self.path_index.write() = None;
                // perf/row-visibility-index: an unknown-scope CAS could have touched
                // an RLS key just as easily as any other — same conservative
                // whole-index drop as the other node-derived caches above.
                #[cfg(feature = "security")]
                {
                    *self.visibility_index.write() = None;
                }
                #[cfg(feature = "result-cache")]
                {
                    footprint.coarse_node = true;
                }
                continue;
            };
            let current = self.node_props_value(&nc.id, None);
            if fields
                .iter()
                .any(|f| matches!(f.as_str(), "type" | "node_type" | "label" | "labels"))
            {
                self.label_index_refile(&nc.id, current.as_ref(), target_version);
                #[cfg(feature = "result-cache")]
                if let Some(val) = &current {
                    footprint.labels.extend(labels_of(val));
                }
            }
            self.property_index_refile(&nc.id, fields, current.as_ref(), target_version);
            #[cfg(feature = "result-cache")]
            footprint.keys.extend(fields.iter().cloned());
            self.path_index_invalidate_for_fields(fields, target_version);
            // perf/row-visibility-index: re-file ONLY when a changed field is one of
            // the RLS keys `row_visibility` actually reads (EITHER naming
            // convention — `crate::isolation::RowVisibility`'s doc) — a CAS that
            // left every RLS key untouched cannot have changed the node's
            // visibility decision, so the warm entry stays valid and is left alone
            // (unlike label/property refiling above, which always re-files on ANY
            // known field set because it must recompute regardless).
            #[cfg(feature = "security")]
            if fields.iter().any(|f| {
                f == crate::isolation::RLS_OWNER_KEY
                    || f == crate::isolation::RLS_VISIBILITY_KEY
                    || f == crate::isolation::RLS_GRANTS_KEY
                    || f == crate::isolation::RLS_OWNER_ID_KEY
                    || f == crate::isolation::RLS_SHARED_SCOPE_KEY
            }) {
                self.visibility_index_set(&nc.id, None, target_version);
            }
        }

        #[cfg(feature = "result-cache")]
        self.dep_clock.note_footprint(&footprint, target_version);
    }

    /// Decode a node's CURRENT property blob (present after an add/update) to JSON, falling back
    /// to a `fallback` blob captured on the change (used for an add whose node the coalescer
    /// already committed). `None` when neither is present or decodable — the drop-and-rebuild
    /// fallback signal for the incremental maintainers.
    fn node_props_value(&self, id: &str, fallback: Option<&[u8]>) -> Option<serde_json::Value> {
        if let Some(props) = self.node_properties.get(id) {
            if let Ok(val) = decode_property_value(props.value().as_slice()) {
                return Some(val);
            }
        }
        fallback.and_then(|blob| decode_property_value(blob).ok())
    }

    /// Keep the warm unlabeled-scan id cache current in place: insert added ids, remove removed
    /// ids (a CAS leaves the id SET unchanged). Cold ⇒ no-op (builds on demand). Stamped so
    /// `mark_dirty` preserves it.
    fn maintain_node_id_index(&self, change: &crate::index::ChangeSet, target_version: u64) {
        if change.added_nodes.is_empty() && change.removed_nodes.is_empty() {
            return;
        }
        let mut guard = self.node_id_index.write();
        let Some(ids) = guard.as_mut() else {
            return;
        };
        for nc in &change.added_nodes {
            insert_sorted(ids, &nc.id);
        }
        for id in &change.removed_nodes {
            remove_sorted(ids, id);
        }
        self.index_stamps
            .node_id
            .store(target_version, std::sync::atomic::Ordering::Release);
    }

    /// File `id` under every label it carries in the WARM label index (no-op when cold). Stamped.
    fn label_index_add(&self, id: &str, val: &serde_json::Value, target_version: u64) {
        let mut guard = self.label_index.write();
        let Some(index) = guard.as_mut() else {
            return;
        };
        for label in labels_of(val) {
            insert_sorted(index.entry(label).or_default(), id);
        }
        self.index_stamps
            .label
            .store(target_version, std::sync::atomic::Ordering::Release);
    }

    /// Unfile `id` from the WARM label index. With `val` (captured pre-removal properties) only its
    /// own label postings are touched; without it, every posting is scanned for the id (sound but
    /// O(#labels)). No-op when cold. Stamped.
    fn label_index_remove(&self, id: &str, val: Option<&serde_json::Value>, target_version: u64) {
        let mut guard = self.label_index.write();
        let Some(index) = guard.as_mut() else {
            return;
        };
        match val {
            Some(val) => {
                for label in labels_of(val) {
                    if let Some(ids) = index.get_mut(&label) {
                        remove_sorted(ids, id);
                    }
                }
            }
            None => {
                for ids in index.values_mut() {
                    remove_sorted(ids, id);
                }
            }
        }
        self.index_stamps
            .label
            .store(target_version, std::sync::atomic::Ordering::Release);
    }

    /// Re-file `id` after a label-field CAS: remove it from every posting, then re-add it under
    /// its CURRENT labels. Bounded by the label cardinality. No-op when cold. Stamped.
    fn label_index_refile(
        &self,
        id: &str,
        current: Option<&serde_json::Value>,
        target_version: u64,
    ) {
        let mut guard = self.label_index.write();
        let Some(index) = guard.as_mut() else {
            return;
        };
        for ids in index.values_mut() {
            remove_sorted(ids, id);
        }
        if let Some(val) = current {
            for label in labels_of(val) {
                insert_sorted(index.entry(label).or_default(), id);
            }
        }
        self.index_stamps
            .label
            .store(target_version, std::sync::atomic::Ordering::Release);
    }

    // ── perf/row-visibility-index: write-time-maintained RLS visibility cache ──

    /// (Re)file `id`'s RLS visibility in the WARM `visibility_index` from its
    /// CURRENT property blob — the shared operation an ADD and an RLS-key-touching
    /// CAS refile both need (compute-and-insert; there is no separate "refile"
    /// shape the way label/property postings need, since this is a flat
    /// `id -> RowVisibility` map, not a `value -> ids` posting list). No-op when
    /// cold (mirrors every other node-derived cache: a cold cache stays cold and
    /// builds on demand via [`Self::live_visibility_index`]). Stamped.
    ///
    /// `blob` is the freshly-written content when the caller already has it (an
    /// ADD's captured `properties_msgpack`); when `None`, the CURRENT
    /// `node_properties` entry (post-write — mirrors [`Self::node_props_value`]'s
    /// identical fallback chain) is read instead, used by the CAS-refile call site
    /// which does not carry a captured blob.
    ///
    /// Correctness: every entry is produced by calling
    /// [`crate::isolation::row_visibility`] on the RAW blob bytes — the EXACT
    /// function [`crate::isolation::IsolationLayer::can_see_node`] called per-node,
    /// per-query before this index existed. This function never reimplements or
    /// approximates that decode; it only memoizes calls to it, so a warm entry is
    /// byte-for-byte identical to what a cold decode of the same blob would
    /// produce. A node with no readable blob at all (should not occur for an
    /// add/update whose node still exists post-write) resolves to
    /// `RowVisibility::default_public()` — the SAME fallback `can_see_node` used
    /// for a missing blob (`view.node_properties.get(id)` returning `None`), not a
    /// new case.
    #[cfg(feature = "security")]
    fn visibility_index_set(&self, id: &str, blob: Option<&[u8]>, target_version: u64) {
        let mut guard = self.visibility_index.write();
        let Some(index) = guard.as_mut() else {
            return;
        };
        let vis = match blob {
            Some(b) => crate::isolation::row_visibility(b),
            None => match self.node_properties.get(id) {
                Some(props) => crate::isolation::row_visibility(props.value().as_slice()),
                None => crate::isolation::RowVisibility::default_public(),
            },
        };
        index.insert(id.to_string(), vis);
        self.index_stamps
            .visibility
            .store(target_version, std::sync::atomic::Ordering::Release);
    }

    /// Unfile `id` from the WARM `visibility_index`. No-op when cold. Stamped.
    #[cfg(feature = "security")]
    fn visibility_index_remove(&self, id: &str, target_version: u64) {
        let mut guard = self.visibility_index.write();
        let Some(index) = guard.as_mut() else {
            return;
        };
        index.remove(id);
        self.index_stamps
            .visibility
            .store(target_version, std::sync::atomic::Ordering::Release);
    }

    /// Lazily build-and-cache [`Self::visibility_index`], returning an owned copy
    /// for a [`GraphView`] snapshot to carry (`GraphView::visibility_index`).
    /// Mirrors [`Self::get_nodes_by_label_page`]'s consult-then-build-once idiom
    /// exactly: a warm index is returned via a cheap clone (cloning `RowVisibility`
    /// structs — small, no allocation-heavy tree walk — not re-decoding); a cold
    /// index pays [`Self::build_visibility_index`]'s ONE full
    /// `row_visibility`-per-node decode pass, stamps itself at the version it
    /// scanned, and publishes the result so every later call (from ANY caller,
    /// building ANY kind of snapshot) sees it warm. From then on the index is kept
    /// current in place by [`Self::invalidate_indexes_for_change`]
    /// (`visibility_index_set`/`_remove`) on every write, so a write-driven
    /// `FilteredViewCache`/`rls_projection_cache` invalidation (which amortize the
    /// FILTERED result, not the per-node decode) is followed by an O(1)-lookup
    /// rebuild here, not a re-decode — this is what makes the query immediately
    /// after a write cheap too, not just the query after that.
    #[cfg(feature = "security")]
    fn live_visibility_index(&self) -> HashMap<String, crate::isolation::RowVisibility> {
        {
            let guard = self.visibility_index.read();
            if let Some(idx) = guard.as_ref() {
                return idx.clone();
            }
        }
        let built_at = self.version();
        let built = self.build_visibility_index();
        {
            let mut guard = self.visibility_index.write();
            self.index_stamps
                .visibility
                .store(built_at, std::sync::atomic::Ordering::Release);
            *guard = Some(built.clone());
        }
        built
    }

    /// Scan the node store once and build the RLS visibility index — the O(V)
    /// `row_visibility`-per-node decode pass this index's incremental maintenance
    /// (`visibility_index_set`/`_remove`) exists to pay AT MOST ONCE under
    /// continuous ingest (mirrors [`Self::build_label_index`]'s identical
    /// "rebuild count under ingest ~1" observability shape, tracked separately via
    /// [`Self::visibility_index_rebuilds`] rather than the pre-existing
    /// [`Self::index_rebuilds`] counter so the two signals are never conflated).
    #[cfg(feature = "security")]
    fn build_visibility_index(&self) -> HashMap<String, crate::isolation::RowVisibility> {
        let n = self
            .visibility_index_rebuilds
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        tracing::debug!(
            target: "epistemic_graph::index_rebuild",
            index = "visibility",
            nodes = self.node_properties.len(),
            total_rebuilds = n,
            "full RLS-visibility-index rebuild (cold cache); warm ingest maintains it incrementally"
        );
        self.node_properties
            .iter()
            .map(|entry| {
                (
                    entry.key().clone(),
                    crate::isolation::row_visibility(entry.value().as_slice()),
                )
            })
            .collect()
    }

    /// Count of FULL rebuilds of [`Self::visibility_index`] (perf/row-visibility-index).
    /// Observability + test-assertion hook ("the fast path is actually taken") —
    /// never on a read hot path.
    #[cfg(feature = "security")]
    pub fn visibility_index_rebuilds(&self) -> u64 {
        self.visibility_index_rebuilds
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// File `id` under every INDEXED property key it has a scalar value for, in the WARM property
    /// index (no-op when cold; never indexes a new key — the index stays demand-bounded). Stamped.
    fn property_index_add(&self, id: &str, val: &serde_json::Value, target_version: u64) {
        let mut guard = self.property_index.write();
        let Some(index) = guard.as_mut() else {
            return;
        };
        for (key, by_value) in index.keys.iter_mut() {
            if let Some(vk) = val.get(key).and_then(Self::property_value_key) {
                insert_sorted(by_value.entry(vk).or_default(), id);
            }
        }
        self.index_stamps
            .property
            .store(target_version, std::sync::atomic::Ordering::Release);
    }

    /// Unfile `id` from the WARM property index. With captured `val`, only its own value posting
    /// per indexed key is touched; without it, every value posting is scanned. No-op when cold.
    fn property_index_remove(
        &self,
        id: &str,
        val: Option<&serde_json::Value>,
        target_version: u64,
    ) {
        let mut guard = self.property_index.write();
        let Some(index) = guard.as_mut() else {
            return;
        };
        for (key, by_value) in index.keys.iter_mut() {
            match val
                .and_then(|v| v.get(key))
                .and_then(Self::property_value_key)
            {
                Some(vk) => {
                    if let Some(ids) = by_value.get_mut(&vk) {
                        remove_sorted(ids, id);
                    }
                }
                None => {
                    for ids in by_value.values_mut() {
                        remove_sorted(ids, id);
                    }
                }
            }
        }
        self.index_stamps
            .property
            .store(target_version, std::sync::atomic::Ordering::Release);
    }

    /// Re-file `id` in the WARM property index for the CHANGED keys only: for each changed key
    /// that is indexed, remove the id from that key's value postings then re-add it under its
    /// current value. No-op when cold. Stamped.
    fn property_index_refile(
        &self,
        id: &str,
        changed_fields: &[String],
        current: Option<&serde_json::Value>,
        target_version: u64,
    ) {
        let mut guard = self.property_index.write();
        let Some(index) = guard.as_mut() else {
            return;
        };
        let mut touched = false;
        for field in changed_fields {
            let Some(by_value) = index.keys.get_mut(field) else {
                continue;
            };
            touched = true;
            for ids in by_value.values_mut() {
                remove_sorted(ids, id);
            }
            if let Some(vk) = current
                .and_then(|v| v.get(field))
                .and_then(Self::property_value_key)
            {
                insert_sorted(by_value.entry(vk).or_default(), id);
            }
        }
        if touched {
            self.index_stamps
                .property
                .store(target_version, std::sync::atomic::Ordering::Release);
        }
    }

    /// The pre-W1.6 scoped path-index invalidation for a CAS: drop a warm JSONPath index only if
    /// a changed field could feed one of its indexed paths (an untargeted / root path observes any
    /// top-level change). The path index is not incrementally maintained; this preserves its
    /// unaffected-retention behavior for updates.
    fn path_index_invalidate_for_fields(&self, changed_fields: &[String], target_version: u64) {
        let mut index = self.path_index.write();
        if index.is_none() {
            return;
        }
        let affected = index.as_ref().is_some_and(|index| {
            index.by_value.keys().any(|path| {
                match crate::jsonpath::parse_path(path).and_then(|parts| parts.into_iter().next()) {
                    Some(crate::jsonpath::Segment::Key(key)) => {
                        changed_fields.iter().any(|f| f == &key)
                    }
                    _ => true,
                }
            })
        });
        if affected {
            *index = None;
        } else {
            // The CAS could not feed any indexed path — the warm path index stays valid. Stamp it
            // forward so `mark_dirty` recognizes it as current and preserves it (W1.6/P7); without
            // the stamp, the stamp-aware nuke would drop this deliberately-retained cache.
            self.index_stamps
                .path
                .store(target_version, std::sync::atomic::Ordering::Release);
        }
    }

    /// Maintain the secondary indexes after a committed write batch
    /// (CONCEPT:EG-KG.storage.write-changeset). This convenience is for callers
    /// outside a topology guard. Lock-owning compound paths call
    /// [`Self::maintain_indexes_at`] with counts read from their [`GraphTxn`], so a
    /// concurrent hybrid read never observes a torn index and completeness
    /// publication never recursively acquires the topology lock.
    ///
    /// Route `change` through the [`IndexManager`] seam —
    ///    each heavy index applies its own delta (the vector store tombstones the
    ///    embeddings of removed nodes, CONCEPT:EG-KG.storage.incremental-ann) — then invalidate only
    ///    lazy label/property/path caches covered by the exact node-field change.
    pub fn maintain_indexes(
        &self,
        change: &crate::index::ChangeSet,
    ) -> crate::index::BatchMaintenance {
        let outcome = self.index_manager.commit_batch(self, change);
        // Label/property/JSON-path postings derive only from nodes. Pure edge
        // batches preserve those potentially expensive warm caches. This convenience runs
        // outside a topology guard, so it predicts the batch's committed version as the next
        // one — an under-estimate is safe (it only risks an extra rebuild, never a stale read).
        self.invalidate_indexes_for_change(change, self.version().saturating_add(1));
        outcome
    }

    /// Maintain indexes for one compound mutation whose externally visible graph
    /// version advances once, regardless of its internal operation count.
    /// `node_count` and `edge_count` come from the caller's held [`GraphTxn`], so
    /// publishing completeness never recursively acquires the topology lock.
    pub fn maintain_indexes_at(
        &self,
        change: &crate::index::ChangeSet,
        target_version: u64,
        node_count: usize,
        edge_count: usize,
    ) -> crate::index::BatchMaintenance {
        let outcome = self.index_manager.commit_batch_at(
            self,
            change,
            target_version,
            node_count as u64,
            edge_count as u64,
        );
        self.invalidate_indexes_for_change(change, target_version);
        outcome
    }

    /// The change-notification fan-out (CONCEPT:EG-KG.compute.cdc-event-emit). A server-layer GraphQL
    /// subscription carrier calls `core.changes().subscribe(&sink)` to receive a
    /// [`ChangeEvent`] on every committed write, turning a poll-only subscription
    /// into a real push (live query). The default build never subscribes, so the
    /// write path pays nothing.
    pub fn changes(&self) -> &ChangeNotifier {
        &self.changes
    }

    /// Attach a durable read-through (CONCEPT:EG-KG.storage.read-through-seam-exercised). Called once at startup
    /// so a node evicted from RAM is still
    /// served from redb on a RAM miss. A `GraphCore` with no read-through behaves
    /// exactly as before — a miss is a genuine absence.
    pub fn set_read_through(&self, rt: Arc<dyn crate::read_through::ReadThrough>) {
        *self.read_through.write() = Some(rt);
    }

    /// Mark this graph as changed since its last checkpoint (Phase C-C). Called by
    /// the dispatch after any successful write op and by the background decay sweep.
    pub fn mark_dirty(&self) {
        self.mark_dirty_inner(true);
    }

    /// Mark a committed write while retaining node-derived lazy indexes that a
    /// preceding incremental maintenance step proved unaffected (for example a
    /// pure edge batch). This is hidden infrastructure API: callers that do not
    /// carry a complete [`crate::index::ChangeSet`] must use [`mark_dirty`].
    #[doc(hidden)]
    pub fn mark_dirty_preserving_indexes(&self) {
        self.mark_dirty_inner(false);
    }

    fn mark_dirty_inner(&self, invalidate_node_indexes: bool) {
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        // Bump the OCC write-version (CONCEPT:EG-KG.txn.occ-graph-core): every committed write —
        // single-op, coalesced batch, and (via the commit path) a multi-op txn —
        // flows through `mark_dirty`, so an in-flight staged transaction's validate
        // step observes any concurrent write that landed since it began. AcqRel so
        // the bump is visible to a commit that reads `version()` under the topo
        // write lock.
        let new_version = self
            .version
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        // Retire the lazy secondary indexes made stale by this write — but only the ones a
        // preceding `maintain_indexes` did NOT already bring current (W1.6/P7,
        // CONCEPT:EG-KG.storage.incremental-index-stamp). A node add/remove that was incrementally
        // applied to the postings stamps its index at (at least) this version, so
        // `invalidate_node_indexes_if_stale` preserves it instead of dropping + rebuilding it;
        // a write that bypassed maintenance (a stale stamp) still drops it. This is what makes
        // the index-rebuild count under continuous ingest ~0 instead of one-per-write.
        if invalidate_node_indexes {
            self.invalidate_node_indexes_if_stale(new_version);
        }
        // Unconditional (unlike the node-derived caches above): a pure edge batch
        // is exactly the case `mark_dirty_preserving_indexes` exists for, and it
        // is exactly the case that changes this index.
        self.invalidate_edge_key_index();
        // CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation (W1.6/P7) — record the
        // version bump on the dependency clock. It floors (coarsely invalidates every
        // dependency-scoped result-cache entry) ONLY when this version was not already accounted
        // for by a `maintain_indexes` footprint — i.e. only for a write that bypassed the
        // change-set path (a follower's replicated apply). A footprinted write does not floor.
        #[cfg(feature = "result-cache")]
        self.dep_clock.note_version_bump(new_version);
        // CONCEPT:EG-KG.compute.cdc-event-emit — fan out a change notification (post-write version) to any
        // live subscribers (the GraphQL subscription carrier). A single relaxed
        // atomic load when there are none, so this is off the write hot path.
        self.changes.emit(new_version);
    }

    /// Drop each lazy node-derived cache whose STAMP is stale relative to `new_version`
    /// (W1.6/P7, CONCEPT:EG-KG.storage.incremental-index-stamp) — the stamp-aware replacement for the
    /// unconditional `invalidate_indexes` the node-write `mark_dirty` used to call. A cache whose
    /// `maintain_indexes` step already stamped it at `>= new_version` (an incrementally applied
    /// add/remove/CAS) is preserved; a cache left stale by a maintenance-bypassing write is
    /// dropped so the next read rebuilds it. The stamp check runs UNDER the cache's own write lock
    /// so it is atomic with the maintainer's stamp store (no torn stamp-vs-content read).
    fn invalidate_node_indexes_if_stale(&self, new_version: u64) {
        Self::drop_if_stale(&self.label_index, &self.index_stamps.label, new_version);
        Self::drop_if_stale(&self.node_id_index, &self.index_stamps.node_id, new_version);
        Self::drop_if_stale(
            &self.property_index,
            &self.index_stamps.property,
            new_version,
        );
        Self::drop_if_stale(&self.path_index, &self.index_stamps.path, new_version);
        // perf/row-visibility-index: same stamp-aware preserve-or-drop treatment as
        // the four caches above — a write whose `invalidate_indexes_for_change` step
        // already incrementally re-filed the touched node(s) (stamp >= new_version)
        // is preserved; a write that bypassed maintenance is dropped.
        #[cfg(feature = "security")]
        Self::drop_if_stale(
            &self.visibility_index,
            &self.index_stamps.visibility,
            new_version,
        );
    }

    /// Set a lazy cache to `None` iff it is warm AND its stamp predates `new_version`. Generic over
    /// the four node-derived caches (W1.6/P7).
    fn drop_if_stale<T>(
        cache: &RwLock<Option<T>>,
        stamp: &std::sync::atomic::AtomicU64,
        new_version: u64,
    ) {
        let mut guard = cache.write();
        if guard.is_some() && stamp.load(std::sync::atomic::Ordering::Acquire) < new_version {
            *guard = None;
        }
    }

    /// Invalidate cached query results for a CHANGE that landed elsewhere
    /// (CONCEPT:EG-KG.coordination.distributed-cache-coherence — distributed cache coherence). A replica tailing the CDC
    /// feed calls this when it observes a REMOTE write for this graph: it bumps the
    /// local `version` (so any cached result keyed on the old version becomes
    /// unreachable) AND drops the cache directly (belt-and-braces — the local data
    /// itself may be rehydrated separately). Unlike `mark_dirty`, this is for a write
    /// that did NOT flow through this node's local write path, so the version must be
    /// advanced explicitly to retire stale cached reads.
    #[cfg(feature = "result-cache")]
    pub fn invalidate_for_remote_change(&self) {
        let new_version = self
            .version
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        self.result_cache.invalidate_all();
        // The dependency clock backs the SAME result cache; a remote change is un-attributable
        // here (its footprint landed on another node), so floor every dependency dimension at the
        // new version — no dependency-scoped entry may survive it (W1.6/P7).
        self.dep_clock.invalidate_all(new_version);
        // The label/property/path caches are derived state; retire them too so a
        // subsequent local read after a remote-applied change rebuilds them. Same
        // carved block as `mark_dirty` (CONCEPT:EG-KG.storage.index-manager-seam).
        self.invalidate_indexes();
        self.invalidate_edge_key_index();
        // CONCEPT:EG-KG.compute.cdc-event-emit — a replicated write must also wake local live-query
        // subscribers, so a subscription reflects remote writes, not just local ones.
        self.changes.emit(new_version);
    }

    /// Current OCC write-version (CONCEPT:EG-KG.txn.occ-graph-core). A staged transaction snapshots
    /// this at begin and an OCC commit re-reads it under the topology write lock to
    /// detect concurrent writes. Cheap atomic load — never on a read hot path.
    pub fn version(&self) -> u64 {
        self.version.load(std::sync::atomic::Ordering::Acquire)
    }

    /// D-OP-1 / D-OB-20 — look up a cached RLS projection for `actor`, valid only if
    /// it was built at exactly `current_version`. `None` on a cold miss or a stale
    /// (version-mismatched) entry; the caller then builds fresh and stores via
    /// [`Self::put_cached_projection`]. See `crate::rls_projection_cache` for why the
    /// (expensive) build must never happen while this lookup's lock is held.
    ///
    /// `pub` (not `pub(crate)`) so `GraphReadAuthority::project_core`, which lives in
    /// the served-facade crate above `eg-core`, can reach it through the re-exported
    /// `graph` module (`src/lib.rs`'s `pub use eg_core::{..., graph, ...}`) — not part
    /// of a stable external API, just this crate's own internal facade boundary.
    #[cfg(feature = "security")]
    pub fn cached_projection(&self, actor: &str, current_version: u64) -> Option<Arc<GraphCore>> {
        self.rls_projection_cache.get(actor, current_version)
    }

    /// U-142/U-143/U-145 (BUG-130) — the current whole-image generation of the
    /// projection cache. A caller MUST capture this immediately before starting an
    /// (expensive, unlocked) [`Self::put_cached_projection`] build and pass the same
    /// value back to it, so a whole-image replacement/clear that lands mid-build
    /// (`Self::invalidate_projection_cache`, called from `replace_snapshot`/`clear`)
    /// discards that build's now-stale result instead of publishing it. See
    /// `crate::rls_projection_cache`'s `generation` field doc for the full rationale.
    #[cfg(feature = "security")]
    pub fn projection_cache_generation(&self) -> u64 {
        self.rls_projection_cache.generation()
    }

    /// D-OP-1 / D-OB-20 — store a freshly built RLS projection for `actor` at
    /// `version`. Call this AFTER the projection is fully built, never while
    /// holding any lock the build itself needed. See [`Self::cached_projection`]'s
    /// doc for why this is `pub`. `generation` must be the value
    /// [`Self::projection_cache_generation`] returned right before the build started
    /// (U-145) — a mismatch at store time means a whole-image replacement invalidated
    /// the cache while this build was in flight, and the result is dropped rather
    /// than published.
    #[cfg(feature = "security")]
    pub fn put_cached_projection(
        &self,
        actor: String,
        version: u64,
        generation: u64,
        projected: Arc<GraphCore>,
    ) {
        self.rls_projection_cache
            .put(actor, version, generation, projected);
    }

    /// U-142/U-143/U-145 (BUG-130) — advance the projection cache's whole-image
    /// generation and drop every cached entry. Called from every whole-image
    /// transition that does not already imply a `version()` bump the cache's
    /// (actor, version) key would itself catch: [`Self::replace_snapshot`]
    /// (same-version resident-image replacement/reconciliation) and [`Self::clear`]
    /// (and [`Self::hibernate`], which reuses it) — a wipe that intentionally does
    /// not bump `version` (see that method's own doc). Mirrors the sibling
    /// `result_cache.invalidate_all()` call already present at both sites; unlike
    /// that cache, this one is per-actor and `security`-only, hence the separate
    /// entry point.
    #[cfg(feature = "security")]
    fn invalidate_projection_cache(&self) {
        self.rls_projection_cache.invalidate_all();
    }

    /// perf/cold-query-floor-analysis (UNCOMPILED proposal) — look up a cached
    /// RLS-filtered `GraphView` for `actor`, valid only at exactly `current_version`.
    /// Mirrors [`Self::cached_projection`] exactly, one layer lighter. See
    /// `crate::rls_view_cache` for the full rationale.
    #[cfg(feature = "security")]
    pub fn cached_filtered_view(
        &self,
        actor: &str,
        current_version: u64,
    ) -> Option<Arc<GraphView>> {
        self.rls_view_cache.get(actor, current_version)
    }

    /// perf/cold-query-floor-analysis (UNCOMPILED proposal) — the filtered-view cache's
    /// current whole-image generation. Capture BEFORE starting an unlocked
    /// `analysis_snapshot_versioned` + `filter_view` build and hand the same value back
    /// to [`Self::put_cached_filtered_view`]. Mirrors [`Self::projection_cache_generation`].
    #[cfg(feature = "security")]
    pub fn filtered_view_cache_generation(&self) -> u64 {
        self.rls_view_cache.generation()
    }

    /// perf/cold-query-floor-analysis (UNCOMPILED proposal) — store a freshly filtered
    /// view for `actor` at `version`, only if `generation` is still current. Mirrors
    /// [`Self::put_cached_projection`] exactly.
    #[cfg(feature = "security")]
    pub fn put_cached_filtered_view(
        &self,
        actor: String,
        version: u64,
        generation: u64,
        view: Arc<GraphView>,
    ) {
        self.rls_view_cache.put(actor, version, generation, view);
    }

    /// perf/cold-query-floor-analysis (UNCOMPILED proposal) — advance the filtered-view
    /// cache's whole-image generation and drop every cached entry. Called alongside
    /// [`Self::invalidate_projection_cache`] at every one of its call sites (same
    /// same-version whole-image transitions apply identically to this cache).
    #[cfg(feature = "security")]
    fn invalidate_filtered_view_cache(&self) {
        self.rls_view_cache.invalidate_all();
    }

    /// Atomically read-and-clear the dirty flag. The checkpoint calls this BEFORE
    /// snapshotting, so a mutation that races the checkpoint re-marks the graph
    /// dirty and is captured by the NEXT checkpoint rather than being lost.
    pub fn take_dirty(&self) -> bool {
        self.dirty.swap(false, std::sync::atomic::Ordering::AcqRel)
    }

    /// Open a write transaction: acquires the topology write lock and borrows the
    /// property maps + ledger. A sequence of mutations under one txn is atomic
    /// w.r.t. other writers (and excludes graph-traversal readers) until it drops.
    /// Single-op convenience methods below open a one-shot txn; multi-op callers
    /// (batch_update, reasoning) hold one txn so the whole batch is atomic.
    pub fn txn(&self) -> GraphTxn<'_> {
        GraphTxn {
            topo: self.topo.write(),
            node_properties: &self.node_properties,
            edge_properties: &self.edge_properties,
            ledger: &self.ledger,
            node_bloom: &self.node_bloom,
            ledger_dropped_total: &self.ledger_dropped_total,
        }
    }

    // ── Node CRUD (one-shot convenience over `txn`) ──────────────────────

    pub fn add_node(&self, node_id: String, properties_msgpack: Vec<u8>) {
        self.txn().add_node(node_id, properties_msgpack);
    }

    /// Insert a node WITHOUT appending a ledger record (D-EGP-1, t1-grounding-0802).
    ///
    /// `add_node`'s ledger write hex-encodes the ENTIRE property blob byte-by-byte
    /// (`HexLedger`'s `Display` impl does one `write!` per byte) into a `String`,
    /// then pushes it under `self.ledger`'s mutex — real, non-trivial cost when
    /// called thousands of times. This is exactly what
    /// `GraphReadAuthority::build_projection` (`src/server/access.rs`) does when the
    /// RLS projection cache misses: it builds a throwaway `GraphCore` one node/edge
    /// at a time via the ordinary `add_node`/`add_edge`, then immediately
    /// `ledger.lock().clear()`s the whole thing, because the synthesized
    /// `ADD_NODE|...` records are never real source history. Every one of those
    /// records was therefore formatted, hex-encoded, and locked into a `Vec` purely
    /// to be thrown away — on a 27,969-node graph this is tens of thousands of
    /// wasted per-byte `write!` calls on every single cache miss, and was found to
    /// be the dominant cost of the ~2.4-15s rebuild time the miss-cost histogram
    /// (`epistemic_graph_projection_cache_miss_build_seconds`) measured live.
    ///
    /// Use ONLY for synthetic/throwaway construction whose ledger is discarded
    /// before ever being read (`build_projection` today) — never for a mutation
    /// whose ledger must reflect real history. Functionally identical to
    /// `add_node` otherwise (same node_map/graph/node_properties/bloom updates).
    pub fn add_node_no_ledger(&self, node_id: String, properties_msgpack: Vec<u8>) {
        {
            let mut topo = self.topo.write();
            if !topo.node_map.contains_key(&node_id) {
                let new_idx = topo.graph.add_node(node_id.clone());
                topo.node_map.insert(node_id.clone(), new_idx);
            }
        }
        self.node_bloom.read().insert(&node_id);
        self.node_properties
            .insert(node_id, Arc::new(properties_msgpack));
    }

    /// Atomically create `node_id` without overwriting an existing node.
    /// Returns `true` only for the writer that inserted it.
    pub fn create_node_if_absent(&self, node_id: String, properties_msgpack: Vec<u8>) -> bool {
        self.txn()
            .create_node_if_absent(node_id, properties_msgpack)
    }

    pub fn remove_node(&self, node_id: String) {
        self.txn().remove_node(node_id.clone());
        self.semantic_store.write().remove_embedding(&node_id);
    }

    /// Bulk memory-only eviction. Unlike a logical delete this does not append a
    /// `REMOVE_NODE` ledger record or dirty the graph; durable state remains the
    /// authority. Semantic entries are removed under one lock so the projection
    /// releases their resident bytes without lock-per-node overhead.
    pub fn evict_resident_nodes(&self, node_ids: &[String]) -> usize {
        let removed = self.txn().evict_resident_nodes(node_ids);
        if removed > 0 {
            let mut semantic = self.semantic_store.write();
            for node_id in node_ids {
                semantic.remove_embedding(node_id);
            }
        }
        removed
    }

    // ── Distribution-valued properties (CONCEPT:EG-KG.compute.uncertainty-values) ──────────────────
    //
    // A `Distribution` is just a tagged JSON object, so it round-trips through
    // the existing arbitrary-JSON property map with NO schema change — these
    // are typed convenience accessors over that convention, generalizing the
    // scalar `confidence` field into a full uncertainty distribution.

    /// Read a distribution-valued property `key` off `node_id` (CONCEPT:EG-KG.compute.uncertainty-values).
    /// Returns `None` if the node is absent, its blob is undecodable, the key is
    /// missing, or the stored JSON is not a valid `Distribution`.
    pub fn get_distribution(&self, node_id: &str, key: &str) -> Option<eg_types::Distribution> {
        let bytes = self.get_node_properties(node_id)?;
        let val = decode_property_value(&bytes).ok()?;
        let field = val.as_object()?.get(key)?.clone();
        serde_json::from_value(field).ok()
    }

    /// Store `dist` as the property `key` on `node_id` (CONCEPT:EG-KG.compute.uncertainty-values), merging
    /// into the node's existing properties (other keys preserved). Creates the
    /// node with a single-key object if it does not yet exist. Returns whether
    /// the property was written (only fails if the value cannot be serialized).
    pub fn set_distribution(
        &self,
        node_id: &str,
        key: &str,
        dist: &eg_types::Distribution,
    ) -> bool {
        let dist_val = match serde_json::to_value(dist) {
            Ok(v) => v,
            Err(_) => return false,
        };
        // Merge into the current blob (or start a fresh object).
        let mut obj = match self.get_node_properties(node_id) {
            Some(bytes) => match decode_property_value(&bytes) {
                Ok(serde_json::Value::Object(o)) => o,
                _ => serde_json::Map::new(),
            },
            None => serde_json::Map::new(),
        };
        obj.insert(key.to_string(), dist_val);
        let reenc = match rmp_serde::to_vec_named(&serde_json::Value::Object(obj)) {
            Ok(b) => b,
            Err(_) => return false,
        };
        self.add_node(node_id.to_string(), reenc);
        true
    }

    /// One-shot serializable gated remove (CONCEPT:EG-KG.txn.serializable-mutation-gate). See
    /// [`GraphTxn::remove_node_if`]; the decode → predicate re-check → remove runs
    /// under ONE topology write guard.
    pub fn remove_node_if(&self, node_id: &str, predicate: &eg_types::RowPredicate) -> bool {
        let removed = self.txn().remove_node_if(node_id, predicate);
        if removed {
            self.semantic_store.write().remove_embedding(node_id);
        }
        removed
    }

    /// One-shot atomic compare-and-set over a single `txn` (CONCEPT:EG-KG.compute.backend backend-
    /// agnostic atomic claim). See [`GraphTxn::compare_and_set_fields`] for the
    /// semantics; the whole read-modify-write runs under one topology write guard.
    pub fn compare_and_set_fields(
        &self,
        node_id: &str,
        conditions: &serde_json::Map<String, serde_json::Value>,
        updates: &serde_json::Map<String, serde_json::Value>,
    ) -> bool {
        self.txn()
            .compare_and_set_fields(node_id, conditions, updates)
    }

    /// One-shot serializable gated compare-and-set (CONCEPT:EG-KG.txn.serializable-mutation-gate). See
    /// [`GraphTxn::compare_and_set_fields_if`]; the decode → predicate re-check →
    /// conditional merge runs under ONE topology write guard.
    pub fn compare_and_set_fields_if(
        &self,
        node_id: &str,
        predicate: &eg_types::RowPredicate,
        conditions: &serde_json::Map<String, serde_json::Value>,
        updates: &serde_json::Map<String, serde_json::Value>,
    ) -> bool {
        self.txn()
            .compare_and_set_fields_if(node_id, predicate, conditions, updates)
    }

    /// Atomically claim the oldest pending node of `label` (CONCEPT:EG-KG.compute.atomically-claim-oldest-pending —
    /// native task queue). Scans `label`'s nodes (via the O(1) label index) for
    /// the smallest `seq` whose `status == "pending"`, then CAS-merges `updates`
    /// (condition `status == "pending"`) under one topology write guard. Returns
    /// `(node_id, updated_properties)` or `None` if nothing was claimable.
    ///
    /// Deterministic: the pick is a total order on the unique `seq` (ties broken
    /// by `node_id`) — a pure function of graph state — and `updates` carries no
    /// clock, so WAL replay and the Raft state machine reproduce the identical
    /// claim. This is the single-round-trip form of the client scan+CAS.
    pub fn claim_next_fields(
        &self,
        label: &str,
        updates: &serde_json::Map<String, serde_json::Value>,
    ) -> Option<(String, serde_json::Value)> {
        let rows = self.get_nodes_by_label(label, 0);
        let mut best: Option<(String, i64)> = None;
        for (id, blob) in &rows {
            let Ok(v) = decode_property_value(blob) else {
                continue;
            };
            let Some(obj) = v.as_object() else { continue };
            if obj.get("status").and_then(|s| s.as_str()) != Some("pending") {
                continue;
            }
            let seq = obj.get("seq").and_then(|s| s.as_i64()).unwrap_or(i64::MAX);
            let better = match &best {
                None => true,
                Some((bid, bseq)) => seq < *bseq || (seq == *bseq && id < bid),
            };
            if better {
                best = Some((id.clone(), seq));
            }
        }
        let (id, _) = best?;
        let mut conditions = serde_json::Map::new();
        conditions.insert(
            "status".to_string(),
            serde_json::Value::String("pending".to_string()),
        );
        if self.compare_and_set_fields(&id, &conditions, updates) {
            let blob = self.node_properties.get(&id)?;
            let val = decode_property_value(&blob).ok()?;
            Some((id, val))
        } else {
            None
        }
    }

    /// Atomically append one published message to EACH of `queues` under ONE
    /// topology write guard (CONCEPT:EG-KG.compute.message-broker-exchanges — message-broker enqueue on top of the
    /// KG-2.303 work-queue). For every queue: read+bump its durable monotonic
    /// `next_seq` counter node (`broker:seq:<queue>`) and append a pending message
    /// node labeled `qmsg:<queue>` (the label
    /// [`claim_next_fields`](Self::claim_next_fields) scans) carrying the hex-encoded
    /// payload, so consume/ack reuse the existing claim/CAS path unchanged. Returns
    /// how many queues were enqueued.
    ///
    /// Deterministic: the seq comes purely from the counter node's current state and
    /// no server clock is written, so replaying the same `Method::Publish` over the
    /// same pre-image (WAL / Raft state machine) reproduces byte-identical message
    /// nodes — the same discipline `claim_next_fields` follows. The whole fan-out
    /// runs under ONE `txn()` guard, so a publisher never observes a partial delivery.
    #[cfg(feature = "broker")]
    pub fn broker_enqueue(
        &self,
        queues: &[String],
        exchange: &str,
        routing_key: &str,
        payload_hex: &str,
    ) -> usize {
        self.broker_enqueue_ex(
            queues,
            exchange,
            routing_key,
            payload_hex,
            &serde_json::Map::new(),
        )
    }

    /// Policy-aware sibling of [`broker_enqueue`](Self::broker_enqueue) (CONCEPT:
    /// EG-277/278/279). Identical atomic multi-queue append, but merges `extra` (e.g.
    /// `priority` / `deliver_at` / `expires_at` / `x-death`) into each message node so
    /// the claim predicate can honor per-message priority, delay, and TTL. An EMPTY
    /// `extra` reproduces the exact EG-275 message shape — the plain `broker_enqueue`
    /// simply calls this with no extras, so the two paths never diverge. Deterministic:
    /// `extra` carries only caller-resolved absolute values (no server clock), so WAL
    /// replay of the originating `Publish`/`PublishEx` reproduces identical nodes.
    #[cfg(feature = "broker")]
    pub fn broker_enqueue_ex(
        &self,
        queues: &[String],
        exchange: &str,
        routing_key: &str,
        payload_hex: &str,
        extra: &serde_json::Map<String, serde_json::Value>,
    ) -> usize {
        let mut txn = self.txn();
        let mut delivered = 0usize;
        for q in queues {
            let seq_id = crate::broker::queue_seq_node_id(q);
            // Current counter (missing ⇒ start at 0), then persist the bump.
            let next = txn
                .node_row_map(&seq_id)
                .and_then(|m| m.get("next_seq").and_then(|s| s.as_i64()))
                .unwrap_or(0);
            let seq_props = serde_json::json!({
                "type": "BrokerQueueSeq",
                "queue": q,
                "next_seq": next + 1,
            });
            if let Ok(blob) = rmp_serde::to_vec_named(&seq_props) {
                txn.add_node(seq_id, blob);
            }
            // The pending message node — labeled so `claim_next_fields` delivers it.
            let mut msg_props = serde_json::Map::new();
            msg_props.insert(
                "type".into(),
                serde_json::Value::String(crate::broker::queue_msg_label(q)),
            );
            msg_props.insert("status".into(), serde_json::Value::String("pending".into()));
            msg_props.insert("seq".into(), serde_json::Value::from(next));
            msg_props.insert(
                "exchange".into(),
                serde_json::Value::String(exchange.into()),
            );
            msg_props.insert(
                "routing_key".into(),
                serde_json::Value::String(routing_key.into()),
            );
            msg_props.insert(
                "payload".into(),
                serde_json::Value::String(payload_hex.into()),
            );
            // Merge caller-resolved policy fields (priority/deliver_at/expires_at/…).
            for (k, v) in extra {
                msg_props.insert(k.clone(), v.clone());
            }
            let msg_props = serde_json::Value::Object(msg_props);
            if let Ok(blob) = rmp_serde::to_vec_named(&msg_props) {
                txn.add_node(crate::broker::message_node_id(q, next), blob);
                delivered += 1;
            }
        }
        // Release the write guard, then invalidate the lazy label index (CONCEPT:
        // KG-2.176) so the new `qmsg:<queue>` message nodes are visible to the very
        // next `claim_next_fields` — a raw `txn().add_node` does NOT bump it, unlike
        // the dispatch shell's post-write `mark_dirty`.
        drop(txn);
        if delivered > 0 {
            self.mark_dirty();
        }
        delivered
    }

    /// Append one RETAINED message to `stream` and return its monotonic offset
    /// (CONCEPT:EG-KG.compute.replayable-append-log — replayable append-log streams). Under ONE topology write
    /// guard: read+bump the stream's durable `next_offset` counter node
    /// (`broker:soff:<stream>`) and append a message node labeled `smsg:<stream>`
    /// carrying the hex payload + `ts = now_ms`. Unlike [`broker_enqueue`](Self::
    /// broker_enqueue) the message is NEVER consumed/deleted by a read — it is served
    /// by offset until an explicit retention trim removes it.
    ///
    /// Deterministic: the offset comes purely from the counter node's current state and
    /// `now_ms` is explicit, so replaying `Method::StreamPublish` over the same
    /// pre-image reproduces a byte-identical node — the same discipline the queue
    /// enqueue follows. Bumps the lazy label index so the appended message is visible
    /// to the very next `stream_read`.
    #[cfg(feature = "broker")]
    pub fn stream_append(&self, stream: &str, payload_hex: &str, now_ms: u64) -> i64 {
        let mut txn = self.txn();
        let off_id = crate::broker::stream_offset_node_id(stream);
        let offset = txn
            .node_row_map(&off_id)
            .and_then(|m| m.get("next_offset").and_then(|s| s.as_i64()))
            .unwrap_or(0);
        let off_props = serde_json::json!({
            "type": crate::broker::STREAM_OFFSET_TYPE,
            "stream": stream,
            "next_offset": offset + 1,
        });
        if let Ok(blob) = rmp_serde::to_vec_named(&off_props) {
            txn.add_node(off_id, blob);
        }
        let msg_props = serde_json::json!({
            "type": crate::broker::stream_msg_label(stream),
            "offset": offset,
            "payload": payload_hex,
            "ts": now_ms,
        });
        if let Ok(blob) = rmp_serde::to_vec_named(&msg_props) {
            txn.add_node(crate::broker::stream_msg_node_id(stream, offset), blob);
        }
        drop(txn);
        self.mark_dirty();
        offset
    }

    /// Remove a set of stream message nodes under ONE topology write guard
    /// (CONCEPT:EG-KG.compute.replayable-append-log — retention trim). Returns how many of `ids` were present and
    /// removed. Backs [`broker::stream_trim`](crate::broker::stream_trim): the caller
    /// resolves the offset-ordered drop set from the retention policy, so this is a
    /// deterministic bulk delete that replays byte-identically.
    #[cfg(feature = "broker")]
    pub fn stream_trim_nodes(&self, ids: &[String]) -> usize {
        let mut txn = self.txn();
        let mut removed = 0usize;
        for id in ids {
            if txn.node_properties.contains_key(id) {
                txn.remove_node(id.clone());
                removed += 1;
            }
        }
        drop(txn);
        if removed > 0 {
            self.mark_dirty();
        }
        removed
    }

    /// Issue the next value of a broker-wide monotonic counter node and persist the
    /// bump under ONE topology write guard (CONCEPT:EG-KG.compute.publisher-confirms-consumer-qos — publisher-confirm +
    /// consumer delivery tags). The counter's `last_tag` starts at 0, so the FIRST
    /// issued tag is `1` and every subsequent call returns a strictly greater value.
    /// Deterministic: the value derives purely from the counter node, so replaying the
    /// originating Method reproduces the identical tag.
    #[cfg(feature = "broker")]
    pub fn broker_next_counter(&self, node_id: &str, type_str: &str) -> i64 {
        let mut txn = self.txn();
        let last = txn
            .node_row_map(node_id)
            .and_then(|m| m.get("last_tag").and_then(|s| s.as_i64()))
            .unwrap_or(0);
        let issued = last + 1;
        let props = serde_json::json!({
            "type": type_str,
            "last_tag": issued,
        });
        if let Ok(blob) = rmp_serde::to_vec_named(&props) {
            txn.add_node(node_id.to_string(), blob);
        }
        drop(txn);
        self.mark_dirty();
        issued
    }

    /// Atomically fence a broker claim and install its fresh positive delivery tag.
    ///
    /// The candidate scan may happen outside the write guard, so this method
    /// revalidates the current status and (for a reclaim) the exact expired lease.
    /// Retiring the prior lookup, bumping the durable counter, stamping the message,
    /// and adding the new O(1) reverse lookup all share one topology transaction.
    #[cfg(feature = "broker")]
    #[allow(clippy::too_many_arguments)]
    pub fn broker_claim_delivery(
        &self,
        node_id: &str,
        queue: &str,
        group: &str,
        consumer: &str,
        expected_status: &str,
        expected_lease_until: Option<u64>,
        now_ms: u64,
        lease_ms: u64,
    ) -> Option<serde_json::Value> {
        if consumer.trim().is_empty() {
            return None;
        }
        let mut txn = self.txn();
        let mut properties = txn.node_row_map(node_id)?;
        if properties.get("status").and_then(serde_json::Value::as_str) != Some(expected_status) {
            return None;
        }
        if expected_status == "claimed" {
            let current_lease = properties
                .get("lease_until")
                .and_then(serde_json::Value::as_u64);
            if current_lease != expected_lease_until
                || current_lease.map(|until| until > now_ms).unwrap_or(true)
            {
                return None;
            }
        } else if expected_status != "pending" {
            return None;
        }

        let counter_id = crate::broker::dtag_seq_node_id();
        let last_tag = match txn.node_row_map(&counter_id) {
            None => 0,
            Some(row)
                if row.get("type").and_then(serde_json::Value::as_str)
                    == Some(crate::broker::BROKER_COUNTER_TYPE) =>
            {
                let value = row.get("last_tag").and_then(serde_json::Value::as_i64)?;
                if value < 0 {
                    return None;
                }
                value
            }
            Some(_) => return None,
        };
        let delivery_tag = last_tag.checked_add(1)?;
        if delivery_tag <= 0 {
            return None;
        }
        let prior_tag = match properties.get("delivery_tag") {
            None | Some(serde_json::Value::Null) => None,
            Some(value) => match value.as_i64() {
                Some(tag) if tag > 0 => Some(tag),
                _ => return None,
            },
        };
        if expected_status == "claimed"
            && (prior_tag.is_none() || prior_tag.is_some_and(|tag| tag > last_tag))
        {
            return None;
        }
        let lookup_id = crate::broker::dtag_lookup_node_id(delivery_tag);
        if txn.has_node(&lookup_id) {
            return None;
        }
        // Clear the old generation in the owned row before stamping the new one.
        // The prior lookup is retired below before any new lookup is installed.
        properties.insert("delivery_tag".into(), serde_json::Value::Null);
        let delivery_count = match properties.get("delivery_count") {
            None => 0,
            Some(value) => {
                let count = value.as_i64()?;
                if count < 0 {
                    return None;
                }
                count
            }
        };
        let next_delivery_count = delivery_count.checked_add(1)?;
        let lease_until = if lease_ms > 0 {
            Some(now_ms.checked_add(lease_ms)?)
        } else {
            None
        };
        properties.insert("status".into(), serde_json::Value::String("claimed".into()));
        properties.insert(
            "owner_group".into(),
            serde_json::Value::String(group.to_string()),
        );
        properties.insert(
            "owner_consumer".into(),
            serde_json::Value::String(consumer.to_string()),
        );
        properties.insert(
            "delivery_count".into(),
            serde_json::Value::from(next_delivery_count),
        );
        properties.insert("claimed_at".into(), serde_json::Value::from(now_ms));
        properties.insert(
            "lease_until".into(),
            lease_until.map_or(serde_json::Value::Null, serde_json::Value::from),
        );
        properties.insert("delivery_tag".into(), serde_json::Value::from(delivery_tag));

        let counter = serde_json::json!({
            "type": crate::broker::BROKER_COUNTER_TYPE,
            "last_tag": delivery_tag,
        });
        let lookup = serde_json::json!({
            "type": crate::broker::DTAG_LOOKUP_TYPE,
            "node_id": node_id,
            "queue": queue,
            "owner_consumer": consumer,
        });
        let message_value = serde_json::Value::Object(properties);
        let counter_blob = rmp_serde::to_vec_named(&counter).ok()?;
        let message_blob = rmp_serde::to_vec_named(&message_value).ok()?;
        let lookup_blob = rmp_serde::to_vec_named(&lookup).ok()?;

        if let Some(tag) = prior_tag {
            txn.remove_node(crate::broker::dtag_lookup_node_id(tag));
        }
        txn.add_node(counter_id, counter_blob);
        txn.add_node(node_id.to_string(), message_blob);
        txn.add_node(lookup_id, lookup_blob);
        Some(message_value)
    }

    /// Atomically acknowledge only the currently claimed delivery owned by
    /// `consumer`. A stale lookup is retired, but it can never remove a reclaimed
    /// message carrying a newer tag. An owner mismatch leaves the live lookup intact.
    #[cfg(feature = "broker")]
    pub fn broker_ack_delivery_tag(&self, delivery_tag: i64, consumer: &str) -> bool {
        if delivery_tag <= 0 || consumer.trim().is_empty() {
            return false;
        }
        let lookup_id = crate::broker::dtag_lookup_node_id(delivery_tag);
        let mut txn = self.txn();
        let Some(lookup) = txn.node_row_map(&lookup_id) else {
            return false;
        };
        if lookup.get("type").and_then(serde_json::Value::as_str)
            != Some(crate::broker::DTAG_LOOKUP_TYPE)
        {
            txn.remove_node(lookup_id);
            return false;
        }
        if lookup
            .get("owner_consumer")
            .and_then(serde_json::Value::as_str)
            != Some(consumer)
        {
            return false;
        }
        let node_id = lookup
            .get("node_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string();
        if node_id.is_empty() {
            txn.remove_node(lookup_id);
            return false;
        }
        let Some(properties) = txn.node_row_map(&node_id) else {
            txn.remove_node(lookup_id);
            return false;
        };
        let current = properties.get("status").and_then(serde_json::Value::as_str)
            == Some("claimed")
            && properties
                .get("delivery_tag")
                .and_then(serde_json::Value::as_i64)
                == Some(delivery_tag);
        if !current {
            txn.remove_node(lookup_id);
            return false;
        }
        if properties
            .get("owner_consumer")
            .and_then(serde_json::Value::as_str)
            != Some(consumer)
        {
            return false;
        }
        txn.remove_node(lookup_id);
        txn.remove_node(node_id);
        true
    }

    /// Extend a still-live claimed delivery lease for its current owner. Every
    /// comparison and the write occur under one guard; caller-supplied time keeps
    /// replay deterministic.
    #[cfg(feature = "broker")]
    pub fn broker_renew_delivery_tag(
        &self,
        delivery_tag: i64,
        consumer: &str,
        now_ms: u64,
        lease_ms: u64,
    ) -> bool {
        if delivery_tag <= 0 || consumer.trim().is_empty() || lease_ms == 0 {
            return false;
        }
        let Some(renewed_until) = now_ms.checked_add(lease_ms) else {
            return false;
        };
        let lookup_id = crate::broker::dtag_lookup_node_id(delivery_tag);
        let mut txn = self.txn();
        let Some(lookup) = txn.node_row_map(&lookup_id) else {
            return false;
        };
        if lookup.get("type").and_then(serde_json::Value::as_str)
            != Some(crate::broker::DTAG_LOOKUP_TYPE)
        {
            txn.remove_node(lookup_id);
            return false;
        }
        if lookup
            .get("owner_consumer")
            .and_then(serde_json::Value::as_str)
            != Some(consumer)
        {
            return false;
        }
        let node_id = lookup
            .get("node_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string();
        let Some(mut properties) = txn.node_row_map(&node_id) else {
            txn.remove_node(lookup_id);
            return false;
        };
        let current = properties.get("status").and_then(serde_json::Value::as_str)
            == Some("claimed")
            && properties
                .get("delivery_tag")
                .and_then(serde_json::Value::as_i64)
                == Some(delivery_tag);
        if !current {
            txn.remove_node(lookup_id);
            return false;
        }
        if properties
            .get("owner_consumer")
            .and_then(serde_json::Value::as_str)
            != Some(consumer)
        {
            return false;
        }
        let current_lease_until = properties
            .get("lease_until")
            .and_then(serde_json::Value::as_u64);
        let Some(current_lease_until) = current_lease_until else {
            return false;
        };
        if current_lease_until <= now_ms {
            return false;
        }
        if renewed_until <= current_lease_until {
            return false;
        }
        properties.insert("lease_until".into(), serde_json::Value::from(renewed_until));
        let value = serde_json::Value::Object(properties);
        let Ok(blob) = rmp_serde::to_vec_named(&value) else {
            return false;
        };
        txn.add_node(node_id, blob);
        true
    }

    /// Atomically fence and end a tag-addressed delivery. Requeue is applied in
    /// the same transaction; a terminal transition removes the original and returns
    /// its immutable properties so the broker layer can perform DLX routing in the
    /// surrounding staged durable mutation.
    #[cfg(feature = "broker")]
    pub fn broker_nack_delivery_tag(
        &self,
        delivery_tag: i64,
        consumer: &str,
        requeue: bool,
    ) -> BrokerNackTransition {
        if delivery_tag <= 0 || consumer.trim().is_empty() {
            return BrokerNackTransition::Absent;
        }
        let lookup_id = crate::broker::dtag_lookup_node_id(delivery_tag);
        let mut txn = self.txn();
        let Some(lookup) = txn.node_row_map(&lookup_id) else {
            return BrokerNackTransition::Absent;
        };
        if lookup.get("type").and_then(serde_json::Value::as_str)
            != Some(crate::broker::DTAG_LOOKUP_TYPE)
        {
            txn.remove_node(lookup_id);
            return BrokerNackTransition::Absent;
        }
        if lookup
            .get("owner_consumer")
            .and_then(serde_json::Value::as_str)
            != Some(consumer)
        {
            return BrokerNackTransition::Absent;
        }
        let node_id = lookup
            .get("node_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string();
        let queue = lookup
            .get("queue")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string();
        if node_id.is_empty() || queue.is_empty() {
            txn.remove_node(lookup_id);
            return BrokerNackTransition::Absent;
        }
        let Some(mut properties) = txn.node_row_map(&node_id) else {
            txn.remove_node(lookup_id);
            return BrokerNackTransition::Absent;
        };
        let current = properties.get("status").and_then(serde_json::Value::as_str)
            == Some("claimed")
            && properties
                .get("delivery_tag")
                .and_then(serde_json::Value::as_i64)
                == Some(delivery_tag);
        if !current {
            txn.remove_node(lookup_id);
            return BrokerNackTransition::Absent;
        }
        if properties
            .get("owner_consumer")
            .and_then(serde_json::Value::as_str)
            != Some(consumer)
        {
            return BrokerNackTransition::Absent;
        }

        let Some(delivery_count) = properties
            .get("delivery_count")
            .and_then(serde_json::Value::as_i64)
            .filter(|count| *count >= 0)
        else {
            return BrokerNackTransition::Absent;
        };
        let max_delivery_count = txn
            .node_row_map(&crate::broker::queue_policy_node_id(&queue))
            .and_then(|policy| {
                policy
                    .get("max_delivery_count")
                    .and_then(serde_json::Value::as_u64)
            });
        let under_max = max_delivery_count
            .map(|max| (delivery_count as i128) < (max as i128))
            .unwrap_or(true);

        if requeue && under_max {
            properties.insert("status".into(), serde_json::Value::String("pending".into()));
            properties.insert("lease_until".into(), serde_json::Value::Null);
            properties.insert("owner_consumer".into(), serde_json::Value::Null);
            properties.insert("owner_group".into(), serde_json::Value::Null);
            properties.insert("delivery_tag".into(), serde_json::Value::Null);
            let value = serde_json::Value::Object(properties);
            let Ok(blob) = rmp_serde::to_vec_named(&value) else {
                return BrokerNackTransition::Absent;
            };
            txn.remove_node(lookup_id);
            txn.add_node(node_id, blob);
            return BrokerNackTransition::Requeued;
        }

        let value = serde_json::Value::Object(properties);
        txn.remove_node(lookup_id);
        txn.remove_node(node_id.clone());
        BrokerNackTransition::Terminal {
            node_id,
            queue,
            properties: value,
        }
    }

    /// Return an expired delivery to pending while retiring its tag generation.
    /// Used by the proactive sweep; the exact observed lease is revalidated so a
    /// concurrent renewal cannot be undone.
    #[cfg(feature = "broker")]
    pub fn broker_release_expired_delivery(
        &self,
        node_id: &str,
        expected_lease_until: Option<u64>,
        now_ms: u64,
    ) -> bool {
        let mut txn = self.txn();
        let Some(mut properties) = txn.node_row_map(node_id) else {
            return false;
        };
        let Some(current_lease) = properties
            .get("lease_until")
            .and_then(serde_json::Value::as_u64)
        else {
            return false;
        };
        if properties.get("status").and_then(serde_json::Value::as_str) != Some("claimed")
            || Some(current_lease) != expected_lease_until
            || current_lease > now_ms
        {
            return false;
        }
        let Some(tag) = properties
            .get("delivery_tag")
            .and_then(serde_json::Value::as_i64)
            .filter(|tag| *tag > 0)
        else {
            return false;
        };
        txn.remove_node(crate::broker::dtag_lookup_node_id(tag));
        properties.insert("status".into(), serde_json::Value::String("pending".into()));
        properties.insert("lease_until".into(), serde_json::Value::Null);
        properties.insert("owner_consumer".into(), serde_json::Value::Null);
        properties.insert("owner_group".into(), serde_json::Value::Null);
        properties.insert("delivery_tag".into(), serde_json::Value::Null);
        let value = serde_json::Value::Object(properties);
        let Ok(blob) = rmp_serde::to_vec_named(&value) else {
            return false;
        };
        txn.add_node(node_id.to_string(), blob);
        true
    }

    /// Idempotent-producer dedup check + high-water-mark bump under ONE topology write
    /// guard (CONCEPT:EG-KG.ingest.effectively-once-publish — effectively-once publish). `node_id` is the producer's
    /// durable dedup node (`broker:producer:<producer_id>`) holding a monotonic
    /// `last_seq`. Returns `true` when `seq` is NEW (strictly above the current mark) —
    /// recording it as the new mark — and `false` when `seq` is a DUPLICATE (at/under
    /// the mark), writing nothing. The mark starts at `-1`, so the first valid `seq`
    /// (0) is accepted. Deterministic: the decision derives purely from the node's
    /// current state, so replaying the originating Method reproduces the identical
    /// mark and duplicate-verdict.
    #[cfg(feature = "broker")]
    pub fn broker_producer_check_and_record(&self, node_id: &str, seq: i64) -> bool {
        let mut txn = self.txn();
        let last = txn
            .node_row_map(node_id)
            .and_then(|m| m.get("last_seq").and_then(|s| s.as_i64()))
            .unwrap_or(-1);
        if seq <= last {
            return false; // duplicate — drop the (unwritten) txn, no state change
        }
        let props = serde_json::json!({
            "type": crate::broker::PRODUCER_SEQ_TYPE,
            "last_seq": seq,
        });
        if let Ok(blob) = rmp_serde::to_vec_named(&props) {
            txn.add_node(node_id.to_string(), blob);
        }
        drop(txn);
        self.mark_dirty();
        true
    }

    /// One-shot non-destructive edge invalidation (CONCEPT:AU-KG.ingest.list-durable-media). See
    /// [`GraphTxn::invalidate_edge`]; runs under one topology write guard.
    pub fn invalidate_edge(
        &self,
        source_id: &str,
        target_id: &str,
        relationship: &str,
        invalid_at: u64,
        tx_now: u64,
    ) -> usize {
        self.txn()
            .invalidate_edge(source_id, target_id, relationship, invalid_at, tx_now)
    }

    /// One-shot atomic edge supersession (CONCEPT:AU-KG.ingest.list-durable-media). See
    /// [`GraphTxn::supersede_edge`]; the close-prior + insert-new run under ONE guard.
    #[allow(clippy::too_many_arguments)]
    pub fn supersede_edge(
        &self,
        new_source: String,
        new_target: String,
        new_properties_msgpack: Vec<u8>,
        prior_source: &str,
        prior_target: &str,
        prior_relationship: &str,
        valid_at: u64,
        tx_now: u64,
    ) -> Result<(), String> {
        self.txn().supersede_edge(
            new_source,
            new_target,
            new_properties_msgpack,
            prior_source,
            prior_target,
            prior_relationship,
            valid_at,
            tx_now,
        )
    }

    pub fn has_node(&self, node_id: &str) -> bool {
        self.topo.read().node_map.contains_key(node_id)
    }

    pub fn get_nodes(&self) -> Vec<(String, Vec<u8>)> {
        self.node_properties
            .iter()
            .map(|e| (e.key().clone(), (**e.value()).clone()))
            .collect()
    }

    /// Return at most `limit` nodes (id, properties) whose `type`/`label`/`labels`
    /// matches `label`; `limit == 0` means no cap. Rows are ordered by node id.
    /// Scans in-engine but bounds the returned payload, so a
    /// `MATCH (n:Label) … LIMIT k` caller no longer materializes every node's
    /// properties over the wire.
    ///
    /// An EMPTY `label` (CONCEPT:EG-KG.query.unlabeled-scan-limit-pushdown) means "no label filter" — a bounded scan of
    /// the WHOLE node store, still honouring `limit`. This gives an unlabeled
    /// `MATCH (n) … LIMIT k` a genuinely bounded RPC to call instead of falling
    /// back to the unbounded `GetNodes` dump (which trips the `RESULT_TOO_LARGE`
    /// overload guard on a large graph even when the caller only wanted `k` rows).
    pub fn get_nodes_by_label(&self, label: &str, limit: usize) -> Vec<(String, Vec<u8>)> {
        self.get_nodes_by_label_page(label, None, limit)
    }

    /// Deterministic keyset page over [`Self::get_nodes_by_label`]. `after` is an
    /// exclusive node-id cursor. This is a live committed-state scan rather than
    /// a cross-request snapshot; synchronizers must serialize a reconcile pass
    /// with writers that could insert an older id.
    pub fn get_nodes_by_label_page(
        &self,
        label: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Vec<(String, Vec<u8>)> {
        if label.is_empty() {
            return self.collect_unlabeled(after, limit);
        }
        // CONCEPT:EG-KG.compute.consult-lazy — consult the lazy `label → ids` index so a label
        // lookup is an O(1) map hit instead of a full DashMap scan that
        // deserializes every node. The cached map is invalidated by `mark_dirty()`
        // after any successful write (the same dirty flag the checkpoint uses), so
        // it never serves a stale view across a mutation.
        {
            let guard = self.label_index.read();
            if let Some(idx) = guard.as_ref() {
                return Self::collect_by_label(idx, &self.node_properties, label, after, limit);
            }
        }
        // Stamp the freshly built index at the version it reflects (W1.6/P7,
        // CONCEPT:EG-KG.storage.incremental-index-stamp). The scan reads the LIVE `node_properties`, so
        // the built map is at least as current as `built_at`; a later write's `mark_dirty` sees
        // the stamp predates its new version and drops the now-stale index. Captured before the
        // scan so the stamp is a safe lower bound (an under-estimate only risks a rebuild).
        let built_at = self.version();
        let built = self.build_label_index();
        let out = Self::collect_by_label(&built, &self.node_properties, label, after, limit);
        {
            let mut guard = self.label_index.write();
            self.index_stamps
                .label
                .store(built_at, std::sync::atomic::Ordering::Release);
            *guard = Some(built);
        }
        out
    }

    /// The unlabeled-scan leg of [`Self::get_nodes_by_label`] (CONCEPT:EG-KG.query.unlabeled-scan-limit-pushdown):
    /// every node in deterministic id order, honouring `limit` (`0` = uncapped).
    /// Only ids are sorted; property blobs are cloned for the requested page.
    fn collect_unlabeled(&self, after: Option<&str>, limit: usize) -> Vec<(String, Vec<u8>)> {
        // Build and publish while holding the topology read guard. Every
        // structural writer takes the matching write guard, so it cannot slip a
        // node add/remove between this snapshot and publication; its later
        // mark_dirty invalidates the cache before the next committed-state read.
        if self.node_id_index.read().is_none() {
            let built_at = self.version();
            let topo = self.topo.read();
            let mut cache = self.node_id_index.write();
            if cache.is_none() {
                let mut ids: Vec<String> = topo.node_map.keys().cloned().collect();
                ids.sort_unstable();
                // Stamp at the built version (W1.6/P7) so `mark_dirty` preserves this scan cache
                // when a later add/remove is incrementally applied, and drops it otherwise.
                self.index_stamps
                    .node_id
                    .store(built_at, std::sync::atomic::Ordering::Release);
                *cache = Some(ids);
            }
        }
        let cache = self.node_id_index.read();
        let ids = cache.as_deref().unwrap_or_default();
        let start = after.map_or(0, |cursor| ids.partition_point(|id| id.as_str() <= cursor));
        let capacity = if limit == 0 {
            ids.len().saturating_sub(start)
        } else {
            limit.min(ids.len().saturating_sub(start))
        };
        let mut out: Vec<(String, Vec<u8>)> = Vec::with_capacity(capacity);
        for id in ids.iter().skip(start) {
            if limit != 0 && out.len() >= limit {
                break;
            }
            if let Some(props) = self.node_properties.get(id) {
                out.push((id.clone(), (**props.value()).clone()));
            }
        }
        out
    }

    /// Materialize the `(id, properties)` rows for `label` from a built index,
    /// honouring `limit` (`0` = uncapped). Skips ids that have since been removed
    /// from the property store (defensive against an in-flight removal that hasn't
    /// yet invalidated the index).
    fn collect_by_label(
        index: &HashMap<String, Vec<String>>,
        node_properties: &DashMap<String, Arc<Vec<u8>>>,
        label: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Vec<(String, Vec<u8>)> {
        let Some(ids) = index.get(label) else {
            return Vec::new();
        };
        let start = after.map_or(0, |cursor| ids.partition_point(|id| id.as_str() <= cursor));
        // Sized allocation instead of doubling growth, mirroring
        // `collect_unlabeled`'s identical capacity computation two functions
        // above — the caller-requested page size upper-bounds the true count
        // (some ids may since have been removed from `node_properties`).
        let capacity = if limit == 0 {
            ids.len().saturating_sub(start)
        } else {
            limit.min(ids.len().saturating_sub(start))
        };
        let mut out: Vec<(String, Vec<u8>)> = Vec::with_capacity(capacity);
        for id in ids.iter().skip(start) {
            if limit != 0 && out.len() >= limit {
                break;
            }
            if let Some(props) = node_properties.get(id) {
                out.push((id.clone(), (**props.value()).clone()));
            }
        }
        out
    }

    /// Scan the node store once and build the secondary label index
    /// (CONCEPT:EG-KG.compute.consult-lazy): each node id is filed under EVERY label it carries.
    /// The label set is read from exactly the fields `get_nodes_by_label` matched
    /// before this index existed, so the two never diverge:
    ///   * `type` (canonical), `node_type` (the field the Python client writes —
    ///     graph_compute normalises `type` ⇒ `node_type` on read-back, so the
    ///     index MUST honour both or a label-scoped MATCH under-returns every
    ///     node_type-keyed node), `label`, and the multi-valued `labels` array.
    fn build_label_index(&self) -> HashMap<String, Vec<String>> {
        // O(V) full rebuild — the exact cost W1.6/P7 incremental maintenance exists to avoid.
        // Counting + logging it is the "rebuild count under ingest ~0" observability signal.
        let n = self
            .index_rebuilds
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        tracing::debug!(
            target: "epistemic_graph::index_rebuild",
            index = "label",
            nodes = self.node_properties.len(),
            total_rebuilds = n,
            "full label-index rebuild (cold cache); warm ingest maintains it incrementally"
        );
        let mut index: HashMap<String, Vec<String>> = HashMap::new();
        for entry in self.node_properties.iter() {
            let Ok(val) = decode_property_value(entry.value().as_slice()) else {
                continue;
            };
            let id = entry.key();
            for key in ["type", "node_type", "label"] {
                if let Some(lbl) = val.get(key).and_then(|v| v.as_str()) {
                    index.entry(lbl.to_string()).or_default().push(id.clone());
                }
            }
            if let Some(arr) = val.get("labels").and_then(|v| v.as_array()) {
                for x in arr {
                    if let Some(lbl) = x.as_str() {
                        index.entry(lbl.to_string()).or_default().push(id.clone());
                    }
                }
            }
        }
        // A node carrying the same value on two of {type,node_type,label} (or a
        // duplicated `labels` entry) would otherwise be listed twice for that
        // label; dedup so the returned rows match the pre-index 1-node-1-row scan.
        for ids in index.values_mut() {
            ids.sort_unstable();
            ids.dedup();
        }
        index
    }

    // ── secondary property index (CONCEPT:EG-KG.query.concept-12) ──────────────────────────

    /// Node ids whose property `key` equals `value`, via the bounded, demand-driven
    /// secondary property index (CONCEPT:EG-KG.query.concept-12). Equality only. The first call
    /// for a given `key` indexes it (subject to the configured cap) and caches the
    /// `value → ids` map; subsequent calls are an O(1) map hit. The cache is
    /// invalidated by `mark_dirty()` after any write, so it never serves a stale
    /// view across a mutation.
    ///
    /// `value` is compared against the canonical string form of the stored property
    /// (see [`Self::property_value_key`]): strings match their text, scalars their
    /// `to_string()`. Returns ids sorted + deduped (one id per node).
    ///
    /// Returns `None` when `key` is NOT (and cannot be) indexed under the bound —
    /// the caller must then full-scan. Returns `Some(vec)` (possibly empty) when the
    /// key IS indexed: an empty vec means "indexed, no node has that value".
    pub fn nodes_by_property(&self, key: &str, value: &str) -> Option<Vec<String>> {
        // Fast path: key already indexed in the cached map.
        {
            let guard = self.property_index.read();
            if let Some(idx) = guard.as_ref() {
                if let Some(by_value) = idx.keys.get(key) {
                    return Some(by_value.get(value).cloned().unwrap_or_default());
                }
            }
        }
        // Slow path: build/extend the index to cover `key` (bounded), then answer. Stamp the
        // index at the pre-build version (W1.6/P7) so `mark_dirty` preserves it across a
        // subsequently incrementally-maintained write. `fetch_max` never regresses a stamp a
        // concurrent maintenance already advanced (the newly-scanned key reflects live data, so
        // the pre-build version is a safe lower bound).
        let built_at = self.version();
        let mut guard = self.property_index.write();
        let idx = guard.get_or_insert_with(PropertyIndex::default);
        let indexed = self.ensure_key_indexed(idx, key);
        self.index_stamps
            .property
            .fetch_max(built_at, std::sync::atomic::Ordering::AcqRel);
        indexed?;
        Some(
            idx.keys
                .get(key)
                .and_then(|by_value| by_value.get(value).cloned())
                .unwrap_or_default(),
        )
    }

    /// Composite equality lookup (CONCEPT:EG-KG.query.concept-12): node ids matching EVERY
    /// `(key, value)` pair, as the intersection of the per-key equality sets. Used
    /// for pushdown of multiple `col = literal` predicates ANDed together. Returns
    /// `None` if ANY key is not (and cannot be) indexed under the bound — the caller
    /// then full-scans the whole predicate; partial pushdown would risk divergence.
    pub fn nodes_by_properties(&self, pairs: &[(&str, &str)]) -> Option<Vec<String>> {
        if pairs.is_empty() {
            return None;
        }
        // Resolve each predicate via the single-key path (each ensures its key is
        // indexed); intersect the resulting id sets. Start from the smallest set.
        let mut sets: Vec<Vec<String>> = Vec::with_capacity(pairs.len());
        for (k, v) in pairs {
            sets.push(self.nodes_by_property(k, v)?);
        }
        sets.sort_by_key(|s| s.len());
        let mut acc = sets.remove(0);
        for s in &sets {
            // Every property posting is already sorted + deduplicated when built.
            // Intersect with the classic two-pointer merge instead of allocating a
            // fresh HashSet for every predicate. This is deterministic O(a+b) time
            // with O(1) scratch (beyond the reused result vector).
            let mut write = 0usize;
            let mut left = 0usize;
            let mut right = 0usize;
            while left < acc.len() && right < s.len() {
                match acc[left].cmp(&s[right]) {
                    std::cmp::Ordering::Less => left += 1,
                    std::cmp::Ordering::Greater => right += 1,
                    std::cmp::Ordering::Equal => {
                        if write != left {
                            acc.swap(write, left);
                        }
                        write += 1;
                        left += 1;
                        right += 1;
                    }
                }
            }
            acc.truncate(write);
            if acc.is_empty() {
                break;
            }
        }
        Some(acc)
    }

    /// Ensure `key` is present in the property index, honouring the bound. Returns
    /// `Some(())` if the key is (now) indexed, `None` if the cap is full and the key
    /// is not already present (so the caller must full-scan). Pre-seeds any keys
    /// named in `EPISTEMIC_GRAPH_INDEXED_PROPERTIES` on the first build.
    // `map_entry` would have us use `entry().or_insert_with`, but the bounded cap
    // means we must NOT insert when the map is full — `entry` always inserts, so the
    // explicit `contains_key`/`len`-then-`insert` is the correct shape here.
    #[allow(clippy::map_entry)]
    fn ensure_key_indexed(&self, idx: &mut PropertyIndex, key: &str) -> Option<()> {
        let cap = Self::max_indexed_properties();
        // First-build pre-seed: index every env-named key (subject to the cap).
        if idx.keys.is_empty() {
            for seed in Self::seed_indexed_properties() {
                if idx.keys.len() >= cap || idx.keys.contains_key(&seed) {
                    continue;
                }
                let map = self.build_property_value_map(&seed);
                idx.keys.insert(seed, map);
            }
        }
        if idx.keys.contains_key(key) {
            return Some(());
        }
        if idx.keys.len() >= cap {
            // Cap reached and this key isn't indexed — refuse (full-scan fallback).
            return None;
        }
        let map = self.build_property_value_map(key);
        idx.keys.insert(key.to_string(), map);
        Some(())
    }

    /// Scan the node store once and build the `value → node ids` map for one
    /// property `key`. Each node is filed under the canonical string form of its
    /// `key` value (if present and a scalar). Ids are sorted + deduped.
    fn build_property_value_map(&self, key: &str) -> HashMap<String, Vec<String>> {
        let n = self
            .index_rebuilds
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        tracing::debug!(
            target: "epistemic_graph::index_rebuild",
            index = "property",
            key,
            nodes = self.node_properties.len(),
            total_rebuilds = n,
            "full property-value-map rebuild (cold key); warm ingest maintains it incrementally"
        );
        let mut by_value: HashMap<String, Vec<String>> = HashMap::new();
        for entry in self.node_properties.iter() {
            let Ok(val) = decode_property_value(entry.value().as_slice()) else {
                continue;
            };
            if let Some(vk) = val.get(key).and_then(Self::property_value_key) {
                by_value.entry(vk).or_default().push(entry.key().clone());
            }
        }
        for ids in by_value.values_mut() {
            ids.sort_unstable();
            ids.dedup();
        }
        by_value
    }

    /// Canonical string key for an equality-indexable property value: strings index
    /// under their text, bools/numbers under their `to_string()`. Arrays/objects/
    /// null are NOT equality-indexable (return `None`) — they fall to a full scan,
    /// matching how an equality predicate on such a column behaves.
    ///
    /// `pub` (CONCEPT:EG-KG.storage.index-manager-seam) so a caller resolving a WHERE/inline-prop
    /// literal to a `Predicate::PropertyEq` value (eg-query's Cypher executor) canonicalizes
    /// EXACTLY the same way this index was built — the single source of truth, not a
    /// second implementation a future edit here could silently drift out of sync with.
    pub fn property_value_key(v: &serde_json::Value) -> Option<String> {
        match v {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Bool(b) => Some(b.to_string()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            _ => None,
        }
    }

    /// Cap on the number of distinct property keys ever indexed
    /// (`EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES`, default 32). `0` is treated as the
    /// default rather than "disable" so a misconfigured empty value can't silently
    /// turn the index off.
    fn max_indexed_properties() -> usize {
        std::env::var("EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_MAX_INDEXED_PROPERTIES)
    }

    /// Property keys to pre-seed into the index on first build
    /// (`EPISTEMIC_GRAPH_INDEXED_PROPERTIES`, comma-separated). Empty when unset.
    fn seed_indexed_properties() -> Vec<String> {
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

    // ── inverted JSONPath path-index (CONCEPT:EG-KG.compute.json-deep-indexing) ─────────────────────────

    /// Node ids whose JSONPath `path` resolves to a scalar equal to `value`, via the
    /// bounded, demand-driven inverted path-index (CONCEPT:EG-KG.compute.json-deep-indexing). This is the
    /// index-accelerated backing for `WHERE props->>'k' = 'v'` (and `props->'k' = …`).
    /// `value` is compared against the canonical string form of the stored scalar (see
    /// [`crate::jsonpath::canonical_scalar`]), so a numeric `30` matches `"30"`.
    ///
    /// Returns `None` when `path` is not (and cannot be) indexed under the bound — the
    /// caller must then full-scan. Returns `Some(vec)` (possibly empty) when the path IS
    /// indexed. The cache is invalidated by `mark_dirty()` after any write, so it never
    /// serves a consistent-stale view across a mutation.
    pub fn nodes_by_json_path(&self, path: &str, value: &str) -> Option<Vec<String>> {
        {
            let guard = self.path_index.read();
            if let Some(idx) = guard.as_ref() {
                if let Some(by_value) = idx.by_value.get(path) {
                    return Some(by_value.get(value).cloned().unwrap_or_default());
                }
            }
        }
        let built_at = self.version();
        let mut guard = self.path_index.write();
        let idx = guard.get_or_insert_with(PathIndex::default);
        let indexed = self.ensure_json_path_indexed(idx, path);
        // Stamp the (re)built path index at the pre-build version (W1.6/P7) so `mark_dirty`
        // preserves it across a CAS that could not feed any indexed path.
        self.index_stamps
            .path
            .fetch_max(built_at, std::sync::atomic::Ordering::AcqRel);
        indexed?;
        Some(
            idx.by_value
                .get(path)
                .and_then(|by_value| by_value.get(value).cloned())
                .unwrap_or_default(),
        )
    }

    /// Node ids for which JSONPath `path` resolves to ANY value — the EXISTENCE set
    /// (CONCEPT:EG-KG.compute.json-deep-indexing), the index-accelerated backing for `props @> …` / `jsonb_path_query`
    /// existence and a selectivity estimate for the planner. Returns `None` when `path`
    /// is not (and cannot be) indexed under the bound (full-scan fallback).
    pub fn nodes_with_json_path(&self, path: &str) -> Option<Vec<String>> {
        {
            let guard = self.path_index.read();
            if let Some(idx) = guard.as_ref() {
                if let Some(ids) = idx.present.get(path) {
                    return Some(ids.clone());
                }
            }
        }
        let built_at = self.version();
        let mut guard = self.path_index.write();
        let idx = guard.get_or_insert_with(PathIndex::default);
        let indexed = self.ensure_json_path_indexed(idx, path);
        self.index_stamps
            .path
            .fetch_max(built_at, std::sync::atomic::Ordering::AcqRel);
        indexed?;
        Some(idx.present.get(path).cloned().unwrap_or_default())
    }

    /// Ensure `path` is present in the JSONPath index, honouring the bound
    /// (CONCEPT:EG-KG.compute.json-deep-indexing). `Some(())` if the path is (now) indexed, `None` if the cap is
    /// full and the path is not already present (caller full-scans). Pre-seeds any paths
    /// named in `EPISTEMIC_GRAPH_INDEXED_JSON_PATHS` on the first build.
    #[allow(clippy::map_entry)]
    fn ensure_json_path_indexed(&self, idx: &mut PathIndex, path: &str) -> Option<()> {
        let cap = Self::max_indexed_json_paths();
        // Track whether we built anything, so a durable store (CONCEPT:EG-KG.storage.path-index-store) is
        // written through exactly once per demand-driven (re)build — not on a pure
        // cache hit where the path was already indexed.
        let mut changed = false;
        if idx.by_value.is_empty() && idx.present.is_empty() {
            for seed in Self::seed_indexed_json_paths() {
                if idx.by_value.len() >= cap || idx.by_value.contains_key(&seed) {
                    continue;
                }
                let (by_value, present) = self.build_json_path_maps(&seed);
                idx.by_value.insert(seed.clone(), by_value);
                idx.present.insert(seed, present);
                changed = true;
            }
        }
        if idx.by_value.contains_key(path) {
            // CONCEPT:EG-KG.storage.path-index-store — a rehydrated seed set (or an earlier same-guard build)
            // already covers `path`; persist the seed build if it happened, then hit.
            if changed {
                self.persist_path_index(idx);
            }
            return Some(());
        }
        if idx.by_value.len() >= cap {
            if changed {
                self.persist_path_index(idx);
            }
            return None;
        }
        let (by_value, present) = self.build_json_path_maps(path);
        idx.by_value.insert(path.to_string(), by_value);
        idx.present.insert(path.to_string(), present);
        // CONCEPT:EG-KG.storage.path-index-store — write the freshly (re)built index through to the durable
        // store so a restart rehydrates it instead of rescanning every node. A no-op
        // when no store is attached (the default), so the in-memory path is unchanged.
        self.persist_path_index(idx);
        Some(())
    }

    /// Attach a durable JSONPath-index store (CONCEPT:EG-KG.storage.path-index-store). Called once at startup
    /// (only when a persist dir is configured): thereafter every demand-driven
    /// (re)build of the path-index is written through, and [`rehydrate_path_index`]
    /// can warm the index from the store at boot. A `GraphCore` with no store attached
    /// behaves exactly as before — the path-index is fully in-memory.
    ///
    /// [`rehydrate_path_index`]: GraphCore::rehydrate_path_index
    pub fn set_path_index_store(&self, store: Arc<dyn crate::path_persist::PathIndexPersistence>) {
        *self.path_index_store.write() = Some(store);
    }

    /// Rehydrate the persisted JSONPath index into memory at boot (CONCEPT:EG-KG.storage.path-index-store).
    /// Loads the durable snapshot (if a store is attached and non-empty) and adopts it
    /// as the live `path_index`, so the FIRST JSON filter after a restart hits the
    /// warm index instead of paying a full node rescan. Returns the number of distinct
    /// JSONPaths adopted (0 when no store is attached / the store is empty). A
    /// subsequent write invalidates the index via `mark_dirty` as usual, and the next
    /// query rebuilds-on-miss (and re-persists) — so rehydration never pins a stale
    /// view across a mutation.
    pub fn rehydrate_path_index(&self) -> usize {
        let Some(store) = self.path_index_store.read().clone() else {
            return 0;
        };
        let Some(snap) = store.load() else {
            return 0;
        };
        if snap.is_empty() {
            return 0;
        }
        let idx = PathIndex::from_persisted(&snap);
        let adopted = idx.by_value.len();
        let mut guard = self.path_index.write();
        // Stamp the rehydrated index at the current version (W1.6/P7) so the stamp-aware
        // `mark_dirty` treats it as current until the first write that could affect a path.
        self.index_stamps
            .path
            .fetch_max(self.version(), std::sync::atomic::Ordering::AcqRel);
        *guard = Some(idx);
        adopted
    }

    /// Write the current in-memory path-index through to the durable store, stamped
    /// with the graph's OCC `version()` (CONCEPT:EG-KG.storage.path-index-store). A no-op when no store is
    /// attached (the default), so the fully-in-memory path is byte-for-byte unchanged.
    /// Best-effort: a persistence error is swallowed by the store impl (the index is
    /// always rebuildable on demand), so it can never fail the query that triggered it.
    fn persist_path_index(&self, idx: &PathIndex) {
        let Some(store) = self.path_index_store.read().clone() else {
            return;
        };
        let snap = idx.to_persisted(self.version());
        store.save(&snap);
    }

    /// JSONPath filter selectivity for the planner cost `Stats` (CONCEPT:EG-KG.storage.path-index-store —
    /// hooking the EG-084 inverted-index id counts into the cross-modal cost model,
    /// CONCEPT:EG-KG.query.concept-14). Returns the fraction of property-bearing nodes that pass the
    /// filter, in `[0,1]` — exactly the `filter_selectivity` a planner feeds
    /// `eg_plan::cost::Stats::estimate` to order a Filter/Rank pair:
    ///   * `value = Some(v)` — an EQUALITY filter (`props->>'k' = 'v'`): the count from
    ///     [`nodes_by_json_path`](GraphCore::nodes_by_json_path) over `|nodes|`;
    ///   * `value = None` — an EXISTENCE/`@>` filter: the count from
    ///     [`nodes_with_json_path`](GraphCore::nodes_with_json_path) over `|nodes|`.
    ///
    /// Returns `None` when `path` cannot be indexed under the bound (the planner then
    /// falls back to its default estimate) — the SAME bound the two count methods use,
    /// so the selectivity source never triggers a build the query itself wouldn't.
    pub fn json_path_selectivity(&self, path: &str, value: Option<&str>) -> Option<f64> {
        let total = self.node_properties.len();
        if total == 0 {
            return Some(0.0);
        }
        let matched = match value {
            Some(v) => self.nodes_by_json_path(path, v)?.len(),
            None => self.nodes_with_json_path(path)?.len(),
        };
        Some((matched as f64 / total as f64).clamp(0.0, 1.0))
    }

    /// Scan the node store once and build, for one JSONPath, both the `value → ids`
    /// equality map (over the canonical scalar form of each matched leaf) and the
    /// existence `ids` set (CONCEPT:EG-KG.compute.json-deep-indexing). A malformed path yields empty maps.
    fn build_json_path_maps(&self, path: &str) -> (HashMap<String, Vec<String>>, Vec<String>) {
        let mut by_value: HashMap<String, Vec<String>> = HashMap::new();
        let mut present: Vec<String> = Vec::new();
        let Some(segs) = crate::jsonpath::parse_path(path) else {
            return (by_value, present);
        };
        for entry in self.node_properties.iter() {
            let Ok(val) = decode_property_value(entry.value().as_slice()) else {
                continue;
            };
            let matches = crate::jsonpath::eval(&val, &segs);
            if matches.is_empty() {
                continue;
            }
            present.push(entry.key().clone());
            for m in matches {
                if let Some(vk) = crate::jsonpath::canonical_scalar(m) {
                    by_value.entry(vk).or_default().push(entry.key().clone());
                }
            }
        }
        for ids in by_value.values_mut() {
            ids.sort_unstable();
            ids.dedup();
        }
        present.sort_unstable();
        present.dedup();
        (by_value, present)
    }

    /// Cap on the number of distinct JSONPaths ever indexed
    /// (`EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS`, default 64) (CONCEPT:EG-KG.compute.json-deep-indexing).
    fn max_indexed_json_paths() -> usize {
        std::env::var("EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_MAX_INDEXED_JSON_PATHS)
    }

    /// JSONPaths to pre-seed into the index on first build
    /// (`EPISTEMIC_GRAPH_INDEXED_JSON_PATHS`, comma-separated) (CONCEPT:EG-KG.compute.json-deep-indexing).
    fn seed_indexed_json_paths() -> Vec<String> {
        std::env::var("EPISTEMIC_GRAPH_INDEXED_JSON_PATHS")
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

    /// Lexical classification gate (CONCEPT:EG-ORCH.routing.lexical-capability-escalation): find every capability term
    /// (a `Tool`/`Skill`/`MCPServer`/… node's name or synonym) that appears as a
    /// whole word in `query`. Embedding-free and backend-universal — the "free"
    /// tier between structural routing and semantic `search_hybrid`: a chat turn
    /// naming a real fleet capability ("list portainer stacks") escalates to the
    /// full graph without paying for a vector search.
    ///
    /// The aho-corasick automaton over capability terms is cached and reused while
    /// the node count is unchanged, so steady-state cost is ~µs per query.
    pub fn match_ontology_terms(&self, query: &str) -> Vec<OntologyMatch> {
        if query.trim().is_empty() {
            return Vec::new();
        }
        let current_count = self.node_properties.len();
        {
            let guard = self.ontology_index.read();
            if let Some(idx) = guard.as_ref() {
                if idx.node_count == current_count {
                    return Self::run_ontology_match(&idx.ac, &idx.metas, query);
                }
            }
        }
        let (ac, metas) = self.build_ontology_index();
        let hits = Self::run_ontology_match(&ac, &metas, query);
        *self.ontology_index.write() = Some(OntologyTermIndex {
            node_count: current_count,
            ac,
            metas,
        });
        hits
    }

    /// Scan the node store once, collecting the deduped capability-term → metadata
    /// map and compiling it into an aho-corasick automaton. Terms shorter than 3
    /// chars are dropped (too noise-prone for whole-word matching).
    fn build_ontology_index(&self) -> (AhoCorasick, Vec<OntologyMatch>) {
        let mut dedup: HashMap<String, OntologyMatch> = HashMap::new();
        for entry in self.node_properties.iter() {
            let Ok(val) = decode_property_value(entry.value().as_slice()) else {
                continue;
            };
            let Some(ntype) = val
                .get("type")
                .and_then(|v| v.as_str())
                .or_else(|| val.get("node_type").and_then(|v| v.as_str()))
            else {
                continue;
            };
            if !CAPABILITY_NODE_TYPES.contains(&ntype) {
                continue;
            }
            let name = val.get("name").and_then(|v| v.as_str()).unwrap_or("");
            // The owning fleet server: a Tool carries `mcp_server`; an MCPServer node
            // IS the server, so fall back to its own name.
            let server = val
                .get("mcp_server")
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| if ntype == "MCPServer" { name } else { "" });
            let mut terms: Vec<&str> = Vec::new();
            if !name.is_empty() {
                terms.push(name);
            }
            if let Some(arr) = val.get("synonyms").and_then(|v| v.as_array()) {
                for s in arr {
                    if let Some(ss) = s.as_str() {
                        if !ss.is_empty() {
                            terms.push(ss);
                        }
                    }
                }
            }
            for term in terms {
                let lc = term.to_lowercase();
                if lc.chars().count() < 3 {
                    continue;
                }
                dedup.entry(lc).or_insert_with(|| OntologyMatch {
                    term: term.to_string(),
                    node_type: ntype.to_string(),
                    label: name.to_string(),
                    mcp_server: server.to_string(),
                    score: term.chars().count() as f64,
                });
            }
        }

        let mut patterns: Vec<String> = Vec::with_capacity(dedup.len());
        let mut metas: Vec<OntologyMatch> = Vec::with_capacity(dedup.len());
        for (lc, meta) in dedup {
            patterns.push(lc);
            metas.push(meta);
        }
        let ac = AhoCorasick::builder()
            .match_kind(MatchKind::LeftmostLongest)
            .build(&patterns)
            .unwrap_or_else(|_| AhoCorasick::new::<[&str; 0], _>([]).expect("empty aho-corasick"));
        (ac, metas)
    }

    /// Run a built automaton over `query`, returning the distinct capability terms
    /// it contains. Matches are restricted to whole words (no alphanumeric char
    /// abutting either end) so a short term never matches inside a larger word.
    fn run_ontology_match(
        ac: &AhoCorasick,
        metas: &[OntologyMatch],
        query: &str,
    ) -> Vec<OntologyMatch> {
        if metas.is_empty() {
            return Vec::new();
        }
        let hay = query.to_lowercase();
        let bytes = hay.as_bytes();
        let mut out: Vec<OntologyMatch> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for m in ac.find_iter(hay.as_str()) {
            let start = m.start();
            let end = m.end();
            let before_ok = start == 0 || !bytes[start - 1].is_ascii_alphanumeric();
            let after_ok = end >= bytes.len() || !bytes[end].is_ascii_alphanumeric();
            if !before_ok || !after_ok {
                continue;
            }
            if let Some(meta) = metas.get(m.pattern().as_usize()) {
                if seen.insert(meta.term.to_lowercase()) {
                    out.push(meta.clone());
                }
            }
        }
        out
    }

    /// Like `get_nodes` but clones the Arc POINTERS, not the bytes — used by the
    /// snapshot/checkpoint hot path (Phase C-A zero-copy).
    pub fn get_nodes_arc(&self) -> Vec<(String, Arc<Vec<u8>>)> {
        self.node_properties
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    pub fn get_node_properties(&self, node_id: &str) -> Option<Vec<u8>> {
        if let Some(a) = self.node_properties.get(node_id) {
            return Some((**a).clone());
        }
        // A durable node may have been
        // evicted from RAM to bound memory (CONCEPT:EG-KG.storage.read-through-seam-exercised); fetch its stored
        // blob from the durable tier so an evicted node still reads back correctly.
        // Without an installed read-through this is a genuine absence.
        self.read_through_get(node_id)
    }

    /// Consult the durable read-through on a RAM miss (CONCEPT:EG-KG.storage.read-through-seam-exercised). Returns
    /// the node's stored property blob from the durable tier, or `None` when no
    /// read-through is attached (default model) or the node is genuinely absent.
    /// Kept lock-scoped: the read-through guard is cloned out and released BEFORE
    /// the (possibly blocking) durable point-read so it never holds the lock across
    /// I/O. Serve-only — it does NOT repopulate RAM, so a full scan over evicted
    /// nodes cannot re-grow the resident set past the cap (memory stays bounded).
    fn read_through_get(&self, node_id: &str) -> Option<Vec<u8>> {
        let rt = self.read_through.read().clone()?;
        // CONCEPT:EG-KG.storage.bloom-negative-lookup-guard — only trust a bloom "no"
        // once `node_bloom` reflects the COMPLETE durable node-id set (never during
        // a paged-lazy-open still in progress: CONCEPT:EG-KG.sharding.paged-lazy-open); a not-yet-
        // paged-in-but-durable node would otherwise be wrongly reported absent. When
        // incomplete this is a no-op guard and behavior is byte-for-byte the
        // pre-bloom original: every miss falls through to the durable point-read.
        if self
            .bloom_complete
            .load(std::sync::atomic::Ordering::Acquire)
            && !self.node_bloom.read().might_contain(node_id)
        {
            return None;
        }
        rt.read_node_blob(node_id)
    }

    /// Mark `node_bloom` as reflecting the COMPLETE known durable node-id set for
    /// this graph (CONCEPT:EG-KG.storage.bloom-negative-lookup-guard). Called by the registry once a
    /// full/eager load or the FINAL page of a paged-lazy-open has replayed every
    /// durable node through `add_node` — before that point `read_through_get`
    /// never gates on the filter, so an in-progress paged open cannot regress.
    #[doc(hidden)]
    pub fn mark_bloom_complete(&self) {
        self.bloom_complete
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn node_count(&self) -> usize {
        self.topo.read().node_map.len()
    }

    /// Return all node IDs without properties (lightweight enumeration).
    pub fn node_ids(&self) -> Vec<String> {
        self.topo.read().node_map.keys().cloned().collect()
    }

    // ── Edge CRUD ────────────────────────────────────────────────────────

    // ── Edge CRUD (one-shot convenience over `txn`) ──────────────────────

    pub fn add_edge(
        &self,
        source_id: String,
        target_id: String,
        properties_msgpack: Vec<u8>,
    ) -> Result<(), String> {
        self.txn()
            .add_edge(source_id, target_id, properties_msgpack)
    }

    /// Edge sibling of [`Self::add_node_no_ledger`] (D-EGP-1, t1-grounding-0802) —
    /// same rationale: `build_projection` inserts every visible edge into a
    /// throwaway `GraphCore` whose ledger is cleared before it is ever read, so
    /// `add_edge`'s per-call `format!("ADD_EDGE|...", HexLedger(...))` + ledger
    /// mutex push is pure waste. Functionally identical to `add_edge` otherwise.
    pub fn add_edge_no_ledger(
        &self,
        source_id: String,
        target_id: String,
        properties_msgpack: Vec<u8>,
    ) -> Result<(), String> {
        {
            let mut topo = self.topo.write();
            let source_idx = *topo
                .node_map
                .get(&source_id)
                .ok_or_else(|| edge_endpoint_not_found("Source", &source_id))?;
            let target_idx = *topo
                .node_map
                .get(&target_id)
                .ok_or_else(|| edge_endpoint_not_found("Target", &target_id))?;
            topo.graph.add_edge(
                source_idx,
                target_idx,
                format!("{}:{}", source_id, target_id),
            );
        }
        self.edge_properties
            .entry((source_id, target_id))
            .or_default()
            .push(Arc::new(properties_msgpack));
        Ok(())
    }

    pub fn remove_edge(&self, source_id: String, target_id: String) {
        self.txn().remove_edge(source_id, target_id);
    }

    pub fn has_edge(&self, source_id: &str, target_id: &str) -> bool {
        let topo = self.topo.read();
        if let (Some(&src_idx), Some(&tgt_idx)) =
            (topo.node_map.get(source_id), topo.node_map.get(target_id))
        {
            topo.graph.find_edge(src_idx, tgt_idx).is_some()
        } else {
            false
        }
    }

    /// Unconditionally materializes EVERY edge's property blob (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation) — the
    /// full-graph-dump primitive behind the served `GetEdges` RPC, which guards
    /// this with an edge-count cap before calling it (`oversize_edge_dump_error`
    /// in `graph_ops.rs`) precisely because it has no bound of its own. Callers
    /// that don't need a full-graph snapshot/export/mining pass should prefer
    /// [`Self::get_edges_page`] (bounded, keyset-paginated) instead of adding a
    /// new unguarded caller here.
    pub fn get_edges(&self) -> Vec<(String, String, Vec<u8>)> {
        let mut res = Vec::new();
        for entry in self.edge_properties.iter() {
            let (src, tgt) = entry.key();
            for props in entry.value() {
                res.push((src.clone(), tgt.clone(), (**props).clone()));
            }
        }
        res
    }

    /// Like `get_edges` but clones the Arc pointers — snapshot hot path (C-A).
    /// Same unbounded-materialization caveat; prefer [`Self::get_edges_page`]
    /// for a bounded read.
    pub fn get_edges_arc(&self) -> Vec<(String, String, Arc<Vec<u8>>)> {
        let mut res = Vec::new();
        for entry in self.edge_properties.iter() {
            let (src, tgt) = entry.key();
            for props in entry.value() {
                res.push((src.clone(), tgt.clone(), props.clone()));
            }
        }
        res
    }

    /// Deterministic keyset page over the edge store (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation — the edge
    /// sibling of `get_nodes_by_label_page`'s unlabeled scan). Returns at most
    /// `limit` edges ordered by `(source, target, ordinal)` — `ordinal` is the
    /// index among parallel edges stored under the same `(source, target)` pair,
    /// mirroring the durable `(graph, src, tgt, ordinal)` row key
    /// (`redb_store::read_graph_dump_page` / `scan_next_edge_ordinal`). `after` is
    /// an EXCLUSIVE `(source, target, ordinal)` cursor (`None` starts at the first
    /// edge); a caller advances it to the last row returned. `limit == 0` means no
    /// cap.
    ///
    /// Unlike `get_edges`/`get_edges_arc` (which clone EVERY edge's property
    /// blob), this clones/decodes properties ONLY for the requested page — the
    /// full scan is over the lightweight `(source, target)` key pairs (cached
    /// after the first call, invalidated on any write), never the property bytes.
    ///
    /// This is a live committed-state scan rather than a cross-request snapshot
    /// (same caveat as `get_nodes_by_label_page`): a concurrent writer can insert
    /// or remove an edge between two page calls; the cursor is not a MVCC
    /// snapshot handle.
    pub fn get_edges_page(
        &self,
        after: Option<(&str, &str, u32)>,
        limit: usize,
    ) -> Vec<(String, String, u32, Vec<u8>)> {
        {
            let guard = self.edge_key_index.read();
            if let Some(keys) = guard.as_ref() {
                return Self::collect_edge_page(keys, &self.edge_properties, after, limit);
            }
        }
        let mut keys: Vec<(String, String)> = self
            .edge_properties
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        keys.sort_unstable();
        let out = Self::collect_edge_page(&keys, &self.edge_properties, after, limit);
        *self.edge_key_index.write() = Some(keys);
        out
    }

    /// Materialize one page of `(source, target, ordinal, properties)` rows from a
    /// SORTED `(source, target)` key list, honouring `limit` (`0` = uncapped) and
    /// the exclusive `after` cursor. Skips a key that has since been removed from
    /// `edge_properties` (defensive against an in-flight removal that hasn't yet
    /// invalidated the cache) — mirrors `GraphCore::collect_by_label`'s identical
    /// defensiveness for the node-label keyset scan.
    fn collect_edge_page(
        keys: &[(String, String)],
        edge_properties: &DashMap<(String, String), Vec<Arc<Vec<u8>>>>,
        after: Option<(&str, &str, u32)>,
        limit: usize,
    ) -> Vec<(String, String, u32, Vec<u8>)> {
        let start = match after {
            Some((s, t, _)) => keys.partition_point(|(ks, kt)| (ks.as_str(), kt.as_str()) < (s, t)),
            None => 0,
        };
        let mut out: Vec<(String, String, u32, Vec<u8>)> =
            Vec::with_capacity(if limit == 0 { 0 } else { limit });
        'outer: for (src, tgt) in keys.iter().skip(start) {
            let Some(props_list) = edge_properties.get(&(src.clone(), tgt.clone())) else {
                continue; // removed since the key list was built; skip defensively.
            };
            for (ordinal, props) in props_list.iter().enumerate() {
                let ordinal = ordinal as u32;
                if let Some((after_src, after_tgt, after_ordinal)) = after {
                    if src.as_str() == after_src
                        && tgt.as_str() == after_tgt
                        && ordinal <= after_ordinal
                    {
                        continue;
                    }
                }
                if limit != 0 && out.len() >= limit {
                    break 'outer;
                }
                out.push((src.clone(), tgt.clone(), ordinal, (**props).clone()));
            }
        }
        out
    }

    pub fn get_edge_properties(&self, source_id: &str, target_id: &str) -> Vec<Vec<u8>> {
        self.edge_properties
            .get(&(source_id.to_string(), target_id.to_string()))
            .map(|v| v.iter().map(|a| (**a).clone()).collect())
            .unwrap_or_default()
    }

    pub fn edge_count(&self) -> usize {
        // Topology and edge-property rows are updated in the same GraphTxn. The
        // StableGraph maintains its cardinality, so this is O(1) instead of walking
        // every endpoint pair and parallel-edge property vector.
        self.topo.read().graph.edge_count()
    }

    /// Approximate resident RAM this graph holds, in bytes (CONCEPT:EG-KG.compute.lane-v —
    /// per-tenant memory budget). A running byte estimate over the in-RAM state:
    /// the node + edge property blobs (the bulk of a graph's footprint), plus a
    /// fixed per-node/per-edge overhead for the petgraph topology + id strings +
    /// map slots, plus the embedding vectors (`len × dim × 4` bytes). This is an
    /// ESTIMATE — it deliberately does NOT walk every allocator block; it sums the
    /// blob lengths already resident (cheap, lock-light) and adds a calibrated
    /// constant for the structural overhead. It is the signal the budget enforcer
    /// and `ResourceStats` report; precision to the byte is unnecessary (the
    /// budget is a soft cap swept periodically), order-of-magnitude accuracy is.
    ///
    /// Costs one pass over the node/edge property maps + the topology read lock.
    /// Called off the hot path (periodic budget sweep / a `ResourceStats` request),
    /// never per-mutation.
    pub fn memory_estimate(&self) -> u64 {
        // Per-node structural overhead: a petgraph node + a node_map entry (the id
        // String is counted via its bytes below) + a node_properties slot. Per-edge:
        // a petgraph edge + an edge_properties Vec slot. Calibrated constants, not a
        // measurement — keep them modest so the blob bytes dominate the estimate.
        const NODE_OVERHEAD: u64 = 64;
        const EDGE_OVERHEAD: u64 = 48;

        let mut bytes: u64 = 0;

        // Node property blobs + their id-string bytes.
        for entry in self.node_properties.iter() {
            bytes += entry.key().len() as u64;
            bytes += entry.value().len() as u64;
            bytes += NODE_OVERHEAD;
        }

        // Edge property blobs + the (src, tgt) key bytes.
        for entry in self.edge_properties.iter() {
            let (src, tgt) = entry.key();
            let key_bytes = (src.len() + tgt.len()) as u64;
            for props in entry.value() {
                bytes += key_bytes + props.len() as u64 + EDGE_OVERHEAD;
            }
        }

        // Embedding vectors: sum of each live vector's `len × 4` bytes (f32).
        bytes += self.semantic_store.read().embedding_bytes();

        bytes
    }

    /// In-degree count for a specific node.
    pub fn in_degree(&self, node_id: &str) -> Result<usize, String> {
        let topo = self.topo.read();
        let idx = topo
            .node_map
            .get(node_id)
            .ok_or_else(|| format!("Node '{}' not found", node_id))?;
        Ok(topo
            .graph
            .edges_directed(*idx, petgraph::Direction::Incoming)
            .count())
    }

    /// Out-degree count for a specific node.
    pub fn out_degree(&self, node_id: &str) -> Result<usize, String> {
        let topo = self.topo.read();
        let idx = topo
            .node_map
            .get(node_id)
            .ok_or_else(|| format!("Node '{}' not found", node_id))?;
        Ok(topo
            .graph
            .edges_directed(*idx, petgraph::Direction::Outgoing)
            .count())
    }

    // ── Neighbor Queries ─────────────────────────────────────────────────

    /// Incoming neighbors (predecessors).
    pub fn get_predecessors(&self, node_id: &str) -> Result<Vec<String>, String> {
        let topo = self.topo.read();
        let idx = topo
            .node_map
            .get(node_id)
            .ok_or_else(|| format!("Node '{}' not found", node_id))?;
        let preds: Vec<String> = topo
            .graph
            .edges_directed(*idx, petgraph::Direction::Incoming)
            .map(|e| topo.graph[e.source()].clone())
            .collect();
        Ok(preds)
    }

    /// Outgoing neighbors (successors).
    pub fn get_successors(&self, node_id: &str) -> Result<Vec<String>, String> {
        let topo = self.topo.read();
        let idx = topo
            .node_map
            .get(node_id)
            .ok_or_else(|| format!("Node '{}' not found", node_id))?;
        let succs: Vec<String> = topo
            .graph
            .edges_directed(*idx, petgraph::Direction::Outgoing)
            .map(|e| topo.graph[e.target()].clone())
            .collect();
        Ok(succs)
    }

    /// All neighbors (both directions, deduplicated).
    pub fn get_neighbors(&self, node_id: &str) -> Result<Vec<String>, String> {
        let topo = self.topo.read();
        let idx = topo
            .node_map
            .get(node_id)
            .ok_or_else(|| format!("Node '{}' not found", node_id))?;
        let mut neighbors = std::collections::HashSet::new();
        for e in topo
            .graph
            .edges_directed(*idx, petgraph::Direction::Incoming)
        {
            neighbors.insert(topo.graph[e.source()].clone());
        }
        for e in topo
            .graph
            .edges_directed(*idx, petgraph::Direction::Outgoing)
        {
            neighbors.insert(topo.graph[e.target()].clone());
        }
        Ok(neighbors.into_iter().collect())
    }

    /// Batch form of [`Self::get_neighbors`] (D-DPF-1): neighbor ids for MANY
    /// nodes under ONE `topo.read()` acquisition instead of one lock
    /// acquisition per node. A node absent from the graph yields an empty
    /// neighbor list at its position rather than failing the whole batch —
    /// callers that need to distinguish "absent" from "no neighbors" should
    /// pair this with `has_batch`. Order matches `node_ids`.
    pub fn get_neighbors_batch(&self, node_ids: Vec<String>) -> Vec<(String, Vec<String>)> {
        let topo = self.topo.read();
        node_ids
            .into_iter()
            .map(|node_id| {
                let neighbors = match topo.node_map.get(&node_id) {
                    Some(idx) => {
                        let mut set = std::collections::HashSet::new();
                        for e in topo
                            .graph
                            .edges_directed(*idx, petgraph::Direction::Incoming)
                        {
                            set.insert(topo.graph[e.source()].clone());
                        }
                        for e in topo
                            .graph
                            .edges_directed(*idx, petgraph::Direction::Outgoing)
                        {
                            set.insert(topo.graph[e.target()].clone());
                        }
                        set.into_iter().collect()
                    }
                    None => Vec::new(),
                };
                (node_id, neighbors)
            })
            .collect()
    }

    // ── Serialization ────────────────────────────────────────────────────

    /// Owned, serializable snapshot of this graph's persistent state. Cheap
    /// relative to serialization (clones the node/edge/ledger/semantic data), so a
    /// checkpoint takes it under a BRIEF lock and serializes OFF the lock.
    /// (CONCEPT:EG-KG.storage.nonblocking-checkpoint — non-blocking checkpoint, A1)
    pub fn snapshot(&self) -> GraphSnapshot {
        // Hold the topology read lock for the duration: every mutation goes through
        // a write txn (topo.write()), so a read guard excludes all writers and the
        // node/edge/ledger views below are a single consistent point-in-time.
        let _topo = self.topo.read();
        // Zero-copy (Phase C-A): clone the Arc POINTERS, not the property bytes —
        // turns the A1-residual ~3s lock-held deep clone of a 450MB graph into a
        // ~µs pointer copy, and removes the transient memory doubling.
        GraphSnapshot {
            schema_version: GRAPH_SNAPSHOT_SCHEMA_VERSION,
            integrity_policy: self.integrity_policy.read().clone(),
            nodes: self.get_nodes_arc(),
            edges: self.get_edges_arc(),
            ledger: self.ledger.lock().clone(),
            semantic_store: self.semantic_store.read().clone(),
        }
    }

    /// Build an isolated mutable graph from an owned snapshot without copying
    /// property blobs. Mutation gateways use this as their staging image: execute
    /// against the fork, durably commit its resulting snapshot, then publish it to
    /// the live core. The caller-supplied version is the authoritative OCC source
    /// version captured with the snapshot.
    pub fn from_snapshot(snapshot: GraphSnapshot, version: u64) -> Result<Self, String> {
        let core = Self::new();
        core.replace_snapshot(snapshot)?;
        core.version
            .store(version, std::sync::atomic::Ordering::Release);
        core.dirty
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(core)
    }

    /// Publish the authoritative version of a newly materialized projection.
    ///
    /// Recovery and graph creation build an unpublished [`GraphCore`] from
    /// durable rows using the ordinary row primitives. Those primitives do not
    /// advance the serving version because replay is not a new mutation. Before
    /// the core becomes visible, its version must therefore adopt the exact
    /// authoritative source watermark. This is a one-shot transition from the
    /// fresh-core version (`0`); refusing any later transition prevents recovery
    /// plumbing from rewinding a live projection.
    #[doc(hidden)]
    pub fn adopt_materialized_version(&self, authoritative_version: u64) -> Result<(), String> {
        if authoritative_version == 0 {
            return Err(
                "materialized version publication requires a committed non-zero version"
                    .to_string(),
            );
        }
        self.version
            .compare_exchange(
                0,
                authoritative_version,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .map_err(|current| {
                format!(
                    "materialized version publication requires a fresh projection (current {current}, authoritative {authoritative_version})"
                )
            })?;
        self.dirty
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Atomically replace this core's persistent image while retaining its
    /// read-through, notifier, and index-service attachments. Property `Arc`s are
    /// moved from the staged snapshot, avoiding a second graph-sized byte copy.
    /// Version/dirty publication is intentionally left to the commit gateway.
    pub fn replace_snapshot(&self, snapshot: GraphSnapshot) -> Result<(), String> {
        snapshot.validate_schema()?;
        let GraphSnapshot {
            schema_version: _,
            integrity_policy,
            nodes,
            edges,
            ledger,
            semantic_store,
        } = snapshot;

        // Validate and construct the complete replacement OFF the live graph.
        // A duplicate node or dangling endpoint must leave the prior image intact;
        // clearing first turned a malformed restore into partial data loss.
        let mut next_topology = Topology::default();
        let mut next_node_properties = Vec::with_capacity(nodes.len());
        for (node_id, properties) in nodes {
            if next_topology.node_map.contains_key(&node_id) {
                return Err(format!("snapshot contains duplicate node '{}'", node_id));
            }
            let index = next_topology.graph.add_node(node_id.clone());
            next_topology.node_map.insert(node_id.clone(), index);
            next_node_properties.push((node_id, properties));
        }
        let mut next_edge_properties: HashMap<_, Vec<_>> = HashMap::new();
        for (source, target, properties) in edges {
            let source_index = next_topology
                .node_map
                .get(&source)
                .copied()
                .ok_or_else(|| format!("snapshot edge source '{}' is missing", source))?;
            let target_index = next_topology
                .node_map
                .get(&target)
                .copied()
                .ok_or_else(|| format!("snapshot edge target '{}' is missing", target))?;
            next_topology.graph.add_edge(
                source_index,
                target_index,
                format!("{}:{}", source, target),
            );
            next_edge_properties
                .entry((source, target))
                .or_default()
                .push(properties);
        }

        // A snapshot is by construction a COMPLETE point-in-time node set (unlike a
        // bounded `MaterialPage`), so the bloom guard can be rebuilt fresh and
        // trusted immediately (CONCEPT:EG-KG.storage.bloom-negative-lookup-guard) — sized off this
        // exact cardinality rather than the prior graph's.
        let next_bloom = crate::bloom::NodeBloomFilter::new(next_node_properties.len(), 0.01);
        for (node_id, _) in &next_node_properties {
            next_bloom.insert(node_id);
        }

        // No fallible validation remains beyond this point. Publish under the
        // topology writer barrier, then invalidate derivatives of the old image.
        let mut topo = self.topo.write();
        *topo = next_topology;
        self.node_properties.clear();
        for (node_id, properties) in next_node_properties {
            self.node_properties.insert(node_id, properties);
        }
        *self.node_bloom.write() = next_bloom;
        self.bloom_complete
            .store(true, std::sync::atomic::Ordering::Release);
        self.edge_properties.clear();
        for (endpoints, properties) in next_edge_properties {
            self.edge_properties.insert(endpoints, properties);
        }
        *self.ledger.lock() = ledger;
        *self.semantic_store.write() = semantic_store;
        *self.integrity_policy.write() = integrity_policy;
        drop(topo);
        self.invalidate_indexes();
        #[cfg(feature = "result-cache")]
        {
            self.result_cache.invalidate_all();
            // The whole image was replaced; the dependency clock's old per-label/per-key versions
            // are meaningless against the new graph and would otherwise spuriously invalidate
            // future entries. Reset it (the cache's entries were just cleared, so nothing survives
            // to revalidate) — W1.6/P7.
            self.dep_clock.reset();
        }
        // U-142/U-143/U-145 (BUG-130): this whole-image replacement may publish at
        // the SAME `version()` a caller (`prepare_snapshot_publish`/
        // `install_committed_snapshot`) already reconciled to, so the per-actor RLS
        // projection cache's (actor, version) key alone would not detect it. See
        // `Self::invalidate_projection_cache`'s doc.
        #[cfg(feature = "security")]
        self.invalidate_projection_cache();
        // perf/cold-query-floor-analysis (UNCOMPILED proposal): the filtered-view cache
        // is subject to the exact same same-version whole-image race.
        #[cfg(feature = "security")]
        self.invalidate_filtered_view_cache();
        Ok(())
    }

    /// Install a staged image at its pre-commit version. The server immediately
    /// calls `mark_dirty()` after this non-awaiting publication step, advancing to
    /// the durable target version and emitting the normal change notification.
    pub fn prepare_snapshot_publish(
        &self,
        snapshot: GraphSnapshot,
        source_version: u64,
    ) -> Result<(), String> {
        self.replace_snapshot(snapshot)?;
        self.version
            .store(source_version, std::sync::atomic::Ordering::Release);
        self.dirty
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Reconcile RAM after a retry discovers that durability committed before the
    /// prior process could publish. This does not increment the already-committed
    /// version; it installs that exact version and wakes local subscribers once.
    pub fn install_committed_snapshot(
        &self,
        snapshot: GraphSnapshot,
        committed_version: u64,
    ) -> Result<(), String> {
        self.replace_snapshot(snapshot)?;
        self.version
            .store(committed_version, std::sync::atomic::Ordering::Release);
        self.dirty.store(true, std::sync::atomic::Ordering::Release);
        self.changes.emit(committed_version);
        Ok(())
    }

    /// Serialize the whole graph using the sole current typed MessagePack schema.
    pub fn to_msgpack(&self) -> Result<Vec<u8>, String> {
        self.snapshot().to_msgpack()
    }

    pub fn clear(&self) {
        // One write txn freezes structure; properties cleared under it so no reader
        // sees a half-cleared graph.
        let mut topo = self.topo.write();
        topo.graph.clear();
        topo.node_map.clear();
        self.node_properties.clear();
        self.edge_properties.clear();
        self.ledger.lock().clear();
        *self.semantic_store.write() = crate::compute::semantic::SemanticStore::new();
        // Drop every cached query result (CONCEPT:EG-KG.coordination.distributed-cache-coherence): `clear` (and `hibernate`,
        // which reuses it) wipes the graph WITHOUT bumping `version`, so the
        // version-keyed cache must be invalidated directly or a post-wipe lookup at the
        // unchanged version could serve a stale result.
        #[cfg(feature = "result-cache")]
        {
            self.result_cache.invalidate_all();
            // Wiping the graph also retires the dependency clock's per-dimension history (W1.6/P7).
            self.dep_clock.reset();
        }
        // U-142/U-143/U-145 (BUG-130): same reasoning as `result_cache` above, applied
        // to the per-actor RLS projection cache — `clear`/`hibernate` wipe the image
        // WITHOUT bumping `version`, so `(actor, version)` alone would keep serving a
        // pre-wipe projection as "current". See `Self::invalidate_projection_cache`.
        #[cfg(feature = "security")]
        self.invalidate_projection_cache();
        // perf/cold-query-floor-analysis (UNCOMPILED proposal): same reasoning, applied
        // to the filtered-view cache.
        #[cfg(feature = "security")]
        self.invalidate_filtered_view_cache();
    }

    /// Hibernate this graph's in-memory state (CONCEPT:EG-KG.storage.100m-tenant — cold-tenant
    /// hibernation). Drops the whole in-RAM topology / node+edge properties /
    /// semantic vectors — exactly what [`Self::clear`] frees — to reclaim a COLD
    /// tenant's memory, WITHOUT touching the durable redb tier (the caller guarantees
    /// the graph is durable first, the SAME durability-gate eviction uses). The
    /// `read_through` seam is left INTACT, so an evicted node's properties still read
    /// from redb on a RAM miss; a full topology/edge view requires a rehydrate.
    /// Reuses `clear`'s single-write-txn atomicity so no reader sees a half-state.
    /// Returns the node count freed (for observability).
    pub fn hibernate(&self) -> usize {
        let freed = self.node_count();
        self.clear();
        freed
    }

    /// Offload this graph's whole in-RAM state to a cold tier (CONCEPT:EG-KG.coordination.distributed-cache-coherence),
    /// then hibernate the RAM. Serializes the graph (`to_msgpack`), pushes it to the
    /// cold store, and only then drops the RAM — so the bytes are safely in the cold
    /// tier before RAM is freed. Returns the node count freed. The NEXT access calls
    /// [`Self::rehydrate_from_cold`] to bring it back. Read-mostly cold tenants thus
    /// spill RAM→redb→object-store and back without data loss.
    #[cfg(feature = "cold-tier")]
    pub fn offload_to_cold(
        &self,
        graph_name: &str,
        tier: &dyn crate::cold_tier::ColdTier,
    ) -> Result<usize, String> {
        let bytes = self.to_msgpack()?;
        tier.offload(graph_name, &bytes)?;
        Ok(self.hibernate())
    }

    /// Rehydrate this graph from the cold tier (CONCEPT:EG-KG.coordination.distributed-cache-coherence): fetch its
    /// offloaded blob and reload it via `from_msgpack`, then drop the cold copy. A
    /// no-op returning `Ok(false)` when the graph was not offloaded (already hot or
    /// never cold). On success the graph's full topology/edges/properties/vectors are
    /// back in RAM, exactly as before the offload.
    #[cfg(feature = "cold-tier")]
    pub fn rehydrate_from_cold(
        &self,
        graph_name: &str,
        tier: &dyn crate::cold_tier::ColdTier,
    ) -> Result<bool, String> {
        match tier.rehydrate(graph_name)? {
            Some(bytes) => {
                self.from_msgpack(&bytes)?;
                tier.remove(graph_name)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub fn from_msgpack(&self, msgpack: &[u8]) -> Result<(), String> {
        let limits = eg_types::msgpack::MsgpackLimits::new(
            MAX_GRAPH_SNAPSHOT_BYTES,
            MAX_GRAPH_SNAPSHOT_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        );
        let snapshot: GraphSnapshot = eg_types::msgpack::decode_bounded(msgpack, limits)
            .map_err(|_| "graph snapshot is invalid or exceeds resource limits".to_string())?;

        // `replace_snapshot` constructs and validates all topology/property state
        // before touching the live image, so a malformed edge or duplicate id is
        // a fail-closed no-op rather than a partially cleared graph.
        self.replace_snapshot(snapshot)
            .map_err(|_| "graph snapshot is invalid or exceeds resource limits".to_string())
    }

    // ── Ledger Operations ────────────────────────────────────────────────
    //
    // BUG A1 follow-up (2026-08-12): `ledger` is a purely IN-MEMORY,
    // ephemeral buffer -- it is NOT part of the durable path. `GetLedger`
    // reproduced returning `[]` after real, durably-committed mutations
    // because this crate's `security`-active RLS projection served an
    // unrelated no-ledger detached core (see `GetLedger`'s server handler
    // and `access::build_projection`'s doc for that root cause, fixed
    // separately) -- but even with that fixed, `ledger` itself can still be
    // emptied or truncated while the underlying mutations remain fully
    // durable in redb: cold-tenant idle offload/hibernate,
    // `MAX_RESIDENT_GRAPHS` eviction + lazy rehydrate, a process restart, or
    // simply exceeding `LEDGER_CAP` (below) all drop history this buffer
    // cannot recover. There is no query fix for that -- it is a property of
    // what this buffer IS (an audit/debug trail, not a change-data-capture
    // log; `CdcHub`, `src/server/cdc.rs`, is a SEPARATE bounded in-memory
    // ring with the identical ephemerality). The honest fix is to make that
    // fact OBSERVABLE rather than silent: `push_ledger` counts every
    // capacity-driven drop, `ledger_watermark` exposes the running total, and
    // `Method::GetLedger`'s response carries it on every read so a caller can
    // DETECT truncation (watermark increased since its last read) instead of
    // inferring completeness from a merely-nonzero read.

    pub fn get_ledger(&self) -> Vec<String> {
        self.ledger.lock().clone()
    }

    /// The 0-based sequence of the OLDEST ledger entry [`Self::get_ledger`]
    /// can currently vouch for (BUG A1 follow-up) -- i.e. how many entries
    /// have been permanently dropped from the front of this `GraphCore`
    /// instance's in-memory ledger by `LEDGER_CAP`'s trim policy. `0` means
    /// nothing has been dropped BY THIS INSTANCE -- it does not by itself
    /// prove no eviction/rehydrate/restart occurred (a freshly rehydrated or
    /// restarted graph's ledger legitimately starts at `0` again, with no
    /// memory that it was ever nonzero); the actionable signal for a caller
    /// is the watermark DECREASING or FAILING TO ADVANCE the way it expects
    /// across reads it knows straddled real mutations, not a bare snapshot
    /// value in isolation.
    pub fn ledger_watermark(&self) -> u64 {
        self.ledger_dropped_total
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    #[doc(hidden)]
    pub fn ledger_len(&self) -> usize {
        self.ledger.lock().len()
    }

    /// Replace only the changed suffix of an authenticated staged ledger image.
    /// The graph-row delta validates its source length before any topology write.
    #[doc(hidden)]
    pub fn replace_ledger_suffix(&self, retain: usize, append: &[String]) -> Result<(), String> {
        let mut ledger = self.ledger.lock();
        if retain > ledger.len() {
            return Err("graph row delta ledger prefix is invalid".to_string());
        }
        ledger.truncate(retain);
        ledger.extend_from_slice(append);
        Ok(())
    }

    pub fn clear_ledger(&self) {
        self.ledger.lock().clear();
    }

    pub fn apply_ledger(&self, transactions: Vec<String>) -> Result<(), String> {
        // Replay the whole batch under one write txn (atomic + no per-op re-lock).
        //
        // A ledger entry's property segment is HEX-ENCODED (`HexLedger`, this
        // module, "byte-identical to hex::encode") -- so replaying it requires
        // `hex::decode` back to the original msgpack property bytes, not a raw
        // `.as_bytes()` reinterpretation of the hex TEXT itself (which just
        // re-encodes the ASCII hex-digit characters as a bogus, undecodable
        // "property blob", silently corrupting every node/edge `ApplyLedger`
        // ever replayed -- caught by the durable commit's own bounded-msgpack
        // decode rejecting it as "invalid or exceeds resource limits" the
        // moment the corrupted blob is durably validated).
        let mut txn = self.txn();
        for tx in transactions {
            let parts: Vec<&str> = tx.split('|').collect();
            if parts.is_empty() {
                continue;
            }
            match parts[0] {
                "ADD_NODE" if parts.len() >= 3 => {
                    if let Ok(props) = hex::decode(parts[2]) {
                        txn.add_node(parts[1].to_string(), props);
                    }
                }
                "ADD_EDGE" if parts.len() >= 4 => {
                    if let Ok(props) = hex::decode(parts[3]) {
                        let _ = txn.add_edge(parts[1].to_string(), parts[2].to_string(), props);
                    }
                }
                "REMOVE_NODE" if parts.len() >= 2 => {
                    txn.remove_node(parts[1].to_string());
                }
                "REMOVE_EDGE" if parts.len() >= 3 => {
                    txn.remove_edge(parts[1].to_string(), parts[2].to_string());
                }
                _ => {}
            }
        }
        Ok(())
    }

    // ── Subgraph Extraction ──────────────────────────────────────────────

    /// Extract a subgraph (read view) containing only the specified node IDs.
    pub fn get_subgraph(&self, node_ids: &[String]) -> GraphView {
        let topo = self.topo.read();
        let mut view = GraphView::default();

        // Copy matching nodes (those that actually exist).
        for nid in node_ids {
            if topo.node_map.contains_key(nid) && !view.node_map.contains_key(nid) {
                let new_idx = view.graph.add_node(nid.clone());
                view.node_map.insert(nid.clone(), new_idx);
                if let Some(props) = self.node_properties.get(nid) {
                    view.node_properties.insert(nid.clone(), props.clone());
                }
                // BUG A3: point-in-time TBox membership for this induced
                // subgraph, consulted by `filter_view`.
                if self.is_schema_node(nid) {
                    view.schema_node_ids.insert(nid.clone());
                }
            }
        }

        // Walk only outgoing adjacency of selected nodes instead of scanning the
        // complete edge-property map. Parallel topology edges share one endpoint
        // property vector, so visit each endpoint pair once.
        let mut seen_pairs = std::collections::HashSet::new();
        for src in view.node_map.keys() {
            let Some(&source_index) = topo.node_map.get(src) else {
                continue;
            };
            for edge in topo
                .graph
                .edges_directed(source_index, petgraph::Direction::Outgoing)
            {
                let tgt = &topo.graph[edge.target()];
                if !view.node_map.contains_key(tgt)
                    || !seen_pairs.insert((src.clone(), tgt.clone()))
                {
                    continue;
                }
                let Some(props) = self.edge_properties.get(&(src.clone(), tgt.clone())) else {
                    continue;
                };
                let (Some(&s), Some(&t)) = (view.node_map.get(src), view.node_map.get(tgt)) else {
                    continue;
                };
                for prop in props.iter() {
                    view.graph.add_edge(s, t, format!("{}:{}", src, tgt));
                    view.edge_properties
                        .entry((src.clone(), tgt.clone()))
                        .or_default()
                        .push(prop.clone());
                }
            }
        }

        view
    }

    // ── Read-Only Compute Snapshots (CONCEPT:EG-KG.txn.per-graph-write-isolation) ────────────────────
    // CPU-heavy read-only algorithms must not run while holding a graph lock —
    // they would starve writers for the whole computation. These snapshots take a
    // cheap O(V+E) structural copy under the topology READ lock (concurrent with
    // other readers; excludes only structural writers) into an unlocked
    // `GraphView`, so the algorithm runs on the blocking pool with no lock held.
    // The ledger and embedding store are never copied — algorithms don't read them.

    /// Topology-only snapshot: petgraph structure + id↔index map. For algorithms
    /// that read only the graph shape (PageRank, betweenness, community detection,
    /// graph coloring, …).
    pub fn topology_snapshot(&self) -> GraphView {
        let topo = self.topo.read();
        GraphView {
            graph: topo.graph.clone(),
            node_map: topo.node_map.clone(),
            node_properties: HashMap::new(),
            edge_properties: HashMap::new(),
            plan_stats_memo: OnceLock::new(),
            label_index_memo: OnceLock::new(),
            distinct_stats_memo: OnceLock::new(),
            // No version captured alongside this snapshot (unlike `analysis_snapshot_versioned`),
            // so a freshly materialized projection couldn't be soundly stamped — omit the handle;
            // a `gds.*` procedure over this view just always re-projects.
            #[cfg(feature = "result-cache")]
            projection_scope: None,
            // No property blobs at all in this snapshot, so nothing for
            // `filter_view` to row-filter here (BUG A3) — empty, not omitted.
            schema_node_ids: std::collections::HashSet::new(),
            // perf/row-visibility-index: same reasoning as `schema_node_ids`
            // immediately above — no property blobs here for `can_see_node` to
            // decide visibility over, so nothing to carry. (`can_see_node`'s
            // missing-entry fallback also makes this safe even if it were used.)
            #[cfg(feature = "security")]
            visibility_index: HashMap::new(),
        }
    }

    /// Topology + property-blob snapshot (still no ledger / embedding store). For
    /// algorithms that also read node/edge property blobs: MST edge weights, VF2
    /// matching, similarity edges, lifecycle metrics.
    pub fn analysis_snapshot(&self) -> GraphView {
        let topo = self.topo.read();
        GraphView {
            graph: topo.graph.clone(),
            node_map: topo.node_map.clone(),
            node_properties: self
                .node_properties
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
            edge_properties: self
                .edge_properties
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
            plan_stats_memo: OnceLock::new(),
            label_index_memo: OnceLock::new(),
            distinct_stats_memo: OnceLock::new(),
            // No version captured alongside this snapshot — see `topology_snapshot`'s identical
            // note. Callers that need named-projection reuse must use
            // `analysis_snapshot_versioned` instead.
            #[cfg(feature = "result-cache")]
            projection_scope: None,
            // BUG A3: point-in-time TBox membership, consulted by `filter_view`.
            schema_node_ids: self.live_schema_node_ids(),
            // perf/row-visibility-index: point-in-time RLS visibility for every
            // node above, consulted by `can_see_node`/`filter_view` — see
            // `GraphView::visibility_index`'s doc.
            #[cfg(feature = "security")]
            visibility_index: self.live_visibility_index(),
        }
    }

    /// An [`analysis_snapshot`](Self::analysis_snapshot) paired with the OCC
    /// `version()` read UNDER the same topology read lock (CONCEPT:EG-KG.coordination.distributed-cache-coherence). Taking
    /// both atomically lets the result cache store a query's bytes under exactly the
    /// version the snapshot reflects: a topology write bumps `version` via
    /// `mark_dirty` only while holding the topo WRITE lock, mutually exclusive with
    /// the read lock held here — so the `(view, version)` pair is point-in-time
    /// consistent and a cache entry can never claim a version newer than its data.
    #[cfg(feature = "result-cache")]
    pub fn analysis_snapshot_versioned(&self) -> (GraphView, u64) {
        let topo = self.topo.read();
        let version = self.version();
        let view = GraphView {
            graph: topo.graph.clone(),
            node_map: topo.node_map.clone(),
            node_properties: self
                .node_properties
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
            edge_properties: self
                .edge_properties
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
            plan_stats_memo: OnceLock::new(),
            label_index_memo: OnceLock::new(),
            distinct_stats_memo: OnceLock::new(),
            // CONCEPT:EG-KG.query.named-graph-projection-catalog (W4.5/N5) — hand this view a
            // SHARED handle (Arc clones — the same live objects, not forks) to the graph's named
            // projection catalog + the dependency clock that validates it, stamped with the SAME
            // `version` just read atomically above. This is the ONE snapshot constructor that
            // populates it (see `ProjectionScope`'s doc for why the others deliberately don't).
            projection_scope: Some(ProjectionScope {
                catalog: Arc::clone(&self.graph_projections),
                dep_clock: Arc::clone(&self.dep_clock),
                version,
            }),
            // BUG A3: point-in-time TBox membership, consulted by `filter_view`.
            schema_node_ids: self.live_schema_node_ids(),
            // perf/row-visibility-index: same as `analysis_snapshot` — see
            // `GraphView::visibility_index`'s doc.
            #[cfg(feature = "security")]
            visibility_index: self.live_visibility_index(),
        };
        (view, version)
    }

    // ── Graph Forking ────────────────────────────────────────────────────

    /// Deep-clone into a new, independent LIVE graph (fresh locks).
    pub fn fork(&self) -> GraphCore {
        let topo = self.topo.read();
        GraphCore {
            topo: RwLock::new(topo.clone()),
            node_properties: self
                .node_properties
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
            edge_properties: self
                .edge_properties
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
            ledger: Mutex::new(self.ledger.lock().clone()),
            semantic_store: RwLock::new(self.semantic_store.read().clone()),
            integrity_policy: RwLock::new(self.integrity_policy.read().clone()),
            dirty: std::sync::atomic::AtomicBool::new(true),
            // Fork starts a fresh OCC version line (CONCEPT:EG-KG.txn.occ-graph-core) — it is a new
            // independent graph; any txn against the fork baselines from 0.
            version: std::sync::atomic::AtomicU64::new(0),
            // A fork is a fresh, detached graph with its own (empty) subscriber set
            // (CONCEPT:EG-KG.compute.cdc-event-emit) — subscriptions attach to the live registered core.
            changes: ChangeNotifier::default(),
            // Fork starts with cold lazy indexes; rebuilt lazily on first use.
            ontology_index: RwLock::new(None),
            label_index: RwLock::new(None),
            node_id_index: RwLock::new(None),
            edge_key_index: RwLock::new(None),
            property_index: RwLock::new(None),
            // CONCEPT:EG-KG.compute.json-deep-indexing — fork starts with a cold JSONPath path-index too.
            path_index: RwLock::new(None),
            // CONCEPT:EG-KG.storage.path-index-store — a fork is a fresh, detached graph (not registered, not
            // backed by a durable tier), so it carries no path-index store; its
            // path-index stays fully in-memory.
            path_index_store: RwLock::new(None),
            // A fork gets its own default index registry (the registry is fixed
            // metadata, not graph state) (CONCEPT:AU-KG.retrieval.architecture-report).
            index_manager: crate::index::IndexManager::with_default_indexes(),
            // A fork is a fresh, detached graph (not registered, not backed by the
            // durable tier), so it carries no read-through (CONCEPT:EG-KG.storage.read-through-seam-exercised).
            read_through: RwLock::new(None),
            // CONCEPT:EG-KG.storage.bloom-negative-lookup-guard — a fork copies the parent's
            // COMPLETE resident node set (no read-through of its own to guard
            // anyway), so its bloom filter is rebuilt fresh from those exact ids
            // and trusted immediately.
            node_bloom: RwLock::new({
                let filter = crate::bloom::NodeBloomFilter::new(self.node_properties.len(), 0.01);
                for id in self.node_properties.iter().map(|e| e.key().clone()) {
                    filter.insert(&id);
                }
                filter
            }),
            bloom_complete: std::sync::atomic::AtomicBool::new(true),
            // A fork starts a fresh OCC version line (version 0 above), so its index stamps and
            // dependency clock start cold too — every lazy index rebuilds on first use.
            index_stamps: IndexStamps::default(),
            index_rebuilds: std::sync::atomic::AtomicU64::new(0),
            #[cfg(feature = "result-cache")]
            dep_clock: Arc::new(crate::dep_scope::DepClock::new()),
            // A fork starts with an empty result cache (its own version line).
            #[cfg(feature = "result-cache")]
            result_cache: crate::result_cache::ResultCache::new(),
            // A fork is a distinct graph (its own version line, above) — it must NOT share the
            // parent's named projections (they were materialized+stamped against the PARENT's
            // version history, meaningless against the fork's fresh one). Fresh + empty; the
            // fork rebuilds any projection it needs on first `gds.graph.project`.
            #[cfg(feature = "result-cache")]
            graph_projections: Arc::new(crate::projection_catalog::ProjectionCatalog::new()),
            // A fork is a distinct graph (its own fresh OCC version line, above) — an
            // RLS projection cached against the PARENT's version history would never
            // hit (this fork's `version()` starts at 0, unrelated to the parent's),
            // so a fresh, empty cache costs nothing and stays correct by construction.
            #[cfg(feature = "security")]
            rls_projection_cache: crate::rls_projection_cache::ProjectionCache::default(),
            // Same reasoning as `rls_projection_cache` immediately above: a fork's own
            // fresh OCC version line can never hit an entry cached against the parent's.
            #[cfg(feature = "security")]
            rls_view_cache: crate::rls_view_cache::FilteredViewCache::default(),
            // perf/row-visibility-index — a fork starts with a cold visibility
            // index too, same reasoning as `label_index`/etc above: rebuilt lazily
            // on first use, then incrementally maintained from its own fresh
            // version line.
            #[cfg(feature = "security")]
            visibility_index: RwLock::new(None),
            #[cfg(feature = "security")]
            visibility_index_rebuilds: std::sync::atomic::AtomicU64::new(0),
            // A fork deep-clones the parent's DATA (node/edge properties above), so
            // its TBox/ABox reverse index (BUG A3) must mirror the parent's live
            // schema-triple counts at fork time too -- schema-ness is content-
            // derived, not graph-identity-derived like the version line/caches above.
            schema_refs: self
                .schema_refs
                .iter()
                .map(|e| (e.key().clone(), *e.value()))
                .collect(),
            // A fork deep-clones the parent's CURRENT ledger contents
            // (`ledger` above), so the watermark (BUG A1 follow-up) must
            // mirror the parent's at fork time too -- the fork's retained
            // window is identical to the parent's at this instant, and its
            // OWN future drops start counting from here.
            ledger_dropped_total: std::sync::atomic::AtomicU64::new(
                self.ledger_dropped_total
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }

    // ── A18 TBox/ABox RLS distinction (BUG A3, 2026-08-12) ─────────────────
    // See `schema_refs`'s own field doc for the full history. `eg-rdf`'s SPARQL
    // UPDATE insert/delete path is the ONLY caller: it marks/unmarks in the
    // SAME transaction as the triple write/delete that makes/unmakes `id` the
    // subject or object of a schema-defining triple.

    /// Record one more LIVE schema-defining triple naming `id` (BUG A3). Call
    /// once per schema-defining triple INSERTED that names `id` as subject or
    /// object, in the same mutation as the triple write.
    pub fn mark_schema_ref(&self, id: &str) {
        *self.schema_refs.entry(id.to_string()).or_insert(0) += 1;
    }

    /// Release one LIVE schema-defining triple naming `id` (BUG A3). Call once
    /// per schema-defining triple REMOVED that names `id` as subject or
    /// object, in the same mutation as the triple delete. Saturating: never
    /// underflows past 0 — a defensive floor, not something callers should
    /// rely on for correctness (mark/unmark are expected to stay symmetric per
    /// triple).
    pub fn unmark_schema_ref(&self, id: &str) {
        if let Some(mut count) = self.schema_refs.get_mut(id) {
            *count = count.saturating_sub(1);
        }
    }

    /// Is `id` CURRENTLY ontology schema (TBox)? DERIVED from the live reverse
    /// index (BUG A3) — true iff at least one schema-defining triple currently
    /// names `id` as subject or object. Never cached; a node whose last
    /// schema-defining triple was deleted answers `false` again immediately,
    /// with no separate "clear the marker" step.
    pub fn is_schema_node(&self, id: &str) -> bool {
        self.schema_refs.get(id).is_some_and(|c| *c > 0)
    }

    /// The full set of node ids CURRENTLY schema (BUG A3) — for snapshotting
    /// into a [`GraphView`] (`GraphView::schema_node_ids`) so
    /// `IsolationLayer::filter_view` can consult it per row without needing a
    /// live `&GraphCore` reference. `O(schema size)`, not `O(graph size)` — an
    /// ontology's class/property/axiom count, never the whole ABox.
    fn live_schema_node_ids(&self) -> std::collections::HashSet<String> {
        self.schema_refs
            .iter()
            .filter(|e| *e.value() > 0)
            .map(|e| e.key().clone())
            .collect()
    }

    pub fn diff_against(&self, other: &GraphView) -> String {
        let topo = self.topo.read();
        let self_nodes: std::collections::HashSet<&String> = topo.node_map.keys().collect();
        let other_nodes: std::collections::HashSet<&String> = other.node_map.keys().collect();

        let added: Vec<&String> = other_nodes.difference(&self_nodes).cloned().collect();
        let removed: Vec<&String> = self_nodes.difference(&other_nodes).cloned().collect();

        let mut modified: Vec<&String> = Vec::new();
        for node_id in self_nodes.intersection(&other_nodes) {
            let self_props = self.node_properties.get(*node_id).map(|a| a.clone());
            let other_props = other.node_properties.get(*node_id).cloned();
            if self_props != other_props {
                modified.push(node_id);
            }
        }

        let self_edges: std::collections::HashSet<(String, String)> = self
            .edge_properties
            .iter()
            .map(|e| e.key().clone())
            .collect();
        let other_edges: std::collections::HashSet<&(String, String)> =
            other.edge_properties.keys().collect();
        let edges_added: Vec<&(String, String)> = other_edges
            .iter()
            .filter(|k| !self_edges.contains(**k))
            .cloned()
            .collect();
        let edges_removed: Vec<&(String, String)> = self_edges
            .iter()
            .filter(|k| !other_edges.contains(k))
            .collect();

        let diff = serde_json::json!({
            "nodes_added": added,
            "nodes_removed": removed,
            "nodes_modified": modified,
            "edges_added": edges_added,
            "edges_removed": edges_removed,
        });
        diff.to_string()
    }

    // ── Compaction ───────────────────────────────────────────────────────

    pub fn compact_nodes_by_type(&self, node_type: &str, threshold: usize) -> Vec<String> {
        let mut candidates: Vec<String> = Vec::new();
        for entry in self.node_properties.iter() {
            let (node_id, props_json) = (entry.key(), entry.value());
            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(props_json.as_slice()) {
                if let Some(t) = val.get("type").and_then(|v| v.as_str()) {
                    if t == node_type {
                        candidates.push(node_id.clone());
                    }
                }
            }
        }

        if candidates.len() <= threshold {
            return Vec::new();
        }

        let summary_id = format!("summary:{}:{}", node_type, candidates.len());
        let summary_props = serde_json::json!({
            "type": format!("{}_summary", node_type),
            "compacted_count": candidates.len(),
            "original_type": node_type,
        });
        self.add_node(summary_id.clone(), summary_props.to_string().into_bytes());

        let mut removed = Vec::new();
        for node_id in &candidates {
            self.remove_node(node_id.clone());
            removed.push(node_id.clone());
        }
        removed
    }

    // ── VF2 Subgraph Matching ────────────────────────────────────────────

    /// VF2 subgraph isomorphism match against a consistent read view so the
    /// worst-case-exponential backtracking never holds a live lock. `max_results`/
    /// `max_steps` (`0` ⇒ [`DEFAULT_VF2_MAX_RESULTS`]/[`DEFAULT_VF2_MAX_STEPS`]) bound
    /// the search; the returned `bool` is `true` when it stopped early against
    /// either budget rather than exhausting the search space.
    pub fn vf2_subgraph_match(
        &self,
        pattern: &GraphView,
        max_results: usize,
        max_steps: usize,
    ) -> (Vec<HashMap<String, String>>, bool) {
        let host = self.analysis_snapshot();
        vf2_match_views(&host, pattern, max_results, max_steps)
    }

    /// The least-recently-added node ids that would be evicted to bring the graph
    /// down to `max_nodes` — the same set (and order) `evict_lru` removes, but
    /// WITHOUT dropping them (CONCEPT:EG-KG.storage.read-through-seam-exercised). Used by the authoritative
    /// eviction path, which must confirm each candidate is durable BEFORE
    /// dropping it (commit-before-ack makes that the common case; the check is the
    /// no-data-loss guarantee). Empty when the graph is at/under the cap.
    pub fn lru_eviction_candidates(&self, max_nodes: usize) -> Vec<String> {
        let mut indexed: Vec<(String, NodeIndex)> = {
            let topo = self.topo.read();
            if topo.node_map.len() <= max_nodes {
                return Vec::new();
            }
            topo.node_map.iter().map(|(k, &v)| (k.clone(), v)).collect()
        };
        let to_evict = indexed.len() - max_nodes;
        // Nodes with the lowest NodeIndex were inserted earliest → approximate LRU.
        // Partition in expected O(N); a full O(N log N) ordering is unnecessary
        // because eviction consumes the set as one batch.
        if to_evict < indexed.len() {
            indexed.select_nth_unstable_by_key(to_evict, |(_, index)| *index);
            indexed.truncate(to_evict);
        }
        indexed.into_iter().map(|(id, _)| id).collect()
    }

    /// Evict nodes down to `max_nodes` by removing the least-recently-added.
    ///
    /// CONCEPT:EG-KG.compute.graph-compute-engine — Memory pressure defense. When the in-memory graph
    /// grows beyond `max_nodes`, this method removes the oldest nodes (by
    /// insertion order in `node_map`) until the count is at or below the cap.
    /// Returns the number of evicted nodes.
    pub fn evict_lru(&self, max_nodes: usize) -> usize {
        let evict_ids = self.lru_eviction_candidates(max_nodes);
        self.evict_resident_nodes(&evict_ids)
    }

    // ── Ebbinghaus Temporal Decay (CONCEPT:EG-KG.compute.graph-compute-engine) ──────────────────────

    /// Apply an Ebbinghaus forgetting-curve decay to every node's and edge's
    /// belief `confidence`, then optionally prune anything below `floor`.
    ///
    /// Retention follows `R = 0.5^(Δt / half_life)` where `Δt` is the seconds
    /// elapsed since the item's `last_access` (falling back to `updated_at` →
    /// `created_at` → `now`, so a freshly-stamped item never decays on its first
    /// sweep). The decayed confidence is persisted and `last_access` advanced to
    /// `now`, so repeated sweeps compound exactly: `R(Δt₁)·R(Δt₂) = R(Δt₁+Δt₂)`.
    /// A per-item `half_life` property overrides `default_half_life` when present
    /// and positive. Properties are read/written as MessagePack (the wire/storage
    /// format produced by `client.nodes.add`).
    pub fn decay_sweep(
        &self,
        now: u64,
        default_half_life: f64,
        floor: f64,
        prune: bool,
    ) -> crate::types::DecayStats {
        let mut stats = crate::types::DecayStats::default();
        let mut node_prune: Vec<String> = Vec::new();
        let mut edge_prune: Vec<(String, String)> = Vec::new();

        // The property re-encode runs under the topology READ lock: it excludes
        // structural writers (add/remove go through a write txn), so a node can't
        // be concurrently removed while we re-insert its decayed properties (which
        // would resurrect it). Reads/other property updates still proceed.
        {
            let _topo = self.topo.read();

            // ── Nodes ──
            let node_ids: Vec<String> = self
                .node_properties
                .iter()
                .map(|e| e.key().clone())
                .collect();
            for nid in node_ids {
                if let Some(bytes) = self.node_properties.get(&nid).map(|r| r.value().clone()) {
                    if let Ok(mut val) = decode_property_value(&bytes) {
                        if let Some(obj) = val.as_object_mut() {
                            let (new_conf, changed) = apply_decay(obj, now, default_half_life);
                            if changed {
                                stats.nodes_decayed += 1;
                                if let Ok(reenc) = rmp_serde::to_vec_named(&val) {
                                    self.node_properties.insert(nid.clone(), Arc::new(reenc));
                                }
                            }
                            if prune && new_conf < floor {
                                node_prune.push(nid.clone());
                            }
                        }
                    }
                }
            }

            // ── Edges ── (edge_properties: (src,tgt) -> Vec<Vec<u8>> parallel edges)
            let edge_keys: Vec<(String, String)> = self
                .edge_properties
                .iter()
                .map(|e| e.key().clone())
                .collect();
            for key in edge_keys {
                let mut min_conf = 1.0f64;
                if let Some(mut blobs) = self.edge_properties.get_mut(&key) {
                    for b in blobs.iter_mut() {
                        if let Ok(mut val) = decode_property_value(b.as_slice()) {
                            if let Some(obj) = val.as_object_mut() {
                                let (new_conf, changed) = apply_decay(obj, now, default_half_life);
                                if changed {
                                    stats.edges_decayed += 1;
                                    if let Ok(reenc) = rmp_serde::to_vec_named(&val) {
                                        *b = Arc::new(reenc);
                                    }
                                }
                                if new_conf < min_conf {
                                    min_conf = new_conf;
                                }
                            }
                        }
                    }
                }
                if prune && min_conf < floor {
                    edge_prune.push(key);
                }
            }
        }

        // ── Prune below floor (each removal takes its own write txn) ──
        for (s, t) in &edge_prune {
            self.remove_edge(s.clone(), t.clone());
            stats.edges_pruned += 1;
        }
        for nid in &node_prune {
            self.remove_node(nid.clone());
            stats.nodes_pruned += 1;
        }
        // Decay/prune mutated persistent state → the next checkpoint must rewrite
        // this graph (Phase C-C). The background sweep does not go through dispatch,
        // so it marks dirty here directly.
        if stats.nodes_decayed > 0
            || stats.edges_decayed > 0
            || stats.nodes_pruned > 0
            || stats.edges_pruned > 0
        {
            self.mark_dirty();
        }
        stats
    }

    /// Refresh the given nodes on access (spaced-repetition reset): stamp
    /// `last_access = now` and restore `confidence = 1.0` so the forgetting
    /// clock restarts. Call when an agent actually reads/uses a fact. Returns
    /// the number of nodes touched.
    pub fn touch_nodes(&self, node_ids: &[String], now: u64) -> usize {
        let _topo = self.topo.read();
        let mut touched = 0usize;
        for nid in node_ids {
            if let Some(bytes) = self.node_properties.get(nid).map(|a| (**a).clone()) {
                if let Ok(mut val) = decode_property_value(&bytes) {
                    if let Some(obj) = val.as_object_mut() {
                        obj.insert("last_access".to_string(), serde_json::json!(now));
                        obj.insert("confidence".to_string(), serde_json::json!(1.0_f64));
                        if let Ok(reenc) = rmp_serde::to_vec_named(&val) {
                            self.node_properties.insert(nid.clone(), Arc::new(reenc));
                            touched += 1;
                        }
                    }
                }
            }
        }
        touched
    }

    // ── Agent-native memory primitives — one-shot wrappers + queries ──────────
    //     (CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg hierarchical summary tier / EG-221 consolidation)

    /// One-shot [`GraphTxn::create_summary_node`] (CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg): the whole
    /// create-node + link-children runs under ONE topology write guard, then the
    /// lazy secondary indexes are invalidated via `mark_dirty` so a subsequent
    /// `summaries_at_level` sees the new node. Returns the summary node id.
    pub fn create_summary_node(
        &self,
        level: u32,
        child_ids: &[String],
        props: serde_json::Map<String, serde_json::Value>,
    ) -> String {
        let id = self.txn().create_summary_node(level, child_ids, props);
        self.mark_dirty();
        id
    }

    /// One-shot [`GraphTxn::consolidate`] (CONCEPT:EG-KG.compute.consolidate-cluster): the whole
    /// create-semantic + redirect-edges + provenance + mark-episodics runs under ONE
    /// topology write guard (atomic + localized — no global reindex), then the lazy
    /// secondary indexes are invalidated. Returns the semantic node id.
    pub fn consolidate(
        &self,
        episodic_ids: &[String],
        semantic_props: serde_json::Map<String, serde_json::Value>,
    ) -> String {
        let id = self.txn().consolidate(episodic_ids, semantic_props);
        self.mark_dirty();
        id
    }

    /// Does a `source → target` edge carrying `relationship` exist? (Read helper for
    /// the summary/consolidation queries — CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg.) Matches on the edge
    /// blob's canonical `relationship` field.
    fn edge_has_relationship(&self, source_id: &str, target_id: &str, relationship: &str) -> bool {
        self.edge_properties
            .get(&(source_id.to_string(), target_id.to_string()))
            .is_some_and(|blobs| {
                blobs.iter().any(|b| {
                    decode_property_value(b)
                        .ok()
                        .and_then(|v| {
                            v.as_object()
                                .and_then(|o| o.get("relationship"))
                                .and_then(|r| r.as_str())
                                .map(|s| s == relationship)
                        })
                        .unwrap_or(false)
                })
            })
    }

    /// CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg — the direct children of summary node `id`: the targets of its
    /// outgoing `SUMMARIZES` edges. Returns ids sorted + deduped. Empty if the node
    /// is absent or has no summary children.
    pub fn summary_children(&self, id: &str) -> Vec<String> {
        let targets: Vec<String> = {
            let topo = self.topo.read();
            let Some(&idx) = topo.node_map.get(id) else {
                return Vec::new();
            };
            topo.graph
                .edges_directed(idx, petgraph::Direction::Outgoing)
                .map(|e| topo.graph[e.target()].clone())
                .collect()
        };
        let mut out: Vec<String> = targets
            .into_iter()
            .filter(|t| self.edge_has_relationship(id, t, "SUMMARIZES"))
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg — all summary node ids at abstraction `level` (nodes typed
    /// `SummaryNode` whose `summary_level` equals `level`). Returns ids sorted +
    /// deduped. Uses the lazy label index (`get_nodes_by_label`) so the `SummaryNode`
    /// scan is O(matches) once the index is warm.
    pub fn summaries_at_level(&self, level: u32) -> Vec<String> {
        let mut out: Vec<String> = self
            .get_nodes_by_label("SummaryNode", 0)
            .into_iter()
            .filter_map(|(id, blob)| {
                let val = decode_property_value(&blob).ok()?;
                let lvl = val.get("summary_level").and_then(|v| v.as_u64())?;
                (lvl == level as u64).then_some(id)
            })
            .collect();
        out.sort();
        out.dedup();
        out
    }

    // ── Memory maintenance — one-shot wrappers (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive) ────────────────
    //     (decay + reinforcement; the AU maintenance loop schedules these)

    /// One-shot [`GraphTxn::reinforce`] (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive): bump access/recency +
    /// importance under ONE topology write guard, then invalidate the lazy secondary
    /// indexes. Returns whether the node existed.
    pub fn reinforce(&self, id: &str, now_ms: u64, weight: f64) -> bool {
        let ok = self.txn().reinforce(id, now_ms, weight);
        if ok {
            self.mark_dirty();
        }
        ok
    }

    /// One-shot [`GraphTxn::decay_node`] (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive). Returns whether it
    /// decayed/stamped the node.
    pub fn decay_node(&self, id: &str, now_ms: u64, half_life_ms: u64) -> bool {
        let ok = self.txn().decay_node(id, now_ms, half_life_ms);
        if ok {
            self.mark_dirty();
        }
        ok
    }

    /// One-shot [`GraphTxn::decay_memories`] (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive): the whole working-set
    /// decay runs under ONE topology write guard (localized — no global scan). Returns
    /// the number of nodes decayed.
    pub fn decay_memories(&self, now_ms: u64, half_life_ms: u64, ids: &[String]) -> usize {
        let n = self.txn().decay_memories(now_ms, half_life_ms, ids);
        if n > 0 {
            self.mark_dirty();
        }
        n
    }

    /// One-shot [`GraphTxn::forget`] (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive). `delete == false` marks
    /// `forgotten` (provenance-preserving default); `true` hard-removes. Returns
    /// whether it acted.
    pub fn forget(&self, id: &str, delete: bool) -> bool {
        let ok = self.txn().forget(id, delete);
        if ok {
            self.mark_dirty();
        }
        ok
    }

    /// One-shot [`GraphTxn::evict_below`] (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive): prune the sub-threshold
    /// members of the working set under ONE topology write guard. Returns the pruned
    /// ids (sorted).
    pub fn evict_below(&self, ids: &[String], threshold: f64, delete: bool) -> Vec<String> {
        let pruned = self.txn().evict_below(ids, threshold, delete);
        if !pruned.is_empty() {
            self.mark_dirty();
        }
        pruned
    }

    /// One-shot [`GraphTxn::maintain`] (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive): decay-then-evict the working
    /// set under ONE topology write guard (atomic + localized — no global reindex) —
    /// the primitive the AU maintenance loop schedules. Returns `(decayed, pruned_ids)`.
    pub fn maintain(
        &self,
        ids: &[String],
        now_ms: u64,
        half_life_ms: u64,
        evict_threshold: f64,
        delete: bool,
    ) -> (usize, Vec<String>) {
        let out = self
            .txn()
            .maintain(ids, now_ms, half_life_ms, evict_threshold, delete);
        self.mark_dirty();
        out
    }

    // ── Scene-graph / 3D world model — one-shot wrappers + queries ────────────
    //     (CONCEPT:EG-KG.compute.scene-graph-primitives)

    /// One-shot [`GraphTxn::add_scene_object`] (CONCEPT:EG-KG.compute.scene-graph-primitives): create the node +
    /// link its parent under ONE write guard, then invalidate the lazy secondary
    /// indexes so a subsequent `scene_children` / `SceneObject` label query sees it.
    /// Returns the new object's id.
    pub fn add_scene_object(&self, pose: &crate::scene::Pose, parent: Option<&str>) -> String {
        let id = self.txn().add_scene_object(pose, parent);
        self.mark_dirty();
        id
    }

    /// One-shot [`GraphTxn::set_pose`] (CONCEPT:EG-KG.compute.scene-graph-primitives).
    pub fn set_pose(&self, id: &str, pose: &crate::scene::Pose) -> bool {
        let ok = self.txn().set_pose(id, pose);
        if ok {
            self.mark_dirty();
        }
        ok
    }

    /// One-shot [`GraphTxn::reparent`] (CONCEPT:EG-KG.compute.scene-graph-primitives).
    pub fn reparent(&self, id: &str, new_parent: Option<&str>) -> bool {
        let ok = self.txn().reparent(id, new_parent);
        if ok {
            self.mark_dirty();
        }
        ok
    }

    /// One-shot [`GraphTxn::add_spatial_relation`] (CONCEPT:EG-KG.compute.scene-graph-primitives).
    pub fn add_spatial_relation(&self, from: &str, to: &str, rel: &str) -> bool {
        let ok = self.txn().add_spatial_relation(from, to, rel);
        if ok {
            self.mark_dirty();
        }
        ok
    }

    /// One-shot [`GraphTxn::set_bounding_volume`] (CONCEPT:EG-KG.compute.scene-graph-primitives).
    pub fn set_bounding_volume(&self, id: &str, aabb: &crate::scene::Aabb) -> bool {
        let ok = self.txn().set_bounding_volume(id, aabb);
        if ok {
            self.mark_dirty();
        }
        ok
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — the stored LOCAL pose of scene object `id`. `None` if the
    /// node is absent or carries no (decodable) `pose`.
    pub fn get_pose(&self, id: &str) -> Option<crate::scene::Pose> {
        let blob = self.get_node_properties(id)?;
        let val = decode_property_value(&blob).ok()?;
        crate::scene::Pose::from_json(val.as_object()?.get("pose")?)
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — the stored axis-aligned bounding volume of scene object `id`,
    /// in its LOCAL frame. `None` if absent / no (decodable) `aabb`.
    pub fn get_bounding_volume(&self, id: &str) -> Option<crate::scene::Aabb> {
        let blob = self.get_node_properties(id)?;
        let val = decode_property_value(&blob).ok()?;
        crate::scene::Aabb::from_json(val.as_object()?.get("aabb")?)
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — the transform parent of scene object `id`: the target of its
    /// outgoing `CHILD_OF` edge, or `None` for a root object / absent node.
    pub fn scene_parent(&self, id: &str) -> Option<String> {
        let idx = *self.topo.read().node_map.get(id)?;
        let targets: Vec<String> = {
            let topo = self.topo.read();
            topo.graph
                .edges_directed(idx, petgraph::Direction::Outgoing)
                .map(|e| topo.graph[e.target()].clone())
                .collect()
        };
        targets
            .into_iter()
            .find(|t| self.edge_has_relationship(id, t, "CHILD_OF"))
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — the WORLD pose of scene object `id`: its local pose composed
    /// with every ancestor's local pose up the `CHILD_OF` chain (root ∘ … ∘ local).
    /// `None` if `id` is absent / has no pose. A cycle guard bounds the walk (a
    /// well-formed hierarchy is acyclic; the guard just prevents a pathological loop
    /// from spinning). Off-lock pure math once the chain is gathered.
    pub fn world_transform(&self, id: &str) -> Option<crate::scene::Pose> {
        // Gather the chain of ids from `id` up to the root (id first).
        let mut chain: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut cur = id.to_string();
        while seen.insert(cur.clone()) {
            chain.push(cur.clone());
            match self.scene_parent(&cur) {
                Some(p) => cur = p,
                None => break,
            }
        }
        // Compose from the ROOT down to `id`: world = root ∘ … ∘ local(id).
        let mut world: Option<crate::scene::Pose> = None;
        for node in chain.iter().rev() {
            let local = self.get_pose(node)?;
            world = Some(match world {
                Some(w) => w.compose(&local),
                None => local,
            });
        }
        world
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — the direct transform children of scene object `id`: the
    /// targets of its outgoing `HAS_CHILD` edges. Sorted + deduped; empty if absent /
    /// no children.
    pub fn scene_children(&self, id: &str) -> Vec<String> {
        let targets: Vec<String> = {
            let topo = self.topo.read();
            let Some(&idx) = topo.node_map.get(id) else {
                return Vec::new();
            };
            topo.graph
                .edges_directed(idx, petgraph::Direction::Outgoing)
                .map(|e| topo.graph[e.target()].clone())
                .collect()
        };
        let mut out: Vec<String> = targets
            .into_iter()
            .filter(|t| self.edge_has_relationship(id, t, "HAS_CHILD"))
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — all transitive transform descendants of scene object `id`
    /// (breadth-first over `HAS_CHILD`). Sorted + deduped; excludes `id` itself. A
    /// visited-set makes it robust to a malformed cyclic hierarchy.
    pub fn scene_descendants(&self, id: &str) -> Vec<String> {
        let mut out: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let mut queue: std::collections::VecDeque<String> = std::collections::VecDeque::new();
        queue.push_back(id.to_string());
        let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();
        visited.insert(id.to_string());
        while let Some(n) = queue.pop_front() {
            for c in self.scene_children(&n) {
                if visited.insert(c.clone()) {
                    out.insert(c.clone());
                    queue.push_back(c);
                }
            }
        }
        out.into_iter().collect()
    }

    /// CONCEPT:EG-KG.compute.scene-graph-primitives — every `(from, to)` pair connected by a spatial relationship
    /// edge carrying `rel` (e.g. `ON`/`IN`/`NEAR`/`SUPPORTS`). Sorted + deduped.
    pub fn objects_with_relation(&self, rel: &str) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = self
            .edge_properties
            .iter()
            .filter_map(|entry| {
                let (src, tgt) = entry.key();
                entry
                    .value()
                    .iter()
                    .any(|b| {
                        decode_property_value(b)
                            .ok()
                            .and_then(|v| {
                                v.as_object()
                                    .and_then(|o| o.get("relationship"))
                                    .and_then(|r| r.as_str())
                                    .map(|s| s == rel)
                            })
                            .unwrap_or(false)
                    })
                    .then(|| (src.clone(), tgt.clone()))
            })
            .collect();
        out.sort();
        out.dedup();
        out
    }

    // ── Action / policy / trajectory memory — one-shot wrappers + queries ─────
    //     (CONCEPT:EG-KG.compute.discounted-return)

    /// One-shot [`GraphTxn::start_trajectory`] (CONCEPT:EG-KG.compute.discounted-return): create/UPSERT the
    /// `:Trajectory` node under ONE topology write guard, then invalidate the lazy
    /// secondary indexes. Returns the trajectory id.
    pub fn start_trajectory(&self, props: serde_json::Map<String, serde_json::Value>) -> String {
        let id = self.txn().start_trajectory(props);
        self.mark_dirty();
        id
    }

    /// One-shot [`GraphTxn::append_step`] (CONCEPT:EG-KG.compute.discounted-return): create the `:Step`, link it
    /// (`BELONGS_TO` + `HAS_STEP` + `NEXT_STEP`), and advance the trajectory's
    /// tail/count — all under ONE topology write guard (atomic), then invalidate the
    /// lazy secondary indexes. Returns the new step id, or `None` if the trajectory is
    /// absent.
    #[allow(clippy::too_many_arguments)]
    pub fn append_step(
        &self,
        traj_id: &str,
        action: serde_json::Value,
        reward: f64,
        state_ref: Option<&str>,
        next_state_ref: Option<&str>,
        t: u64,
    ) -> Option<String> {
        let id = self
            .txn()
            .append_step(traj_id, action, reward, state_ref, next_state_ref, t);
        if id.is_some() {
            self.mark_dirty();
        }
        id
    }

    /// Decode a step node's stored property object (CONCEPT:EG-KG.compute.discounted-return read helper).
    /// `None` if the node is absent or its blob is not a decodable object.
    fn step_object(&self, id: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
        let blob = self.get_node_properties(id)?;
        match decode_property_value(&blob) {
            Ok(serde_json::Value::Object(o)) => Some(o),
            _ => None,
        }
    }

    /// CONCEPT:EG-KG.compute.discounted-return — the ordered `(step_id, step_object)` pairs of trajectory
    /// `traj_id`: the targets of its outgoing `HAS_STEP` edges, ordered by each step's
    /// `step_index` (append order). Ties on `step_index` fall back to the id for a
    /// deterministic total order. Empty if the trajectory is absent / has no steps.
    fn ordered_steps(
        &self,
        traj_id: &str,
    ) -> Vec<(String, serde_json::Map<String, serde_json::Value>)> {
        let targets: Vec<String> = {
            let topo = self.topo.read();
            let Some(&idx) = topo.node_map.get(traj_id) else {
                return Vec::new();
            };
            topo.graph
                .edges_directed(idx, petgraph::Direction::Outgoing)
                .map(|e| topo.graph[e.target()].clone())
                .collect()
        };
        let mut steps: Vec<(String, serde_json::Map<String, serde_json::Value>)> = targets
            .into_iter()
            .filter(|t| self.edge_has_relationship(traj_id, t, "HAS_STEP"))
            .filter_map(|t| self.step_object(&t).map(|o| (t, o)))
            .collect();
        steps.sort_by(|a, b| {
            let ai = a.1.get("step_index").and_then(|v| v.as_u64()).unwrap_or(0);
            let bi = b.1.get("step_index").and_then(|v| v.as_u64()).unwrap_or(0);
            ai.cmp(&bi).then_with(|| a.0.cmp(&b.0))
        });
        steps
    }

    /// CONCEPT:EG-KG.compute.discounted-return — the ordered step ids of trajectory `traj_id` (append order,
    /// following the `NEXT_STEP` chain). Empty if the trajectory is absent / has no
    /// steps.
    pub fn trajectory_steps(&self, traj_id: &str) -> Vec<String> {
        self.ordered_steps(traj_id)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    }

    /// CONCEPT:EG-KG.compute.discounted-return — the DISCOUNTED return of trajectory `traj_id`: `Σ gamma^t *
    /// reward` over its ordered steps, where `t` is each step's stored timestep and
    /// `reward` its reward (a step missing `reward` contributes 0; a step missing `t`
    /// reads as `t = 0`). With sequential timesteps `t = 0,1,2,…` this is the standard
    /// RL return `Σ gamma^i r_i`. Deterministic — `gamma` is caller-supplied, no
    /// clock/RNG. `0.0` for an absent / empty trajectory.
    pub fn discounted_return(&self, traj_id: &str, gamma: f64) -> f64 {
        self.ordered_steps(traj_id)
            .iter()
            .map(|(_, o)| {
                let reward = o.get("reward").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let t = o.get("t").and_then(|v| v.as_u64()).unwrap_or(0);
                gamma.powf(t as f64) * reward
            })
            .sum()
    }

    /// CONCEPT:EG-KG.compute.discounted-return — the UNDISCOUNTED total reward of trajectory `traj_id`: the plain
    /// sum of its steps' `reward` values (equivalent to `discounted_return` with
    /// `gamma = 1.0`). `0.0` for an absent / empty trajectory.
    pub fn total_reward(&self, traj_id: &str) -> f64 {
        self.ordered_steps(traj_id)
            .iter()
            .map(|(_, o)| o.get("reward").and_then(|v| v.as_f64()).unwrap_or(0.0))
            .sum()
    }

    /// CONCEPT:EG-KG.compute.discounted-return — SLIDING-window undiscounted returns over trajectory `traj_id`:
    /// for ordered rewards `r_0 … r_{n-1}`, returns `[Σ r_i..i+window]` for each start
    /// `i` in `0..=n-window` (the n-step return at each position). Empty when `window`
    /// is 0 or larger than the step count. Deterministic. Useful for spotting the
    /// best/worst stretch of a long episode for prioritized replay.
    pub fn windowed_returns(&self, traj_id: &str, window: usize) -> Vec<f64> {
        let rewards: Vec<f64> = self
            .ordered_steps(traj_id)
            .iter()
            .map(|(_, o)| o.get("reward").and_then(|v| v.as_f64()).unwrap_or(0.0))
            .collect();
        if window == 0 || window > rewards.len() {
            return Vec::new();
        }
        (0..=rewards.len() - window)
            .map(|i| rewards[i..i + window].iter().sum())
            .collect()
    }

    /// CONCEPT:EG-KG.compute.discounted-return — the trajectory in `traj_ids` with the HIGHEST discounted return
    /// (for prioritized replay / policy selection). Ties are broken deterministically by
    /// the smaller id. `None` for an empty input. Absent trajectories score `0.0`.
    pub fn best_trajectory(&self, traj_ids: &[String], gamma: f64) -> Option<String> {
        traj_ids
            .iter()
            .map(|id| (id.clone(), self.discounted_return(id, gamma)))
            .min_by(|a, b| {
                // Max return, then min id — negate the return comparison for `min_by`.
                b.1.partial_cmp(&a.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0))
            })
            .map(|(id, _)| id)
    }

    /// CONCEPT:EG-KG.compute.discounted-return — the trajectory in `traj_ids` with the LOWEST discounted return
    /// (for hard-negative mining / avoidance). Ties are broken deterministically by the
    /// smaller id. `None` for an empty input. Absent trajectories score `0.0`.
    pub fn worst_trajectory(&self, traj_ids: &[String], gamma: f64) -> Option<String> {
        traj_ids
            .iter()
            .map(|id| (id.clone(), self.discounted_return(id, gamma)))
            .min_by(|a, b| {
                a.1.partial_cmp(&b.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0))
            })
            .map(|(id, _)| id)
    }
}

// ── Free Functions (non-method helpers) ──────────────────────────────────

/// Apply the Ebbinghaus retention curve to a single property map in place.
///
/// Reads `confidence` (default 1.0), `last_access` (→ `updated_at` → `created_at`
/// → `now`) and an optional per-item `half_life`. Writes the decayed
/// `confidence` and advances `last_access` to `now`. Returns `(new_confidence,
/// changed)`; `changed` is false when no time elapsed (fresh item) so callers can
/// skip a re-encode. `last_access` is always stamped so the next sweep has an
/// anchor.
fn apply_decay(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    now: u64,
    default_half_life: f64,
) -> (f64, bool) {
    let confidence = obj
        .get("confidence")
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0);
    let last_access = obj
        .get("last_access")
        .and_then(|v| v.as_u64())
        .or_else(|| obj.get("updated_at").and_then(|v| v.as_u64()))
        .or_else(|| obj.get("created_at").and_then(|v| v.as_u64()))
        .unwrap_or(now);
    let half_life = obj
        .get("half_life")
        .and_then(|v| v.as_f64())
        .filter(|h| *h > 0.0)
        .unwrap_or(default_half_life);

    if now <= last_access || half_life <= 0.0 {
        // Nothing to decay yet; ensure there is an anchor for the next sweep.
        obj.insert("last_access".to_string(), serde_json::json!(now));
        return (confidence, false);
    }

    let dt = (now - last_access) as f64;
    let retention = 0.5_f64.powf(dt / half_life);
    let new_conf = (confidence * retention).clamp(0.0, 1.0);
    obj.insert("confidence".to_string(), serde_json::json!(new_conf));
    obj.insert("last_access".to_string(), serde_json::json!(now));
    (new_conf, true)
}

/// Conservative default cap on the number of matches [`vf2_match_views`] collects
/// before stopping the backtracking search early. VF2 subgraph isomorphism is
/// NP-hard with no bound otherwise; a caller wanting more must pass an explicit
/// `max_results` on the request.
pub const DEFAULT_VF2_MAX_RESULTS: usize = 1_000;

/// Conservative default cap on the number of candidate-pair attempts (one per
/// `backtrack_match` inner-loop iteration) [`vf2_match_views`] spends before
/// stopping early, regardless of how many matches it has already found. Bounds
/// worst-case CPU on a pathological pattern/host pair.
pub const DEFAULT_VF2_MAX_STEPS: usize = 2_000_000;

/// Collected matches plus search-budget accounting threaded through
/// [`backtrack_match`] (bundled into one struct rather than 2 more positional
/// params, keeping the function at 7 arguments). `truncated` is checked at the
/// top of every recursive call, so a tripped budget unwinds the whole call tree
/// promptly instead of finishing the in-flight branch.
struct Vf2Search {
    matches: Vec<HashMap<String, String>>,
    max_results: usize,
    max_steps: usize,
    steps: usize,
    truncated: bool,
}

/// VF2 subgraph match of `pattern` against an already-materialized `host`
/// `GraphView` (vs [`GraphCore::vf2_subgraph_match`], which snapshots its own
/// live graph first). Lets an off-lock caller — e.g. the Cypher exec
/// (CONCEPT:EG-KG.query.dep-free-behind), which already holds the `analysis_snapshot()` view — reuse
/// the exact same matcher without re-snapshotting. Each result maps a pattern
/// node id → the host node id it bound to.
///
/// `max_results`/`max_steps` bound the otherwise-unbounded backtracking search
/// (`0` ⇒ [`DEFAULT_VF2_MAX_RESULTS`]/[`DEFAULT_VF2_MAX_STEPS`]); the returned
/// `bool` is `true` when the search stopped early against either budget rather
/// than exhausting the search space (a PARTIAL result, not proof no further
/// match exists).
pub fn vf2_match_views(
    host: &GraphView,
    pattern: &GraphView,
    max_results: usize,
    max_steps: usize,
) -> (Vec<HashMap<String, String>>, bool) {
    let pattern_nodes: Vec<String> = pattern.node_map.keys().cloned().collect();
    if pattern_nodes.is_empty() {
        return (Vec::new(), false);
    }
    let mut current_mapping = HashMap::new();
    let mut mapped_targets = std::collections::HashSet::new();
    let mut search = Vf2Search {
        matches: Vec::new(),
        max_results: if max_results == 0 {
            DEFAULT_VF2_MAX_RESULTS
        } else {
            max_results
        },
        max_steps: if max_steps == 0 {
            DEFAULT_VF2_MAX_STEPS
        } else {
            max_steps
        },
        steps: 0,
        truncated: false,
    };
    backtrack_match(
        host,
        0,
        &pattern_nodes,
        &mut current_mapping,
        &mut mapped_targets,
        pattern,
        &mut search,
    );
    (search.matches, search.truncated)
}

fn backtrack_match(
    host: &GraphView,
    pattern_node_idx: usize,
    pattern_nodes: &[String],
    current_mapping: &mut HashMap<String, String>,
    mapped_targets: &mut std::collections::HashSet<String>,
    pattern: &GraphView,
    search: &mut Vf2Search,
) {
    if search.truncated {
        return;
    }
    if pattern_node_idx == pattern_nodes.len() {
        search.matches.push(current_mapping.clone());
        if search.matches.len() >= search.max_results {
            search.truncated = true;
        }
        return;
    }

    let p_node = &pattern_nodes[pattern_node_idx];

    for t_node in host.node_map.keys() {
        if search.truncated {
            return;
        }
        search.steps += 1;
        if search.steps > search.max_steps {
            search.truncated = true;
            return;
        }
        if mapped_targets.contains(t_node) {
            continue;
        }

        if check_match(host, p_node, t_node, current_mapping, pattern) {
            current_mapping.insert(p_node.clone(), t_node.clone());
            mapped_targets.insert(t_node.clone());

            backtrack_match(
                host,
                pattern_node_idx + 1,
                pattern_nodes,
                current_mapping,
                mapped_targets,
                pattern,
                search,
            );

            current_mapping.remove(p_node);
            mapped_targets.remove(t_node);

            if search.truncated {
                return;
            }
        }
    }
}

fn check_match(
    host: &GraphView,
    p_node: &str,
    t_node: &str,
    current_mapping: &HashMap<String, String>,
    pattern: &GraphView,
) -> bool {
    let p_props = pattern
        .node_properties
        .get(p_node)
        .map(|s| s.as_slice())
        .unwrap_or(b"{}");
    let t_props = host
        .node_properties
        .get(t_node)
        .map(|s| s.as_slice())
        .unwrap_or(b"{}");

    if !match_props(p_props, t_props) {
        return false;
    }

    let p_idx = match pattern.node_map.get(p_node) {
        Some(&idx) => idx,
        None => return false,
    };

    // In-edges
    for in_edge in pattern
        .graph
        .edges_directed(p_idx, petgraph::Direction::Incoming)
    {
        let p_src = &pattern.graph[in_edge.source()];
        if let Some(t_src) = current_mapping.get(p_src) {
            if !host.has_edge(t_src, t_node) {
                return false;
            }
            if !check_edge_props(host, pattern, p_src, p_node, t_src, t_node) {
                return false;
            }
        }
    }

    // Out-edges
    for out_edge in pattern
        .graph
        .edges_directed(p_idx, petgraph::Direction::Outgoing)
    {
        let p_tgt = &pattern.graph[out_edge.target()];
        if let Some(t_tgt) = current_mapping.get(p_tgt) {
            if !host.has_edge(t_node, t_tgt) {
                return false;
            }
            if !check_edge_props(host, pattern, p_node, &p_tgt.clone(), t_node, t_tgt) {
                return false;
            }
        }
    }

    true
}

fn check_edge_props(
    host: &GraphView,
    pattern: &GraphView,
    p_src: &str,
    p_tgt: &str,
    t_src: &str,
    t_tgt: &str,
) -> bool {
    if let Some(p_props_list) = pattern
        .edge_properties
        .get(&(p_src.to_string(), p_tgt.to_string()))
    {
        if let Some(t_props_list) = host
            .edge_properties
            .get(&(t_src.to_string(), t_tgt.to_string()))
        {
            for p_edge_props in p_props_list {
                let mut matched = false;
                for t_edge_props in t_props_list {
                    if match_props(p_edge_props, t_edge_props) {
                        matched = true;
                        break;
                    }
                }
                if !matched {
                    return false;
                }
            }
        } else {
            return false;
        }
    }
    true
}

pub fn match_props(p_msgpack: &[u8], t_msgpack: &[u8]) -> bool {
    let p_val: serde_json::Value = match decode_property_value(p_msgpack) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let t_val: serde_json::Value = match decode_property_value(t_msgpack) {
        Ok(v) => v,
        Err(_) => return false,
    };

    if let (Some(p_obj), Some(t_obj)) = (p_val.as_object(), t_val.as_object()) {
        for (k, v) in p_obj {
            if let Some(t_v) = t_obj.get(k) {
                if v != t_v {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    } else {
        p_val == t_val
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn props(map: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&map).unwrap()
    }

    // ── read-your-own-writes overlay (CONCEPT:EG-KG.compute.kg-transaction-is-pinned) ────────────────────────

    fn overlay_obj(map: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        match map {
            serde_json::Value::Object(o) => o,
            _ => panic!("expected object"),
        }
    }

    #[test]
    fn overlay_add_is_visible_in_view() {
        let core = GraphCore::new();
        core.add_node("n1".into(), props(serde_json::json!({"rank": 1})));
        let mut view = core.analysis_snapshot();
        assert!(!view.has_node("n9"));
        view.overlay_add_node("n9".into(), props(serde_json::json!({"rank": 9})));
        // The overlay sees the buffered add; the live core does NOT.
        assert!(view.has_node("n9"));
        assert_eq!(
            view.node_row_object("n9").unwrap().get("rank"),
            Some(&serde_json::json!(9))
        );
        assert!(!core.has_node("n9"), "live core untouched by overlay");
    }

    // ── Distribution-valued properties (CONCEPT:EG-KG.compute.uncertainty-values) ──────────────────

    #[test]
    fn distribution_property_roundtrips() {
        let core = GraphCore::new();
        core.add_node(
            "m1".into(),
            props(serde_json::json!({"type": "Measurement"})),
        );
        let dist = eg_types::Distribution::Gaussian {
            mean: 3.5,
            std: 0.75,
        };
        assert!(core.set_distribution("m1", "reading", &dist));
        let back = core.get_distribution("m1", "reading").expect("stored dist");
        assert_eq!(back, dist);
        // Set merges — the pre-existing `type` key survives.
        let blob = core.get_node_properties("m1").unwrap();
        let obj = decode_property_value(&blob).unwrap();
        assert_eq!(
            obj.get("type"),
            Some(&serde_json::json!("Measurement")),
            "existing properties must be preserved on set_distribution"
        );
    }

    #[test]
    fn distribution_property_missing_and_bad_return_none() {
        let core = GraphCore::new();
        // Absent node.
        assert!(core.get_distribution("ghost", "reading").is_none());
        // Present node, absent key.
        core.add_node("m2".into(), props(serde_json::json!({"x": 1})));
        assert!(core.get_distribution("m2", "reading").is_none());
        // set on a not-yet-existing node creates it.
        let d = eg_types::Distribution::Beta {
            alpha: 2.0,
            beta: 5.0,
        };
        assert!(core.set_distribution("m3", "belief", &d));
        assert_eq!(core.get_distribution("m3", "belief"), Some(d));
    }

    #[test]
    fn overlay_add_edge_is_bfs_reachable_in_view() {
        use petgraph::Direction::Outgoing;
        let core = GraphCore::new();
        core.add_node("a".into(), props(serde_json::json!({"rank": 1})));
        core.add_node("b".into(), props(serde_json::json!({"rank": 2})));
        let mut view = core.analysis_snapshot();
        // No edge yet: `b` is not an outgoing neighbor of `a`.
        let a_idx = *view.node_map.get("a").unwrap();
        let neighbor_ids = |v: &GraphView, idx: NodeIndex| -> Vec<String> {
            v.graph
                .edges_directed(idx, Outgoing)
                .map(|e| v.graph[e.target()].clone())
                .collect()
        };
        assert!(!neighbor_ids(&view, a_idx).contains(&"b".to_string()));
        // Stage an edge a→b in the overlay only.
        assert!(view.overlay_add_edge(
            "a".into(),
            "b".into(),
            props(serde_json::json!({"rel": "knows"})),
        ));
        // The staged edge is now BFS-reachable: b is an outgoing neighbor of a.
        assert!(neighbor_ids(&view, a_idx).contains(&"b".to_string()));
        // Edge properties recorded on the overlaid view.
        assert!(view
            .edge_properties
            .contains_key(&("a".to_string(), "b".to_string())));
        // The live core is untouched (no committed edge).
        assert!(core.get_edge_properties("a", "b").is_empty());
        // An edge to a missing endpoint is dropped and reports false.
        assert!(!view.overlay_add_edge("a".into(), "ghost".into(), props(serde_json::json!({}))));
        // Removing the staged edge hides it again.
        view.overlay_remove_edge("a", "b");
        assert!(!neighbor_ids(&view, a_idx).contains(&"b".to_string()));
        assert!(!view
            .edge_properties
            .contains_key(&("a".to_string(), "b".to_string())));
    }

    #[test]
    fn overlay_remove_hides_node_in_view() {
        let core = GraphCore::new();
        core.add_node("n1".into(), props(serde_json::json!({"rank": 1})));
        let mut view = core.analysis_snapshot();
        view.overlay_remove_node("n1");
        assert!(!view.has_node("n1"));
        assert!(view.node_row_object("n1").is_none());
        assert!(core.has_node("n1"), "live core untouched by overlay");
    }

    #[test]
    fn overlay_cas_merges_when_condition_holds() {
        let core = GraphCore::new();
        core.add_node(
            "n1".into(),
            props(serde_json::json!({"rank": 1, "state": "open"})),
        );
        let mut view = core.analysis_snapshot();
        // Condition holds → merge applied in the view.
        assert!(view.overlay_compare_and_set_fields(
            "n1",
            &overlay_obj(serde_json::json!({"state": "open"})),
            &overlay_obj(serde_json::json!({"rank": 2})),
        ));
        assert_eq!(
            view.node_row_object("n1").unwrap().get("rank"),
            Some(&serde_json::json!(2))
        );
        // Condition fails → no-op.
        assert!(!view.overlay_compare_and_set_fields(
            "n1",
            &overlay_obj(serde_json::json!({"state": "closed"})),
            &overlay_obj(serde_json::json!({"rank": 3})),
        ));
        assert_eq!(
            view.node_row_object("n1").unwrap().get("rank"),
            Some(&serde_json::json!(2)),
            "failed CAS left the value unchanged"
        );
        // A CAS on an absent node is a no-op returning false.
        assert!(!view.overlay_compare_and_set_fields(
            "nope",
            &serde_json::Map::new(),
            &overlay_obj(serde_json::json!({"x": 1})),
        ));
    }

    // ── secondary property index (CONCEPT:EG-KG.query.concept-12) ──────────────────────────

    /// Serializes the env-mutating property-index tests (env is process-global and
    /// Rust runs tests on parallel threads). Shared crate-wide (`crate::PROP_ENV_LOCK`)
    /// so the `index` manager cap test serializes against these too.
    use crate::PROP_ENV_LOCK;

    fn prop_graph() -> GraphCore {
        let core = GraphCore::new();
        for (id, val) in [
            (
                "n1",
                serde_json::json!({"type": "Agent", "team": "blue", "rank": 1}),
            ),
            (
                "n2",
                serde_json::json!({"type": "Agent", "team": "red", "rank": 2}),
            ),
            (
                "n3",
                serde_json::json!({"type": "Tool", "team": "blue", "rank": 3}),
            ),
            (
                "n4",
                serde_json::json!({"type": "Tool", "team": "blue", "rank": 3}),
            ),
        ] {
            core.add_node(id.into(), props(val));
        }
        core
    }

    // ── non-destructive edge invalidation / supersession (CONCEPT:AU-KG.ingest.list-durable-media) ──

    #[test]
    fn invalidate_edge_closes_windows_without_deleting() {
        let core = GraphCore::new();
        core.add_node("a".into(), props(serde_json::json!({"type": "E"})));
        core.add_node("b".into(), props(serde_json::json!({"type": "E"})));
        core.add_edge(
            "a".into(),
            "b".into(),
            props(serde_json::json!({"relationship": "LIKES", "valid_from": 100, "tx_from": 100})),
        )
        .unwrap();

        let n = core.invalidate_edge("a", "b", "LIKES", 200, 250);
        assert_eq!(n, 1);

        // The edge still EXISTS (non-destructive) with closed windows.
        let blobs = core.get_edge_properties("a", "b");
        assert_eq!(blobs.len(), 1);
        let v: serde_json::Value = rmp_serde::from_slice(&blobs[0]).unwrap();
        assert_eq!(v.get("valid_until").and_then(|x| x.as_u64()), Some(200));
        assert_eq!(v.get("tx_to").and_then(|x| x.as_u64()), Some(250));

        // Idempotent: re-invalidating at/after the close instant is a no-op.
        assert_eq!(core.invalidate_edge("a", "b", "LIKES", 200, 300), 0);
        // A different relationship between the same pair is untouched.
        assert_eq!(core.invalidate_edge("a", "b", "HATES", 200, 250), 0);
    }

    #[test]
    fn supersede_edge_is_atomic_close_plus_insert() {
        let core = GraphCore::new();
        core.add_node("a".into(), props(serde_json::json!({"type": "E"})));
        core.add_node("b".into(), props(serde_json::json!({"type": "E"})));
        core.add_edge(
            "a".into(),
            "b".into(),
            props(serde_json::json!({"relationship": "LIKES", "valid_from": 100, "tx_from": 100})),
        )
        .unwrap();

        core.supersede_edge(
            "a".into(),
            "b".into(),
            props(serde_json::json!({
                "relationship": "DISLIKES", "valid_from": 200, "tx_from": 200,
                "supersedes": "a:b:LIKES"
            })),
            "a",
            "b",
            "LIKES",
            200,
            200,
        )
        .unwrap();

        // Both edges coexist: the prior LIKES (closed at 200) + the new DISLIKES.
        let blobs = core.get_edge_properties("a", "b");
        assert_eq!(blobs.len(), 2);
        let rels: Vec<(String, Option<u64>)> = blobs
            .iter()
            .map(|b| {
                let v: serde_json::Value = rmp_serde::from_slice(b).unwrap();
                (
                    v.get("relationship")
                        .and_then(|x| x.as_str())
                        .unwrap()
                        .to_string(),
                    v.get("valid_until").and_then(|x| x.as_u64()),
                )
            })
            .collect();
        assert!(rels.contains(&("LIKES".to_string(), Some(200))));
        assert!(rels.contains(&("DISLIKES".to_string(), None)));
    }

    #[test]
    fn property_index_returns_correct_ids() {
        // Hold the env lock + pin the cap to default: sibling tests mutate
        // `EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES` globally, which would otherwise
        // race this default-cap test under parallel execution.
        let _g = PROP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES");
        std::env::remove_var("EPISTEMIC_GRAPH_INDEXED_PROPERTIES");
        let core = prop_graph();
        // Demand-driven: indexing `team` on first call. Equality lookup.
        let mut blue = core.nodes_by_property("team", "blue").unwrap();
        blue.sort();
        assert_eq!(blue, vec!["n1", "n3", "n4"]);
        assert_eq!(core.nodes_by_property("team", "red").unwrap(), vec!["n2"]);
        // Indexed key, no matching value -> empty (Some, not None).
        assert_eq!(
            core.nodes_by_property("team", "green").unwrap(),
            Vec::<String>::new()
        );
        // Numeric value indexes under its canonical string form.
        let mut r3 = core.nodes_by_property("rank", "3").unwrap();
        r3.sort();
        assert_eq!(r3, vec!["n3", "n4"]);
    }

    #[test]
    fn property_index_invalidates_after_mutation() {
        let _g = PROP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES");
        std::env::remove_var("EPISTEMIC_GRAPH_INDEXED_PROPERTIES");
        let core = prop_graph();
        assert_eq!(core.nodes_by_property("team", "red").unwrap(), vec!["n2"]);
        let v0 = core.version();
        // Move n1 from blue -> red. The dispatch layer calls mark_dirty after a
        // write (mirrors the label-index test); that bumps version + drops the index.
        core.add_node(
            "n1".into(),
            props(serde_json::json!({"type": "Agent", "team": "red"})),
        );
        core.mark_dirty();
        assert_ne!(core.version(), v0, "write must bump version");
        let mut red = core.nodes_by_property("team", "red").unwrap();
        red.sort();
        assert_eq!(
            red,
            vec!["n1", "n2"],
            "rebuilt index must reflect the write"
        );
        assert_eq!(
            core.nodes_by_property("team", "blue").unwrap(),
            vec!["n3", "n4"]
        );
    }

    // ── inverted JSONPath path-index (CONCEPT:EG-KG.compute.json-deep-indexing) ─────────────────────────

    /// Build a graph of deep JSON documents for the path-index tests.
    fn json_graph() -> GraphCore {
        let core = GraphCore::new();
        core.add_node(
            "n1".into(),
            props(serde_json::json!({
                "type": "Doc", "meta": {"lang": "rust", "year": 2024},
                "tags": ["a", "b"]
            })),
        );
        core.add_node(
            "n2".into(),
            props(serde_json::json!({
                "type": "Doc", "meta": {"lang": "go", "year": 2024},
                "tags": ["b", "c"]
            })),
        );
        core.add_node(
            "n3".into(),
            props(serde_json::json!({
                "type": "Doc", "meta": {"lang": "rust", "year": 2025}
            })),
        );
        core
    }

    #[test]
    fn eg084_path_index_deep_equality_and_existence() {
        let _g = PROP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS");
        std::env::remove_var("EPISTEMIC_GRAPH_INDEXED_JSON_PATHS");
        let core = json_graph();
        // Deep `->>`-style equality via the index (demand-driven build of `$.meta.lang`).
        let mut rust = core.nodes_by_json_path("$.meta.lang", "rust").unwrap();
        rust.sort();
        assert_eq!(rust, vec!["n1", "n3"]);
        assert_eq!(
            core.nodes_by_json_path("$.meta.lang", "go").unwrap(),
            vec!["n2"]
        );
        // Numeric leaf indexes under its canonical string form.
        let mut y24 = core.nodes_by_json_path("$.meta.year", "2024").unwrap();
        y24.sort();
        assert_eq!(y24, vec!["n1", "n2"]);
        // Existence: n1/n2 have `tags`, n3 does not.
        let mut has_tags = core.nodes_with_json_path("$.tags").unwrap();
        has_tags.sort();
        assert_eq!(has_tags, vec!["n1", "n2"]);
        // Wildcard existence over array elements.
        let mut any_tag = core.nodes_with_json_path("$.tags[*]").unwrap();
        any_tag.sort();
        assert_eq!(any_tag, vec!["n1", "n2"]);
    }

    #[test]
    fn eg084_path_index_maintained_on_add_cas_remove() {
        let _g = PROP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS");
        std::env::remove_var("EPISTEMIC_GRAPH_INDEXED_JSON_PATHS");
        let core = json_graph();
        assert_eq!(
            core.nodes_by_json_path("$.meta.lang", "go").unwrap(),
            vec!["n2"]
        );

        // ADD: a fresh node lands in the rebuilt index.
        let v0 = core.version();
        core.add_node(
            "n4".into(),
            props(serde_json::json!({"type": "Doc", "meta": {"lang": "go"}})),
        );
        core.mark_dirty();
        assert_ne!(core.version(), v0, "add must bump version");
        let mut go = core.nodes_by_json_path("$.meta.lang", "go").unwrap();
        go.sort();
        assert_eq!(go, vec!["n2", "n4"], "index reflects the add");

        // CAS (property change): rewrite n2's nested lang go -> rust (upsert).
        core.add_node(
            "n2".into(),
            props(serde_json::json!({"type": "Doc", "meta": {"lang": "rust"}})),
        );
        core.mark_dirty();
        assert_eq!(
            core.nodes_by_json_path("$.meta.lang", "go").unwrap(),
            vec!["n4"],
            "index reflects the nested-value CAS"
        );
        let mut rust = core.nodes_by_json_path("$.meta.lang", "rust").unwrap();
        rust.sort();
        assert_eq!(rust, vec!["n1", "n2", "n3"]);

        // REMOVE: n4 gone from the index.
        core.remove_node("n4".into());
        core.mark_dirty();
        assert_eq!(
            core.nodes_by_json_path("$.meta.lang", "go").unwrap(),
            Vec::<String>::new(),
            "index reflects the remove"
        );
    }

    #[test]
    fn eg084_path_index_containment_selectivity() {
        let _g = PROP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS");
        std::env::remove_var("EPISTEMIC_GRAPH_INDEXED_JSON_PATHS");
        let core = json_graph();
        // `props @> '{"meta":{"lang":"rust"}}'`: the existence set at `$.meta.lang`
        // (index-accelerated candidates) narrows to n1/n3, then per-row containment
        // confirms — here every candidate qualifies.
        let candidates = core.nodes_with_json_path("$.meta.lang").unwrap();
        let mut kept: Vec<String> = candidates
            .into_iter()
            .filter(|id| {
                let blob = core.get_node_properties(id).unwrap();
                let v: serde_json::Value = rmp_serde::from_slice(&blob).unwrap();
                crate::jsonpath::path_contains(
                    &v,
                    "$",
                    &serde_json::json!({"meta": {"lang": "rust"}}),
                )
            })
            .collect();
        kept.sort();
        assert_eq!(kept, vec!["n1", "n3"]);
    }

    #[test]
    fn eg084_path_index_bounded_cap_falls_back() {
        let _g = PROP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS", "1");
        std::env::remove_var("EPISTEMIC_GRAPH_INDEXED_JSON_PATHS");
        let core = json_graph();
        // First path indexes fine.
        assert!(core.nodes_by_json_path("$.meta.lang", "rust").is_some());
        // Second distinct path exceeds the cap -> None (caller full-scans).
        assert!(core.nodes_by_json_path("$.meta.year", "2024").is_none());
        std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS");
    }

    // ── durable JSONPath index persistence (CONCEPT:EG-KG.storage.path-index-store) ───────────────────

    /// CONCEPT:EG-KG.storage.path-index-store — a demand-driven build is written through to the store, and a
    /// FRESH graph rehydrates it at boot and serves the JSON filter WITHOUT any node
    /// data (proving the answer came from the persisted index, not a rescan). Sharing
    /// one `InMemoryPathIndexStore` `Arc` across the two cores simulates a save→reopen.
    #[test]
    fn eg308_path_index_rehydrates_from_store_without_rescan() {
        let _g = PROP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS");
        std::env::remove_var("EPISTEMIC_GRAPH_INDEXED_JSON_PATHS");
        let store: Arc<dyn crate::path_persist::PathIndexPersistence> =
            Arc::new(crate::path_persist::InMemoryPathIndexStore::new());

        // Core 1: build the index over real nodes -> write-through persists it.
        let core1 = json_graph();
        core1.set_path_index_store(store.clone());
        let mut rust = core1.nodes_by_json_path("$.meta.lang", "rust").unwrap();
        rust.sort();
        assert_eq!(rust, vec!["n1", "n3"]);
        let _ = core1.nodes_with_json_path("$.tags").unwrap();
        // The store now holds the built snapshot.
        let snap = store.load().expect("build wrote the index through");
        assert!(snap.by_value.contains_key("$.meta.lang"));
        assert!(snap.present.contains_key("$.tags"));

        // Core 2: a FRESH, EMPTY graph (no nodes at all) attached to the SAME store.
        let core2 = GraphCore::new();
        core2.set_path_index_store(store.clone());
        assert_eq!(core2.node_count(), 0, "core2 has no node data");
        let adopted = core2.rehydrate_path_index();
        assert!(adopted >= 1, "rehydrate adopts the persisted paths");
        // Served purely from the rehydrated index — a rescan of core2's (empty) node
        // store could NOT produce these ids.
        let mut rust2 = core2.nodes_by_json_path("$.meta.lang", "rust").unwrap();
        rust2.sort();
        assert_eq!(
            rust2,
            vec!["n1", "n3"],
            "rehydrated index answers the filter"
        );
        let mut tags2 = core2.nodes_with_json_path("$.tags").unwrap();
        tags2.sort();
        assert_eq!(tags2, vec!["n1", "n2"]);
    }

    /// CONCEPT:EG-KG.storage.path-index-store — after a mutation the in-memory index is invalidated and rebuilt
    /// on the next query, and that rebuild RE-PERSISTS, so the durable snapshot tracks
    /// the new graph state (never a stale view across a write).
    #[test]
    fn eg308_path_index_persist_stays_consistent_after_mutation() {
        let _g = PROP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS");
        std::env::remove_var("EPISTEMIC_GRAPH_INDEXED_JSON_PATHS");
        let store: Arc<dyn crate::path_persist::PathIndexPersistence> =
            Arc::new(crate::path_persist::InMemoryPathIndexStore::new());
        let core = json_graph();
        core.set_path_index_store(store.clone());

        assert_eq!(
            core.nodes_by_json_path("$.meta.lang", "rust").unwrap(),
            vec!["n1", "n3"]
        );
        let v0 = core.version();
        let snap0 = store.load().unwrap();
        assert_eq!(
            snap0.stamp, v0,
            "snapshot is stamped with the build version"
        );

        // Mutate: add n5 as rust. mark_dirty drops the in-memory index (and the
        // persisted copy is now stale until the next rebuild).
        core.add_node(
            "n5".into(),
            props(serde_json::json!({"type": "Doc", "meta": {"lang": "rust"}})),
        );
        core.mark_dirty();
        assert_ne!(core.version(), v0, "write bumps version");

        // The next query rebuilds AND re-persists at the new version.
        let mut rust = core.nodes_by_json_path("$.meta.lang", "rust").unwrap();
        rust.sort();
        assert_eq!(
            rust,
            vec!["n1", "n3", "n5"],
            "rebuilt index reflects the add"
        );
        let snap1 = store.load().unwrap();
        assert_eq!(
            snap1.by_value["$.meta.lang"]["rust"],
            vec!["n1".to_string(), "n3".to_string(), "n5".to_string()],
            "persisted snapshot tracks the mutation"
        );
        assert_eq!(snap1.stamp, core.version(), "re-persist restamps");
    }

    /// CONCEPT:EG-KG.storage.path-index-store — the inverted-index id counts feed the planner cost `Stats`
    /// selectivity: `json_path_selectivity` returns |matching ids| / |nodes| for an
    /// equality (`Some`) or existence (`None`) filter, and `None` when unindexable.
    #[test]
    fn eg308_json_path_selectivity_from_index_counts() {
        let _g = PROP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS");
        std::env::remove_var("EPISTEMIC_GRAPH_INDEXED_JSON_PATHS");
        let core = json_graph(); // 3 nodes: n1/n3 rust, n2 go; n1/n2 have tags
                                 // Equality: 2 of 3 nodes match `$.meta.lang = rust`.
        let sel_rust = core
            .json_path_selectivity("$.meta.lang", Some("rust"))
            .unwrap();
        assert!((sel_rust - 2.0 / 3.0).abs() < 1e-9, "got {sel_rust}");
        // Equality: 1 of 3 match `go`.
        let sel_go = core
            .json_path_selectivity("$.meta.lang", Some("go"))
            .unwrap();
        assert!((sel_go - 1.0 / 3.0).abs() < 1e-9, "got {sel_go}");
        // Existence: 2 of 3 have `$.tags`.
        let sel_tags = core.json_path_selectivity("$.tags", None).unwrap();
        assert!((sel_tags - 2.0 / 3.0).abs() < 1e-9, "got {sel_tags}");
        // A more selective filter yields a smaller fraction than a broad one — the
        // exact signal the cost model's filter-first/vector-first choice keys on.
        assert!(sel_go < sel_rust);

        // Unindexable under a cap-of-1: the second distinct path returns None so the
        // planner falls back to its default estimate (same bound as the count methods).
        std::env::set_var("EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS", "1");
        let capped = json_graph();
        assert!(capped
            .json_path_selectivity("$.meta.lang", Some("rust"))
            .is_some());
        assert!(capped.json_path_selectivity("$.meta.year", None).is_none());
        std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS");
    }

    /// CONCEPT:EG-KG.storage.path-index-store — with NO store attached (the default), the path-index stays
    /// fully in-memory: `rehydrate_path_index` is a 0-op and queries behave exactly as
    /// the pre-EG-308 EG-084 path.
    #[test]
    fn eg308_no_store_leaves_path_index_in_memory_unchanged() {
        let _g = PROP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS");
        std::env::remove_var("EPISTEMIC_GRAPH_INDEXED_JSON_PATHS");
        let core = json_graph();
        assert_eq!(
            core.rehydrate_path_index(),
            0,
            "no store -> nothing to adopt"
        );
        let mut rust = core.nodes_by_json_path("$.meta.lang", "rust").unwrap();
        rust.sort();
        assert_eq!(rust, vec!["n1", "n3"], "unchanged EG-084 behavior");
    }

    /// CONCEPT:EG-KG.compute.cdc-event-emit — a committed write emits a `ChangeEvent` carrying the bumped
    /// version to a registered [`ChangeSink`]; with no subscriber the write path is a
    /// no-op fan-out (single atomic load), and dropping the subscriber's `Arc`
    /// unsubscribes.
    #[test]
    fn change_notifier_emits_on_write() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let core = GraphCore::new();
        core.changes().set_graph("g1");
        // No subscribers yet: emit is a no-op, and the write path stays quiet.
        assert!(!core.changes().has_subscribers());
        core.add_node("n0".into(), props(serde_json::json!({"type": "T"})));
        core.mark_dirty();

        // Register a sink that records the (graph, version) of each event.
        struct Rec {
            last_version: AtomicU64,
            hits: AtomicU64,
            graph: parking_lot::Mutex<String>,
        }
        impl ChangeSink for Rec {
            fn on_change(&self, event: &ChangeEvent) {
                self.last_version.store(event.version, Ordering::SeqCst);
                self.hits.fetch_add(1, Ordering::SeqCst);
                *self.graph.lock() = event.graph.clone();
            }
        }
        let rec = Arc::new(Rec {
            last_version: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            graph: parking_lot::Mutex::new(String::new()),
        });
        let sink: Arc<dyn ChangeSink> = rec.clone();
        core.changes().subscribe(&sink);
        assert!(core.changes().has_subscribers());

        core.add_node("n1".into(), props(serde_json::json!({"type": "T"})));
        core.mark_dirty();
        assert_eq!(rec.hits.load(Ordering::SeqCst), 1, "one write, one event");
        assert_eq!(
            rec.last_version.load(Ordering::SeqCst),
            core.version(),
            "the event carries the post-write OCC version"
        );
        assert_eq!(*rec.graph.lock(), "g1", "the event names the graph");

        // Dropping the subscriber's Arc unsubscribes: the next emit prunes the dead
        // Weak and the write path returns to the no-subscriber (no-op) state.
        drop(sink);
        drop(rec);
        core.mark_dirty();
        assert!(
            !core.changes().has_subscribers(),
            "dropping the sink Arc unsubscribes"
        );
    }

    #[test]
    fn change_notifier_callbacks_run_outside_the_subscriber_mutex() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        struct Counter(AtomicU64);
        impl ChangeSink for Counter {
            fn on_change(&self, _event: &ChangeEvent) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct SubscribeFromCallback {
            notifier: Arc<ChangeNotifier>,
            next: Arc<dyn ChangeSink>,
        }
        impl ChangeSink for SubscribeFromCallback {
            fn on_change(&self, _event: &ChangeEvent) {
                self.notifier.subscribe(&self.next);
            }
        }

        let notifier = Arc::new(ChangeNotifier::default());
        let counter = Arc::new(Counter(AtomicU64::new(0)));
        let next: Arc<dyn ChangeSink> = counter.clone();
        let reentrant: Arc<dyn ChangeSink> = Arc::new(SubscribeFromCallback {
            notifier: notifier.clone(),
            next: next.clone(),
        });
        notifier.subscribe(&reentrant);

        // This would deadlock if `emit` held `sinks` while invoking callbacks.
        notifier.emit(1);
        assert_eq!(counter.0.load(Ordering::SeqCst), 0);
        notifier.emit(2);
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn property_index_composite_lookup() {
        let _g = PROP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES");
        std::env::remove_var("EPISTEMIC_GRAPH_INDEXED_PROPERTIES");
        let core = prop_graph();
        // team=blue AND type=Tool -> n3, n4 (n1 is blue but Agent).
        let mut got = core
            .nodes_by_properties(&[("team", "blue"), ("type", "Tool")])
            .unwrap();
        got.sort();
        assert_eq!(got, vec!["n3", "n4"]);
        // No node is both red and Tool.
        assert_eq!(
            core.nodes_by_properties(&[("team", "red"), ("type", "Tool")])
                .unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn property_index_bounded_cap_falls_back() {
        let _g = PROP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES", "1");
        std::env::remove_var("EPISTEMIC_GRAPH_INDEXED_PROPERTIES");
        let core = prop_graph();
        // First key indexes fine.
        assert!(core.nodes_by_property("team", "blue").is_some());
        // Second distinct key hits the cap (1) -> None, caller must full-scan.
        assert!(
            core.nodes_by_property("type", "Tool").is_none(),
            "cap=1 must refuse a second key"
        );
        std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES");
    }

    #[test]
    fn property_index_seed_opt_in() {
        let _g = PROP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("EPISTEMIC_GRAPH_INDEXED_PROPERTIES", " team , type ");
        std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES");
        let core = prop_graph();
        // A query on a pre-seeded key works (seeded on first build).
        let mut blue = core.nodes_by_property("team", "blue").unwrap();
        blue.sort();
        assert_eq!(blue, vec!["n1", "n3", "n4"]);
        assert_eq!(core.nodes_by_property("type", "Agent").unwrap().len(), 2);
        std::env::remove_var("EPISTEMIC_GRAPH_INDEXED_PROPERTIES");
    }

    /// CONCEPT:EG-KG.storage.read-through-seam-exercised — the read-through seam, exercised purely in eg-core with a
    /// stub backing store (no facade/redb needed): after a node is dropped from RAM
    /// (eviction), `get_node_properties` serves it from the attached read-through;
    /// without a read-through the same miss is a genuine absence (default model).
    #[test]
    fn read_through_serves_evicted_node() {
        use std::collections::HashMap;
        use std::sync::Mutex;

        #[derive(Debug, Default)]
        struct StubStore {
            rows: Mutex<HashMap<String, Vec<u8>>>,
        }
        impl crate::read_through::ReadThrough for StubStore {
            fn read_node_blob(&self, node_id: &str) -> Option<Vec<u8>> {
                self.rows.lock().unwrap().get(node_id).cloned()
            }
        }

        let core = GraphCore::new();
        // Resident node — read comes from RAM, never consults read-through.
        core.add_node("hot".into(), props(serde_json::json!({"i": 1})));

        // A durable store holding a node that is NOT resident in RAM (an evicted one).
        let store = Arc::new(StubStore::default());
        store
            .rows
            .lock()
            .unwrap()
            .insert("cold".into(), props(serde_json::json!({"i": 2})));

        // Before attaching: a RAM miss is a genuine absence.
        assert_eq!(core.get_node_properties("cold"), None);

        core.set_read_through(store);

        // After attaching: the resident node still reads from RAM…
        assert_eq!(
            core.get_node_properties("hot"),
            Some(props(serde_json::json!({"i": 1})))
        );
        // …and the evicted node reads through to the durable store with fidelity.
        assert_eq!(
            core.get_node_properties("cold"),
            Some(props(serde_json::json!({"i": 2})))
        );
        // A node in neither RAM nor the store is still absent.
        assert_eq!(core.get_node_properties("absent"), None);
    }

    /// CONCEPT:EG-KG.storage.bloom-negative-lookup-guard — once the bloom filter is marked complete, a
    /// miss for an id that was NEVER inserted skips the durable read-through call
    /// entirely (zero I/O), while a genuinely durable (evicted) node still reads
    /// through with fidelity. Before `mark_bloom_complete`, behavior is unchanged:
    /// every miss still consults the durable store (covers the in-progress
    /// paged-lazy-open window, where a not-yet-paged-in node must not be treated
    /// as absent).
    #[test]
    fn bloom_guard_skips_durable_read_for_never_inserted_key_once_complete() {
        use std::collections::HashMap;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Mutex;

        #[derive(Debug, Default)]
        struct CountingStore {
            rows: Mutex<HashMap<String, Vec<u8>>>,
            calls: AtomicUsize,
        }
        impl crate::read_through::ReadThrough for CountingStore {
            fn read_node_blob(&self, node_id: &str) -> Option<Vec<u8>> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.rows.lock().unwrap().get(node_id).cloned()
            }
        }

        let core = GraphCore::new();
        core.add_node("hot".into(), props(serde_json::json!({"i": 1})));
        // Simulate node-cap eviction of "cold": it WAS added (so `add_node`
        // recorded it in the bloom), then dropped from RAM only — exactly what
        // eviction does, per `read_through_serves_evicted_node` above.
        core.add_node("cold".into(), props(serde_json::json!({"i": 2})));
        core.node_properties.remove("cold");

        let store = Arc::new(CountingStore::default());
        store
            .rows
            .lock()
            .unwrap()
            .insert("cold".into(), props(serde_json::json!({"i": 2})));
        core.set_read_through(store.clone());

        // Before `mark_bloom_complete`: an id NEVER passed to `add_node` still
        // falls through to the durable store (byte-for-byte pre-bloom behavior).
        assert_eq!(core.get_node_properties("never-existed"), None);
        assert_eq!(
            store.calls.load(Ordering::SeqCst),
            1,
            "guard must be a no-op before the filter is marked complete"
        );

        core.mark_bloom_complete();

        // After: a node evicted from RAM but genuinely durable (was `add_node`'d,
        // hence in the bloom) still reads through with fidelity.
        assert_eq!(
            core.get_node_properties("cold"),
            Some(props(serde_json::json!({"i": 2})))
        );
        assert_eq!(store.calls.load(Ordering::SeqCst), 2);

        // A key that was NEVER inserted anywhere is now rejected by the bloom
        // filter BEFORE the durable call — the call count does not advance.
        assert_eq!(core.get_node_properties("never-existed"), None);
        assert_eq!(
            store.calls.load(Ordering::SeqCst),
            2,
            "bloom guard must skip the durable read for a never-inserted key"
        );
    }

    fn confidence_of(core: &GraphCore, id: &str) -> f64 {
        let bytes = core.get_node_properties(id).expect("node exists");
        let v: serde_json::Value = rmp_serde::from_slice(&bytes).unwrap();
        v.get("confidence").and_then(|c| c.as_f64()).unwrap()
    }

    fn obj(map: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        map.as_object().unwrap().clone()
    }

    fn field_of(core: &GraphCore, id: &str, field: &str) -> Option<serde_json::Value> {
        let bytes = core.get_node_properties(id)?;
        let v: serde_json::Value = rmp_serde::from_slice(&bytes).ok()?;
        v.get(field).cloned()
    }

    #[test]
    fn cas_succeeds_when_condition_matches_and_merges_updates() {
        let g = GraphCore::new();
        g.add_node(
            "task1".to_string(),
            props(serde_json::json!({"type": "Task", "status": "pending"})),
        );
        let ok = g.compare_and_set_fields(
            "task1",
            &obj(serde_json::json!({"status": "pending"})),
            &obj(serde_json::json!({"status": "claimed", "owner": "worker-7"})),
        );
        assert!(ok, "CAS should succeed when the condition matches");
        assert_eq!(
            field_of(&g, "task1", "status"),
            Some(serde_json::json!("claimed"))
        );
        assert_eq!(
            field_of(&g, "task1", "owner"),
            Some(serde_json::json!("worker-7"))
        );
        // Untouched existing field is preserved.
        assert_eq!(
            field_of(&g, "task1", "type"),
            Some(serde_json::json!("Task"))
        );
    }

    #[test]
    fn cas_fails_and_does_not_mutate_when_condition_mismatches() {
        let g = GraphCore::new();
        g.add_node(
            "task2".to_string(),
            props(serde_json::json!({"type": "Task", "status": "claimed"})),
        );
        let ok = g.compare_and_set_fields(
            "task2",
            &obj(serde_json::json!({"status": "pending"})),
            &obj(serde_json::json!({"status": "claimed", "owner": "intruder"})),
        );
        assert!(!ok, "CAS should fail when the condition does not match");
        assert_eq!(
            field_of(&g, "task2", "status"),
            Some(serde_json::json!("claimed"))
        );
        assert_eq!(field_of(&g, "task2", "owner"), None, "must not be mutated");
    }

    #[test]
    fn cas_fails_when_node_missing() {
        let g = GraphCore::new();
        let ok = g.compare_and_set_fields(
            "absent",
            &obj(serde_json::json!({"status": "pending"})),
            &obj(serde_json::json!({"status": "claimed"})),
        );
        assert!(!ok, "CAS on a missing node returns false");
        assert!(g.get_node_properties("absent").is_none());
    }

    #[test]
    fn cas_treats_missing_field_as_null() {
        let g = GraphCore::new();
        g.add_node(
            "task3".to_string(),
            props(serde_json::json!({"type": "Task"})),
        );
        // condition `owner: null` means "absent or null" — matches the absent field.
        let ok = g.compare_and_set_fields(
            "task3",
            &obj(serde_json::json!({"owner": null})),
            &obj(serde_json::json!({"owner": "worker-1"})),
        );
        assert!(ok, "null condition should match an absent field");
        assert_eq!(
            field_of(&g, "task3", "owner"),
            Some(serde_json::json!("worker-1"))
        );
        // A second claim with the same null condition now fails (owner is set).
        let ok2 = g.compare_and_set_fields(
            "task3",
            &obj(serde_json::json!({"owner": null})),
            &obj(serde_json::json!({"owner": "worker-2"})),
        );
        assert!(!ok2, "owner already set — second claim must fail");
        assert_eq!(
            field_of(&g, "task3", "owner"),
            Some(serde_json::json!("worker-1"))
        );
    }

    #[test]
    fn cas_if_gates_on_predicate() {
        // CONCEPT:EG-KG.txn.serializable-mutation-gate — the serializable gate only mutates a node whose CURRENT
        // row still matches the predicate.
        use eg_types::{CmpOp, RowPredicate};
        let g = GraphCore::new();
        g.add_node(
            "n1".to_string(),
            props(serde_json::json!({"type": "Agent", "rank": 5})),
        );
        let pred_match = RowPredicate::And(vec![
            RowPredicate::Cmp {
                col: "type".into(),
                op: CmpOp::Eq,
                value: serde_json::json!("Agent"),
            },
            RowPredicate::Cmp {
                col: "rank".into(),
                op: CmpOp::Gt,
                value: serde_json::json!(2),
            },
        ]);
        let ok = g.compare_and_set_fields_if(
            "n1",
            &pred_match,
            &obj(serde_json::json!({})),
            &obj(serde_json::json!({"active": false})),
        );
        assert!(ok, "predicate holds → update applies");
        assert_eq!(field_of(&g, "n1", "active"), Some(serde_json::json!(false)));
        // A predicate that no longer holds is a no-op.
        let pred_no = RowPredicate::Cmp {
            col: "rank".into(),
            op: CmpOp::Lt,
            value: serde_json::json!(2),
        };
        let ok2 = g.compare_and_set_fields_if(
            "n1",
            &pred_no,
            &obj(serde_json::json!({})),
            &obj(serde_json::json!({"active": true})),
        );
        assert!(!ok2, "predicate false → no mutation");
        assert_eq!(field_of(&g, "n1", "active"), Some(serde_json::json!(false)));
    }

    #[test]
    fn remove_node_if_gates_on_predicate() {
        // CONCEPT:EG-KG.txn.serializable-mutation-gate — `id` is injected so a predicate may reference it.
        use eg_types::{CmpOp, RowPredicate};
        let g = GraphCore::new();
        g.add_node(
            "keep".to_string(),
            props(serde_json::json!({"type": "Tool"})),
        );
        g.add_node(
            "drop".to_string(),
            props(serde_json::json!({"type": "Tool"})),
        );
        let pred = RowPredicate::Cmp {
            col: "id".into(),
            op: CmpOp::Eq,
            value: serde_json::json!("drop"),
        };
        assert!(!g.remove_node_if("keep", &pred), "id mismatch → kept");
        assert!(g.remove_node_if("drop", &pred), "id matches → removed");
        assert!(g.has_node("keep"));
        assert!(!g.has_node("drop"));
    }

    #[test]
    fn decay_halves_confidence_at_one_half_life() {
        let g = GraphCore::new();
        let now = 1_000_000u64;
        g.add_node(
            "n1".to_string(),
            props(serde_json::json!({"type": "Fact", "confidence": 1.0, "last_access": now - 100})),
        );
        let stats = g.decay_sweep(now, 100.0, 0.0, false);
        assert_eq!(stats.nodes_decayed, 1);
        let c = confidence_of(&g, "n1");
        assert!((c - 0.5).abs() < 1e-9, "expected ~0.5, got {c}");
    }

    #[test]
    fn get_nodes_by_label_filters_and_limits() {
        let g = GraphCore::new();
        g.add_node(
            "a1".to_string(),
            props(serde_json::json!({"type": "Agent", "name": "A"})),
        );
        g.add_node(
            "a2".to_string(),
            props(serde_json::json!({"type": "Agent", "name": "B"})),
        );
        g.add_node("c1".to_string(), props(serde_json::json!({"type": "Code"})));
        g.add_node(
            "l1".to_string(),
            props(serde_json::json!({"labels": ["Skill", "X"]})),
        );
        // The Python client keys the label on `node_type`, not `type` — the index
        // must find these too (else label-scoped MATCH under-returns).
        g.add_node(
            "nt1".to_string(),
            props(serde_json::json!({"node_type": "Tool", "label": ""})),
        );

        assert_eq!(g.get_nodes_by_label("Agent", 0).len(), 2); // type match, no cap
        assert_eq!(g.get_nodes_by_label("Agent", 1).len(), 1); // limit bounds result
        assert_eq!(g.get_nodes_by_label("Code", 0).len(), 1);
        let skills = g.get_nodes_by_label("Skill", 0); // "labels" array membership
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].0, "l1");
        let tools = g.get_nodes_by_label("Tool", 0); // `node_type` match
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].0, "nt1");
        assert!(g.get_nodes_by_label("Nonexistent", 0).is_empty());
    }

    /// Engine follow-up A (CONCEPT:EG-KG.query.unlabeled-scan-limit-pushdown): an EMPTY label means "no label
    /// filter" — a bounded scan across every node regardless of type, still
    /// honouring `limit`. This is the lever an unlabeled `MATCH (n) … LIMIT k`
    /// pushes its LIMIT into instead of falling back to the unbounded `GetNodes`
    /// dump (which trips the `RESULT_TOO_LARGE` overload guard on a large graph
    /// even when the caller only asked for a handful of rows).
    #[test]
    fn get_nodes_by_label_empty_label_is_unlabeled_bounded_scan() {
        let g = GraphCore::new();
        g.add_node(
            "a1".to_string(),
            props(serde_json::json!({"type": "Agent", "name": "A"})),
        );
        g.add_node(
            "a2".to_string(),
            props(serde_json::json!({"type": "Agent", "name": "B"})),
        );
        g.add_node("c1".to_string(), props(serde_json::json!({"type": "Code"})));

        // No cap: every node, regardless of label.
        assert_eq!(g.get_nodes_by_label("", 0).len(), 3);
        // A present limit bounds the unlabeled scan too — the pushdown target.
        assert_eq!(g.get_nodes_by_label("", 1).len(), 1);
        assert_eq!(g.get_nodes_by_label("", 2).len(), 2);
        // A limit at/above the node count returns everything, not an error.
        assert_eq!(g.get_nodes_by_label("", 100).len(), 3);
    }

    fn capability_graph() -> GraphCore {
        let g = GraphCore::new();
        // Fleet Tool nodes (KG-2.133 schema) — name + product synonyms.
        g.add_node(
            "tool_portainer-mcp_stack".to_string(),
            props(serde_json::json!({
                "type": "Tool", "name": "portainer_stack",
                "synonyms": ["portainer", "portainer-mcp"], "mcp_server": "portainer-mcp"
            })),
        );
        g.add_node(
            "tool_github-mcp_issues".to_string(),
            props(serde_json::json!({
                "node_type": "Tool", "name": "github_issues",
                "synonyms": ["github", "github-mcp"], "mcp_server": "github-mcp"
            })),
        );
        // A non-capability node must never seed a term.
        g.add_node(
            "code1".to_string(),
            props(serde_json::json!({"type": "Code", "name": "deploy"})),
        );
        g
    }

    #[test]
    fn match_ontology_terms_hits_capability_by_synonym() {
        let g = capability_graph();
        // The two validation cases (a product name, not a tool name).
        let hits = g.match_ontology_terms("Can you list the stacks I have on portainer?");
        assert!(hits
            .iter()
            .any(|h| h.term == "portainer" && h.node_type == "Tool"));
        // the match carries the owning fleet server so a caller can bind its toolset
        assert!(hits
            .iter()
            .any(|h| h.term == "portainer" && h.mcp_server == "portainer-mcp"));

        let gh = g.match_ontology_terms("use the github mcp to fetch open issues");
        assert!(gh.iter().any(|h| h.term == "github"));
        // also matches the exact tool name when spelled out
        assert!(g
            .match_ontology_terms("call github_issues now")
            .iter()
            .any(|h| h.term == "github_issues"));
    }

    #[test]
    fn match_ontology_terms_is_whole_word_and_typed() {
        let g = capability_graph();
        // 'portainer' inside a larger word must NOT match (whole-word gate).
        assert!(g
            .match_ontology_terms("teleportainerish gibberish")
            .is_empty());
        // trivial chat names no capability → no escalation signal.
        assert!(g.match_ontology_terms("hey, how are you today?").is_empty());
        // a non-capability node's name ("deploy") is never a term.
        assert!(g.match_ontology_terms("please deploy it").is_empty());
        assert!(g.match_ontology_terms("").is_empty());
    }

    #[test]
    fn match_ontology_terms_cache_refreshes_on_node_change() {
        let g = capability_graph();
        assert!(g.match_ontology_terms("anything about gitlab?").is_empty());
        // Add a new capability node; the index must rebuild on the changed count.
        g.add_node(
            "tool_gitlab-mcp_mr".to_string(),
            props(serde_json::json!({
                "type": "Tool", "name": "gitlab_mr", "synonyms": ["gitlab", "gitlab-mcp"]
            })),
        );
        assert!(g
            .match_ontology_terms("anything about gitlab?")
            .iter()
            .any(|h| h.term == "gitlab"));
    }

    #[test]
    fn fresh_node_does_not_decay() {
        let g = GraphCore::new();
        let now = 1_000_000u64;
        g.add_node(
            "n1".to_string(),
            props(serde_json::json!({"type": "Fact", "confidence": 1.0, "last_access": now})),
        );
        let stats = g.decay_sweep(now, 100.0, 0.0, false);
        assert_eq!(stats.nodes_decayed, 0);
        assert!((confidence_of(&g, "n1") - 1.0).abs() < 1e-12);
    }

    #[test]
    fn adjacency_bounded_remove_preserves_unrelated_and_drops_parallel_edges() {
        let g = GraphCore::new();
        for id in ["a", "b", "c", "d"] {
            g.add_node(id.into(), props(serde_json::json!({"id": id})));
        }
        g.add_edge("a".into(), "b".into(), props(serde_json::json!({"n": 1})))
            .unwrap();
        g.add_edge("a".into(), "b".into(), props(serde_json::json!({"n": 2})))
            .unwrap();
        g.add_edge(
            "b".into(),
            "b".into(),
            props(serde_json::json!({"self": true})),
        )
        .unwrap();
        g.add_edge(
            "c".into(),
            "d".into(),
            props(serde_json::json!({"keep": true})),
        )
        .unwrap();
        assert_eq!(g.edge_count(), 4);

        g.remove_node("b".into());

        assert_eq!(g.edge_count(), 1, "topology keeps an O(1) exact edge count");
        assert!(g.get_edge_properties("a", "b").is_empty());
        assert!(g.get_edge_properties("b", "b").is_empty());
        assert_eq!(g.get_edge_properties("c", "d").len(), 1);
        assert!(g.has_edge("c", "d"));
    }

    #[test]
    fn induced_subgraph_walks_selected_adjacency_and_preserves_parallel_edges() {
        let g = GraphCore::new();
        for id in ["a", "b", "c", "outside"] {
            g.add_node(id.into(), props(serde_json::json!({"id": id})));
        }
        for ordinal in 0..2 {
            g.add_edge(
                "a".into(),
                "b".into(),
                props(serde_json::json!({"ordinal": ordinal})),
            )
            .unwrap();
        }
        g.add_edge("b".into(), "c".into(), props(serde_json::json!({})))
            .unwrap();
        g.add_edge("a".into(), "outside".into(), props(serde_json::json!({})))
            .unwrap();

        let view = g.get_subgraph(&["a".into(), "b".into(), "a".into(), "c".into()]);
        assert_eq!(view.graph.node_count(), 3);
        assert_eq!(view.graph.edge_count(), 3);
        assert_eq!(
            view.edge_properties
                .get(&("a".to_string(), "b".to_string()))
                .unwrap()
                .len(),
            2
        );
        assert!(!view.node_map.contains_key("outside"));
        assert!(!view
            .edge_properties
            .contains_key(&("a".to_string(), "outside".to_string())));
    }

    /// CONCEPT:EG-KG.ingest.resets-socket-so-assimilation — `get_edges_page` walked with `limit=1` recovers
    /// EXACTLY the same edges as `get_edges()`, including every parallel edge
    /// under one `(source, target)` pair, in strictly increasing `(source,
    /// target, ordinal)` order.
    #[test]
    fn edges_page_walk_matches_full_dump_including_parallel_edges() {
        let g = GraphCore::new();
        for id in ["a", "b", "c", "d"] {
            g.add_node(id.into(), props(serde_json::json!({"id": id})));
        }
        g.add_edge("a".into(), "b".into(), props(serde_json::json!({"n": 1})))
            .unwrap();
        g.add_edge("a".into(), "b".into(), props(serde_json::json!({"n": 2})))
            .unwrap();
        g.add_edge("a".into(), "d".into(), props(serde_json::json!({})))
            .unwrap();
        g.add_edge("b".into(), "c".into(), props(serde_json::json!({})))
            .unwrap();

        let mut full = g.get_edges();
        full.sort();
        assert_eq!(full.len(), 4);

        let mut after: Option<(String, String, u32)> = None;
        let mut paged: Vec<(String, String, u32, Vec<u8>)> = Vec::new();
        loop {
            let after_ref = after
                .as_ref()
                .map(|(s, t, ord)| (s.as_str(), t.as_str(), *ord));
            let page = g.get_edges_page(after_ref, 1);
            if page.is_empty() {
                break;
            }
            assert_eq!(page.len(), 1, "limit=1 must return at most one row");
            let (s, t, ord, _) = page[0].clone();
            after = Some((s, t, ord));
            paged.extend(page);
            assert!(paged.len() <= full.len(), "pagination did not terminate");
        }

        assert_eq!(paged.len(), 4);
        for w in paged.windows(2) {
            let a = (w[0].0.as_str(), w[0].1.as_str(), w[0].2);
            let b = (w[1].0.as_str(), w[1].1.as_str(), w[1].2);
            assert!(a < b, "page rows must strictly increase: {a:?} then {b:?}");
        }
        let mut paged_triples: Vec<(String, String, Vec<u8>)> =
            paged.into_iter().map(|(s, t, _, p)| (s, t, p)).collect();
        paged_triples.sort();
        assert_eq!(paged_triples, full);
    }

    /// `limit == 0` is uncapped, matching `GetNodesByLabel`'s convention.
    #[test]
    fn edges_page_limit_zero_is_uncapped() {
        let g = GraphCore::new();
        for id in ["a", "b", "c"] {
            g.add_node(id.into(), props(serde_json::json!({})));
        }
        g.add_edge("a".into(), "b".into(), props(serde_json::json!({})))
            .unwrap();
        g.add_edge("a".into(), "c".into(), props(serde_json::json!({})))
            .unwrap();
        assert_eq!(g.get_edges_page(None, 0).len(), 2);
    }

    /// An empty graph pages to an empty first page (no panic).
    #[test]
    fn edges_page_on_empty_graph_is_empty() {
        let g = GraphCore::new();
        assert!(g.get_edges_page(None, 10).is_empty());
    }

    /// The edge-key cache must be invalidated by BOTH `mark_dirty` and
    /// `mark_dirty_preserving_indexes` (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation) — the latter is the
    /// "pure edge batch" path that deliberately skips the node-derived caches,
    /// which is exactly the case that changes this index. A regression that ties
    /// invalidation to `invalidate_indexes` instead (the node-only caches) would
    /// serve a stale page after an edge-only commit.
    #[test]
    fn edges_page_cache_invalidates_on_preserving_indexes_write_too() {
        let g = GraphCore::new();
        for id in ["a", "b", "c"] {
            g.add_node(id.into(), props(serde_json::json!({})));
        }
        g.add_edge("a".into(), "b".into(), props(serde_json::json!({})))
            .unwrap();
        // Build the cache.
        assert_eq!(g.get_edges_page(None, 0).len(), 1);
        assert!(g.edge_key_index.read().is_some());

        // Simulate the "pure edge batch" dispatch path.
        g.add_edge("a".into(), "c".into(), props(serde_json::json!({})))
            .unwrap();
        g.mark_dirty_preserving_indexes();
        assert!(
            g.edge_key_index.read().is_none(),
            "mark_dirty_preserving_indexes must still invalidate the edge-key cache"
        );
        assert_eq!(
            g.get_edges_page(None, 0).len(),
            2,
            "the new edge must be visible after re-seeding"
        );

        // And the plain node-oriented `mark_dirty` path invalidates it too.
        assert!(g.edge_key_index.read().is_some());
        g.mark_dirty();
        assert!(g.edge_key_index.read().is_none());
    }

    /// A single-node pattern (empty properties, so it matches ANY host node)
    /// against a 10-node host, with generous budgets that never trip, finds
    /// every host node exactly once and reports `truncated: false`.
    #[test]
    fn vf2_match_finds_every_candidate_with_generous_defaults() {
        let host = GraphCore::new();
        for i in 0..10 {
            host.add_node(format!("n{i}"), props(serde_json::json!({})));
        }
        let pattern = GraphCore::new();
        pattern.add_node("p".into(), props(serde_json::json!({})));

        let (matches, truncated) = host.vf2_subgraph_match(&pattern.analysis_snapshot(), 0, 0);
        assert_eq!(matches.len(), 10);
        assert!(!truncated, "generous defaults must not truncate 10 matches");
    }

    /// CONCEPT:EG-KG.mining.gspan-frequent-subgraph — `max_results` stops the backtracking search the moment it
    /// has collected enough matches, reporting `truncated: true` for the
    /// caller-visible partial result.
    #[test]
    fn vf2_match_truncates_at_max_results() {
        let host = GraphCore::new();
        for i in 0..10 {
            host.add_node(format!("n{i}"), props(serde_json::json!({})));
        }
        let pattern = GraphCore::new();
        pattern.add_node("p".into(), props(serde_json::json!({})));

        let (matches, truncated) = host.vf2_subgraph_match(&pattern.analysis_snapshot(), 2, 0);
        assert_eq!(matches.len(), 2, "must stop at exactly max_results");
        assert!(truncated);
    }

    /// CONCEPT:EG-KG.mining.gspan-frequent-subgraph — `max_steps` bounds the number of candidate-pair attempts
    /// independent of `max_results`, so a pathological search cannot run
    /// unbounded CPU even when it is still finding matches. A 1-node pattern
    /// spends exactly one step per host node considered, so `max_steps=3`
    /// stops after exactly 3 candidates (all matching here) with `truncated:
    /// true`.
    #[test]
    fn vf2_match_truncates_at_max_steps_independent_of_max_results() {
        let host = GraphCore::new();
        for i in 0..10 {
            host.add_node(format!("n{i}"), props(serde_json::json!({})));
        }
        let pattern = GraphCore::new();
        pattern.add_node("p".into(), props(serde_json::json!({})));

        // max_results is generous (100, never trips); only max_steps=3 bounds it.
        let (matches, truncated) = host.vf2_subgraph_match(&pattern.analysis_snapshot(), 100, 3);
        assert_eq!(
            matches.len(),
            3,
            "exactly 3 candidate-pair attempts were spent before the step budget tripped"
        );
        assert!(truncated);
    }

    #[test]
    fn msgpack_roundtrip_preserves_nodes_edges_props() {
        // A3: to_msgpack now encodes the typed snapshot directly. Round-trip must
        // preserve node/edge property BYTES exactly (they are opaque msgpack blobs).
        let g = GraphCore::new();
        let p1 = props(serde_json::json!({"type": "Code", "language": "java", "n": 7}));
        let p2 = props(serde_json::json!({"type": "Code", "language": "rust"}));
        g.add_node("a".to_string(), p1.clone());
        g.add_node("b".to_string(), p2.clone());
        let _ = g.add_edge(
            "a".to_string(),
            "b".to_string(),
            props(serde_json::json!({"relationship": "CALLS"})),
        );
        g.ledger.lock().push("evt1".to_string());
        let expected_ledger = g.get_ledger(); // includes auto ADD_NODE/ADD_EDGE entries

        let bytes = g.to_msgpack().unwrap();
        let g2 = GraphCore::new();
        g2.from_msgpack(&bytes).unwrap();

        assert_eq!(g2.node_count(), 2);
        assert_eq!(g2.get_node_properties("a"), Some(p1));
        assert_eq!(g2.get_node_properties("b"), Some(p2));
        assert_eq!(g2.get_edge_properties("a", "b").len(), 1);
        assert_eq!(g2.get_ledger(), expected_ledger);
    }

    /// BUG A1 follow-up (2026-08-12): the mutation ledger is an in-memory,
    /// CAPPED ring, not a durable change log — pushing past `LEDGER_CAP`
    /// silently drops the oldest half. `ledger_watermark()` makes that drop
    /// OBSERVABLE: it must advance by exactly the dropped count, and the
    /// surviving entries must be exactly the newest window (the earliest
    /// entries are genuinely gone, not merely hidden).
    #[test]
    fn ledger_cap_drop_advances_the_watermark() {
        let g = GraphCore::new();
        assert_eq!(
            g.ledger_watermark(),
            0,
            "a fresh graph has never dropped anything"
        );

        // Push one more than the cap through the SAME `GraphTxn::push_ledger`
        // every production mutation uses (bypassing add_node's own
        // auto-ledger entries, which would make the exact counts fragile to
        // unrelated formatting) so this test pins the real policy, not a
        // test-only shortcut.
        let txn = g.txn();
        for i in 0..100_001u64 {
            txn.push_ledger(format!("evt{i}"));
        }
        drop(txn);

        assert_eq!(
            g.ledger_watermark(),
            50_000,
            "exceeding the 100_000 cap by 1 must drop exactly one 50_000-entry trim"
        );
        let entries = g.get_ledger();
        assert_eq!(
            entries.len(),
            50_001,
            "100_001 pushed - 50_000 dropped = 50_001 retained"
        );
        assert_eq!(
            entries.first().map(String::as_str),
            Some("evt50000"),
            "the oldest SURVIVING entry must be the first one past the drop"
        );
        assert_eq!(
            entries.last().map(String::as_str),
            Some("evt100000"),
            "the newest entry is always retained"
        );
        // The dropped entries are genuinely gone, not just hidden.
        assert!(!entries.iter().any(|e| e == "evt0"));
        assert!(!entries.iter().any(|e| e == "evt49999"));
    }

    #[test]
    fn malformed_snapshot_restore_is_bounded_and_leaves_live_graph_unchanged() {
        let g = GraphCore::new();
        let original = props(serde_json::json!({"value": "keep"}));
        g.add_node("live".to_string(), original.clone());

        let malformed = GraphSnapshot {
            schema_version: GRAPH_SNAPSHOT_SCHEMA_VERSION,
            integrity_policy: None,
            nodes: vec![("replacement".to_string(), Arc::new(Vec::new()))],
            edges: vec![(
                "replacement".to_string(),
                "missing".to_string(),
                Arc::new(Vec::new()),
            )],
            ledger: vec!["untrusted".to_string()],
            semantic_store: crate::compute::semantic::SemanticStore::new(),
        }
        .to_msgpack()
        .unwrap();
        assert_eq!(
            g.from_msgpack(&malformed).unwrap_err(),
            "graph snapshot is invalid or exceeds resource limits"
        );
        assert_eq!(g.get_node_properties("live"), Some(original.clone()));
        assert_eq!(g.node_count(), 1);

        // map{"nodes": array32(2^32-1)}: the structural preflight rejects the
        // allocation hint before serde can reserve it or mutate the graph.
        let allocation_bomb = [
            0x81, 0xa5, b'n', b'o', b'd', b'e', b's', 0xdd, 0xff, 0xff, 0xff, 0xff,
        ];
        assert_eq!(
            g.from_msgpack(&allocation_bomb).unwrap_err(),
            "graph snapshot is invalid or exceeds resource limits"
        );
        assert_eq!(g.get_node_properties("live"), Some(original));
        assert_eq!(g.node_count(), 1);
    }

    #[test]
    fn snapshot_schema_version_and_shape_are_mandatory() {
        let g = GraphCore::new();
        let valid = g.snapshot().to_msgpack().unwrap();

        let mut missing: serde_json::Value = rmp_serde::from_slice(&valid).unwrap();
        missing.as_object_mut().unwrap().remove("schema_version");
        let missing = rmp_serde::to_vec_named(&missing).unwrap();
        assert_eq!(
            g.from_msgpack(&missing).unwrap_err(),
            "graph snapshot is invalid or exceeds resource limits"
        );

        let mut unknown: serde_json::Value = rmp_serde::from_slice(&valid).unwrap();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("obsolete".to_string(), serde_json::json!(true));
        let unknown = rmp_serde::to_vec_named(&unknown).unwrap();
        assert_eq!(
            g.from_msgpack(&unknown).unwrap_err(),
            "graph snapshot is invalid or exceeds resource limits"
        );

        let mut wrong = g.snapshot();
        wrong.schema_version = GRAPH_SNAPSHOT_SCHEMA_VERSION + 1;
        assert_eq!(
            wrong.to_msgpack().unwrap_err(),
            format!(
                "unsupported graph snapshot schema version {}; expected {}",
                GRAPH_SNAPSHOT_SCHEMA_VERSION + 1,
                GRAPH_SNAPSHOT_SCHEMA_VERSION
            )
        );
    }

    #[test]
    fn decay_compounds_across_sweeps() {
        // R(Δt₁)·R(Δt₂) must equal R(Δt₁+Δt₂): two one-half-life sweeps → 0.25.
        let g = GraphCore::new();
        g.add_node(
            "n1".to_string(),
            props(serde_json::json!({"type": "Fact", "confidence": 1.0, "last_access": 1000u64})),
        );
        g.decay_sweep(1100, 100.0, 0.0, false);
        g.decay_sweep(1200, 100.0, 0.0, false);
        let c = confidence_of(&g, "n1");
        assert!((c - 0.25).abs() < 1e-9, "expected ~0.25, got {c}");
    }

    #[test]
    fn touch_resets_confidence_and_clock() {
        let g = GraphCore::new();
        let now = 5000u64;
        g.add_node(
            "n1".to_string(),
            props(serde_json::json!({"type": "Fact", "confidence": 0.3, "last_access": 1000u64})),
        );
        assert_eq!(g.touch_nodes(&["n1".to_string()], now), 1);
        assert!((confidence_of(&g, "n1") - 1.0).abs() < 1e-12);
        // Immediately after touch, a sweep at the same instant must not decay.
        assert_eq!(g.decay_sweep(now, 100.0, 0.0, false).nodes_decayed, 0);
    }

    #[test]
    fn prune_removes_below_floor() {
        let g = GraphCore::new();
        let now = 1_000_000u64;
        // ~4 half-lives elapsed → retention ≈ 0.0625, below the 0.1 floor.
        g.add_node(
            "n1".to_string(),
            props(serde_json::json!({"type": "Fact", "confidence": 1.0, "last_access": now - 400})),
        );
        let stats = g.decay_sweep(now, 100.0, 0.1, true);
        assert_eq!(stats.nodes_pruned, 1);
        assert!(!g.has_node("n1"));
    }

    #[test]
    fn dirty_flag_mechanics_drive_incremental_checkpoint() {
        use std::sync::atomic::Ordering;
        let g = GraphCore::new();
        // A fresh graph starts dirty so it is checkpointed once (Phase C-C).
        assert!(g.dirty.load(Ordering::Relaxed));
        // take_dirty atomically reports-and-clears.
        assert!(g.take_dirty());
        assert!(!g.dirty.load(Ordering::Relaxed));
        assert!(!g.take_dirty());

        // A no-op decay (fresh node, no time elapsed) must NOT re-dirty the graph.
        let now = 1000u64;
        g.add_node(
            "n1".to_string(),
            props(serde_json::json!({"type": "Fact", "confidence": 1.0, "last_access": now})),
        );
        g.take_dirty(); // ignore any earlier state
        assert_eq!(g.decay_sweep(now, 100.0, 0.0, false).nodes_decayed, 0);
        assert!(
            !g.dirty.load(Ordering::Relaxed),
            "no-op decay must stay clean"
        );

        // A decay that actually changes confidence marks the graph dirty so the
        // background sweep's writes are captured by the next checkpoint.
        let later = now + 100;
        assert_eq!(g.decay_sweep(later, 100.0, 0.0, false).nodes_decayed, 1);
        assert!(
            g.dirty.load(Ordering::Relaxed),
            "real decay must mark dirty"
        );
    }

    // ── label index (CONCEPT:EG-KG.compute.consult-lazy) ──────────────────────────────────

    fn ids_of(rows: &[(String, Vec<u8>)]) -> Vec<String> {
        let mut v: Vec<String> = rows.iter().map(|(id, _)| id.clone()).collect();
        v.sort();
        v
    }

    #[test]
    fn label_index_hit_returns_matching_nodes_across_fields() {
        let g = GraphCore::new();
        // `type`, `node_type`, `label`, and `labels[]` must all be honoured.
        g.add_node("a".into(), props(serde_json::json!({"type": "Task"})));
        g.add_node("b".into(), props(serde_json::json!({"node_type": "Task"})));
        g.add_node("c".into(), props(serde_json::json!({"label": "Task"})));
        g.add_node(
            "d".into(),
            props(serde_json::json!({"labels": ["Other", "Task"]})),
        );
        g.add_node("e".into(), props(serde_json::json!({"type": "Person"})));

        // First call builds the index; assert it is now cached.
        let rows = g.get_nodes_by_label("Task", 0);
        assert_eq!(ids_of(&rows), vec!["a", "b", "c", "d"]);
        assert!(
            g.label_index.read().is_some(),
            "label lookup must populate the lazy index"
        );

        // Second call is served from the cache and returns the same set.
        let rows2 = g.get_nodes_by_label("Task", 0);
        assert_eq!(ids_of(&rows2), vec!["a", "b", "c", "d"]);

        // A different label is served from the same cached index.
        assert_eq!(ids_of(&g.get_nodes_by_label("Person", 0)), vec!["e"]);
        // An unknown label yields nothing.
        assert!(g.get_nodes_by_label("Nope", 0).is_empty());
    }

    #[test]
    fn label_index_respects_limit() {
        let g = GraphCore::new();
        for i in 0..5 {
            g.add_node(format!("n{i}"), props(serde_json::json!({"type": "T"})));
        }
        assert_eq!(g.get_nodes_by_label("T", 2).len(), 2);
        assert_eq!(g.get_nodes_by_label("T", 0).len(), 5);
    }

    #[test]
    fn label_keyset_pages_are_sorted_exclusive_and_complete() {
        let g = GraphCore::new();
        for id in ["n09", "n01", "n12", "n03", "n05"] {
            g.add_node(
                id.into(),
                props(serde_json::json!({"type": "SourceRecord"})),
            );
        }
        g.add_node(
            "other".into(),
            props(serde_json::json!({"type": "Unrelated"})),
        );

        let first = g.get_nodes_by_label_page("SourceRecord", None, 2);
        assert_eq!(ids_of(&first), vec!["n01", "n03"]);
        let second = g.get_nodes_by_label_page("SourceRecord", Some("n03"), 2);
        assert_eq!(ids_of(&second), vec!["n05", "n09"]);
        let third = g.get_nodes_by_label_page("SourceRecord", Some("n09"), 2);
        assert_eq!(ids_of(&third), vec!["n12"]);

        let unlabeled = g.get_nodes_by_label_page("", Some("n05"), 0);
        assert_eq!(ids_of(&unlabeled), vec!["n09", "n12", "other"]);
        assert!(
            g.node_id_index.read().is_some(),
            "unlabeled pagination must warm the sorted id directory"
        );

        // A committed structural mutation retires the directory; rebuilding it
        // includes the new id and retains exclusive-cursor ordering.
        g.add_node("n10".into(), props(serde_json::json!({"type": "Other"})));
        g.mark_dirty();
        assert!(g.node_id_index.read().is_none());
        assert_eq!(
            ids_of(&g.get_nodes_by_label_page("", Some("n09"), 0)),
            vec!["n10", "n12", "other"]
        );
    }

    #[test]
    fn label_index_invalidated_after_mutation() {
        let g = GraphCore::new();
        g.add_node("a".into(), props(serde_json::json!({"type": "Task"})));
        // Build the cache.
        assert_eq!(g.get_nodes_by_label("Task", 0).len(), 1);
        assert!(g.label_index.read().is_some());

        // A mutation (modelled by the dispatch calling mark_dirty after a write)
        // must drop the cache so the next lookup reflects the new node.
        g.add_node("b".into(), props(serde_json::json!({"type": "Task"})));
        g.mark_dirty();
        assert!(
            g.label_index.read().is_none(),
            "mark_dirty must invalidate the label index"
        );
        assert_eq!(ids_of(&g.get_nodes_by_label("Task", 0)), vec!["a", "b"]);

        // A node whose label changed must move buckets after invalidation.
        g.add_node("a".into(), props(serde_json::json!({"type": "Done"})));
        g.mark_dirty();
        assert_eq!(ids_of(&g.get_nodes_by_label("Task", 0)), vec!["b"]);
        assert_eq!(ids_of(&g.get_nodes_by_label("Done", 0)), vec!["a"]);
    }

    #[test]
    fn pure_edge_changes_preserve_node_derived_caches() {
        let _guard = PROP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let g = GraphCore::new();
        g.add_node(
            "a".into(),
            props(serde_json::json!({"type": "Task", "team": "blue", "meta": {"kind": "x"}})),
        );
        g.add_node(
            "b".into(),
            props(serde_json::json!({"type": "Task", "team": "red", "meta": {"kind": "y"}})),
        );
        assert_eq!(g.get_nodes_by_label("Task", 0).len(), 2);
        assert_eq!(g.nodes_by_property("team", "blue").unwrap(), vec!["a"]);
        assert_eq!(g.nodes_by_json_path("$.meta.kind", "y").unwrap(), vec!["b"]);

        g.add_edge("a".into(), "b".into(), props(serde_json::json!({})))
            .unwrap();
        let mut change = crate::index::ChangeSet::new();
        change.record_add_edge("a".into(), "b".into());
        g.maintain_indexes_at(&change, g.version() + 1, 2, 1);
        g.mark_dirty_preserving_indexes();

        assert!(g.label_index.read().is_some());
        assert!(g.property_index.read().is_some());
        assert!(g.path_index.read().is_some());
        assert_eq!(g.get_nodes_by_label("Task", 0).len(), 2);
        assert_eq!(g.nodes_by_property("team", "blue").unwrap(), vec!["a"]);
        assert_eq!(g.nodes_by_json_path("$.meta.kind", "y").unwrap(), vec!["b"]);
    }

    #[test]
    fn field_scoped_update_incrementally_maintains_covering_lazy_cache() {
        // W1.6/P7: a field-scoped CAS now INCREMENTALLY re-files the affected posting instead of
        // dropping the covering cache. The `team` update moves `a` from `team=blue` to `team=red`
        // in place; the label + path caches (unaffected) stay warm; the property cache ALSO stays
        // warm (refiled), not dropped — the pre-W1.6 behavior was to null it, forcing a rebuild.
        let _guard = PROP_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let g = GraphCore::new();
        g.add_node(
            "a".into(),
            props(serde_json::json!({
                "type": "Task", "team": "blue", "meta": {"kind": "x"}
            })),
        );
        assert_eq!(g.get_nodes_by_label("Task", 0).len(), 1);
        assert_eq!(g.nodes_by_property("team", "blue").unwrap(), vec!["a"]);
        assert_eq!(g.nodes_by_json_path("$.meta.kind", "x").unwrap(), vec!["a"]);
        let rebuilds_before = g.index_rebuilds();

        let mut conditions = serde_json::Map::new();
        conditions.insert("team".into(), serde_json::json!("blue"));
        let mut updates = serde_json::Map::new();
        updates.insert("team".into(), serde_json::json!("red"));
        assert!(g.compare_and_set_fields("a", &conditions, &updates));
        let mut change = crate::index::ChangeSet::new();
        change
            .updated_nodes
            .push(crate::index::NodeChange::with_fields(
                "a".into(),
                vec!["team".into()],
            ));
        g.maintain_indexes_at(&change, g.version() + 1, 1, 0);
        g.mark_dirty_preserving_indexes();

        assert!(g.label_index.read().is_some(), "labels were unaffected");
        assert!(g.path_index.read().is_some(), "$.meta was unaffected");
        assert!(
            g.property_index.read().is_some(),
            "the property index is incrementally re-filed (W1.6), not dropped"
        );
        // The refile is correct: `a` moved from blue to red, with NO full rebuild.
        assert_eq!(g.nodes_by_property("team", "red").unwrap(), vec!["a"]);
        assert!(
            g.nodes_by_property("team", "blue").unwrap().is_empty(),
            "the old value posting no longer lists a"
        );
        assert_eq!(
            g.index_rebuilds(),
            rebuilds_before,
            "an in-place refile must not trigger a full index rebuild"
        );
    }

    // ── perf/row-visibility-index: write-time-maintained RLS visibility cache ──
    // (CONCEPT:EG-KG.sharding.row-level-security perf). These assert the FAST PATH is actually
    // taken (via `visibility_index_rebuilds()`, a counter — never timing, per
    // GOC-70) and that the incrementally-maintained index stays correct across a
    // write that changes ownership. Bit-for-bit decision equivalence between the
    // index path and the decode path is proven separately in
    // `crate::isolation`'s `row_visibility_index_equivalence` test module.

    #[test]
    #[cfg(feature = "security")]
    fn visibility_index_warms_once_and_survives_writes_without_rebuilding() {
        let g = GraphCore::new();
        g.add_node(
            "a".into(),
            props(serde_json::json!({"_owner": "alice", "_visibility": "private"})),
        );
        g.add_node("b".into(), props(serde_json::json!({"name": "untagged"})));

        assert_eq!(g.visibility_index_rebuilds(), 0, "cold before any snapshot");
        let view1 = g.analysis_snapshot();
        assert_eq!(
            g.visibility_index_rebuilds(),
            1,
            "the first snapshot must warm the index with exactly one full rebuild"
        );
        assert_eq!(
            view1
                .visibility_index
                .get("a")
                .and_then(|v| v.owner.as_deref()),
            Some("alice")
        );

        // A second snapshot with nothing written in between must hit the warm
        // index — no rebuild.
        let _view2 = g.analysis_snapshot();
        assert_eq!(
            g.visibility_index_rebuilds(),
            1,
            "a warm re-read must not rebuild"
        );

        // An RLS-key-touching CAS, committed the way the write coalescer does
        // (mirrors `crates/eg-core/tests/dependency_scoped_cache.rs`'s
        // `commit_add`/`commit_remove_captured` shape: mutate, describe the
        // change, `maintain_indexes` + `mark_dirty`).
        let mut conditions = serde_json::Map::new();
        conditions.insert("_owner".into(), serde_json::json!("alice"));
        let mut updates = serde_json::Map::new();
        updates.insert("_owner".into(), serde_json::json!("bob"));
        assert!(g.compare_and_set_fields("a", &conditions, &updates));
        let mut change = crate::index::ChangeSet::new();
        change
            .updated_nodes
            .push(crate::index::NodeChange::with_fields(
                "a".into(),
                vec!["_owner".into()],
            ));
        g.maintain_indexes(&change);
        g.mark_dirty();

        let view3 = g.analysis_snapshot();
        assert_eq!(
            g.visibility_index_rebuilds(),
            1,
            "an RLS-key-touching CAS must be incrementally re-filed, not trigger a full rebuild"
        );
        assert_eq!(
            view3
                .visibility_index
                .get("a")
                .and_then(|v| v.owner.as_deref()),
            Some("bob"),
            "the warm index must reflect the ownership change made via incremental refile"
        );
    }

    #[test]
    #[cfg(feature = "security")]
    fn visibility_index_remove_unfiles_a_deleted_node() {
        let g = GraphCore::new();
        g.add_node("a".into(), props(serde_json::json!({"_owner": "alice"})));
        g.add_node("b".into(), props(serde_json::json!({"_owner": "bob"})));
        let _ = g.analysis_snapshot(); // warm the index
        assert!(g
            .visibility_index
            .read()
            .as_ref()
            .expect("warmed above")
            .contains_key("a"));

        let captured = g.get_node_properties("a");
        g.remove_node("a".into());
        let mut change = crate::index::ChangeSet::new();
        match captured {
            Some(b) => change.record_remove_node_with_properties("a".into(), b),
            None => change.record_remove_node("a".into()),
        }
        g.maintain_indexes(&change);
        g.mark_dirty();

        assert!(
            !g.visibility_index
                .read()
                .as_ref()
                .expect("still warm — only one node was removed")
                .contains_key("a"),
            "a removed node must be unfiled from the warm visibility index"
        );
        assert_eq!(
            g.visibility_index_rebuilds(),
            1,
            "a removal must be incrementally unfiled, not trigger a full rebuild"
        );
    }

    #[test]
    #[cfg(feature = "security")]
    fn visibility_index_drops_wholesale_on_unknown_scope_update_then_rewarms_on_next_read() {
        let g = GraphCore::new();
        g.add_node("a".into(), props(serde_json::json!({"_owner": "alice"})));
        let _ = g.analysis_snapshot();
        assert_eq!(g.visibility_index_rebuilds(), 1);
        assert!(g.visibility_index.read().is_some(), "warmed above");

        // An update carrying `changed_fields: None` (`NodeChange::with_properties`)
        // is the "unknown scope" shape — could have touched an RLS key just as
        // easily as any other field, so the conservative fallback drops the
        // whole index rather than guessing which entry to touch.
        let mut change = crate::index::ChangeSet::new();
        change
            .updated_nodes
            .push(crate::index::NodeChange::with_properties(
                "a".into(),
                props(serde_json::json!({"_owner": "bob"})),
            ));
        g.maintain_indexes(&change);
        g.mark_dirty();

        assert!(
            g.visibility_index.read().is_none(),
            "an unknown-scope update must conservatively drop the whole warm index"
        );

        // The next snapshot rebuilds it — exactly one MORE full rebuild, never a
        // silent stale-serve.
        let _view = g.analysis_snapshot();
        assert_eq!(g.visibility_index_rebuilds(), 2);
    }

    #[test]
    fn label_index_dedups_node_with_repeated_label() {
        let g = GraphCore::new();
        // Same value on type + node_type + labels[] → still ONE row for that label.
        g.add_node(
            "a".into(),
            props(serde_json::json!({"type": "Task", "node_type": "Task", "labels": ["Task"]})),
        );
        let rows = g.get_nodes_by_label("Task", 0);
        assert_eq!(ids_of(&rows), vec!["a"]);
    }

    // ── Agent-native memory primitives (CONCEPT:EG-KG.compute.hierarchical-summary-tier-eg / EG-221) ──────────────

    /// Decode a node's stored property object (test helper).
    fn obj_of(g: &GraphCore, id: &str) -> serde_json::Map<String, serde_json::Value> {
        let blob = g.get_node_properties(id).expect("node present");
        match decode_property_value(&blob).unwrap() {
            serde_json::Value::Object(o) => o,
            _ => panic!("not an object"),
        }
    }

    #[test]
    fn eg220_create_summary_node_links_children_and_queries() {
        let g = GraphCore::new();
        g.add_node("e1".into(), props(serde_json::json!({"type": "Episodic"})));
        g.add_node("e2".into(), props(serde_json::json!({"type": "Episodic"})));
        let sid = g.create_summary_node(
            1,
            &["e2".into(), "e1".into()],
            [("text".to_string(), serde_json::json!("day one"))]
                .into_iter()
                .collect(),
        );
        // Stored markers + caller text.
        let o = obj_of(&g, &sid);
        assert_eq!(o.get("type"), Some(&serde_json::json!("SummaryNode")));
        assert_eq!(o.get("summary_level"), Some(&serde_json::json!(1)));
        assert_eq!(o.get("summary_child_count"), Some(&serde_json::json!(2)));
        assert_eq!(o.get("text"), Some(&serde_json::json!("day one")));
        // Children query (sorted) + level query.
        assert_eq!(g.summary_children(&sid), vec!["e1", "e2"]);
        assert_eq!(g.summaries_at_level(1), vec![sid.clone()]);
        assert!(g.summaries_at_level(2).is_empty());
    }

    #[test]
    fn eg220_summary_ladder_multi_level() {
        let g = GraphCore::new();
        for e in ["a", "b", "c", "d"] {
            g.add_node(e.into(), props(serde_json::json!({"type": "Episodic"})));
        }
        let l1a = g.create_summary_node(1, &["a".into(), "b".into()], serde_json::Map::new());
        let l1b = g.create_summary_node(1, &["c".into(), "d".into()], serde_json::Map::new());
        // A level-2 summary whose children are the level-1 summaries.
        let l2 = g.create_summary_node(2, &[l1a.clone(), l1b.clone()], serde_json::Map::new());
        let mut lvl1 = g.summaries_at_level(1);
        lvl1.sort();
        let mut expect = vec![l1a.clone(), l1b.clone()];
        expect.sort();
        assert_eq!(lvl1, expect);
        assert_eq!(g.summaries_at_level(2), vec![l2.clone()]);
        assert_eq!(g.summary_children(&l2), expect);
    }

    #[test]
    fn eg220_create_summary_node_is_idempotent() {
        let g = GraphCore::new();
        g.add_node("x".into(), props(serde_json::json!({"type": "Episodic"})));
        let a = g.create_summary_node(1, &["x".into()], serde_json::Map::new());
        let before = g.edge_count();
        // Same inputs ⇒ same id, no duplicate SUMMARIZES edge.
        let b = g.create_summary_node(1, &["x".into()], serde_json::Map::new());
        assert_eq!(a, b);
        assert_eq!(g.edge_count(), before, "re-run must not stack edges");
        assert_eq!(g.summary_children(&a), vec!["x"]);
    }

    #[test]
    fn eg220_create_summary_node_skips_absent_children() {
        let g = GraphCore::new();
        g.add_node(
            "real".into(),
            props(serde_json::json!({"type": "Episodic"})),
        );
        let sid =
            g.create_summary_node(1, &["real".into(), "ghost".into()], serde_json::Map::new());
        // Only the existing child is linked — no dangling edge to "ghost".
        assert_eq!(g.summary_children(&sid), vec!["real"]);
    }

    #[test]
    fn eg221_consolidate_merges_props_redirects_edges_and_marks_episodics() {
        let g = GraphCore::new();
        // Two episodic memories, each with an EXTERNAL neighbour + bitemporal window.
        g.add_node(
            "ep1".into(),
            props(serde_json::json!({"type": "Episodic", "tx_from": 100, "tx_to": 200})),
        );
        g.add_node(
            "ep2".into(),
            props(serde_json::json!({"type": "Episodic", "tx_from": 150, "tx_to": 300})),
        );
        g.add_node("actor".into(), props(serde_json::json!({"type": "Person"})));
        g.add_node("topic".into(), props(serde_json::json!({"type": "Topic"})));
        // actor -> ep1 (incoming to cluster), ep2 -> topic (outgoing from cluster).
        g.add_edge(
            "actor".into(),
            "ep1".into(),
            props(serde_json::json!({"relationship": "OBSERVED"})),
        )
        .unwrap();
        g.add_edge(
            "ep2".into(),
            "topic".into(),
            props(serde_json::json!({"relationship": "ABOUT"})),
        )
        .unwrap();

        let sem = g.consolidate(
            &["ep1".into(), "ep2".into()],
            [
                ("type".to_string(), serde_json::json!("SemanticMemory")),
                (
                    "summary".to_string(),
                    serde_json::json!("actor discussed topic"),
                ),
            ]
            .into_iter()
            .collect(),
        );

        // Merged props + bitemporal span over children (min tx_from, max tx_to).
        let o = obj_of(&g, &sem);
        assert_eq!(
            o.get("summary"),
            Some(&serde_json::json!("actor discussed topic"))
        );
        assert_eq!(
            o.get("consolidated_from_count"),
            Some(&serde_json::json!(2))
        );
        assert_eq!(o.get("tx_from"), Some(&serde_json::json!(100)));
        assert_eq!(o.get("tx_to"), Some(&serde_json::json!(300)));

        // External edges redirected onto the semantic node.
        assert!(g.edge_has_relationship("actor", &sem, "OBSERVED"));
        assert!(g.edge_has_relationship(&sem, "topic", "ABOUT"));

        // Provenance CONSOLIDATES edges semantic -> each episodic.
        assert!(g.edge_has_relationship(&sem, "ep1", "CONSOLIDATES"));
        assert!(g.edge_has_relationship(&sem, "ep2", "CONSOLIDATES"));

        // Episodics preserved + marked (NOT deleted — bitemporal history intact).
        assert!(g.has_node("ep1") && g.has_node("ep2"));
        let e1 = obj_of(&g, "ep1");
        assert_eq!(e1.get("consolidated"), Some(&serde_json::json!(true)));
        assert_eq!(e1.get("consolidated_into"), Some(&serde_json::json!(sem)));
        assert_eq!(
            e1.get("tx_from"),
            Some(&serde_json::json!(100)),
            "child bitemporal preserved"
        );
    }

    #[test]
    fn eg221_consolidate_open_child_leaves_span_open() {
        let g = GraphCore::new();
        // One child has NO tx_to (still valid) ⇒ consolidated node stays open.
        g.add_node(
            "o1".into(),
            props(serde_json::json!({"type": "Episodic", "tx_from": 10, "tx_to": 20})),
        );
        g.add_node(
            "o2".into(),
            props(serde_json::json!({"type": "Episodic", "tx_from": 15})),
        );
        let sem = g.consolidate(&["o1".into(), "o2".into()], serde_json::Map::new());
        let o = obj_of(&g, &sem);
        assert_eq!(o.get("tx_from"), Some(&serde_json::json!(10)));
        assert!(o.get("tx_to").is_none(), "open child keeps the span open");
    }

    #[test]
    fn eg221_consolidate_is_localized_other_nodes_untouched() {
        let g = GraphCore::new();
        g.add_node("c1".into(), props(serde_json::json!({"type": "Episodic"})));
        g.add_node("c2".into(), props(serde_json::json!({"type": "Episodic"})));
        // An unrelated node NOT connected to the cluster.
        g.add_node(
            "bystander".into(),
            props(serde_json::json!({"type": "Note", "keep": "me"})),
        );
        let before = obj_of(&g, "bystander");
        let before_edges = g.edge_count();

        let sem = g.consolidate(&["c1".into(), "c2".into()], serde_json::Map::new());

        // Bystander untouched (no reindex/rewrite reached it).
        assert_eq!(
            obj_of(&g, "bystander"),
            before,
            "unrelated node must be untouched"
        );
        assert!(g.summary_children("bystander").is_empty());
        // Only the two CONSOLIDATES provenance edges were added (no external edges).
        assert_eq!(g.edge_count(), before_edges + 2);
        assert!(g.has_node(&sem));
    }

    #[test]
    fn eg221_consolidate_is_deterministic_and_idempotent() {
        let g = GraphCore::new();
        g.add_node("d1".into(), props(serde_json::json!({"type": "Episodic"})));
        g.add_node("d2".into(), props(serde_json::json!({"type": "Episodic"})));
        // Order-independent id (sorted cluster) + re-run adds no duplicate edges.
        let a = g.consolidate(&["d2".into(), "d1".into()], serde_json::Map::new());
        let mid = g.edge_count();
        let b = g.consolidate(&["d1".into(), "d2".into()], serde_json::Map::new());
        assert_eq!(a, b, "deterministic id independent of input order");
        assert_eq!(g.edge_count(), mid, "idempotent re-run stacks no edges");
    }

    // ── Memory maintenance — decay + reinforcement (CONCEPT:EG-KG.maintenance.combined-maintenance-primitive) ────────────

    /// Decode a node's `importance` as f64 (test helper).
    fn imp_of(g: &GraphCore, id: &str) -> f64 {
        obj_of(g, id)
            .get("importance")
            .and_then(|v| v.as_f64())
            .expect("importance present")
    }

    #[test]
    fn eg222_reinforce_bumps_importance_access_and_recency() {
        let g = GraphCore::new();
        g.add_node(
            "m".into(),
            props(serde_json::json!({"importance": 2.0, "access_count": 3, "last_access_ms": 100})),
        );
        assert!(g.reinforce("m", 500, 1.5));
        let o = obj_of(&g, "m");
        assert!((imp_of(&g, "m") - 3.5).abs() < 1e-9, "importance += weight");
        assert_eq!(o.get("access_count"), Some(&serde_json::json!(4)));
        assert_eq!(o.get("last_access_ms"), Some(&serde_json::json!(500)));
    }

    #[test]
    fn eg222_reinforce_requires_explicit_importance() {
        let g = GraphCore::new();
        g.add_node("m".into(), props(serde_json::json!({"type": "Episodic"})));
        assert!(!g.reinforce("m", 42, 0.25));
        let o = obj_of(&g, "m");
        assert!(o.get("importance").is_none());
        assert!(o.get("access_count").is_none());
        assert!(o.get("last_access_ms").is_none());
    }

    #[test]
    fn eg222_reinforce_missing_node_is_noop() {
        let g = GraphCore::new();
        assert!(!g.reinforce("ghost", 1, 1.0));
    }

    #[test]
    fn eg222_reinforce_revives_forgotten_memory() {
        let g = GraphCore::new();
        g.add_node(
            "m".into(),
            props(serde_json::json!({"importance": 0.1, "forgotten": true})),
        );
        assert!(g.reinforce("m", 10, 5.0));
        let o = obj_of(&g, "m");
        assert_eq!(o.get("forgotten"), Some(&serde_json::json!(false)));
        assert!((imp_of(&g, "m") - 5.1).abs() < 1e-9);
    }

    #[test]
    fn eg222_decay_halves_importance_over_one_half_life() {
        let g = GraphCore::new();
        g.add_node(
            "m".into(),
            props(serde_json::json!({"importance": 4.0, "last_access_ms": 1000})),
        );
        let hl = 10_000u64;
        assert!(g.decay_node("m", 1000 + hl, hl));
        assert!((imp_of(&g, "m") - 2.0).abs() < 1e-9, "one half-life halves");
        let o = obj_of(&g, "m");
        // Decay is not an access: last_access untouched, last_decay stamped.
        assert_eq!(o.get("last_access_ms"), Some(&serde_json::json!(1000)));
        assert_eq!(o.get("last_decay_ms"), Some(&serde_json::json!(1000 + hl)));
    }

    #[test]
    fn eg222_decay_is_idempotent_at_fixed_now() {
        let g = GraphCore::new();
        g.add_node(
            "m".into(),
            props(serde_json::json!({"importance": 8.0, "last_access_ms": 0})),
        );
        let hl = 100u64;
        g.decay_node("m", 100, hl); // one half-life ⇒ 4.0
        assert!((imp_of(&g, "m") - 4.0).abs() < 1e-9);
        g.decay_node("m", 100, hl); // same now ⇒ no further decay
        assert!(
            (imp_of(&g, "m") - 4.0).abs() < 1e-9,
            "idempotent at fixed now"
        );
    }

    #[test]
    fn eg222_decay_composes_over_two_steps() {
        let g = GraphCore::new();
        g.add_node(
            "m".into(),
            props(serde_json::json!({"importance": 8.0, "last_access_ms": 0})),
        );
        let hl = 100u64;
        g.decay_node("m", 100, hl); // -> 4.0
        g.decay_node("m", 200, hl); // another half-life -> 2.0
        assert!(
            (imp_of(&g, "m") - 2.0).abs() < 1e-9,
            "two half-lives compose to a quarter"
        );
    }

    #[test]
    fn eg222_decay_leaves_node_without_importance_untouched() {
        let g = GraphCore::new();
        g.add_node(
            "plain".into(),
            props(serde_json::json!({"type": "Note", "text": "hi"})),
        );
        assert!(
            !g.decay_node("plain", 5000, 100),
            "no importance ⇒ untouched"
        );
        let o = obj_of(&g, "plain");
        assert!(o.get("importance").is_none());
        assert!(o.get("last_decay_ms").is_none());
        assert_eq!(o.get("text"), Some(&serde_json::json!("hi")));
    }

    #[test]
    fn eg222_evict_below_marks_only_subthreshold_in_working_set() {
        let g = GraphCore::new();
        g.add_node("low".into(), props(serde_json::json!({"importance": 0.2})));
        g.add_node("high".into(), props(serde_json::json!({"importance": 0.9})));
        g.add_node(
            "outside".into(),
            props(serde_json::json!({"importance": 0.1})),
        );
        // Working set excludes "outside" — localized.
        let pruned = g.evict_below(&["low".into(), "high".into()], 0.5, false);
        assert_eq!(pruned, vec!["low"]);
        assert_eq!(
            obj_of(&g, "low").get("forgotten"),
            Some(&serde_json::json!(true))
        );
        assert!(
            obj_of(&g, "high").get("forgotten").is_none(),
            "above threshold survives"
        );
        assert!(
            obj_of(&g, "outside").get("forgotten").is_none(),
            "localized: node outside the working set is untouched"
        );
        // Marked, not deleted — provenance preserved.
        assert!(g.get_node_properties("low").is_some());
    }

    #[test]
    fn eg222_evict_below_delete_removes_node() {
        let g = GraphCore::new();
        g.add_node("low".into(), props(serde_json::json!({"importance": 0.1})));
        let pruned = g.evict_below(&["low".into()], 0.5, true);
        assert_eq!(pruned, vec!["low"]);
        assert!(g.get_node_properties("low").is_none(), "hard-deleted");
    }

    #[test]
    fn eg222_evict_below_requires_explicit_importance() {
        let g = GraphCore::new();
        g.add_node("bare".into(), props(serde_json::json!({"type": "Note"})));
        assert!(g.evict_below(&["bare".into()], 0.9, false).is_empty());
        assert!(obj_of(&g, "bare").get("forgotten").is_none());
        assert!(g.evict_below(&["bare".into()], 2.0, false).is_empty());
        assert!(obj_of(&g, "bare").get("forgotten").is_none());
    }

    #[test]
    fn eg222_forget_marks_by_default_and_deletes_when_asked() {
        let g = GraphCore::new();
        g.add_node("a".into(), props(serde_json::json!({"importance": 3.0})));
        g.add_node("b".into(), props(serde_json::json!({"importance": 3.0})));
        assert!(g.forget("a", false));
        assert_eq!(
            obj_of(&g, "a").get("forgotten"),
            Some(&serde_json::json!(true))
        );
        assert!(
            g.get_node_properties("a").is_some(),
            "mark preserves the node"
        );
        assert!(g.forget("b", true));
        assert!(
            g.get_node_properties("b").is_none(),
            "delete removes the node"
        );
        assert!(!g.forget("ghost", false), "missing node is a no-op");
    }

    #[test]
    fn eg222_maintain_decays_then_evicts_working_set() {
        let g = GraphCore::new();
        // "fades": importance 1.0 decays to 0.25 over two half-lives -> below 0.5.
        g.add_node(
            "fades".into(),
            props(serde_json::json!({"importance": 1.0, "last_access_ms": 0})),
        );
        // "sticks": high importance survives the same decay.
        g.add_node(
            "sticks".into(),
            props(serde_json::json!({"importance": 100.0, "last_access_ms": 0})),
        );
        let hl = 100u64;
        let ids = vec!["fades".to_string(), "sticks".to_string()];
        let (decayed, pruned) = g.maintain(&ids, 200, hl, 0.5, false);
        assert_eq!(decayed, 2, "both decayed");
        assert_eq!(pruned, vec!["fades"]);
        assert_eq!(
            obj_of(&g, "fades").get("forgotten"),
            Some(&serde_json::json!(true))
        );
        assert!(obj_of(&g, "sticks").get("forgotten").is_none());
        assert!((imp_of(&g, "sticks") - 25.0).abs() < 1e-9);
    }

    #[test]
    fn eg222_decay_memories_is_localized_to_working_set() {
        let g = GraphCore::new();
        for id in ["in1", "in2", "untouched"] {
            g.add_node(
                id.into(),
                props(serde_json::json!({"importance": 4.0, "last_access_ms": 0})),
            );
        }
        let hl = 100u64;
        let n = g.decay_memories(100, hl, &["in1".into(), "in2".into()]);
        assert_eq!(n, 2);
        assert!((imp_of(&g, "in1") - 2.0).abs() < 1e-9);
        assert!((imp_of(&g, "in2") - 2.0).abs() < 1e-9);
        // Not in the working set ⇒ never scanned/decayed.
        assert!((imp_of(&g, "untouched") - 4.0).abs() < 1e-9);
        assert!(obj_of(&g, "untouched").get("last_decay_ms").is_none());
    }

    // ── Scene-graph / 3D world model (CONCEPT:EG-KG.compute.scene-graph-primitives) ─────────────────────────

    use crate::scene::{Aabb, Pose, Quat, Vec3};

    fn approx_vec(a: Vec3, b: Vec3) {
        let e = 1e-9;
        assert!(
            (a.x - b.x).abs() < e && (a.y - b.y).abs() < e && (a.z - b.z).abs() < e,
            "{a:?} != {b:?}"
        );
    }

    /// A pose that is a pure translation.
    fn t_pose(x: f64, y: f64, z: f64) -> Pose {
        Pose {
            translation: Vec3::new(x, y, z),
            rotation: Quat::IDENTITY,
            scale: Vec3::ONE,
        }
    }

    #[test]
    fn eg087_world_transform_composes_translation_over_three_levels() {
        let g = GraphCore::new();
        let root = g.add_scene_object(&t_pose(1.0, 0.0, 0.0), None);
        let mid = g.add_scene_object(&t_pose(0.0, 2.0, 0.0), Some(&root));
        let leaf = g.add_scene_object(&t_pose(0.0, 0.0, 3.0), Some(&mid));
        // World translation is the sum of the chain (identity rotation, unit scale).
        approx_vec(
            g.world_transform(&leaf).unwrap().translation,
            Vec3::new(1.0, 2.0, 3.0),
        );
        // Hierarchy queries.
        assert_eq!(g.scene_children(&root), vec![mid.clone()]);
        assert_eq!(g.scene_parent(&leaf).as_deref(), Some(mid.as_str()));
        let mut desc = g.scene_descendants(&root);
        desc.sort();
        let mut expect = vec![mid.clone(), leaf.clone()];
        expect.sort();
        assert_eq!(desc, expect);
    }

    #[test]
    fn eg087_world_transform_applies_parent_rotation_and_scale() {
        let g = GraphCore::new();
        // Parent: 90° about +Z, uniform scale 2, origin.
        let s = std::f64::consts::FRAC_PI_4.sin();
        let parent_pose = Pose {
            translation: Vec3::ZERO,
            rotation: Quat::new(0.0, 0.0, s, s),
            scale: Vec3::new(2.0, 2.0, 2.0),
        };
        let parent = g.add_scene_object(&parent_pose, None);
        // Child at local +X(1). Parent scales it to (2,0,0) then rotates +90°→(0,2,0).
        let child = g.add_scene_object(&t_pose(1.0, 0.0, 0.0), Some(&parent));
        let w = g.world_transform(&child).unwrap();
        approx_vec(w.translation, Vec3::new(0.0, 2.0, 0.0));
        // Composed scale multiplies; composed point of local origin == world t.
        approx_vec(w.scale, Vec3::new(2.0, 2.0, 2.0));
        approx_vec(w.transform_point(Vec3::ZERO), Vec3::new(0.0, 2.0, 0.0));
    }

    #[test]
    fn eg087_reparent_updates_world_transform() {
        let g = GraphCore::new();
        let a = g.add_scene_object(&t_pose(10.0, 0.0, 0.0), None);
        let b = g.add_scene_object(&t_pose(0.0, 20.0, 0.0), None);
        let obj = g.add_scene_object(&t_pose(1.0, 1.0, 1.0), Some(&a));
        approx_vec(
            g.world_transform(&obj).unwrap().translation,
            Vec3::new(11.0, 1.0, 1.0),
        );
        // Reparent under b: world recomposes down the new chain, old edge is gone.
        assert!(g.reparent(&obj, Some(&b)));
        approx_vec(
            g.world_transform(&obj).unwrap().translation,
            Vec3::new(1.0, 21.0, 1.0),
        );
        assert_eq!(g.scene_parent(&obj).as_deref(), Some(b.as_str()));
        assert!(g.scene_children(&a).is_empty(), "old parent link dropped");
        assert_eq!(g.scene_children(&b), vec![obj.clone()]);
        // Detach to a root: world == local.
        assert!(g.reparent(&obj, None));
        assert!(g.scene_parent(&obj).is_none());
        approx_vec(
            g.world_transform(&obj).unwrap().translation,
            Vec3::new(1.0, 1.0, 1.0),
        );
    }

    #[test]
    fn eg087_spatial_relations_add_and_query() {
        let g = GraphCore::new();
        let table = g.add_scene_object(&Pose::identity(), None);
        let cup = g.add_scene_object(&Pose::identity(), None);
        let book = g.add_scene_object(&Pose::identity(), None);
        assert!(g.add_spatial_relation(&cup, &table, "ON"));
        assert!(g.add_spatial_relation(&book, &table, "ON"));
        assert!(g.add_spatial_relation(&table, &cup, "SUPPORTS"));
        // Idempotent re-add is a no-op.
        assert!(!g.add_spatial_relation(&cup, &table, "ON"));
        // Absent endpoint / self-loop refused.
        assert!(!g.add_spatial_relation(&cup, "ghost", "NEAR"));
        assert!(!g.add_spatial_relation(&cup, &cup, "NEAR"));
        let mut on = g.objects_with_relation("ON");
        on.sort();
        let mut expect = vec![(cup.clone(), table.clone()), (book.clone(), table.clone())];
        expect.sort();
        assert_eq!(on, expect);
        assert_eq!(
            g.objects_with_relation("SUPPORTS"),
            vec![(table.clone(), cup.clone())]
        );
        assert!(g.objects_with_relation("IN").is_empty());
    }

    #[test]
    fn eg087_bounding_volume_contains_and_intersects() {
        let g = GraphCore::new();
        let id = g.add_scene_object(&Pose::identity(), None);
        assert!(g.get_bounding_volume(&id).is_none());
        let box_ = Aabb::new(Vec3::ZERO, Vec3::new(4.0, 4.0, 4.0));
        assert!(g.set_bounding_volume(&id, &box_));
        let stored = g.get_bounding_volume(&id).unwrap();
        assert_eq!(stored, box_);
        // Pure-math predicates.
        assert!(stored.contains_point(Vec3::new(2.0, 2.0, 2.0)));
        assert!(!stored.contains_point(Vec3::new(5.0, 0.0, 0.0)));
        let inner = Aabb::new(Vec3::new(1.0, 1.0, 1.0), Vec3::new(2.0, 2.0, 2.0));
        assert!(stored.contains(&inner));
        let overlap = Aabb::new(Vec3::new(3.0, 3.0, 3.0), Vec3::new(6.0, 6.0, 6.0));
        assert!(stored.intersects(&overlap));
        assert!(!stored.contains(&overlap));
        // Pose stored on the same node still reads back independently.
        assert!(g.get_pose(&id).is_some());
    }

    #[test]
    fn eg087_add_scene_object_is_deterministic_and_stores_type() {
        // Same graph state + inputs ⇒ same derived id (no RNG/clock).
        let g1 = GraphCore::new();
        let g2 = GraphCore::new();
        let a = g1.add_scene_object(&t_pose(1.0, 2.0, 3.0), None);
        let b = g2.add_scene_object(&t_pose(1.0, 2.0, 3.0), None);
        assert_eq!(a, b, "deterministic id over (state, parent, pose)");
        assert!(a.starts_with("scene:"));
        let o = obj_of(&g1, &a);
        assert_eq!(o.get("type"), Some(&serde_json::json!("SceneObject")));
        // Absent parent is skipped — no dangling hierarchy edge, object is a root.
        let orphan = g1.add_scene_object(&Pose::identity(), Some("ghost"));
        assert!(g1.scene_parent(&orphan).is_none());
    }

    // ── Action / policy / trajectory memory (CONCEPT:EG-KG.compute.discounted-return) ──────────────────

    /// Build a trajectory with `rewards[i]` at timestep `i` (action = "a{i}").
    /// Returns the trajectory id.
    fn traj_with_rewards(g: &GraphCore, rewards: &[f64]) -> String {
        let tid = g.start_trajectory(serde_json::Map::new());
        for (i, r) in rewards.iter().enumerate() {
            let s = g
                .append_step(
                    &tid,
                    serde_json::json!(format!("a{i}")),
                    *r,
                    None,
                    None,
                    i as u64,
                )
                .expect("trajectory exists");
            assert!(s.starts_with(&tid), "step id namespaced under trajectory");
        }
        tid
    }

    #[test]
    fn eg099_append_step_builds_ordered_chain_and_bookkeeping() {
        let g = GraphCore::new();
        let tid = g.start_trajectory(
            [("policy".to_string(), serde_json::json!("greedy"))]
                .into_iter()
                .collect(),
        );
        assert!(tid.starts_with("trajectory:"));
        // Trajectory markers + caller prop.
        let to = obj_of(&g, &tid);
        assert_eq!(to.get("type"), Some(&serde_json::json!("Trajectory")));
        assert_eq!(to.get("policy"), Some(&serde_json::json!("greedy")));
        assert_eq!(to.get("step_count"), Some(&serde_json::json!(0)));

        let s0 = g
            .append_step(
                &tid,
                serde_json::json!("up"),
                1.0,
                Some("st0"),
                Some("st1"),
                0,
            )
            .unwrap();
        let s1 = g
            .append_step(
                &tid,
                serde_json::json!("down"),
                2.0,
                Some("st1"),
                Some("st2"),
                1,
            )
            .unwrap();
        let s2 = g
            .append_step(&tid, serde_json::json!("left"), 3.0, None, None, 2)
            .unwrap();

        // Ordered chain (append order).
        assert_eq!(
            g.trajectory_steps(&tid),
            vec![s0.clone(), s1.clone(), s2.clone()]
        );

        // Step fields stored verbatim + structural markers.
        let o1 = obj_of(&g, &s1);
        assert_eq!(o1.get("type"), Some(&serde_json::json!("Step")));
        assert_eq!(o1.get("action"), Some(&serde_json::json!("down")));
        assert_eq!(o1.get("reward"), Some(&serde_json::json!(2.0)));
        assert_eq!(o1.get("t"), Some(&serde_json::json!(1)));
        assert_eq!(o1.get("step_index"), Some(&serde_json::json!(1)));
        assert_eq!(o1.get("state_ref"), Some(&serde_json::json!("st1")));
        assert_eq!(o1.get("next_state_ref"), Some(&serde_json::json!("st2")));
        // Omitted refs are simply absent (not null).
        let o2 = obj_of(&g, &s2);
        assert!(!o2.contains_key("state_ref"));

        // Trajectory bookkeeping advanced.
        let to = obj_of(&g, &tid);
        assert_eq!(to.get("step_count"), Some(&serde_json::json!(3)));
        assert_eq!(to.get("head_step"), Some(&serde_json::json!(s0)));
        assert_eq!(to.get("tail_step"), Some(&serde_json::json!(s2)));

        // Temporal chain: NEXT_STEP s0→s1→s2, BELONGS_TO step→trajectory.
        assert!(g.edge_has_relationship(&s0, &s1, "NEXT_STEP"));
        assert!(g.edge_has_relationship(&s1, &s2, "NEXT_STEP"));
        assert!(!g.edge_has_relationship(&s0, &s2, "NEXT_STEP"));
        assert!(g.edge_has_relationship(&s1, &tid, "BELONGS_TO"));
        assert!(g.edge_has_relationship(&tid, &s1, "HAS_STEP"));
    }

    #[test]
    fn eg099_append_step_on_absent_trajectory_is_none() {
        let g = GraphCore::new();
        assert!(g
            .append_step("ghost", serde_json::json!("x"), 1.0, None, None, 0)
            .is_none());
        assert!(g.trajectory_steps("ghost").is_empty());
    }

    #[test]
    fn eg099_discounted_return_matches_hand_computed_value() {
        let g = GraphCore::new();
        // rewards 1, 2, 3 at t = 0, 1, 2 with gamma = 0.5.
        let tid = traj_with_rewards(&g, &[1.0, 2.0, 3.0]);
        // Σ gamma^t r_t = 0.5^0*1 + 0.5^1*2 + 0.5^2*3 = 1 + 1 + 0.75 = 2.75.
        let got = g.discounted_return(&tid, 0.5);
        assert!((got - 2.75).abs() < 1e-9, "discounted return {got} != 2.75");
        // gamma = 1.0 ⇒ plain sum == total_reward.
        assert!((g.discounted_return(&tid, 1.0) - 6.0).abs() < 1e-9);
        assert!((g.total_reward(&tid) - 6.0).abs() < 1e-9);
        // Absent / empty trajectories score 0.
        assert_eq!(g.discounted_return("ghost", 0.9), 0.0);
        let empty = g.start_trajectory(serde_json::Map::new());
        assert_eq!(g.discounted_return(&empty, 0.9), 0.0);
        assert_eq!(g.total_reward(&empty), 0.0);
    }

    #[test]
    fn eg099_windowed_returns_slides_over_rewards() {
        let g = GraphCore::new();
        let tid = traj_with_rewards(&g, &[1.0, 2.0, 3.0, 4.0]);
        // window 2 ⇒ [1+2, 2+3, 3+4] = [3, 5, 7].
        assert_eq!(g.windowed_returns(&tid, 2), vec![3.0, 5.0, 7.0]);
        // window == len ⇒ single full-sum element.
        assert_eq!(g.windowed_returns(&tid, 4), vec![10.0]);
        // Degenerate windows ⇒ empty.
        assert!(g.windowed_returns(&tid, 0).is_empty());
        assert!(g.windowed_returns(&tid, 5).is_empty());
    }

    #[test]
    fn eg099_best_and_worst_trajectory_selection() {
        let g = GraphCore::new();
        let lo = traj_with_rewards(&g, &[0.0, 0.0, 1.0]); // return @gamma=1 => 1.0
        let hi = traj_with_rewards(&g, &[5.0, 5.0]); // => 10.0
        let mid = traj_with_rewards(&g, &[2.0, 2.0]); // => 4.0
        let ids = vec![lo.clone(), hi.clone(), mid.clone()];
        assert_eq!(g.best_trajectory(&ids, 1.0), Some(hi.clone()));
        assert_eq!(g.worst_trajectory(&ids, 1.0), Some(lo.clone()));
        // Empty input ⇒ None.
        assert_eq!(g.best_trajectory(&[], 1.0), None);
        assert_eq!(g.worst_trajectory(&[], 1.0), None);
    }

    #[test]
    fn eg099_best_worst_break_ties_deterministically_by_id() {
        let g = GraphCore::new();
        // Two trajectories with the SAME return — selection must pick the smaller id.
        let a = g.start_trajectory(
            [("k".to_string(), serde_json::json!("a"))]
                .into_iter()
                .collect(),
        );
        let b = g.start_trajectory(
            [("k".to_string(), serde_json::json!("b"))]
                .into_iter()
                .collect(),
        );
        g.append_step(&a, serde_json::json!("x"), 3.0, None, None, 0)
            .unwrap();
        g.append_step(&b, serde_json::json!("x"), 3.0, None, None, 0)
            .unwrap();
        let mut ids = vec![a.clone(), b.clone()];
        ids.sort();
        let smallest = ids[0].clone();
        assert_eq!(g.best_trajectory(&ids, 0.9), Some(smallest.clone()));
        assert_eq!(g.worst_trajectory(&ids, 0.9), Some(smallest));
    }

    #[test]
    fn eg099_start_trajectory_is_deterministic_and_upserts_without_reset() {
        // Same graph state + props ⇒ same derived id (no RNG/clock).
        let g1 = GraphCore::new();
        let g2 = GraphCore::new();
        let p: serde_json::Map<String, serde_json::Value> =
            [("task".to_string(), serde_json::json!("nav"))]
                .into_iter()
                .collect();
        let a = g1.start_trajectory(p.clone());
        let b = g2.start_trajectory(p.clone());
        assert_eq!(a, b, "deterministic id over (state, props)");
        // Append a step, then re-run start_trajectory with the same props (upsert):
        // step_count must NOT reset, and the honoured `id` prop routes to the same node.
        g1.append_step(&a, serde_json::json!("go"), 1.0, None, None, 0)
            .unwrap();
        let again = g1.start_trajectory(
            [
                ("id".to_string(), serde_json::json!(a.clone())),
                ("task".to_string(), serde_json::json!("nav2")),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(again, a, "explicit id prop honoured");
        let o = obj_of(&g1, &a);
        assert_eq!(
            o.get("step_count"),
            Some(&serde_json::json!(1)),
            "upsert preserves in-progress count"
        );
        assert_eq!(
            o.get("task"),
            Some(&serde_json::json!("nav2")),
            "props merged on upsert"
        );
    }
}

#[cfg(test)]
mod concurrency_tests {
    // Phase C-B: the split-lock store exists FOR multi-writer concurrency, so it
    // must be validated under real thread contention (not just the single-threaded
    // correctness tests above). These tests run many writers/readers against ONE
    // `Arc<GraphCore>` and assert the core invariants hold: no panic/deadlock,
    // every write lands, and topology membership always agrees with the property
    // maps (each mutation is atomic under the topology write guard).
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn pbytes(i: usize) -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({"type": "Code", "i": i})).unwrap()
    }

    #[test]
    fn concurrent_add_nodes_all_land() {
        let core = Arc::new(GraphCore::new());
        let (writers, per) = (8usize, 500usize);
        let mut handles = Vec::new();
        for w in 0..writers {
            let c = core.clone();
            handles.push(thread::spawn(move || {
                for k in 0..per {
                    c.add_node(format!("w{w}_n{k}"), pbytes(k));
                }
            }));
        }
        // Readers hammer the topology + property maps concurrently with writers —
        // property reads take no topology lock, so they must never deadlock or
        // observe a torn map.
        for _ in 0..4 {
            let c = core.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..2000 {
                    let _ = c.node_count();
                    let _ = c.get_nodes_arc().len();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(core.node_count(), writers * per);
        // node_map (topology) and node_properties (DashMap) agree on cardinality.
        assert_eq!(core.get_nodes_arc().len(), writers * per);
    }

    #[test]
    fn concurrent_create_if_absent_has_exactly_one_winner() {
        let core = Arc::new(GraphCore::new());
        let writers = 16usize;
        let barrier = Arc::new(std::sync::Barrier::new(writers));
        let mut handles = Vec::with_capacity(writers);
        for writer in 0..writers {
            let core = Arc::clone(&core);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                (
                    writer,
                    core.create_node_if_absent("shared".to_string(), pbytes(writer)),
                )
            }));
        }
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        let winners: Vec<_> = outcomes
            .iter()
            .filter_map(|(writer, created)| created.then_some(*writer))
            .collect();
        assert_eq!(winners.len(), 1, "exactly one atomic create may succeed");
        assert_eq!(core.node_count(), 1);
        let stored = core.get_node_properties("shared").unwrap();
        let stored = decode_property_value(&stored).unwrap();
        assert_eq!(
            stored.get("i").and_then(serde_json::Value::as_u64),
            Some(winners[0] as u64),
            "losers must not overwrite the winning properties"
        );
    }

    #[test]
    fn concurrent_add_edges_and_snapshot_consistent() {
        let core = Arc::new(GraphCore::new());
        let n = 200usize;
        for i in 0..n {
            core.add_node(format!("n{i}"), pbytes(i));
        }
        let threads = 8usize;
        let mut handles = Vec::new();
        for t in 0..threads {
            let c = core.clone();
            handles.push(thread::spawn(move || {
                for i in 0..n - 1 {
                    if i % threads == t {
                        let _ = c.add_edge(format!("n{i}"), format!("n{}", i + 1), pbytes(i));
                    }
                }
            }));
        }
        // A snapshotter runs concurrently: snapshot() holds the topology read lock,
        // so every snapshot it produces is an internally consistent point-in-time.
        let c = core.clone();
        let snapper = thread::spawn(move || {
            for _ in 0..100 {
                let s = c.snapshot();
                assert!(s.nodes.len() <= n);
            }
        });
        for h in handles {
            h.join().unwrap();
        }
        snapper.join().unwrap();
        assert_eq!(core.edge_count(), n - 1);
    }

    #[test]
    fn concurrent_remove_add_keeps_membership_consistent() {
        // The classic resurrection/dangle hazard: interleaved add+remove of the
        // SAME id. Each op is atomic under the topology write guard, so at
        // quiescence topology membership must equal property membership — never a
        // live node index without properties, nor an orphan property.
        let core = Arc::new(GraphCore::new());
        core.add_node("x".into(), pbytes(0));
        let mut handles = Vec::new();
        for t in 0..6usize {
            let c = core.clone();
            handles.push(thread::spawn(move || {
                for k in 0..1000usize {
                    if (t + k) % 2 == 0 {
                        c.add_node("x".into(), pbytes(k));
                    } else {
                        c.remove_node("x".into());
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            core.has_node("x"),
            core.get_node_properties("x").is_some(),
            "topology and property membership must agree at quiescence"
        );
    }

    #[test]
    fn concurrent_property_reads_during_topology_writes() {
        // Property reads (DashMap, no topology lock) must run concurrently with
        // structural writers without deadlock and only ever see whole values.
        let core = Arc::new(GraphCore::new());
        for i in 0..100usize {
            core.add_node(format!("n{i}"), pbytes(i));
        }
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut handles = Vec::new();
        // Structural churn: add/remove a moving id set.
        {
            let c = core.clone();
            let s = stop.clone();
            handles.push(thread::spawn(move || {
                let mut k = 1000usize;
                while !s.load(std::sync::atomic::Ordering::Relaxed) {
                    c.add_node(format!("n{k}"), pbytes(k));
                    c.remove_node(format!("n{}", k - 1));
                    k += 1;
                }
            }));
        }
        // Readers decode whatever they find — a torn blob would fail to decode.
        for _ in 0..6 {
            let c = core.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..5000 {
                    for i in 0..100usize {
                        if let Some(b) = c.get_node_properties(&format!("n{i}")) {
                            assert!(decode_property_value(&b).is_ok());
                        }
                    }
                }
            }));
        }
        for _ in 0..2 {
            handles.pop().unwrap().join().unwrap();
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn memory_estimate_grows_with_nodes_and_shrinks_on_eviction() {
        let core = GraphCore::new();
        assert_eq!(core.memory_estimate(), 0, "empty graph estimates 0 bytes");

        for i in 0..100usize {
            core.add_node(format!("n{i}"), pbytes(i));
        }
        let full = core.memory_estimate();
        // 100 nodes each carry a blob + id + overhead — comfortably non-trivial.
        assert!(full > 100 * 64, "estimate {full} too small for 100 nodes");

        // Adding edges raises the estimate.
        for i in 1..100usize {
            core.add_edge(format!("n{}", i - 1), format!("n{i}"), pbytes(i))
                .unwrap();
        }
        let with_edges = core.memory_estimate();
        assert!(
            with_edges > full,
            "edges should raise the estimate: {with_edges} !> {full}"
        );

        // Evicting half the nodes shrinks it.
        core.evict_lru(50);
        let evicted = core.memory_estimate();
        assert!(
            evicted < with_edges,
            "eviction should shrink the estimate: {evicted} !< {with_edges}"
        );

        // Hibernation (drop all RAM) returns to ~0.
        core.hibernate();
        assert_eq!(core.memory_estimate(), 0, "hibernated graph estimates 0");
    }

    #[test]
    fn authoritative_materialized_version_is_a_one_shot_publication() {
        let core = GraphCore::new();
        core.add_node("recovered".to_string(), pbytes(1));
        assert_eq!(core.version(), 0, "row replay is not a new mutation");

        core.adopt_materialized_version(7).unwrap();
        assert_eq!(core.version(), 7);
        assert!(
            core.adopt_materialized_version(8).is_err(),
            "a live projection cannot be rewound or advanced by recovery plumbing"
        );
        assert_eq!(core.version(), 7);
    }
}
