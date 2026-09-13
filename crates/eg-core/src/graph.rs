// CONCEPT:EG-KG.compute.graph-compute-engine - Core Graph Storage Module
//
// Core petgraph DiGraph CRUD operations, node/edge storage,
// serialization, ledger, and repository parsing.

use aho_corasick::{AhoCorasick, MatchKind};
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock, RwLockWriteGuard};
use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::EdgeRef;
use std::collections::HashMap;
use std::sync::Arc;

mod analysis_support;
mod core_broker_claim;
mod core_broker_delivery;
mod core_broker_reclaim;
mod core_cache;
mod core_decay;
mod core_dirty;
mod core_edge_lifecycle;
mod core_edge_queries;
mod core_fork;
mod core_helpers;
mod core_index_changes;
mod core_index_nodes;
mod core_index_property;
mod core_index_visibility;
mod core_ledger;
mod core_memory;
mod core_neighbors;
mod core_node_mutation;
mod core_node_queries;
mod core_ontology;
mod core_path_dirty;
mod core_path_queries;
mod core_property_queries;
mod core_readthrough;
mod core_scene;
mod core_schema_analysis;
mod core_state;
mod core_stream_enqueue;
mod core_trajectory;
mod snapshot;
mod txn_edges;
mod txn_maintenance;
mod txn_memory;
mod txn_nodes;
mod txn_scene;
mod txn_trajectory;
mod view;

use analysis_support::apply_decay;
pub use analysis_support::{
    match_props, vf2_match_views, DEFAULT_VF2_MAX_RESULTS, DEFAULT_VF2_MAX_STEPS,
};
use core_helpers::{edge_endpoint_not_found, push_ledger_impl, HexLedger};
pub use snapshot::{GraphSnapshot, IntegrityPolicy, GRAPH_SNAPSHOT_SCHEMA_VERSION};
pub use view::GraphView;
#[cfg(feature = "result-cache")]
pub use view::ProjectionScope;

/// The graph TOPOLOGY — the petgraph structure + the id→index map. Mutated only
/// under `GraphCore::topo` write lock, read under its read lock. Kept separate
/// from properties so that property reads/writes (the common hot path) never
/// contend on the structural lock. (Phase C-B)
#[derive(Debug, Default, Clone)]
pub struct Topology {
    pub graph: StableDiGraph<String, String>,
    pub node_map: HashMap<String, NodeIndex>,
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
pub use eg_types::compute_result::algorithms::OntologyMatch;

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
    /// Property blobs for every ordered `(source, target)` pair, keyed by the pair
    /// and holding a `Vec` of ONE ENTRY PER PARALLEL EDGE (CONCEPT:EG-KG.compute.
    /// edge-write-read-model) — the topology graph is a real `petgraph` multigraph
    /// (`Topology::graph`), and this `Vec` is its property-storage sibling, ordinal-
    /// aligned with it (see `GraphCore::get_edges_page`'s `ordinal`).
    ///
    /// **This is deliberate, not an accident of implementation** — two established,
    /// independent uses both need more than one entry per pair:
    ///   1. **Distinct typed edges.** A pair may legitimately carry more than one
    ///      `relationship`-tagged edge at once (e.g. `PART_OF` AND a separately
    ///      asserted `SUPPORTS`) — `plain add_edge` never guards against this, and
    ///      most readers correctly treat each blob as an independent logical edge,
    ///      filtering by its own `relationship` field (`eg-rdf::update::edge_rel`,
    ///      `eg-plan::exec::rel_matches`, `mining`/`graphlearn`'s
    ///      `edge_matches_relation`, `GraphTxn::has_relationship_edge`).
    ///   2. **Bitemporal history.** `GraphTxn::invalidate_edge` / `supersede_edge`
    ///      NEVER delete — they close a matching entry's `valid_until`/`tx_to`
    ///      window in place and, for `supersede_edge`, push a new entry alongside
    ///      it. The prior and current state of one `relationship` therefore
    ///      legitimately coexist in the same `Vec`, selected by
    ///      `valid_from`/`valid_until`/`tx_from`/`tx_to`, exactly as
    ///      `eg-plan::exec::live_at`/`Op::AsOf` already select among NODE property
    ///      history for the same four keys.
    ///
    /// **The defect this codebase actually had** was never the storage shape — it
    /// was that some readers picked ONE entry (`.first()`/`.last()`/`.next()`)
    /// without applying either selection rule above, so whichever entry a writer
    /// happened to push last (or first) silently shadowed the others for that
    /// reader. The measured, severe instance was reasoning materialisation
    /// relabeling an asserted edge (fixed by the connect-only guard,
    /// `pair_already_connected` in `eg-compute::reasoning`, commit `28968274`);
    /// smaller instances (a CDC pre-image, an epistemic-projection bootstrap
    /// silently dropping a shadowed `GENERATED_BY`/`DERIVED_FROM` edge) are fixed
    /// alongside this note. A NEW reader over `edge_properties` must pick among
    /// multiple entries EXPLICITLY — by `relationship`, by bitemporal liveness, or
    /// both — never by Vec position.
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

/// The immutable inputs of one [`GraphCore::broker_claim_delivery`] call, bundled
/// so the validate/stamp/encode stages can be split out of a single 8-argument
/// method without threading the same eight values through each of them.
#[cfg(feature = "broker")]
struct BrokerClaimRequest<'a> {
    node_id: &'a str,
    queue: &'a str,
    group: &'a str,
    consumer: &'a str,
    expected_status: &'a str,
    expected_lease_until: Option<u64>,
    now_ms: u64,
    lease_ms: u64,
}

/// The result of resolving a delivery tag through its O(1) reverse-lookup node.
#[cfg(feature = "broker")]
enum BrokerTagOwner {
    /// The lookup is live, well-typed, and owned by the caller.
    Owned {
        lookup_id: String,
        node_id: String,
        queue: String,
    },
    /// The tag does not address a delivery this caller may fence. A stale or
    /// wrong-typed lookup has already been retired; an owner mismatch has NOT
    /// been (the live lookup belongs to somebody else).
    Rejected,
}

/// How a message row stands relative to the delivery tag being fenced.
#[cfg(feature = "broker")]
enum BrokerClaimLiveness {
    /// Claimed, carrying this exact tag, and owned by the caller.
    Live,
    /// Not claimed, or carrying a different (newer) tag — the lookup is stale.
    Stale,
    /// The live claim belongs to another consumer.
    NotOwner,
}

/// Everything a validated broker claim will write, encoded up front. Built with
/// READ-ONLY access to the transaction, so a rejected claim never mutates.
#[cfg(feature = "broker")]
struct BrokerClaim {
    counter_id: String,
    lookup_id: String,
    /// The prior generation's reverse-lookup tag, retired before the new one lands.
    prior_tag: Option<i64>,
    counter_blob: Vec<u8>,
    message_blob: Vec<u8>,
    lookup_blob: Vec<u8>,
    /// The stamped message row returned to the caller.
    message_value: serde_json::Value,
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

/// Whether any property blob recorded on edge `source_id -> target_id` declares
/// `relationship`. The crate's one answer to that question: both the write transaction
/// and the store read through it, so a change to the edge-property encoding has one site.
fn edge_declares_relationship(
    edge_properties: &DashMap<(String, String), Vec<Arc<Vec<u8>>>>,
    source_id: &str,
    target_id: &str,
    relationship: &str,
) -> bool {
    edge_properties
        .get(&(source_id.to_string(), target_id.to_string()))
        .is_some_and(|blobs| {
            blobs
                .iter()
                .any(|blob| blob_declares_relationship(blob, relationship))
        })
}

fn blob_declares_relationship(blob: &[u8], relationship: &str) -> bool {
    decode_property_value(blob)
        .ok()
        .and_then(|value| {
            value
                .get("relationship")
                .and_then(|rel| rel.as_str())
                .map(|value| value == relationship)
        })
        .unwrap_or(false)
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
