// CONCEPT:KG-2.16 - Core Graph Storage Module
//
// Core petgraph DiGraph CRUD operations, node/edge storage,
// serialization, ledger, and repository parsing.

use aho_corasick::{AhoCorasick, MatchKind};
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock, RwLockWriteGuard};
use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::EdgeRef;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

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
#[derive(Debug, Default, Clone)]
pub struct GraphView {
    pub graph: StableDiGraph<String, String>,
    pub node_map: HashMap<String, NodeIndex>,
    pub node_properties: HashMap<String, Arc<Vec<u8>>>,
    pub edge_properties: HashMap<(String, String), Vec<Arc<Vec<u8>>>>,
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
/// (CONCEPT:EG-010). `term` is the matched alias/name, `node_type` its capability
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
/// and reused while `node_count` matches the live store (CONCEPT:EG-010).
#[derive(Debug)]
struct OntologyTermIndex {
    node_count: usize,
    ac: AhoCorasick,
    metas: Vec<OntologyMatch>,
}

/// Secondary property index (CONCEPT:KG-2.199): a bounded, demand-driven set of
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

/// Inverted JSONPath path-index (CONCEPT:EG-084 — document/JSON deep indexing): for
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

/// Default cap on the number of distinct JSONPaths ever indexed (CONCEPT:EG-084).
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

/// A change notification (CONCEPT:EG-064 — GraphQL real subscriptions via CDC).
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

/// Dependency-light change-notification fan-out (CONCEPT:EG-064). A
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
        let mut sinks = self.sinks.lock();
        let event = ChangeEvent {
            graph: self.graph.read().clone(),
            version,
        };
        sinks.retain(|w| match w.upgrade() {
            Some(s) => {
                s.on_change(&event);
                true
            }
            None => false,
        });
        if sinks.is_empty() {
            self.active
                .store(false, std::sync::atomic::Ordering::Release);
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
    /// Has this graph been mutated since its last checkpoint? (Phase C-C —
    /// incremental checkpointing.) Starts `true` so a freshly created or freshly
    /// loaded graph is snapshotted once; thereafter `checkpoint_all` skips graphs
    /// that are still clean, so an idle tenant costs no checkpoint I/O.
    pub dirty: std::sync::atomic::AtomicBool,
    /// Monotonic write-version counter for optimistic concurrency control
    /// (CONCEPT:KG-2.180 — OCC ACID transactions). Bumped once per COMMITTED write
    /// (every single-op/coalesced write via `mark_dirty`, and once per multi-op
    /// txn commit). A staged OCC transaction snapshots this at begin and re-checks
    /// it (plus the read-set node versions) under the commit lock; a concurrent
    /// inline/coalesced write that bumped it forces the txn to re-validate. Read
    /// cheaply via `version()`; never gates a read path.
    pub version: std::sync::atomic::AtomicU64,
    /// Change-notification fan-out (CONCEPT:EG-064). `mark_dirty` (and the
    /// remote-change path) emit a [`ChangeEvent`] carrying the bumped `version`; a
    /// server-layer GraphQL subscription carrier subscribes to turn a poll-only
    /// subscription into a real push (re-resolve-on-change). Dep-light + off the hot
    /// path when there are no subscribers, so the default/Pi build is unaffected.
    changes: ChangeNotifier,
    /// Cached aho-corasick index of capability-node terms for the lexical
    /// classification gate (CONCEPT:EG-010). Built lazily and reused while the
    /// node count is unchanged, so `match_ontology_terms` is ~µs per query
    /// instead of a full node scan. `None` until first use / after invalidation.
    ontology_index: RwLock<Option<OntologyTermIndex>>,
    /// Cached secondary label index (CONCEPT:KG-2.176): `label → node ids` so
    /// `get_nodes_by_label` is an O(1) map lookup instead of a full DashMap scan
    /// that deserializes every node's properties. Built lazily on first label
    /// lookup and invalidated by `mark_dirty()` after any successful write (the
    /// same dirty flag the checkpoint uses) — a property update can change a
    /// node's label without changing `node_count`, so this index must NOT key its
    /// validity on node count the way the ontology index does. `None` until first
    /// use / after invalidation. A node appears under every label it carries
    /// across `type`/`node_type`/`label`/`labels` (mirrors `get_nodes_by_label`).
    label_index: RwLock<Option<HashMap<String, Vec<String>>>>,
    /// Cached secondary PROPERTY index (CONCEPT:KG-2.199): for each indexed
    /// property key, a `value → node ids` map so `nodes_by_property(key, value)`
    /// is an O(1) map lookup instead of a full DashMap scan that deserializes
    /// every node's properties (the perf win behind SQL `WHERE prop = 'x'`
    /// predicate pushdown in eg-query). The set of indexed keys is BOUNDED and
    /// demand-driven: a key is indexed on its FIRST `nodes_by_property` call and
    /// then cached, up to `EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES` keys (default
    /// 32); keys named in `EPISTEMIC_GRAPH_INDEXED_PROPERTIES` (comma-separated)
    /// are pre-seeded on first use. Indexing every key would be unbounded memory,
    /// hence the cap. Invalidated by `mark_dirty()` after any successful write —
    /// the SAME dirty flag the checkpoint + label index use — so it never serves a
    /// stale view across a mutation (a property write can change an indexed value
    /// without changing `node_count`, so validity must NOT key on node count).
    /// `None` until first use / after invalidation; the inner map only ever holds
    /// the keys demanded so far.
    property_index: RwLock<Option<PropertyIndex>>,
    /// Cached inverted JSONPath path-index (CONCEPT:EG-084 — document/JSON deep
    /// indexing): `jsonpath → value → ids` (equality/`->>`) + `jsonpath → ids`
    /// (existence/`@>` selectivity), so a deep JSON filter is index-accelerated
    /// instead of a full node scan. Bounded + demand-driven exactly like
    /// `property_index`, and invalidated by the SAME `mark_dirty()` after any write —
    /// a JSON write can change a nested value without changing `node_count`, so
    /// validity must NOT key on node count. `None` until first use / after
    /// invalidation.
    path_index: RwLock<Option<PathIndex>>,
    /// The unified secondary-index registry/seam (CONCEPT:KG-2.213). Owns the
    /// `SecondaryIndex` descriptors (label, property, + discoverable vector /
    /// ontology) so a planner consults ONE registry — `index_for(predicate)` /
    /// `descriptors_for_column(col)` — instead of bespoke per-index checks. The
    /// label/property CACHES still live in the fields above (lazy + `mark_dirty`-
    /// invalidated); the manager only routes, so their behavior is unchanged. The
    /// registry is fixed for the graph's lifetime ⇒ no interior locking needed.
    index_manager: crate::index::IndexManager,
    /// Read-through into the durable tier on a RAM MISS (CONCEPT:KG-2.191). Set
    /// only under redb-AUTHORITATIVE mode, where a node may have been evicted from
    /// RAM once it is durable in redb. On a node-property miss the read path
    /// consults this to serve the evicted node's stored blob, so eviction can bound
    /// memory WITHOUT making the node unreadable. `None` (the default, and always
    /// off authoritative mode) means a miss is a genuine absence — behavior is then
    /// byte-for-byte unchanged. See `crate::read_through`.
    read_through: RwLock<Option<Arc<dyn crate::read_through::ReadThrough>>>,
    /// Version-keyed query-RESULT cache (CONCEPT:KG-2.233, feature `result-cache`).
    /// Caches the serialized bytes of a read query (`Sql`/`Cypher`/`Sparql`/
    /// `UnifiedQuery`) keyed by `(query-hash, version())`. A repeated identical query
    /// on an UNCHANGED graph hits; any write bumps `version` so the next lookup keys
    /// on a new version and misses (recompute) — staleness is impossible by
    /// construction. Bounded LRU, pure-Rust, so it folds into the lean Pi tier.
    #[cfg(feature = "result-cache")]
    result_cache: crate::result_cache::ResultCache,
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
}

/// Owned, serializable persistent state of a graph — exactly what a snapshot file
/// holds. Two roles (CONCEPT:KG-2.8):
///
/// * **Non-blocking checkpoint (A1):** producing it clones the node/edge/ledger/
///   semantic data (a memcpy, fast relative to encoding), so `checkpoint_all` can
///   take it under a BRIEF lock and serialize it OFF the lock — instead of holding
///   the lock through the whole ~10s MessagePack encode of a 450MB graph, which
///   froze every concurrent writer.
/// * **Direct serialization (A3):** encoded straight via `rmp_serde`. Node/edge
///   properties are ALREADY MessagePack byte blobs; the previous path round-tripped
///   them through `serde_json::Value`, re-encoding every property byte as a JSON
///   number — pure overhead and the dominant allocator in checkpoint flamegraphs.
///   The on-disk shape (a map keyed `nodes`/`edges`/`ledger`/`semantic_store`) is
///   unchanged, so `from_msgpack` reads both pre- and post-change snapshot files.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct GraphSnapshot {
    // Arc-valued (Phase C-A): building a snapshot clones Arc pointers, not the
    // property bytes. `Arc<Vec<u8>>` serializes byte-for-byte the same as
    // `Vec<u8>`, so old and new snapshot files remain interchangeable.
    pub nodes: Vec<(String, Arc<Vec<u8>>)>,
    pub edges: Vec<(String, String, Arc<Vec<u8>>)>,
    pub ledger: Vec<String>,
    pub semantic_store: crate::compute::semantic::SemanticStore,
}

impl GraphSnapshot {
    /// Serialize this snapshot to MessagePack (called OFF the graph lock).
    pub fn to_msgpack(&self) -> Result<Vec<u8>, String> {
        rmp_serde::to_vec_named(self).map_err(|e| e.to_string())
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
    /// conflict checks over an overlaid snapshot — CONCEPT:EG-049.)
    pub fn has_node(&self, node_id: &str) -> bool {
        self.node_map.contains_key(node_id)
    }

    /// The decoded property object for a node in this view, or `None` when the node
    /// is absent / its blob is not a decodable object (CONCEPT:EG-049 RETURNING over
    /// an overlaid snapshot).
    pub fn node_row_object(
        &self,
        node_id: &str,
    ) -> Option<serde_json::Map<String, serde_json::Value>> {
        let blob = self.node_properties.get(node_id)?;
        match rmp_serde::from_slice::<serde_json::Value>(blob) {
            Ok(serde_json::Value::Object(o)) => Some(o),
            _ => None,
        }
    }

    // ── Read-your-own-writes overlay (CONCEPT:EG-049) ────────────────────
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
        let mut val = match rmp_serde::from_slice::<serde_json::Value>(&bytes) {
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
}

/// Streams a byte slice as lowercase hex DIRECTLY into a formatter (CONCEPT:EG-028).
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

/// Cap the ledger in place, dropping the oldest half once it exceeds the bound.
/// Shared by every mutation so the trim policy lives in one spot.
fn push_ledger(ledger: &mut Vec<String>, entry: String) {
    ledger.push(entry);
    if ledger.len() > 100_000 {
        ledger.drain(0..50_000);
    }
}

impl<'a> GraphTxn<'a> {
    // ── Node CRUD (under the held topology write guard) ──────────────────

    pub fn add_node(&mut self, node_id: String, properties_msgpack: Vec<u8>) {
        if !self.topo.node_map.contains_key(&node_id) {
            let new_idx = self.topo.graph.add_node(node_id.clone());
            self.topo.node_map.insert(node_id.clone(), new_idx);
        }
        let log = format!("ADD_NODE|{}|{}", node_id, HexLedger(&properties_msgpack));
        self.node_properties
            .insert(node_id.clone(), Arc::new(properties_msgpack));
        push_ledger(&mut self.ledger.lock(), log);
    }

    pub fn remove_node(&mut self, node_id: String) {
        if let Some(idx) = self.topo.node_map.remove(&node_id) {
            // Properties first, then topology: a crash mid-remove can never leave
            // a live node index whose properties already vanished (which on reload
            // would resurrect a half-deleted node). Topology is the source of truth.
            self.node_properties.remove(&node_id);
            self.edge_properties
                .retain(|k, _| k.0 != node_id && k.1 != node_id);
            self.topo.graph.remove_node(idx);
            push_ledger(&mut self.ledger.lock(), format!("REMOVE_NODE|{}", node_id));
        }
    }

    /// Serializable gated remove (CONCEPT:EG-045). Decodes the node's CURRENT
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
    /// predicate evaluation (CONCEPT:EG-045). The synthetic `id` column is injected
    /// (the blob stores only properties, not the node id) so a predicate may
    /// reference `id` alongside property columns. `None` if absent/undecodable.
    fn node_row_map(&self, node_id: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
        let bytes = self.node_properties.get(node_id)?.value().clone();
        let val = rmp_serde::from_slice::<serde_json::Value>(&bytes).ok()?;
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
            None => return Err(format!("Source node '{}' not found", source_id)),
        };
        let target_idx = match self.topo.node_map.get(&target_id) {
            Some(&idx) => idx,
            None => return Err(format!("Target node '{}' not found", target_id)),
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
        push_ledger(&mut self.ledger.lock(), log);
        Ok(())
    }

    pub fn remove_edge(&mut self, source_id: String, target_id: String) {
        if let (Some(&src_idx), Some(&tgt_idx)) = (
            self.topo.node_map.get(&source_id),
            self.topo.node_map.get(&target_id),
        ) {
            if let Some(edge_idx) = self.topo.graph.find_edge(src_idx, tgt_idx) {
                self.topo.graph.remove_edge(edge_idx);
            }
            self.edge_properties
                .remove(&(source_id.clone(), target_id.clone()));
            push_ledger(
                &mut self.ledger.lock(),
                format!("REMOVE_EDGE|{}|{}", source_id, target_id),
            );
        }
    }

    /// Atomic compare-and-set on a node's property blob (CONCEPT:KG-2 backend-
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
        let mut val = match rmp_serde::from_slice::<serde_json::Value>(&bytes) {
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
        push_ledger(&mut self.ledger.lock(), log);
        true
    }

    /// Serializable gated compare-and-set (CONCEPT:EG-045). Like
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
    /// (CONCEPT:KG-2.251). Sets the matching edge's `valid_until = invalid_at`
    /// (event-time close) and `tx_to = tx_now` (belief retracted) — it does NOT
    /// remove the edge, so an `AS OF` before `invalid_at` still sees the fact and the
    /// `AS OF TX` history of what-we-believed is preserved. Matches the edge(s)
    /// between `(source_id, target_id)` whose `relationship`/`type` == `relationship`
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
            let Ok(mut val) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) else {
                continue;
            };
            let Some(obj) = val.as_object_mut() else {
                continue;
            };
            let rel_matches = obj
                .get("relationship")
                .or_else(|| obj.get("type"))
                .and_then(|v| v.as_str())
                == Some(relationship);
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
            push_ledger(
                &mut self.ledger.lock(),
                format!(
                    "INVALIDATE_EDGE|{}|{}|{}|{}|{}",
                    source_id, target_id, relationship, invalid_at, tx_now
                ),
            );
        }
        updated
    }

    /// Atomically SUPERSEDE a prior edge with a new one (CONCEPT:KG-2.251) under the
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
}

impl GraphCore {
    pub fn new() -> Self {
        GraphCore {
            topo: RwLock::new(Topology::default()),
            node_properties: DashMap::new(),
            edge_properties: DashMap::new(),
            ledger: Mutex::new(Vec::new()),
            semantic_store: RwLock::new(crate::compute::semantic::SemanticStore::new()),
            dirty: std::sync::atomic::AtomicBool::new(true),
            version: std::sync::atomic::AtomicU64::new(0),
            changes: ChangeNotifier::default(),
            ontology_index: RwLock::new(None),
            label_index: RwLock::new(None),
            property_index: RwLock::new(None),
            // CONCEPT:EG-084 — cold JSONPath path-index; built lazily on first use.
            path_index: RwLock::new(None),
            index_manager: crate::index::IndexManager::with_default_indexes(),
            read_through: RwLock::new(None),
            #[cfg(feature = "result-cache")]
            result_cache: crate::result_cache::ResultCache::new(),
        }
    }

    /// The version-keyed query-result cache (CONCEPT:KG-2.233). The query handlers
    /// consult it before executing a read and populate it after a miss; correctness
    /// rests on `version()` keying (a write bumps the version, retiring every prior
    /// result). See `crate::result_cache`.
    #[cfg(feature = "result-cache")]
    pub fn result_cache(&self) -> &crate::result_cache::ResultCache {
        &self.result_cache
    }

    /// The unified secondary-index registry/seam (CONCEPT:KG-2.213). A planner
    /// (eg-query's pushdown, eg-plan's Filter leg) consults this ONE registry to
    /// ask "which index covers this predicate?" / "what indexes cover column X?"
    /// instead of hard-coding per-index checks. The label/property caches it routes
    /// to still live on this `GraphCore`, so their lazy + `mark_dirty` semantics
    /// are unchanged.
    pub fn indexes(&self) -> &crate::index::IndexManager {
        &self.index_manager
    }

    /// The change-notification fan-out (CONCEPT:EG-064). A server-layer GraphQL
    /// subscription carrier calls `core.changes().subscribe(&sink)` to receive a
    /// [`ChangeEvent`] on every committed write, turning a poll-only subscription
    /// into a real push (live query). The default build never subscribes, so the
    /// write path pays nothing.
    pub fn changes(&self) -> &ChangeNotifier {
        &self.changes
    }

    /// Attach a durable read-through (CONCEPT:KG-2.191). Called once at startup
    /// (only under redb-authoritative mode) so a node evicted from RAM is still
    /// served from redb on a RAM miss. A `GraphCore` with no read-through behaves
    /// exactly as before — a miss is a genuine absence.
    pub fn set_read_through(&self, rt: Arc<dyn crate::read_through::ReadThrough>) {
        *self.read_through.write() = Some(rt);
    }

    /// Mark this graph as changed since its last checkpoint (Phase C-C). Called by
    /// the dispatch after any successful write op and by the background decay sweep.
    pub fn mark_dirty(&self) {
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        // Bump the OCC write-version (CONCEPT:KG-2.180): every committed write —
        // single-op, coalesced batch, and (via the commit path) a multi-op txn —
        // flows through `mark_dirty`, so an in-flight staged transaction's validate
        // step observes any concurrent write that landed since it began. AcqRel so
        // the bump is visible to a commit that reads `version()` under the topo
        // write lock.
        let new_version = self
            .version
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        // Invalidate the lazy label index (CONCEPT:KG-2.176): a write may have
        // added/removed a node or rewritten a node's label, so the cached
        // `label → ids` map is stale and is rebuilt on the next label lookup.
        *self.label_index.write() = None;
        // Invalidate the lazy property index (CONCEPT:KG-2.199): a write may have
        // changed an indexed property value or added/removed a node, so the cached
        // `value → ids` maps are stale and are rebuilt (over the same demanded keys)
        // on the next `nodes_by_property` lookup.
        *self.property_index.write() = None;
        // Invalidate the lazy JSONPath path-index (CONCEPT:EG-084): a write may have
        // added/removed a node or changed a nested JSON value, so the cached
        // `path → value → ids` maps are stale and are rebuilt (over the same demanded
        // paths) on the next `nodes_by_json_path` / `nodes_with_json_path` lookup. This
        // is how the index is "maintained on the mutation path": every committed write
        // funnels through `mark_dirty` under the topology write guard, so the index is
        // never consistent-stale across a mutation.
        *self.path_index.write() = None;
        // CONCEPT:EG-064 — fan out a change notification (post-write version) to any
        // live subscribers (the GraphQL subscription carrier). A single relaxed
        // atomic load when there are none, so this is off the write hot path.
        self.changes.emit(new_version);
    }

    /// Invalidate cached query results for a CHANGE that landed elsewhere
    /// (CONCEPT:KG-2.233 — distributed cache coherence). A replica tailing the CDC
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
        // The label/property indexes are also derived state; retire them too so a
        // subsequent local read after a remote-applied change rebuilds them.
        *self.label_index.write() = None;
        *self.property_index.write() = None;
        // CONCEPT:EG-084 — the JSONPath path-index is derived state too; retire it.
        *self.path_index.write() = None;
        // CONCEPT:EG-064 — a replicated write must also wake local live-query
        // subscribers, so a subscription reflects remote writes, not just local ones.
        self.changes.emit(new_version);
    }

    /// Current OCC write-version (CONCEPT:KG-2.180). A staged transaction snapshots
    /// this at begin and an OCC commit re-reads it under the topology write lock to
    /// detect concurrent writes. Cheap atomic load — never on a read hot path.
    pub fn version(&self) -> u64 {
        self.version.load(std::sync::atomic::Ordering::Acquire)
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
        }
    }

    // ── Node CRUD (one-shot convenience over `txn`) ──────────────────────

    pub fn add_node(&self, node_id: String, properties_msgpack: Vec<u8>) {
        self.txn().add_node(node_id, properties_msgpack);
    }

    pub fn remove_node(&self, node_id: String) {
        self.txn().remove_node(node_id);
    }

    // ── Distribution-valued properties (CONCEPT:EG-086) ──────────────────
    //
    // A `Distribution` is just a tagged JSON object, so it round-trips through
    // the existing arbitrary-JSON property map with NO schema change — these
    // are typed convenience accessors over that convention, generalizing the
    // scalar `confidence` field into a full uncertainty distribution.

    /// Read a distribution-valued property `key` off `node_id` (CONCEPT:EG-086).
    /// Returns `None` if the node is absent, its blob is undecodable, the key is
    /// missing, or the stored JSON is not a valid `Distribution`.
    pub fn get_distribution(
        &self,
        node_id: &str,
        key: &str,
    ) -> Option<eg_types::Distribution> {
        let bytes = self.get_node_properties(node_id)?;
        let val = rmp_serde::from_slice::<serde_json::Value>(&bytes).ok()?;
        let field = val.as_object()?.get(key)?.clone();
        serde_json::from_value(field).ok()
    }

    /// Store `dist` as the property `key` on `node_id` (CONCEPT:EG-086), merging
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
            Some(bytes) => match rmp_serde::from_slice::<serde_json::Value>(&bytes) {
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

    /// One-shot serializable gated remove (CONCEPT:EG-045). See
    /// [`GraphTxn::remove_node_if`]; the decode → predicate re-check → remove runs
    /// under ONE topology write guard.
    pub fn remove_node_if(&self, node_id: &str, predicate: &eg_types::RowPredicate) -> bool {
        self.txn().remove_node_if(node_id, predicate)
    }

    /// One-shot atomic compare-and-set over a single `txn` (CONCEPT:KG-2 backend-
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

    /// One-shot serializable gated compare-and-set (CONCEPT:EG-045). See
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

    /// Atomically claim the oldest pending node of `label` (CONCEPT:KG-2.303 —
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
            let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob) else {
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
            let val = rmp_serde::from_slice::<serde_json::Value>(&blob).ok()?;
            Some((id, val))
        } else {
            None
        }
    }

    /// Atomically append one published message to EACH of `queues` under ONE
    /// topology write guard (CONCEPT:EG-275 — message-broker enqueue on top of the
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
            let msg_props = serde_json::json!({
                "type": crate::broker::queue_msg_label(q),
                "status": "pending",
                "seq": next,
                "exchange": exchange,
                "routing_key": routing_key,
                "payload": payload_hex,
            });
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

    /// One-shot non-destructive edge invalidation (CONCEPT:KG-2.251). See
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

    /// One-shot atomic edge supersession (CONCEPT:KG-2.251). See
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
    /// matches `label`; `limit == 0` means no cap. Scans in-engine (cheap,
    /// in-memory) but bounds the returned payload, so a `MATCH (n:Label) … LIMIT k`
    /// caller no longer materializes every node's properties over the wire.
    pub fn get_nodes_by_label(&self, label: &str, limit: usize) -> Vec<(String, Vec<u8>)> {
        // CONCEPT:KG-2.176 — consult the lazy `label → ids` index so a label
        // lookup is an O(1) map hit instead of a full DashMap scan that
        // deserializes every node. The cached map is invalidated by `mark_dirty()`
        // after any successful write (the same dirty flag the checkpoint uses), so
        // it never serves a stale view across a mutation.
        {
            let guard = self.label_index.read();
            if let Some(idx) = guard.as_ref() {
                return Self::collect_by_label(idx, &self.node_properties, label, limit);
            }
        }
        let built = self.build_label_index();
        let out = Self::collect_by_label(&built, &self.node_properties, label, limit);
        *self.label_index.write() = Some(built);
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
        limit: usize,
    ) -> Vec<(String, Vec<u8>)> {
        let mut out: Vec<(String, Vec<u8>)> = Vec::new();
        let Some(ids) = index.get(label) else {
            return out;
        };
        for id in ids {
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
    /// (CONCEPT:KG-2.176): each node id is filed under EVERY label it carries.
    /// The label set is read from exactly the fields `get_nodes_by_label` matched
    /// before this index existed, so the two never diverge:
    ///   * `type` (canonical), `node_type` (the field the Python client writes —
    ///     graph_compute normalises `type` ⇒ `node_type` on read-back, so the
    ///     index MUST honour both or a label-scoped MATCH under-returns every
    ///     node_type-keyed node), `label`, and the multi-valued `labels` array.
    fn build_label_index(&self) -> HashMap<String, Vec<String>> {
        let mut index: HashMap<String, Vec<String>> = HashMap::new();
        for entry in self.node_properties.iter() {
            let Ok(val) = rmp_serde::from_slice::<serde_json::Value>(entry.value().as_slice())
            else {
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

    // ── secondary property index (CONCEPT:KG-2.199) ──────────────────────────

    /// Node ids whose property `key` equals `value`, via the bounded, demand-driven
    /// secondary property index (CONCEPT:KG-2.199). Equality only. The first call
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
        // Slow path: build/extend the index to cover `key` (bounded), then answer.
        let mut guard = self.property_index.write();
        let idx = guard.get_or_insert_with(PropertyIndex::default);
        self.ensure_key_indexed(idx, key)?;
        Some(
            idx.keys
                .get(key)
                .and_then(|by_value| by_value.get(value).cloned())
                .unwrap_or_default(),
        )
    }

    /// Composite equality lookup (CONCEPT:KG-2.199): node ids matching EVERY
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
            let other: std::collections::HashSet<&String> = s.iter().collect();
            acc.retain(|id| other.contains(id));
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
        let mut by_value: HashMap<String, Vec<String>> = HashMap::new();
        for entry in self.node_properties.iter() {
            let Ok(val) = rmp_serde::from_slice::<serde_json::Value>(entry.value().as_slice())
            else {
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
    fn property_value_key(v: &serde_json::Value) -> Option<String> {
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

    // ── inverted JSONPath path-index (CONCEPT:EG-084) ─────────────────────────

    /// Node ids whose JSONPath `path` resolves to a scalar equal to `value`, via the
    /// bounded, demand-driven inverted path-index (CONCEPT:EG-084). This is the
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
        let mut guard = self.path_index.write();
        let idx = guard.get_or_insert_with(PathIndex::default);
        self.ensure_json_path_indexed(idx, path)?;
        Some(
            idx.by_value
                .get(path)
                .and_then(|by_value| by_value.get(value).cloned())
                .unwrap_or_default(),
        )
    }

    /// Node ids for which JSONPath `path` resolves to ANY value — the EXISTENCE set
    /// (CONCEPT:EG-084), the index-accelerated backing for `props @> …` / `jsonb_path_query`
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
        let mut guard = self.path_index.write();
        let idx = guard.get_or_insert_with(PathIndex::default);
        self.ensure_json_path_indexed(idx, path)?;
        Some(idx.present.get(path).cloned().unwrap_or_default())
    }

    /// Ensure `path` is present in the JSONPath index, honouring the bound
    /// (CONCEPT:EG-084). `Some(())` if the path is (now) indexed, `None` if the cap is
    /// full and the path is not already present (caller full-scans). Pre-seeds any paths
    /// named in `EPISTEMIC_GRAPH_INDEXED_JSON_PATHS` on the first build.
    #[allow(clippy::map_entry)]
    fn ensure_json_path_indexed(&self, idx: &mut PathIndex, path: &str) -> Option<()> {
        let cap = Self::max_indexed_json_paths();
        if idx.by_value.is_empty() && idx.present.is_empty() {
            for seed in Self::seed_indexed_json_paths() {
                if idx.by_value.len() >= cap || idx.by_value.contains_key(&seed) {
                    continue;
                }
                let (by_value, present) = self.build_json_path_maps(&seed);
                idx.by_value.insert(seed.clone(), by_value);
                idx.present.insert(seed, present);
            }
        }
        if idx.by_value.contains_key(path) {
            return Some(());
        }
        if idx.by_value.len() >= cap {
            return None;
        }
        let (by_value, present) = self.build_json_path_maps(path);
        idx.by_value.insert(path.to_string(), by_value);
        idx.present.insert(path.to_string(), present);
        Some(())
    }

    /// Scan the node store once and build, for one JSONPath, both the `value → ids`
    /// equality map (over the canonical scalar form of each matched leaf) and the
    /// existence `ids` set (CONCEPT:EG-084). A malformed path yields empty maps.
    fn build_json_path_maps(
        &self,
        path: &str,
    ) -> (HashMap<String, Vec<String>>, Vec<String>) {
        let mut by_value: HashMap<String, Vec<String>> = HashMap::new();
        let mut present: Vec<String> = Vec::new();
        let Some(segs) = crate::jsonpath::parse_path(path) else {
            return (by_value, present);
        };
        for entry in self.node_properties.iter() {
            let Ok(val) = rmp_serde::from_slice::<serde_json::Value>(entry.value().as_slice())
            else {
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
    /// (`EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS`, default 64) (CONCEPT:EG-084).
    fn max_indexed_json_paths() -> usize {
        std::env::var("EPISTEMIC_GRAPH_MAX_INDEXED_JSON_PATHS")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_MAX_INDEXED_JSON_PATHS)
    }

    /// JSONPaths to pre-seed into the index on first build
    /// (`EPISTEMIC_GRAPH_INDEXED_JSON_PATHS`, comma-separated) (CONCEPT:EG-084).
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

    /// Lexical classification gate (CONCEPT:EG-010): find every capability term
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
            let Ok(val) = rmp_serde::from_slice::<serde_json::Value>(entry.value().as_slice())
            else {
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
        // RAM miss. Under redb-authoritative mode a durable node may have been
        // evicted from RAM to bound memory (CONCEPT:KG-2.191); fetch its stored
        // blob from the durable tier so an evicted node still reads back correctly.
        // The read-through is only ever set under authoritative mode, so this is a
        // no-op (genuine absence) in the default model — behavior unchanged.
        self.read_through_get(node_id)
    }

    /// Consult the durable read-through on a RAM miss (CONCEPT:KG-2.191). Returns
    /// the node's stored property blob from the durable tier, or `None` when no
    /// read-through is attached (default model) or the node is genuinely absent.
    /// Kept lock-scoped: the read-through guard is cloned out and released BEFORE
    /// the (possibly blocking) durable point-read so it never holds the lock across
    /// I/O. Serve-only — it does NOT repopulate RAM, so a full scan over evicted
    /// nodes cannot re-grow the resident set past the cap (memory stays bounded).
    fn read_through_get(&self, node_id: &str) -> Option<Vec<u8>> {
        let rt = self.read_through.read().clone()?;
        rt.read_node_blob(node_id)
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

    pub fn get_edge_properties(&self, source_id: &str, target_id: &str) -> Vec<Vec<u8>> {
        self.edge_properties
            .get(&(source_id.to_string(), target_id.to_string()))
            .map(|v| v.iter().map(|a| (**a).clone()).collect())
            .unwrap_or_default()
    }

    pub fn edge_count(&self) -> usize {
        self.edge_properties.iter().map(|e| e.value().len()).sum()
    }

    /// Approximate resident RAM this graph holds, in bytes (CONCEPT:KG-2.234 —
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

    // ── Serialization ────────────────────────────────────────────────────

    /// Owned, serializable snapshot of this graph's persistent state. Cheap
    /// relative to serialization (clones the node/edge/ledger/semantic data), so a
    /// checkpoint takes it under a BRIEF lock and serializes OFF the lock.
    /// (CONCEPT:KG-2.8 — non-blocking checkpoint, A1)
    pub fn snapshot(&self) -> GraphSnapshot {
        // Hold the topology read lock for the duration: every mutation goes through
        // a write txn (topo.write()), so a read guard excludes all writers and the
        // node/edge/ledger views below are a single consistent point-in-time.
        let _topo = self.topo.read();
        // Zero-copy (Phase C-A): clone the Arc POINTERS, not the property bytes —
        // turns the A1-residual ~3s lock-held deep clone of a 450MB graph into a
        // ~µs pointer copy, and removes the transient memory doubling.
        GraphSnapshot {
            nodes: self.get_nodes_arc(),
            edges: self.get_edges_arc(),
            ledger: self.ledger.lock().clone(),
            semantic_store: self.semantic_store.read().clone(),
        }
    }

    /// Serialize the whole graph to MessagePack. Now encodes the typed snapshot
    /// directly (A3) instead of round-tripping every property byte through
    /// `serde_json::Value`; the on-disk shape is unchanged so `from_msgpack` reads
    /// pre- and post-change files alike.
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
        // Drop every cached query result (CONCEPT:KG-2.233): `clear` (and `hibernate`,
        // which reuses it) wipes the graph WITHOUT bumping `version`, so the
        // version-keyed cache must be invalidated directly or a post-wipe lookup at the
        // unchanged version could serve a stale result.
        #[cfg(feature = "result-cache")]
        self.result_cache.invalidate_all();
    }

    /// Hibernate this graph's in-memory state (CONCEPT:KG-2.224 — cold-tenant
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

    /// Offload this graph's whole in-RAM state to a cold tier (CONCEPT:KG-2.233),
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

    /// Rehydrate this graph from the cold tier (CONCEPT:KG-2.233): fetch its
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
        let graph_map: HashMap<String, serde_json::Value> =
            rmp_serde::from_slice(msgpack).map_err(|e| e.to_string())?;

        // Reset + reload under ONE write txn — the whole load is atomic w.r.t. any
        // concurrent reader/writer, and replaying through the txn avoids re-locking
        // per node/edge.
        let mut txn = self.txn();
        txn.topo.graph.clear();
        txn.topo.node_map.clear();
        self.node_properties.clear();
        self.edge_properties.clear();
        self.ledger.lock().clear();

        if let Some(nodes_val) = graph_map.get("nodes") {
            let nodes: Vec<(String, Vec<u8>)> =
                serde_json::from_value(nodes_val.clone()).map_err(|e| e.to_string())?;
            for (node_id, props) in nodes {
                txn.add_node(node_id, props);
            }
        }

        if let Some(edges_val) = graph_map.get("edges") {
            let edges: Vec<(String, String, Vec<u8>)> =
                serde_json::from_value(edges_val.clone()).map_err(|e| e.to_string())?;
            for (src, tgt, props) in edges {
                let _ = txn.add_edge(src, tgt, props);
            }
        }

        if let Some(ledger_val) = graph_map.get("ledger") {
            let ledger: Vec<String> =
                serde_json::from_value(ledger_val.clone()).map_err(|e| e.to_string())?;
            *self.ledger.lock() = ledger;
        }

        if let Some(store_val) = graph_map.get("semantic_store") {
            let store: crate::compute::semantic::SemanticStore =
                serde_json::from_value(store_val.clone()).map_err(|e| e.to_string())?;
            *self.semantic_store.write() = store;
        }

        Ok(())
    }

    // ── Ledger Operations ────────────────────────────────────────────────

    pub fn get_ledger(&self) -> Vec<String> {
        self.ledger.lock().clone()
    }

    pub fn clear_ledger(&self) {
        self.ledger.lock().clear();
    }

    pub fn apply_ledger(&self, transactions: Vec<String>) -> Result<(), String> {
        // Replay the whole batch under one write txn (atomic + no per-op re-lock).
        let mut txn = self.txn();
        for tx in transactions {
            let parts: Vec<&str> = tx.split('|').collect();
            if parts.is_empty() {
                continue;
            }
            match parts[0] {
                "ADD_NODE" if parts.len() >= 3 => {
                    txn.add_node(parts[1].to_string(), parts[2].as_bytes().to_vec());
                }
                "ADD_EDGE" if parts.len() >= 4 => {
                    let _ = txn.add_edge(
                        parts[1].to_string(),
                        parts[2].to_string(),
                        parts[3].as_bytes().to_vec(),
                    );
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
        let id_set: std::collections::HashSet<&String> = node_ids.iter().collect();

        // Copy matching nodes (those that actually exist).
        for nid in node_ids {
            if topo.node_map.contains_key(nid) {
                let new_idx = view.graph.add_node(nid.clone());
                view.node_map.insert(nid.clone(), new_idx);
                if let Some(props) = self.node_properties.get(nid) {
                    view.node_properties.insert(nid.clone(), props.clone());
                }
            }
        }

        // Copy edges where both endpoints made it into the subgraph.
        for entry in self.edge_properties.iter() {
            let (src, tgt) = entry.key();
            if id_set.contains(src) && id_set.contains(tgt) {
                if let (Some(&s), Some(&t)) = (view.node_map.get(src), view.node_map.get(tgt)) {
                    for props in entry.value() {
                        view.graph.add_edge(s, t, format!("{}:{}", src, tgt));
                        view.edge_properties
                            .entry((src.clone(), tgt.clone()))
                            .or_default()
                            .push(props.clone());
                    }
                }
            }
        }

        view
    }

    // ── Read-Only Compute Snapshots (CONCEPT:KG-2.51) ────────────────────
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
        }
    }

    /// An [`analysis_snapshot`](Self::analysis_snapshot) paired with the OCC
    /// `version()` read UNDER the same topology read lock (CONCEPT:KG-2.233). Taking
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
            dirty: std::sync::atomic::AtomicBool::new(true),
            // Fork starts a fresh OCC version line (CONCEPT:KG-2.180) — it is a new
            // independent graph; any txn against the fork baselines from 0.
            version: std::sync::atomic::AtomicU64::new(0),
            // A fork is a fresh, detached graph with its own (empty) subscriber set
            // (CONCEPT:EG-064) — subscriptions attach to the live registered core.
            changes: ChangeNotifier::default(),
            // Fork starts with cold lazy indexes; rebuilt lazily on first use.
            ontology_index: RwLock::new(None),
            label_index: RwLock::new(None),
            property_index: RwLock::new(None),
            // CONCEPT:EG-084 — fork starts with a cold JSONPath path-index too.
            path_index: RwLock::new(None),
            // A fork gets its own default index registry (the registry is fixed
            // metadata, not graph state) (CONCEPT:KG-2.213).
            index_manager: crate::index::IndexManager::with_default_indexes(),
            // A fork is a fresh, detached graph (not registered, not backed by the
            // durable tier), so it carries no read-through (CONCEPT:KG-2.191).
            read_through: RwLock::new(None),
            // A fork starts with an empty result cache (its own version line).
            #[cfg(feature = "result-cache")]
            result_cache: crate::result_cache::ResultCache::new(),
        }
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

    // ── Repository Parsing ───────────────────────────────────────────────

    pub fn parse_repository(&self, root_path: &str) -> Result<(), String> {
        let root = std::path::Path::new(root_path);
        if !root.exists() {
            return Err(format!("Path '{}' does not exist", root_path));
        }
        let mut files = Vec::new();
        walk_dir_recursive(root, &mut files);

        for path in files {
            if let Ok(relative) = path.strip_prefix(root) {
                let rel_str = relative.to_string_lossy().to_string();

                let file_props = format!("{{\"type\": \"file\", \"path\": \"{}\"}}", rel_str);
                self.add_node(rel_str.clone(), file_props.into_bytes());

                if let Ok(mut file) = std::fs::File::open(&path) {
                    let mut content = String::new();
                    if file.read_to_string(&mut content).is_ok() {
                        let lines: Vec<&str> = content.lines().collect();
                        for (idx, line) in lines.iter().enumerate() {
                            let trimmed = line.trim();
                            self.parse_code_line(trimmed, &rel_str, idx + 1);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn parse_code_line(&self, trimmed: &str, rel_str: &str, line_num: usize) {
        // Python/JS class definition
        if trimmed.starts_with("class ") {
            if let Some(class_name) = trimmed.split_whitespace().nth(1) {
                let clean_name = class_name
                    .split('(')
                    .next()
                    .unwrap_or("")
                    .split(':')
                    .next()
                    .unwrap_or("")
                    .trim();
                if !clean_name.is_empty() {
                    let node_id = format!("{}::{}", rel_str, clean_name);
                    let props = format!(
                        "{{\"type\": \"class\", \"file\": \"{}\", \"line\": {}}}",
                        rel_str, line_num
                    );
                    self.add_node(node_id.clone(), props.into_bytes());
                    let edge_props = "{\"relationship\": \"contains\"}".to_string();
                    let _ = self.add_edge(rel_str.to_string(), node_id, edge_props.into_bytes());
                }
            }
        }

        // Python function definition
        if trimmed.starts_with("def ") {
            if let Some(func_name) = trimmed.split_whitespace().nth(1) {
                let clean_name = func_name
                    .split('(')
                    .next()
                    .unwrap_or("")
                    .split(':')
                    .next()
                    .unwrap_or("")
                    .trim();
                if !clean_name.is_empty() {
                    let node_id = format!("{}::{}", rel_str, clean_name);
                    let props = format!(
                        "{{\"type\": \"function\", \"file\": \"{}\", \"line\": {}}}",
                        rel_str, line_num
                    );
                    self.add_node(node_id.clone(), props.into_bytes());
                    let edge_props = "{\"relationship\": \"contains\"}".to_string();
                    let _ = self.add_edge(rel_str.to_string(), node_id, edge_props.into_bytes());
                }
            }
        }

        // JavaScript/TypeScript function
        if trimmed.starts_with("function ") {
            if let Some(func_name) = trimmed.split_whitespace().nth(1) {
                let clean_name = func_name.split('(').next().unwrap_or("").trim();
                if !clean_name.is_empty() {
                    let node_id = format!("{}::{}", rel_str, clean_name);
                    let props = format!(
                        "{{\"type\": \"function\", \"file\": \"{}\", \"line\": {}}}",
                        rel_str, line_num
                    );
                    self.add_node(node_id.clone(), props.into_bytes());
                    let edge_props = "{\"relationship\": \"contains\"}".to_string();
                    let _ = self.add_edge(rel_str.to_string(), node_id, edge_props.into_bytes());
                }
            }
        }
    }

    // ── VF2 Subgraph Matching ────────────────────────────────────────────

    pub fn vf2_subgraph_match(&self, pattern: &GraphView) -> Vec<HashMap<String, String>> {
        // Match against a consistent read view so the O(V·E) backtracking never
        // holds a live lock.
        let host = self.analysis_snapshot();
        vf2_match_views(&host, pattern)
    }

    /// The least-recently-added node ids that would be evicted to bring the graph
    /// down to `max_nodes` — the same set (and order) `evict_lru` removes, but
    /// WITHOUT dropping them (CONCEPT:KG-2.191). Used by the redb-authoritative
    /// eviction path, which must confirm each candidate is durable in redb BEFORE
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
        indexed.sort_by_key(|(_, idx)| *idx);
        indexed
            .into_iter()
            .take(to_evict)
            .map(|(id, _)| id)
            .collect()
    }

    /// Evict nodes down to `max_nodes` by removing the least-recently-added.
    ///
    /// CONCEPT:KG-2.16 — Memory pressure defense. When the in-memory graph
    /// grows beyond `max_nodes`, this method removes the oldest nodes (by
    /// insertion order in `node_map`) until the count is at or below the cap.
    /// Returns the number of evicted nodes.
    pub fn evict_lru(&self, max_nodes: usize) -> usize {
        // Snapshot the id↔index map under the read lock, then remove off-lock.
        let mut indexed: Vec<(String, NodeIndex)> = {
            let topo = self.topo.read();
            if topo.node_map.len() <= max_nodes {
                return 0;
            }
            topo.node_map.iter().map(|(k, &v)| (k.clone(), v)).collect()
        };
        let to_evict = indexed.len() - max_nodes;

        // Nodes with the lowest NodeIndex were inserted earliest → approximate LRU.
        indexed.sort_by_key(|(_, idx)| *idx);

        let evict_ids: Vec<String> = indexed
            .into_iter()
            .take(to_evict)
            .map(|(id, _)| id)
            .collect();

        for node_id in &evict_ids {
            self.remove_node(node_id.clone());
        }

        evict_ids.len()
    }

    // ── Ebbinghaus Temporal Decay (CONCEPT:KG-2.16) ──────────────────────

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
                    if let Ok(mut val) = rmp_serde::from_slice::<serde_json::Value>(&bytes) {
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
                        if let Ok(mut val) =
                            rmp_serde::from_slice::<serde_json::Value>(b.as_slice())
                        {
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
                if let Ok(mut val) = rmp_serde::from_slice::<serde_json::Value>(&bytes) {
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

pub fn walk_dir_recursive(dir: &std::path::Path, files: &mut Vec<std::path::PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let name = path.file_name().unwrap_or_default().to_string_lossy();
                if !name.starts_with('.') && name != "node_modules" && name != "target" {
                    walk_dir_recursive(&path, files);
                }
            } else {
                let ext = path.extension().unwrap_or_default().to_string_lossy();
                if ext == "py"
                    || ext == "js"
                    || ext == "ts"
                    || ext == "rs"
                    || ext == "go"
                    || ext == "tsx"
                    || ext == "jsx"
                    || ext == "mjs"
                {
                    files.push(path);
                }
            }
        }
    }
}

/// VF2 subgraph match of `pattern` against an already-materialized `host`
/// `GraphView` (vs [`GraphCore::vf2_subgraph_match`], which snapshots its own
/// live graph first). Lets an off-lock caller — e.g. the Cypher exec
/// (CONCEPT:KG-2.179), which already holds the `analysis_snapshot()` view — reuse
/// the exact same matcher without re-snapshotting. Each result maps a pattern
/// node id → the host node id it bound to.
pub fn vf2_match_views(host: &GraphView, pattern: &GraphView) -> Vec<HashMap<String, String>> {
    let mut matches = Vec::new();
    let pattern_nodes: Vec<String> = pattern.node_map.keys().cloned().collect();
    if pattern_nodes.is_empty() {
        return matches;
    }
    let mut current_mapping = HashMap::new();
    let mut mapped_targets = std::collections::HashSet::new();
    backtrack_match(
        host,
        0,
        &pattern_nodes,
        &mut current_mapping,
        &mut mapped_targets,
        pattern,
        &mut matches,
    );
    matches
}

fn backtrack_match(
    host: &GraphView,
    pattern_node_idx: usize,
    pattern_nodes: &[String],
    current_mapping: &mut HashMap<String, String>,
    mapped_targets: &mut std::collections::HashSet<String>,
    pattern: &GraphView,
    matches: &mut Vec<HashMap<String, String>>,
) {
    if pattern_node_idx == pattern_nodes.len() {
        matches.push(current_mapping.clone());
        return;
    }

    let p_node = &pattern_nodes[pattern_node_idx];

    for t_node in host.node_map.keys() {
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
                matches,
            );

            current_mapping.remove(p_node);
            mapped_targets.remove(t_node);
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
    let p_val: serde_json::Value = match rmp_serde::from_slice(p_msgpack) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let t_val: serde_json::Value = match rmp_serde::from_slice(t_msgpack) {
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

    // ── read-your-own-writes overlay (CONCEPT:EG-049) ────────────────────────

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

    // ── Distribution-valued properties (CONCEPT:EG-086) ──────────────────

    #[test]
    fn distribution_property_roundtrips() {
        let core = GraphCore::new();
        core.add_node("m1".into(), props(serde_json::json!({"type": "Measurement"})));
        let dist = eg_types::Distribution::Gaussian {
            mean: 3.5,
            std: 0.75,
        };
        assert!(core.set_distribution("m1", "reading", &dist));
        let back = core.get_distribution("m1", "reading").expect("stored dist");
        assert_eq!(back, dist);
        // Set merges — the pre-existing `type` key survives.
        let blob = core.get_node_properties("m1").unwrap();
        let obj = rmp_serde::from_slice::<serde_json::Value>(&blob).unwrap();
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

    // ── secondary property index (CONCEPT:KG-2.199) ──────────────────────────

    /// Serializes the env-mutating property-index tests (env is process-global and
    /// Rust runs tests on parallel threads).
    static PROP_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

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

    // ── non-destructive edge invalidation / supersession (CONCEPT:KG-2.251) ──

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

    // ── inverted JSONPath path-index (CONCEPT:EG-084) ─────────────────────────

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
        assert_eq!(core.nodes_by_json_path("$.meta.lang", "go").unwrap(), vec!["n2"]);
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
        assert_eq!(core.nodes_by_json_path("$.meta.lang", "go").unwrap(), vec!["n2"]);

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
                crate::jsonpath::path_contains(&v, "$", &serde_json::json!({"meta": {"lang": "rust"}}))
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

    /// CONCEPT:EG-064 — a committed write emits a `ChangeEvent` carrying the bumped
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

    /// CONCEPT:KG-2.191 — the read-through seam, exercised purely in eg-core with a
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
        // CONCEPT:EG-045 — the serializable gate only mutates a node whose CURRENT
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
        // CONCEPT:EG-045 — `id` is injected so a predicate may reference it.
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
            props(serde_json::json!({"type": "CALLS"})),
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

    #[test]
    fn from_msgpack_reads_legacy_serde_json_value_format() {
        // Backward compat: reproduce the PRE-A3 on-disk shape (values round-tripped
        // through serde_json::Value before rmp encoding) and assert from_msgpack
        // still loads it — so existing __commons__.mp snapshots keep loading.
        let g = GraphCore::new();
        let p = props(serde_json::json!({"type": "Code", "v": 42}));
        g.add_node("a".to_string(), p.clone());
        let _ = g.add_edge(
            "a".to_string(),
            "a".to_string(),
            props(serde_json::json!({"type": "SELF"})),
        );

        let mut legacy = std::collections::HashMap::new();
        legacy.insert(
            "nodes".to_string(),
            serde_json::to_value(g.get_nodes()).unwrap(),
        );
        legacy.insert(
            "edges".to_string(),
            serde_json::to_value(g.get_edges()).unwrap(),
        );
        legacy.insert(
            "ledger".to_string(),
            serde_json::to_value(g.get_ledger()).unwrap(),
        );
        legacy.insert(
            "semantic_store".to_string(),
            serde_json::to_value(&*g.semantic_store.read()).unwrap(),
        );
        let legacy_bytes = rmp_serde::to_vec_named(&legacy).unwrap();

        let g2 = GraphCore::new();
        g2.from_msgpack(&legacy_bytes).unwrap();
        assert_eq!(g2.node_count(), 1);
        assert_eq!(g2.get_node_properties("a"), Some(p));
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

    // ── label index (CONCEPT:KG-2.176) ──────────────────────────────────

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
                            assert!(rmp_serde::from_slice::<serde_json::Value>(&b).is_ok());
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
}
