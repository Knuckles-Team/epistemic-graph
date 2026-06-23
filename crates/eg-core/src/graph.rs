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
        let log = format!("ADD_NODE|{}|{}", node_id, hex::encode(&properties_msgpack));
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
            hex::encode(&properties_msgpack)
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
        let log = format!("CAS_NODE|{}|{}", node_id, hex::encode(&reenc));
        self.node_properties
            .insert(node_id.to_string(), Arc::new(reenc));
        push_ledger(&mut self.ledger.lock(), log);
        true
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
            ontology_index: RwLock::new(None),
            label_index: RwLock::new(None),
        }
    }

    /// Mark this graph as changed since its last checkpoint (Phase C-C). Called by
    /// the dispatch after any successful write op and by the background decay sweep.
    pub fn mark_dirty(&self) {
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        // Invalidate the lazy label index (CONCEPT:KG-2.176): a write may have
        // added/removed a node or rewritten a node's label, so the cached
        // `label → ids` map is stale and is rebuilt on the next label lookup.
        *self.label_index.write() = None;
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
        self.node_properties.get(node_id).map(|a| (**a).clone())
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
            // Fork starts with cold lazy indexes; rebuilt lazily on first use.
            ontology_index: RwLock::new(None),
            label_index: RwLock::new(None),
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
        let mut matches = Vec::new();
        let pattern_nodes: Vec<String> = pattern.node_map.keys().cloned().collect();
        if pattern_nodes.is_empty() {
            return matches;
        }
        let mut current_mapping = HashMap::new();
        let mut mapped_targets = std::collections::HashSet::new();

        backtrack_match(
            &host,
            0,
            &pattern_nodes,
            &mut current_mapping,
            &mut mapped_targets,
            pattern,
            &mut matches,
        );
        matches
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
}
