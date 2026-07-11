// CONCEPT:EG-KG.sharding.multi-tenant-registry — Multi-Tenant Graph Registry
//
// Manages named graphs with lifecycle operations. The `__commons__` graph
// is always present as the shared, world-readable/writable commons graph
// (every authenticated agent can read/write it). It is NOT a message bus —
// it is a default shared graph; see isolation.rs.
//
// ## Catalog vs resident (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3)
//
// To represent millions of tenants/agents/graphs the registry splits "the engine
// knows about this graph" from "this graph has a resident `GraphCore`":
//
//   * `catalog` — a lightweight durable-cardinality record (name, type, owner) per
//     REGISTERED graph. Costs a `DashMap` row, never a `GraphCore`. Populated
//     eagerly by `create_graph` and (at boot, under a lazy-startup backend) by
//     `register_catalog_only` from the durable `graph_meta` table alone — no
//     node/edge data is read to populate it.
//   * `graphs` — the BOUNDED hot-context cache: only catalog entries that have
//     actually been materialized (a real `Arc<GraphCore>`, live topology/props/
//     indexes). `open_lazy` promotes a catalog-only entry into this map on first
//     access, replaying its durable material through the SAME `GraphMaterializer`
//     seam `PersistenceBackend` implements over redb (mirrors `ReadThroughFactory`).
//     The server layer (`server::persistence::cold_offload::admit_capacity`)
//     bounds this map's size and evicts the coldest entry back to catalog-only
//     before admitting a new one — durability-gated, so a graph is never lost,
//     only evicted; `__commons__` is pinned and never evicted.
//
// Every existing registry API (`get`/`get_mut`/`create_graph`/`delete_graph`/
// `all_entries`) keeps operating on the RESIDENT set — a small/eager deployment
// (catalog == resident always, because nothing is ever evicted) sees byte-for-byte
// unchanged behavior. `exists`/`list` are widened to the catalog (a graph is
// "known" whether or not it is currently hot), which is a pure correctness
// improvement with no behavior change when catalog == resident.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;

use crate::graph::GraphCore;
use crate::protocol::GraphType;

/// Metadata for a registered graph.
///
/// The write-ahead log is NOT held here: WAL file I/O is owned by the single
/// off-reactor `WalService` (in `eg-server`), keyed by the graph's sanitized
/// file name, so durable mutations append without any per-entry lock (Phase B3).
#[derive(Debug, Clone)]
pub struct GraphEntry {
    pub name: String,
    pub graph_type: GraphType,
    pub core: Arc<GraphCore>,
    pub owner: Option<String>,
}

/// A lightweight catalog row (CONCEPT:EG-KG.sharding.lazy-graph-catalog) — the cost of a
/// REGISTERED graph with no resident `GraphCore`. Millions of these cost a
/// `DashMap` row each, not a topology/props/semantic-store/ledger/cache.
#[derive(Debug, Clone)]
pub struct CatalogRecord {
    pub name: String,
    pub graph_type: GraphType,
    pub owner: Option<String>,
}

/// A graph's durable material, replayed into a freshly constructed `GraphCore` on
/// lazy first-open (CONCEPT:EG-KG.sharding.lazy-graph-catalog). Shape-compatible with the facade's
/// `GraphDump` but defined HERE (eg-core, below the facade) so the registry never
/// depends on a facade type — mirrors the `ReadThrough`/`ReadThroughFactory` DAG
/// discipline.
#[derive(Debug, Clone, Default)]
pub struct GraphMaterial {
    pub nodes: Vec<(String, Vec<u8>)>,
    pub edges: Vec<(String, String, Vec<u8>)>,
    pub semantic: Vec<u8>,
}

/// Builds a graph's durable material for a lazy first-open (CONCEPT:EG-KG.sharding.lazy-graph-catalog).
/// Implemented in the facade over the redb backend (reusing the existing
/// `read_graph_dump_blocking` per-graph rehydrate path) and injected into the
/// registry once at startup, only under redb-authoritative mode — mirrors
/// [`crate::read_through::ReadThroughFactory`]. `None` (the default) ⇒ no
/// materializer wired, so `open_lazy` simply constructs an empty core (the
/// rebuildable-cache model / a brand-new catalog entry with nothing durable yet).
pub trait GraphMaterializer: Send + Sync {
    /// Fetch `graph_name`'s durable material. `None` ⇒ nothing durable (a
    /// genuinely fresh graph, or a backend with no lazy-materialize support).
    fn materialize(&self, graph_name: &str) -> Option<GraphMaterial>;
}

/// Multi-tenant graph registry.
pub struct GraphRegistry {
    /// The bounded hot-context cache: only graphs with a materialized `GraphCore`.
    graphs: HashMap<String, GraphEntry>,
    /// The full catalog of every REGISTERED graph (CONCEPT:EG-KG.sharding.lazy-graph-catalog), resident or
    /// not. A `DashMap` (not a `HashMap`) so catalog-only registration
    /// (`register_catalog_only`) needs no registry write-lock — it is exactly as
    /// cheap under a shared `&self` as a resident-cache hit.
    catalog: DashMap<String, CatalogRecord>,
    /// Durable read-through factory (CONCEPT:EG-KG.storage.read-through-seam-exercised). Set once at startup ONLY
    /// under redb-authoritative mode; when present, every `GraphCore` the registry
    /// creates (and every existing one, via `attach_read_through_all`) gains a
    /// per-graph read-through so an evicted node still reads from redb. `None` (the
    /// default and always in the rebuildable-cache model) ⇒ no read-through wiring,
    /// behavior unchanged.
    read_through_factory: Option<Arc<dyn crate::read_through::ReadThroughFactory>>,
    /// Server-layer secondary-index factory (CONCEPT:EG-KG.storage.incremental-text /
    /// .incremental-temporal / .incremental-derived-owl). Set once at startup when the
    /// text/tsdb/owl features are active; when present, every `GraphCore` the registry
    /// creates (and every existing one) gains its per-graph text/temporal/derived-OWL
    /// indexes via [`GraphCore::register_index`], so a committed write batch maintains
    /// them incrementally. `None` (default) ⇒ no server indexes wired — behavior
    /// unchanged (mirrors `read_through_factory`).
    secondary_index_factory: Option<Arc<dyn crate::index::SecondaryIndexFactory>>,
    /// Durable-material factory for lazy first-open (CONCEPT:EG-KG.sharding.lazy-graph-catalog). Set once at
    /// startup, only under redb-authoritative mode, mirroring `read_through_factory`.
    materializer: Option<Arc<dyn GraphMaterializer>>,
}

impl Default for GraphRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphRegistry {
    /// Create a new registry with the `__commons__` graph pre-created.
    pub fn new() -> Self {
        let mut graphs = HashMap::new();
        graphs.insert(
            "__commons__".to_string(),
            GraphEntry {
                name: "__commons__".to_string(),
                graph_type: GraphType::Commons,
                core: Arc::new(GraphCore::new()),
                owner: None,
            },
        );
        let catalog = DashMap::new();
        catalog.insert(
            "__commons__".to_string(),
            CatalogRecord {
                name: "__commons__".to_string(),
                graph_type: GraphType::Commons,
                owner: None,
            },
        );
        GraphRegistry {
            graphs,
            catalog,
            read_through_factory: None,
            secondary_index_factory: None,
            materializer: None,
        }
    }

    /// Install the durable read-through factory and attach a per-graph read-through
    /// to every graph that ALREADY exists (CONCEPT:EG-KG.storage.read-through-seam-exercised) — the pre-created
    /// `__commons__` plus anything `load_all` recovered before this was set. Called
    /// once at startup under redb-authoritative mode; future `create_graph` calls
    /// pick the factory up automatically.
    pub fn set_read_through_factory(
        &mut self,
        factory: Arc<dyn crate::read_through::ReadThroughFactory>,
    ) {
        for entry in self.graphs.values() {
            entry.core.set_read_through(factory.for_graph(&entry.name));
        }
        self.read_through_factory = Some(factory);
    }

    /// Install the server-layer secondary-index factory and register the per-graph
    /// text/temporal/derived-OWL indexes onto every graph that ALREADY exists
    /// (CONCEPT:EG-KG.storage.incremental-text / .incremental-temporal / .incremental-derived-owl) —
    /// the pre-created `__commons__` plus anything `load_all` recovered. Called once at
    /// startup when the text/tsdb/owl features are active; future `create_graph` calls
    /// pick the factory up automatically. Mirrors [`set_read_through_factory`](Self::set_read_through_factory).
    pub fn set_secondary_index_factory(
        &mut self,
        factory: Arc<dyn crate::index::SecondaryIndexFactory>,
    ) {
        for entry in self.graphs.values() {
            for idx in factory.for_graph(&entry.name) {
                entry.core.register_index(idx);
            }
        }
        self.secondary_index_factory = Some(factory);
    }

    /// Install the durable-material factory for lazy first-open (CONCEPT:EG-KG.sharding.lazy-graph-catalog).
    /// Called once at startup, only under redb-authoritative mode; `open_lazy`
    /// picks it up automatically. Unlike `set_read_through_factory` /
    /// `set_secondary_index_factory` there is nothing to backfill onto existing
    /// entries — a materializer is only ever consulted at the moment a catalog-only
    /// graph is promoted to resident.
    pub fn set_materializer(&mut self, materializer: Arc<dyn GraphMaterializer>) {
        self.materializer = Some(materializer);
    }

    /// Create a new named graph — eagerly resident (the caller is about to use it).
    /// Errs if the name is already known, whether resident OR catalog-only (a
    /// lazily-registered-but-not-yet-opened graph is still "already exists").
    pub fn create_graph(
        &mut self,
        name: &str,
        graph_type: GraphType,
        owner: Option<String>,
    ) -> Result<(), String> {
        if self.graphs.contains_key(name) || self.catalog.contains_key(name) {
            return Err(format!("Graph '{}' already exists", name));
        }
        let core = Arc::new(GraphCore::new());
        // Transparently wire read-through for the new graph when authoritative
        // (CONCEPT:EG-KG.storage.read-through-seam-exercised), so a graph created after startup is eviction-safe too.
        if let Some(factory) = &self.read_through_factory {
            core.set_read_through(factory.for_graph(name));
        }
        // Wire the per-graph server-layer indexes (text/temporal/derived-OWL) when the
        // factory is installed, so a graph created after startup is index-maintained too.
        if let Some(factory) = &self.secondary_index_factory {
            for idx in factory.for_graph(name) {
                core.register_index(idx);
            }
        }
        self.catalog.insert(
            name.to_string(),
            CatalogRecord {
                name: name.to_string(),
                graph_type,
                owner: owner.clone(),
            },
        );
        self.graphs.insert(
            name.to_string(),
            GraphEntry {
                name: name.to_string(),
                graph_type,
                core,
                owner,
            },
        );
        Ok(())
    }

    /// Register a graph in the CATALOG ONLY — no `GraphCore` is constructed
    /// (CONCEPT:EG-KG.sharding.lazy-graph-catalog). Used by a lazy-startup backend to populate the
    /// registry from durable `graph_meta` rows without hydrating any node/edge
    /// data. A no-op if the name is already resident (never shadows a live core)
    /// or already cataloged (idempotent re-registration, e.g. a repeated boot
    /// scan). Takes `&self` — the catalog is a `DashMap`, so this needs no
    /// registry write-lock and is as cheap as a resident-cache hit.
    pub fn register_catalog_only(&self, name: &str, graph_type: GraphType, owner: Option<String>) {
        if self.graphs.contains_key(name) || self.catalog.contains_key(name) {
            return;
        }
        self.catalog.insert(
            name.to_string(),
            CatalogRecord {
                name: name.to_string(),
                graph_type,
                owner,
            },
        );
    }

    /// Lazily materialize a catalog-known graph's resident `GraphCore`
    /// (CONCEPT:EG-KG.sharding.lazy-graph-catalog — bounded hot-context cache). Returns `true` when the
    /// graph is resident afterward (either it already was, or it was just
    /// promoted from a catalog-only entry); `false` when `name` is genuinely
    /// unknown (neither resident nor cataloged). When a `GraphMaterializer` is
    /// installed, its durable material (nodes/edges/semantic store) is replayed
    /// via the SAME `add_node`/`add_edge` calls the eager boot path uses, so a
    /// lazily-opened graph is byte-identical to an eagerly-loaded one. The
    /// caller (`server::persistence::cold_offload::admit_capacity`) is
    /// responsible for bounded-cache admission BEFORE calling this.
    pub fn open_lazy(&mut self, name: &str) -> bool {
        if self.graphs.contains_key(name) {
            return true; // already resident (e.g. a race with another opener)
        }
        let rec = match self.catalog.get(name) {
            Some(r) => r.clone(),
            None => return false, // genuinely unknown
        };
        let core = Arc::new(GraphCore::new());
        if let Some(materializer) = &self.materializer {
            if let Some(material) = materializer.materialize(name) {
                for (node_id, props) in material.nodes {
                    core.add_node(node_id, props);
                }
                for (src, tgt, props) in material.edges {
                    let _ = core.add_edge(src, tgt, props);
                }
                if !material.semantic.is_empty() {
                    if let Ok(store) = rmp_serde::from_slice::<crate::compute::semantic::SemanticStore>(
                        &material.semantic,
                    ) {
                        *core.semantic_store.write() = store;
                    }
                }
            }
        }
        if let Some(factory) = &self.read_through_factory {
            core.set_read_through(factory.for_graph(name));
        }
        if let Some(factory) = &self.secondary_index_factory {
            for idx in factory.for_graph(name) {
                core.register_index(idx);
            }
        }
        self.graphs.insert(
            name.to_string(),
            GraphEntry {
                name: name.to_string(),
                graph_type: rec.graph_type,
                core,
                owner: rec.owner,
            },
        );
        true
    }

    /// Evict a resident graph back to catalog-only (CONCEPT:EG-KG.sharding.lazy-graph-catalog —
    /// bounded hot-context cache). REMOVES the whole `GraphEntry`/`GraphCore` from
    /// the resident map — not merely hibernating its content like
    /// `GraphCore::hibernate` — so the freed memory includes the topology/props/
    /// semantic-store/cache structures themselves, matching the catalog-row-vs-
    /// resident-`GraphCore` cost model. The catalog row survives, so the graph is
    /// still known and `open_lazy` re-materializes it on the next access. The
    /// CALLER is responsible for the durability gate (confirm durable first —
    /// the same discipline `hibernate`/cold-offload use) before calling this.
    /// `__commons__` is pinned and always a no-op here.
    pub fn evict_resident(&mut self, name: &str) -> bool {
        if name == "__commons__" {
            return false;
        }
        self.graphs.remove(name).is_some()
    }

    /// Number of graphs with a resident `GraphCore` (the bounded hot-context
    /// cache's current size).
    pub fn resident_len(&self) -> usize {
        self.graphs.len()
    }

    /// Number of graphs known to the catalog (resident + catalog-only) — the
    /// TRUE registered-graph count (CONCEPT:EG-KG.sharding.lazy-graph-catalog), unbounded by any
    /// resident-cache cap.
    pub fn catalog_len(&self) -> usize {
        self.catalog.len()
    }

    /// Is this graph currently resident (a live `GraphCore`)?
    pub fn is_resident(&self, name: &str) -> bool {
        self.graphs.contains_key(name)
    }

    /// Delete a named graph — resident, catalog-only, or both. Cannot delete
    /// `__commons__`. Errs only when the name is unknown to BOTH the resident map
    /// and the catalog (a catalog-only / evicted graph deletes cleanly, since the
    /// engine still knows about it even with no live `GraphCore`).
    pub fn delete_graph(&mut self, name: &str) -> Result<(), String> {
        if name == "__commons__" {
            return Err("Cannot delete the __commons__ graph".to_string());
        }
        let had_catalog = self.catalog.remove(name).is_some();
        let had_resident = self.graphs.remove(name).is_some();
        if had_catalog || had_resident {
            Ok(())
        } else {
            Err(format!("Graph '{}' not found", name))
        }
    }

    /// Get a reference to a graph entry. Resident-only — a catalog-only (not yet
    /// materialized) graph is NOT returned here; callers that must transparently
    /// lazily-open should go through `server::persistence::cold_offload::lazy_open`
    /// first (see `dispatch_graph_op`).
    pub fn get(&self, name: &str) -> Option<&GraphEntry> {
        self.graphs.get(name)
    }

    /// Get a mutable reference to a graph entry. Resident-only (see `get`).
    pub fn get_mut(&mut self, name: &str) -> Option<&mut GraphEntry> {
        self.graphs.get_mut(name)
    }

    /// List all REGISTERED graph names and types (CONCEPT:EG-KG.sharding.lazy-graph-catalog) — the
    /// catalog, so a graph evicted to catalog-only (or never yet opened) still
    /// appears here. In eager/default mode the catalog and resident set are
    /// always in lockstep, so this is byte-for-byte the old resident-only
    /// enumeration.
    pub fn list(&self) -> Vec<(String, GraphType)> {
        self.catalog
            .iter()
            .map(|e| (e.key().clone(), e.value().graph_type))
            .collect()
    }

    /// Check if a graph is known — resident OR catalog-only (CONCEPT:EG-KG.sharding.lazy-graph-catalog).
    /// A graph evicted back to catalog-only still "exists" (it re-opens on
    /// access); only a name never registered at all is absent.
    pub fn exists(&self, name: &str) -> bool {
        self.graphs.contains_key(name) || self.catalog.contains_key(name)
    }

    /// Get all RESIDENT graph entries for checkpoint/persistence/budget accounting
    /// (CONCEPT:EG-KG.sharding.lazy-graph-catalog). A catalog-only (evicted or never-opened) graph has
    /// nothing pending — its last mutation is already durable, or it has none —
    /// so it correctly has no entry here.
    pub fn all_entries(&self) -> Vec<&GraphEntry> {
        self.graphs.values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bus_exists_on_creation() {
        let reg = GraphRegistry::new();
        assert!(reg.exists("__commons__"));
        assert_eq!(reg.list().len(), 1);
    }

    #[test]
    fn test_create_and_delete_graph() {
        let mut reg = GraphRegistry::new();
        reg.create_graph("agent:planner", GraphType::Agent, Some("planner".into()))
            .unwrap();
        assert!(reg.exists("agent:planner"));
        assert_eq!(reg.list().len(), 2);

        reg.delete_graph("agent:planner").unwrap();
        assert!(!reg.exists("agent:planner"));
    }

    #[test]
    fn test_cannot_delete_bus() {
        let mut reg = GraphRegistry::new();
        assert!(reg.delete_graph("__commons__").is_err());
    }

    #[test]
    fn test_duplicate_create_fails() {
        let mut reg = GraphRegistry::new();
        reg.create_graph("test", GraphType::Team, None).unwrap();
        assert!(reg.create_graph("test", GraphType::Team, None).is_err());
    }

    #[test]
    fn test_delete_nonexistent_fails() {
        let mut reg = GraphRegistry::new();
        assert!(reg.delete_graph("nope").is_err());
    }

    // ── Catalog / lazy-open / bounded hot-context cache (DIST-P2-3) ─────────────

    /// A `GraphMaterializer` test double: durable material for named graphs, keyed
    /// in-memory (stands in for redb's `graph_meta`/nodes/edges tables).
    struct FakeMaterializer {
        material: std::collections::HashMap<String, GraphMaterial>,
    }

    impl GraphMaterializer for FakeMaterializer {
        fn materialize(&self, graph_name: &str) -> Option<GraphMaterial> {
            self.material.get(graph_name).cloned()
        }
    }

    fn props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec(&v).unwrap()
    }

    /// N graphs registered in the catalog cost only catalog rows — NONE are
    /// resident until accessed. `open_lazy` then materializes exactly the one
    /// touched, leaving the rest catalog-only (proves "catalog rows, not resident
    /// GraphCores" for the untouched majority).
    #[test]
    fn catalog_registers_without_residency_then_opens_lazily() {
        let mut reg = GraphRegistry::new(); // __commons__ resident + cataloged

        // Register N (say 50) catalog-only graphs with durable material behind
        // them — simulating a lazy-startup boot scan of `graph_meta`.
        let n = 50;
        let mut material = std::collections::HashMap::new();
        for i in 0..n {
            let name = format!("tenant:{i}");
            reg.register_catalog_only(&name, GraphType::Agent, None);
            material.insert(
                name.clone(),
                GraphMaterial {
                    nodes: vec![(format!("n{i}"), props(serde_json::json!({"i": i})))],
                    edges: Vec::new(),
                    semantic: Vec::new(),
                },
            );
        }
        reg.set_materializer(Arc::new(FakeMaterializer { material }));

        // Catalog knows all N + commons; NONE of the N are resident yet.
        assert_eq!(reg.catalog_len(), n + 1);
        assert_eq!(reg.resident_len(), 1, "only __commons__ is resident");
        for i in 0..n {
            let name = format!("tenant:{i}");
            assert!(reg.exists(&name), "catalog-known graph must `exist`");
            assert!(!reg.is_resident(&name), "not yet accessed ⇒ not resident");
        }

        // Access ONE cold graph: it lazily opens with its durable data intact.
        assert!(reg.open_lazy("tenant:7"));
        assert!(reg.is_resident("tenant:7"));
        let core = reg.get("tenant:7").unwrap().core.clone();
        assert_eq!(core.node_count(), 1);
        assert_eq!(
            core.get_node_properties("n7"),
            Some(props(serde_json::json!({"i": 7})))
        );

        // Every OTHER graph is still catalog-only — only the one touched hydrated.
        assert_eq!(reg.resident_len(), 2, "__commons__ + the one opened graph");
        assert!(!reg.is_resident("tenant:8"));

        // A genuinely unknown name never opens.
        assert!(!reg.open_lazy("no-such-graph"));
    }

    /// An evicted graph's catalog row survives, and a subsequent access
    /// re-materializes it with ALL its data intact (durability-gated eviction
    /// never loses data — only the RAM copy is dropped).
    #[test]
    fn evicted_graph_reopens_with_data_intact() {
        let mut reg = GraphRegistry::new();
        reg.create_graph("agent:a", GraphType::Agent, None).unwrap();
        reg.get("agent:a").unwrap().core.add_node(
            "x".into(),
            props(serde_json::json!({"payload": "hello"})),
        );
        assert_eq!(reg.get("agent:a").unwrap().core.node_count(), 1);

        // Wire a materializer that serves the SAME data back (standing in for the
        // durable redb tier the real facade reads from — the registry itself
        // never persists anything; it only drops/re-fetches).
        let mut material = std::collections::HashMap::new();
        material.insert(
            "agent:a".to_string(),
            GraphMaterial {
                nodes: vec![("x".to_string(), props(serde_json::json!({"payload": "hello"})))],
                edges: Vec::new(),
                semantic: Vec::new(),
            },
        );
        reg.set_materializer(Arc::new(FakeMaterializer { material }));

        // Evict: the resident GraphCore is gone, but the catalog row survives.
        assert!(reg.evict_resident("agent:a"));
        assert!(!reg.is_resident("agent:a"));
        assert!(reg.exists("agent:a"), "catalog row survives eviction");
        assert!(reg.get("agent:a").is_none(), "resident-only `get` misses");

        // Re-open: data comes back intact.
        assert!(reg.open_lazy("agent:a"));
        assert!(reg.is_resident("agent:a"));
        let core = reg.get("agent:a").unwrap().core.clone();
        assert_eq!(core.node_count(), 1);
        assert_eq!(
            core.get_node_properties("x"),
            Some(props(serde_json::json!({"payload": "hello"})))
        );
    }

    /// `__commons__` can never be evicted, even if a caller tries.
    #[test]
    fn commons_is_pinned_against_eviction() {
        let mut reg = GraphRegistry::new();
        assert!(!reg.evict_resident("__commons__"));
        assert!(reg.is_resident("__commons__"));
    }

    /// Deleting a catalog-only (evicted / never-opened) graph succeeds — the
    /// engine still knows about it even with no live `GraphCore`.
    #[test]
    fn delete_graph_works_on_catalog_only_entry() {
        let mut reg = GraphRegistry::new();
        reg.register_catalog_only("cold:x", GraphType::Team, None);
        assert!(reg.exists("cold:x"));
        assert!(!reg.is_resident("cold:x"));
        reg.delete_graph("cold:x").unwrap();
        assert!(!reg.exists("cold:x"));
    }

    /// `list`/`exists` report the FULL catalog (registered graphs), not just the
    /// resident subset — millions of registered-but-cold graphs must still be
    /// enumerable / "exist" without paying for residency.
    #[test]
    fn list_and_exists_reflect_full_catalog_not_just_resident() {
        let reg = GraphRegistry::new();
        for i in 0..10 {
            reg.register_catalog_only(&format!("cold:{i}"), GraphType::Team, None);
        }
        assert_eq!(reg.catalog_len(), 11); // __commons__ + 10
        assert_eq!(reg.resident_len(), 1); // only __commons__
        assert_eq!(reg.list().len(), 11);
        for i in 0..10 {
            assert!(reg.exists(&format!("cold:{i}")));
        }
    }
}
