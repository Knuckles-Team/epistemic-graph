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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use dashmap::DashMap;

use crate::graph::{GraphCore, GraphSnapshot};
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
    /// Immutable identity of this create/delete incarnation.  A same-name
    /// recreate always receives a different value, so an asynchronous opener or
    /// cached handle can be fenced before it publishes into the new graph.
    pub incarnation_id: String,
    /// Shared cancellation token for work owned by this exact incarnation.
    cancellation: Arc<AtomicBool>,
}

/// A lightweight catalog row (CONCEPT:EG-KG.sharding.lazy-graph-catalog) — the cost of a
/// REGISTERED graph with no resident `GraphCore`. Millions of these cost a
/// `DashMap` row each, not a topology/props/semantic-store/ledger/cache.
#[derive(Debug, Clone)]
pub struct CatalogRecord {
    pub name: String,
    pub graph_type: GraphType,
    pub owner: Option<String>,
    pub incarnation_id: String,
    cancellation: Arc<AtomicBool>,
}

/// Public, immutable graph handle.  Long-running work must retain this rather
/// than only a graph name and call [`GraphRegistry::is_current_handle`] before
/// publishing.  The `Arc<GraphCore>` may remain alive after delete, but its
/// incarnation can never again become current.
#[derive(Debug, Clone)]
pub struct GraphHandle {
    pub name: String,
    pub incarnation_id: String,
    pub core: Arc<GraphCore>,
    cancellation: Arc<AtomicBool>,
}

impl GraphHandle {
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.load(Ordering::Acquire)
    }
}

/// Honest materialization state exposed by lifecycle responses and health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterializationPhase {
    CatalogOnly,
    Partial,
    Complete,
    Failed,
}

/// Completeness/freshness watermark for one graph incarnation.  A graph in
/// `Partial` is resident for bounded recovery work, but whole-graph operations
/// must not claim complete results until `valid` is true.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializationManifest {
    pub incarnation_id: String,
    pub phase: MaterializationPhase,
    pub source_snapshot_version: Option<u64>,
    pub completeness_cursor: Option<MaterializeCursor>,
    pub loaded_nodes: u64,
    pub loaded_edges: u64,
    pub valid: bool,
}

impl MaterializationManifest {
    fn catalog_only(incarnation_id: String) -> Self {
        Self {
            incarnation_id,
            phase: MaterializationPhase::CatalogOnly,
            source_snapshot_version: None,
            completeness_cursor: None,
            loaded_nodes: 0,
            loaded_edges: 0,
            valid: false,
        }
    }

    fn complete(incarnation_id: String, source_snapshot_version: Option<u64>) -> Self {
        Self {
            incarnation_id,
            phase: MaterializationPhase::Complete,
            source_snapshot_version,
            completeness_cursor: None,
            loaded_nodes: 0,
            loaded_edges: 0,
            valid: true,
        }
    }

    /// Advance the authoritative version covered by a complete resident image.
    ///
    /// Gateway completions can finish their post-commit bookkeeping out of order,
    /// so this watermark is monotonic. Catalog-only, partial, and failed images
    /// remain untouched: their next materialization must establish freshness from
    /// durable authority instead of inheriting a live-core version.
    pub fn advance_committed_version(&mut self, committed_version: u64) -> bool {
        if committed_version == 0 || self.phase != MaterializationPhase::Complete || !self.valid {
            return false;
        }
        self.source_snapshot_version = Some(
            self.source_snapshot_version
                .map_or(committed_version, |current| current.max(committed_version)),
        );
        true
    }
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
    /// Authoritative graph-scoped integrity policy restored with the data image.
    pub integrity_policy: Option<crate::graph::IntegrityPolicy>,
    /// Durable incarnation read in the same source snapshot as the rows.
    pub incarnation_id: Option<String>,
    /// Authoritative graph version covered by this material.
    pub source_snapshot_version: Option<u64>,
}

/// Builds a graph's durable material for a lazy first-open (CONCEPT:EG-KG.sharding.lazy-graph-catalog).
/// Implemented in the facade over the redb backend (reusing the existing
/// `read_graph_dump_blocking` per-graph rehydrate path) and injected into the
/// registry once at startup — mirrors [`crate::read_through::ReadThroughFactory`].
/// `None` means no durable materializer is installed.
pub trait GraphMaterializer: Send + Sync {
    /// Fetch `graph_name`'s durable material. `None` ⇒ nothing durable (a
    /// genuinely fresh graph, or a backend with no lazy-materialize support).
    fn materialize(&self, graph_name: &str) -> Option<GraphMaterial>;

    /// Fetch ONE bounded page of `graph_name`'s durable material (CONCEPT:EG-KG.sharding.paged-lazy-open,
    /// DIST-P2-5) — the working-subset counterpart of [`materialize`](Self::materialize)
    /// consumed by [`GraphRegistry::open_lazy_paged`]/[`GraphRegistry::page_in`].
    /// `cursor` resumes from a prior page (`None` starts from the beginning);
    /// `page_size` bounds the combined node+edge count returned (never exactly
    /// zero — a `0` is treated as `1` so a page always makes progress). Returns
    /// `None` exactly when [`materialize`](Self::materialize) would (nothing
    /// durable for this graph).
    ///
    /// The default implementation pages over an in-memory `materialize()` call —
    /// correct for ANY implementer (it still bounds how much is replayed into the
    /// `GraphCore` per call, which is the working-subset property `open_lazy_paged`
    /// needs), but it does not avoid materialize()'s own full-fetch cost at the
    /// SOURCE (e.g. a redb backend still reads every row up front). A backend that
    /// wants a genuinely bounded, streaming page straight off its durable store
    /// (no full in-memory fetch first) overrides this directly — a documented,
    /// justified follow-up beyond this seam's minimum bar.
    fn materialize_page(
        &self,
        graph_name: &str,
        cursor: Option<MaterializeCursor>,
        page_size: usize,
    ) -> Option<MaterialPage> {
        let first_page = cursor.is_none();
        let material = self.materialize(graph_name)?;
        let page_size = page_size.max(1);
        let node_offset = cursor
            .as_ref()
            .map(|c| c.node_offset)
            .unwrap_or(0)
            .min(material.nodes.len());
        let edge_offset = cursor
            .as_ref()
            .map(|c| c.edge_offset)
            .unwrap_or(0)
            .min(material.edges.len());

        let node_end = (node_offset + page_size).min(material.nodes.len());
        let nodes = material.nodes[node_offset..node_end].to_vec();
        let nodes_exhausted = node_end >= material.nodes.len();

        // Spend the page's REMAINING budget on edges only once every node has
        // already been paged in — mirrors `open_lazy`'s eager ordering (nodes
        // fully replayed before edges are added), so a partially-opened graph
        // never has a dangling edge referencing a not-yet-added node.
        let edge_budget = page_size.saturating_sub(nodes.len());
        let (edges, edge_end) = if nodes_exhausted && edge_budget > 0 {
            let edge_end = (edge_offset + edge_budget).min(material.edges.len());
            (material.edges[edge_offset..edge_end].to_vec(), edge_end)
        } else {
            (Vec::new(), edge_offset)
        };
        let edges_exhausted = nodes_exhausted && edge_end >= material.edges.len();

        Some(MaterialPage {
            nodes,
            edges,
            // The semantic store is a single opaque blob (not a paginated
            // collection) -- attach it only to the FIRST page so it lands exactly
            // once across the whole paged sequence.
            semantic: if first_page {
                material.semantic
            } else {
                Vec::new()
            },
            integrity_policy: if first_page {
                material.integrity_policy
            } else {
                None
            },
            next_cursor: if nodes_exhausted && edges_exhausted {
                None
            } else {
                Some(MaterializeCursor {
                    node_offset: node_end,
                    edge_offset: edge_end,
                    ..MaterializeCursor::default()
                })
            },
            incarnation_id: material.incarnation_id,
            source_snapshot_version: material.source_snapshot_version,
        })
    }
}

/// Resume point into a [`GraphMaterializer`]'s durable material for paged lazy
/// open (CONCEPT:EG-KG.sharding.paged-lazy-open, DIST-P2-5). Opaque to the caller beyond
/// threading it back into [`GraphMaterializer::materialize_page`]/
/// [`GraphRegistry::page_in`]. Offsets remain portable progress counters for the
/// default in-memory materializer. Durable ordered stores may additionally carry
/// opaque keyset positions, allowing each page to seek in `O(log N)` instead of
/// rescanning and skipping the complete prior prefix.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct MaterializeCursor {
    pub node_offset: usize,
    pub edge_offset: usize,
    /// Last durable node key returned by a keyset-capable materializer.
    /// Never exposed by the public protocol; it is an internal resume token.
    pub node_after: Option<String>,
    /// Last durable `(source, target, ordinal)` edge key returned by a
    /// keyset-capable materializer. The ordinal is required for parallel edges.
    pub edge_after: Option<(String, String, u32)>,
}

impl std::fmt::Debug for MaterializeCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MaterializeCursor")
            .field("node_offset", &self.node_offset)
            .field("edge_offset", &self.edge_offset)
            .field("has_node_keyset", &self.node_after.is_some())
            .field("has_edge_keyset", &self.edge_after.is_some())
            .finish()
    }
}

/// One bounded page of a graph's durable material (CONCEPT:EG-KG.sharding.paged-lazy-open, DIST-P2-5) —
/// the paged counterpart of [`GraphMaterial`]. `next_cursor` is `Some` while more
/// material remains to be paged in via [`GraphRegistry::page_in`], `None` once
/// this was the last page.
#[derive(Debug, Clone, Default)]
pub struct MaterialPage {
    pub nodes: Vec<(String, Vec<u8>)>,
    pub edges: Vec<(String, String, Vec<u8>)>,
    pub semantic: Vec<u8>,
    /// Authoritative graph-control state, attached to the first page only.
    pub integrity_policy: Option<crate::graph::IntegrityPolicy>,
    pub next_cursor: Option<MaterializeCursor>,
    /// Durable incarnation read with this page.  Production materializers must
    /// populate it; `None` is retained for in-memory/test materializers.
    pub incarnation_id: Option<String>,
    /// Authoritative version sampled in the page's source read transaction.
    pub source_snapshot_version: Option<u64>,
}

/// Outcome of [`GraphRegistry::open_lazy_paged`]: whether the graph ended up
/// resident, and — when there is more durable material than fit in the first
/// page — the [`MaterializeCursor`] to resume with via [`GraphRegistry::page_in`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PagedOpenOutcome {
    pub resident: bool,
    pub cursor: Option<MaterializeCursor>,
    /// False means the fetched page belonged to a deleted/recreated incarnation
    /// and was discarded without publication.
    pub accepted: bool,
}

/// Prepared lazy-open operation.  It owns only cloneable factories and immutable
/// catalog metadata, so durable I/O can run without holding the registry or
/// server-wide write lock.  Publication is separately fenced by incarnation and
/// cancellation token.
#[derive(Clone)]
pub struct LazyOpenTicket {
    name: String,
    graph_type: GraphType,
    owner: Option<String>,
    incarnation_id: String,
    cancellation: Arc<AtomicBool>,
    materializer: Option<Arc<dyn GraphMaterializer>>,
}

impl std::fmt::Debug for LazyOpenTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyOpenTicket")
            .field("name", &self.name)
            .field("incarnation_id", &self.incarnation_id)
            .field("cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl LazyOpenTicket {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn incarnation_id(&self) -> &str {
        &self.incarnation_id
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.load(Ordering::Acquire)
    }

    pub fn materialize(&self) -> Option<GraphMaterial> {
        if self.is_cancelled() {
            return None;
        }
        self.materializer.as_ref()?.materialize(&self.name)
    }

    pub fn materialize_page(
        &self,
        cursor: Option<MaterializeCursor>,
        page_size: usize,
    ) -> Option<MaterialPage> {
        if self.is_cancelled() {
            return None;
        }
        self.materializer
            .as_ref()?
            .materialize_page(&self.name, cursor, page_size)
    }

    fn accepts_material(&self, incarnation_id: Option<&str>) -> bool {
        !self.is_cancelled()
            && incarnation_id
                .map(|value| value == self.incarnation_id)
                .unwrap_or(true)
    }
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
    /// Durable read-through factory (CONCEPT:EG-KG.storage.read-through-seam-exercised). Set once at startup;
    /// when present, every `GraphCore` the registry
    /// creates (and every existing one, via `attach_read_through_all`) gains a
    /// per-graph read-through so an evicted node still reads from authority.
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
    /// startup, mirroring `read_through_factory`.
    materializer: Option<Arc<dyn GraphMaterializer>>,
    /// Per-name lifecycle/materialization state. The value itself is shared so
    /// background page publication updates a stable health/readiness view.
    materialization: DashMap<String, Arc<RwLock<MaterializationManifest>>>,
}

impl Default for GraphRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphRegistry {
    fn next_incarnation_id() -> String {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        format!("incarnation:local:{}", NEXT.fetch_add(1, Ordering::Relaxed))
    }

    /// Create a new registry with the `__commons__` graph pre-created.
    pub fn new() -> Self {
        let commons_incarnation = "incarnation:commons:v1".to_string();
        let commons_cancel = Arc::new(AtomicBool::new(false));
        let mut graphs = HashMap::new();
        graphs.insert(
            "__commons__".to_string(),
            GraphEntry {
                name: "__commons__".to_string(),
                graph_type: GraphType::Commons,
                core: Arc::new(GraphCore::new()),
                owner: None,
                incarnation_id: commons_incarnation.clone(),
                cancellation: commons_cancel.clone(),
            },
        );
        let catalog = DashMap::new();
        catalog.insert(
            "__commons__".to_string(),
            CatalogRecord {
                name: "__commons__".to_string(),
                graph_type: GraphType::Commons,
                owner: None,
                incarnation_id: commons_incarnation.clone(),
                cancellation: commons_cancel,
            },
        );
        let materialization = DashMap::new();
        materialization.insert(
            "__commons__".to_string(),
            Arc::new(RwLock::new(MaterializationManifest::complete(
                commons_incarnation,
                Some(0),
            ))),
        );
        GraphRegistry {
            graphs,
            catalog,
            read_through_factory: None,
            secondary_index_factory: None,
            materializer: None,
            materialization,
        }
    }

    /// Install the durable read-through factory and attach a per-graph read-through
    /// to every graph that ALREADY exists (CONCEPT:EG-KG.storage.read-through-seam-exercised) — the pre-created
    /// `__commons__` plus anything `load_all` recovered before this was set. Called
    /// once at startup; future `create_graph` calls
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
    /// text/temporal/derived-OWL/spatial indexes onto every graph that ALREADY exists
    /// (CONCEPT:EG-KG.storage.incremental-text / .incremental-temporal / .incremental-derived-owl) —
    /// the pre-created `__commons__` plus anything `load_all` recovered. Spatial state
    /// is synchronously backfilled before registration so recovered material is never
    /// represented by an available empty index. Future `create_graph` calls pick the
    /// factory up automatically. Mirrors [`set_read_through_factory`](Self::set_read_through_factory).
    pub fn set_secondary_index_factory(
        &mut self,
        factory: Arc<dyn crate::index::SecondaryIndexFactory>,
    ) {
        for entry in self.graphs.values() {
            register_secondary_indexes(&entry.core, factory.as_ref(), &entry.name, true);
        }
        self.secondary_index_factory = Some(factory);
    }

    /// Install the durable-material factory for lazy first-open (CONCEPT:EG-KG.sharding.lazy-graph-catalog).
    /// Called once at startup; `open_lazy`
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
        self.create_graph_with_incarnation(name, graph_type, owner, Self::next_incarnation_id(), 0)
    }

    /// Create with a caller-provided immutable incarnation (the authoritative
    /// lifecycle batch id in server mode).  The id is opaque and must be unique
    /// for every same-name recreation.
    pub fn create_graph_with_incarnation(
        &mut self,
        name: &str,
        graph_type: GraphType,
        owner: Option<String>,
        incarnation_id: String,
        source_snapshot_version: u64,
    ) -> Result<(), String> {
        if self.graphs.contains_key(name) || self.catalog.contains_key(name) {
            return Err(format!("Graph '{}' already exists", name));
        }
        if incarnation_id.trim().is_empty() {
            return Err("graph incarnation id must not be empty".to_string());
        }
        let cancellation = Arc::new(AtomicBool::new(false));
        let core = Arc::new(GraphCore::new());
        if source_snapshot_version > 0 {
            core.adopt_materialized_version(source_snapshot_version)?;
        }
        // Transparently wire read-through for the new graph when authoritative
        // (CONCEPT:EG-KG.storage.read-through-seam-exercised), so a graph created after startup is eviction-safe too.
        if let Some(factory) = &self.read_through_factory {
            core.set_read_through(factory.for_graph(name));
        }
        // Wire the per-graph server-layer indexes (text/temporal/derived-OWL) when the
        // factory is installed, so a graph created after startup is index-maintained too.
        if let Some(factory) = &self.secondary_index_factory {
            register_secondary_indexes(&core, factory.as_ref(), name, true);
        }
        self.catalog.insert(
            name.to_string(),
            CatalogRecord {
                name: name.to_string(),
                graph_type,
                owner: owner.clone(),
                incarnation_id: incarnation_id.clone(),
                cancellation: cancellation.clone(),
            },
        );
        self.materialization.insert(
            name.to_string(),
            Arc::new(RwLock::new(MaterializationManifest::complete(
                incarnation_id.clone(),
                Some(source_snapshot_version),
            ))),
        );
        self.graphs.insert(
            name.to_string(),
            GraphEntry {
                name: name.to_string(),
                graph_type,
                core,
                owner,
                incarnation_id,
                cancellation,
            },
        );
        Ok(())
    }

    /// Atomically replace one registered graph with an exact committed snapshot.
    ///
    /// Raft snapshot install uses this after the raw durable rows have committed.
    /// The new core is fully reconstructed before it becomes reachable, the exact
    /// durable incarnation is retained, and every handle/ticket belonging to the
    /// displaced incarnation is cancelled. Readers therefore observe either the
    /// prior complete core or the restored complete core, never an empty/partial
    /// intermediate projection.
    pub fn install_committed_graph(
        &mut self,
        name: &str,
        graph_type: GraphType,
        owner: Option<String>,
        incarnation_id: String,
        snapshot: GraphSnapshot,
        committed_version: u64,
    ) -> Result<Arc<GraphCore>, String> {
        if name.trim().is_empty() || incarnation_id.trim().is_empty() {
            return Err("committed graph identity fields must not be empty".to_string());
        }
        let core = Arc::new(GraphCore::from_snapshot(snapshot, committed_version)?);
        if let Some(factory) = &self.read_through_factory {
            core.set_read_through(factory.for_graph(name));
        }
        if let Some(factory) = &self.secondary_index_factory {
            register_secondary_indexes(&core, factory.as_ref(), name, true);
        }

        if let Some((_, record)) = self.catalog.remove(name) {
            record.cancellation.store(true, Ordering::Release);
        }
        if let Some(entry) = self.graphs.remove(name) {
            entry.cancellation.store(true, Ordering::Release);
        }
        let cancellation = Arc::new(AtomicBool::new(false));
        self.catalog.insert(
            name.to_string(),
            CatalogRecord {
                name: name.to_string(),
                graph_type,
                owner: owner.clone(),
                incarnation_id: incarnation_id.clone(),
                cancellation: cancellation.clone(),
            },
        );
        self.materialization.insert(
            name.to_string(),
            Arc::new(RwLock::new(MaterializationManifest::complete(
                incarnation_id.clone(),
                Some(committed_version),
            ))),
        );
        self.graphs.insert(
            name.to_string(),
            GraphEntry {
                name: name.to_string(),
                graph_type,
                core: core.clone(),
                owner,
                incarnation_id,
                cancellation,
            },
        );
        Ok(core)
    }

    /// Register a graph in the CATALOG ONLY — no `GraphCore` is constructed
    /// (CONCEPT:EG-KG.sharding.lazy-graph-catalog). Used by a lazy-startup backend to populate the
    /// registry from durable `graph_meta` rows without hydrating any node/edge
    /// data. A no-op if the name is already resident (never shadows a live core)
    /// or already cataloged (idempotent re-registration, e.g. a repeated boot
    /// scan). Takes `&self` — the catalog is a `DashMap`, so this needs no
    /// registry write-lock and is as cheap as a resident-cache hit.
    pub fn register_catalog_only(&self, name: &str, graph_type: GraphType, owner: Option<String>) {
        self.register_catalog_only_with_incarnation(
            name,
            graph_type,
            owner,
            Self::next_incarnation_id(),
        );
    }

    /// Catalog-only registration using the incarnation persisted with graph
    /// metadata.  Repeated recovery is idempotent only for the same incarnation;
    /// a conflicting row is deliberately ignored here and must be reconciled by
    /// the lifecycle authority rather than silently replacing live state.
    pub fn register_catalog_only_with_incarnation(
        &self,
        name: &str,
        graph_type: GraphType,
        owner: Option<String>,
        incarnation_id: String,
    ) {
        if self.graphs.contains_key(name) || self.catalog.contains_key(name) {
            return;
        }
        let cancellation = Arc::new(AtomicBool::new(false));
        self.catalog.insert(
            name.to_string(),
            CatalogRecord {
                name: name.to_string(),
                graph_type,
                owner,
                incarnation_id: incarnation_id.clone(),
                cancellation,
            },
        );
        self.materialization.insert(
            name.to_string(),
            Arc::new(RwLock::new(MaterializationManifest::catalog_only(
                incarnation_id,
            ))),
        );
    }

    /// Capture immutable catalog metadata and cloneable materializer state for a
    /// lock-free durable fetch.  The returned ticket is publication-fenced and
    /// becomes cancelled on delete/eviction.
    pub fn prepare_lazy_open(&self, name: &str) -> Option<LazyOpenTicket> {
        if let Some(entry) = self.graphs.get(name) {
            return Some(LazyOpenTicket {
                name: entry.name.clone(),
                graph_type: entry.graph_type,
                owner: entry.owner.clone(),
                incarnation_id: entry.incarnation_id.clone(),
                cancellation: entry.cancellation.clone(),
                materializer: self.materializer.clone(),
            });
        }
        let record = self.catalog.get(name)?.clone();
        Some(LazyOpenTicket {
            name: record.name,
            graph_type: record.graph_type,
            owner: record.owner,
            incarnation_id: record.incarnation_id,
            cancellation: record.cancellation,
            materializer: self.materializer.clone(),
        })
    }

    /// Return a generation-safe handle to the current resident incarnation.
    pub fn handle(&self, name: &str) -> Option<GraphHandle> {
        let entry = self.graphs.get(name)?;
        Some(GraphHandle {
            name: entry.name.clone(),
            incarnation_id: entry.incarnation_id.clone(),
            core: entry.core.clone(),
            cancellation: entry.cancellation.clone(),
        })
    }

    /// Fence a retained handle before it publishes asynchronous work.
    pub fn is_current_handle(&self, handle: &GraphHandle) -> bool {
        !handle.is_cancelled()
            && self
                .graphs
                .get(&handle.name)
                .is_some_and(|entry| entry.incarnation_id == handle.incarnation_id)
    }

    /// Current completeness/freshness watermark for lifecycle responses/health.
    pub fn materialization_manifest(&self, name: &str) -> Option<MaterializationManifest> {
        self.materialization
            .get(name)
            .and_then(|manifest| manifest.read().ok().map(|value| value.clone()))
    }

    pub fn materialization_handle(
        &self,
        name: &str,
    ) -> Option<Arc<RwLock<MaterializationManifest>>> {
        self.materialization
            .get(name)
            .map(|manifest| manifest.value().clone())
    }

    pub fn materialization_manifests(&self) -> Vec<MaterializationManifest> {
        self.materialization
            .iter()
            .filter_map(|entry| entry.value().read().ok().map(|value| value.clone()))
            .collect()
    }

    pub fn catalog_record(&self, name: &str) -> Option<CatalogRecord> {
        self.catalog.get(name).map(|record| record.clone())
    }

    fn ticket_is_current(&self, ticket: &LazyOpenTicket) -> bool {
        !ticket.is_cancelled()
            && self.catalog.get(ticket.name()).is_some_and(|record| {
                record.incarnation_id == ticket.incarnation_id
                    && Arc::ptr_eq(&record.cancellation, &ticket.cancellation)
            })
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
            return true;
        }
        let Some(ticket) = self.prepare_lazy_open(name) else {
            return false;
        };
        let material = ticket.materialize();
        self.publish_lazy_material(&ticket, material)
    }

    /// Publish a full durable fetch prepared outside the registry lock.  Returns
    /// false if delete/recreate/eviction cancelled the ticket or the source rows
    /// report a different incarnation.
    pub fn publish_lazy_material(
        &mut self,
        ticket: &LazyOpenTicket,
        material: Option<GraphMaterial>,
    ) -> bool {
        if self.graphs.contains_key(ticket.name()) {
            return self.ticket_is_current(ticket);
        }
        if !self.ticket_is_current(ticket) {
            return false;
        }
        if ticket.materializer.is_some() && material.is_none() {
            return false;
        }
        if !ticket.accepts_material(
            material
                .as_ref()
                .and_then(|value| value.incarnation_id.as_deref()),
        ) {
            return false;
        }
        let core = Arc::new(GraphCore::new());
        let source_snapshot_version = material
            .as_ref()
            .and_then(|value| value.source_snapshot_version);
        if let Some(material) = material {
            if let Some(policy) = material.integrity_policy {
                core.set_integrity_policy(policy);
            }
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
        // CONCEPT:EG-KG.storage.bloom-negative-lookup-guard — this is the FULL/eager
        // (non-paged) load: every durable node (if any) was just replayed through
        // `add_node` above, so the bloom filter now reflects the complete set.
        core.mark_bloom_complete();
        if let Some(version) = source_snapshot_version.filter(|version| *version > 0) {
            if core.adopt_materialized_version(version).is_err() {
                return false;
            }
        }
        if let Some(factory) = &self.read_through_factory {
            core.set_read_through(factory.for_graph(ticket.name()));
        }
        if let Some(factory) = &self.secondary_index_factory {
            register_secondary_indexes(&core, factory.as_ref(), ticket.name(), true);
        }
        if !secondary_indexes_valid(&core) {
            if let Some(manifest) = self.materialization.get(ticket.name()) {
                if let Ok(mut manifest) = manifest.write() {
                    manifest.phase = MaterializationPhase::Failed;
                    manifest.valid = false;
                }
            }
            return false;
        }
        if !self.ticket_is_current(ticket) {
            return false;
        }
        self.graphs.insert(
            ticket.name.clone(),
            GraphEntry {
                name: ticket.name.clone(),
                graph_type: ticket.graph_type,
                core,
                owner: ticket.owner.clone(),
                incarnation_id: ticket.incarnation_id.clone(),
                cancellation: ticket.cancellation.clone(),
            },
        );
        if let Some(manifest) = self.materialization.get(ticket.name()) {
            if let Ok(mut manifest) = manifest.write() {
                let mut complete = MaterializationManifest::complete(
                    ticket.incarnation_id.clone(),
                    source_snapshot_version,
                );
                complete.loaded_nodes = self
                    .graphs
                    .get(ticket.name())
                    .map(|entry| entry.core.node_count() as u64)
                    .unwrap_or(0);
                complete.loaded_edges = self
                    .graphs
                    .get(ticket.name())
                    .map(|entry| entry.core.edge_count() as u64)
                    .unwrap_or(0);
                *manifest = complete;
            }
        }
        true
    }

    /// Lazily materialize a catalog-known graph's resident `GraphCore` from a
    /// SINGLE bounded page of its durable material (CONCEPT:EG-KG.sharding.paged-lazy-open, DIST-P2-5)
    /// rather than the whole topology [`open_lazy`](Self::open_lazy) replays in
    /// one shot — the working-subset seam for a graph whose FULL rehydrate is too
    /// costly to do inline on the request that triggered the open. The graph
    /// becomes resident (queryable) immediately with just this page's nodes/
    /// edges; the caller pages in the rest via [`page_in`](Self::page_in) (e.g.
    /// spawned as a background task) using the returned cursor. Already-resident
    /// / genuinely-unknown behave exactly like `open_lazy` (`resident: true`/
    /// `false`, `cursor: None`) — this is a strict superset, never a regression.
    pub fn open_lazy_paged(&mut self, name: &str, page_size: usize) -> PagedOpenOutcome {
        if self.graphs.contains_key(name) {
            return PagedOpenOutcome {
                resident: true,
                cursor: None,
                accepted: true,
            };
        }
        let Some(ticket) = self.prepare_lazy_open(name) else {
            return PagedOpenOutcome {
                resident: false,
                cursor: None,
                accepted: false,
            };
        };
        let page = ticket.materialize_page(None, page_size);
        self.publish_lazy_first_page(&ticket, page)
    }

    /// Publish the first bounded page fetched outside the global registry lock.
    pub fn publish_lazy_first_page(
        &mut self,
        ticket: &LazyOpenTicket,
        page: Option<MaterialPage>,
    ) -> PagedOpenOutcome {
        if self.graphs.contains_key(ticket.name()) {
            let accepted = self.ticket_is_current(ticket);
            return PagedOpenOutcome {
                resident: accepted,
                cursor: None,
                accepted,
            };
        }
        if !self.ticket_is_current(ticket)
            || (ticket.materializer.is_some() && page.is_none())
            || !ticket.accepts_material(
                page.as_ref()
                    .and_then(|value| value.incarnation_id.as_deref()),
            )
        {
            return PagedOpenOutcome {
                resident: false,
                cursor: None,
                accepted: false,
            };
        }
        let core = Arc::new(GraphCore::new());
        let mut cursor = None;
        let mut source_snapshot_version = None;
        let mut loaded_nodes = 0;
        let mut loaded_edges = 0;
        if let Some(page) = page {
            cursor = page.next_cursor.clone();
            source_snapshot_version = page.source_snapshot_version;
            loaded_nodes = page.nodes.len() as u64;
            loaded_edges = page.edges.len() as u64;
            apply_material_page(&core, &page);
        }
        // CONCEPT:EG-KG.storage.bloom-negative-lookup-guard — a `None` cursor means this
        // first page was ALSO the last (nothing left to page in), so the bloom
        // filter already reflects the complete durable node set. When a cursor
        // remains, leave it unmarked: `page_in`/`apply_lazy_page_to_handle` marks
        // it on the eventual final page instead, so a not-yet-paged-in node is
        // never mistaken for a genuine absence in the meantime.
        if cursor.is_none() {
            core.mark_bloom_complete();
        }
        if let Some(version) = source_snapshot_version.filter(|version| *version > 0) {
            if core.adopt_materialized_version(version).is_err() {
                return PagedOpenOutcome {
                    resident: false,
                    cursor: None,
                    accepted: false,
                };
            }
        }
        if let Some(factory) = &self.read_through_factory {
            core.set_read_through(factory.for_graph(ticket.name()));
        }
        if let Some(factory) = &self.secondary_index_factory {
            // Every content-derived index is registered with an invalid/building
            // manifest until the final page performs a stable full rebuild.
            register_secondary_indexes(&core, factory.as_ref(), ticket.name(), cursor.is_none());
        }
        if cursor.is_none() && !secondary_indexes_valid(&core) {
            if let Some(manifest) = self.materialization.get(ticket.name()) {
                if let Ok(mut manifest) = manifest.write() {
                    manifest.phase = MaterializationPhase::Failed;
                    manifest.valid = false;
                }
            }
            return PagedOpenOutcome {
                resident: false,
                cursor: None,
                accepted: false,
            };
        }
        if !self.ticket_is_current(ticket) {
            return PagedOpenOutcome {
                resident: false,
                cursor: None,
                accepted: false,
            };
        }
        self.graphs.insert(
            ticket.name.clone(),
            GraphEntry {
                name: ticket.name.clone(),
                graph_type: ticket.graph_type,
                core,
                owner: ticket.owner.clone(),
                incarnation_id: ticket.incarnation_id.clone(),
                cancellation: ticket.cancellation.clone(),
            },
        );
        if let Some(manifest) = self.materialization.get(ticket.name()) {
            if let Ok(mut manifest) = manifest.write() {
                *manifest = MaterializationManifest {
                    incarnation_id: ticket.incarnation_id.clone(),
                    phase: if cursor.is_some() {
                        MaterializationPhase::Partial
                    } else {
                        MaterializationPhase::Complete
                    },
                    source_snapshot_version,
                    completeness_cursor: cursor.clone(),
                    loaded_nodes,
                    loaded_edges,
                    valid: cursor.is_none(),
                };
            }
        }
        PagedOpenOutcome {
            resident: true,
            cursor,
            accepted: true,
        }
    }

    /// Page in the NEXT bounded batch of an already-resident graph's durable
    /// material (CONCEPT:EG-KG.sharding.paged-lazy-open, DIST-P2-5), continuing from `cursor` (as
    /// returned by [`open_lazy_paged`](Self::open_lazy_paged) or a prior
    /// `page_in` call). Returns the next cursor (`Some`) while more material
    /// remains, `None` once exhausted — or when `name` isn't resident, or no
    /// materializer is installed (nothing to page in either way).
    pub fn page_in(
        &mut self,
        name: &str,
        cursor: MaterializeCursor,
        page_size: usize,
    ) -> Option<MaterializeCursor> {
        let ticket = self.prepare_lazy_open(name)?;
        let page = ticket.materialize_page(Some(cursor), page_size)?;
        self.publish_lazy_next_page(&ticket, page)
    }

    /// Publish a continuation page fetched outside the registry lock.  A source
    /// version change or incarnation mismatch invalidates the partial material;
    /// the caller must evict/restart instead of advertising a mixed snapshot.
    pub fn publish_lazy_next_page(
        &mut self,
        ticket: &LazyOpenTicket,
        page: MaterialPage,
    ) -> Option<MaterializeCursor> {
        if !self.ticket_is_current(ticket)
            || !ticket.accepts_material(page.incarnation_id.as_deref())
        {
            return None;
        }
        let handle = self.handle(ticket.name())?;
        let manifest = self.materialization_handle(ticket.name())?;
        Self::apply_lazy_page_to_handle(ticket, &handle, &manifest, page)
    }

    /// Apply and, on the final page, rebuild indexes using only per-graph state.
    /// The caller holds the lifecycle stripe; no server-wide state lock is needed
    /// while the potentially expensive rebuild executes.
    pub fn apply_lazy_page_to_handle(
        ticket: &LazyOpenTicket,
        handle: &GraphHandle,
        manifest_ref: &Arc<RwLock<MaterializationManifest>>,
        page: MaterialPage,
    ) -> Option<MaterializeCursor> {
        if ticket.is_cancelled()
            || handle.is_cancelled()
            || handle.incarnation_id != ticket.incarnation_id
            || !ticket.accepts_material(page.incarnation_id.as_deref())
        {
            return None;
        }
        let prior_snapshot = manifest_ref
            .read()
            .ok()
            .and_then(|manifest| manifest.source_snapshot_version);
        if prior_snapshot.is_some()
            && page.source_snapshot_version.is_some()
            && prior_snapshot != page.source_snapshot_version
        {
            if let Ok(mut manifest) = manifest_ref.write() {
                manifest.phase = MaterializationPhase::Failed;
                manifest.valid = false;
                manifest.completeness_cursor = None;
            }
            return None;
        }
        apply_material_page(&handle.core, &page);
        let next_cursor = page.next_cursor.clone();
        let final_page = next_cursor.is_none();
        if let Ok(mut manifest) = manifest_ref.write() {
            manifest.loaded_nodes = manifest
                .loaded_nodes
                .saturating_add(page.nodes.len() as u64);
            manifest.loaded_edges = manifest
                .loaded_edges
                .saturating_add(page.edges.len() as u64);
            manifest.source_snapshot_version = manifest
                .source_snapshot_version
                .or(page.source_snapshot_version);
            manifest.completeness_cursor = next_cursor.clone();
            // Exhausting the source cursor is not availability: maintained
            // indexes must first rebuild against this exact resident image.
            manifest.phase = MaterializationPhase::Partial;
            manifest.valid = false;
        }
        if final_page {
            // CONCEPT:EG-KG.storage.bloom-negative-lookup-guard — every durable node has now
            // been replayed through `add_node` across this graph's pages, so the
            // bloom filter reflects the complete set regardless of index-rebuild
            // outcome (index validity is orthogonal to node-id completeness).
            handle.core.mark_bloom_complete();
            rebuild_secondary_indexes(&handle.core);
            let indexes_valid = secondary_indexes_valid(&handle.core);
            if let Ok(mut manifest) = manifest_ref.write() {
                manifest.phase = if indexes_valid {
                    MaterializationPhase::Complete
                } else {
                    MaterializationPhase::Failed
                };
                manifest.valid = indexes_valid;
            }
        }
        next_cursor
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
        let Some(entry) = self.graphs.remove(name) else {
            return false;
        };
        entry.cancellation.store(true, Ordering::Release);
        // Eviction is not a new durable incarnation, but any in-flight page task
        // for the old resident core must be cancelled.  Install a fresh token for
        // the next open while retaining the immutable incarnation id.
        if let Some(mut record) = self.catalog.get_mut(name) {
            record.cancellation = Arc::new(AtomicBool::new(false));
        }
        if let Some(manifest) = self.materialization.get(name) {
            if let Ok(mut manifest) = manifest.write() {
                *manifest = MaterializationManifest::catalog_only(entry.incarnation_id);
            }
        }
        true
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
        let catalog = self.catalog.remove(name).map(|(_, record)| record);
        let resident = self.graphs.remove(name);
        if let Some(record) = &catalog {
            record.cancellation.store(true, Ordering::Release);
        }
        if let Some(entry) = &resident {
            entry.cancellation.store(true, Ordering::Release);
        }
        self.materialization.remove(name);
        let had_catalog = catalog.is_some();
        let had_resident = resident.is_some();
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

/// Replay one [`MaterialPage`] into `core` via the SAME `add_node`/`add_edge`/
/// semantic-store calls [`GraphRegistry::open_lazy`]'s full-material path uses
/// (CONCEPT:EG-KG.sharding.paged-lazy-open, DIST-P2-5) — shared by
/// [`GraphRegistry::open_lazy_paged`] and [`GraphRegistry::page_in`] so a paged
/// open is byte-identical, one page at a time, to the eager/full-material one.
fn apply_material_page(core: &Arc<GraphCore>, page: &MaterialPage) {
    if let Some(policy) = &page.integrity_policy {
        core.set_integrity_policy(policy.clone());
    }
    for (node_id, props) in &page.nodes {
        core.add_node(node_id.clone(), props.clone());
    }
    for (src, tgt, props) in &page.edges {
        let _ = core.add_edge(src.clone(), tgt.clone(), props.clone());
    }
    if !page.semantic.is_empty() {
        if let Ok(store) =
            rmp_serde::from_slice::<crate::compute::semantic::SemanticStore>(&page.semantic)
        {
            *core.semantic_store.write() = store;
        }
    }
}

/// Register one graph's server-layer indexes. Spatial indexes are derived entirely
/// from resident graph material, so they must be backfilled before planner pushdown
/// may advertise them. `material_complete == false` is the paged-lazy-open case:
/// every maintained index remains explicitly incomplete until the final page
/// rebuilds all of them.
fn register_secondary_indexes(
    core: &Arc<GraphCore>,
    factory: &dyn crate::index::SecondaryIndexFactory,
    name: &str,
    material_complete: bool,
) {
    for idx in factory.for_graph(name) {
        let source_snapshot_version = core.version();
        idx.publish_manifest(crate::index::IndexManifest::building(
            source_snapshot_version,
            crate::index::IndexCompletenessCursor {
                nodes: core.node_count() as u64,
                edges: core.edge_count() as u64,
                complete: false,
            },
        ));
        if material_complete {
            // Build every maintained server index before publication. Persistent
            // text and spatial state from a prior process is never trusted merely
            // because it opened successfully.
            match idx.full_rebuild(core) {
                Ok(()) => idx.publish_manifest(crate::index::IndexManifest::valid(
                    source_snapshot_version,
                    core.node_count() as u64,
                    core.edge_count() as u64,
                )),
                Err(_) => {
                    let mut manifest = idx.manifest();
                    manifest.validity = crate::index::IndexValidity::Failed;
                    manifest.completeness.complete = false;
                    idx.publish_manifest(manifest);
                }
            }
        }
        core.register_index(idx);
    }
}

/// Complete every maintained-index backfill after paged lazy-open consumes its
/// final page. Manifests become valid only after each stable rebuild succeeds.
fn rebuild_secondary_indexes(core: &Arc<GraphCore>) {
    core.indexes().rebuild_server_indexes(core);
}

fn secondary_indexes_valid(core: &Arc<GraphCore>) -> bool {
    core.indexes()
        .server_manifests()
        .into_iter()
        .all(|(_, manifest)| manifest.covers(core.version()))
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
    fn versioned_create_publishes_the_authoritative_watermark() {
        let mut reg = GraphRegistry::new();
        reg.create_graph_with_incarnation(
            "tenant:versioned-create",
            GraphType::Agent,
            None,
            "incarnation:create:versioned".to_string(),
            9,
        )
        .unwrap();

        let handle = reg.handle("tenant:versioned-create").unwrap();
        assert_eq!(handle.core.version(), 9);
        assert_eq!(
            reg.materialization_manifest("tenant:versioned-create")
                .unwrap()
                .source_snapshot_version,
            Some(9)
        );
    }

    #[test]
    fn committed_snapshot_replaces_the_core_and_incarnation_atomically() {
        let mut reg = GraphRegistry::new();
        reg.create_graph_with_incarnation(
            "tenant:snapshot",
            GraphType::Agent,
            Some("opaque-owner".to_string()),
            "incarnation:old".to_string(),
            3,
        )
        .unwrap();
        let old = reg.handle("tenant:snapshot").unwrap();
        old.core
            .add_node("old".to_string(), props(serde_json::json!({"v": 1})));

        let snapshot = GraphSnapshot {
            schema_version: crate::graph::GRAPH_SNAPSHOT_SCHEMA_VERSION,
            integrity_policy: None,
            nodes: vec![(
                "restored".to_string(),
                Arc::new(props(serde_json::json!({"v": 2}))),
            )],
            edges: Vec::new(),
            ledger: Vec::new(),
            semantic_store: crate::compute::semantic::SemanticStore::new(),
        };
        let restored = reg
            .install_committed_graph(
                "tenant:snapshot",
                GraphType::Agent,
                Some("opaque-owner".to_string()),
                "incarnation:restored".to_string(),
                snapshot,
                11,
            )
            .unwrap();

        assert!(old.is_cancelled());
        assert!(!restored.has_node("old"));
        assert!(restored.has_node("restored"));
        assert_eq!(restored.version(), 11);
        assert_eq!(
            reg.handle("tenant:snapshot").unwrap().incarnation_id,
            "incarnation:restored"
        );
    }

    #[test]
    fn committed_watermark_advances_monotonically_and_not_after_eviction() {
        let mut reg = GraphRegistry::new();
        let name = "tenant:committed-watermark";
        reg.create_graph_with_incarnation(
            name,
            GraphType::Agent,
            None,
            "incarnation:committed-watermark".to_string(),
            9,
        )
        .unwrap();
        let handle = reg.handle(name).unwrap();
        let manifest = reg.materialization_handle(name).unwrap();

        handle.core.mark_dirty();
        assert!(manifest
            .write()
            .unwrap()
            .advance_committed_version(handle.core.version()));
        assert_eq!(
            reg.materialization_manifest(name)
                .unwrap()
                .source_snapshot_version,
            Some(10)
        );

        assert!(manifest.write().unwrap().advance_committed_version(9));
        assert_eq!(
            reg.materialization_manifest(name)
                .unwrap()
                .source_snapshot_version,
            Some(10),
            "late completions must not regress the freshness watermark"
        );

        assert!(reg.evict_resident(name));
        assert!(!manifest.write().unwrap().advance_committed_version(11));
        let evicted = reg.materialization_manifest(name).unwrap();
        assert_eq!(evicted.phase, MaterializationPhase::CatalogOnly);
        assert!(!evicted.valid);
        assert_eq!(evicted.source_snapshot_version, None);
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
                    source_snapshot_version: Some((i + 1) as u64),
                    ..GraphMaterial::default()
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
        assert_eq!(core.version(), 8);
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

    /// `open_lazy_paged` (CONCEPT:EG-KG.sharding.paged-lazy-open, DIST-P2-5) materializes a graph
    /// from a BOUNDED working subset — not the whole topology `open_lazy` replays
    /// in one shot — and `page_in` pages in the rest. The graph is resident
    /// (queryable) after the FIRST page already, with only that page's nodes
    /// visible; by the time every page has been paged in, the result is
    /// byte-identical to an eager `open_lazy` of the same durable material.
    #[test]
    fn open_lazy_paged_materializes_a_bounded_working_set_then_pages_in_the_rest() {
        let mut reg = GraphRegistry::new();
        let name = "tenant:paged";
        reg.register_catalog_only(name, GraphType::Agent, None);

        // 10 nodes + 4 edges of durable material behind the catalog-only entry.
        let nodes: Vec<(String, Vec<u8>)> = (0..10)
            .map(|i| (format!("n{i}"), props(serde_json::json!({"i": i}))))
            .collect();
        let edges: Vec<(String, String, Vec<u8>)> = (0..4)
            .map(|i| {
                (
                    format!("n{i}"),
                    format!("n{}", i + 1),
                    props(serde_json::json!({"e": i})),
                )
            })
            .collect();
        let mut material = std::collections::HashMap::new();
        material.insert(
            name.to_string(),
            GraphMaterial {
                nodes: nodes.clone(),
                edges: edges.clone(),
                semantic: Vec::new(),
                source_snapshot_version: Some(11),
                ..GraphMaterial::default()
            },
        );
        reg.set_materializer(Arc::new(FakeMaterializer { material }));

        // First page: only 3 nodes worth of budget. The graph is ALREADY resident
        // (queryable) with just that bounded working set — not all 10 nodes.
        let outcome = reg.open_lazy_paged(name, 3);
        assert!(outcome.resident, "resident after the very first page");
        assert!(reg.is_resident(name));
        assert_eq!(reg.get(name).unwrap().core.version(), 11);
        assert_eq!(
            reg.get(name).unwrap().core.node_count(),
            3,
            "only the FIRST page's nodes are present — a bounded working set, not the whole topology"
        );
        let mut cursor = outcome.cursor.expect("more material remains after page 1");

        // Page in the rest, 3 units at a time, until exhausted.
        let mut pages_paged_in = 0;
        while let Some(next) = reg.page_in(name, cursor, 3) {
            cursor = next;
            pages_paged_in += 1;
            assert!(
                pages_paged_in < 20,
                "must terminate — cursor isn't advancing"
            );
        }

        // Once every page has landed, the paged-open result is byte-identical to
        // the eager `open_lazy` of the SAME material (10 nodes, 4 edges).
        let core = reg.get(name).unwrap().core.clone();
        assert_eq!(core.version(), 11);
        assert_eq!(
            core.node_count(),
            10,
            "all nodes present after paging in the rest"
        );
        for (node_id, expected_props) in &nodes {
            assert_eq!(
                core.get_node_properties(node_id),
                Some(expected_props.clone())
            );
        }
        for (src, tgt, _) in &edges {
            assert!(
                core.has_edge(src, tgt),
                "edge {src}->{tgt} must be present after full paging"
            );
        }

        // A genuinely unknown name never opens, exactly like `open_lazy`.
        let unknown = reg.open_lazy_paged("no-such-graph", 3);
        assert!(!unknown.resident);
        assert!(unknown.cursor.is_none());

        // Already-resident is a no-op that reports done (no cursor) immediately.
        let already = reg.open_lazy_paged(name, 3);
        assert!(already.resident);
        assert!(already.cursor.is_none());
    }

    /// An evicted graph's catalog row survives, and a subsequent access
    /// re-materializes it with ALL its data intact (durability-gated eviction
    /// never loses data — only the RAM copy is dropped).
    #[test]
    fn evicted_graph_reopens_with_data_intact() {
        let mut reg = GraphRegistry::new();
        reg.create_graph("agent:a", GraphType::Agent, None).unwrap();
        reg.get("agent:a")
            .unwrap()
            .core
            .add_node("x".into(), props(serde_json::json!({"payload": "hello"})));
        assert_eq!(reg.get("agent:a").unwrap().core.node_count(), 1);

        // Wire a materializer that serves the SAME data back (standing in for the
        // durable redb tier the real facade reads from — the registry itself
        // never persists anything; it only drops/re-fetches).
        let mut material = std::collections::HashMap::new();
        material.insert(
            "agent:a".to_string(),
            GraphMaterial {
                nodes: vec![(
                    "x".to_string(),
                    props(serde_json::json!({"payload": "hello"})),
                )],
                edges: Vec::new(),
                semantic: Vec::new(),
                ..GraphMaterial::default()
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

    #[test]
    fn delete_recreate_cancels_stale_page_ticket_and_preserves_new_incarnation() {
        let mut reg = GraphRegistry::new();
        reg.register_catalog_only_with_incarnation(
            "tenant:churn",
            GraphType::Agent,
            None,
            "incarnation:create:one".to_string(),
        );
        let ticket = reg.prepare_lazy_open("tenant:churn").unwrap();
        let first = MaterialPage {
            nodes: vec![("old".to_string(), props(serde_json::json!({"v": 1})))],
            next_cursor: Some(MaterializeCursor {
                node_offset: 1,
                edge_offset: 0,
                ..MaterializeCursor::default()
            }),
            incarnation_id: Some("incarnation:create:one".to_string()),
            source_snapshot_version: Some(1),
            ..MaterialPage::default()
        };
        assert!(reg.publish_lazy_first_page(&ticket, Some(first)).accepted);

        reg.delete_graph("tenant:churn").unwrap();
        assert!(ticket.is_cancelled());
        reg.create_graph_with_incarnation(
            "tenant:churn",
            GraphType::Agent,
            None,
            "incarnation:create:two".to_string(),
            0,
        )
        .unwrap();

        let stale = MaterialPage {
            nodes: vec![("stale".to_string(), props(serde_json::json!({"v": 0})))],
            incarnation_id: Some("incarnation:create:one".to_string()),
            source_snapshot_version: Some(1),
            ..MaterialPage::default()
        };
        assert!(reg.publish_lazy_next_page(&ticket, stale).is_none());
        let current = reg.handle("tenant:churn").unwrap();
        assert_eq!(current.incarnation_id, "incarnation:create:two");
        assert_eq!(current.core.node_count(), 0);
    }

    #[test]
    fn changed_source_snapshot_fails_partial_materialization_before_availability() {
        let mut reg = GraphRegistry::new();
        reg.register_catalog_only_with_incarnation(
            "tenant:versioned",
            GraphType::Agent,
            None,
            "incarnation:stable".to_string(),
        );
        let ticket = reg.prepare_lazy_open("tenant:versioned").unwrap();
        let first = MaterialPage {
            nodes: vec![("a".to_string(), props(serde_json::json!({"v": 1})))],
            next_cursor: Some(MaterializeCursor {
                node_offset: 1,
                edge_offset: 0,
                ..MaterializeCursor::default()
            }),
            incarnation_id: Some("incarnation:stable".to_string()),
            source_snapshot_version: Some(7),
            ..MaterialPage::default()
        };
        assert!(reg.publish_lazy_first_page(&ticket, Some(first)).accepted);
        assert!(
            !reg.materialization_manifest("tenant:versioned")
                .unwrap()
                .valid
        );

        let changed = MaterialPage {
            nodes: vec![("b".to_string(), props(serde_json::json!({"v": 2})))],
            incarnation_id: Some("incarnation:stable".to_string()),
            source_snapshot_version: Some(8),
            ..MaterialPage::default()
        };
        assert!(reg.publish_lazy_next_page(&ticket, changed).is_none());
        let manifest = reg.materialization_manifest("tenant:versioned").unwrap();
        assert_eq!(manifest.phase, MaterializationPhase::Failed);
        assert!(!manifest.valid);
        assert_eq!(reg.get("tenant:versioned").unwrap().core.node_count(), 1);
    }

    #[test]
    fn materialize_cursor_debug_redacts_durable_row_keys() {
        let cursor = MaterializeCursor {
            node_offset: 4,
            edge_offset: 2,
            node_after: Some("sensitive-node-identifier".to_string()),
            edge_after: Some((
                "sensitive-source".to_string(),
                "sensitive-target".to_string(),
                7,
            )),
        };
        let rendered = format!("{cursor:?}");
        assert!(rendered.contains("has_node_keyset: true"));
        assert!(rendered.contains("has_edge_keyset: true"));
        assert!(!rendered.contains("sensitive"));
    }
}
