use super::*;

impl GraphCore {
    pub fn new() -> Self {
        Self::empty()
    }

    /// The field-by-field construction behind [`GraphCore::new`].
    ///
    /// Split out of `new` deliberately: every lazy secondary index starts cold
    /// (`None`) and `dirty` starts `true`, so this is one long literal with no
    /// branching — keeping it in its own item leaves `new` a one-liner and keeps
    /// the construction readable as a single list of field defaults.
    pub(super) fn empty() -> Self {
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
}
