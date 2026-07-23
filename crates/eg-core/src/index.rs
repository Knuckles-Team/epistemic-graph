// CONCEPT:EG-KG.storage.index-manager-seam — the unified IndexManager seam.
//
// Today the engine's secondary indexes are each bolted ad-hoc onto `GraphCore`:
// the lazy LABEL index (CONCEPT:EG-KG.compute.consult-lazy), the demand-driven PROPERTY equality
// index (CONCEPT:EG-KG.query.concept-12), the ontology aho-corasick term index (CONCEPT:EG-ORCH.routing.lexical-capability-escalation),
// and the eg-ann vector index (the `SemanticStore`). Each is rebuilt-on-mutation
// via `version()`/`mark_dirty`, and every pushdown consumer (eg-query's
// `NodesTableProvider`, eg-plan's Filter leg) has to know each one individually.
//
// This module introduces ONE registry/seam over those indexes:
//
//   * [`SecondaryIndex`] — the common trait every secondary index implements:
//     `kind`, `descriptor` (what it covers, for a planner), `covers(predicate)`,
//     `lookup(core, predicate)`, `invalidate(core)`.
//   * [`IndexManager`] — owns the registered indexes and answers the two questions
//     a planner asks: "which index covers this predicate?" ([`IndexManager::index_for`])
//     and "what indexes cover column X?" ([`IndexManager::descriptors_for_column`]).
//
// CRITICAL — behavior preservation. The LABEL and PROPERTY indexes keep their
// EXISTING cached state on `GraphCore` (the `label_index` / `property_index`
// `RwLock<Option<…>>` cells) and their EXACT lazy-build + `mark_dirty`
// invalidation semantics. The trait impls here are thin descriptors that route
// to the same `GraphCore` methods (`get_nodes_by_label`, `nodes_by_property`),
// so label/property perf and their tests are untouched — the manager is the
// registry/routing seam, NOT a relocation of the cache storage.
//
// The HNSW / eg-ann vector index and the aho-corasick ontology index stay where
// they live (`compute::semantic` / the `ontology_index` cell) but are made
// DISCOVERABLE here via a descriptor so a planner can enumerate them — they are
// registered as `lookup`-less (a `Predicate`-based equality `lookup` is not their
// shape; they serve kNN / lexical scans through their own surfaces).
//
// ── Extension point (text G4 / spatial G5 / time F) ──────────────────────────
// Adding a new index TYPE is a closed, three-step change that does NOT touch the
// manager core:
//   1. add a variant to [`IndexKind`] (and, if it answers a new predicate shape,
//      a variant to [`Predicate`]);
//   2. implement [`SecondaryIndex`] for the new index (its own cache + lazy build,
//      invalidated off `mark_dirty`/`version()` like the others);
//   3. `register` an instance in [`IndexManager::with_default_indexes`].
// `index_for`/`descriptors_for_column`/`invalidate_all` iterate the registry
// generically, so they pick the new index up with no edit. See `docs/`.

use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::RwLock;

use crate::graph::GraphCore;

/// One node touched by a committed write batch (CONCEPT:EG-KG.storage.write-changeset). Carries
/// the node id and — for adds/updates — OPTIONALLY the new property blob, so a
/// content-derived index (text / temporal) can compute its own delta without
/// re-reading the graph (re-reading `core` under the batch's held topology lock would
/// deadlock). The coalescer captures `properties_msgpack` ONLY when a `needs_content`
/// server index is registered on the graph (CONCEPT:EG-KG.storage.incremental-text /
/// .incremental-temporal — the flag is `IndexManager::wants_change_content`); otherwise
/// it stays `None` and the hot path pays no per-op blob clone (the vector store keys off
/// removals alone). For an ADD the blob is the full property map; for a CAS update it is
/// the `updates` map (a field-scoped content index reads only its own field from it).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NodeChange {
    pub id: String,
    pub properties_msgpack: Option<Vec<u8>>,
    /// Exact top-level fields changed by a partial update. `None` means the
    /// complete node image may have changed (add/upsert/remove semantics), so a
    /// cache that cannot apply a delta must invalidate conservatively.
    #[serde(default)]
    pub changed_fields: Option<Vec<String>>,
}

impl NodeChange {
    pub fn new(id: String) -> Self {
        Self {
            id,
            properties_msgpack: None,
            changed_fields: None,
        }
    }
    pub fn with_properties(id: String, properties_msgpack: Vec<u8>) -> Self {
        Self {
            id,
            properties_msgpack: Some(properties_msgpack),
            changed_fields: None,
        }
    }

    pub fn with_properties_and_fields(
        id: String,
        properties_msgpack: Vec<u8>,
        changed_fields: Vec<String>,
    ) -> Self {
        Self {
            id,
            properties_msgpack: Some(properties_msgpack),
            changed_fields: Some(changed_fields),
        }
    }

    pub fn with_fields(id: String, changed_fields: Vec<String>) -> Self {
        Self {
            id,
            properties_msgpack: None,
            changed_fields: Some(changed_fields),
        }
    }
}

/// One edge touched by a committed write batch.
#[derive(Debug, Clone)]
pub struct EdgeChange {
    pub source: String,
    pub target: String,
}

/// The per-batch delta collected inside the coalescer's single topology lock
/// (CONCEPT:EG-KG.storage.write-changeset). It records exactly which nodes/edges were
/// added / updated / removed by a committed [`WriteOp`](crate) batch, so the
/// [`IndexManager`] can maintain the heavy secondary indexes incrementally instead
/// of dropping and rebuilding them. Only successful ops are recorded (a failed
/// add-edge or a lost CAS contributes nothing).
#[derive(Debug, Clone, Default)]
pub struct ChangeSet {
    pub added_nodes: Vec<NodeChange>,
    pub updated_nodes: Vec<NodeChange>,
    pub removed_nodes: Vec<String>,
    pub added_edges: Vec<EdgeChange>,
    pub removed_edges: Vec<EdgeChange>,
}

impl ChangeSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// No mutation was recorded (an empty batch, or a batch of only no-op writes).
    pub fn is_empty(&self) -> bool {
        self.added_nodes.is_empty()
            && self.updated_nodes.is_empty()
            && self.removed_nodes.is_empty()
            && self.added_edges.is_empty()
            && self.removed_edges.is_empty()
    }

    /// Whether this batch can change a node-derived label/property/JSON-path
    /// posting. Pure edge deltas leave those caches byte-for-byte valid.
    pub fn has_node_changes(&self) -> bool {
        !self.added_nodes.is_empty()
            || !self.updated_nodes.is_empty()
            || !self.removed_nodes.is_empty()
    }

    /// Total number of recorded node + edge changes.
    pub fn len(&self) -> usize {
        self.added_nodes.len()
            + self.updated_nodes.len()
            + self.removed_nodes.len()
            + self.added_edges.len()
            + self.removed_edges.len()
    }

    pub fn record_add_node(&mut self, id: String) {
        self.added_nodes.push(NodeChange::new(id));
    }
    pub fn record_update_node(&mut self, id: String) {
        self.updated_nodes.push(NodeChange::new(id));
    }
    pub fn record_remove_node(&mut self, id: String) {
        self.removed_nodes.push(id);
    }
    pub fn record_add_edge(&mut self, source: String, target: String) {
        self.added_edges.push(EdgeChange { source, target });
    }
    pub fn record_remove_edge(&mut self, source: String, target: String) {
        self.removed_edges.push(EdgeChange { source, target });
    }
}

/// Why a secondary index could not maintain a delta (CONCEPT:EG-KG.storage.index-manager-seam).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexError {
    /// Incremental maintenance failed. The index is marked stale and excluded from
    /// planner selection until explicit recovery rebuilds it.
    Failed(String),
}

impl std::fmt::Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexError::Failed(e) => write!(f, "index delta failed: {e}"),
        }
    }
}

impl std::error::Error for IndexError {}

/// Per-batch maintenance tally returned by [`IndexManager::commit_batch`] — how
/// many registered indexes applied their delta vs became stale. Mainly for the
/// seam's tests / in-process diagnostics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchMaintenance {
    pub deltas_applied: usize,
    pub failures: usize,
}

/// The kind of a secondary index. New index types (text, spatial, time) add a
/// variant here without touching the manager core (extension point above).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    /// Lazy `label → node ids` map (CONCEPT:EG-KG.compute.consult-lazy).
    Label,
    /// Bounded, demand-driven `key → value → node ids` equality index
    /// (CONCEPT:EG-KG.query.concept-12).
    Property,
    /// aho-corasick capability-term lexical index (CONCEPT:EG-ORCH.routing.lexical-capability-escalation). Discoverable;
    /// served through its own `match_ontology_terms` surface, not equality lookup.
    Ontology,
    /// HNSW / eg-ann vector index (the `SemanticStore`). Discoverable; served
    /// through kNN, not equality lookup.
    Vector,
    /// Server-layer eg-text Tantivy BM25 full-text index (CONCEPT:EG-KG.storage.incremental-text).
    /// Content-derived; served through its own BM25 search surface, not equality lookup.
    Text,
    /// Server-layer eg-tsdb time-series index (CONCEPT:EG-KG.storage.incremental-temporal).
    /// Content-derived; served through its own range/PromQL surface.
    Temporal,
    /// Server-layer eg-rdf derived-OWL materialized-closure index
    /// (CONCEPT:EG-KG.storage.incremental-derived-owl). Differentially materialized by the
    /// reasoner on-demand; its `apply_delta` is a documented no-op (see the adapter).
    DerivedOwl,
    /// Server-layer eg-geo maintained spatial index (CONCEPT:EG-KG.storage.incremental-spatial, L37):
    /// a per-layer item set (`node_id -> bbox`) maintained incrementally by the committed
    /// write batch, backing a lazily-(re)built packed Hilbert R-tree — the persistent
    /// counterpart to `Op::SpatialScan`'s prior per-query ephemeral R-tree rebuild.
    /// Content-derived; served through its own bbox-query surface, not equality lookup.
    Spatial,
    // Future index kinds register here (CONCEPT:AU-KG.query.text-spatial-time text / spatial / time)
    // with their own `SecondaryIndex` impl — the manager core does not change.
}

/// Lifecycle state of a maintained index build.  Merely registering an index
/// never makes it planner-visible; only `Valid` with a completeness cursor that
/// covers the source snapshot may be advertised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexValidity {
    Building,
    Valid,
    Stale,
    Failed,
}

/// Explicit source coverage for a maintained index.  The cursor is row-count
/// based because node/edge materialization is paged independently.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexCompletenessCursor {
    pub nodes: u64,
    pub edges: u64,
    pub complete: bool,
}

/// Manifest published by every server-maintained index.  `build_version` is the
/// manifest schema/algorithm generation, not a filesystem or host identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexManifest {
    pub source_snapshot_version: u64,
    pub build_version: u32,
    pub completeness: IndexCompletenessCursor,
    pub validity: IndexValidity,
}

impl IndexManifest {
    pub const BUILD_VERSION: u32 = 1;

    pub fn building(source_snapshot_version: u64, completeness: IndexCompletenessCursor) -> Self {
        Self {
            source_snapshot_version,
            build_version: Self::BUILD_VERSION,
            completeness,
            validity: IndexValidity::Building,
        }
    }

    pub fn valid(source_snapshot_version: u64, nodes: u64, edges: u64) -> Self {
        Self {
            source_snapshot_version,
            build_version: Self::BUILD_VERSION,
            completeness: IndexCompletenessCursor {
                nodes,
                edges,
                complete: true,
            },
            validity: IndexValidity::Valid,
        }
    }

    pub fn covers(&self, source_snapshot_version: u64) -> bool {
        self.validity == IndexValidity::Valid
            && self.completeness.complete
            && self.source_snapshot_version >= source_snapshot_version
    }
}

impl Default for IndexManifest {
    fn default() -> Self {
        Self::building(0, IndexCompletenessCursor::default())
    }
}

/// A structural predicate a planner can ask the manager to resolve. Equality-only
/// for now — the shape the label and property pushdowns already speak. A future
/// index kind that answers a different shape (a text MATCH, a spatial WITHIN, a
/// time BETWEEN) adds its own variant here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    /// `n` carries label `0` across any of `type`/`node_type`/`label`/`labels`
    /// (mirrors `GraphCore::get_nodes_by_label`).
    LabelEq(String),
    /// Property `key` equals the canonical string `value` (mirrors
    /// `GraphCore::nodes_by_property`).
    PropertyEq { key: String, value: String },
}

/// Discoverability metadata for one registered index: enough for a planner to ask
/// "what covers column X / this predicate shape?" WITHOUT running a lookup. Cheap
/// to clone and free of any graph state.
#[derive(Debug, Clone)]
pub struct IndexDescriptor {
    pub kind: IndexKind,
    /// Columns this index covers. `["__label__"]` for the label index (a virtual
    /// column over the label fields); the demanded/seedable property keys for the
    /// property index (`None` ⇒ "any scalar column, demand-driven under the bound").
    pub columns: IndexColumns,
    /// Does this index serve a `Predicate`-shaped equality `lookup`? `false` for
    /// the discoverable-only vector / ontology indexes (kNN / lexical surfaces).
    pub serves_lookup: bool,
}

/// What columns an index covers, for discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexColumns {
    /// A fixed set of column names (e.g. the label virtual column).
    Fixed(Vec<String>),
    /// Any scalar column, indexed on demand under the bounded cap (the property
    /// index). A planner treats this as "covers any equality column".
    AnyScalarDemandDriven,
    /// Not a column-oriented index (vector kNN / ontology lexical) — discoverable
    /// by kind, not by column.
    NonColumnar,
}

/// The virtual column name the label index covers (label lives across several
/// physical fields, so it is exposed under one stable name to a planner).
pub const LABEL_COLUMN: &str = "__label__";

/// One secondary index behind the manager. Implementers keep their own cache
/// (lazy-built, `version()`/`mark_dirty`-invalidated) — the manager only routes.
/// `'static` (every impl in this codebase already is — owned server-lifetime
/// state, never a borrowed index) so [`Self::as_any`] can downcast a trait object
/// back to its concrete type (CONCEPT:EG-KG.query.served-text-index-binding — the planner
/// pushdown seam uses this to recover the server's concrete `GraphTextIndex`/
/// vector index from behind the generic registry instead of rebuilding a
/// throwaway snapshot index per query).
pub trait SecondaryIndex: Send + Sync + 'static {
    /// This index's kind.
    fn kind(&self) -> IndexKind;

    /// Discoverability metadata — what this index covers, for a planner.
    fn descriptor(&self) -> IndexDescriptor;

    /// Does this index cover `predicate` (could resolve it via `lookup`)? Cheap,
    /// state-free — a planner calls this before committing to a `lookup`. An index
    /// with `serves_lookup == false` always returns `false`.
    fn covers(&self, predicate: &Predicate) -> bool;

    /// Resolve `predicate` to the matching node ids via this index's cache,
    /// building/extending it lazily over `core`. Returns:
    ///   * `Some(ids)` — resolved through the index (possibly empty).
    ///   * `None` — this index can NOT resolve `predicate` under its policy (e.g.
    ///     the bounded property cap is full for a new key) ⇒ the caller full-scans.
    fn lookup(&self, core: &GraphCore, predicate: &Predicate) -> Option<Vec<String>>;

    /// Does this index derive its state from node CONTENT — the property blob of an
    /// added/updated node — rather than from ids/removals alone
    /// (CONCEPT:EG-KG.storage.incremental-text / .incremental-temporal)? A text or
    /// temporal index reads a node's fields, so it needs the coalescer to CAPTURE the
    /// property msgpack into each [`NodeChange`]; the vector/label/property/ontology
    /// indexes key off ids/removals only and keep the write hot path zero-clone.
    ///
    /// The DEFAULT is `false`. When a graph has NO content-consuming index registered,
    /// the coalescer skips the per-op blob clone entirely (the vector-only default
    /// path is unchanged); it captures blobs ONLY once such an index is registered.
    fn needs_content(&self) -> bool {
        false
    }

    /// Current build/completeness manifest.  Core-owned lazy descriptors do not
    /// persist a separate maintained store and therefore use the conservative
    /// default. Server-maintained indexes override both this and
    /// [`Self::publish_manifest`].
    fn manifest(&self) -> IndexManifest {
        IndexManifest::default()
    }

    /// Publish a lifecycle transition for a maintained index.  The registry calls
    /// this before/after recovery rebuilds; the write path advances it after a
    /// successful delta.  Default is a no-op for core-owned descriptors.
    fn publish_manifest(&self, _manifest: IndexManifest) {}

    /// Whether this implementation owns and enforces a persisted completeness
    /// manifest. Lightweight/custom indexes written before manifests existed keep
    /// the default `false`; server-maintained planner indexes override it.
    fn maintains_manifest(&self) -> bool {
        false
    }

    // ── Write-path maintenance seam (CONCEPT:EG-KG.storage.index-manager-seam) ─────────────
    //
    // These give the ONE registry a write side to match its read side. Every
    // registered index has one current incremental behavior. A failure marks the
    // maintained index stale; recovery rebuild is explicit and never runs as a
    // hidden write-path fallback.

    /// Incrementally apply a committed batch's `change` to this index's state.
    ///
    /// Core-owned lazy descriptors return `Ok(())` because their exact field-aware
    /// invalidation is performed once by `GraphCore` after this registry pass.
    /// Stateful indexes update their resident structures directly.
    fn apply_delta(&self, core: &GraphCore, change: &ChangeSet) -> Result<(), IndexError>;

    /// Rebuild — or, for a lazy index, invalidate for rebuild-on-read — this index
    /// over `core`. The manager's fallback when `apply_delta` can't maintain a delta.
    ///
    /// The DEFAULT is a no-op: the lazy label/property/path caches are invalidated
    /// centrally by [`GraphCore::invalidate_indexes`], so their descriptors need do
    /// nothing here. A stateful index that owns its own cache clears/rebuilds it.
    fn full_rebuild(&self, core: &GraphCore) -> Result<(), IndexError> {
        let _ = core;
        Ok(())
    }

    /// Downcast hook (CONCEPT:EG-KG.query.served-text-index-binding): recover the CONCRETE type behind this
    /// trait object. A REQUIRED method (not a default) — the `&Self -> &dyn Any`
    /// unsizing coercion needs `Self: Sized` at the impl site, which a default
    /// body typechecked generically over the trait's abstract `Self` cannot
    /// assume (that would exclude the method from the vtable, making it
    /// uncallable through `&dyn SecondaryIndex` — exactly the shape a caller
    /// needs). Every impl's body is the same one-liner: `fn as_any(&self) -> &dyn
    /// std::any::Any { self }`. A caller holding `&dyn SecondaryIndex` (e.g. via
    /// [`IndexManager::with_server_index`]) calls
    /// `.as_any().downcast_ref::<ConcreteType>()` to recover it — the seam a
    /// planner pushdown uses to reach the server-layer `GraphTextIndex` (or a
    /// future spatial/vector server index) directly, instead of rebuilding an
    /// equivalent index from a snapshot on every query.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// The label index descriptor (CONCEPT:EG-KG.compute.consult-lazy). Holds no state — it routes to
/// `GraphCore::get_nodes_by_label`, which owns the lazy `label_index` cache.
#[derive(Debug, Default)]
pub struct LabelIndex;

impl SecondaryIndex for LabelIndex {
    fn kind(&self) -> IndexKind {
        IndexKind::Label
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn descriptor(&self) -> IndexDescriptor {
        IndexDescriptor {
            kind: IndexKind::Label,
            columns: IndexColumns::Fixed(vec![LABEL_COLUMN.to_string()]),
            serves_lookup: true,
        }
    }

    fn covers(&self, predicate: &Predicate) -> bool {
        matches!(predicate, Predicate::LabelEq(_))
    }

    fn lookup(&self, core: &GraphCore, predicate: &Predicate) -> Option<Vec<String>> {
        let Predicate::LabelEq(label) = predicate else {
            return None;
        };
        // Route to the existing lazy label index (un-capped id list). The label
        // index never refuses (always `Some`), so the planner always gets the set.
        let ids = core
            .get_nodes_by_label(label, 0)
            .into_iter()
            .map(|(id, _props)| id)
            .collect();
        Some(ids)
    }

    fn apply_delta(&self, _core: &GraphCore, _change: &ChangeSet) -> Result<(), IndexError> {
        Ok(())
    }
}

/// The property equality index descriptor (CONCEPT:EG-KG.query.concept-12). Holds no state — it
/// routes to `GraphCore::nodes_by_property`, which owns the bounded, demand-driven
/// `property_index` cache (incl. the cap + env seed policy).
#[derive(Debug, Default)]
pub struct PropertyEqIndex;

impl SecondaryIndex for PropertyEqIndex {
    fn kind(&self) -> IndexKind {
        IndexKind::Property
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn descriptor(&self) -> IndexDescriptor {
        IndexDescriptor {
            kind: IndexKind::Property,
            columns: IndexColumns::AnyScalarDemandDriven,
            serves_lookup: true,
        }
    }

    fn covers(&self, predicate: &Predicate) -> bool {
        matches!(predicate, Predicate::PropertyEq { .. })
    }

    fn lookup(&self, core: &GraphCore, predicate: &Predicate) -> Option<Vec<String>> {
        let Predicate::PropertyEq { key, value } = predicate else {
            return None;
        };
        // Route to the existing bounded property index. `None` here means the key
        // is not (and cannot be) indexed under the cap — the caller full-scans.
        core.nodes_by_property(key, value)
    }

    fn apply_delta(&self, _core: &GraphCore, _change: &ChangeSet) -> Result<(), IndexError> {
        Ok(())
    }
}

/// Discoverable-only descriptor for the aho-corasick ontology term index
/// (CONCEPT:EG-ORCH.routing.lexical-capability-escalation). It is NOT a `Predicate`-equality index — it serves a lexical
/// scan via `GraphCore::match_ontology_terms` — so it `covers` nothing and never
/// `lookup`s; it exists in the registry purely so a planner can enumerate it.
#[derive(Debug, Default)]
pub struct OntologyIndexDescriptor;

impl SecondaryIndex for OntologyIndexDescriptor {
    fn kind(&self) -> IndexKind {
        IndexKind::Ontology
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn descriptor(&self) -> IndexDescriptor {
        IndexDescriptor {
            kind: IndexKind::Ontology,
            columns: IndexColumns::NonColumnar,
            serves_lookup: false,
        }
    }

    fn covers(&self, _predicate: &Predicate) -> bool {
        false
    }

    fn lookup(&self, _core: &GraphCore, _predicate: &Predicate) -> Option<Vec<String>> {
        None
    }

    fn apply_delta(&self, _core: &GraphCore, _change: &ChangeSet) -> Result<(), IndexError> {
        Ok(())
    }
}

/// Discoverable-only descriptor for the vector index (the `SemanticStore`: HNSW or,
/// under the `ann` feature, eg-ann IVF-PQ). It serves kNN via the semantic store's
/// own surface, not `Predicate` equality — so, like the ontology index, it `covers`
/// nothing and never `lookup`s; it is registered so a planner can ask "is there a
/// vector index?" through the one registry.
#[derive(Debug, Default)]
pub struct VectorIndexDescriptor;

impl SecondaryIndex for VectorIndexDescriptor {
    fn kind(&self) -> IndexKind {
        IndexKind::Vector
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn descriptor(&self) -> IndexDescriptor {
        IndexDescriptor {
            kind: IndexKind::Vector,
            columns: IndexColumns::NonColumnar,
            serves_lookup: false,
        }
    }

    fn covers(&self, _predicate: &Predicate) -> bool {
        false
    }

    fn lookup(&self, _core: &GraphCore, _predicate: &Predicate) -> Option<Vec<String>> {
        None
    }

    /// Incremental vector-store maintenance for a committed batch
    /// (CONCEPT:EG-KG.storage.incremental-ann). A REMOVED node's embedding must be
    /// tombstoned so kNN never returns a dead node — the `SemanticStore`
    /// (the native `eg-ann` backend) already inserts/overwrites
    /// incrementally via `add_embedding`, and now removes incrementally via
    /// `remove_embedding` (arena swap-remove + index tombstone, no full rebuild).
    ///
    /// Embedding ADDS/UPDATES do NOT flow through the topology write batch — they
    /// arrive on the separate `TxnAddEmbedding` op (already incremental) — so this
    /// delta actions removals only. Runs under the coalescer's topology lock, so a
    /// concurrent hybrid read sees topology + embedding removal atomically.
    fn apply_delta(&self, core: &GraphCore, change: &ChangeSet) -> Result<(), IndexError> {
        if change.removed_nodes.is_empty() {
            return Ok(());
        }
        let mut store = core.semantic_store.write();
        for id in &change.removed_nodes {
            store.remove_embedding(id);
        }
        Ok(())
    }

    /// Explicit recovery compaction for the vector store. A clean compaction drops
    /// tombstones so the resident index reflects exactly the live embedding set.
    fn full_rebuild(&self, core: &GraphCore) -> Result<(), IndexError> {
        core.semantic_store.read().force_compact();
        Ok(())
    }
}

/// Builds the SERVER-LAYER secondary indexes for one graph (CONCEPT:EG-KG.storage.incremental-text /
/// .incremental-temporal / .incremental-derived-owl). The text (`eg-text` Tantivy),
/// temporal (`eg-tsdb`), and derived-OWL (`eg-rdf` reasoner) indexes live ABOVE
/// `eg-core` in the crate DAG, so `eg-core` cannot name their concrete types — but
/// they implement [`SecondaryIndex`] (defined here), so the server layer hands them
/// in as boxed trait objects.
///
/// Mirrors [`crate::read_through::ReadThroughFactory`]: the [`crate::registry::GraphRegistry`]
/// calls [`for_graph`](Self::for_graph) once per graph — for every graph that already
/// exists when the factory is installed at startup, and for every graph created
/// afterward — and registers the returned indexes into that graph's `IndexManager`
/// via [`GraphCore::register_index`]. The committed-batch write path
/// ([`GraphCore::maintain_indexes`]) then drives their incremental
/// [`SecondaryIndex::apply_delta`] with no further per-write wiring.
pub trait SecondaryIndexFactory: Send + Sync {
    /// The server-layer secondary indexes for graph `name` (may be empty).
    fn for_graph(&self, name: &str) -> Vec<Box<dyn SecondaryIndex>>;
}

/// The single registry/seam over a graph's secondary indexes (CONCEPT:EG-KG.storage.index-manager-seam).
///
/// Owned by [`GraphCore`]. A planner consults ONE manager instead of bespoke
/// per-index checks:
///   * [`IndexManager::index_for`] — "which index covers this predicate?" (the
///     pushdown registry: eg-query / eg-plan ask this instead of hard-coding the
///     label/property checks).
///   * [`IndexManager::lookup`] — resolve a predicate through the covering index
///     (or `None` to full-scan).
///   * [`IndexManager::descriptors`] / [`IndexManager::descriptors_for_column`] —
///     "what indexes exist / cover column X?" (discovery, incl. the vector +
///     ontology indexes).
///
/// Invalidation stays where it always was: each concrete index's cache lives on
/// `GraphCore` and is dropped by `mark_dirty()`; the manager does not duplicate
/// that (`invalidate_all` is a hook the future stateful indexes can use).
///
/// **Two tiers of registered index.** The `indexes` vec is the FIXED set of
/// core-owned descriptors (label / property / ontology / vector), built once at
/// construction and read lock-free on the query hot path
/// (`index_for`/`lookup`/`descriptors`). The `server_indexes` vec holds the
/// SERVER-LAYER indexes (text / temporal / derived-OWL, CONCEPT:EG-KG.storage.incremental-text
/// etc.) registered AFTER construction by the [`SecondaryIndexFactory`] — they need
/// interior mutability (a graph gains them when the factory is installed), so they
/// sit behind an `RwLock`. They serve no `Predicate` lookup, so they never touch the
/// read hot path; only the write-batch maintenance loop ([`commit_batch`](Self::commit_batch)
/// / [`rebuild_all`](Self::rebuild_all)) iterates them.
pub struct IndexManager {
    indexes: Vec<Box<dyn SecondaryIndex>>,
    /// Server-layer indexes registered post-construction (text/temporal/derived-OWL).
    server_indexes: RwLock<Vec<Box<dyn SecondaryIndex>>>,
    /// Cached "some registered server index derives from node content" — read on the
    /// write hot path to decide whether the coalescer must capture property blobs.
    /// Set when a `needs_content` index registers; a single relaxed atomic load, no
    /// lock, keeps the vector-only default path zero-clone.
    content_capture: AtomicBool,
}

impl std::fmt::Debug for IndexManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut kinds: Vec<IndexKind> = self.indexes.iter().map(|i| i.kind()).collect();
        kinds.extend(self.server_indexes.read().iter().map(|i| i.kind()));
        f.debug_struct("IndexManager")
            .field("kinds", &kinds)
            .field(
                "content_capture",
                &self.content_capture.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::with_default_indexes()
    }
}

impl IndexManager {
    /// An empty manager (no indexes registered). Mostly for tests; production uses
    /// [`Self::with_default_indexes`].
    pub fn new() -> Self {
        Self {
            indexes: Vec::new(),
            server_indexes: RwLock::new(Vec::new()),
            content_capture: AtomicBool::new(false),
        }
    }

    /// The default registry every `GraphCore` gets: the equality-serving LABEL and
    /// PROPERTY indexes, plus the discoverable-only ONTOLOGY and VECTOR indexes.
    /// A new index type is added by registering it here (extension point) — the
    /// rest of the manager is generic over the registry.
    pub fn with_default_indexes() -> Self {
        let mut mgr = Self::new();
        mgr.register(Box::new(LabelIndex));
        mgr.register(Box::new(PropertyEqIndex));
        mgr.register(Box::new(OntologyIndexDescriptor));
        mgr.register(Box::new(VectorIndexDescriptor));
        mgr
    }

    /// Register a CORE secondary index (label/property/ontology/vector).
    /// Construction-time only — the core set is fixed for a graph's lifetime and is
    /// read lock-free on the query hot path.
    pub fn register(&mut self, index: Box<dyn SecondaryIndex>) {
        self.indexes.push(index);
    }

    /// Register a SERVER-LAYER secondary index (text/temporal/derived-OWL,
    /// CONCEPT:EG-KG.storage.incremental-text etc.) AFTER construction, via `&self`
    /// (the [`SecondaryIndexFactory`] installs these when a graph is created, and
    /// `GraphCore` is shared behind an `Arc`). If the index derives from node content
    /// ([`SecondaryIndex::needs_content`]) this flips the cached `content_capture`
    /// flag so the write coalescer starts capturing property blobs for this graph.
    pub fn register_server_index(&self, index: Box<dyn SecondaryIndex>) {
        if index.needs_content() {
            self.content_capture.store(true, Ordering::Relaxed);
        }
        self.server_indexes.write().push(index);
    }

    /// Does any registered server index derive from node content, so the coalescer
    /// must capture the property blob into each [`NodeChange`]? A single relaxed
    /// atomic load on the write hot path — `false` (the default, no content index
    /// registered) keeps the vector-only path zero-clone.
    #[inline]
    pub fn wants_change_content(&self) -> bool {
        self.content_capture.load(Ordering::Relaxed)
    }

    /// Look up a registered SERVER-LAYER index (text/temporal/derived-OWL,
    /// CONCEPT:EG-KG.query.served-text-index-binding) by `kind` and run `f` against it WHILE holding the
    /// read lock, returning `f`'s result. This is the planner-pushdown seam: a
    /// caller ABOVE `eg-core` (which cannot name the concrete server-side index
    /// types — they live above it in the crate DAG) recovers one via
    /// `idx.as_any().downcast_ref::<ConcreteType>()` inside `f`, searches/reads it
    /// directly, and returns an OWNED result — so no reference to the index (or
    /// the lock guard) ever escapes this call, keeping the seam lifetime-simple.
    /// `None` when no server-layer index of `kind` is registered on this graph
    /// (the caller falls back to its own snapshot-derived equivalent).
    pub fn with_server_index<F, R>(&self, kind: IndexKind, f: F) -> Option<R>
    where
        F: FnOnce(&dyn SecondaryIndex) -> R,
    {
        self.server_indexes
            .read()
            .iter()
            .find(|idx| idx.kind() == kind)
            .map(|idx| f(idx.as_ref()))
    }

    /// The pushdown registry's core question: the first registered index that
    /// covers `predicate`, or `None` if no index does (the caller full-scans). One
    /// seam — eg-query's `supports_filters_pushdown` and eg-plan's Filter leg ask
    /// THIS instead of hard-coding the label/property checks.
    pub fn index_for(&self, predicate: &Predicate) -> Option<&dyn SecondaryIndex> {
        self.indexes
            .iter()
            .map(|b| b.as_ref())
            .find(|idx| idx.covers(predicate))
    }

    /// Resolve `predicate` through its covering index, building the index's cache
    /// lazily over `core`. `None` ⇒ no covering index, OR the covering index
    /// refused under its policy (bounded cap) ⇒ the caller full-scans.
    pub fn lookup(&self, core: &GraphCore, predicate: &Predicate) -> Option<Vec<String>> {
        self.index_for(predicate)?.lookup(core, predicate)
    }

    /// Descriptors for every registered index — discovery for a planner ("what
    /// indexes exist?"), including the discoverable-only vector + ontology indexes.
    pub fn descriptors(&self) -> Vec<IndexDescriptor> {
        self.indexes.iter().map(|i| i.descriptor()).collect()
    }

    /// Descriptors of every index that covers `column` — "what indexes cover
    /// column X?". A `Fixed`-column index matches by name; an
    /// `AnyScalarDemandDriven` index (the property index) matches ANY column (it
    /// indexes on demand under the bound); a `NonColumnar` index never matches a
    /// column query.
    pub fn descriptors_for_column(&self, column: &str) -> Vec<IndexDescriptor> {
        self.descriptors()
            .into_iter()
            .filter(|d| match &d.columns {
                IndexColumns::Fixed(cols) => cols.iter().any(|c| c == column),
                IndexColumns::AnyScalarDemandDriven => true,
                IndexColumns::NonColumnar => false,
            })
            .collect()
    }

    /// Invalidate every registered index over `core`. The label/property indexes
    /// are invalidated by `GraphCore::mark_dirty()` (their cache lives there), so
    /// this is currently a no-op for them; it is the hook a FUTURE index whose
    /// cache lives in its own struct uses to clear on a write. Kept so the
    /// invalidation seam is single even as index types are added.
    pub fn invalidate_all(&self, core: &GraphCore) {
        let _ = core;
        // No registered index holds its own cache yet; future stateful indexes
        // clear theirs here. The label/property caches are cleared by mark_dirty.
    }

    /// Maintain every registered index for a committed write batch
    /// (CONCEPT:EG-KG.storage.write-changeset) through
    /// [`SecondaryIndex::apply_delta`]. Called by [`GraphCore::maintain_indexes`]
    /// inside the coalescer's topology lock so topology + index moves publish
    /// together. Returns a [`BatchMaintenance`] tally (deltas applied vs rebuilt).
    ///
    /// This convenience form is for callers that do not hold the topology lock.
    /// Lock-owning mutation paths use [`Self::commit_batch_at`] and supply their
    /// already-observed counts, avoiding a recursive graph-lock acquisition.
    pub fn commit_batch(&self, core: &GraphCore, change: &ChangeSet) -> BatchMaintenance {
        self.commit_batch_at(
            core,
            change,
            core.version().saturating_add(change.len() as u64),
            core.node_count() as u64,
            core.edge_count() as u64,
        )
    }

    /// Maintain indexes and publish an explicit target-version/completeness tuple.
    /// Compound operations such as public `BatchUpdate` advance the graph version
    /// once even when they contain many row mutations, while the write coalescer
    /// advances it once per independently acknowledged write.
    pub fn commit_batch_at(
        &self,
        core: &GraphCore,
        change: &ChangeSet,
        target_version: u64,
        node_count: u64,
        edge_count: u64,
    ) -> BatchMaintenance {
        let mut tally = BatchMaintenance::default();
        if !change.is_empty() {
            for idx in &self.indexes {
                match idx.apply_delta(core, change) {
                    Ok(()) => tally.deltas_applied += 1,
                    Err(_) => tally.failures += 1,
                }
            }
        }
        // Server-layer indexes (text/temporal/derived-OWL) — driven by the SAME batch
        // delta so text/tsdb/reason stay in step with the topology under one lock
        // (CONCEPT:EG-KG.storage.incremental-text / .incremental-temporal / .incremental-derived-owl).
        // Their `apply_delta` reads ONLY the ChangeSet's captured blobs — NEVER
        // `core`'s topology lock, which this batch already holds. A failure marks
        // the manifest stale and excludes the index from planner selection.
        for idx in self.server_indexes.read().iter() {
            let prior = idx.manifest();
            // A delta is safe only over an index that completely covers the
            // pre-mutation source. Never let one later delta bless a stale or
            // partially materialized index as complete.
            if idx.maintains_manifest() && !prior.covers(core.version()) {
                let mut stale = prior;
                stale.validity = IndexValidity::Stale;
                stale.completeness.complete = false;
                idx.publish_manifest(stale);
                tally.failures += 1;
                continue;
            }
            if change.is_empty() {
                // Vector-only batches leave server-derived content unchanged but
                // still advance the graph version. Carry forward known-complete
                // coverage without rebuilding or applying a phantom delta.
                idx.publish_manifest(IndexManifest::valid(target_version, node_count, edge_count));
                continue;
            }
            match idx.apply_delta(core, change) {
                Ok(()) => {
                    idx.publish_manifest(IndexManifest::valid(
                        target_version,
                        node_count,
                        edge_count,
                    ));
                    tally.deltas_applied += 1;
                }
                Err(_) => {
                    let mut manifest = idx.manifest();
                    manifest.validity = IndexValidity::Stale;
                    manifest.completeness.complete = false;
                    idx.publish_manifest(manifest);
                    tally.failures += 1;
                }
            }
        }
        tally
    }

    /// Rebuild every server-maintained index against one stable source snapshot,
    /// publishing `Valid` only after the build succeeds.  Failure remains
    /// explicit and planner-ineligible.
    pub fn rebuild_server_indexes(&self, core: &GraphCore) {
        let source_snapshot_version = core.version();
        let nodes = core.node_count() as u64;
        let edges = core.edge_count() as u64;
        for idx in self.server_indexes.read().iter() {
            idx.publish_manifest(IndexManifest::building(
                source_snapshot_version,
                IndexCompletenessCursor {
                    nodes: 0,
                    edges: 0,
                    complete: false,
                },
            ));
            match idx.full_rebuild(core) {
                Ok(()) => idx.publish_manifest(IndexManifest::valid(
                    source_snapshot_version,
                    nodes,
                    edges,
                )),
                Err(_) => {
                    let mut manifest = idx.manifest();
                    manifest.validity = IndexValidity::Failed;
                    manifest.completeness.complete = false;
                    idx.publish_manifest(manifest);
                }
            }
        }
    }

    /// Snapshot of every server index manifest for health/readiness reporting.
    pub fn server_manifests(&self) -> Vec<(IndexKind, IndexManifest)> {
        self.server_indexes
            .read()
            .iter()
            .map(|index| (index.kind(), index.manifest()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    fn graph() -> GraphCore {
        let g = GraphCore::new();
        g.add_node("a".into(), props(json!({"type": "Task", "team": "blue"})));
        g.add_node("b".into(), props(json!({"type": "Task", "team": "red"})));
        g.add_node("c".into(), props(json!({"type": "Person", "team": "blue"})));
        g
    }

    /// `index_for` routes a label predicate to the LABEL index and a property
    /// predicate to the PROPERTY index — the one-registry pushdown question.
    #[test]
    fn index_for_routes_predicate_to_the_right_index() {
        let g = graph();
        let mgr = g.indexes();
        assert_eq!(
            mgr.index_for(&Predicate::LabelEq("Task".into()))
                .map(|i| i.kind()),
            Some(IndexKind::Label)
        );
        assert_eq!(
            mgr.index_for(&Predicate::PropertyEq {
                key: "team".into(),
                value: "blue".into()
            })
            .map(|i| i.kind()),
            Some(IndexKind::Property)
        );
    }

    /// `lookup` through the manager resolves to the SAME ids the direct
    /// `GraphCore` methods return — the registry is a pure routing seam.
    #[test]
    fn lookup_matches_direct_graphcore_methods() {
        let g = graph();
        let mgr = g.indexes();

        let mut via_mgr = mgr
            .lookup(&g, &Predicate::LabelEq("Task".into()))
            .expect("label index always resolves");
        via_mgr.sort();
        let mut direct: Vec<String> = g
            .get_nodes_by_label("Task", 0)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        direct.sort();
        assert_eq!(via_mgr, direct);
        assert_eq!(via_mgr, vec!["a".to_string(), "b".to_string()]);

        let via_mgr = mgr.lookup(
            &g,
            &Predicate::PropertyEq {
                key: "team".into(),
                value: "blue".into(),
            },
        );
        assert_eq!(via_mgr, g.nodes_by_property("team", "blue"));
    }

    /// The bounded property cap surfaces THROUGH the manager: a cap-overflowing key
    /// returns `None` (caller full-scans), exactly as the direct path does.
    #[test]
    fn lookup_propagates_bounded_cap_refusal() {
        // Serialize against the `graph` property-index tests: this test pins the global
        // cap to 1, which would otherwise race a sibling default-cap test (shared
        // `crate::PROP_ENV_LOCK`, env is process-global under parallel test execution).
        let _env = crate::PROP_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        std::env::set_var("EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES", "1");
        std::env::remove_var("EPISTEMIC_GRAPH_INDEXED_PROPERTIES");
        let g = graph();
        let mgr = g.indexes();
        assert!(mgr
            .lookup(
                &g,
                &Predicate::PropertyEq {
                    key: "team".into(),
                    value: "blue".into()
                }
            )
            .is_some());
        assert!(
            mgr.lookup(
                &g,
                &Predicate::PropertyEq {
                    key: "type".into(),
                    value: "Task".into()
                }
            )
            .is_none(),
            "cap=1 must refuse a second key through the manager (full-scan fallback)"
        );
        std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES");
    }

    /// Discovery: "what indexes cover column X?". The label virtual column is
    /// covered by the LABEL index; an arbitrary property column is covered by the
    /// demand-driven PROPERTY index; the vector + ontology indexes are discoverable
    /// but non-columnar (never match a column query).
    #[test]
    fn descriptors_for_column_answers_discovery() {
        let g = graph();
        let mgr = g.indexes();

        let label_cov = mgr.descriptors_for_column(LABEL_COLUMN);
        assert!(label_cov.iter().any(|d| d.kind == IndexKind::Label));
        // The property index is demand-driven over any scalar column, so it also
        // "covers" the label virtual column name in the discovery sense.
        assert!(label_cov.iter().any(|d| d.kind == IndexKind::Property));

        let team_cov = mgr.descriptors_for_column("team");
        assert!(team_cov.iter().any(|d| d.kind == IndexKind::Property));
        // Vector / ontology are non-columnar — never returned for a column query.
        assert!(team_cov
            .iter()
            .all(|d| d.kind != IndexKind::Vector && d.kind != IndexKind::Ontology));

        // …but they ARE enumerable through the full descriptor list (discovery).
        let kinds: Vec<IndexKind> = mgr.descriptors().into_iter().map(|d| d.kind).collect();
        assert!(kinds.contains(&IndexKind::Vector));
        assert!(kinds.contains(&IndexKind::Ontology));
    }

    /// The discoverable-only indexes never serve a lookup (kNN / lexical surfaces),
    /// so `index_for` never routes an equality predicate to them.
    #[test]
    fn discoverable_only_indexes_do_not_serve_lookup() {
        let g = graph();
        let mgr = g.indexes();
        for d in mgr.descriptors() {
            match d.kind {
                IndexKind::Label | IndexKind::Property => assert!(d.serves_lookup),
                // Non-lookup surfaces (kNN / lexical / range / on-demand reason / bbox).
                IndexKind::Vector
                | IndexKind::Ontology
                | IndexKind::Text
                | IndexKind::Temporal
                | IndexKind::DerivedOwl
                | IndexKind::Spatial => assert!(!d.serves_lookup),
            }
        }
    }

    // ── Write-path maintenance seam (CONCEPT:EG-KG.storage.write-changeset / .index-manager-seam) ──

    use crate::compute::semantic::SemanticStore;

    fn v3(x: f32, y: f32, z: f32) -> Vec<f32> {
        vec![x, y, z]
    }

    /// The `ChangeSet` records each op shape distinctly and reports emptiness/length.
    #[test]
    fn changeset_records_each_op_shape() {
        let mut cs = ChangeSet::new();
        assert!(cs.is_empty());
        cs.record_add_node("a".into());
        cs.record_update_node("b".into());
        cs.record_remove_node("c".into());
        cs.record_add_edge("a".into(), "b".into());
        cs.record_remove_edge("b".into(), "a".into());
        assert!(!cs.is_empty());
        assert_eq!(cs.len(), 5);
        assert_eq!(cs.added_nodes.len(), 1);
        assert_eq!(cs.updated_nodes[0].id, "b");
        assert_eq!(cs.removed_nodes, vec!["c".to_string()]);
        assert_eq!(cs.added_edges[0].source, "a");
        assert_eq!(cs.removed_edges[0].target, "a");
    }

    /// `commit_batch` invokes the one current incremental behavior on every
    /// registered index; the vector index removes the embedding in-place.
    #[test]
    fn commit_batch_invokes_apply_delta_and_removes_embedding() {
        let g = GraphCore::new();
        {
            let mut s = g.semantic_store.write();
            s.add_embedding("a".into(), v3(1.0, 0.0, 0.0));
            s.add_embedding("b".into(), v3(0.0, 1.0, 0.0));
            s.add_embedding("c".into(), v3(0.0, 0.0, 1.0));
        }
        let mut cs = ChangeSet::new();
        cs.record_remove_node("b".into());

        let tally = g.indexes().commit_batch(&g, &cs);
        assert!(
            tally.deltas_applied >= 1,
            "vector apply_delta must run: {tally:?}"
        );
        assert_eq!(
            tally.failures, 0,
            "all current deltas must succeed: {tally:?}"
        );

        let s = g.semantic_store.read();
        assert!(
            s.get_embedding("b").is_none(),
            "removed node's embedding must be gone"
        );
        assert!(s.get_embedding("a").is_some());
        assert!(s.get_embedding("c").is_some());
    }

    /// A test server index that records the ids it saw, keyed off the ChangeSet's
    /// captured content blob — the shape the text/temporal adapters use.
    #[derive(Default)]
    struct RecordingServerIndex {
        seen_content: std::sync::Mutex<Vec<String>>,
        needs: bool,
    }
    impl SecondaryIndex for RecordingServerIndex {
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
        fn lookup(&self, _c: &GraphCore, _p: &Predicate) -> Option<Vec<String>> {
            None
        }
        fn needs_content(&self) -> bool {
            self.needs
        }
        fn apply_delta(&self, _c: &GraphCore, change: &ChangeSet) -> Result<(), IndexError> {
            let mut seen = self.seen_content.lock().unwrap();
            for nc in change.added_nodes.iter().chain(change.updated_nodes.iter()) {
                if let Some(blob) = &nc.properties_msgpack {
                    let v: serde_json::Value = rmp_serde::from_slice(blob).unwrap();
                    seen.push(format!("{}={}", nc.id, v["text"].as_str().unwrap_or("")));
                }
            }
            Ok(())
        }
    }

    /// Registering a `needs_content` server index flips `wants_change_content`, and a
    /// committed batch drives its `apply_delta` through the manager (the server-index
    /// registration seam — CONCEPT:EG-KG.storage.incremental-text).
    #[test]
    fn server_index_registration_drives_apply_delta_and_content_flag() {
        let g = GraphCore::new();
        assert!(
            !g.wants_change_content(),
            "no content index registered yet ⇒ hot path stays zero-clone"
        );
        let rec = std::sync::Arc::new(RecordingServerIndex {
            needs: true,
            ..Default::default()
        });
        // Register a cloneable handle so the test can inspect what the index saw.
        struct Shared(std::sync::Arc<RecordingServerIndex>);
        impl SecondaryIndex for Shared {
            fn kind(&self) -> IndexKind {
                self.0.kind()
            }
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn descriptor(&self) -> IndexDescriptor {
                self.0.descriptor()
            }
            fn covers(&self, p: &Predicate) -> bool {
                self.0.covers(p)
            }
            fn lookup(&self, c: &GraphCore, p: &Predicate) -> Option<Vec<String>> {
                self.0.lookup(c, p)
            }
            fn needs_content(&self) -> bool {
                self.0.needs_content()
            }
            fn apply_delta(&self, c: &GraphCore, ch: &ChangeSet) -> Result<(), IndexError> {
                self.0.apply_delta(c, ch)
            }
        }
        g.register_index(Box::new(Shared(rec.clone())));
        assert!(
            g.wants_change_content(),
            "a needs_content server index flips the capture flag"
        );

        // Drive a batch carrying a captured content blob.
        let mut cs = ChangeSet::new();
        cs.added_nodes.push(NodeChange::with_properties(
            "a".into(),
            props(json!({"text": "hello world"})),
        ));
        let tally = g.maintain_indexes(&cs);
        assert!(tally.deltas_applied >= 1, "server index applied: {tally:?}");
        assert_eq!(
            rec.seen_content.lock().unwrap().as_slice(),
            ["a=hello world"]
        );
    }

    /// An empty change short-circuits and touches nothing.
    #[test]
    fn commit_batch_empty_is_noop() {
        let g = GraphCore::new();
        g.semantic_store
            .write()
            .add_embedding("a".into(), v3(1.0, 0.0, 0.0));
        let tally = g.indexes().commit_batch(&g, &ChangeSet::new());
        assert_eq!(tally, BatchMaintenance::default());
        assert!(g.semantic_store.read().get_embedding("a").is_some());
    }

    /// EQUIVALENCE (CONCEPT:EG-KG.storage.incremental-ann): incremental removal through the seam
    /// yields the SAME kNN result set as a store rebuilt from only the surviving
    /// embeddings. Exact brute-force regime (N well below the ANN build threshold)
    /// so the comparison is deterministic.
    #[test]
    fn incremental_removal_equals_full_rebuild_baseline() {
        let g = GraphCore::new();
        let vectors: Vec<(String, Vec<f32>)> = (0..64)
            .map(|i| {
                (
                    format!("n{i}"),
                    v3(i as f32, (i % 7) as f32 * 0.1, (i % 3) as f32 * 0.1),
                )
            })
            .collect();
        {
            let mut s = g.semantic_store.write();
            for (id, v) in &vectors {
                s.add_embedding(id.clone(), v.clone());
            }
        }

        // Remove every 5th node through the maintenance seam; keep the survivors.
        let mut cs = ChangeSet::new();
        let mut survivors: Vec<(String, Vec<f32>)> = Vec::new();
        for (i, (id, v)) in vectors.iter().enumerate() {
            if i % 5 == 0 {
                cs.record_remove_node(id.clone());
            } else {
                survivors.push((id.clone(), v.clone()));
            }
        }
        g.maintain_indexes(&cs);

        let query = v3(41.0, 0.3, 0.1);
        let mut inc: Vec<String> = g
            .semantic_store
            .read()
            .semantic_search(&query, 8)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        inc.sort();

        // No removed id may surface.
        for i in (0..64).step_by(5) {
            assert!(
                !inc.contains(&format!("n{i}")),
                "removed node n{i} must not appear: {inc:?}"
            );
        }

        // Baseline: a store rebuilt from ONLY the survivors.
        let mut base = SemanticStore::new();
        for (id, v) in &survivors {
            base.add_embedding(id.clone(), v.clone());
        }
        let mut baseline: Vec<String> = base
            .semantic_search(&query, 8)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        baseline.sort();

        assert_eq!(
            inc, baseline,
            "incremental delete result set must equal a full rebuild baseline"
        );
    }

    // ── `with_server_index` downcast seam (CONCEPT:EG-KG.query.served-text-index-binding) ────────────

    /// A tiny concrete server-layer index the downcast test recovers by type.
    #[derive(Default)]
    struct ProbeIndex {
        tag: std::sync::Mutex<String>,
    }
    impl SecondaryIndex for ProbeIndex {
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
        fn lookup(&self, _c: &GraphCore, _p: &Predicate) -> Option<Vec<String>> {
            None
        }
        fn apply_delta(&self, _c: &GraphCore, _change: &ChangeSet) -> Result<(), IndexError> {
            Ok(())
        }
    }

    /// `with_server_index` finds a registered server-layer index by [`IndexKind`] and
    /// hands it to the closure as `&dyn SecondaryIndex`, which downcasts it back to its
    /// CONCRETE type via `as_any` — the exact seam the planner pushdown uses to reach a
    /// server-layer text/vector/spatial index instead of rebuilding an equivalent one
    /// from a snapshot per query.
    #[test]
    fn with_server_index_finds_and_downcasts_a_registered_index() {
        let g = GraphCore::new();
        g.register_index(Box::new(ProbeIndex {
            tag: std::sync::Mutex::new("hello".to_string()),
        }));

        let seen = g.indexes().with_server_index(IndexKind::Text, |idx| {
            idx.as_any()
                .downcast_ref::<ProbeIndex>()
                .map(|p| p.tag.lock().unwrap().clone())
        });
        assert_eq!(
            seen,
            Some(Some("hello".to_string())),
            "with_server_index must find the registered Text index and downcast it back \
             to ProbeIndex"
        );
    }

    /// No registered server index of that kind ⇒ `None` (the caller falls back to its
    /// own snapshot-derived equivalent).
    #[test]
    fn with_server_index_is_none_when_nothing_registered() {
        let g = GraphCore::new();
        let seen = g
            .indexes()
            .with_server_index(IndexKind::Text, |_idx| "unreachable");
        assert!(seen.is_none());
    }

    /// A registered index of the WRONG kind is never matched — `with_server_index` is
    /// keyed by `kind()`, not just "any server index".
    #[test]
    fn with_server_index_does_not_match_the_wrong_kind() {
        let g = GraphCore::new();
        g.register_index(Box::new(ProbeIndex::default())); // kind() == Text
        let seen = g
            .indexes()
            .with_server_index(IndexKind::Temporal, |_idx| "unreachable");
        assert!(
            seen.is_none(),
            "Temporal lookup must not match a Text index"
        );
    }
}
