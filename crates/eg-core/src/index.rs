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

use crate::graph::GraphCore;

/// Is incremental heavy-index maintenance enabled process-wide? Read ONCE from
/// `EPISTEMIC_GRAPH_INCREMENTAL_INDEX` and cached, so the write hot path pays a
/// single relaxed atomic load rather than an env lookup per batch. Default ON; the
/// kill-switch values `0|false|off|no` select the legacy invalidate-and-rebuild
/// path (CONCEPT:EG-KG.storage.write-changeset).
pub fn incremental_index_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        !matches!(
            std::env::var("EPISTEMIC_GRAPH_INCREMENTAL_INDEX")
                .ok()
                .map(|v| v.trim().to_ascii_lowercase())
                .as_deref(),
            Some("0") | Some("false") | Some("off") | Some("no")
        )
    })
}

/// One node touched by a committed write batch (CONCEPT:EG-KG.storage.write-changeset). Carries
/// the node id and — for adds/updates — OPTIONALLY the new property blob, so a
/// content-derived index (text / temporal) can compute its own delta without
/// re-reading the graph. The coalescer leaves `properties_msgpack` as `None` today
/// (the only wired consumer, the vector store, keys off removals), keeping the hot
/// path free of a per-op blob clone; a future text/temporal index that needs the
/// content sets a capture flag (see the write-coalescer plumbing).
#[derive(Debug, Clone)]
pub struct NodeChange {
    pub id: String,
    pub properties_msgpack: Option<Vec<u8>>,
}

impl NodeChange {
    pub fn new(id: String) -> Self {
        Self {
            id,
            properties_msgpack: None,
        }
    }
    pub fn with_properties(id: String, properties_msgpack: Vec<u8>) -> Self {
        Self {
            id,
            properties_msgpack: Some(properties_msgpack),
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
    /// This index has no incremental path for the given change shape — the
    /// [`IndexManager`] falls back to [`SecondaryIndex::full_rebuild`]. This is the
    /// DEFAULT for the lazy label/property/ontology indexes in D1.
    DeltaUnsupported,
    /// The incremental apply itself failed; the manager falls back to a full rebuild.
    Failed(String),
}

impl std::fmt::Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexError::DeltaUnsupported => write!(f, "index delta unsupported"),
            IndexError::Failed(e) => write!(f, "index delta failed: {e}"),
        }
    }
}

impl std::error::Error for IndexError {}

/// Per-batch maintenance tally returned by [`IndexManager::commit_batch`] — how
/// many registered indexes applied their delta incrementally vs fell back to a
/// full rebuild. Mainly for the seam's tests / in-process diagnostics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchMaintenance {
    pub deltas_applied: usize,
    pub rebuilds: usize,
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
    // Future index kinds register here (CONCEPT:AU-KG.query.text-spatial-time text / spatial / time)
    // with their own `SecondaryIndex` impl — the manager core does not change.
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
pub trait SecondaryIndex: Send + Sync {
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

    // ── Write-path maintenance seam (CONCEPT:EG-KG.storage.index-manager-seam) ─────────────
    //
    // These give the ONE registry a write side to match its read side: on a
    // committed write batch the [`IndexManager`] calls `apply_delta` on each
    // registered index, falling back to `full_rebuild` when a delta can't be
    // applied. Both default to the lazy-index behavior (no incremental path,
    // rebuild-on-read), so the label/property/ontology descriptors inherit the
    // exact pre-seam semantics; an index with real incremental state (the vector
    // store) overrides `apply_delta`.

    /// Incrementally apply a committed batch's `change` to this index's state.
    ///
    /// The DEFAULT returns [`IndexError::DeltaUnsupported`], which tells the manager
    /// to fall back to [`full_rebuild`](Self::full_rebuild) — the behavior the lazy
    /// label / property / path / ontology indexes keep (they rebuild on read). A
    /// concrete incremental index (the vector `SemanticStore`) overrides this to
    /// upsert / tombstone in place with NO full rebuild.
    fn apply_delta(&self, core: &GraphCore, change: &ChangeSet) -> Result<(), IndexError> {
        let _ = (core, change);
        Err(IndexError::DeltaUnsupported)
    }

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
}

/// The label index descriptor (CONCEPT:EG-KG.compute.consult-lazy). Holds no state — it routes to
/// `GraphCore::get_nodes_by_label`, which owns the lazy `label_index` cache.
#[derive(Debug, Default)]
pub struct LabelIndex;

impl SecondaryIndex for LabelIndex {
    fn kind(&self) -> IndexKind {
        IndexKind::Label
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
    /// (hnsw_rs default / eg-ann IVF-PQ under `ann`) already inserts/overwrites
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

    /// Fallback for the vector store. `apply_delta` never errors (a removal always
    /// succeeds or is absent), so this is only reachable via
    /// [`IndexManager::rebuild_all`]. A clean compaction drops any tombstones so the
    /// resident index reflects exactly the live embedding set.
    fn full_rebuild(&self, core: &GraphCore) -> Result<(), IndexError> {
        core.semantic_store.read().force_compact();
        Ok(())
    }
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
/// that (`invalidate_all` is a hook the future stateful indexes can use). The
/// manager itself is immutable after construction (a fixed registry), so it needs
/// no interior locking.
pub struct IndexManager {
    indexes: Vec<Box<dyn SecondaryIndex>>,
}

impl std::fmt::Debug for IndexManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexManager")
            .field(
                "kinds",
                &self.indexes.iter().map(|i| i.kind()).collect::<Vec<_>>(),
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

    /// Register a secondary index. Construction-time only (the registry is fixed
    /// for a graph's lifetime).
    pub fn register(&mut self, index: Box<dyn SecondaryIndex>) {
        self.indexes.push(index);
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
    /// (CONCEPT:EG-KG.storage.write-changeset): for each index try the incremental
    /// [`SecondaryIndex::apply_delta`]; on any error fall back to
    /// [`SecondaryIndex::full_rebuild`]. Called by [`GraphCore::maintain_indexes`]
    /// inside the coalescer's topology lock so topology + index moves publish
    /// together. Returns a [`BatchMaintenance`] tally (deltas applied vs rebuilt).
    ///
    /// An empty change short-circuits (a no-op batch touches nothing). The lazy
    /// label/property/path caches are invalidated separately by the caller via
    /// [`GraphCore::invalidate_indexes`]; their descriptors here take the default
    /// `DeltaUnsupported` → no-op `full_rebuild`, so they cost nothing in this loop.
    pub fn commit_batch(&self, core: &GraphCore, change: &ChangeSet) -> BatchMaintenance {
        let mut tally = BatchMaintenance::default();
        if change.is_empty() {
            return tally;
        }
        for idx in &self.indexes {
            match idx.apply_delta(core, change) {
                Ok(()) => tally.deltas_applied += 1,
                Err(_) => {
                    // Best-effort fallback: a failed rebuild is logged by the index
                    // and leaves it to rebuild-on-read; never poisons the batch.
                    let _ = idx.full_rebuild(core);
                    tally.rebuilds += 1;
                }
            }
        }
        tally
    }

    /// Full-rebuild every registered index over `core` (the maintenance / kill-switch
    /// escape hatch). Ignores per-index errors — a stateful index that can't rebuild
    /// falls back to its own rebuild-on-read.
    pub fn rebuild_all(&self, core: &GraphCore) {
        for idx in &self.indexes {
            let _ = idx.full_rebuild(core);
        }
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
                IndexKind::Vector | IndexKind::Ontology => assert!(!d.serves_lookup),
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

    /// The kill-switch defaults to incremental ON when the env var is unset.
    #[test]
    fn incremental_index_enabled_defaults_on_when_unset() {
        if std::env::var_os("EPISTEMIC_GRAPH_INCREMENTAL_INDEX").is_none() {
            assert!(incremental_index_enabled());
        }
    }

    /// `commit_batch` invokes `apply_delta` on every registered index: the vector
    /// index applies a real delta (removes the embedding), the lazy/discoverable
    /// descriptors fall back (default `DeltaUnsupported` → no-op rebuild).
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
        // The vector descriptor applied a delta; the label/property/ontology ones
        // returned DeltaUnsupported and fell back (their lazy caches invalidate
        // separately via GraphCore::invalidate_indexes).
        assert!(
            tally.deltas_applied >= 1,
            "vector apply_delta must run: {tally:?}"
        );
        assert!(tally.rebuilds >= 1, "lazy descriptors fall back: {tally:?}");

        let s = g.semantic_store.read();
        assert!(
            s.get_embedding("b").is_none(),
            "removed node's embedding must be gone"
        );
        assert!(s.get_embedding("a").is_some());
        assert!(s.get_embedding("c").is_some());
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
}
