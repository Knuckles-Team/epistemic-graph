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
}
