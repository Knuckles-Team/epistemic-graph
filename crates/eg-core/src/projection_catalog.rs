// CONCEPT:EG-KG.query.named-graph-projection-catalog (W4.5 / N5) — the `gds.graph.project`-
// equivalent: a NAMED, MATERIALIZED graph projection an algorithm can be re-run against
// without re-scanning the live graph view, invalidated on write using the SAME W1.6 DepClock
// discipline the result cache's dependency-scoped entries use (`crate::dep_scope`) — not a
// parallel invalidation scheme.
//
// ## Why this is a distinct cache from `result_cache`
//
// `ResultCache` caches the SERIALIZED OUTPUT of a whole query (rows/bytes) keyed by query
// identity. A named projection is different in kind: it is an INPUT artifact — a materialized
// `eg_compute::graph_algos::AdjacencyGraph` — that MULTIPLE DIFFERENT algorithms (`gds.pageRank`,
// `gds.louvain`, `gds.betweenness`, …) each read as their starting structure. Caching only the
// final rows of ONE algorithm call would not save the NEXT, DIFFERENT algorithm from re-scanning
// the graph into its own adjacency structure — the win Neo4j GDS's `gds.graph.project` catalog
// gives is exactly this: materialize the projection ONCE, run MANY algorithms against it. Hence
// a separate, name-keyed catalog living alongside `result_cache` on `GraphCore`.
//
// ## Type erasure across the crate DAG
//
// `eg-core` sits BELOW `eg-compute` in the workspace dependency DAG (`eg-types → eg-core →
// eg-compute → …`), so it cannot name `eg_compute::graph_algos::AdjacencyGraph` directly — doing
// so would create a cycle. Exactly like `GraphView::plan_stats_memo` (which erases eg-plan's
// `ColumnStats` through `dyn Any`), a projection's materialized value is stored as `Arc<dyn Any +
// Send + Sync>` and downcast by the PRODUCING crate (`eg-query`'s `gds.rs`, which depends on
// `eg-compute` and therefore knows the concrete type).
//
// ## Invalidation — reuses `DepClock`, not a parallel scheme
//
// A projection's [`Entry`] carries the [`DepSet`] it depends on (today always the whole graph —
// `[Dim::AllNodes, Dim::AllEdges]`, since `gds.graph.project` scans every resident node + edge)
// and the graph `version()` it was materialized at. [`ProjectionCatalog::get`] revalidates via
// `DepClock::is_valid` — the IDENTICAL check `ResultCache::get_dep` performs — so a projection
// survives any write DISJOINT from its dependency set and is evicted the moment one overlaps (or
// the clock floors on an un-attributable write). No separate write-path hook is added: the SAME
// `note_footprint`/`note_version_bump` calls that already feed the result cache's `DepClock`
// (`crate::graph::GraphCore::invalidate_indexes_for_change` / `mark_dirty`) are the only feed —
// see `crate::graph::ProjectionScope`, the handle that carries a shared `Arc` to both this
// catalog and that SAME clock instance into a read-only `GraphView`.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::dep_scope::{DepClock, DepSet};

/// One named, materialized projection.
struct Entry {
    /// The type-erased materialized value (an `eg_compute::graph_algos::AdjacencyGraph<String>`
    /// in practice) — downcast by the producing crate.
    blob: Arc<dyn Any + Send + Sync>,
    /// The graph `version()` this projection reflects — the reference point `DepClock::is_valid`
    /// compares its dependency dimensions' last-write versions against.
    computed_at: u64,
    /// The dependency set this projection covers, validated against the graph's `DepClock` on
    /// each lookup (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation).
    deps: DepSet,
    /// Node/edge counts at materialization time (observability — `gds.graph.list`).
    node_count: usize,
    edge_count: usize,
    /// The `relationshipWeightProperty` this projection was built with, if any — surfaced so a
    /// caller can confirm a named projection matches the weighting it expects (`gds.graph.list`).
    weight_property: Option<String>,
}

/// The named-projection catalog (CONCEPT:EG-KG.query.named-graph-projection-catalog). One
/// instance per `GraphCore`, `Arc`-shared into every [`crate::graph::GraphView`] snapshot via
/// [`crate::graph::ProjectionScope`] so code holding only a view (never the live, lock-bearing
/// `GraphCore`) can still reach it. Bounded only by how many DISTINCT names a caller
/// materializes — a projection itself is O(V+E), so this is a power-user surface, not an
/// auto-populated cache; `gds.graph.drop` reclaims memory explicitly (mirroring Neo4j GDS).
#[derive(Default)]
pub struct ProjectionCatalog {
    inner: Mutex<HashMap<String, Entry>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl std::fmt::Debug for ProjectionCatalog {
    /// Mirrors `ResultCache`'s `Debug` impl: summary counters, not the (non-`Debug`, type-erased)
    /// entry contents — `GraphCore` derives `Debug` and holds this behind an `Arc`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (hits, misses) = self.stats();
        f.debug_struct("ProjectionCatalog")
            .field("len", &self.len())
            .field("hits", &hits)
            .field("misses", &misses)
            .finish()
    }
}

impl ProjectionCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Materialize (or overwrite) a named projection.
    #[allow(clippy::too_many_arguments)]
    pub fn put(
        &self,
        name: &str,
        blob: Arc<dyn Any + Send + Sync>,
        computed_at: u64,
        deps: DepSet,
        node_count: usize,
        edge_count: usize,
        weight_property: Option<String>,
    ) {
        self.inner.lock().insert(
            name.to_string(),
            Entry {
                blob,
                computed_at,
                deps,
                node_count,
                edge_count,
                weight_property,
            },
        );
        // GOOD LOGS: materialization is the expensive O(V+E) step this whole catalog exists to
        // amortize away on the NEXT call — worth a line every time it actually happens.
        tracing::debug!(
            target: "epistemic_graph::gds_projection",
            name,
            computed_at,
            node_count,
            edge_count,
            "named graph projection materialized"
        );
    }

    /// Look up a named projection, revalidating it against `clock`
    /// (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation — the SAME DepClock
    /// discipline `ResultCache::get_dep` uses). `Some` = HIT: the caller reuses the materialized
    /// value and skips its own scan. `None` = MISS: absent, or a write since `computed_at`
    /// touched the dependency set — a stale entry is evicted so a subsequent `put` replaces it
    /// cleanly instead of leaking the old blob.
    pub fn get(&self, name: &str, clock: &DepClock) -> Option<Arc<dyn Any + Send + Sync>> {
        let mut inner = self.inner.lock();
        let valid = match inner.get(name) {
            Some(entry) => clock.is_valid(&entry.deps, entry.computed_at),
            None => {
                drop(inner);
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        if !valid {
            inner.remove(name);
            drop(inner);
            self.misses.fetch_add(1, Ordering::Relaxed);
            // GOOD LOGS: the invalidation decision (mirrors `ResultCache::get_dep`'s eviction log)
            // — a write since materialization touched this projection's dependency set.
            tracing::debug!(
                target: "epistemic_graph::gds_projection",
                name,
                "named graph projection invalidated: a dependency was written since materialization"
            );
            return None;
        }
        let blob = Arc::clone(&inner.get(name).expect("present checked above").blob);
        drop(inner);
        self.hits.fetch_add(1, Ordering::Relaxed);
        // GOOD LOGS: the reuse decision — the exact signal the projection-reuse bench asserts on
        // (a repeated/different algo call against the same name skipped the projection scan).
        tracing::debug!(
            target: "epistemic_graph::gds_projection",
            name,
            "named graph projection reused: scan skipped"
        );
        Some(blob)
    }

    /// Drop a named projection (`gds.graph.drop`). `true` iff it existed.
    pub fn drop_projection(&self, name: &str) -> bool {
        self.inner.lock().remove(name).is_some()
    }

    /// Whether a named projection is currently cataloged, WITHOUT validating it against a clock
    /// (`gds.graph.exists` — Neo4j GDS semantics: catalog membership, not freshness).
    pub fn exists(&self, name: &str) -> bool {
        self.inner.lock().contains_key(name)
    }

    /// `(name, node_count, edge_count, weight_property)` for every cataloged projection, sorted
    /// by name (`gds.graph.list`; deterministic order — no per-entry freshness check, mirroring
    /// [`Self::exists`]).
    pub fn list(&self) -> Vec<(String, usize, usize, Option<String>)> {
        let inner = self.inner.lock();
        let mut rows: Vec<_> = inner
            .iter()
            .map(|(name, e)| {
                (
                    name.clone(),
                    e.node_count,
                    e.edge_count,
                    e.weight_property.clone(),
                )
            })
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    }

    /// `(hits, misses)` since construction — the proof counters a reuse bench/test reads to show
    /// a second algo call against the SAME name skipped materialization.
    pub fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }

    /// Number of currently cataloged projections (observability/tests).
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dep_scope::{Dim, WriteFootprint};

    fn whole_graph_deps() -> DepSet {
        DepSet::new(vec![Dim::AllNodes, Dim::AllEdges])
    }

    #[test]
    fn put_then_get_hits() {
        let cat = ProjectionCatalog::new();
        let clock = DepClock::new();
        cat.put("g", Arc::new(42i32), 0, whole_graph_deps(), 3, 2, None);
        let got = cat.get("g", &clock).expect("hit");
        assert_eq!(*got.downcast_ref::<i32>().unwrap(), 42);
        assert_eq!(cat.stats(), (1, 0));
    }

    #[test]
    fn miss_on_absent_name() {
        let cat = ProjectionCatalog::new();
        let clock = DepClock::new();
        assert!(cat.get("nope", &clock).is_none());
        assert_eq!(cat.stats(), (0, 1));
    }

    #[test]
    fn write_touching_deps_invalidates() {
        let cat = ProjectionCatalog::new();
        let clock = DepClock::new();
        cat.put("g", Arc::new(1i32), 0, whole_graph_deps(), 1, 0, None);
        assert!(cat.get("g", &clock).is_some());

        // A node write at version 1 touches AllNodes.
        clock.note_footprint(
            &WriteFootprint {
                node_changed: true,
                ..Default::default()
            },
            1,
        );
        clock.note_version_bump(1);

        assert!(cat.get("g", &clock).is_none(), "must invalidate");
        assert!(
            !cat.exists("g"),
            "stale entry must be evicted on invalidation, not merely reported invalid"
        );
        // One hit (the pre-write `get`) then one miss (the post-write, now-invalid `get`).
        assert_eq!(
            cat.stats(),
            (1, 1),
            "the invalidated lookup counts as a miss"
        );
    }

    #[test]
    fn disjoint_write_survives() {
        // A projection's whole-graph deps ([AllNodes, AllEdges]) intersect every node/edge
        // write by construction — this test documents that today there is no write which
        // leaves BOTH untouched (see the module doc's DAG note), by checking the sibling
        // guarantee dep_scope itself proves: a footprinted write does not FLOOR (so an
        // (as-yet-hypothetical) narrower future projection's disjoint deps would survive).
        let cat = ProjectionCatalog::new();
        let clock = DepClock::new();
        let narrow = DepSet::new(vec![Dim::Label("Other".to_string())]);
        cat.put("g", Arc::new(1i32), 0, narrow, 1, 0, None);

        clock.note_footprint(
            &WriteFootprint {
                labels: vec!["Unrelated".to_string()],
                node_changed: true,
                ..Default::default()
            },
            1,
        );
        clock.note_version_bump(1);

        assert!(
            cat.get("g", &clock).is_some(),
            "a disjoint label write must not invalidate a narrower-scoped projection"
        );
    }

    #[test]
    fn drop_and_exists() {
        let cat = ProjectionCatalog::new();
        assert!(!cat.exists("g"));
        cat.put("g", Arc::new(1i32), 0, whole_graph_deps(), 1, 0, None);
        assert!(cat.exists("g"));
        assert!(cat.drop_projection("g"));
        assert!(!cat.exists("g"));
        assert!(!cat.drop_projection("g"), "second drop reports absent");
    }

    #[test]
    fn list_sorted_by_name_with_metadata() {
        let cat = ProjectionCatalog::new();
        cat.put("zeta", Arc::new(1i32), 0, whole_graph_deps(), 1, 0, None);
        cat.put(
            "alpha",
            Arc::new(1i32),
            0,
            whole_graph_deps(),
            2,
            1,
            Some("w".to_string()),
        );
        let rows = cat.list();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], ("alpha".to_string(), 2, 1, Some("w".to_string())));
        assert_eq!(rows[1], ("zeta".to_string(), 1, 0, None));
    }

    #[test]
    fn overwrite_replaces_entry() {
        let cat = ProjectionCatalog::new();
        let clock = DepClock::new();
        cat.put("g", Arc::new(1i32), 0, whole_graph_deps(), 1, 0, None);
        cat.put("g", Arc::new(2i32), 0, whole_graph_deps(), 5, 4, None);
        assert_eq!(cat.len(), 1);
        let got = cat.get("g", &clock).unwrap();
        assert_eq!(*got.downcast_ref::<i32>().unwrap(), 2);
    }
}
