// CONCEPT:EG-KG.sharding.row-level-security (perf/cold-query-floor-analysis) — bounded
// per-actor cache for the RLS-FILTERED `GraphView` that `Method::CypherQuery` (and its
// SQL/SPARQL/GraphQL/RDF siblings) build on every cache-miss read.
//
// ## Why this exists
//
// The `~900ms` fixed per-cold-query floor localized by this lane is dominated by
// `IsolationLayer::filter_view` (`crates/eg-core/src/isolation.rs`): for EVERY node in
// the snapshot (not just the rows a query's WHERE clause ultimately matches),
// `can_see_node` calls `row_visibility(blob)`, which runs a FULL bounded msgpack decode
// of that node's property blob into a `BTreeMap<String, serde_json::Value>` just to read
// 2-3 small RLS metadata keys (`_owner`/`_visibility`/`_shared_scope`). That is the exact
// same shape of bug `build_cypher_label_index` had before `perf/warm-label-index`
// (commit `2662713b`) fixed it for the label index — an O(V) msgpack-decode-every-node
// pass — except this one is NOT memoized anywhere: `Method::CypherQuery`'s cache-miss
// branch (`src/server/handlers/query.rs`) calls `core.analysis_snapshot_versioned()` +
// `rls.filter_view(caller, &mut snap)` FRESH on every miss of the whole-RESULT cache,
// even though the filtered view for a given (actor, version) pair is a pure function of
// data the engine has already computed for one caller a moment ago.
//
// Measured live (graph-os pod, `epistemic_graph_projection_cache_miss_build_seconds`,
// which pays the SAME `filter_view` cost as one ingredient of the larger `project_core`
// rebuild `HasNode` triggers): 298 misses, mean 1.10s/miss — matching the reported
// 900ms-1.2s floor almost exactly, on a live graph in the 27k-69k node range.
//
// ## What this does
//
// Cache the RLS-filtered `Arc<GraphView>` PER ACTOR, invalidated the moment the source
// graph's `version()` advances — the SAME invalidate-on-version-change idiom
// `GraphCore::label_index`/`ontology_index` use, and the SAME per-actor + whole-image
// `generation` idiom `crate::rls_projection_cache::ProjectionCache` already uses for
// `project_core`'s cached `Arc<GraphCore>` (this module is a structural sibling of that
// one, holding a lighter-weight `GraphView` instead of a second whole `GraphCore`).
//
// Correctness / RLS safety: a cache hit fires ONLY when (actor, version) match exactly,
// mirroring `ProjectionCache::get`'s proof obligation — the filtered view is a pure
// function of (source graph content, actor's ACL grants) at a given version, so a hit is
// byte-identical to a fresh `filter_view` rebuild for that same (actor, version) pair.
// Two distinct actors NEVER share or evict each other's entry (keyed by actor id, see
// `distinct_actors_do_not_share_or_evict_each_other`-equivalent test below). A
// same-version whole-image transition (`GraphCore::replace_snapshot`/`clear`/
// `hibernate`) that does not advance `version()` is caught by the SAME `generation`
// counter `ProjectionCache` uses for exactly this reason (U-142/U-143/U-145) — see
// `GraphCore::invalidate_projection_cache`'s call sites, which this cache's own
// `GraphCore::invalidate_filtered_view_cache` is invoked alongside.
//
// A cached `GraphView` is served ONLY to the read-only, no-write-set-overlay callers
// (`Method::CypherQuery`'s plain read branch). Any caller that further mutates the view
// (e.g. `overlay_write_set` for a read-your-own-writes staged transaction, or a later
// `rls.filter_view` call that expects to mutate its own owned copy) MUST NOT take a
// cached entry without cloning first — see this module's `get` doc.

use crate::graph::GraphView;
use crate::per_actor_cache::PerActorCache;

/// The per-actor RLS-filtered `GraphView` cache. One instance lives on each
/// [`crate::graph::GraphCore`], gated behind the `security` feature — the only build where
/// `IsolationLayer::filter_view` does non-trivial (msgpack-decode-per-node) work worth
/// amortizing; see this module's doc.
///
/// A cached `GraphView` is served ONLY to read-only, no-write-set-overlay callers
/// (`Method::CypherQuery`'s plain read branch). Any caller that further mutates the view
/// (`overlay_write_set` for a read-your-own-writes staged transaction, or a later
/// `rls.filter_view` call that expects to mutate its own owned copy) MUST NOT take a
/// cached entry without cloning first.
///
/// The bounded LRU, the `(actor, version)` hit rule and the whole-image `generation` race
/// check are [`PerActorCache`]'s; this alias only fixes what is cached.
pub(crate) type FilteredViewCache = PerActorCache<GraphView>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn view() -> Arc<GraphView> {
        Arc::new(GraphView::default())
    }

    #[test]
    fn miss_on_cold_actor() {
        let cache = FilteredViewCache::default();
        assert!(cache.get("alice", 0).is_none());
    }

    #[test]
    fn hit_after_put_at_same_version() {
        let cache = FilteredViewCache::default();
        let v = view();
        cache.put("alice".to_string(), 3, cache.generation(), v.clone());
        let hit = cache.get("alice", 3);
        assert!(hit.is_some());
        assert!(Arc::ptr_eq(&hit.unwrap(), &v));
    }

    #[test]
    fn stale_version_is_a_miss() {
        let cache = FilteredViewCache::default();
        cache.put("alice".to_string(), 3, cache.generation(), view());
        assert!(cache.get("alice", 4).is_none());
    }

    #[test]
    fn distinct_actors_do_not_share_or_evict_each_other() {
        let cache = FilteredViewCache::default();
        let alice_view = view();
        let bob_view = view();
        cache.put(
            "alice".to_string(),
            1,
            cache.generation(),
            alice_view.clone(),
        );
        cache.put("bob".to_string(), 1, cache.generation(), bob_view.clone());
        assert!(Arc::ptr_eq(&cache.get("alice", 1).unwrap(), &alice_view));
        assert!(Arc::ptr_eq(&cache.get("bob", 1).unwrap(), &bob_view));
    }

    #[test]
    fn invalidate_all_evicts_a_same_version_entry_that_a_plain_version_check_would_keep_serving() {
        let cache = FilteredViewCache::default();
        let stale = view();
        cache.put("alice".to_string(), 7, cache.generation(), stale.clone());
        assert!(cache.get("alice", 7).is_some(), "sanity: cache is warm");

        cache.invalidate_all();

        assert!(
            cache.get("alice", 7).is_none(),
            "a whole-image replacement/clear must evict every cached filtered view even \
             when the (version) key alone would still read as current"
        );
    }

    #[test]
    fn a_build_racing_invalidation_never_publishes_its_stale_result() {
        let cache = FilteredViewCache::default();
        let generation_at_build_start = cache.generation();

        cache.invalidate_all();

        let stale_result = view();
        cache.put(
            "alice".to_string(),
            0,
            generation_at_build_start,
            stale_result,
        );

        assert!(cache.get("alice", 0).is_none());

        let fresh_result = view();
        cache.put(
            "alice".to_string(),
            0,
            cache.generation(),
            fresh_result.clone(),
        );
        assert!(Arc::ptr_eq(&cache.get("alice", 0).unwrap(), &fresh_result));
    }
}
