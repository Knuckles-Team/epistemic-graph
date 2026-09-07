// CONCEPT:EG-KG.sharding.row-level-security (D-OP-1 / D-OB-20) — bounded per-actor
// cache for `GraphReadAuthority::project_core`'s RLS projection.
//
// ## Why this exists
//
// `project_core` (the `epistemic-graph` crate's `src/server/access.rs`) materializes
// a SECOND `GraphCore` on every RLS-active read: sort + clone every visible node,
// sort + clone every visible edge, and copy every visible embedding out of the
// semantic store — `O(V log V + E log E + V*d)`. Unpatched, it pays this on EVERY
// call, independent of what was actually asked for (a `HasNode` point lookup paid
// the same cost as a full graph dump). Measured live against `__commons__`
// (25,075 nodes, 1024-dim embeddings): ~103 MB memcpy'd per call, 2.7-3.0s per
// `HasNode` against a 50ms budget (D-OB-20 — "the engine is ~17,000x slower than
// its own healthy baseline").
//
// The Cypher/RDF read paths (`src/server/handlers/query.rs`, `rdf.rs`) avoid the
// second-graph cost entirely: they call `GraphCore::analysis_snapshot()` +
// `IsolationLayer::filter_view` directly on the OWNED snapshot and execute against
// that filtered `GraphView` in place — no second `GraphCore` is ever built. That
// mechanism is NOT directly reusable for `project_core`'s callers as a drop-in,
// though: every primitive/algorithm handler downstream of `try_handle`'s terminal
// match (`has_node`, `get_neighbors`, `shortest_path`, community mining, semantic
// search, …) is written against `&GraphCore`/`Arc<GraphCore>`, not `&GraphView`.
// Rewriting every one of them to accept a `GraphView` (or thread an isolation-aware
// context through each primitive) is a materially larger, more invasive change than
// caching the existing, already-correct materialization — see
// `docs/architecture/d-op-1-projection-cache.md` option (c) for the full accounting
// of why that path was assessed and not chosen as the primary fix.
//
// ## What this does instead
//
// Cache the materialized `Arc<GraphCore>` PER ACTOR, invalidated the moment the
// source graph's `version()` (already bumped once per committed write) advances —
// the SAME invalidate-on-version-change idiom `GraphCore::ontology_index` /
// `label_index` already use for their own lazy caches (`crates/eg-core/src/graph.rs`),
// extended to be per-actor because unlike those two (actor-agnostic), an RLS
// projection is PER-ACTOR: two concurrent, distinct actors must never evict or
// observe each other's entry.
//
// Correctness: a cache hit fires ONLY when (actor, version) match exactly. The
// projection is a pure function of (source graph content, actor's ACL grants) at a
// given version, so a hit returns byte-identical content to a fresh rebuild for that
// same (actor, version) pair — this is an amortization of `project_core`'s existing
// guarantee, not a relaxation of it. See
// `alice_and_bob_same_tenant_shared_graph_cannot_observe_each_others_rows`
// (`src/server/access.rs`) — the correctness oracle a cache must not disturb — plus
// the write-then-reread invalidation case added alongside this cache in the same
// file.
//
// Building a fresh projection is the expensive operation this cache exists to
// amortize, so it MUST NOT run while holding this cache's lock (that would simply
// move the O(V) stall from "every call" to "every call that loses a race with a
// rebuild", and would block unrelated actors' cache HITS behind one actor's
// cold-miss rebuild). Callers therefore: read-check under the lock, drop it, build
// off-lock on a miss, then re-acquire briefly to store. A concurrent miss for the
// same actor may redundantly rebuild — both rebuilds are pure functions of the same
// (actor, version) and produce identical content, so this is a wasted CPU cycle
// under a race, never a correctness hazard.

use crate::graph::GraphCore;
use crate::per_actor_cache::PerActorCache;

/// The per-actor `project_core` cache. One instance lives on each [`GraphCore`], gated
/// behind the `security` feature (the only build where `project_core`'s expensive path is
/// ever reached — see `GraphReadAuthority::is_active`).
///
/// The bounded LRU, the `(actor, version)` hit rule and the whole-image `generation` race
/// check are [`PerActorCache`]'s; this alias only fixes what is cached.
pub(crate) type ProjectionCache = PerActorCache<GraphCore>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::per_actor_cache::CAPACITY;

    fn core() -> Arc<GraphCore> {
        Arc::new(GraphCore::new())
    }

    #[test]
    fn miss_on_cold_actor() {
        let cache = ProjectionCache::default();
        assert!(cache.get("alice", 0).is_none());
    }

    #[test]
    fn hit_after_put_at_same_version() {
        let cache = ProjectionCache::default();
        let c = core();
        cache.put("alice".to_string(), 3, cache.generation(), c.clone());
        let hit = cache.get("alice", 3);
        assert!(hit.is_some());
        assert!(Arc::ptr_eq(&hit.unwrap(), &c));
    }

    #[test]
    fn stale_version_is_a_miss() {
        let cache = ProjectionCache::default();
        cache.put("alice".to_string(), 3, cache.generation(), core());
        assert!(cache.get("alice", 4).is_none());
    }

    #[test]
    fn distinct_actors_do_not_share_or_evict_each_other() {
        let cache = ProjectionCache::default();
        let alice_core = core();
        let bob_core = core();
        cache.put(
            "alice".to_string(),
            1,
            cache.generation(),
            alice_core.clone(),
        );
        cache.put("bob".to_string(), 1, cache.generation(), bob_core.clone());
        assert!(Arc::ptr_eq(&cache.get("alice", 1).unwrap(), &alice_core));
        assert!(Arc::ptr_eq(&cache.get("bob", 1).unwrap(), &bob_core));
    }

    #[test]
    fn capacity_evicts_the_oldest_actor() {
        let cache = ProjectionCache::default();
        for i in 0..CAPACITY {
            cache.put(format!("actor-{i}"), 0, cache.generation(), core());
        }
        // `actor-0` is the oldest in recency order (inserted first, never touched
        // since). One more DISTINCT actor beyond capacity must evict it — checked
        // without any intervening `get` on `actor-0`, since a `get` would itself
        // touch (and thus protect) the entry this test is verifying gets evicted.
        cache.put("actor-overflow".to_string(), 0, cache.generation(), core());
        assert!(cache.get("actor-0", 0).is_none());
        assert!(cache.get("actor-overflow", 0).is_some());
        assert!(cache.get("actor-1", 0).is_some());
    }

    /// U-142/U-143/U-145 (BUG-130) — the regression this cache exists to satisfy:
    /// a whole-image transition (`GraphCore::replace_snapshot`/`clear`/`hibernate`)
    /// can leave `version()` numerically unchanged (a same-version reconciliation)
    /// or deliberately skip bumping it at all (`clear`/`hibernate`). Without an
    /// independent generation, a cached entry at that same `version` would still
    /// look "current" to `get` and would keep serving the PRE-replacement content —
    /// exactly the governed-vs-Cypher disagreement U-142 reproduced live. FAILS
    /// without the generation check in `put`/`invalidate_all` (the old code had no
    /// `invalidate_all` at all, so the stale entry would remain gettable forever);
    /// PASSES with it.
    #[test]
    fn invalidate_all_evicts_a_same_version_entry_that_a_plain_version_check_would_keep_serving() {
        let cache = ProjectionCache::default();
        let stale = core();
        cache.put("alice".to_string(), 7, cache.generation(), stale.clone());
        assert!(cache.get("alice", 7).is_some(), "sanity: cache is warm");

        // Simulates `GraphCore::replace_snapshot`/`clear` — the resident image
        // changed underneath the SAME `version` (or without bumping it at all).
        cache.invalidate_all();

        assert!(
            cache.get("alice", 7).is_none(),
            "a whole-image replacement/clear must evict every cached projection even \
             when the (version) key alone would still read as current — a plain \
             version-keyed cache with no generation would wrongly return `stale` here"
        );
    }

    /// U-145's specific race: an expensive projection build starts (captures the
    /// CURRENT generation), a whole-image replacement invalidates while that build
    /// is still in flight, and only THEN does the build finish and try to publish.
    /// Constructed deterministically per GOC-70 — no spawned tasks / no hoping two
    /// threads interleave a particular way; the generation is captured, the
    /// invalidation is executed synchronously, and only then is the (now-stale)
    /// build result handed to `put`, reproducing the exact ordering "build starts
    /// before invalidation, publishes after" without depending on scheduling.
    #[test]
    fn a_build_racing_invalidation_never_publishes_its_stale_result() {
        let cache = ProjectionCache::default();
        // Step 1: a build for `alice` starts and captures the generation token
        // BEFORE the (expensive, here-elided) rebuild work runs.
        let generation_at_build_start = cache.generation();

        // Step 2: a whole-image replacement lands WHILE that build is still
        // running (deterministically sequenced here, not raced).
        cache.invalidate_all();

        // Step 3: the build finishes and tries to publish using the generation it
        // captured in step 1 — now stale.
        let stale_result = core();
        cache.put(
            "alice".to_string(),
            0,
            generation_at_build_start,
            stale_result.clone(),
        );

        assert!(
            cache.get("alice", 0).is_none(),
            "a build that started before an invalidation must not publish its \
             now-stale result after the invalidation completed"
        );

        // A fresh build started AFTER the invalidation (current generation) still
        // publishes normally — invalidation does not wedge the cache shut.
        let fresh_result = core();
        cache.put(
            "alice".to_string(),
            0,
            cache.generation(),
            fresh_result.clone(),
        );
        assert!(Arc::ptr_eq(&cache.get("alice", 0).unwrap(), &fresh_result));
    }
}
