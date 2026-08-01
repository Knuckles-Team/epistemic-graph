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

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::graph::GraphCore;

/// Bounded so a deployment with many distinct concurrent actors cannot grow this
/// unboundedly. Realistic concurrent-actor counts (service accounts, interactive
/// users) are small; a capacity miss costs exactly what every call cost before this
/// cache existed — never worse.
const CAPACITY: usize = 64;

#[derive(Default)]
struct Inner {
    entries: HashMap<String, (u64, Arc<GraphCore>)>,
    // Recency order, oldest at the front. `HashMap` iteration order is not defined,
    // so eviction order is tracked explicitly here rather than relied on from the map.
    order: VecDeque<String>,
}

impl Inner {
    fn touch(&mut self, actor: &str) {
        if let Some(pos) = self.order.iter().position(|a| a == actor) {
            self.order.remove(pos);
        }
        self.order.push_back(actor.to_string());
    }
}

/// One instance lives on each [`GraphCore`], gated behind the `security` feature
/// (the only build where `project_core`'s expensive path is ever reached — see
/// `GraphReadAuthority::is_active`).
#[derive(Default)]
pub(crate) struct ProjectionCache(Mutex<Inner>);

impl std::fmt::Debug for ProjectionCache {
    /// `GraphCore` derives `Debug`; mirror `ResultCache`/`ProjectionCatalog`'s own
    /// `Debug` impls (summary counts, not cached content — entries hold `Arc<GraphCore>`,
    /// which is itself not `Debug`, and per-actor cache contents are not diagnostic
    /// output anyone should print anyway).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.0.lock().entries.len();
        f.debug_struct("ProjectionCache")
            .field("len", &len)
            .finish()
    }
}

impl ProjectionCache {
    /// Fresh hit only: `None` on a cold miss OR a stale (version-mismatched) entry.
    /// A stale entry is left in place (not evicted here) — it is cheap to hold until
    /// the next `put` for that actor overwrites it, and evicting eagerly would need
    /// the same lock a concurrent rebuild's `put` also wants.
    pub(crate) fn get(&self, actor: &str, current_version: u64) -> Option<Arc<GraphCore>> {
        let mut inner = self.0.lock();
        let hit = match inner.entries.get(actor) {
            Some((version, core)) if *version == current_version => Some(core.clone()),
            _ => None,
        };
        if hit.is_some() {
            inner.touch(actor);
        }
        hit
    }

    /// Insert/overwrite this actor's entry with a freshly built projection. Called
    /// AFTER the build completes, under a separate (short) lock acquisition from
    /// `get`'s — see the module doc for why the build itself must stay off-lock.
    pub(crate) fn put(&self, actor: String, version: u64, core: Arc<GraphCore>) {
        let mut inner = self.0.lock();
        if !inner.entries.contains_key(&actor) && inner.entries.len() >= CAPACITY {
            if let Some(evict) = inner.order.pop_front() {
                inner.entries.remove(&evict);
            }
        }
        inner.touch(&actor);
        inner.entries.insert(actor, (version, core));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        cache.put("alice".to_string(), 3, c.clone());
        let hit = cache.get("alice", 3);
        assert!(hit.is_some());
        assert!(Arc::ptr_eq(&hit.unwrap(), &c));
    }

    #[test]
    fn stale_version_is_a_miss() {
        let cache = ProjectionCache::default();
        cache.put("alice".to_string(), 3, core());
        assert!(cache.get("alice", 4).is_none());
    }

    #[test]
    fn distinct_actors_do_not_share_or_evict_each_other() {
        let cache = ProjectionCache::default();
        let alice_core = core();
        let bob_core = core();
        cache.put("alice".to_string(), 1, alice_core.clone());
        cache.put("bob".to_string(), 1, bob_core.clone());
        assert!(Arc::ptr_eq(&cache.get("alice", 1).unwrap(), &alice_core));
        assert!(Arc::ptr_eq(&cache.get("bob", 1).unwrap(), &bob_core));
    }

    #[test]
    fn capacity_evicts_the_oldest_actor() {
        let cache = ProjectionCache::default();
        for i in 0..CAPACITY {
            cache.put(format!("actor-{i}"), 0, core());
        }
        // `actor-0` is the oldest in recency order (inserted first, never touched
        // since). One more DISTINCT actor beyond capacity must evict it — checked
        // without any intervening `get` on `actor-0`, since a `get` would itself
        // touch (and thus protect) the entry this test is verifying gets evicted.
        cache.put("actor-overflow".to_string(), 0, core());
        assert!(cache.get("actor-0", 0).is_none());
        assert!(cache.get("actor-overflow", 0).is_some());
        assert!(cache.get("actor-1", 0).is_some());
    }
}
