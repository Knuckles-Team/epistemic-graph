// CONCEPT:EG-KG.query.version-keyed-result-cache — version-keyed query-RESULT cache (feature `result-cache`).
//
// The engine already caches the SQL *schema* (the inferred `nodes`/`edges` Arrow
// tables, CONCEPT:EG-KG.query.version-keyed-cache) and node *properties* (read-through, CONCEPT:EG-KG.storage.read-through-seam-exercised).
// What was missing is a cache of the RESULT of a read query — the serialized bytes
// a `Sql`/`Cypher`/`Sparql`/`UnifiedQuery` returns. For a read-mostly graph the
// SAME query is re-run unchanged between writes; recomputing it (scan → execute →
// serialize) is wasted work whose answer is identical until the graph mutates.
//
// ## Correctness rests on the per-GraphCore version generation
//
// Each `GraphCore` carries a monotonic OCC write-`version()` (CONCEPT:EG-KG.txn.multi-op-occ-acid)
// bumped by `mark_dirty()` on EVERY committed write (single-op, coalesced batch,
// txn commit, and — on a follower — a replicated `mutation_apply::apply`). The result cache is
// keyed by `(query-hash, version)`:
//
//   * Same query, UNCHANGED graph ⇒ the version matches ⇒ a HIT serves the cached
//     bytes WITHOUT recomputing.
//   * ANY write bumps the version ⇒ the next lookup of that query keys on a new
//     version ⇒ a MISS ⇒ recompute (and the entry is correct, never stale).
//
// So invalidation is implicit and total: a single atomic bump retires EVERY cached
// result for the graph at once. We do not evict per-write; a stale entry simply can
// never be looked up (its `(hash, old_version)` key is dead) and ages out of the LRU.
//
// ## Dependency-scoped entries (W1.6/P7) — surviving unrelated writes
//
// The version-keyed path above is CORRECT but COARSE: under a mixed read/write workload its
// hit-rate collapses toward the WRITE rate, because ANY write to ANY part of the graph retires
// EVERY cached result. `get_dep`/`put_dep` add a second, finer namespace
// (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation). A dependency-scoped entry is keyed on
// query IDENTITY alone (`(query_hash, actor_scope)`, no version) and tagged with the
// [`crate::dep_scope::DepSet`] it READ (the labels it scanned, whether it read all nodes/edges).
// On lookup it is revalidated against the graph's [`crate::dep_scope::DepClock`]: it stays valid
// until a write TOUCHES one of its dimensions, so a repeated `MATCH (:A)` survives a continuous
// stream of `:B` inserts. A query whose dependency set cannot be computed soundly does NOT use
// this path — it keeps the version-keyed `get`/`put` above (coarse, unchanged). Correctness is
// the clock's job: a stale hit is impossible because `DepClock::is_valid` floors on any
// un-attributable write. See `crate::dep_scope`.
//
// ## Bounded LRU — pure-Rust, Pi-safe
//
// A fixed-capacity LRU keyed by a 128-bit query hash + the version. NO external
// crate (no `lru`/`hashbrown`) — a `HashMap<Key, (bytes, tick)>` plus a bounded
// `BTreeMap<tick, Key>` recency index. Eviction removes the first tree entry in
// O(log capacity), rather than scanning every cached result. Pure `std` + hash, so
// it folds into the lean Pi tier with no dep cost. Behind a `parking_lot::Mutex`
// (already an eg-core dep); query execution itself always runs off-lock.

use parking_lot::Mutex;

use crate::dep_scope::DepSet;

mod dependency;
mod hashing;
mod lifecycle;
mod metrics;
mod versioned;

/// Default number of distinct (query, version) results retained per graph.
/// Overridable via `EPISTEMIC_GRAPH_RESULT_CACHE_CAP` (0 disables the cache).
const DEFAULT_CAP: usize = 256;

/// Read the per-graph result-cache capacity from the environment, defaulting to
/// [`DEFAULT_CAP`]. `0` disables caching (every lookup misses, nothing is stored).
fn cap_from_env() -> usize {
    std::env::var("EPISTEMIC_GRAPH_RESULT_CACHE_CAP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_CAP)
}

/// A bounded, version-keyed LRU cache of serialized query results for ONE graph.
/// Lives on `GraphCore`; the query handlers consult it before executing a read and
/// populate it after a miss. Empty + zero-cost until first use.
pub struct ResultCache {
    inner: Mutex<Inner>,
    cap: usize,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
}

struct Inner {
    /// Version-keyed entries and their LRU index. The focused child module owns
    /// the key and entry types as well as their read/write operations.
    versioned: versioned::VersionedEntries,
    /// Dependency-scoped entries and their LRU index. The focused child module
    /// owns dependency validity and eviction operations.
    dependency: dependency::DependencyEntries,
    /// Monotonic access counter shared by both namespaces; the entry with the smallest `tick` is
    /// the LRU within its own recency index.
    clock: u64,
}

/// [`ResultCache::put`] for an encoded method result: only a MessagePack `Raw` payload is
/// cacheable, so any other payload is not stored.
pub fn cache_result(
    cache: &ResultCache,
    query_hash: u128,
    version: u64,
    payload: &eg_types::protocol::ResultPayload,
) {
    if let eg_types::protocol::ResultPayload::Raw(bytes) = payload {
        cache.put(query_hash, version, bytes.clone());
    }
}

/// [`ResultCache::put_dep`] for an encoded method result: only a MessagePack `Raw` payload
/// is cacheable, so any other payload is not stored.
pub fn cache_dep_result(
    cache: &ResultCache,
    query_hash: u128,
    actor_scope_hash: u64,
    computed_at: u64,
    deps: DepSet,
    payload: &eg_types::protocol::ResultPayload,
) {
    if let eg_types::protocol::ResultPayload::Raw(bytes) = payload {
        cache.put_dep(
            query_hash,
            actor_scope_hash,
            computed_at,
            deps,
            bytes.clone(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hit_on_unchanged_version_miss_after_bump() {
        let c = ResultCache::new();
        let q = ResultCache::hash_query("sql", b"SELECT COUNT(*) FROM nodes");

        // Cold: miss, then populate at version 0.
        assert_eq!(c.get(q, 0), None);
        c.put(q, 0, b"result-v0".to_vec());

        // Same query, SAME version ⇒ HIT, exact bytes, no recompute.
        assert_eq!(c.get(q, 0).as_deref(), Some(&b"result-v0"[..]));

        // A write bumped the version ⇒ the v1 key MISSES (cannot serve the stale v0).
        assert_eq!(c.get(q, 1), None);
        c.put(q, 1, b"result-v1".to_vec());
        assert_eq!(c.get(q, 1).as_deref(), Some(&b"result-v1"[..]));

        // 2 hits, 2 misses.
        assert_eq!(c.stats(), (2, 2));
    }

    #[test]
    fn distinct_queries_do_not_collide() {
        let c = ResultCache::new();
        let a = ResultCache::hash_query("sql", b"SELECT 1");
        let b = ResultCache::hash_query("cypher", b"SELECT 1");
        assert_ne!(a, b, "kind must namespace the hash");
        c.put(a, 0, b"a".to_vec());
        c.put(b, 0, b"b".to_vec());
        assert_eq!(c.get(a, 0).as_deref(), Some(&b"a"[..]));
        assert_eq!(c.get(b, 0).as_deref(), Some(&b"b"[..]));
    }

    #[test]
    fn lru_evicts_least_recently_used() {
        let c = ResultCache::with_cap(2);
        let q1 = ResultCache::hash_query("sql", b"q1");
        let q2 = ResultCache::hash_query("sql", b"q2");
        let q3 = ResultCache::hash_query("sql", b"q3");

        c.put(q1, 0, b"1".to_vec());
        c.put(q2, 0, b"2".to_vec());
        // Touch q1 so q2 is the LRU.
        assert!(c.get(q1, 0).is_some());
        // Inserting q3 evicts q2 (LRU), keeps q1.
        c.put(q3, 0, b"3".to_vec());
        assert_eq!(c.len(), 2);
        assert!(c.get(q1, 0).is_some());
        assert!(c.get(q3, 0).is_some());
        assert!(c.get(q2, 0).is_none(), "q2 was the LRU and must be evicted");
        let inner = c.inner.lock();
        assert_eq!(inner.versioned.recency.len(), inner.versioned.map.len());
        assert!(inner.versioned.map.iter().all(|(key, entry)| inner
            .versioned
            .recency
            .get(&entry.tick)
            == Some(key)));
    }

    #[test]
    fn recency_index_stays_bounded_under_repeated_replacement() {
        let c = ResultCache::with_cap(8);
        for version in 0..1_024 {
            let q = ResultCache::hash_query("sql", format!("q{}", version % 13).as_bytes());
            c.put(q, version, version.to_le_bytes().to_vec());
            if version % 3 == 0 {
                let _ = c.get(q, version);
            }
        }
        let inner = c.inner.lock();
        assert_eq!(inner.versioned.map.len(), 8);
        assert_eq!(inner.versioned.recency.len(), 8);
        assert!(inner.versioned.map.iter().all(|(key, entry)| inner
            .versioned
            .recency
            .get(&entry.tick)
            == Some(key)));
    }

    #[test]
    fn invalidate_all_clears() {
        let c = ResultCache::new();
        let q = ResultCache::hash_query("sql", b"q");
        c.put(q, 5, b"x".to_vec());
        assert_eq!(c.len(), 1);
        c.invalidate_all();
        assert_eq!(c.len(), 0);
        assert!(c.get(q, 5).is_none());
    }

    #[test]
    fn two_actors_never_share_a_cached_result() {
        // CONCEPT:EG-KG.query.rls-scoped-result-cache — the SAME query at the SAME version
        // under two different RLS actors keys distinctly, so actor B can never read the
        // bytes actor A cached.
        let c = ResultCache::new();
        let q = ResultCache::hash_query("unified", b"MATCH (n) RETURN n");
        let a = ResultCache::hash_actor_scope("agent-A");
        let b = ResultCache::hash_actor_scope("agent-B");
        assert_ne!(a, b, "distinct actors hash distinctly");

        // A populates its scoped result at version 7.
        c.put_scoped(q, 7, a, b"A-visible-rows".to_vec());
        // A HITs its own scoped entry.
        assert_eq!(
            c.get_scoped(q, 7, a).as_deref(),
            Some(&b"A-visible-rows"[..])
        );
        // B MISSES — it never sees A's rows even though query + version match.
        assert_eq!(c.get_scoped(q, 7, b), None);
        // The unscoped (security-off) namespace ALSO misses A's scoped entry.
        assert_eq!(c.get(q, 7), None);
    }

    #[test]
    fn unscoped_get_put_is_scope_zero() {
        // The plain get/put path is exactly actor scope 0, so an explicit scope-0 scoped
        // call sees the same entry an unscoped put made (single-tenant byte-identity).
        let c = ResultCache::new();
        let q = ResultCache::hash_query("sql", b"SELECT 1");
        c.put(q, 0, b"x".to_vec());
        assert_eq!(c.get_scoped(q, 0, 0).as_deref(), Some(&b"x"[..]));
        assert_eq!(ResultCache::hash_actor_scope(""), 0);
    }

    #[test]
    fn cap_zero_disables() {
        let c = ResultCache::with_cap(0);
        let q = ResultCache::hash_query("sql", b"q");
        c.put(q, 0, b"x".to_vec());
        assert!(c.get(q, 0).is_none());
        assert_eq!(c.len(), 0);
    }
}
