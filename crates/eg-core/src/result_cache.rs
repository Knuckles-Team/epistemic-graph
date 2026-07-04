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
// txn commit, and — on a follower — a replicated `wal::apply`). The result cache is
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
// ## Bounded LRU — pure-Rust, Pi-safe
//
// A fixed-capacity LRU keyed by a 128-bit query hash + the version. NO external
// crate (no `lru`/`hashbrown`) — a `HashMap<Key, (bytes, tick)>` plus a monotonic
// access tick; eviction drops the least-recently-used entry when at capacity. Pure
// `std` + hash, so it folds into the lean Pi tier with no dep cost. Behind a
// `parking_lot::Mutex` (already an eg-core dep): a lookup is a hash probe, an insert
// a probe + at most one eviction scan — never the query work itself, which runs
// off-lock on the blocking pool.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use parking_lot::Mutex;

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

/// Cache key: the query identity (a 128-bit hash of the query kind + text/plan), the
/// GraphCore version the result was computed against, and the RLS ACTOR-SCOPE hash
/// (CONCEPT:EG-KG.query.rls-scoped-result-cache). A different version is a different key,
/// so a write (which bumps the version) makes every prior result unreachable; a different
/// `actor_scope_hash` is ALSO a different key, so a result computed under one RLS actor's
/// row-visibility is NEVER served to a different actor. When security/RLS is off the
/// scope is `0` for every caller, so the key collapses to the plain `(query_hash, version)`
/// pair and single-tenant caching is byte-for-byte the pre-RLS behavior.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Key {
    query_hash: u128,
    version: u64,
    actor_scope_hash: u64,
}

/// One cached result + its last-access tick (for LRU recency).
struct Entry {
    bytes: Vec<u8>,
    tick: u64,
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
    map: HashMap<Key, Entry>,
    /// Monotonic access counter; the entry with the smallest `tick` is the LRU.
    clock: u64,
}

impl std::fmt::Debug for ResultCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (hits, misses) = self.stats();
        f.debug_struct("ResultCache")
            .field("cap", &self.cap)
            .field("len", &self.len())
            .field("hits", &hits)
            .field("misses", &misses)
            .finish()
    }
}

impl Default for ResultCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ResultCache {
    pub fn new() -> Self {
        ResultCache {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                clock: 0,
            }),
            cap: cap_from_env(),
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Hash a query's identity into the 128-bit key component. `kind` distinguishes
    /// the surfaces (so an identical text under SQL vs Cypher never collides) and
    /// `payload` is the query text or the canonical-bytes of a plan. Two `u64`
    /// `DefaultHasher` passes (salted) give a 128-bit space — collision-resistant
    /// enough for a per-graph in-memory cache (a collision would only serve a wrong
    /// cached result; the version still gates staleness across writes).
    pub fn hash_query(kind: &str, payload: &[u8]) -> u128 {
        fn one(salt: u64, kind: &str, payload: &[u8]) -> u64 {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            salt.hash(&mut h);
            kind.hash(&mut h);
            payload.hash(&mut h);
            h.finish()
        }
        let lo = one(0, kind, payload);
        let hi = one(0x9E37_79B9_7F4A_7C15, kind, payload);
        ((hi as u128) << 64) | (lo as u128)
    }

    /// Hash an RLS ACTOR-SCOPE identity (its row-visibility key, e.g. the caller's
    /// `agent_id`) into the `actor_scope_hash` key component
    /// (CONCEPT:EG-KG.query.rls-scoped-result-cache). An EMPTY scope — the single-tenant /
    /// security-off case — hashes to `0`, so the key collapses to `(query_hash, version)`
    /// and existing callers that never pass a scope are byte-for-byte unchanged. Two
    /// callers with the same visibility key hash the same (they may share a cached result);
    /// two with different keys hash differently (they NEVER do).
    pub fn hash_actor_scope(scope: &str) -> u64 {
        if scope.is_empty() {
            return 0;
        }
        let mut h = std::collections::hash_map::DefaultHasher::new();
        "actor-scope".hash(&mut h);
        scope.hash(&mut h);
        // Keep 0 reserved for "no scope": a real scope that happens to hash to 0 is
        // nudged to 1, so a non-empty scope can never collide with the unscoped key.
        let v = h.finish();
        if v == 0 {
            1
        } else {
            v
        }
    }

    /// Look up a cached result for `query_hash` at `version` in the UNSCOPED (actor
    /// scope `0`) namespace. `Some(bytes)` is a HIT (current); `None` is a MISS. This is
    /// the security-off / single-tenant path — identical to before RLS scoping existed.
    pub fn get(&self, query_hash: u128, version: u64) -> Option<Vec<u8>> {
        self.get_scoped(query_hash, version, 0)
    }

    /// Look up a cached result keyed additionally on the RLS `actor_scope_hash`
    /// (CONCEPT:EG-KG.query.rls-scoped-result-cache). A result computed under one actor's
    /// row-visibility is unreachable under a different actor's scope, so a cached plan
    /// result is NEVER served across RLS actors. Pass `0` for the unscoped/security-off
    /// case (what [`get`](Self::get) does). Updates recency on a hit and bumps the
    /// hit/miss counters (the proof seam).
    pub fn get_scoped(
        &self,
        query_hash: u128,
        version: u64,
        actor_scope_hash: u64,
    ) -> Option<Vec<u8>> {
        if self.cap == 0 {
            self.misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return None;
        }
        let key = Key {
            query_hash,
            version,
            actor_scope_hash,
        };
        let mut inner = self.inner.lock();
        inner.clock += 1;
        let now = inner.clock;
        if let Some(e) = inner.map.get_mut(&key) {
            e.tick = now;
            let bytes = e.bytes.clone();
            drop(inner);
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Some(bytes)
        } else {
            drop(inner);
            self.misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            None
        }
    }

    /// Insert a freshly-computed result into the UNSCOPED (actor scope `0`) namespace.
    /// Evicts the LRU entry first when at capacity. A no-op when disabled (`cap == 0`).
    pub fn put(&self, query_hash: u128, version: u64, bytes: Vec<u8>) {
        self.put_scoped(query_hash, version, 0, bytes);
    }

    /// Insert a freshly-computed result keyed additionally on the RLS `actor_scope_hash`
    /// (CONCEPT:EG-KG.query.rls-scoped-result-cache). Evicts the LRU entry first when at
    /// capacity. A no-op when the cache is disabled (`cap == 0`).
    pub fn put_scoped(
        &self,
        query_hash: u128,
        version: u64,
        actor_scope_hash: u64,
        bytes: Vec<u8>,
    ) {
        if self.cap == 0 {
            return;
        }
        let key = Key {
            query_hash,
            version,
            actor_scope_hash,
        };
        let mut inner = self.inner.lock();
        inner.clock += 1;
        let now = inner.clock;
        // Evict the LRU entry if inserting a NEW key would exceed capacity.
        if !inner.map.contains_key(&key) && inner.map.len() >= self.cap {
            if let Some(lru) = inner
                .map
                .iter()
                .min_by_key(|(_, e)| e.tick)
                .map(|(k, _)| *k)
            {
                inner.map.remove(&lru);
            }
        }
        inner.map.insert(key, Entry { bytes, tick: now });
    }

    /// Drop every cached result. Called when the graph's whole in-RAM state is
    /// wiped (`clear`/`hibernate`) — those paths don't necessarily bump the version,
    /// so the cache is cleared directly to guarantee no post-wipe stale hit.
    pub fn invalidate_all(&self) {
        let mut inner = self.inner.lock();
        inner.map.clear();
    }

    /// `(hits, misses)` since construction — the proof counters a test reads to show
    /// a repeated query on an unchanged graph hit the cache (didn't recompute) and a
    /// query after a write missed (recomputed).
    pub fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(std::sync::atomic::Ordering::Relaxed),
            self.misses.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Current number of retained entries (observability/tests).
    pub fn len(&self) -> usize {
        self.inner.lock().map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
        std::env::set_var("EPISTEMIC_GRAPH_RESULT_CACHE_CAP", "2");
        let c = ResultCache::new();
        std::env::remove_var("EPISTEMIC_GRAPH_RESULT_CACHE_CAP");
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
        std::env::set_var("EPISTEMIC_GRAPH_RESULT_CACHE_CAP", "0");
        let c = ResultCache::new();
        std::env::remove_var("EPISTEMIC_GRAPH_RESULT_CACHE_CAP");
        let q = ResultCache::hash_query("sql", b"q");
        c.put(q, 0, b"x".to_vec());
        assert!(c.get(q, 0).is_none());
        assert_eq!(c.len(), 0);
    }
}
