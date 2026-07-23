//! Bounded Cypher AST/plan cache (CONCEPT:EG-KG.query.dep-free-behind) — `exec_cypher_params`
//! re-[`parser::parse`]s the SAME query TEXT on every call, even when a caller (an
//! MCP tool, a UQL surface, a served `Method::Cypher` handler) issues the identical
//! Cypher string over and over against a live-changing graph. For a hot, repeated
//! query that re-parse + re-plan is wasted CPU on every single call, even though the
//! parse result never changes.
//!
//! ## Why this cache is schema-independent and NEVER invalidates
//!
//! [`parser::parse`] is a PURE function of the query text alone — its signature
//! (`fn parse(input: &str) -> Result<CypherQuery, String>`) takes no `GraphView`/
//! `GraphCore`, and nothing under `cypher::parser` reads graph state. A query
//! parameter (`$name`) is NEVER resolved at parse time either — it stays a symbolic
//! `PropVal::Param`/`ListExpr::Param` node in the [`CypherQuery`] AST (see
//! `plan.rs`), resolved only later, in `exec.rs`, against the `Params` map supplied
//! to `exec_cypher_params`. So two calls with IDENTICAL TEXT — regardless of
//! `$params`, regardless of which graph or graph VERSION they run against — parse to
//! the STRUCTURALLY IDENTICAL `CypherQuery`. Unlike `eg-core::result_cache`'s
//! version-keyed `ResultCache` (which caches query RESULTS and MUST invalidate on
//! every write, since a result depends on graph content), this cache has NO
//! version/epoch key at all: there is nothing for a graph write to invalidate, so
//! entries live until evicted by capacity alone. That also means ONE process-wide
//! instance ([`global`]) safely serves every graph, tenant, and caller — there is no
//! version/RLS/tenant axis to separate a plan by.
//!
//! ## Bounded LRU — same hand-rolled shape as `eg-core::result_cache`, no new dep
//!
//! `HashMap<Arc<str>, Entry>` plus a bounded `BTreeMap<tick, Arc<str>>` recency
//! index — the identical idiom `crates/eg-core/src/result_cache.rs`'s version-keyed
//! result cache uses, adapted for a TEXT key and an `Arc<CypherQuery>` payload
//! instead of a `(hash, version)` key and `Arc<Vec<u8>>` result bytes. Eviction
//! removes the first (smallest-tick) recency entry in O(log capacity) rather than
//! scanning every cached plan. A cache HIT clones only the `Arc` (a refcount bump),
//! never the AST tree itself — cheap even for a large parsed query.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use super::parser;
use super::plan::CypherQuery;

/// Default number of distinct query-text entries retained. Overridable via
/// `EPISTEMIC_GRAPH_CYPHER_PLAN_CACHE` (`0` disables the cache: every call
/// re-parses, nothing is stored — mirrors `result_cache.rs`'s
/// `EPISTEMIC_GRAPH_RESULT_CACHE_CAP` convention).
const DEFAULT_CAP: usize = 256;

/// Read the process-wide plan-cache capacity from the environment, defaulting to
/// [`DEFAULT_CAP`].
fn cap_from_env() -> usize {
    std::env::var("EPISTEMIC_GRAPH_CYPHER_PLAN_CACHE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_CAP)
}

/// One cached plan: the parsed AST plus a clone of its own map key (so a hit can
/// reinsert into `recency` without a second map lookup) and its last-access tick.
struct Entry {
    key: Arc<str>,
    ast: Arc<CypherQuery>,
    tick: u64,
}

struct Inner {
    map: HashMap<Arc<str>, Entry>,
    /// Tick order contains exactly one entry for every key in `map`.
    recency: BTreeMap<u64, Arc<str>>,
    /// Monotonic access counter; the entry with the smallest `tick` is the LRU.
    clock: u64,
}

/// A bounded, process-wide LRU cache of parsed Cypher ASTs keyed on the EXACT query
/// text. Schema-independent (see module doc) — never invalidated by a graph write.
pub(crate) struct PlanCache {
    inner: Mutex<Inner>,
    cap: usize,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl std::fmt::Debug for PlanCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (hits, misses) = self.stats();
        f.debug_struct("PlanCache")
            .field("cap", &self.cap)
            .field("len", &self.len())
            .field("hits", &hits)
            .field("misses", &misses)
            .finish()
    }
}

impl Default for PlanCache {
    fn default() -> Self {
        Self::new()
    }
}

impl PlanCache {
    pub(crate) fn new() -> Self {
        Self::with_cap(cap_from_env())
    }

    /// Construct a cache with an EXPLICIT capacity, bypassing the env override —
    /// the same discipline `ResultCache::with_cap` documents: tests use this
    /// directly so cache construction never depends on the process-global
    /// `EPISTEMIC_GRAPH_CYPHER_PLAN_CACHE` (which would otherwise let one test's
    /// `set_var`/`remove_var` leak into a concurrently-constructed cache).
    pub(crate) fn with_cap(cap: usize) -> Self {
        PlanCache {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                recency: BTreeMap::new(),
                clock: 0,
            }),
            cap,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Look up `text`'s cached AST. `Some` is a HIT (bumps recency + the hit
    /// counter); `None` is a MISS (bumps the miss counter). Cap `0` always misses
    /// without touching the map — the disabled state.
    pub(crate) fn get(&self, text: &str) -> Option<Arc<CypherQuery>> {
        if self.cap == 0 {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let mut inner = self.inner.lock().unwrap();
        inner.clock += 1;
        let now = inner.clock;
        if let Some((old_tick, ast, key)) = inner.map.get_mut(text).map(|entry| {
            let old_tick = entry.tick;
            entry.tick = now;
            (old_tick, Arc::clone(&entry.ast), Arc::clone(&entry.key))
        }) {
            inner.recency.remove(&old_tick);
            inner.recency.insert(now, key);
            drop(inner);
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(ast)
        } else {
            drop(inner);
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert a freshly-parsed AST for `text`. Evicts the LRU entry first when
    /// inserting a new key would exceed capacity. A no-op when disabled (`cap ==
    /// 0`). A `text` already present (a concurrent miss raced this one) just
    /// refreshes recency + payload rather than growing the map — both racers still
    /// end up with a valid, structurally-identical AST for that text.
    pub(crate) fn put(&self, text: &str, ast: Arc<CypherQuery>) {
        if self.cap == 0 {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        inner.clock += 1;
        let now = inner.clock;
        if let Some(entry) = inner.map.get_mut(text) {
            let old_tick = entry.tick;
            entry.tick = now;
            entry.ast = ast;
            let key = Arc::clone(&entry.key);
            inner.recency.remove(&old_tick);
            inner.recency.insert(now, key);
            return;
        }
        if inner.map.len() >= self.cap {
            if let Some((&lru_tick, lru_key)) = inner.recency.first_key_value() {
                let lru_key = Arc::clone(lru_key);
                inner.recency.remove(&lru_tick);
                inner.map.remove(&lru_key);
            }
        }
        let key: Arc<str> = Arc::from(text);
        inner.map.insert(
            Arc::clone(&key),
            Entry {
                key: Arc::clone(&key),
                ast,
                tick: now,
            },
        );
        inner.recency.insert(now, key);
    }

    /// Return `text`'s cached AST, parsing (and caching the SUCCESSFUL result)
    /// on a miss. A parse ERROR is never cached — it's cheap to reproduce, and the
    /// caller returns it immediately either way, so caching it would only add a
    /// second `Entry` shape for no benefit.
    pub(crate) fn get_or_parse(&self, text: &str) -> Result<Arc<CypherQuery>, String> {
        if let Some(ast) = self.get(text) {
            return Ok(ast);
        }
        let ast = Arc::new(parser::parse(text)?);
        self.put(text, Arc::clone(&ast));
        Ok(ast)
    }

    /// `(hits, misses)` since construction — the proof counter a test reads to show
    /// a repeated query text hit the cache (didn't reparse) and a distinct text
    /// missed (did).
    pub(crate) fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }

    /// Current number of retained entries (observability/tests).
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().unwrap().map.len()
    }
}

/// The process-wide plan cache. ONE instance safely serves every graph, tenant, and
/// caller in the process (see the module doc's schema-independence argument) — unlike
/// `eg-core::ResultCache`, which must be constructed per-graph because a query
/// RESULT depends on graph content and has to invalidate on a write.
pub(crate) fn global() -> &'static PlanCache {
    static CACHE: OnceLock<PlanCache> = OnceLock::new();
    CACHE.get_or_init(PlanCache::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(text: &str) -> Arc<CypherQuery> {
        Arc::new(parser::parse(text).unwrap())
    }

    #[test]
    fn repeat_text_hits_after_first_put() {
        let c = PlanCache::with_cap(8);
        let text = "MATCH (a:Person) RETURN a";

        assert!(c.get(text).is_none(), "cold cache must miss");
        c.put(text, q(text));

        let hit = c.get(text);
        assert!(hit.is_some(), "same text must hit after put");
        assert_eq!(c.stats(), (1, 1));
    }

    #[test]
    fn distinct_text_misses() {
        let c = PlanCache::with_cap(8);
        c.put("MATCH (a:Person) RETURN a", q("MATCH (a:Person) RETURN a"));

        assert!(
            c.get("MATCH (a:Doc) RETURN a").is_none(),
            "different query text must never hit another text's entry"
        );
        assert_eq!(c.stats(), (0, 1));
    }

    #[test]
    fn get_or_parse_misses_then_hits_the_same_arc() {
        let c = PlanCache::with_cap(8);
        let text = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b";

        let first = c.get_or_parse(text).unwrap();
        assert_eq!(c.stats(), (0, 1), "first call must be a genuine miss");

        let second = c.get_or_parse(text).unwrap();
        assert_eq!(c.stats(), (1, 1), "second call must hit");
        assert!(
            Arc::ptr_eq(&first, &second),
            "a hit must return the SAME Arc allocation, not a fresh reparse"
        );
    }

    #[test]
    fn get_or_parse_never_caches_a_parse_error() {
        let c = PlanCache::with_cap(8);
        let bad = "MATCH (a:Person RETURN a"; // unbalanced paren — parse error
        assert!(c.get_or_parse(bad).is_err());
        assert!(c.get_or_parse(bad).is_err(), "still an error, never cached");
        assert_eq!(c.len(), 0, "a parse error must never populate the cache");
    }

    #[test]
    fn cap_zero_disables_the_cache() {
        let c = PlanCache::with_cap(0);
        let text = "MATCH (a:Person) RETURN a";
        c.put(text, q(text));
        assert_eq!(c.len(), 0, "put must be a no-op when disabled");
        assert!(c.get(text).is_none(), "get must always miss when disabled");
        assert!(c.get(text).is_none());
        assert_eq!(c.stats(), (0, 2), "every disabled lookup counts as a miss");
    }

    #[test]
    fn bounded_lru_evicts_least_recently_used() {
        let c = PlanCache::with_cap(2);
        let t1 = "MATCH (a:Person) RETURN a";
        let t2 = "MATCH (a:Doc) RETURN a";
        let t3 = "MATCH (a:Server) RETURN a";

        c.put(t1, q(t1));
        c.put(t2, q(t2));
        // Touch t1 so t2 becomes the LRU.
        assert!(c.get(t1).is_some());
        // Inserting t3 evicts t2 (LRU), keeps t1.
        c.put(t3, q(t3));

        assert_eq!(c.len(), 2);
        assert!(c.get(t1).is_some(), "t1 was touched, must survive");
        assert!(c.get(t3).is_some(), "t3 is the newest insert, must survive");
        assert!(c.get(t2).is_none(), "t2 was the LRU and must be evicted");

        let inner = c.inner.lock().unwrap();
        assert_eq!(inner.recency.len(), inner.map.len());
        assert!(inner.map.values().all(|entry| inner
            .recency
            .get(&entry.tick)
            .is_some_and(|k| **k == *entry.key)));
    }

    #[test]
    fn concurrent_miss_race_on_the_same_text_does_not_corrupt_bookkeeping() {
        // Two `put`s for the SAME text (the get-or-parse TOCTOU race two threads can
        // hit on a cold key) must not grow the map past one entry for that key nor
        // leave `recency` out of sync with `map`.
        let c = PlanCache::with_cap(8);
        let text = "MATCH (a:Person) RETURN a";
        c.put(text, q(text));
        c.put(text, q(text));
        assert_eq!(c.len(), 1);
        let inner = c.inner.lock().unwrap();
        assert_eq!(inner.recency.len(), inner.map.len());
    }

    #[test]
    fn global_singleton_is_reused_across_calls() {
        // `global()` always returns the SAME process-wide instance.
        assert!(std::ptr::eq(global(), global()));
    }
}
