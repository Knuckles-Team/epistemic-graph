use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::dep_scope::{DepClock, DepSet};

use super::ResultCache;

/// Cache key for a DEPENDENCY-SCOPED entry (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation,
/// W1.6/P7): the query identity + RLS actor scope, WITHOUT the graph version. Unlike the
/// version-keyed [`super::versioned::Key`], a dependency-scoped entry is not retired by every
/// write — it is keyed on identity alone and revalidated against the [`DepClock`] on each lookup,
/// so it survives any write DISJOINT from the query's dependency set.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct DepKey {
    pub(super) query_hash: u128,
    pub(super) actor_scope_hash: u64,
}

/// One dependency-scoped cached result: the bytes, the graph version it was computed at, the
/// dependency set it READ, and its LRU tick. Validity is `DepClock::is_valid(deps, computed_at)`.
pub(super) struct DepEntry {
    pub(super) bytes: Arc<Vec<u8>>,
    /// The graph `version()` the result was computed against — the reference point the clock's
    /// per-dimension write versions are compared against.
    pub(super) computed_at: u64,
    /// The dimensions this result depends on. A write invalidates the entry iff its change-set
    /// overlaps this set (or floors the clock past `computed_at`).
    pub(super) deps: DepSet,
    pub(super) tick: u64,
}

/// Storage for the dependency-scoped namespace and its LRU index.
pub(super) struct DependencyEntries {
    /// Dependency-scoped entries (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation,
    /// W1.6/P7): keyed on query identity + actor scope (NOT version), revalidated against the
    /// `DepClock` on lookup. This namespace has an LRU independently bounded by `ResultCache::cap`.
    pub(super) map: HashMap<DepKey, DepEntry>,
    /// Tick order for `map`; exactly one entry per key.
    pub(super) recency: BTreeMap<u64, DepKey>,
}

impl DependencyEntries {
    pub(super) fn new() -> Self {
        Self {
            map: HashMap::new(),
            recency: BTreeMap::new(),
        }
    }

    pub(super) fn clear(&mut self) {
        self.map.clear();
        self.recency.clear();
    }
}

impl ResultCache {
    /// Look up a DEPENDENCY-SCOPED cached result (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation,
    /// W1.6/P7). Unlike [`get_scoped`](Self::get_scoped), the entry is keyed on identity alone
    /// (`query_hash` + `actor_scope_hash`, no version) and is served across any write that did NOT
    /// touch its dependency set: `clock.is_valid(entry.deps, entry.computed_at)` decides. A hit
    /// updates recency and the hit counter; a lookup that finds a now-STALE entry (a write
    /// overlapped its deps, or floored the clock) EVICTS it and misses, so a subsequent recompute
    /// re-populates it. This is the path a query with a soundly computable dependency set uses
    /// instead of the version-keyed path; a query whose dependencies cannot be computed keeps
    /// using `get`/`put` (coarse, version-keyed) unchanged.
    pub fn get_dep(
        &self,
        query_hash: u128,
        actor_scope_hash: u64,
        clock: &DepClock,
    ) -> Option<Vec<u8>> {
        if self.cap == 0 {
            self.misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return None;
        }
        let key = DepKey {
            query_hash,
            actor_scope_hash,
        };
        let mut inner = self.inner.lock();
        inner.clock += 1;
        let now = inner.clock;
        // Read the entry's validity + old tick, ending the immutable borrow before mutating.
        let (present, valid, old_tick) = match inner.dependency.map.get(&key) {
            Some(entry) => (
                true,
                clock.is_valid(&entry.deps, entry.computed_at),
                entry.tick,
            ),
            None => (false, false, 0),
        };
        if !present {
            drop(inner);
            self.misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return None;
        }
        if !valid {
            // A write since `computed_at` overlapped this query's dependency set (or floored the
            // clock). Evict the stale entry so the recompute can re-cache it.
            inner.dependency.recency.remove(&old_tick);
            inner.dependency.map.remove(&key);
            drop(inner);
            self.misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // GOOD LOGS: the eviction decision — a cached result was retired because a write
            // touched one of its dependencies (the correctness-critical path).
            tracing::debug!(
                target: "epistemic_graph::dep_cache",
                query_hash,
                "dependency-scoped hit evicted: a dependency was written since it was computed"
            );
            self.maybe_log_rate();
            return None;
        }
        let bytes = {
            let entry = inner
                .dependency
                .map
                .get_mut(&key)
                .expect("present checked above");
            entry.tick = now;
            Arc::clone(&entry.bytes)
        };
        inner.dependency.recency.remove(&old_tick);
        inner.dependency.recency.insert(now, key);
        drop(inner);
        self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.maybe_log_rate();
        // Owned copy off-lock, mirroring `get_scoped`.
        Some((*bytes).clone())
    }

    /// GOOD LOGS: periodically emit the cache hit-rate + size — the "periodic hit-rate/eviction
    /// metric" (W1.6/P7). Fires once every 1024 accesses (cheap modulo on the shared counters), so
    /// it never floods yet gives a running picture of how well dependency-scoping is holding the
    /// hit-rate up under a mixed read/write workload.
    fn maybe_log_rate(&self) {
        let hits = self.hits.load(std::sync::atomic::Ordering::Relaxed);
        let misses = self.misses.load(std::sync::atomic::Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 || !total.is_multiple_of(1024) {
            return;
        }
        let (version_entries, dep_entries) = {
            let inner = self.inner.lock();
            (inner.versioned.map.len(), inner.dependency.map.len())
        };
        tracing::debug!(
            target: "epistemic_graph::dep_cache",
            hits,
            misses,
            hit_rate = hits as f64 / total as f64,
            version_entries,
            dep_entries,
            "result-cache hit-rate metric"
        );
    }

    /// Insert a DEPENDENCY-SCOPED result computed at graph version `computed_at` with dependency
    /// set `deps` (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation, W1.6/P7). Evicts the LRU
    /// dependency entry first when at capacity. A no-op when the cache is disabled (`cap == 0`).
    pub fn put_dep(
        &self,
        query_hash: u128,
        actor_scope_hash: u64,
        computed_at: u64,
        deps: DepSet,
        bytes: Vec<u8>,
    ) {
        if self.cap == 0 {
            return;
        }
        let key = DepKey {
            query_hash,
            actor_scope_hash,
        };
        let mut inner = self.inner.lock();
        inner.clock += 1;
        let now = inner.clock;
        let old_tick = inner.dependency.map.get(&key).map(|entry| entry.tick);
        if old_tick.is_none() && inner.dependency.map.len() >= self.cap {
            if let Some((&lru_tick, &lru_key)) = inner.dependency.recency.first_key_value() {
                inner.dependency.recency.remove(&lru_tick);
                inner.dependency.map.remove(&lru_key);
            }
        } else if let Some(old_tick) = old_tick {
            inner.dependency.recency.remove(&old_tick);
        }
        inner.dependency.map.insert(
            key,
            DepEntry {
                bytes: Arc::new(bytes),
                computed_at,
                deps,
                tick: now,
            },
        );
        inner.dependency.recency.insert(now, key);
    }
}
