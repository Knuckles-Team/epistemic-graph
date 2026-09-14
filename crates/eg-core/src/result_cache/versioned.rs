use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use super::ResultCache;

/// Cache key: the query identity (a 128-bit hash of the query kind + text/plan), the
/// GraphCore version the result was computed against, and the RLS ACTOR-SCOPE hash
/// (CONCEPT:EG-KG.query.rls-scoped-result-cache). A different version is a different key,
/// so a write (which bumps the version) makes every prior result unreachable; a different
/// `actor_scope_hash` is ALSO a different key, so a result computed under one RLS actor's
/// row-visibility is NEVER served to a different actor. When security/RLS is off the
/// scope is `0` for every caller, so the key collapses to the plain `(query_hash, version)`
/// pair and single-tenant caching is byte-for-byte the pre-RLS behavior.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct Key {
    pub(super) query_hash: u128,
    pub(super) version: u64,
    pub(super) actor_scope_hash: u64,
}

/// One cached result + its last-access tick (for LRU recency).
pub(super) struct Entry {
    /// Shared so a hit clones only a pointer while holding the cache mutex; the
    /// caller-required owned `Vec` copy happens after recency bookkeeping unlocks.
    pub(super) bytes: Arc<Vec<u8>>,
    pub(super) tick: u64,
}

/// Storage for the version-keyed namespace and its LRU index.
pub(super) struct VersionedEntries {
    /// Tick order contains exactly one entry for every key in `map`.
    pub(super) map: HashMap<Key, Entry>,
    pub(super) recency: BTreeMap<u64, Key>,
}

impl VersionedEntries {
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
        if let Some((old_tick, bytes)) = inner.versioned.map.get_mut(&key).map(|entry| {
            let old_tick = entry.tick;
            entry.tick = now;
            (old_tick, Arc::clone(&entry.bytes))
        }) {
            inner.versioned.recency.remove(&old_tick);
            inner.versioned.recency.insert(now, key);
            drop(inner);
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // The API intentionally returns owned bytes. Perform that O(payload)
            // copy off-lock so one large cached response cannot serialize unrelated
            // cache hits behind the LRU mutex.
            Some((*bytes).clone())
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
        let old_tick = inner.versioned.map.get(&key).map(|entry| entry.tick);
        // Evict the LRU entry if inserting a NEW key would exceed capacity.
        if old_tick.is_none() && inner.versioned.map.len() >= self.cap {
            if let Some((&lru_tick, &lru_key)) = inner.versioned.recency.first_key_value() {
                inner.versioned.recency.remove(&lru_tick);
                inner.versioned.map.remove(&lru_key);
            }
        } else if let Some(old_tick) = old_tick {
            inner.versioned.recency.remove(&old_tick);
        }
        inner.versioned.map.insert(
            key,
            Entry {
                bytes: Arc::new(bytes),
                tick: now,
            },
        );
        inner.versioned.recency.insert(now, key);
    }
}
