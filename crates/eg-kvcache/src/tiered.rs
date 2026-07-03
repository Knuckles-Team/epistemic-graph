//! The tiered KV-cache store (CONCEPT:EG-185).
//!
//! [`TieredCache`] is a three-tier, byte-budgeted cache for KV-cache blocks that lets an
//! LLM runtime **survive OOM by offloading blocks to cheaper tiers** instead of dropping
//! them (or crashing):
//!
//! * **HOT** — raw blocks in RAM, an importance/recency-scored LRU. Fastest; smallest
//!   byte budget. Every access bumps the block's score.
//! * **WARM** — still in RAM but **compressed** ([`crate::compress`]), so more blocks
//!   fit before anything has to leave RAM.
//! * **COLD** — a [`ColdStore`] blob device (an in-RAM map by default, or a durable redb
//!   store behind the `durable` feature). This is the disk-offload rung: bytes that no
//!   longer fit in RAM live here and, with `durable`, survive a restart.
//!
//! **Movement is score-driven.** On a hit a block is *promoted* toward HOT and its score
//! rises. Under byte pressure the lowest-scoring UNPINNED block is *demoted* one rung
//! (HOT→WARM→COLD) — its bytes move, they are NOT dropped — and only a block that falls
//! out of the COLD budget is finally dropped. [`TieredCache::pin`] protects a block from
//! ALL eviction (e.g. a shared prefix's KV every request needs).
//!
//! All capacity is accounted in **bytes** per tier (KV pages vary wildly in size), never
//! in entry count.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use crate::cold::{ColdStore, MemoryColdStore};
use crate::compress::{Codec, StoredBlock};
use crate::value::CacheValue;
use crate::version::DataVersion;

/// A HOT-tier resident: the raw value plus its score / recency / size.
struct HotEntry<V> {
    value: V,
    score: f64,
    last: u64,
    bytes: usize,
}

/// A WARM-tier resident: the compressed block plus its score / recency.
struct WarmEntry {
    block: StoredBlock,
    score: f64,
    last: u64,
}

/// COLD-tier per-key metadata (the bytes live in the [`ColdStore`]).
struct ColdMeta {
    /// The codec the spilled bytes were produced with (CONCEPT:EG-315) — the tag a
    /// promote reconstructs the block by.
    codec: Codec,
    orig_len: usize,
    stored_len: usize,
    score: f64,
    last: u64,
}

/// Which tier a key currently resides in (used by [`TieredCache::tier_of`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    /// Raw, in RAM.
    Hot,
    /// Compressed, in RAM.
    Warm,
    /// In the cold blob device (possibly durable).
    Cold,
}

/// A point-in-time snapshot of cache occupancy + activity counters (CONCEPT:EG-185).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Entries currently in HOT.
    pub hot_entries: usize,
    /// Entries currently in WARM.
    pub warm_entries: usize,
    /// Entries currently in COLD.
    pub cold_entries: usize,
    /// Bytes resident in HOT (raw sizes).
    pub hot_bytes: usize,
    /// Bytes resident in WARM (compressed sizes).
    pub warm_bytes: usize,
    /// Bytes resident in COLD (compressed sizes).
    pub cold_bytes: usize,
    /// Keys currently pinned.
    pub pinned: usize,
    /// Lifetime `get` hits (any tier).
    pub hits: u64,
    /// Lifetime `get` misses.
    pub misses: u64,
    /// Lifetime promotions (a hit in WARM/COLD moved back to HOT).
    pub promotions: u64,
    /// Lifetime HOT→WARM demotions.
    pub demotions_to_warm: u64,
    /// Lifetime WARM→COLD demotions.
    pub demotions_to_cold: u64,
    /// Lifetime blocks dropped (fell out of the COLD budget, or a cold write failed).
    pub drops: u64,
    /// Lifetime entries RETIRED as stale by a data-version bump (CONCEPT:EG-364) — either
    /// eagerly on [`TieredCache::set_data_version`] or lazily on a [`TieredCache::get`] of
    /// a now-stale key. The invalidation proof counter.
    pub invalidations: u64,
}

/// A three-tier, byte-budgeted, score-driven KV-cache store (CONCEPT:EG-185).
///
/// `K` is the block key (e.g. a token-span / prefix hash), `V` the block value (any
/// [`CacheValue`]; the common case is [`crate::Block`] = `Vec<u8>`). Construct with
/// [`TieredCache::new`] for the pure-RAM cold tier, or [`TieredCache::with_cold_store`]
/// to plug in a durable ([`crate::cold::RedbColdStore`]) device.
pub struct TieredCache<K, V>
where
    K: Hash + Eq + Clone,
    V: CacheValue,
{
    hot: HashMap<K, HotEntry<V>>,
    warm: HashMap<K, WarmEntry>,
    cold_meta: HashMap<K, ColdMeta>,
    cold_store: Box<dyn ColdStore<K>>,
    pinned: HashSet<K>,

    /// The [`DataVersion`] each resident key was derived at (CONCEPT:EG-364), layered
    /// ON TOP of the tier maps so the tiering/dedup/LRU/pinning machinery is untouched.
    /// A key absent here (or mapped to [`DataVersion::Agnostic`]) is a pure KV page that
    /// never goes stale — so a cache used purely for content-addressed pages (`put`, never
    /// `set_data_version`) is unaffected and zero-cost.
    versions: HashMap<K, DataVersion>,
    /// The cache's current data version; [`DataVersion::Agnostic`] until a
    /// [`TieredCache::set_data_version`] call (tracking inactive = nothing stale).
    current_version: DataVersion,

    hot_cap: usize,
    warm_cap: usize,
    cold_cap: usize,

    /// The codec used to compress blocks on HOT→WARM demotion (CONCEPT:EG-315). Defaults
    /// to the dependency-free [`Codec::Rle`]; set a real codec via
    /// [`TieredCache::with_warm_codec`].
    warm_codec: Codec,

    hot_bytes: usize,
    warm_bytes: usize,
    cold_bytes: usize,

    clock: u64,
    // Lifetime counters.
    hits: u64,
    misses: u64,
    promotions: u64,
    demotions_to_warm: u64,
    demotions_to_cold: u64,
    drops: u64,
    invalidations: u64,
}

impl<K, V> TieredCache<K, V>
where
    K: Hash + Eq + Clone,
    V: CacheValue,
{
    /// A cache with the given per-tier **byte** budgets and the default in-RAM COLD
    /// store ([`MemoryColdStore`]) — no durable device, no dependency.
    pub fn new(hot_cap: usize, warm_cap: usize, cold_cap: usize) -> Self
    where
        K: 'static,
    {
        Self::with_cold_store(
            hot_cap,
            warm_cap,
            cold_cap,
            Box::new(MemoryColdStore::new()),
        )
    }

    /// A cache with an explicit COLD backing store — e.g. a durable
    /// [`crate::cold::RedbColdStore`] so demoted bytes survive an OOM / restart
    /// (CONCEPT:EG-185).
    pub fn with_cold_store(
        hot_cap: usize,
        warm_cap: usize,
        cold_cap: usize,
        cold_store: Box<dyn ColdStore<K>>,
    ) -> Self {
        TieredCache {
            hot: HashMap::new(),
            warm: HashMap::new(),
            cold_meta: HashMap::new(),
            cold_store,
            pinned: HashSet::new(),
            versions: HashMap::new(),
            current_version: DataVersion::Agnostic,
            hot_cap,
            warm_cap,
            cold_cap,
            warm_codec: Codec::default(),
            hot_bytes: 0,
            warm_bytes: 0,
            cold_bytes: 0,
            clock: 0,
            hits: 0,
            misses: 0,
            promotions: 0,
            demotions_to_warm: 0,
            demotions_to_cold: 0,
            drops: 0,
            invalidations: 0,
        }
    }

    /// Select the WARM-tier compression codec (CONCEPT:EG-315), builder-style.
    ///
    /// The default is [`Codec::Rle`] (dependency-free). Pass [`Codec::Zstd`] (feature
    /// `compression`) or [`Codec::Lz4`] (feature `lz4`) for a real codec, or
    /// [`Codec::Raw`] to disable WARM compression entirely. Only affects blocks demoted
    /// AFTER the call; the raw fallback still applies per-block, so incompressible pages
    /// are never expanded whatever the choice.
    pub fn with_warm_codec(mut self, codec: Codec) -> Self {
        self.warm_codec = codec;
        self
    }

    /// The WARM-tier compression codec this cache demotes with (CONCEPT:EG-315).
    pub fn warm_codec(&self) -> Codec {
        self.warm_codec
    }

    /// Monotonic logical clock tick (recency tiebreak for eviction).
    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    /// Insert / overwrite a block as a version-[`Agnostic`](DataVersion::Agnostic) pure KV
    /// page (never version-invalidated). It lands in HOT with a fresh score; byte pressure
    /// is then resolved by cascading demotion (CONCEPT:EG-185). Use
    /// [`put_versioned`](Self::put_versioned) for DERIVED context that must go stale on a
    /// graph write.
    pub fn put(&mut self, key: K, value: V) {
        self.put_versioned(key, value, DataVersion::Agnostic);
    }

    /// Insert / overwrite a block stamped with the [`DataVersion`] it was `derived_at`
    /// (CONCEPT:EG-364). Once the cache's current version moves past it (via
    /// [`set_data_version`](Self::set_data_version)) the entry is retired / misses. Tiering
    /// is otherwise identical to [`put`](Self::put) — the version is layered on top and
    /// does not affect scoring / demotion.
    pub fn put_versioned(&mut self, key: K, value: V, derived_at: DataVersion) {
        // Drop any prior copy in every tier so accounting stays exact.
        self.remove_resident(&key);
        let clk = self.tick();
        let bytes = value.byte_len();
        self.hot_bytes += bytes;
        self.versions.insert(key.clone(), derived_at);
        self.hot.insert(
            key,
            HotEntry {
                value,
                score: 1.0,
                last: clk,
                bytes,
            },
        );
        self.enforce_hot();
    }

    /// The cache's current data version (CONCEPT:EG-364).
    pub fn current_version(&self) -> DataVersion {
        self.current_version
    }

    /// Advance the cache's current data version (CONCEPT:EG-364) — the hook a graph write
    /// drives (a committed write bumps `GraphCore::version()`, KG-2.180, then calls this).
    /// EAGERLY retires every version-tagged entry now stale (across ALL tiers, freeing its
    /// bytes — pinned or not, because staleness is a correctness concern that overrides the
    /// keep-hot pin contract), mirroring `ResultCache`'s atomic version-bump retire
    /// (KG-2.233). [`DataVersion::Agnostic`] pure KV pages are untouched.
    pub fn set_data_version(&mut self, version: DataVersion) {
        self.current_version = version;
        let stale: Vec<K> = self
            .versions
            .iter()
            .filter(|(_, v)| !v.is_fresh(version))
            .map(|(k, _)| k.clone())
            .collect();
        for k in stale {
            self.remove_resident(&k);
            self.versions.remove(&k);
            self.invalidations += 1;
        }
    }

    /// Whether `key` is resident but STALE for the current data version (CONCEPT:EG-364).
    fn is_stale(&self, key: &K) -> bool {
        self.versions
            .get(key)
            .map(|v| !v.is_fresh(self.current_version))
            .unwrap_or(false)
    }

    /// Look a block up. On a hit it is promoted toward HOT and its score bumped; on a
    /// miss returns `None`. Requires `&mut self` because a hit mutates tiers / scores.
    ///
    /// A resident-but-STALE entry (derived at an older data version, CONCEPT:EG-364) is
    /// treated as a MISS and retired here (the lazy backstop to
    /// [`set_data_version`](Self::set_data_version)'s eager sweep) so stale LLM/agent
    /// context is never served.
    pub fn get(&mut self, key: &K) -> Option<V> {
        if self.is_stale(key) {
            self.remove_resident(key);
            self.versions.remove(key);
            self.invalidations += 1;
            self.misses += 1;
            return None;
        }
        let clk = self.tick();
        if let Some(e) = self.hot.get_mut(key) {
            e.score += 1.0;
            e.last = clk;
            self.hits += 1;
            return Some(e.value.clone());
        }
        // WARM hit → decompress + promote to HOT.
        if let Some(w) = self.warm.remove(key) {
            self.warm_bytes -= w.block.stored_len();
            let bytes = w.block.decode();
            let value = V::from_bytes(&bytes);
            self.hits += 1;
            self.promotions += 1;
            self.insert_hot(key.clone(), value, w.score + 1.0, clk);
            return self.hot.get(key).map(|e| e.value.clone());
        }
        // COLD hit → read blob + decompress + promote to HOT.
        if let Some(m) = self.cold_meta.remove(key) {
            self.cold_bytes -= m.stored_len;
            let blob = self.cold_store.get(key).ok().flatten();
            let _ = self.cold_store.remove(key);
            if let Some(raw) = blob {
                let sb = StoredBlock {
                    data: raw,
                    codec: m.codec,
                    orig_len: m.orig_len,
                };
                let value = V::from_bytes(&sb.decode());
                self.hits += 1;
                self.promotions += 1;
                self.insert_hot(key.clone(), value, m.score + 1.0, clk);
                return self.hot.get(key).map(|e| e.value.clone());
            }
        }
        self.misses += 1;
        None
    }

    /// Whether `key` is resident in ANY tier AND still fresh for the current data version
    /// (a stale entry reads as absent, CONCEPT:EG-364).
    pub fn contains(&self, key: &K) -> bool {
        if self.is_stale(key) {
            return false;
        }
        self.hot.contains_key(key)
            || self.warm.contains_key(key)
            || self.cold_meta.contains_key(key)
    }

    /// Which tier `key` currently lives in, if resident and fresh. A stale entry
    /// (CONCEPT:EG-364) reports `None`.
    pub fn tier_of(&self, key: &K) -> Option<Tier> {
        if self.is_stale(key) {
            return None;
        }
        if self.hot.contains_key(key) {
            Some(Tier::Hot)
        } else if self.warm.contains_key(key) {
            Some(Tier::Warm)
        } else if self.cold_meta.contains_key(key) {
            Some(Tier::Cold)
        } else {
            None
        }
    }

    /// Pin a key so it is NEVER demoted or evicted (CONCEPT:EG-185). If resident it is
    /// pulled up to HOT; a not-yet-resident key is remembered so a later `put` is pinned.
    pub fn pin(&mut self, key: &K) {
        self.pinned.insert(key.clone());
        // Pull a resident-but-cold/warm pinned block up into HOT (protected + fast).
        if !self.hot.contains_key(key) {
            let clk = self.tick();
            if let Some(w) = self.warm.remove(key) {
                self.warm_bytes -= w.block.stored_len();
                let value = V::from_bytes(&w.block.decode());
                self.insert_hot(key.clone(), value, w.score, clk);
            } else if let Some(m) = self.cold_meta.remove(key) {
                self.cold_bytes -= m.stored_len;
                if let Some(raw) = self.cold_store.get(key).ok().flatten() {
                    let _ = self.cold_store.remove(key);
                    let sb = StoredBlock {
                        data: raw,
                        codec: m.codec,
                        orig_len: m.orig_len,
                    };
                    let value = V::from_bytes(&sb.decode());
                    self.insert_hot(key.clone(), value, m.score, clk);
                }
            }
        }
    }

    /// Unpin a key, making it eligible for demotion / eviction again.
    pub fn unpin(&mut self, key: &K) {
        self.pinned.remove(key);
        self.enforce_hot();
    }

    /// Whether `key` is pinned.
    pub fn is_pinned(&self, key: &K) -> bool {
        self.pinned.contains(key)
    }

    /// Forcibly remove `key` from every tier (and unpin it, and drop its version stamp).
    /// Returns whether it existed.
    pub fn evict(&mut self, key: &K) -> bool {
        self.pinned.remove(key);
        self.versions.remove(key);
        self.remove_resident(key)
    }

    /// A snapshot of occupancy + lifetime counters (CONCEPT:EG-185).
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hot_entries: self.hot.len(),
            warm_entries: self.warm.len(),
            cold_entries: self.cold_meta.len(),
            hot_bytes: self.hot_bytes,
            warm_bytes: self.warm_bytes,
            cold_bytes: self.cold_bytes,
            pinned: self.pinned.len(),
            hits: self.hits,
            misses: self.misses,
            promotions: self.promotions,
            demotions_to_warm: self.demotions_to_warm,
            demotions_to_cold: self.demotions_to_cold,
            drops: self.drops,
            invalidations: self.invalidations,
        }
    }

    /// Total resident entries across all tiers.
    pub fn len(&self) -> usize {
        self.hot.len() + self.warm.len() + self.cold_meta.len()
    }

    /// Whether the cache holds no resident blocks.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // ---- internal machinery -------------------------------------------------

    /// Insert straight into HOT (used by promotion) then rebalance.
    fn insert_hot(&mut self, key: K, value: V, score: f64, clk: u64) {
        let bytes = value.byte_len();
        self.hot_bytes += bytes;
        self.hot.insert(
            key,
            HotEntry {
                value,
                score,
                last: clk,
                bytes,
            },
        );
        self.enforce_hot();
    }

    /// Remove a key from whichever tier it is in, fixing byte accounting. Returns
    /// whether it was resident. Does NOT touch the pinned set.
    fn remove_resident(&mut self, key: &K) -> bool {
        if let Some(e) = self.hot.remove(key) {
            self.hot_bytes -= e.bytes;
            true
        } else if let Some(w) = self.warm.remove(key) {
            self.warm_bytes -= w.block.stored_len();
            true
        } else if let Some(m) = self.cold_meta.remove(key) {
            self.cold_bytes -= m.stored_len;
            let _ = self.cold_store.remove(key);
            true
        } else {
            false
        }
    }

    /// Pick the lowest-scoring UNPINNED key of a tier (tiebreak: oldest access first).
    fn lowest_hot(&self) -> Option<K> {
        self.hot
            .iter()
            .filter(|(k, _)| !self.pinned.contains(*k))
            .min_by(|a, b| {
                a.1.score
                    .partial_cmp(&b.1.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.1.last.cmp(&b.1.last))
            })
            .map(|(k, _)| k.clone())
    }

    fn lowest_warm(&self) -> Option<K> {
        self.warm
            .iter()
            .filter(|(k, _)| !self.pinned.contains(*k))
            .min_by(|a, b| {
                a.1.score
                    .partial_cmp(&b.1.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.1.last.cmp(&b.1.last))
            })
            .map(|(k, _)| k.clone())
    }

    fn lowest_cold(&self) -> Option<K> {
        self.cold_meta
            .iter()
            .filter(|(k, _)| !self.pinned.contains(*k))
            .min_by(|a, b| {
                a.1.score
                    .partial_cmp(&b.1.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.1.last.cmp(&b.1.last))
            })
            .map(|(k, _)| k.clone())
    }

    /// Enforce the HOT byte budget by demoting to WARM, then cascade downward. Pinned
    /// blocks are never chosen, so a pinned working set may exceed the budget (by
    /// design — pins are a hard "keep this" contract).
    fn enforce_hot(&mut self) {
        while self.hot_bytes > self.hot_cap {
            let Some(k) = self.lowest_hot() else { break };
            let e = self.hot.remove(&k).expect("lowest_hot key present");
            self.hot_bytes -= e.bytes;
            // Demote HOT → WARM: compress the raw block with the configured codec
            // (CONCEPT:EG-315).
            let bytes = e.value.as_bytes();
            let sb = StoredBlock::encode_with(self.warm_codec, &bytes);
            self.warm_bytes += sb.stored_len();
            self.warm.insert(
                k,
                WarmEntry {
                    block: sb,
                    score: e.score,
                    last: e.last,
                },
            );
            self.demotions_to_warm += 1;
        }
        self.enforce_warm();
    }

    /// Enforce the WARM byte budget by demoting to COLD, then cascade.
    fn enforce_warm(&mut self) {
        while self.warm_bytes > self.warm_cap {
            let Some(k) = self.lowest_warm() else { break };
            let w = self.warm.remove(&k).expect("lowest_warm key present");
            self.warm_bytes -= w.block.stored_len();
            // Demote WARM → COLD: spill the compressed bytes to the cold device.
            let stored_len = w.block.stored_len();
            match self.cold_store.put(&k, &w.block.data) {
                Ok(()) => {
                    self.cold_bytes += stored_len;
                    self.cold_meta.insert(
                        k,
                        ColdMeta {
                            codec: w.block.codec,
                            orig_len: w.block.orig_len,
                            stored_len,
                            score: w.score,
                            last: w.last,
                        },
                    );
                    self.demotions_to_cold += 1;
                }
                Err(_) => {
                    // Cold device rejected the write — the block is dropped.
                    self.versions.remove(&k); // it left the cache entirely (EG-364)
                    self.drops += 1;
                }
            }
        }
        self.enforce_cold();
    }

    /// Enforce the COLD byte budget by DROPPING the lowest-scoring cold blocks — the
    /// only place a block leaves the cache entirely.
    fn enforce_cold(&mut self) {
        while self.cold_bytes > self.cold_cap {
            let Some(k) = self.lowest_cold() else { break };
            let m = self.cold_meta.remove(&k).expect("lowest_cold key present");
            self.cold_bytes -= m.stored_len;
            let _ = self.cold_store.remove(&k);
            self.versions.remove(&k); // it left the cache entirely (EG-364)
            self.drops += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Block;

    /// An INCOMPRESSIBLE KV page of `len` bytes seeded by `seed` — consecutive bytes
    /// differ, so the WARM RLE codec falls back to raw and the page keeps its full byte
    /// size in every tier. This is what makes the byte-budget / cascade assertions below
    /// exercise real tier transitions (a run-heavy page would shrink to a few bytes in
    /// WARM and never cascade).
    fn page(len: usize, seed: u8) -> Block {
        (0..len)
            .map(|i| {
                (i as u8)
                    .wrapping_mul(31)
                    .wrapping_add(seed)
                    .wrapping_add(1)
            })
            .collect()
    }

    /// CONCEPT:EG-185 — capacity is accounted in BYTES; a put over the HOT budget
    /// demotes rather than rejects, and stats reflect the byte split.
    #[test]
    fn eg185_capacity_accounting_is_in_bytes() {
        // HOT holds ~100 bytes, WARM/COLD generous.
        let mut c: TieredCache<u64, Block> = TieredCache::new(100, 10_000, 10_000);
        c.put(1, page(40, 1));
        c.put(2, page(40, 2));
        let s = c.stats();
        assert_eq!(s.hot_bytes, 80, "two 40-byte pages fit HOT");
        assert_eq!(s.hot_entries, 2);
        // Third page pushes HOT over 100 ⇒ the lowest-score block demotes to WARM.
        c.put(3, page(40, 3));
        let s = c.stats();
        assert!(s.hot_bytes <= 100, "HOT stays within its byte budget");
        assert_eq!(
            s.hot_entries + s.warm_entries + s.cold_entries,
            3,
            "no block lost"
        );
    }

    /// CONCEPT:EG-185 — the LRU evicts the LOWEST-SCORE block first: accessing a block
    /// raises its score so a colder neighbour is the one demoted.
    #[test]
    fn eg185_lru_evicts_lowest_score_first() {
        let mut c: TieredCache<u64, Block> = TieredCache::new(80, 10_000, 10_000);
        c.put(1, page(40, 1));
        c.put(2, page(40, 2));
        // Bump key 1's score with repeated hits; key 2 stays cold.
        for _ in 0..5 {
            assert!(c.get(&1).is_some());
        }
        // A third block forces one demotion; the low-score key 2 must be the victim.
        c.put(3, page(40, 3));
        assert_eq!(
            c.tier_of(&1),
            Some(Tier::Hot),
            "hot, frequently-used block stays HOT"
        );
        assert_eq!(
            c.tier_of(&2),
            Some(Tier::Warm),
            "lowest-score block demoted"
        );
    }

    /// CONCEPT:EG-185 — a hit in a lower tier PROMOTES the block back to HOT.
    #[test]
    fn eg185_promotion_on_access_moves_block_back_to_hot() {
        let mut c: TieredCache<u64, Block> = TieredCache::new(80, 10_000, 10_000);
        c.put(1, page(40, 1));
        c.put(2, page(40, 2));
        c.put(3, page(40, 3)); // forces a demotion to WARM
                               // Find a block that got demoted, then access it and confirm it is promoted.
        let demoted = [1u64, 2, 3]
            .into_iter()
            .find(|k| c.tier_of(k) == Some(Tier::Warm))
            .expect("some block demoted to WARM");
        assert_eq!(
            c.get(&demoted).unwrap(),
            page(40, demoted as u8),
            "value survives the round-trip"
        );
        assert_eq!(
            c.tier_of(&demoted),
            Some(Tier::Hot),
            "access promoted it back to HOT"
        );
        assert!(c.stats().promotions >= 1);
    }

    /// CONCEPT:EG-185 — demotion under pressure MOVES bytes to WARM/COLD; it does NOT
    /// drop them. The value is still retrievable after cascading all the way to COLD.
    #[test]
    fn eg185_demotion_under_pressure_moves_bytes_not_drops() {
        // Tiny HOT + tiny WARM so a block must cascade HOT→WARM→COLD, but a huge COLD
        // so nothing is actually dropped.
        let mut c: TieredCache<u64, Block> = TieredCache::new(50, 30, 1_000_000);
        for k in 0..10u64 {
            c.put(k, page(40, k as u8));
        }
        let s = c.stats();
        assert_eq!(s.drops, 0, "nothing dropped while COLD has room");
        assert_eq!(
            s.hot_entries + s.warm_entries + s.cold_entries,
            10,
            "all 10 blocks retained"
        );
        assert!(s.cold_entries > 0, "pressure pushed bytes down to COLD");
        // Every block is still retrievable (bytes were offloaded, not lost).
        for k in 0..10u64 {
            assert_eq!(
                c.get(&k),
                Some(page(40, k as u8)),
                "block {k} recoverable after offload"
            );
        }
    }

    /// CONCEPT:EG-185 — a block only leaves the cache when it falls out of the COLD
    /// budget (the final drop rung).
    #[test]
    fn eg185_drop_only_when_cold_budget_exceeded() {
        // All tiers tiny ⇒ blocks must eventually drop out of COLD.
        let mut c: TieredCache<u64, Block> = TieredCache::new(40, 40, 40);
        for k in 0..10u64 {
            c.put(k, page(40, k as u8));
        }
        let s = c.stats();
        assert!(s.drops > 0, "small COLD budget forces drops");
        assert!(c.len() < 10, "some blocks evicted entirely");
    }

    /// CONCEPT:EG-185 — a PINNED block is never demoted or evicted, even under heavy
    /// pressure that drops everything else.
    #[test]
    fn eg185_pin_prevents_eviction_under_pressure() {
        let mut c: TieredCache<u64, Block> = TieredCache::new(40, 40, 40);
        c.put(0, page(40, 0));
        c.pin(&0);
        // Flood the cache; the pinned block must stay HOT throughout. (Inspect the tier
        // WITHOUT `get`, which would bump the block's score and mask the eviction test.)
        for k in 1..50u64 {
            c.put(k, page(40, k as u8));
        }
        assert_eq!(c.tier_of(&0), Some(Tier::Hot), "pinned block never demoted");
        assert!(c.contains(&0), "pinned block still resident");
        // After unpin the low-score block becomes evictable again under pressure.
        c.unpin(&0);
        for k in 50..100u64 {
            c.put(k, page(40, k as u8));
        }
        assert_ne!(
            c.tier_of(&0),
            Some(Tier::Hot),
            "after unpin the block is demoted/dropped under pressure like any other"
        );
    }

    /// CONCEPT:EG-185 — `evict` forcibly removes a block from all tiers (and unpins it).
    #[test]
    fn eg185_evict_forcibly_removes_across_tiers() {
        let mut c: TieredCache<u64, Block> = TieredCache::new(1_000, 1_000, 1_000);
        c.put(7, page(40, 7));
        c.pin(&7);
        assert!(c.evict(&7), "evict reports it existed");
        assert!(!c.contains(&7), "gone from every tier");
        assert!(!c.is_pinned(&7), "evict also unpins");
    }

    /// CONCEPT:EG-185 — re-`put` of an existing key overwrites (no double-accounting).
    #[test]
    fn eg185_reput_overwrites_without_double_accounting() {
        let mut c: TieredCache<u64, Block> = TieredCache::new(1_000, 1_000, 1_000);
        c.put(1, page(40, 1));
        c.put(1, page(60, 1));
        assert_eq!(c.stats().hot_entries, 1);
        assert_eq!(
            c.stats().hot_bytes,
            60,
            "byte accounting reflects the overwrite"
        );
        assert_eq!(c.get(&1), Some(page(60, 1)));
    }

    /// CONCEPT:EG-185 — a miss is counted and returns None.
    #[test]
    fn eg185_miss_returns_none_and_counts() {
        let mut c: TieredCache<u64, Block> = TieredCache::new(1_000, 1_000, 1_000);
        assert_eq!(c.get(&42), None);
        assert_eq!(c.stats().misses, 1);
    }

    /// A compressible KV-page-like block that a real codec shrinks well (a long
    /// low-entropy region), UNLIKE [`page`] which is engineered to defeat the RLE codec.
    fn compressible_page(len: usize, seed: u8) -> Block {
        let mut b = vec![seed; len];
        for (i, x) in b.iter_mut().enumerate().take(len / 8) {
            *x = (i % 4) as u8;
        }
        b
    }

    /// CONCEPT:EG-315 — the default cache still uses the dependency-free RLE codec.
    #[test]
    fn eg315_default_warm_codec_is_rle() {
        let c: TieredCache<u64, Block> = TieredCache::new(100, 100, 100);
        assert_eq!(c.warm_codec(), Codec::Rle);
    }

    /// CONCEPT:EG-315 — with the `zstd` codec selected, a HOT→WARM demote compresses the
    /// block to a WARM copy far smaller than raw, and a later hit PROMOTES it back to HOT
    /// with the exact value intact (demote/promote round-trip through a real codec).
    #[cfg(feature = "compression")]
    #[test]
    fn eg315_zstd_codec_warm_demote_promote_roundtrip() {
        // HOT holds two 512-byte pages (raw); a third forces one demote to WARM. WARM/COLD
        // generous so the demoted page stays WARM (mirrors the EG-185 promotion sizing).
        let mut c: TieredCache<u64, Block> =
            TieredCache::new(1024, 1_000_000, 1_000_000).with_warm_codec(Codec::Zstd);
        assert_eq!(c.warm_codec(), Codec::Zstd);
        for k in 0..3u64 {
            c.put(k, compressible_page(512, k as u8));
        }
        let demoted = [0u64, 1, 2]
            .into_iter()
            .find(|k| c.tier_of(k) == Some(Tier::Warm))
            .expect("some page demoted to WARM");
        let s = c.stats();
        assert!(
            s.warm_bytes < 512,
            "zstd shrank the WARM copy below the raw 512 bytes (got {})",
            s.warm_bytes
        );
        // A hit reconstructs the block byte-exact and promotes it back to HOT.
        assert_eq!(
            c.get(&demoted),
            Some(compressible_page(512, demoted as u8)),
            "zstd WARM promote reconstructs the block"
        );
        assert_eq!(c.tier_of(&demoted), Some(Tier::Hot));
        assert!(c.stats().promotions >= 1);
    }

    /// CONCEPT:EG-315 — the `zstd` codec survives a full HOT→WARM→COLD cascade and back:
    /// every block is still recoverable byte-exact after being spilled to COLD compressed.
    #[cfg(feature = "compression")]
    #[test]
    fn eg315_zstd_codec_cascade_to_cold_recoverable() {
        // HOT holds ~1 page; WARM is smaller than even a zstd-compressed page, so blocks
        // must cascade all the way to COLD. Huge COLD ⇒ nothing dropped.
        let mut c: TieredCache<u64, Block> =
            TieredCache::new(600, 20, 10_000_000).with_warm_codec(Codec::Zstd);
        let pages: Vec<Block> = (0..10u64)
            .map(|k| compressible_page(512, k as u8))
            .collect();
        for (k, p) in pages.iter().enumerate() {
            c.put(k as u64, p.clone());
        }
        let s = c.stats();
        assert_eq!(s.drops, 0, "nothing dropped while COLD has room");
        assert!(
            s.cold_entries > 0,
            "pressure pushed compressed bytes to COLD"
        );
        for (k, p) in pages.iter().enumerate() {
            assert_eq!(
                c.get(&(k as u64)),
                Some(p.clone()),
                "block {k} recoverable byte-exact after a zstd cold spill"
            );
        }
    }

    /// CONCEPT:EG-315 — incompressible pages under the `zstd` codec fall back to raw, so
    /// the tier machinery still cascades on their true (unshrunk) size and every block
    /// round-trips.
    #[cfg(feature = "compression")]
    #[test]
    fn eg315_zstd_codec_incompressible_pages_fall_back_and_cascade() {
        let mut c: TieredCache<u64, Block> =
            TieredCache::new(50, 30, 1_000_000).with_warm_codec(Codec::Zstd);
        for k in 0..10u64 {
            c.put(k, page(40, k as u8)); // high-entropy ⇒ raw fallback even under zstd
        }
        let s = c.stats();
        assert_eq!(s.drops, 0, "nothing dropped while COLD has room");
        assert_eq!(
            s.hot_entries + s.warm_entries + s.cold_entries,
            10,
            "all blocks retained despite raw fallback"
        );
        for k in 0..10u64 {
            assert_eq!(c.get(&k), Some(page(40, k as u8)), "block {k} recoverable");
        }
    }

    /// CONCEPT:EG-364 — a versioned (derived-context) block is served while the data
    /// version holds, then MISSES after a `set_data_version` bump, whatever tier it sits
    /// in. Eager retire frees it and the invalidation counter records it.
    #[test]
    fn eg359_version_bump_invalidates_across_tiers() {
        let mut c: TieredCache<u64, Block> = TieredCache::new(1_000, 1_000, 1_000);
        c.set_data_version(DataVersion::At(0));
        c.put_versioned(1, page(40, 1), DataVersion::At(0));
        assert_eq!(c.get(&1), Some(page(40, 1)));
        // A graph write bumps the version ⇒ the entry is stale ⇒ retired + miss.
        c.set_data_version(DataVersion::At(1));
        assert_eq!(c.get(&1), None, "stale block is a miss");
        assert!(!c.contains(&1));
        assert_eq!(c.tier_of(&1), None);
        assert_eq!(c.stats().invalidations, 1);
        assert!(c.is_empty(), "stale block retired, cache empty");
        // Re-derive at the new version ⇒ served again (fresh content).
        c.put_versioned(1, page(60, 1), DataVersion::At(1));
        assert_eq!(c.get(&1), Some(page(60, 1)));
    }

    /// CONCEPT:EG-364 — the lazy backstop: even WITHOUT calling `set_data_version` to
    /// eagerly sweep, a `get` of a key whose stamp is behind the current version misses
    /// (belt-and-suspenders). Here we bump first (eager) then confirm a fresh put at the
    /// new version survives — proving no false invalidation of current-version data.
    #[test]
    fn eg359_no_false_invalidation_on_stable_version() {
        let mut c: TieredCache<u64, Block> = TieredCache::new(1_000, 1_000, 1_000);
        c.set_data_version(DataVersion::At(3));
        c.put_versioned(1, page(40, 1), DataVersion::At(3));
        // Version STABLE across many reads ⇒ every read HITs, nothing invalidated.
        for _ in 0..5 {
            assert_eq!(c.get(&1), Some(page(40, 1)));
        }
        assert_eq!(
            c.stats().invalidations,
            0,
            "stable version ⇒ no invalidation"
        );
    }

    /// CONCEPT:EG-364 — `put` (version-agnostic pure KV pages) is immune to version bumps,
    /// so all existing tiering behaviour is preserved when versioning is unused.
    #[test]
    fn eg359_agnostic_puts_survive_version_bumps() {
        let mut c: TieredCache<u64, Block> = TieredCache::new(1_000, 1_000, 1_000);
        c.put(1, page(40, 1)); // Agnostic
        c.set_data_version(DataVersion::At(0));
        c.set_data_version(DataVersion::At(99));
        assert_eq!(c.get(&1), Some(page(40, 1)), "pure KV page immune");
        assert_eq!(c.stats().invalidations, 0);
    }

    /// CONCEPT:EG-364 — a stale versioned block that has cascaded down to COLD is still
    /// correctly invalidated (retired from the cold tier + its cold blob removed).
    #[test]
    fn eg359_invalidates_a_block_sitting_in_cold() {
        // Tiny HOT/WARM so a versioned block cascades to COLD; generous COLD keeps it.
        let mut c: TieredCache<u64, Block> = TieredCache::new(50, 30, 1_000_000);
        c.set_data_version(DataVersion::At(0));
        c.put_versioned(1, page(40, 1), DataVersion::At(0));
        // Flood with agnostic pages to push key 1 down the tiers.
        for k in 10..30u64 {
            c.put(k, page(40, k as u8));
        }
        assert!(c.contains(&1), "still resident (offloaded, not dropped)");
        // Bump the version ⇒ key 1 retired wherever it landed.
        c.set_data_version(DataVersion::At(1));
        assert_eq!(c.get(&1), None, "stale cold block invalidated");
        assert!(c.stats().invalidations >= 1);
    }
}
