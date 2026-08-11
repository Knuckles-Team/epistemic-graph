//! Networked (mutation-store-backed) shared KV-cache backend (CONCEPT:EG-KG.backend.networked-shared-kv).
//!
//! Where [`eg_kvcache::SharedKvIndex`] (EG-186) is an **in-process, ephemeral**
//! content-addressed index, [`SharedKvStoreBackend`] is the same
//! [`eg_kvcache::SharedKvBackend`] seam over the engine's **durable, mutation-store-backed
//! KV store** ([`crate::server::kv::KvStore`], the `kv.redb` the `Method::KvGet`/`KvPut`
//! wire surface owns). This is the documented follow-up the `eg-kvcache` crate report and
//! the HTTP surface both point at ("A follow-up networked backend would swap this for an
//! RPC / object-store client implementing the SAME `SharedKvBackend` trait").
//!
//! ## Why this makes the cache FLEET-SHARED
//!
//! Every LLM-serving instance (a vLLM / LMCache worker) that reaches THIS engine — over
//! the `/kv` HTTP surface or the `KvGet`/`KvPut` wire methods — reads and writes the SAME
//! `kv.redb`. So a prefix block a warm worker (instance A) PUTs is a cache **HIT** for a
//! cold worker (instance B): cross-instance prefix reuse *through the engine*, not merely
//! within one process. And because the store is redb-durable (commit-before-ack), the
//! shared cache **survives an engine restart** — the L2 tier the `docs/interfaces/kvcache.md`
//! hierarchy calls "durable + dedup · shared cross-instance".
//!
//! ## Storage layout
//!
//! One namespace holds the blocks, one holds the invalidation epoch:
//!
//! * `kvcache-blocks / <token-hash> -> [tag u8][version u64 BE][KV page bytes…]` — the
//!   block, framed with the [`DataVersion`] it was derived at (`tag 0` = `Agnostic`
//!   pure content-addressed page; `tag 1` = `At(v)` derived context).
//! * `kvcache-meta / data_version -> <u64 BE>` — the store's current data version
//!   (CONCEPT:EG-KG.storage.content-addressed-put). Absent ⇒ version-tracking inactive
//!   (`Agnostic`). Persisted so EVERY instance agrees on freshness across the fleet — a
//!   graph write on one node invalidates stale derived-context for all of them.
//!
//! ## Freshness is gated LAZILY (durable-appropriate)
//!
//! `SharedKvIndex::set_data_version` EAGERLY retires stale entries to free RAM. A durable
//! store gates **lazily on read** instead (a stale entry reads as absent) and persists the
//! new epoch to `kvcache-meta`; scanning-and-deleting the whole namespace on every graph
//! write would be an unbounded cost per write. Callers that want to reclaim disk run the
//! bounded [`SharedKvStoreBackend::retire_stale`] sweep out of band. A pure content-addressed
//! page (`Agnostic`) never reads the epoch at all — the dedup hot path stays a single redb
//! point-read.
//!
//! ## Metrics (Seam-6 hit-rate, W3.7 shape)
//!
//! Atomic `get_hits` / `get_misses` / `dedup_hits` / `put_new` / `stale_missed` counters
//! feed [`SharedKvStoreBackend::stats`] and a periodic `tracing` hit-rate line in the SAME
//! shape as `eg-core`'s `result_cache` (`target: "epistemic_graph::kvcache"`, fields
//! `hits`/`misses`/`hit_rate`), plus a per-op `trace` carrying the block hash, hit/miss,
//! and the instance id (so cross-instance reuse is visible in logs).

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use eg_kvcache::{Block, DataVersion, SharedKvBackend};

use crate::server::kv::KvStore;

/// KV namespace for the framed KV-cache block bytes.
const BLOCK_NS: &str = "kvcache-blocks";
/// KV namespace for the invalidation-epoch meta key.
const META_NS: &str = "kvcache-meta";
/// Meta key holding the store's current [`DataVersion`] (absent ⇒ `Agnostic`).
const VERSION_KEY: &str = "data_version";
/// Fixed frame header: 1 discriminant byte + 8 big-endian version bytes.
const FRAME_HEADER: usize = 9;
/// Emit the periodic hit-rate metric once every this many accesses (mirrors
/// `result_cache::maybe_log_rate`'s 1024-access cadence — cheap, never floods).
const HIT_RATE_LOG_EVERY: u64 = 1024;

/// Occupancy + hit-rate counters for a [`SharedKvStoreBackend`], shaped like
/// [`eg_kvcache::SharedStats`] / the `/kv/stats` JSON so a dashboard consumes either
/// backend identically (the W3.7 Seam-6 hit-rate export shape).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SharedKvStoreStats {
    /// Lifetime `get_block` hits (a fresh block served).
    pub get_hits: u64,
    /// Lifetime `get_block` misses (absent OR stale).
    pub get_misses: u64,
    /// Lifetime `put_block` calls that found the block ALREADY present — a
    /// cross-instance dedup win (another instance had already stored it).
    pub dedup_hits: u64,
    /// Lifetime `put_block` calls that created a NEW block.
    pub put_new: u64,
    /// Subset of `get_misses` that missed specifically because the entry was STALE
    /// (version-invalidated), not absent — the invalidation-proof counter.
    pub stale_missed: u64,
}

impl SharedKvStoreStats {
    /// Hit-rate over all `get_block` calls (`0.0` when there were none).
    pub fn hit_rate(&self) -> f64 {
        let total = self.get_hits + self.get_misses;
        if total == 0 {
            0.0
        } else {
            self.get_hits as f64 / total as f64
        }
    }
}

/// A content-addressed shared KV-cache backend over the engine's durable,
/// mutation-store-backed [`KvStore`] (CONCEPT:EG-KG.backend.networked-shared-kv).
///
/// The canonical NETWORKED [`SharedKvBackend`]: identical blocks are stored ONCE in
/// `kv.redb` and shared by every serving instance that reaches this engine. Holds an
/// `Arc<KvStore>` (so the HTTP surface and the `KvGet`/`KvPut` wire methods share ONE
/// store) plus atomic hit-rate counters; all real work is `&self` (redb ops are `&self`,
/// counters are atomic), so the `&mut self` in the trait is a pure signature detail.
pub struct SharedKvStoreBackend {
    store: Arc<KvStore>,
    /// This serving instance's id — stamped on every metric so cross-instance reuse is
    /// visible in the logs (e.g. instance B logging a hit on a hash instance A stored).
    instance_id: String,
    get_hits: AtomicU64,
    get_misses: AtomicU64,
    dedup_hits: AtomicU64,
    put_new: AtomicU64,
    stale_missed: AtomicU64,
    accesses: AtomicU64,
}

impl SharedKvStoreBackend {
    /// Wrap a durable [`KvStore`] as a fleet-shared KV-cache backend. `instance_id`
    /// labels this serving instance in the metrics/logs.
    pub fn new(store: Arc<KvStore>, instance_id: impl Into<String>) -> Self {
        Self {
            store,
            instance_id: instance_id.into(),
            get_hits: AtomicU64::new(0),
            get_misses: AtomicU64::new(0),
            dedup_hits: AtomicU64::new(0),
            put_new: AtomicU64::new(0),
            stale_missed: AtomicU64::new(0),
            accesses: AtomicU64::new(0),
        }
    }

    /// `true` when the backing store lands writes durably on disk (a persist dir is
    /// configured) — a fleet whose shared cache survives an engine restart.
    pub fn is_durable(&self) -> bool {
        self.store.is_durable()
    }

    // ── block ops (the SharedKvBackend seam, `&self` internals) ─────────────────────

    /// Fetch the fresh block stored under `hash`, else `None` (absent OR stale).
    /// Counts the hit/miss and emits the per-op + periodic hit-rate `tracing`.
    fn fetch(&self, hash: &str) -> Option<Block> {
        let raw = match self.store.get(BLOCK_NS, hash) {
            Ok(Some(raw)) => raw,
            Ok(None) => return self.miss(hash, false),
            Err(error) => {
                // A store read error is a cache MISS (never crash token generation), but
                // surface it — it is not a normal absent-key outcome.
                tracing::warn!(
                    target: "epistemic_graph::kvcache",
                    instance = %self.instance_id,
                    hash = %hash,
                    error = %error,
                    "kvcache durable get failed, treating as miss"
                );
                return self.miss(hash, false);
            }
        };
        let Some((version, block)) = decode_frame(&raw) else {
            tracing::warn!(
                target: "epistemic_graph::kvcache",
                instance = %self.instance_id,
                hash = %hash,
                "kvcache stored block frame is malformed, treating as miss"
            );
            return self.miss(hash, false);
        };
        // Freshness gate (CONCEPT:EG-KG.storage.content-addressed-put): an Agnostic pure
        // KV page is fresh forever and never reads the epoch (the dedup hot path); a
        // versioned derived-context entry is fresh only at the store's current version.
        let fresh = match version {
            DataVersion::Agnostic => true,
            DataVersion::At(_) => version.is_fresh(self.current_version()),
        };
        if fresh {
            self.get_hits.fetch_add(1, Ordering::Relaxed);
            self.trace_access(hash, true);
            self.maybe_log_rate();
            Some(block.to_vec())
        } else {
            self.stale_missed.fetch_add(1, Ordering::Relaxed);
            self.miss(hash, true)
        }
    }

    fn miss(&self, hash: &str, _stale: bool) -> Option<Block> {
        self.get_misses.fetch_add(1, Ordering::Relaxed);
        self.trace_access(hash, false);
        self.maybe_log_rate();
        None
    }

    /// Store `block` under `hash`, framed with the [`DataVersion`] it was derived at.
    /// Returns `Ok(true)` iff a NEW entry was created; `Ok(false)` on a dedup hit against
    /// an already-present block (the cross-instance / prefix-cache win). A durable write
    /// failure is surfaced as `Err` so the HTTP surface can 500 rather than silently
    /// claim the block is cached. Public so the HTTP `KvCacheStore` (which holds this
    /// backend by shared `&self`) can route a PUT here without the trait's `&mut self`.
    pub fn store_block(
        &self,
        hash: &str,
        block: Block,
        derived_at: DataVersion,
    ) -> Result<bool, String> {
        // Whether a row occupying this key is FRESH right now — not merely whether
        // SOME row physically occupies it. An `Agnostic` page is fresh forever
        // (content-addressed: any present row is byte-identical), so this matches
        // the old raw-presence check for that case unchanged. A versioned `At(v)`
        // entry, though, can legitimately be a DIFFERENT value at a new version —
        // that is the whole point of `DataVersion::At` — so a STALE row sitting
        // under this key is superseded content, not a duplicate of what's being
        // written now: a re-publish over it must count as a fresh creation, not a
        // dedup (`version_bump_invalidates_derived_context_fleet_wide` proves the
        // fleet-wide-miss → re-publish round trip; treating that re-publish as a
        // "dedup" both under-reports `put_new` and — via the `false` return this
        // whole method surfaces to callers as "not created" — makes a real refresh
        // of a genuinely fresh value indistinguishable from restoring a stale one).
        let existing_is_fresh = self
            .store
            .get(BLOCK_NS, hash)
            .ok()
            .flatten()
            .as_deref()
            .and_then(decode_frame)
            .is_some_and(|(version, _)| match version {
                DataVersion::Agnostic => true,
                DataVersion::At(_) => version.is_fresh(self.current_version()),
            });
        // A content-addressed page already present (and therefore fresh) is
        // identical bytes — skip the redundant durable rewrite and count the
        // cross-instance dedup (the hot path).
        if existing_is_fresh && derived_at.is_agnostic() {
            self.dedup_hits.fetch_add(1, Ordering::Relaxed);
            self.trace_put(hash, false);
            return Ok(false);
        }
        let frame = encode_frame(&block, derived_at);
        self.store.put(BLOCK_NS, hash, frame)?;
        if existing_is_fresh {
            // A versioned re-publish of an ALREADY-fresh value REFRESHES the
            // entry's version stamp (un-stales it) — still a dedup.
            self.dedup_hits.fetch_add(1, Ordering::Relaxed);
            self.trace_put(hash, false);
            Ok(false)
        } else {
            self.put_new.fetch_add(1, Ordering::Relaxed);
            self.trace_put(hash, true);
            Ok(true)
        }
    }

    /// Whether a fresh (non-stale) block exists under `hash`.
    fn has(&self, hash: &str) -> bool {
        match self.store.get(BLOCK_NS, hash) {
            Ok(Some(raw)) => match decode_frame(&raw) {
                Some((DataVersion::Agnostic, _)) => true,
                Some((version @ DataVersion::At(_), _)) => version.is_fresh(self.current_version()),
                None => false,
            },
            _ => false,
        }
    }

    /// Advance the store's current data version (CONCEPT:EG-KG.storage.content-addressed-put),
    /// persisting it so the WHOLE fleet gates freshness against the same epoch. Stale
    /// entries are then read-gated lazily (see [`retire_stale`](Self::retire_stale) for
    /// bounded disk reclamation). Passing `Agnostic` clears tracking. Public so the HTTP
    /// `KvCacheStore` can route a version bump here without the trait's `&mut self`.
    pub fn set_version(&self, version: DataVersion) -> Result<(), String> {
        match version {
            DataVersion::At(v) => self
                .store
                .put(META_NS, VERSION_KEY, v.to_be_bytes().to_vec()),
            DataVersion::Agnostic => {
                self.store.delete(META_NS, VERSION_KEY)?;
                Ok(())
            }
        }
    }

    /// The store's current data version (`Agnostic` when tracking is inactive).
    pub fn current_version(&self) -> DataVersion {
        match self.store.get(META_NS, VERSION_KEY) {
            Ok(Some(bytes)) if bytes.len() == 8 => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes);
                DataVersion::At(u64::from_be_bytes(buf))
            }
            _ => DataVersion::Agnostic,
        }
    }

    /// Bounded reclamation sweep: scan up to `max` blocks and DELETE the ones now stale
    /// under the current version, returning how many were retired. The opt-in,
    /// out-of-band disk counterpart to `SharedKvIndex`'s eager in-RAM retirement. Pure
    /// content-addressed (`Agnostic`) pages are never retired.
    pub fn retire_stale(&self, max: usize) -> Result<usize, String> {
        let current = self.current_version();
        if current.is_agnostic() {
            return Ok(0); // tracking inactive ⇒ nothing is stale
        }
        let pairs = self.store.scan(BLOCK_NS, "", max)?;
        let mut retired = 0usize;
        for (key, raw) in pairs {
            if let Some((version @ DataVersion::At(_), _)) = decode_frame(&raw) {
                if !version.is_fresh(current) && self.store.delete(BLOCK_NS, &key)? {
                    retired += 1;
                }
            }
        }
        if retired > 0 {
            tracing::debug!(
                target: "epistemic_graph::kvcache",
                instance = %self.instance_id,
                retired,
                "kvcache retired stale derived-context blocks"
            );
        }
        Ok(retired)
    }

    /// Graph-topology-aware paging (CONCEPT:EG-KG.memory.graph-guided-paging): page the
    /// top-`budget` PRESENT blocks from the shared durable store into RAM, in the caller's
    /// graph-locality order. `ranked_keys` is a block-key ranking a caller derives from the
    /// resident graph's own topology (e.g. `eg_kvcache::graph_importance::pagerank_importance`
    /// over a `GraphView`, mapped node-id → block-key) — so the KV blocks of the most
    /// graph-central nodes are the ones paged into the fast tier first, exactly like
    /// `TieredCache::apply_importance_scores` shapes eviction by centrality. Returns the
    /// `(key, block)` pairs actually resident (misses are skipped), capped at `budget`.
    pub fn page_in_ranked(&self, ranked_keys: &[String], budget: usize) -> Vec<(String, Block)> {
        let mut paged = Vec::new();
        for key in ranked_keys {
            if paged.len() >= budget {
                break;
            }
            if let Some(block) = self.fetch(key) {
                paged.push((key.clone(), block));
            }
        }
        paged
    }

    /// Current occupancy + hit-rate counters (the Seam-6 export shape).
    pub fn stats(&self) -> SharedKvStoreStats {
        SharedKvStoreStats {
            get_hits: self.get_hits.load(Ordering::Relaxed),
            get_misses: self.get_misses.load(Ordering::Relaxed),
            dedup_hits: self.dedup_hits.load(Ordering::Relaxed),
            put_new: self.put_new.load(Ordering::Relaxed),
            stale_missed: self.stale_missed.load(Ordering::Relaxed),
        }
    }

    /// The instance id stamped on this backend's metrics/logs.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    // ── tracing (GOOD LOGS: per-op + periodic hit-rate, never println!) ─────────────

    fn trace_access(&self, hash: &str, hit: bool) {
        tracing::trace!(
            target: "epistemic_graph::kvcache",
            instance = %self.instance_id,
            hash = %hash,
            hit,
            "kvcache get"
        );
    }

    fn trace_put(&self, hash: &str, created: bool) {
        tracing::trace!(
            target: "epistemic_graph::kvcache",
            instance = %self.instance_id,
            hash = %hash,
            created,
            "kvcache put"
        );
    }

    /// Periodic hit-rate metric, once every [`HIT_RATE_LOG_EVERY`] accesses — the SAME
    /// shape as `result_cache::maybe_log_rate` (W3.7 Seam-6): `hits`/`misses`/`hit_rate`
    /// under `target: "epistemic_graph::kvcache"`.
    fn maybe_log_rate(&self) {
        let n = self.accesses.fetch_add(1, Ordering::Relaxed) + 1;
        if !n.is_multiple_of(HIT_RATE_LOG_EVERY) {
            return;
        }
        let stats = self.stats();
        tracing::debug!(
            target: "epistemic_graph::kvcache",
            instance = %self.instance_id,
            hits = stats.get_hits,
            misses = stats.get_misses,
            dedup_hits = stats.dedup_hits,
            stale_missed = stats.stale_missed,
            hit_rate = stats.hit_rate(),
            "kvcache hit-rate metric"
        );
    }
}

/// Frame a block with its [`DataVersion`]: `[tag u8][version u64 BE][bytes…]`.
fn encode_frame(block: &[u8], version: DataVersion) -> Vec<u8> {
    let mut frame = Vec::with_capacity(FRAME_HEADER + block.len());
    match version {
        DataVersion::Agnostic => {
            frame.push(0);
            frame.extend_from_slice(&0u64.to_be_bytes());
        }
        DataVersion::At(v) => {
            frame.push(1);
            frame.extend_from_slice(&v.to_be_bytes());
        }
    }
    frame.extend_from_slice(block);
    frame
}

/// Decode a framed block into `(version, block-bytes)`; `None` on a malformed frame.
fn decode_frame(frame: &[u8]) -> Option<(DataVersion, &[u8])> {
    if frame.len() < FRAME_HEADER {
        return None;
    }
    let version = match frame[0] {
        0 => DataVersion::Agnostic,
        1 => {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&frame[1..FRAME_HEADER]);
            DataVersion::At(u64::from_be_bytes(buf))
        }
        _ => return None,
    };
    Some((version, &frame[FRAME_HEADER..]))
}

impl SharedKvBackend for SharedKvStoreBackend {
    fn get_block(&self, hash: &str) -> Option<Block> {
        self.fetch(hash)
    }

    fn put_block(&mut self, hash: &str, block: Block, derived_at: DataVersion) -> bool {
        // Graceful degradation for the generic seam (LMCache treats a failed offload as a
        // non-fatal miss): a durable write error becomes `false` (not-created) + a warn.
        match self.store_block(hash, block, derived_at) {
            Ok(created) => created,
            Err(error) => {
                tracing::warn!(
                    target: "epistemic_graph::kvcache",
                    instance = %self.instance_id,
                    hash = %hash,
                    error = %error,
                    "kvcache durable put failed"
                );
                false
            }
        }
    }

    fn contains(&self, hash: &str) -> bool {
        self.has(hash)
    }

    fn set_data_version(&mut self, version: DataVersion) {
        if let Err(error) = self.set_version(version) {
            tracing::warn!(
                target: "epistemic_graph::kvcache",
                instance = %self.instance_id,
                error = %error,
                "kvcache set_data_version failed"
            );
        }
    }
}

/// Order block keys by a graph-locality importance ranking (CONCEPT:EG-KG.memory.graph-guided-paging),
/// highest-importance first, dropping any key that has no mapping. The convenience the
/// snapshot/page-in path uses to page graph-central blocks in first: hand it a
/// `node-id → score` ranking (`pagerank_importance`/`degree_centrality_importance`) plus a
/// `node-id → block-key` map. Kept here (not in the leaf crate) because the ranking is
/// produced by the facade's already-linked `eg-compute` graph algorithms.
pub fn rank_keys_by_importance<F>(scores: &[(String, f64)], key_of: F) -> Vec<String>
where
    F: Fn(&str) -> Option<String>,
{
    let mut ranked: Vec<(String, f64)> = scores
        .iter()
        .filter_map(|(node, score)| key_of(node).map(|key| (key, *score)))
        .collect();
    // Descending importance; NaN-safe (treat unordered as equal).
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let mut seen = HashSet::new();
    ranked
        .into_iter()
        .map(|(key, _)| key)
        .filter(|key| seen.insert(key.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(tag: &str) -> Arc<KvStore> {
        let dir = std::env::temp_dir().join(format!(
            "eg-kvcache-shared-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        Arc::new(KvStore::open(Some(dir.to_str().unwrap())).unwrap())
    }

    /// CONCEPT:EG-KG.backend.networked-shared-kv — THE acceptance proof: two serving
    /// instances (A and B) over ONE durable engine store. A block A PUTs is a cache HIT
    /// for B — cross-instance prefix reuse through the engine — and B's get is measured.
    #[test]
    fn cross_instance_shared_prefix_hit_through_the_engine() {
        let shared = store("xinst");
        let mut instance_a = SharedKvStoreBackend::new(shared.clone(), "A");
        let instance_b = SharedKvStoreBackend::new(shared.clone(), "B");

        let prefix_hash = "tok:shared-system-prompt-prefix";
        let kv_page: Block = vec![7u8; 4096];

        // B has never seen the prefix ⇒ a MISS (would recompute).
        assert_eq!(instance_b.get_block(prefix_hash), None);
        assert!(!instance_b.contains(prefix_hash));

        // A (the warm worker) computes + PUTs the prefix block.
        assert!(
            instance_a.put_block(prefix_hash, kv_page.clone(), DataVersion::Agnostic),
            "A stores a brand-new block"
        );

        // B (a cold worker) now HITs the SAME prefix through the engine — no recompute.
        assert!(
            instance_b.contains(prefix_hash),
            "B sees A's block via the shared store"
        );
        assert_eq!(
            instance_b.get_block(prefix_hash),
            Some(kv_page),
            "the block A wrote is a cross-instance HIT for B"
        );

        // Measured: B recorded exactly one miss (the cold probe) then one hit.
        let bstats = instance_b.stats();
        assert_eq!(bstats.get_hits, 1, "B's cross-instance hit is counted");
        assert_eq!(bstats.get_misses, 1, "B's initial cold miss is counted");
        assert!((bstats.hit_rate() - 0.5).abs() < 1e-9);
        // A stored one new block, no dedup yet.
        assert_eq!(instance_a.stats().put_new, 1);
    }

    /// CONCEPT:EG-KG.backend.networked-shared-kv — a second instance PUTting the SAME
    /// content-addressed block dedups (the shared store already has it) rather than
    /// re-storing, and the dedup is counted (the cross-instance store-once win).
    #[test]
    fn second_instance_put_dedups_against_the_shared_store() {
        let shared = store("dedup");
        let mut a = SharedKvStoreBackend::new(shared.clone(), "A");
        let mut b = SharedKvStoreBackend::new(shared.clone(), "B");
        let hash = "tok:hot-prefix";
        let page: Block = vec![3u8; 512];

        assert!(
            a.put_block(hash, page.clone(), DataVersion::Agnostic),
            "A: new"
        );
        assert!(
            !b.put_block(hash, page.clone(), DataVersion::Agnostic),
            "B: same content already in the shared store ⇒ dedup, not new"
        );
        assert_eq!(b.stats().dedup_hits, 1);
        assert_eq!(b.stats().put_new, 0);
    }

    /// CONCEPT:EG-KG.storage.content-addressed-put — a versioned derived-context entry is
    /// served, then a `set_data_version` bump (a graph write, PERSISTED so all instances
    /// agree) makes it a stale MISS on ANOTHER instance; a re-publish at the new version
    /// serves fresh. Agnostic pages are immune.
    #[test]
    fn version_bump_invalidates_derived_context_fleet_wide() {
        let shared = store("ver");
        let mut writer = SharedKvStoreBackend::new(shared.clone(), "writer");
        let reader = SharedKvStoreBackend::new(shared.clone(), "reader");

        writer.set_data_version(DataVersion::At(0));
        assert!(writer.put_block("agent-ctx", b"ctx-at-v0".to_vec(), DataVersion::At(0)));
        // A pure KV page alongside it (must survive the bump).
        assert!(writer.put_block("pure-page", vec![1u8; 64], DataVersion::Agnostic));

        // The READER instance (separate handle) sees the fresh context via the shared store.
        assert_eq!(
            reader.get_block("agent-ctx").as_deref(),
            Some(&b"ctx-at-v0"[..])
        );

        // A graph write bumps the epoch (persisted) ⇒ the reader now MISSES the stale ctx…
        writer.set_data_version(DataVersion::At(1));
        assert_eq!(
            reader.get_block("agent-ctx"),
            None,
            "stale context is a fleet-wide miss"
        );
        assert!(!reader.contains("agent-ctx"));
        assert_eq!(reader.stats().stale_missed, 1, "the stale miss is counted");
        // …but the pure content-addressed page is untouched.
        assert_eq!(reader.get_block("pure-page"), Some(vec![1u8; 64]));

        // Re-publish freshly-derived context at v1 ⇒ served again.
        assert!(writer.put_block("agent-ctx", b"ctx-at-v1".to_vec(), DataVersion::At(1)));
        assert_eq!(
            reader.get_block("agent-ctx").as_deref(),
            Some(&b"ctx-at-v1"[..])
        );

        // retire_stale is a no-op now (nothing stale at v1), and it never touches pure pages.
        assert_eq!(writer.retire_stale(1000).unwrap(), 0);
        assert!(writer.contains("pure-page"));
    }

    /// CONCEPT:EG-KG.storage.content-addressed-put — `retire_stale` reclaims stale
    /// derived-context blocks from disk (the bounded, out-of-band sweep) while leaving
    /// fresh + agnostic entries.
    #[test]
    fn retire_stale_reclaims_only_stale_derived_blocks() {
        let shared = store("retire");
        let mut b = SharedKvStoreBackend::new(shared.clone(), "s");
        b.set_data_version(DataVersion::At(5));
        assert!(b.put_block("stale-a", b"x".to_vec(), DataVersion::At(4))); // arrives stale
        assert!(b.put_block("stale-b", b"y".to_vec(), DataVersion::At(3))); // arrives stale
        assert!(b.put_block("fresh", b"z".to_vec(), DataVersion::At(5)));
        assert!(b.put_block("pure", vec![9u8; 8], DataVersion::Agnostic));

        let retired = b.retire_stale(1000).unwrap();
        assert_eq!(retired, 2, "both stale derived blocks retired");
        assert!(b.contains("fresh"));
        assert!(b.contains("pure"));
        assert_eq!(b.get_block("stale-a"), None);
    }

    /// CONCEPT:EG-KG.memory.graph-guided-paging — graph topology decides paging over the
    /// SHARED backend: PageRank over a star graph ranks the hub above the leaves, so
    /// `page_in_ranked` with a budget of 1 pages in the hub's KV block (the graph-central
    /// one), NOT a peripheral leaf's.
    #[test]
    fn graph_topology_pages_central_blocks_in_first() {
        use eg_compute::algorithms::pagerank;
        use eg_compute::graph::GraphCore;

        // A star graph: every leaf cites the hub.
        let g = GraphCore::new();
        g.add_node("hub".to_string(), b"{}".to_vec());
        for i in 0..6 {
            let leaf = format!("leaf{i}");
            g.add_node(leaf.clone(), b"{}".to_vec());
            g.add_edge(leaf, "hub".to_string(), b"{}".to_vec()).unwrap();
        }
        let scores = pagerank(&g.topology_snapshot(), 0.85, 20);

        // Every node's KV block lives in the shared store, keyed "kv:<node>".
        let shared = store("locality");
        let mut backend = SharedKvStoreBackend::new(shared.clone(), "s");
        for (node, _) in &scores {
            assert!(backend.put_block(
                &format!("kv:{node}"),
                vec![0u8; 128],
                DataVersion::Agnostic
            ));
        }

        // Rank block-keys by graph importance, then page in only the single most central.
        let ranked = rank_keys_by_importance(&scores, |node| Some(format!("kv:{node}")));
        assert_eq!(
            ranked.first().map(String::as_str),
            Some("kv:hub"),
            "hub ranks first"
        );
        let paged = backend.page_in_ranked(&ranked, 1);
        assert_eq!(paged.len(), 1);
        assert_eq!(
            paged[0].0, "kv:hub",
            "the graph-central block is paged in first"
        );
    }

    /// The framed block round-trips its version stamp + bytes, and a malformed frame reads
    /// as a miss (defensive decode).
    #[test]
    fn frame_encode_decode_round_trips() {
        for version in [
            DataVersion::Agnostic,
            DataVersion::At(0),
            DataVersion::At(42),
        ] {
            let block = vec![1u8, 2, 3, 255, 0, 10];
            let frame = encode_frame(&block, version);
            let (decoded_version, decoded_block) = decode_frame(&frame).unwrap();
            assert_eq!(decoded_version, version);
            assert_eq!(decoded_block, &block[..]);
        }
        assert!(
            decode_frame(&[0u8; 4]).is_none(),
            "too-short frame is malformed"
        );
        assert!(
            decode_frame(&[9u8; 16]).is_none(),
            "bad discriminant is malformed"
        );
    }

    /// Durability: a block one instance PUTs survives a store close + reopen, so the fleet
    /// cache survives an engine restart (the L2 durability property).
    #[test]
    fn shared_block_survives_store_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "eg-kvcache-reopen-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.to_str().unwrap();
        {
            let s = Arc::new(KvStore::open(Some(path)).unwrap());
            let mut a = SharedKvStoreBackend::new(s, "A");
            assert!(a.put_block("survivor", vec![5u8; 256], DataVersion::Agnostic));
        }
        // A "restarted" engine reopens the SAME kv.redb — the shared block is still there.
        let s = Arc::new(KvStore::open(Some(path)).unwrap());
        let b = SharedKvStoreBackend::new(s, "B");
        assert_eq!(b.get_block("survivor"), Some(vec![5u8; 256]));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
