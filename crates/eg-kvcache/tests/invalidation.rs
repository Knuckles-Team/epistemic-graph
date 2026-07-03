//! Data-version invalidation seam tests (CONCEPT:EG-359).
//!
//! The seam-scenario-3 deliverable: an update to the underlying graph data must
//! invalidate cached agent/LLM context derived from it — a stale KV/context entry is
//! NEVER served. These end-to-end tests drive the public crate API exactly as the engine
//! (via `GraphCore::version()`, KG-2.180) and the EG-187 HTTP surface do:
//!
//! * populate a versioned entry → bump the data version (simulate a graph write) → assert
//!   the entry is now a MISS and a re-populate returns FRESH content (no stale serve);
//! * cache-ON vs cache-OFF A/B — the logical result is IDENTICAL either way (the cache
//!   only changes latency, never correctness);
//! * unchanged-data path — version stable ⇒ cache HIT, no false invalidation.
//!
//! Both cache surfaces are covered: the multi-instance shared backend
//! ([`SharedKvIndex`], EG-186 — the store the HTTP surface exposes to vLLM/LMCache) and
//! the single-instance tiered cache ([`TieredCache`], EG-185).

use eg_kvcache::{Block, DataVersion, SharedKvBackend, SharedKvIndex, TieredCache};

/// A deterministic "assembled context / KV page for `key` derived from graph data at
/// `data_version`" — the bytes an agent-context builder would produce. Distinct
/// `data_version`s yield distinct bytes, so a stale serve is detectable byte-for-byte.
fn derive_context(key: &str, data_version: u64) -> Block {
    format!("ctx[{key}]@v{data_version}").into_bytes()
}

// ── SharedKvIndex (EG-186 / the EG-187 HTTP surface store) ─────────────────────────

/// CONCEPT:EG-359 — populate → bump data version → MISS → re-populate returns fresh.
/// The core "no stale LLM context after a write" contract on the shared backend.
#[test]
fn shared_bump_invalidates_then_repopulate_is_fresh() {
    let mut idx = SharedKvIndex::new();
    idx.set_data_version(DataVersion::At(10));

    // Agent context assembled from graph data at version 10, cached under a token-hash.
    let v10 = derive_context("agent-a", 10);
    idx.put_block("agent-a", v10.clone(), DataVersion::At(10));
    assert_eq!(idx.get_block("agent-a"), Some(v10), "fresh entry served");

    // Underlying graph data changes → the engine bumps GraphCore::version() → drives
    // set_data_version. The old context is now STALE.
    idx.set_data_version(DataVersion::At(11));
    assert_eq!(
        idx.get_block("agent-a"),
        None,
        "stale v10 context must NOT be served after the write"
    );

    // Re-derive against the new data + re-cache → the fresh context is served.
    let v11 = derive_context("agent-a", 11);
    assert_ne!(
        v11,
        derive_context("agent-a", 10),
        "fresh context differs from stale"
    );
    idx.put_block("agent-a", v11.clone(), DataVersion::At(11));
    assert_eq!(idx.get_block("agent-a"), Some(v11), "re-populate is fresh");
}

/// CONCEPT:EG-359 — unchanged-data path: version stable ⇒ every read is a HIT, nothing is
/// falsely invalidated.
#[test]
fn shared_stable_version_never_false_invalidates() {
    let mut idx = SharedKvIndex::new();
    idx.set_data_version(DataVersion::At(7));
    let ctx = derive_context("agent-b", 7);
    idx.put_block("agent-b", ctx.clone(), DataVersion::At(7));

    // Many lookups while the data version holds — all HITs, byte-identical.
    for _ in 0..8 {
        assert_eq!(idx.get_block("agent-b"), Some(ctx.clone()));
    }
    assert_eq!(idx.stats().stale_retired, 0, "no false invalidation");
    assert_eq!(idx.stats().unique_blocks, 1, "entry still resident");
}

/// CONCEPT:EG-359 — cache-ON vs cache-OFF A/B: the logical answer an agent reads is
/// IDENTICAL whether or not the cache is consulted. A cache MISS (cold, or invalidated by
/// a write) just means "recompute from the graph"; the recomputed value equals what the
/// cache would have held. Correctness never depends on the cache.
#[test]
fn shared_cache_on_vs_off_identical_logical_result() {
    // The "source of truth": recompute context from the (current) graph data version.
    fn ground_truth(key: &str, data_version: u64) -> Block {
        derive_context(key, data_version)
    }

    // A read THROUGH the cache: serve if fresh, else recompute + backfill (write-through).
    fn cached_read(idx: &mut SharedKvIndex, key: &str, data_version: u64) -> Block {
        if let Some(hit) = idx.get_block(key) {
            return hit;
        }
        let fresh = ground_truth(key, data_version);
        idx.put_block(key, fresh.clone(), DataVersion::At(data_version));
        fresh
    }

    let mut idx = SharedKvIndex::new();

    // Walk a sequence of (data_version) steps that includes writes (version bumps).
    for step in 0..6u64 {
        let data_version = step; // a graph write each step advances the version
        idx.set_data_version(DataVersion::At(data_version));

        // Cache-OFF answer = ground truth. Cache-ON answer = read through the cache.
        let off = ground_truth("agent-a", data_version);
        let on_first = cached_read(&mut idx, "agent-a", data_version); // cold/invalidated
        let on_second = cached_read(&mut idx, "agent-a", data_version); // warm HIT

        assert_eq!(
            on_first, off,
            "cache-ON (miss→recompute) == cache-OFF @v{data_version}"
        );
        assert_eq!(
            on_second, off,
            "cache-ON (HIT) == cache-OFF @v{data_version}"
        );
        assert_eq!(
            on_first, on_second,
            "HIT equals the recomputed value @v{data_version}"
        );
        // And critically it is NEVER a stale earlier-version value.
        if data_version > 0 {
            assert_ne!(on_second, ground_truth("agent-a", data_version - 1));
        }
    }
}

/// CONCEPT:EG-359 — pure content-addressed (Agnostic) KV pages are immune to version
/// bumps, so the EG-186 cross-instance dedup win survives graph writes (the versioning is
/// layered on top, it does not break the content-addressed path).
#[test]
fn shared_agnostic_pages_immune_to_writes() {
    let mut idx = SharedKvIndex::new();
    let h = idx.put_by_content(vec![5u8; 64]); // pure KV page, Agnostic
    for v in 0..5u64 {
        idx.set_data_version(DataVersion::At(v));
    }
    assert_eq!(
        idx.get_block(&h),
        Some(vec![5u8; 64]),
        "pure page never stale"
    );
    assert_eq!(idx.stats().stale_retired, 0);
}

// ── TieredCache (EG-185 single-instance offload cache) ─────────────────────────────

/// CONCEPT:EG-359 — the same populate → bump → MISS → fresh-repopulate contract on the
/// tiered cache, whatever tier the entry occupies.
#[test]
fn tiered_bump_invalidates_then_repopulate_is_fresh() {
    let mut c: TieredCache<String, Block> = TieredCache::new(10_000, 10_000, 10_000);
    c.set_data_version(DataVersion::At(0));
    let v0 = derive_context("k", 0);
    c.put_versioned("k".into(), v0.clone(), DataVersion::At(0));
    assert_eq!(c.get(&"k".into()), Some(v0));

    c.set_data_version(DataVersion::At(1)); // a graph write
    assert_eq!(c.get(&"k".into()), None, "stale entry misses");
    assert_eq!(c.stats().invalidations, 1);

    let v1 = derive_context("k", 1);
    c.put_versioned("k".into(), v1.clone(), DataVersion::At(1));
    assert_eq!(c.get(&"k".into()), Some(v1), "re-populate is fresh");
}

/// CONCEPT:EG-359 — cache-ON vs cache-OFF A/B on the tiered cache under REAL tier pressure
/// (tiny budgets force demotion/drops): the logical answer is identical either way, and it
/// is never a stale earlier-version value.
#[test]
fn tiered_cache_on_vs_off_identical_under_pressure() {
    fn ground_truth(key: u64, data_version: u64) -> Block {
        derive_context(&format!("k{key}"), data_version)
    }
    fn cached_read(c: &mut TieredCache<u64, Block>, key: u64, data_version: u64) -> Block {
        if let Some(hit) = c.get(&key) {
            return hit;
        }
        let fresh = ground_truth(key, data_version);
        c.put_versioned(key, fresh.clone(), DataVersion::At(data_version));
        fresh
    }

    // Tiny tiers so entries genuinely cascade + drop while we read.
    let mut c: TieredCache<u64, Block> = TieredCache::new(64, 48, 128);

    for data_version in 0..4u64 {
        c.set_data_version(DataVersion::At(data_version));
        for key in 0..12u64 {
            let off = ground_truth(key, data_version);
            let on = cached_read(&mut c, key, data_version);
            assert_eq!(
                on, off,
                "cache-ON == cache-OFF (key {key} @v{data_version})"
            );
            if data_version > 0 {
                assert_ne!(
                    on,
                    ground_truth(key, data_version - 1),
                    "never a stale prior-version value"
                );
            }
        }
    }
}

/// CONCEPT:EG-359 — unchanged-data path on the tiered cache: a stable version yields
/// repeated HITs with no invalidation (cache genuinely accelerates, doesn't just miss).
#[test]
fn tiered_stable_version_hits_without_invalidation() {
    let mut c: TieredCache<u64, Block> = TieredCache::new(10_000, 10_000, 10_000);
    c.set_data_version(DataVersion::At(42));
    let ctx = derive_context("k", 42);
    c.put_versioned(1, ctx.clone(), DataVersion::At(42));
    for _ in 0..6 {
        assert_eq!(c.get(&1), Some(ctx.clone()));
    }
    let s = c.stats();
    assert_eq!(s.invalidations, 0, "stable version ⇒ no invalidation");
    assert!(s.hits >= 6, "reads were genuine cache hits");
}
