//! Persistent viz engine state (D-VZ-1 lane V4, "engine integration").
//!
//! Replaces `handlers::viz`'s former fresh-per-request `ColumnStore` (built and
//! discarded on every single `Method::Viz` call — see that module's former
//! "Scope — V4-LITE, not full V4" doc section) with:
//!
//! 1. A **persistent** [`ColumnStore`] living for the process lifetime, so a
//!    second render against a dataset already ingested does not need the
//!    caller to resend it (`VizRenderRequest::dataset` becomes optional, see
//!    `handlers::viz`).
//! 2. A bounded [`RenderCache`] keyed by [`render_cache_key`] — see that
//!    function's doc for the exact key composition and why it is content-
//!    addressed rather than version-counter-keyed.
//! 3. Durable render provenance ([`super::viz_provenance`]).
//!
//! ## The cache key, and the mistake it avoids
//!
//! D-OP-1's RLS projection cache (`src/server/access.rs`, a DIFFERENT lane's
//! work — not touched here) keys on `GraphCore::version()`: correct for that
//! use, because it is a CORRECTNESS control that must invalidate on any write
//! to the graph it protects. Keying a PERFORMANCE cache the same way — on a
//! whole-graph or whole-engine version — would mean any write anywhere
//! invalidates every cached render, driving the hit rate toward zero under
//! real write traffic: worse than no cache at all, since it still pays the
//! bookkeeping cost. This cache instead keys on content the render actually
//! depends on:
//!
//! - [`eg_viz_columnstore::ColumnStore::content_fingerprint`] — a hash over
//!   the dataset's chunk `content_id`s (already computed at ingest, content-
//!   addressed, no rescan). Writing an UNRELATED dataset never changes this
//!   one's fingerprint; re-ingesting byte-identical data still fingerprints
//!   identically (a genuine cache HIT a version counter could never give).
//! - width_px/height_px/format/budget, folded in by [`render_cache_key`]
//!   because [`eg_viz_core::job::query_hash`] itself does not cover them (a
//!   caller can request the same spec+dataset at a different canvas size and
//!   legitimately get different pixel-space geometry back).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::RwLock;
use sha2::{Digest, Sha256};

use eg_types::viz::VizFormat;
use eg_viz_columnstore::ColumnStore;
use eg_viz_core::{FrameBudget, ViewResult};

use super::viz_provenance::VizProvenanceStore;

/// Bounded so a deployment serving many distinct (spec, dataset, canvas,
/// format) combinations cannot grow this unboundedly — a capacity miss costs
/// exactly what every render cost before this cache existed, never worse.
/// Mirrors `crates/eg-core/src/rls_projection_cache.rs`'s `CAPACITY` constant
/// in spirit (same bounded-LRU shape), sized larger because a render cache
/// entry is typically much smaller than a full RLS-projected graph.
const RENDER_CACHE_CAPACITY: usize = 512;

/// One cached, fully-rendered response — a cache HIT returns this with ZERO
/// recomputation (no re-resolve, no re-tier-select, no re-encode).
#[derive(Clone)]
pub(crate) struct CachedRender {
    pub(crate) view_result: ViewResult,
    pub(crate) content_type: &'static str,
    pub(crate) bytes: Arc<[u8]>,
}

#[derive(Default)]
struct RenderCacheInner {
    entries: HashMap<String, CachedRender>,
    // Recency order, oldest at the front — same explicit-tracking rationale as
    // `rls_projection_cache::Inner::order` (HashMap iteration order is not
    // defined).
    order: VecDeque<String>,
}

impl RenderCacheInner {
    fn touch(&mut self, key: &str) {
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
        }
        self.order.push_back(key.to_string());
    }
}

struct RenderCache(RwLock<RenderCacheInner>);

impl RenderCache {
    fn new() -> Self {
        RenderCache(RwLock::new(RenderCacheInner::default()))
    }

    fn get(&self, key: &str) -> Option<CachedRender> {
        let mut inner = self.0.write();
        let hit = inner.entries.get(key).cloned();
        if hit.is_some() {
            inner.touch(key);
        }
        hit
    }

    fn put(&self, key: String, value: CachedRender) {
        let mut inner = self.0.write();
        if !inner.entries.contains_key(&key) && inner.entries.len() >= RENDER_CACHE_CAPACITY {
            if let Some(evict) = inner.order.pop_front() {
                inner.entries.remove(&evict);
            }
        }
        inner.touch(&key);
        inner.entries.insert(key, value);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.0.read().entries.len()
    }
}

/// The full engine-level cache key (lane V4) — folds `width_px`/`height_px`/
/// `format`/`budget` into an already-content-addressed `query_hash` (see the
/// module doc's "cache key" section for why those extra fields are necessary
/// and why `query_hash` alone is not sufficient).
pub(crate) fn render_cache_key(
    query_hash: &str,
    width_px: u32,
    height_px: u32,
    format: VizFormat,
    budget: FrameBudget,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"epistemic-graph.viz_render_cache_key.v1\0");
    hasher.update(query_hash.as_bytes());
    hasher.update([0u8]);
    hasher.update(width_px.to_le_bytes());
    hasher.update(height_px.to_le_bytes());
    // `VizFormat` is a small closed enum (Png/Svg/Pdf) with a derived `Debug`
    // that is stable within this codebase's own control -- not parsed by any
    // external consumer, only hashed.
    hasher.update(format!("{format:?}").as_bytes());
    hasher.update([0u8]);
    hasher.update(budget.max_primitives.to_le_bytes());
    hasher.update(budget.max_bytes.to_le_bytes());
    format!("eg:viz_render:{}", hex::encode(&hasher.finalize()[..16]))
}

/// Process-lifetime engine state for the native visualization surface. One
/// instance lives on [`super::state::ServerState`] behind an `Arc` (see
/// `handlers::viz`), gated identically to that handler (`viz-static-export`).
pub struct VizEngineState {
    /// The persistent ColumnStore every `Method::Viz` render now shares,
    /// instead of building and discarding a fresh one per request. Datasets
    /// are content-addressed at the chunk level (`eg-viz-columnstore`), so
    /// re-ingesting identical bytes is cheap and does not defeat the render
    /// cache above (see [`eg_viz_columnstore::ColumnStore::content_fingerprint`]).
    pub(crate) store: RwLock<ColumnStore>,
    cache: RenderCache,
    pub(crate) provenance: VizProvenanceStore,
}

impl VizEngineState {
    /// `persist_dir` mirrors the engine's own `GRAPH_SERVICE_PERSIST_DIR`
    /// convention: `Some` (and a build with the `redb` feature) durably backs
    /// provenance; `None`, or a build without `redb`, keeps provenance
    /// in-memory only (the render cache itself is ALWAYS in-memory-only —
    /// it is a performance cache, not a durability boundary, so there is no
    /// "durable render cache" mode to configure).
    pub fn new(persist_dir: Option<&str>) -> Self {
        #[cfg(feature = "redb")]
        let provenance = match persist_dir {
            Some(dir) => VizProvenanceStore::open(dir).unwrap_or_else(|e| {
                tracing::warn!(
                    "failed to open durable viz provenance store at {dir}: {e}; \
                     falling back to in-memory (renders still work, provenance \
                     will not survive a restart)"
                );
                VizProvenanceStore::in_memory()
            }),
            None => VizProvenanceStore::in_memory(),
        };
        #[cfg(not(feature = "redb"))]
        let provenance = {
            let _ = persist_dir;
            VizProvenanceStore::in_memory()
        };

        VizEngineState {
            store: RwLock::new(ColumnStore::new()),
            cache: RenderCache::new(),
            provenance,
        }
    }

    pub(crate) fn cache_get(&self, key: &str) -> Option<CachedRender> {
        self.cache.get(key)
    }

    pub(crate) fn cache_put(&self, key: String, value: CachedRender) {
        self.cache.put(key, value)
    }

    #[cfg(test)]
    pub(crate) fn cache_len(&self) -> usize {
        self.cache.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_viz_core::FrameBudget;

    #[test]
    fn render_cache_key_is_deterministic() {
        let budget = FrameBudget::new(1000, 1000);
        let a = render_cache_key("qh1", 800, 600, VizFormat::Png, budget);
        let b = render_cache_key("qh1", 800, 600, VizFormat::Png, budget);
        assert_eq!(a, b);
    }

    #[test]
    fn render_cache_key_differs_by_canvas_size() {
        let budget = FrameBudget::new(1000, 1000);
        let a = render_cache_key("qh1", 800, 600, VizFormat::Png, budget);
        let b = render_cache_key("qh1", 400, 300, VizFormat::Png, budget);
        assert_ne!(a, b, "different canvas dimensions must not collide");
    }

    #[test]
    fn render_cache_key_differs_by_format() {
        let budget = FrameBudget::new(1000, 1000);
        let png = render_cache_key("qh1", 800, 600, VizFormat::Png, budget);
        let svg = render_cache_key("qh1", 800, 600, VizFormat::Svg, budget);
        assert_ne!(png, svg);
    }

    #[test]
    fn render_cache_key_differs_by_budget() {
        let a = render_cache_key(
            "qh1",
            800,
            600,
            VizFormat::Png,
            FrameBudget::new(1000, 1000),
        );
        let b = render_cache_key(
            "qh1",
            800,
            600,
            VizFormat::Png,
            FrameBudget::new(2_000_000, 1000),
        );
        assert_ne!(a, b);
    }

    fn sample_cached(tag: &str) -> CachedRender {
        CachedRender {
            view_result: ViewResult::exact(format!("qh-{tag}"), 1, Vec::new(), 0, 0).unwrap(),
            content_type: "image/png",
            bytes: Arc::from(vec![1u8, 2, 3]),
        }
    }

    #[test]
    fn cache_hit_returns_the_stored_value_with_no_recomputation_marker() {
        let engine = VizEngineState::new(None);
        assert!(engine.cache_get("k1").is_none());
        engine.cache_put("k1".to_string(), sample_cached("a"));
        let hit = engine.cache_get("k1").unwrap();
        assert_eq!(hit.view_result.query_hash, "qh-a");
    }

    #[test]
    fn cache_evicts_least_recently_used_entry_past_capacity() {
        let engine = VizEngineState::new(None);
        for i in 0..(RENDER_CACHE_CAPACITY + 10) {
            engine.cache_put(format!("k{i}"), sample_cached(&i.to_string()));
        }
        assert_eq!(engine.cache_len(), RENDER_CACHE_CAPACITY);
        // The earliest-inserted keys must have been evicted.
        assert!(engine.cache_get("k0").is_none());
        // The most recently inserted key must still be present.
        assert!(engine
            .cache_get(&format!("k{}", RENDER_CACHE_CAPACITY + 9))
            .is_some());
    }
}
