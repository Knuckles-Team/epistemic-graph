// CONCEPT:KG-2.207 — Semantic Embedding Store backed by the native eg-ann
// IVF-PQ + OPQ + SQ8-refine index (feature `ann`).
//
// Drop-in replacement for the hnsw_rs `SemanticStore`: identical public API
// (`new`/`add_embedding`/`semantic_search`/`force_compact`/`len`/`is_empty`) and
// identical on-disk serde shape (only the `embeddings` map is persisted, so a
// snapshot written by either backend is readable by either). For tiny stores it
// uses brute-force cosine; past `ANN_BUILD_THRESHOLD` it builds and maintains an
// eg-ann index. A persisted eg-ann index reopens WITHOUT rebuilding from raw
// vectors — the no-rebuild win — but the snapshot path here rebuilds lazily from
// the resident embeddings on first search after load (matching the existing
// SemanticStore checkpoint contract); call `save_index`/`load_index` for the
// no-rebuild persistent index path.

use crate::compute::semantic_ann::{AnnIndex, ANN_BUILD_THRESHOLD};
use parking_lot::RwLock;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU8, Ordering};

/// Threshold below which we use brute-force (index overhead not worth it).
const BRUTE_FORCE_THRESHOLD: usize = 32;
/// Rebuild/compact the index once tombstoned rows exceed this fraction of total.
const COMPACT_TOMBSTONE_PCT: f32 = 0.30;

// CONCEPT:EG-013 — index readiness state. The cold-start bug was that the FIRST
// `semantic_search` after a restart triggered a full IVF-PQ+OPQ build INLINE on the
// request path (single-threaded SVD over a 1024² matrix + k-means over 168k vectors
// → minutes, pegging one core, never finishing within the request timeout). The
// index now builds OFF the query path (`warm`, run by a background warm-on-start
// task) and persists across restarts; the query path checks this flag and serves a
// fast exact brute-force result while the index is still `Cold`, instead of building.
const STATE_COLD: u8 = 0;
const STATE_READY: u8 = 1;

pub struct SemanticStore {
    embeddings: HashMap<String, Vec<f32>>,
    /// eg-ann IVF-PQ index. `None` until the store is WARMED (off the query path)
    /// or a persisted index is reopened. The index is `#[serde(skip)]`, so a fresh
    /// snapshot load starts `Cold`; it is NEVER built inline on a search.
    index: RwLock<Option<AnnIndex>>,
    /// LIVE embedding count the index reflects (staleness check after load).
    built_len: RwLock<usize>,
    /// CONCEPT:EG-013 — `STATE_COLD`/`STATE_READY`. `Ready` ⟺ `index` is `Some` and
    /// reflects the current embeddings. Read on every search to decide ANN-vs-brute.
    state: AtomicU8,
}

impl Clone for SemanticStore {
    fn clone(&self) -> Self {
        Self {
            embeddings: self.embeddings.clone(),
            index: RwLock::new(None),
            built_len: RwLock::new(0),
            state: AtomicU8::new(STATE_COLD),
        }
    }
}

impl std::fmt::Debug for SemanticStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SemanticStore")
            .field("embeddings", &self.embeddings.len())
            .field("backend", &"eg-ann")
            .finish()
    }
}

// Persist ONLY the embeddings map — identical wire shape to the HNSW backend, so
// snapshots are interchangeable.
impl Serialize for SemanticStore {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("SemanticStore", 1)?;
        st.serialize_field("embeddings", &self.embeddings)?;
        st.end()
    }
}

impl<'de> Deserialize<'de> for SemanticStore {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            embeddings: HashMap<String, Vec<f32>>,
        }
        let raw = Raw::deserialize(d)?;
        Ok(Self {
            embeddings: raw.embeddings,
            index: RwLock::new(None),
            built_len: RwLock::new(0),
            state: AtomicU8::new(STATE_COLD),
        })
    }
}

impl Default for SemanticStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SemanticStore {
    pub fn new() -> Self {
        Self {
            embeddings: HashMap::new(),
            index: RwLock::new(None),
            built_len: RwLock::new(0),
            state: AtomicU8::new(STATE_COLD),
        }
    }

    /// Raw stored embedding for `node_id`, if present (CONCEPT:KG-2.255 — the MMR
    /// reranker needs per-candidate vectors to compute pairwise diversity).
    pub fn get_embedding(&self, node_id: &str) -> Option<Vec<f32>> {
        self.embeddings.get(node_id).cloned()
    }

    pub fn add_embedding(&mut self, node_id: String, embedding: Vec<f32>) {
        self.embeddings.insert(node_id.clone(), embedding.clone());
        let live_len = self.embeddings.len();

        let mut idx = self.index.write();
        match idx.as_mut() {
            None => {
                // Not built yet → stays brute-force until the threshold; built
                // lazily on the next search.
            }
            Some(ann) => {
                // Incremental insert (overwrite tombstones the prior row).
                if !ann.add(&node_id, &embedding) {
                    // Dimension drift → drop the index; a background warm rebuilds it.
                    *idx = None;
                    *self.built_len.write() = 0;
                    self.state.store(STATE_COLD, Ordering::Release);
                    return;
                }
                *self.built_len.write() = live_len;
                // Deferred compaction once tombstones pile up.
                if ann.tombstone_ratio() >= COMPACT_TOMBSTONE_PCT {
                    ann.compact();
                }
            }
        }
    }

    /// Force a clean compaction that drops all tombstones (ops/maintenance hook).
    pub fn force_compact(&self) {
        if let Some(ann) = self.index.write().as_mut() {
            ann.compact();
        }
    }

    pub fn semantic_search(&self, query_embedding: &[f32], n_results: usize) -> Vec<(String, f32)> {
        if self.embeddings.len() < BRUTE_FORCE_THRESHOLD.max(ANN_BUILD_THRESHOLD) {
            return self.brute_force_search(query_embedding, n_results);
        }
        // CONCEPT:EG-013 — NEVER build the index inline on the request path. Use the
        // ANN index only if it has been warmed AND still reflects the current
        // embeddings; otherwise serve an EXACT brute-force result (sub-second even at
        // 168k×1024) while the background warm task builds/loads the index. `try_read`
        // means a search that races an in-progress `warm` (which holds the index
        // write lock) falls straight through to brute force instead of blocking.
        if self.state.load(Ordering::Acquire) == STATE_READY {
            if let Some(guard) = self.index.try_read() {
                if let Some(ann) = guard.as_ref() {
                    if *self.built_len.read() == self.embeddings.len() {
                        return ann.search(query_embedding, n_results);
                    }
                }
            }
        }
        self.brute_force_search(query_embedding, n_results)
    }

    /// True once a fresh ANN index is resident (the "semantic index ready" signal).
    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_READY
            && *self.built_len.read() == self.embeddings.len()
    }

    /// True if a resident index reflects the CURRENT embedding count (no staleness).
    /// Used by the warm task to decide whether a reopened persisted index is usable
    /// as-is or must be rebuilt because the store grew since it was saved.
    pub fn index_matches_len(&self) -> bool {
        self.index.read().is_some() && *self.built_len.read() == self.embeddings.len()
    }

    /// CONCEPT:EG-013 — build the IVF-PQ index OFF the query path. Called by the
    /// background warm-on-start task (and `save_index`), NEVER by `semantic_search`.
    /// No-op for stores below the build threshold (brute force is exact + fast).
    /// `label` is the graph name, for the build-throughput log line.
    pub fn warm(&self, label: &str) {
        if self.embeddings.len() < BRUTE_FORCE_THRESHOLD.max(ANN_BUILD_THRESHOLD) {
            return;
        }
        self.ensure_index(label);
    }

    /// Ensure the index reflects the current embeddings. Builds (IVF-PQ train +
    /// encode) if absent or stale and flips the store to `Ready`. This is the
    /// expensive path — it is run only off the request path (`warm`/`save_index`).
    fn ensure_index(&self, label: &str) {
        {
            let idx = self.index.read();
            if idx.is_some() && *self.built_len.read() == self.embeddings.len() {
                return;
            }
        }
        let mut idx = self.index.write();
        if idx.is_some() && *self.built_len.read() == self.embeddings.len() {
            return; // another thread built it while we waited
        }
        let n = self.embeddings.len();
        let span = tracing::info_span!("ann_index_build", graph = label, n_vectors = n);
        let _g = span.enter();
        let start = std::time::Instant::now();
        *idx = AnnIndex::build(&self.embeddings);
        let built = idx.is_some();
        *self.built_len.write() = if built { n } else { 0 };
        self.state.store(
            if built { STATE_READY } else { STATE_COLD },
            Ordering::Release,
        );
        tracing::info!(
            graph = label,
            n_vectors = n,
            build_ms = start.elapsed().as_millis() as u64,
            ready = built,
            "semantic ANN index build complete (CONCEPT:EG-013)"
        );
    }

    /// Brute-force cosine similarity search. CONCEPT:EG-014 — this is the path EVERY
    /// query takes until the ANN index warms (and after every restart), so it is
    /// rayon-parallel across all cores with a partial-select top-k. The per-vector
    /// distance uses the SIMD-friendly chunked `dot_product` below. String ids are
    /// cloned only for the surviving top-k, never for all 168k candidates.
    fn brute_force_search(&self, query_embedding: &[f32], n_results: usize) -> Vec<(String, f32)> {
        let query_norm = dot_product(query_embedding, query_embedding).sqrt();
        if query_norm == 0.0 || n_results == 0 {
            return Vec::new();
        }
        let inv_qnorm = 1.0 / query_norm;
        // Parallel distance map — borrows ids (no clone) so the 168k-candidate fan-out
        // stays pointer-cheap. rayon's adaptive splitting makes this a no-op-overhead
        // sequential pass for tiny stores and a full 24-core fan-out for large ones.
        let mut scored: Vec<(&String, f32)> = self
            .embeddings
            .par_iter()
            .filter_map(|(node_id, emb)| {
                let emb_norm = dot_product(emb, emb).sqrt();
                if emb_norm == 0.0 {
                    None
                } else {
                    let similarity = dot_product(query_embedding, emb) * inv_qnorm / emb_norm;
                    Some((node_id, similarity))
                }
            })
            .collect();
        // Top-k: partial-select the k best (O(n)) then sort only that prefix, rather
        // than a full O(n log n) sort of all candidates.
        let cmp_desc = |a: &(&String, f32), b: &(&String, f32)| {
            b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
        };
        let k = n_results.min(scored.len());
        if k < scored.len() {
            scored.select_nth_unstable_by(k.saturating_sub(1), cmp_desc);
            scored.truncate(k);
        }
        scored.sort_by(cmp_desc);
        scored
            .into_iter()
            .map(|(id, sim)| (id.clone(), sim))
            .collect()
    }

    /// Persist the eg-ann index (codes + meta + id map) for a no-rebuild reopen.
    /// Builds the index first if it isn't resident. Errors if there is nothing to
    /// index (empty / below the build threshold).
    pub fn save_index(&self, dir: &Path) -> std::io::Result<()> {
        self.ensure_index("save_index");
        match self.index.read().as_ref() {
            Some(ann) => ann.save(dir),
            None => Err(std::io::Error::other(
                "no ANN index to persist (store empty or below build threshold)",
            )),
        }
    }

    /// Reopen a persisted eg-ann index WITHOUT rebuilding from raw vectors and
    /// attach it to this store. The caller is responsible for the matching
    /// `embeddings` map (loaded from the snapshot).
    pub fn load_index(&self, dir: &Path) -> std::io::Result<()> {
        let ann = AnnIndex::load(dir)?;
        let n = ann.live_len();
        *self.index.write() = Some(ann);
        *self.built_len.write() = n;
        // Reopened WITHOUT rebuilding — flip to Ready so searches use it immediately.
        // (If the store grew since the save, `built_len != embeddings.len()` and the
        // search staleness check falls back to brute force until a re-warm.)
        self.state.store(STATE_READY, Ordering::Release);
        Ok(())
    }

    /// Returns the number of stored embeddings.
    pub fn len(&self) -> usize {
        self.embeddings.len()
    }

    /// Returns true if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.embeddings.is_empty()
    }

    /// Approximate resident bytes held by the embedding vectors (CONCEPT:KG-2.234):
    /// the sum of every stored vector's `len × 4` (f32). Used by the per-tenant
    /// memory-budget estimate; the IVF-PQ index built on top is rebuildable and not
    /// counted (the raw vectors are the durable footprint).
    pub fn embedding_bytes(&self) -> u64 {
        self.embeddings
            .values()
            .map(|v| (v.len() * std::mem::size_of::<f32>()) as u64)
            .sum()
    }
}

/// SIMD-friendly dot product (CONCEPT:EG-014). Accumulating into a single `f32`
/// serializes the floating-point dependency chain and defeats vectorization; instead
/// we accumulate into 8 independent lanes over `chunks_exact(8)` slices. The fixed
/// length-8 inner loop is bounds-check-free (the compiler proves `len == 8`) and maps
/// to one packed 256-bit AVX2 multiply-add per chunk, so a 1024-dim dot product is
/// ~128 vector ops instead of 1024 scalar ones. The tail (< 8 elems) is scalar.
#[inline]
fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = [0.0f32; 8];
    let mut ca = a.chunks_exact(8);
    let mut cb = b.chunks_exact(8);
    for (x, y) in ca.by_ref().zip(cb.by_ref()) {
        for l in 0..8 {
            acc[l] += x[l] * y[l];
        }
    }
    let mut s = ((acc[0] + acc[4]) + (acc[1] + acc[5])) + ((acc[2] + acc[6]) + (acc[3] + acc[7]));
    for (x, y) in ca.remainder().iter().zip(cb.remainder()) {
        s += x * y;
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Plain scalar reference for the SIMD-kernel A/B check.
    fn scalar_dot(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
    }

    #[test]
    fn simd_dot_product_matches_scalar_within_epsilon() {
        // CONCEPT:EG-014 — the 8-lane chunked dot product must equal the scalar one
        // for ALL lengths, including non-multiples of 8 (the `chunks_exact` tail).
        for &len in &[0usize, 1, 7, 8, 9, 15, 16, 31, 1000, 1024] {
            let a: Vec<f32> = (0..len).map(|i| (i as f32 * 0.013).sin()).collect();
            let b: Vec<f32> = (0..len).map(|i| (i as f32 * 0.021 + 1.0).cos()).collect();
            let s = scalar_dot(&a, &b);
            let v = dot_product(&a, &b);
            assert!(
                (s - v).abs() <= 1e-3 * (1.0 + s.abs()),
                "len={len}: simd {v} vs scalar {s}"
            );
        }
    }

    #[test]
    fn parallel_brute_force_matches_sequential_topk() {
        // CONCEPT:EG-014 — the rayon-parallel brute force + partial-select top-k must
        // return exactly the same top-k (ids and order) as a naive sequential cosine
        // scan. N below ANN_BUILD_THRESHOLD so `semantic_search` takes the brute path.
        let dim = 64;
        let n = 2000;
        let mut store = SemanticStore::new();
        let mut seed = 7u64;
        let mut rng = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            ((seed >> 33) as f32 / (1u64 << 31) as f32) - 0.5
        };
        let mut vecs: Vec<(String, Vec<f32>)> = Vec::new();
        for i in 0..n {
            let v: Vec<f32> = (0..dim).map(|_| rng()).collect();
            store.add_embedding(format!("n{i}"), v.clone());
            vecs.push((format!("n{i}"), v));
        }
        let query: Vec<f32> = (0..dim).map(|_| rng()).collect();
        let k = 25;

        let got = store.semantic_search(&query, k);

        // Independent sequential reference (scalar cosine).
        let qn = scalar_dot(&query, &query).sqrt();
        let mut want: Vec<(String, f32)> = vecs
            .iter()
            .filter_map(|(id, v)| {
                let vn = scalar_dot(v, v).sqrt();
                if vn == 0.0 {
                    None
                } else {
                    Some((id.clone(), scalar_dot(&query, v) / (qn * vn)))
                }
            })
            .collect();
        want.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        want.truncate(k);

        assert_eq!(got.len(), k);
        let got_ids: Vec<&String> = got.iter().map(|(id, _)| id).collect();
        let want_ids: Vec<&String> = want.iter().map(|(id, _)| id).collect();
        assert_eq!(got_ids, want_ids, "parallel top-k must match sequential");
        for ((_, gs), (_, ws)) in got.iter().zip(want.iter()) {
            assert!((gs - ws).abs() <= 1e-4, "score parity: {gs} vs {ws}");
        }
    }

    #[test]
    fn brute_force_below_threshold() {
        let mut store = SemanticStore::new();
        store.add_embedding("a".into(), vec![1.0, 0.0, 0.0]);
        store.add_embedding("b".into(), vec![0.0, 1.0, 0.0]);
        store.add_embedding("c".into(), vec![0.9, 0.1, 0.0]);
        let results = store.semantic_search(&[1.0, 0.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "a");
        assert!(results[0].1 > 0.99);
    }

    #[test]
    fn empty_store() {
        let store = SemanticStore::new();
        assert!(store.semantic_search(&[1.0, 0.0], 5).is_empty());
    }

    #[test]
    fn serde_roundtrip_preserves_embeddings() {
        let mut store = SemanticStore::new();
        for i in 0..50 {
            let mut emb = vec![0.0f32; 8];
            emb[i % 8] = 1.0;
            emb[(i + 1) % 8] = 0.5;
            store.add_embedding(format!("node_{i}"), emb);
        }
        let bytes = rmp_serde::to_vec_named(&store).unwrap();
        let restored: SemanticStore = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(restored.len(), 50);
        let results = restored.semantic_search(&[1.0, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 5);
        assert!(!results.is_empty());
    }

    #[test]
    fn ann_index_builds_and_persists_without_rebuild() {
        // Cross the build threshold so the eg-ann index activates, persist it, and
        // reload via the no-rebuild path; results must match.
        let dim = 32;
        let n = ANN_BUILD_THRESHOLD + 500;
        let mut store = SemanticStore::new();
        // Clustered data so PQ has structure to quantize.
        let mut seed = 12345u64;
        let mut rng = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            ((seed >> 33) as f32 / (1u64 << 31) as f32) - 0.5
        };
        let centers: Vec<Vec<f32>> = (0..40)
            .map(|_| (0..dim).map(|_| rng() * 2.0).collect())
            .collect();
        let mut vecs = Vec::new();
        for i in 0..n {
            let c = &centers[i % centers.len()];
            let v: Vec<f32> = (0..dim).map(|j| c[j] + rng() * 0.2).collect();
            store.add_embedding(format!("n{i}"), v.clone());
            vecs.push((format!("n{i}"), v));
        }
        // Build the index OFF the query path (warm), then query the ANN path.
        store.warm("test");
        assert!(store.is_ready(), "warm must make the store Ready");
        let q = &vecs[100].1;
        let before = store.semantic_search(q, 10);
        assert!(!before.is_empty());
        assert_eq!(before[0].0, "n100", "self should be top-1");

        let tmp = std::env::temp_dir().join(format!("eg-ann-store-{}", std::process::id()));
        store.save_index(&tmp).unwrap();

        // Fresh store with the same embeddings (snapshot path) + no-rebuild load.
        let reloaded = SemanticStore {
            embeddings: store.embeddings.clone(),
            index: RwLock::new(None),
            built_len: RwLock::new(0),
            state: AtomicU8::new(STATE_COLD),
        };
        reloaded.load_index(&tmp).unwrap();
        assert!(
            reloaded.is_ready(),
            "no-rebuild reload must leave the store Ready"
        );
        let after = reloaded.semantic_search(q, 10);
        assert_eq!(
            before.iter().map(|r| r.0.clone()).collect::<Vec<_>>(),
            after.iter().map(|r| r.0.clone()).collect::<Vec<_>>(),
            "no-rebuild reload must match"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn search_never_builds_inline_cold_start_is_brute_force() {
        // CONCEPT:EG-013 — the cold-start fix. A large store loaded fresh (index
        // `None`, state `Cold`, exactly the post-restart shape) must answer searches
        // WITHOUT triggering an inline IVF-PQ build: the store stays Cold and serves
        // exact brute-force results. Only `warm` (off the query path) builds it.
        let dim = 32;
        let n = ANN_BUILD_THRESHOLD + 500;
        let mut store = SemanticStore::new();
        let mut seed = 999u64;
        let mut rng = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            ((seed >> 33) as f32 / (1u64 << 31) as f32) - 0.5
        };
        let centers: Vec<Vec<f32>> = (0..40)
            .map(|_| (0..dim).map(|_| rng() * 2.0).collect())
            .collect();
        let mut query = vec![0.0f32; dim];
        for i in 0..n {
            let c = &centers[i % centers.len()];
            let v: Vec<f32> = (0..dim).map(|j| c[j] + rng() * 0.2).collect();
            if i == 100 {
                query = v.clone();
            }
            store.add_embedding(format!("n{i}"), v);
        }

        // COLD: no index has ever been built (no warm, no save).
        assert!(!store.is_ready(), "fresh load must be Cold");
        assert!(store.index.read().is_none(), "no index resident yet");

        // A search must NOT build the index — it serves brute force and stays Cold.
        let cold = store.semantic_search(&query, 10);
        assert!(!cold.is_empty(), "cold search still returns (brute force)");
        assert_eq!(cold[0].0, "n100", "brute force is exact: self is top-1");
        assert!(
            !store.is_ready(),
            "the query path must NOT have built the index inline"
        );
        assert!(
            store.index.read().is_none(),
            "the query path must leave the index unbuilt (no inline rebuild)"
        );

        // Warming (off the query path) makes it Ready; searches then use the index.
        store.warm("test");
        assert!(store.is_ready(), "warm builds the index and flips to Ready");
        let warm = store.semantic_search(&query, 10);
        assert!(!warm.is_empty());
        assert_eq!(warm[0].0, "n100", "ANN path self is top-1");
    }
}
