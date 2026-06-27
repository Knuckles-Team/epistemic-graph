// CONCEPT:KG-2.207 — Semantic Embedding Store backed by the native eg-ann
// IVF-PQ + OPQ + SQ8-refine index (feature `ann`).
//
// Drop-in replacement for the hnsw_rs `SemanticStore`: identical public API
// (`new`/`add_embedding`/`semantic_search`/`force_compact`/`len`/`is_empty`). For
// tiny stores it uses brute-force cosine; past `ANN_BUILD_THRESHOLD` it builds and
// maintains an eg-ann index. A persisted eg-ann index reopens WITHOUT rebuilding
// from raw vectors — the no-rebuild win — but the snapshot path here rebuilds lazily
// from the resident embeddings on first search after load (matching the existing
// SemanticStore checkpoint contract); call `save_index`/`load_index` for the
// no-rebuild persistent index path.
//
// CONCEPT:EG-015 — contiguous embedding arena. EG-014 proved the brute-force/ANN
// path is MEMORY-BOUND, not core-bound: the embeddings used to live as a
// `HashMap<String, Vec<f32>>`, i.e. one scattered heap allocation per vector
// (~168k separate ~4 KB allocations for 168k×1024). Every brute-force scan
// pointer-chased 168k allocations, so the CPU stalled on cache misses and rayon
// only got ~2× of a possible ~20×. The embeddings now live in ONE flat row-major
// `Vec<f32>` of `rows×dim` plus a parallel `Vec<String>` id table; a scan streams
// contiguous rows (`par_chunks_exact(dim)`), which is hardware-prefetchable,
// cache-friendly and NUMA-friendly. CONCEPT:EG-016 — each row's L2 norm is cached
// so cosine is ONE dot product per candidate instead of two.

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

/// CONCEPT:EG-015 — the contiguous row-major embedding arena.
///
/// Invariants:
///   * `data.len() == ids.len() * dim` (fully dense — there are NO tombstone rows;
///     the store has no delete, and an overwrite is an in-place row update, so the
///     arena is always exactly the live set).
///   * `norms.len() == ids.len()` — `norms[r]` is the cached L2 norm of row `r`.
///   * `id_to_row[id]` is the current row for `id`; row `r`'s id is `ids[r]`.
#[derive(Clone, Default)]
struct EmbeddingArena {
    /// Embedding dimensionality. `0` until the first vector is inserted.
    dim: usize,
    /// Row-major flat storage: row `r` is `data[r*dim .. (r+1)*dim]`. ONE allocation.
    data: Vec<f32>,
    /// Parallel id table: `ids[r]` is the node id occupying row `r`.
    ids: Vec<String>,
    /// node id → its current row index.
    id_to_row: HashMap<String, usize>,
    /// CONCEPT:EG-016 — cached per-row L2 norm (`norms[r] = ‖row r‖₂`). Lets cosine
    /// skip recomputing `√(emb·emb)` on every query.
    norms: Vec<f32>,
}

impl EmbeddingArena {
    #[inline]
    fn len(&self) -> usize {
        self.ids.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Row `r` as a contiguous slice.
    #[inline]
    fn row(&self, r: usize) -> &[f32] {
        &self.data[r * self.dim..(r + 1) * self.dim]
    }

    /// Current embedding for `id`, if present.
    #[inline]
    fn get(&self, id: &str) -> Option<&[f32]> {
        self.id_to_row.get(id).map(|&r| self.row(r))
    }

    /// Insert (append a new dense row) or overwrite (in-place, arena stays dense).
    /// Returns `true` if this insert changed the arena dimensionality (drift), which
    /// re-initializes the arena at the new dim — matching the "re-embed = clean
    /// slate" operational reality (the embedder dim is fixed in practice, so this is
    /// the rare model-swap path, never the steady state).
    fn insert(&mut self, id: String, emb: &[f32]) -> bool {
        if emb.is_empty() {
            return false;
        }
        let mut drift = false;
        if self.dim == 0 {
            self.dim = emb.len();
        } else if emb.len() != self.dim {
            // Flat arena is single-dim; a different dim cannot share the buffer.
            tracing::warn!(
                old_dim = self.dim,
                new_dim = emb.len(),
                "embedding dimensionality changed — re-initializing the arena (CONCEPT:EG-015)"
            );
            self.data.clear();
            self.ids.clear();
            self.id_to_row.clear();
            self.norms.clear();
            self.dim = emb.len();
            drift = true;
        }
        let norm = l2_norm(emb);
        match self.id_to_row.get(&id).copied() {
            Some(r) => {
                // In-place overwrite — the arena stays contiguous and dense.
                self.data[r * self.dim..(r + 1) * self.dim].copy_from_slice(emb);
                self.norms[r] = norm;
            }
            None => {
                let r = self.ids.len();
                self.data.extend_from_slice(emb);
                self.norms.push(norm);
                self.id_to_row.insert(id.clone(), r);
                self.ids.push(id);
            }
        }
        drift
    }

    /// Reconstruct an arena from the persisted flat wire shape (CONCEPT:EG-015).
    /// `dim` may be `0` in older flat snapshots — it is then derived from the row
    /// count. Norms are recomputed on load (cheap; keeps the wire shape minimal and
    /// avoids any persisted-vs-recomputed drift).
    fn from_flat(mut dim: usize, ids: Vec<String>, data: Vec<f32>) -> Self {
        if dim == 0 && !ids.is_empty() {
            dim = data.len() / ids.len();
        }
        let mut id_to_row = HashMap::with_capacity(ids.len());
        for (r, id) in ids.iter().enumerate() {
            id_to_row.insert(id.clone(), r);
        }
        let norms = if dim > 0 {
            data.chunks_exact(dim).map(l2_norm).collect()
        } else {
            Vec::new()
        };
        Self {
            dim,
            data,
            ids,
            id_to_row,
            norms,
        }
    }

    /// CONCEPT:EG-015 — one-time migration read of the LEGACY `HashMap` wire shape.
    /// A snapshot written before the arena (or by the hnsw default backend) persists
    /// `embeddings: HashMap<String, Vec<f32>>`; we fold it into the dense arena on
    /// load and write the flat shape thereafter (read-old / write-new).
    fn from_legacy_map(map: HashMap<String, Vec<f32>>) -> Self {
        let mut arena = EmbeddingArena::default();
        for (id, v) in map {
            arena.insert(id, &v);
        }
        arena
    }

    /// Resident bytes held by the flat embedding buffer (CONCEPT:KG-2.234).
    #[inline]
    fn embedding_bytes(&self) -> u64 {
        (self.data.len() * std::mem::size_of::<f32>()) as u64
    }
}

pub struct SemanticStore {
    /// CONCEPT:EG-015 — contiguous arena of all live embeddings (see above).
    arena: EmbeddingArena,
    /// eg-ann IVF-PQ index. `None` until the store is WARMED (off the query path)
    /// or a persisted index is reopened. The index is non-serialized, so a fresh
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
            arena: self.arena.clone(),
            index: RwLock::new(None),
            built_len: RwLock::new(0),
            state: AtomicU8::new(STATE_COLD),
        }
    }
}

impl std::fmt::Debug for SemanticStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SemanticStore")
            .field("embeddings", &self.arena.len())
            .field("dim", &self.arena.dim)
            .field("backend", &"eg-ann")
            .finish()
    }
}

// CONCEPT:EG-015 — persist the FLAT arena (`dim` + `ids` + row-major `data`) rather
// than the old per-vector `HashMap`. Norms are derived on load. The Deserialize side
// still reads the legacy `embeddings` map (one-time migration; read-old/write-new).
impl Serialize for SemanticStore {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("SemanticStore", 3)?;
        st.serialize_field("dim", &self.arena.dim)?;
        st.serialize_field("ids", &self.arena.ids)?;
        st.serialize_field("data", &self.arena.data)?;
        st.end()
    }
}

impl<'de> Deserialize<'de> for SemanticStore {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            // CONCEPT:EG-015 new flat wire shape.
            #[serde(default)]
            dim: Option<usize>,
            #[serde(default)]
            ids: Option<Vec<String>>,
            #[serde(default)]
            data: Option<Vec<f32>>,
            // Legacy per-vector map (migration read only).
            #[serde(default)]
            embeddings: Option<HashMap<String, Vec<f32>>>,
        }
        let raw = Raw::deserialize(d)?;
        let arena = match (raw.ids, raw.data) {
            (Some(ids), Some(data)) => {
                EmbeddingArena::from_flat(raw.dim.unwrap_or(0), ids, data)
            }
            _ => match raw.embeddings {
                Some(map) => EmbeddingArena::from_legacy_map(map),
                None => EmbeddingArena::default(),
            },
        };
        Ok(Self {
            arena,
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
            arena: EmbeddingArena::default(),
            index: RwLock::new(None),
            built_len: RwLock::new(0),
            state: AtomicU8::new(STATE_COLD),
        }
    }

    /// Raw stored embedding for `node_id`, if present (CONCEPT:KG-2.255 — the MMR
    /// reranker needs per-candidate vectors to compute pairwise diversity).
    pub fn get_embedding(&self, node_id: &str) -> Option<Vec<f32>> {
        self.arena.get(node_id).map(|s| s.to_vec())
    }

    pub fn add_embedding(&mut self, node_id: String, embedding: Vec<f32>) {
        // CONCEPT:EG-015 — append/overwrite a contiguous row (no per-vector alloc).
        let drift = self.arena.insert(node_id.clone(), &embedding);
        let live_len = self.arena.len();

        if drift {
            // Dimensionality changed → the arena re-initialized; the index is stale.
            // A background warm rebuilds it.
            *self.index.write() = None;
            *self.built_len.write() = 0;
            self.state.store(STATE_COLD, Ordering::Release);
            return;
        }

        let mut idx = self.index.write();
        match idx.as_mut() {
            None => {
                // Not built yet → stays brute-force until the threshold; built
                // lazily (off the query path) on warm.
            }
            Some(ann) => {
                // Incremental insert (overwrite tombstones the prior row in the index).
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
        if self.arena.len() < BRUTE_FORCE_THRESHOLD.max(ANN_BUILD_THRESHOLD) {
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
                    if *self.built_len.read() == self.arena.len() {
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
            && *self.built_len.read() == self.arena.len()
    }

    /// True if a resident index reflects the CURRENT embedding count (no staleness).
    /// Used by the warm task to decide whether a reopened persisted index is usable
    /// as-is or must be rebuilt because the store grew since it was saved.
    pub fn index_matches_len(&self) -> bool {
        self.index.read().is_some() && *self.built_len.read() == self.arena.len()
    }

    /// CONCEPT:EG-013 — build the IVF-PQ index OFF the query path. Called by the
    /// background warm-on-start task (and `save_index`), NEVER by `semantic_search`.
    /// No-op for stores below the build threshold (brute force is exact + fast).
    /// `label` is the graph name, for the build-throughput log line.
    pub fn warm(&self, label: &str) {
        if self.arena.len() < BRUTE_FORCE_THRESHOLD.max(ANN_BUILD_THRESHOLD) {
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
            if idx.is_some() && *self.built_len.read() == self.arena.len() {
                return;
            }
        }
        let mut idx = self.index.write();
        if idx.is_some() && *self.built_len.read() == self.arena.len() {
            return; // another thread built it while we waited
        }
        let n = self.arena.len();
        let span = tracing::info_span!("ann_index_build", graph = label, n_vectors = n);
        let _g = span.enter();
        let start = std::time::Instant::now();
        // CONCEPT:EG-015 — build straight off the contiguous arena (no HashMap rebuild).
        *idx = AnnIndex::build(&self.arena.ids, &self.arena.data, self.arena.dim);
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

    /// Brute-force cosine similarity search. CONCEPT:EG-014/EG-015 — this is the path
    /// EVERY query takes until the ANN index warms (and after every restart), so it
    /// is rayon-parallel across all cores with a partial-select top-k. EG-015 streams
    /// the embeddings as CONTIGUOUS rows (`par_chunks_exact(dim)`) out of one flat
    /// buffer — hardware-prefetchable, cache-friendly, NUMA-friendly — instead of
    /// pointer-chasing 168k scattered heap allocations. EG-016 reads the row's cached
    /// L2 norm rather than recomputing `√(emb·emb)`, so cosine is ONE dot product per
    /// candidate. String ids are cloned only for the surviving top-k.
    fn brute_force_search(&self, query_embedding: &[f32], n_results: usize) -> Vec<(String, f32)> {
        let arena = &self.arena;
        if arena.is_empty() || n_results == 0 || arena.dim == 0 {
            return Vec::new();
        }
        let query_norm = l2_norm(query_embedding);
        if query_norm == 0.0 {
            return Vec::new();
        }
        let inv_qnorm = 1.0 / query_norm;
        let dim = arena.dim;
        // Parallel distance map over CONTIGUOUS rows. `par_chunks_exact` hands each
        // worker a cache-line-aligned run of rows; the row index recovers the id only
        // for survivors, so the 168k-candidate fan-out stays pointer-cheap. rayon's
        // adaptive splitting is a no-op-overhead sequential pass for tiny stores and a
        // full all-core fan-out for large ones.
        let norms = &arena.norms;
        let mut scored: Vec<(usize, f32)> = arena
            .data
            .par_chunks_exact(dim)
            .enumerate()
            .filter_map(|(row, emb)| {
                let emb_norm = norms[row]; // CONCEPT:EG-016 cached
                if emb_norm == 0.0 {
                    None
                } else {
                    let similarity = dot_product(query_embedding, emb) * inv_qnorm / emb_norm;
                    Some((row, similarity))
                }
            })
            .collect();
        // Top-k: partial-select the k best (O(n)) then sort only that prefix, rather
        // than a full O(n log n) sort of all candidates.
        let cmp_desc = |a: &(usize, f32), b: &(usize, f32)| {
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
            .map(|(row, sim)| (arena.ids[row].clone(), sim))
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
    /// `embeddings` arena (loaded from the snapshot).
    pub fn load_index(&self, dir: &Path) -> std::io::Result<()> {
        let ann = AnnIndex::load(dir)?;
        let n = ann.live_len();
        *self.index.write() = Some(ann);
        *self.built_len.write() = n;
        // Reopened WITHOUT rebuilding — flip to Ready so searches use it immediately.
        // (If the store grew since the save, `built_len != arena.len()` and the
        // search staleness check falls back to brute force until a re-warm.)
        self.state.store(STATE_READY, Ordering::Release);
        Ok(())
    }

    /// Returns the number of stored embeddings.
    pub fn len(&self) -> usize {
        self.arena.len()
    }

    /// Returns true if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.arena.is_empty()
    }

    /// Approximate resident bytes held by the embedding vectors (CONCEPT:KG-2.234):
    /// the flat arena's `len × 4` (f32). Used by the per-tenant memory-budget
    /// estimate; the IVF-PQ index built on top is rebuildable and not counted (the
    /// raw vectors are the durable footprint).
    pub fn embedding_bytes(&self) -> u64 {
        self.arena.embedding_bytes()
    }
}

/// SIMD-friendly dot product (CONCEPT:EG-014). Accumulating into a single `f32`
/// serializes the floating-point dependency chain and defeats vectorization; instead
/// we accumulate into 8 independent lanes over `chunks_exact(8)` slices. The fixed
/// length-8 inner loop is bounds-check-free (the compiler proves `len == 8`) and maps
/// to one packed 256-bit AVX2 multiply-add per chunk (under `-C target-cpu=x86-64-v3`,
/// see `.cargo/config.toml`), so a 1024-dim dot product is ~128 vector ops instead of
/// 1024 scalar ones. The tail (< 8 elems) is scalar.
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

/// CONCEPT:EG-016 — L2 norm via the SIMD dot kernel (cached per row at insert).
#[inline]
fn l2_norm(v: &[f32]) -> f32 {
    dot_product(v, v).sqrt()
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
        // CONCEPT:EG-014/EG-015 — the rayon-parallel contiguous brute force +
        // partial-select top-k must return exactly the same top-k (ids and order) as a
        // naive sequential cosine scan. N below ANN_BUILD_THRESHOLD so `semantic_search`
        // takes the brute path.
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
    fn arena_overwrite_is_in_place_and_dense() {
        // CONCEPT:EG-015 — overwriting an id reuses its row (the arena stays dense:
        // data.len() == rows*dim, no tombstones) and the new vector wins in search.
        let mut store = SemanticStore::new();
        for i in 0..10 {
            store.add_embedding(format!("n{i}"), vec![i as f32, 0.0, 0.0]);
        }
        assert_eq!(store.arena.len(), 10);
        assert_eq!(store.arena.data.len(), 10 * 3);
        // Overwrite n3 to point a brand-new direction.
        store.add_embedding("n3".into(), vec![0.0, 0.0, 99.0]);
        assert_eq!(store.arena.len(), 10, "overwrite must not add a row");
        assert_eq!(store.arena.data.len(), 10 * 3, "arena stays dense on overwrite");
        let res = store.semantic_search(&[0.0, 0.0, 1.0], 1);
        assert_eq!(res[0].0, "n3", "latest overwrite wins");
        // Cached norm reflects the overwrite.
        let r = store.arena.id_to_row["n3"];
        assert!((store.arena.norms[r] - 99.0).abs() < 1e-3);
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
        assert_eq!(restored.arena.dim, 8, "flat shape round-trips dim");
        assert_eq!(restored.arena.data.len(), 50 * 8);
        // Norms recomputed on load and match.
        let r = restored.arena.id_to_row["node_0"];
        assert!((restored.arena.norms[r] - l2_norm(restored.arena.row(r))).abs() < 1e-6);
        let results = restored.semantic_search(&[1.0, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 5);
        assert!(!results.is_empty());
    }

    #[test]
    fn legacy_hashmap_snapshot_migrates_to_arena() {
        // CONCEPT:EG-015 — a snapshot written in the OLD `embeddings: HashMap` wire
        // shape (or by the hnsw default backend) must still load: read-old/write-new.
        #[derive(serde::Serialize)]
        struct LegacyStore {
            embeddings: HashMap<String, Vec<f32>>,
        }
        let mut map = HashMap::new();
        for i in 0..40usize {
            let mut emb = vec![0.0f32; 8];
            emb[i % 8] = 1.0;
            map.insert(format!("n{i}"), emb);
        }
        let legacy = LegacyStore {
            embeddings: map.clone(),
        };
        let bytes = rmp_serde::to_vec_named(&legacy).unwrap();
        // Deserialize through the NEW SemanticStore — the migration path.
        let restored: SemanticStore = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(restored.len(), 40, "legacy map migrated into the arena");
        assert_eq!(restored.arena.dim, 8);
        assert_eq!(restored.arena.data.len(), 40 * 8, "dense arena after migration");
        // And it re-serializes in the new flat shape.
        let rebytes = rmp_serde::to_vec_named(&restored).unwrap();
        let again: SemanticStore = rmp_serde::from_slice(&rebytes).unwrap();
        assert_eq!(again.len(), 40);
        assert_eq!(again.arena.data.len(), 40 * 8);
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
            arena: store.arena.clone(),
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
