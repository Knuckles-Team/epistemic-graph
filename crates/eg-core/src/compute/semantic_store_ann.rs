// CONCEPT:EG-KG.sharding.semantic-embedding-store-backed — Semantic Embedding Store backed by the native eg-ann
// IVF-PQ + OPQ + SQ8-refine index (feature `ann`).
//
// Native `eg-ann` `SemanticStore` implementation with the stable public API
// (`new`/`add_embedding`/`semantic_search`/`force_compact`/`len`/`is_empty`). For
// tiny stores it uses brute-force cosine; past `ANN_BUILD_THRESHOLD` it builds and
// maintains an eg-ann index. A persisted eg-ann index reopens WITHOUT rebuilding
// from raw vectors — the no-rebuild win — but the snapshot path here rebuilds lazily
// from the resident embeddings on first search after load (matching the existing
// SemanticStore checkpoint contract); call `export_generation`/`adopt_generation`
// for the no-rebuild durable index path, whose durable leg is one admitted
// `Native(SemanticIndex)` mutation (`compute::semantic_ann_codes`).
//
// CONCEPT:EG-KG.storage.arena-row-append — contiguous embedding arena. EG-014 proved the brute-force/ANN
// path is MEMORY-BOUND, not core-bound: the embeddings used to live as a
// `HashMap<String, Vec<f32>>`, i.e. one scattered heap allocation per vector
// (~168k separate ~4 KB allocations for 168k×1024). Every brute-force scan
// pointer-chased 168k allocations, so the CPU stalled on cache misses and rayon
// only got ~2× of a possible ~20×. The embeddings now live in ONE flat row-major
// `Vec<f32>` of `rows×dim` plus a parallel `Vec<String>` id table; a scan streams
// contiguous rows (`par_chunks_exact(dim)`), which is hardware-prefetchable,
// cache-friendly and NUMA-friendly. CONCEPT:EG-KG.compute.cached-row-norm — each row's L2 norm is cached
// so cosine is ONE dot product per candidate instead of two.

use super::{check_embedding_dimension, EmbeddingDimensionError, SemanticQueryError};
use crate::compute::semantic_ann::{AnnIndex, ANN_BUILD_THRESHOLD};
use eg_types::{EmbeddingSpaceRef, StampedVector};
use parking_lot::RwLock;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};

/// Raw/native exact vectors may use the generic 16k coordinate ceiling.  The
/// persisted IVF-PQ artifact below has a stricter 4k ceiling; wider rows stay
/// on exact search and never cross the maintained-index boundary.
const MAX_GENERIC_DIMENSION: usize = eg_types::MAX_EMBEDDING_DIMENSIONS;
const MAX_MAINTAINED_DIMENSION: usize = eg_types::MAX_MAINTAINED_ANN_DIMENSIONS;
const INDEX_MANIFEST_MAGIC: &[u8] = b"EGSEMSTORE\x01\0";
const MAX_INDEX_MANIFEST_BYTES: usize = 64 * 1024 * 1024;
const MAX_INDEX_MEMBERS: usize = 5_000_000;
const MAX_MEMBER_ID_BYTES: usize = 4_096;

/// Threshold below which we use brute-force (index overhead not worth it).
const BRUTE_FORCE_THRESHOLD: usize = 32;
/// Rebuild/compact the index once tombstoned rows exceed this fraction of total.
const COMPACT_TOMBSTONE_PCT: f32 = 0.30;

// CONCEPT:EG-KG.storage.semantic-index-directory — index readiness state. The cold-start bug was that the FIRST
// `semantic_search` after a restart triggered a full IVF-PQ+OPQ build INLINE on the
// request path (single-threaded SVD over a 1024² matrix + k-means over 168k vectors
// → minutes, pegging one core, never finishing within the request timeout). The
// index now builds OFF the query path (`warm`, run by a background warm-on-start
// task) and persists across restarts; the query path checks this flag and serves a
// fast exact brute-force result while the index is still `Cold`, instead of building.
const STATE_COLD: u8 = 0;
const STATE_READY: u8 = 1;
// W0.4 — a warm build claimed in flight (`ensure_index`'s duplicate-concurrent-
// warm guard below): distinct from `STATE_COLD` so a second trigger (e.g. the
// on-write hook racing the periodic re-check sweep for the SAME graph) can tell
// "nothing built yet" apart from "somebody is already building" and back off
// instead of blocking on the index write lock for the whole build.
const STATE_WARMING: u8 = 2;

/// CONCEPT:EG-KG.storage.arena-row-append — the contiguous row-major embedding arena.
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
    /// CONCEPT:EG-KG.compute.cached-row-norm — cached per-row L2 norm (`norms[r] = ‖row r‖₂`). Lets cosine
    /// skip recomputing `√(emb·emb)` on every query.
    norms: Vec<f32>,
}

impl EmbeddingArena {
    fn len(&self) -> usize {
        self.ids.len()
    }

    fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Row `r` as a contiguous slice.
    fn row(&self, r: usize) -> &[f32] {
        &self.data[r * self.dim..(r + 1) * self.dim]
    }

    /// Current embedding for `id`, if present.
    fn get(&self, id: &str) -> Option<&[f32]> {
        self.id_to_row.get(id).map(|&r| self.row(r))
    }

    /// Insert (append a new dense row) or overwrite (in-place, arena stays dense).
    ///
    /// CONCEPT:EG-KG.compute.rank-dim-mismatch-guard (BUG-007, P0 — data loss): a mismatched
    /// dimension used to be silently "fixed" by clearing the WHOLE arena (`data`/
    /// `ids`/`id_to_row`/`norms`) and re-initializing at the new width, erasing every
    /// unrelated embedding on a single malformed write. That is now a hard reject:
    /// `check_embedding_dimension` runs FIRST and returns `Err` before this method
    /// touches `self` at all, so a rejected write leaves the arena byte-for-byte
    /// unchanged — proven by `mismatched_dimension_insert_does_not_erase_corpus`.
    fn insert(&mut self, id: String, emb: &[f32]) -> Result<(), EmbeddingDimensionError> {
        if emb.len() > MAX_GENERIC_DIMENSION {
            return Err(EmbeddingDimensionError::Oversized {
                received: emb.len(),
                max: MAX_GENERIC_DIMENSION,
            });
        }
        self.dim = check_embedding_dimension(emb, self.dim)?;
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
        Ok(())
    }

    /// Remove `id` from the dense arena via O(dim) swap-remove (CONCEPT:EG-KG.storage.incremental-ann):
    /// the last row is moved into the vacated slot so the arena stays contiguous and
    /// dense (`data.len() == ids.len() * dim`). Returns `true` if a row was removed,
    /// `false` if `id` was absent. The resident ANN index is tombstoned separately by
    /// the caller — its rows are keyed independently of arena position, so a
    /// swap-remove here does not corrupt it.
    fn remove(&mut self, id: &str) -> bool {
        let Some(r) = self.id_to_row.remove(id) else {
            return false;
        };
        let last = self.ids.len() - 1;
        if r != last {
            // Move the last row's vector + norm + id into the vacated slot `r`.
            self.data
                .copy_within(last * self.dim..(last + 1) * self.dim, r * self.dim);
            self.norms[r] = self.norms[last];
            let moved = self.ids[last].clone();
            self.ids[r] = moved.clone();
            self.id_to_row.insert(moved, r);
        }
        self.data.truncate(last * self.dim);
        self.norms.pop();
        self.ids.pop();
        true
    }

    /// Reconstruct an arena from the sole current persisted flat wire shape
    /// (CONCEPT:EG-KG.storage.arena-row-append). Norms are recomputed on load.
    fn from_flat(dim: usize, ids: Vec<String>, data: Vec<f32>) -> Result<Self, String> {
        validate_flat_shape(dim, ids.len(), data.len())?;
        if let Some(index) = data.iter().position(|value| !value.is_finite()) {
            return Err(format!(
                "embedding arena contains a non-finite value at flat index {index}"
            ));
        }
        let mut id_to_row = HashMap::with_capacity(ids.len());
        for (r, id) in ids.iter().enumerate() {
            if id_to_row.insert(id.clone(), r).is_some() {
                return Err("embedding arena contains duplicate ids".to_string());
            }
        }
        let norms = if dim > 0 {
            data.chunks_exact(dim).map(l2_norm).collect()
        } else {
            Vec::new()
        };
        Ok(Self {
            dim,
            data,
            ids,
            id_to_row,
            norms,
        })
    }

    /// Resident bytes held by the flat embedding buffer (CONCEPT:EG-KG.compute.lane-v).
    fn embedding_bytes(&self) -> u64 {
        (self.data.len() * std::mem::size_of::<f32>()) as u64
    }
}

fn validate_flat_shape(dim: usize, row_count: usize, data_len: usize) -> Result<(), String> {
    if dim > MAX_GENERIC_DIMENSION {
        return Err(format!(
            "embedding arena dimension {dim} exceeds the generic maximum of {MAX_GENERIC_DIMENSION}"
        ));
    }
    let expected = row_count
        .checked_mul(dim)
        .ok_or_else(|| "embedding arena dimensions overflow".to_string())?;
    if expected != data_len || (dim == 0 && row_count != 0) {
        return Err("embedding arena dimensions are inconsistent".to_string());
    }
    Ok(())
}

pub struct SemanticStore {
    /// CONCEPT:EG-KG.storage.arena-row-append — contiguous arena of all live embeddings (see above).
    arena: EmbeddingArena,
    /// Exact model/preprocessing coordinate space for model-produced queries.
    /// `None` preserves legacy raw-vector stores without inventing an identity.
    space: Option<EmbeddingSpaceRef>,
    /// eg-ann IVF-PQ index. `None` until the store is WARMED (off the query path)
    /// or a persisted index is reopened. The index is non-serialized, so a fresh
    /// snapshot load starts `Cold`; it is NEVER built inline on a search.
    index: RwLock<Option<AnnIndex>>,
    /// LIVE embedding count the index reflects (staleness check after load).
    built_len: RwLock<usize>,
    /// CONCEPT:EG-KG.storage.semantic-index-directory — `STATE_COLD`/`STATE_READY`. `Ready` ⟺ `index` is `Some` and
    /// reflects the current embeddings. Read on every search to decide ANN-vs-brute.
    state: AtomicU8,
}

mod semantic_ann_index;
mod semantic_ann_lifecycle;
mod semantic_ann_mutation;
mod semantic_ann_persistence;
mod semantic_ann_query;
mod semantic_ann_shape;

use crate::compute::semantic_ann::AnnIndexImage;
use semantic_ann_persistence::PersistedIndexManifest as IndexManifest;
pub use semantic_ann_persistence::SemanticGenerationImage;

impl Clone for SemanticStore {
    fn clone(&self) -> Self {
        Self {
            arena: self.arena.clone(),
            space: self.space.clone(),
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
            .field("space", &self.space.as_ref().map(|space| &space.digest))
            .field("backend", &"eg-ann")
            .finish()
    }
}

fn valid_query(query: &[f32], store_dim: usize) -> bool {
    query.len() <= MAX_GENERIC_DIMENSION && check_embedding_dimension(query, store_dim).is_ok()
}

fn canonical_members(ids: &[String]) -> std::io::Result<Vec<String>> {
    if ids.len() > MAX_INDEX_MEMBERS {
        return Err(invalid_index_manifest(
            "ANN member set exceeds its safety bound",
        ));
    }
    if ids.iter().any(|id| id.len() > MAX_MEMBER_ID_BYTES) {
        return Err(invalid_index_manifest(
            "ANN member id exceeds its safety bound",
        ));
    }
    let mut members = ids.to_vec();
    members.sort_unstable();
    if members.windows(2).any(|window| window[0] == window[1]) {
        return Err(invalid_index_manifest(
            "ANN member set contains a duplicate id",
        ));
    }
    Ok(members)
}

fn encode_index_manifest(manifest: &IndexManifest) -> std::io::Result<Vec<u8>> {
    let payload = rmp_serde::to_vec_named(manifest)
        .map_err(|_| invalid_index_manifest("ANN store manifest serialization failed"))?;
    let total = INDEX_MANIFEST_MAGIC
        .len()
        .checked_add(payload.len())
        .ok_or_else(|| invalid_index_manifest("ANN store manifest size overflow"))?;
    if total > MAX_INDEX_MANIFEST_BYTES {
        return Err(invalid_index_manifest(
            "ANN store manifest exceeds its safety bound",
        ));
    }
    let mut encoded = Vec::with_capacity(total);
    encoded.extend_from_slice(INDEX_MANIFEST_MAGIC);
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn decode_index_manifest(bytes: &[u8]) -> std::io::Result<IndexManifest> {
    if bytes.len() > MAX_INDEX_MANIFEST_BYTES {
        return Err(invalid_index_manifest(
            "ANN store manifest exceeds its safety bound",
        ));
    }
    let payload = bytes
        .strip_prefix(INDEX_MANIFEST_MAGIC)
        .ok_or_else(|| invalid_index_manifest("ANN store manifest format is unsupported"))?;
    // `Deserializer::from_read_ref` (the `ReadRefReader` constructor) has no
    // `position()` accessor; only the `Cursor`-backed constructor tracks bytes
    // consumed, which is what the trailing-bytes check below needs.
    let mut deserializer = rmp_serde::Deserializer::new(std::io::Cursor::new(payload));
    let manifest = IndexManifest::deserialize(&mut deserializer)
        .map_err(|_| invalid_index_manifest("ANN store manifest is invalid"))?;
    if deserializer.position() != payload.len() as u64 {
        return Err(invalid_index_manifest(
            "ANN store manifest contains trailing bytes",
        ));
    }
    validate_manifest(&manifest)?;
    Ok(manifest)
}

fn validate_manifest(manifest: &IndexManifest) -> std::io::Result<()> {
    if manifest.version != 1
        || manifest.dimension == 0
        || manifest.dimension > MAX_MAINTAINED_DIMENSION
    {
        return Err(invalid_index_manifest(
            "ANN store manifest version or dimension is unsupported",
        ));
    }
    canonical_members(&manifest.members)?;
    let Some(space) = manifest.space.as_ref() else {
        return Ok(());
    };
    space.validate().map_err(invalid_index_manifest)?;
    if space.dimensions != manifest.dimension || space.dimensions > MAX_MAINTAINED_DIMENSION {
        return Err(invalid_index_manifest(
            "ANN store manifest space does not match its maintained dimension",
        ));
    }
    Ok(())
}

fn invalid_index_manifest(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
}

/// SIMD-friendly dot product (CONCEPT:EG-KG.compute.lane-chunked-dot-product). Accumulating into a single `f32`
/// serializes the floating-point dependency chain and defeats vectorization; instead
/// we accumulate into 8 independent lanes over `chunks_exact(8)` slices. The fixed
/// length-8 inner loop is bounds-check-free (the compiler proves `len == 8`) and maps
/// to one packed 256-bit AVX2 multiply-add per chunk (under `-C target-cpu=x86-64-v3`,
/// see `.cargo/config.toml`), so a 1024-dim dot product is ~128 vector ops instead of
/// 1024 scalar ones. The tail (< 8 elems) is scalar.
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

/// CONCEPT:EG-KG.compute.cached-row-norm — L2 norm via the SIMD dot kernel (cached per row at insert).
fn l2_norm(v: &[f32]) -> f32 {
    dot_product(v, v).sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_space(model: &str, dimensions: usize) -> EmbeddingSpaceRef {
        EmbeddingSpaceRef::pinned(model, "1", "a".repeat(64), "b".repeat(64), dimensions, true)
            .unwrap()
    }

    #[test]
    fn stamped_search_requires_exact_declared_space() {
        let space = test_space("model", 2);
        let mut store = SemanticStore::new_in_space(space.clone()).unwrap();
        store.add_embedding("a".into(), vec![1.0, 0.0]).unwrap();
        let other = StampedVector::new(test_space("other", 2), vec![1.0, 0.0]).unwrap();
        assert!(matches!(
            store.semantic_search_stamped_filtered(&other, 1, |_| true),
            Err(SemanticQueryError::SpaceMismatch { .. })
        ));
        let exact = StampedVector::new(space, vec![1.0, 0.0]).unwrap();
        assert_eq!(
            store
                .semantic_search_stamped_filtered(&exact, 1, |_| true)
                .unwrap()[0]
                .0,
            "a"
        );
    }

    #[test]
    fn declared_space_survives_serde_and_rejects_wrong_width_insert() {
        let space = test_space("model", 2);
        let store = SemanticStore::new_in_space(space.clone()).unwrap();
        let bytes = rmp_serde::to_vec_named(&store).unwrap();
        let mut restored: SemanticStore = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(restored.space(), Some(&space));
        assert_eq!(
            restored.add_embedding("wrong".into(), vec![1.0; 3]),
            Err(EmbeddingDimensionError::Mismatch {
                expected: 2,
                received: 3,
            })
        );
    }

    #[test]
    fn raw_queries_reject_width_and_non_finite_values() {
        let mut store = SemanticStore::new();
        store.add_embedding("a".into(), vec![1.0, 0.0]).unwrap();
        assert!(store.semantic_search(&[1.0], 1).is_empty());
        assert!(store.semantic_search(&[1.0, f32::NAN], 1).is_empty());
        assert!(store
            .semantic_search_filtered(&[1.0, f32::INFINITY], 1, |_| true)
            .is_empty());
    }

    #[test]
    fn serde_rejects_unknown_fields_and_non_finite_rows() {
        let unknown = rmp_serde::to_vec_named(&serde_json::json!({
            "dim": 2,
            "ids": ["a"],
            "data": [1.0, 0.0],
            "unknown": true
        }))
        .unwrap();
        assert!(rmp_serde::from_slice::<SemanticStore>(&unknown).is_err());

        #[derive(serde::Serialize)]
        struct Raw {
            dim: usize,
            ids: Vec<String>,
            data: Vec<f32>,
        }
        let poisoned = rmp_serde::to_vec_named(&Raw {
            dim: 2,
            ids: vec!["a".to_string()],
            data: vec![1.0, f32::NAN],
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<SemanticStore>(&poisoned).is_err());
    }

    #[test]
    fn index_manifest_is_closed_and_trailing_bytes_are_rejected() {
        let manifest = IndexManifest {
            version: 1,
            dimension: 2,
            members: vec!["a".to_string()],
            space: None,
        };
        let encoded = encode_index_manifest(&manifest).unwrap();
        assert_eq!(
            decode_index_manifest(&encoded).unwrap().members,
            vec!["a".to_string()]
        );

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(decode_index_manifest(&trailing).is_err());

        let mut unknown = INDEX_MANIFEST_MAGIC.to_vec();
        unknown.extend_from_slice(
            &rmp_serde::to_vec_named(&serde_json::json!({
                "version": 1,
                "dimension": 2,
                "members": ["a"],
                "space": null,
                "unknown": true
            }))
            .unwrap(),
        );
        assert!(decode_index_manifest(&unknown).is_err());
    }

    /// Plain scalar reference for the SIMD-kernel A/B check.
    fn scalar_dot(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
    }

    #[test]
    fn simd_dot_product_matches_scalar_within_epsilon() {
        // CONCEPT:EG-KG.compute.lane-chunked-dot-product — the 8-lane chunked dot product must equal the scalar one
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
    fn remove_embedding_drops_from_arena_and_search() {
        // CONCEPT:EG-KG.storage.incremental-ann — a removed embedding must vanish from both the
        // arena (swap-remove keeps it dense) and search results, with survivors intact.
        let mut store = SemanticStore::new();
        for i in 0..50 {
            let mut v = vec![0.0f32; 8];
            v[i % 8] = 1.0;
            v[(i + 1) % 8] = (i as f32) / 50.0;
            store.add_embedding(format!("n{i}"), v).unwrap();
        }
        assert_eq!(store.len(), 50);

        assert!(store.remove_embedding("n7"));
        assert!(!store.remove_embedding("n7"), "second remove is a no-op");
        assert_eq!(store.len(), 49);
        assert!(store.get_embedding("n7").is_none());
        // A survivor's vector is still exactly retrievable (swap-remove preserved it).
        assert!(store.get_embedding("n8").is_some());

        // Arena stays dense: data.len() == ids.len() * dim (invariant for correct rows).
        let q = store.get_embedding("n8").unwrap();
        let hits = store.semantic_search(&q, 5);
        assert!(hits.iter().any(|(id, _)| id == "n8"));
        assert!(
            hits.iter().all(|(id, _)| id != "n7"),
            "removed id must never surface: {hits:?}"
        );
    }

    /// BUG-007 (P0, data-loss class): a write with a mismatched embedding dimension
    /// used to clear the ENTIRE arena — `EmbeddingArena::insert` cleared
    /// `data`/`ids`/`id_to_row`/`norms` and re-initialized at the new width on any
    /// dimension change, destroying every unrelated embedding on one malformed
    /// write. This test populates a known 10-vector corpus, hashes it, issues a
    /// mismatched-dimension write, and proves BOTH that the write is rejected with a
    /// typed error AND that the arena is byte-for-byte unchanged (not merely
    /// unchanged in count) — via an exact snapshot comparison AND an independent
    /// content hash of that snapshot.
    #[test]
    fn mismatched_dimension_insert_does_not_erase_corpus() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn digest(snapshot: &[(String, Vec<f32>)]) -> u64 {
            let mut hasher = DefaultHasher::new();
            for (id, vector) in snapshot {
                id.hash(&mut hasher);
                for component in vector {
                    component.to_bits().hash(&mut hasher);
                }
            }
            hasher.finish()
        }

        let mut store = SemanticStore::new();
        let known: Vec<(String, Vec<f32>)> = (0..10)
            .map(|i| {
                let mut v = vec![0.0f32; 8];
                v[i % 8] = 1.0;
                (format!("n{i}"), v)
            })
            .collect();
        for (id, v) in &known {
            store.add_embedding(id.clone(), v.clone()).unwrap();
        }
        assert_eq!(store.len(), 10, "setup: 10 known vectors resident");

        let before_snapshot = store.embeddings_snapshot();
        let before_digest = digest(&before_snapshot);

        // A hostile write with a DIFFERENT dimension (4 instead of 8) arrives.
        let result = store.add_embedding("intruder".into(), vec![1.0, 2.0, 3.0, 4.0]);

        // It must be REJECTED with a typed error identifying expected/received...
        assert_eq!(
            result,
            Err(EmbeddingDimensionError::Mismatch {
                expected: 8,
                received: 4
            }),
            "a mismatched-dimension write must be rejected with a typed error, not \
             silently applied or silently dropped"
        );
        assert!(
            store.get_embedding("intruder").is_none(),
            "the rejected vector must not have been inserted either"
        );

        // ...and the pre-existing corpus must be PROVABLY untouched: same length,
        // same digest, and byte-for-byte identical snapshot — not merely the same
        // count (a count-only check would miss a corpus that was cleared and
        // partially refilled to the same size).
        let after_snapshot = store.embeddings_snapshot();
        let after_digest = digest(&after_snapshot);
        assert_eq!(
            store.len(),
            10,
            "existing corpus must not be cleared (BUG-007)"
        );
        assert_eq!(
            before_digest, after_digest,
            "arena content digest must be identical after a rejected write"
        );
        assert_eq!(
            before_snapshot, after_snapshot,
            "arena content must be byte-for-byte identical after a rejected write"
        );
        for (id, v) in &known {
            assert_eq!(
                store.get_embedding(id).as_deref(),
                Some(v.as_slice()),
                "existing vector for {id} must survive a rejected mismatched write"
            );
        }
    }

    /// GOC-08: a NaN/Inf component passes every LENGTH check a mismatched-dimension
    /// write would fail, so before this guard it would reach the arena/index fully
    /// intact — its cached L2 norm and every downstream cosine score become `NaN`,
    /// silently corrupting rankings rather than erroring (see
    /// `EmbeddingDimensionError::NonFinite`'s doc comment). This is the BUG-007
    /// erasure test's sibling for the *content*-validity axis rather than the
    /// *length* axis: same digest + byte-for-byte proof that a rejected write
    /// leaves the resident corpus completely untouched.
    #[test]
    fn non_finite_embedding_insert_does_not_erase_corpus() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn digest(snapshot: &[(String, Vec<f32>)]) -> u64 {
            let mut hasher = DefaultHasher::new();
            for (id, vector) in snapshot {
                id.hash(&mut hasher);
                for component in vector {
                    component.to_bits().hash(&mut hasher);
                }
            }
            hasher.finish()
        }

        let mut store = SemanticStore::new();
        let known: Vec<(String, Vec<f32>)> = (0..10)
            .map(|i| {
                let mut v = vec![0.0f32; 8];
                v[i % 8] = 1.0;
                (format!("n{i}"), v)
            })
            .collect();
        for (id, v) in &known {
            store.add_embedding(id.clone(), v.clone()).unwrap();
        }
        assert_eq!(store.len(), 10, "setup: 10 known vectors resident");

        let before_snapshot = store.embeddings_snapshot();
        let before_digest = digest(&before_snapshot);

        // Same width as the store (8) so ONLY the non-finite check can catch it —
        // proves this is a distinct guard, not a length check in disguise.
        let mut poisoned = vec![0.0f32; 8];
        poisoned[3] = f32::NAN;
        let result = store.add_embedding("intruder".into(), poisoned);
        assert_eq!(
            result,
            Err(EmbeddingDimensionError::NonFinite { index: 3 }),
            "a NaN component must be rejected with a typed error naming its index"
        );
        assert!(
            store.get_embedding("intruder").is_none(),
            "the rejected vector must not have been inserted either"
        );

        let after_snapshot = store.embeddings_snapshot();
        let after_digest = digest(&after_snapshot);
        assert_eq!(store.len(), 10, "existing corpus must not be cleared");
        assert_eq!(
            before_digest, after_digest,
            "arena content digest must be identical after a rejected write"
        );
        assert_eq!(
            before_snapshot, after_snapshot,
            "arena content must be byte-for-byte identical after a rejected write"
        );

        // +/-infinity are rejected the same way.
        let mut plus_inf = vec![0.0f32; 8];
        plus_inf[0] = f32::INFINITY;
        assert_eq!(
            store.add_embedding("inf-intruder".into(), plus_inf),
            Err(EmbeddingDimensionError::NonFinite { index: 0 })
        );
        let mut minus_inf = vec![0.0f32; 8];
        minus_inf[7] = f32::NEG_INFINITY;
        assert_eq!(
            store.add_embedding("neg-inf-intruder".into(), minus_inf),
            Err(EmbeddingDimensionError::NonFinite { index: 7 })
        );
        assert_eq!(
            store.len(),
            10,
            "existing corpus must survive both infinity rejections too"
        );
    }

    /// Neighbouring hostile input: a zero-length embedding must be rejected with a
    /// typed error, never silently ignored and never accepted as establishing a
    /// legitimate zero-width dimension.
    #[test]
    fn zero_dimension_embedding_is_rejected() {
        let mut store = SemanticStore::new();
        assert_eq!(
            store.add_embedding("a".into(), vec![]),
            Err(EmbeddingDimensionError::Empty)
        );
        assert!(
            store.is_empty(),
            "store must remain empty after a rejected empty vector"
        );

        // Also rejected once the store already has an established dimension.
        store
            .add_embedding("b".into(), vec![1.0, 0.0, 0.0])
            .unwrap();
        assert_eq!(
            store.add_embedding("c".into(), vec![]),
            Err(EmbeddingDimensionError::Empty)
        );
        assert_eq!(
            store.len(),
            1,
            "existing vector must survive a rejected empty write"
        );
    }

    /// Neighbouring hostile input: an embedding far beyond any realistic model width
    /// must be rejected with a typed error before any allocation, rather than being
    /// accepted and OOM-risking the arena.
    #[test]
    fn oversized_dimension_embedding_is_rejected() {
        let mut store = SemanticStore::new();
        store
            .add_embedding("a".into(), vec![1.0, 0.0, 0.0])
            .unwrap();
        let oversized = vec![0.0f32; MAX_GENERIC_DIMENSION + 1];
        assert_eq!(
            store.add_embedding("intruder".into(), oversized),
            Err(EmbeddingDimensionError::Oversized {
                received: MAX_GENERIC_DIMENSION + 1,
                max: MAX_GENERIC_DIMENSION,
            })
        );
        assert_eq!(
            store.len(),
            1,
            "existing vector must survive a rejected oversized write"
        );
    }

    #[test]
    fn remove_embedding_index_path_tombstones() {
        // Above the ANN build threshold + warmed → the resident IVF-PQ index serves
        // search; removing a node must tombstone its row so kNN never returns it, with
        // no full rebuild.
        let dim = 32;
        let n = ANN_BUILD_THRESHOLD + 200;
        let mut store = SemanticStore::new();
        let mut seed = 4242u64;
        let mut rng = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            ((seed >> 33) as f32 / (1u64 << 31) as f32) - 0.5
        };
        let mut target = vec![0.0f32; dim];
        for i in 0..n {
            let v: Vec<f32> = (0..dim).map(|_| rng()).collect();
            if i == 123 {
                target = v.clone();
            }
            store.add_embedding(format!("n{i}"), v).unwrap();
        }
        store.warm("test");
        assert!(store.is_ready(), "index should warm above threshold");

        // n123 is its own nearest neighbor before removal.
        let before = store.semantic_search(&target, 5);
        assert!(before.iter().any(|(id, _)| id == "n123"));

        assert!(store.remove_embedding("n123"));
        let after = store.semantic_search(&target, 5);
        assert!(
            after.iter().all(|(id, _)| id != "n123"),
            "tombstoned node must not surface via the ANN index: {after:?}"
        );
    }

    /// W0.4 — a warm already `STATE_WARMING` must make `warm()` back off
    /// immediately (no build, no state change) instead of blocking on the index
    /// write lock, so a second trigger for the same graph (the on-write hook
    /// racing the periodic re-check sweep) never wastes a `spawn_blocking` slot
    /// for the whole build duration.
    #[test]
    fn warm_backs_off_when_already_warming() {
        let dim = 16;
        let n = ANN_BUILD_THRESHOLD + 50;
        let mut store = SemanticStore::new();
        for i in 0..n {
            let mut v = vec![0.0f32; dim];
            v[i % dim] = 1.0;
            store.add_embedding(format!("n{i}"), v).unwrap();
        }
        assert!(!store.is_ready(), "not warmed yet");

        // Simulate a build already in flight (as `ensure_index`'s claim leaves it
        // for the whole build duration).
        store.state.store(STATE_WARMING, Ordering::Release);
        store.warm("test");
        assert!(
            !store.is_ready(),
            "a racing warm() must not build while another is in flight"
        );
        assert!(
            store.index.read().is_none(),
            "the index must be untouched by the backed-off caller"
        );
        assert!(
            store.is_warming(),
            "the in-flight claim must survive untouched"
        );

        // Once the in-flight build "finishes" (state reset), a fresh warm() must
        // still succeed normally — the guard never permanently wedges the store.
        store.state.store(STATE_COLD, Ordering::Release);
        store.warm("test");
        assert!(
            store.is_ready(),
            "warm() must build once nothing is in flight"
        );
        assert_eq!(store.len(), n);
    }

    /// W0.4 — two REAL concurrent `warm()` calls on the same store (the on-write
    /// trigger racing the periodic re-check sweep in practice) must leave the
    /// store correctly warmed, with no panics, corruption, or dropped rows —
    /// exactly one of them performs the build, the other observes `STATE_WARMING`
    /// via the atomic claim in `ensure_index` and returns immediately.
    #[test]
    fn concurrent_warm_calls_are_race_free() {
        let dim = 16;
        let n = ANN_BUILD_THRESHOLD + 50;
        let mut store = SemanticStore::new();
        for i in 0..n {
            let mut v = vec![0.0f32; dim];
            v[i % dim] = 1.0;
            store.add_embedding(format!("n{i}"), v).unwrap();
        }
        let store = std::sync::Arc::new(store);
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let store = store.clone();
                std::thread::spawn(move || store.warm("concurrent-test"))
            })
            .collect();
        for h in handles {
            h.join()
                .expect("warm() must not panic under concurrent callers");
        }
        assert!(store.is_ready(), "the index must end up warmed");
        assert_eq!(store.len(), n, "no rows lost across the concurrent warms");
        let q = store.get_embedding("n0").unwrap();
        let hits = store.semantic_search(&q, 5);
        assert!(hits.iter().any(|(id, _)| id == "n0"));
    }

    #[test]
    fn parallel_brute_force_matches_sequential_topk() {
        // CONCEPT:EG-KG.compute.lane-chunked-dot-product/EG-015 — the rayon-parallel contiguous brute force +
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
            store.add_embedding(format!("n{i}"), v.clone()).unwrap();
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
        store
            .add_embedding("a".into(), vec![1.0, 0.0, 0.0])
            .unwrap();
        store
            .add_embedding("b".into(), vec![0.0, 1.0, 0.0])
            .unwrap();
        store
            .add_embedding("c".into(), vec![0.9, 0.1, 0.0])
            .unwrap();
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
        // CONCEPT:EG-KG.storage.arena-row-append — overwriting an id reuses its row (the arena stays dense:
        // data.len() == rows*dim, no tombstones) and the new vector wins in search.
        let mut store = SemanticStore::new();
        for i in 0..10 {
            store
                .add_embedding(format!("n{i}"), vec![i as f32, 0.0, 0.0])
                .unwrap();
        }
        assert_eq!(store.arena.len(), 10);
        assert_eq!(store.arena.data.len(), 10 * 3);
        // Overwrite n3 to point a brand-new direction.
        store
            .add_embedding("n3".into(), vec![0.0, 0.0, 99.0])
            .unwrap();
        assert_eq!(store.arena.len(), 10, "overwrite must not add a row");
        assert_eq!(
            store.arena.data.len(),
            10 * 3,
            "arena stays dense on overwrite"
        );
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
            store.add_embedding(format!("node_{i}"), emb).unwrap();
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
    fn serde_rejects_retired_or_inconsistent_arena_shapes() {
        #[derive(serde::Serialize)]
        struct RetiredMapShape {
            embeddings: HashMap<String, Vec<f32>>,
        }
        let retired = rmp_serde::to_vec_named(&RetiredMapShape {
            embeddings: HashMap::from([("n".to_string(), vec![1.0, 0.0])]),
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<SemanticStore>(&retired).is_err());

        let inconsistent = rmp_serde::to_vec_named(&serde_json::json!({
            "dim": 3,
            "ids": ["n"],
            "data": [1.0, 0.0]
        }))
        .unwrap();
        assert!(rmp_serde::from_slice::<SemanticStore>(&inconsistent).is_err());
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
            store.add_embedding(format!("n{i}"), v.clone()).unwrap();
            vecs.push((format!("n{i}"), v));
        }
        // Build the index OFF the query path (warm), then query the ANN path.
        store.warm("test");
        assert!(store.is_ready(), "warm must make the store Ready");
        let q = &vecs[100].1;
        let before = store.semantic_search(q, 10);
        assert!(!before.is_empty());
        assert_eq!(before[0].0, "n100", "self should be top-1");

        let image = store.export_generation().unwrap();

        // Fresh store with the same embeddings (snapshot path) + no-rebuild
        // activation from the generation image.
        let reloaded = SemanticStore {
            arena: store.arena.clone(),
            space: store.space.clone(),
            index: RwLock::new(None),
            built_len: RwLock::new(0),
            state: AtomicU8::new(STATE_COLD),
        };
        reloaded.adopt_generation(&image).unwrap();
        assert!(
            reloaded.is_ready(),
            "no-rebuild activation must leave the store Ready"
        );
        let after = reloaded.semantic_search(q, 10);
        assert_eq!(
            before.iter().map(|r| r.0.clone()).collect::<Vec<_>>(),
            after.iter().map(|r| r.0.clone()).collect::<Vec<_>>(),
            "no-rebuild activation must match"
        );
    }

    #[test]
    fn search_never_builds_inline_cold_start_is_brute_force() {
        // CONCEPT:EG-KG.storage.semantic-index-directory — the cold-start fix. A large store loaded fresh (index
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
            store.add_embedding(format!("n{i}"), v).unwrap();
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
