// CONCEPT:EG-KG.compute.semantic-embedding-store-hnsw — Semantic Embedding Store with HNSW Index (default backend).
//
// High-performance embedding store using the in-tree eg-ann HNSW index for
// O(log n) approximate nearest-neighbor search. Falls back to
// brute-force cosine for small collections (< 32 embeddings).
//
// This is the DEFAULT `SemanticStore`. Under the `ann` feature it is replaced by
// the eg-ann IVF-PQ+OPQ+SQ8-refine backend (`semantic_store_ann.rs`,
// CONCEPT:EG-KG.sharding.semantic-embedding-store-backed), which reopens a persisted index without rebuilding from raw
// vectors. `compute::semantic` re-exports whichever backend is active.

use super::{check_embedding_dimension, EmbeddingDimensionError, SemanticQueryError};
use eg_ann::{HnswIndex as NativeHnswIndex, Metric};
use eg_types::{EmbeddingSpaceRef, StampedVector};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Generic native exact/vector surfaces accept this width.  A resident HNSW
/// artifact uses the narrower maintained-ANN ceiling below; wider vectors stay
/// on the exact path and never cross the eg-ann constructor boundary.
const MAX_GENERIC_DIMENSION: usize = eg_types::MAX_EMBEDDING_DIMENSIONS;
const MAX_MAINTAINED_DIMENSION: usize = eg_types::MAX_MAINTAINED_ANN_DIMENSIONS;

/// Maximum number of connections per layer in the HNSW graph.
const HNSW_MAX_NB_CONN: usize = 16;
/// Expansion factor during search (higher = more accurate, slower).
const HNSW_EF_SEARCH: usize = 64;
/// Beam width for a FILTERED search (CONCEPT:EG-KG.retrieval.hybrid-metadata-prefilter). Wider than
/// `HNSW_EF_SEARCH` because a selective predicate forces the walk to route through
/// disallowed "bridge" nodes before it collects a full beam of allowed results; the
/// explored count is `~ef/selectivity`, still a constant independent of the store
/// size (so latency stays sub-linear in |V|). Sized to hold filtered recall@10 ≥ 0.9
/// down to ~1% selectivity (validated by the eg-ann `filtered_ann` bench).
const HNSW_EF_SEARCH_FILTERED: usize = 128;
/// Expansion factor while inserting points.
const HNSW_EF_CONSTRUCTION: usize = 200;
/// Stable seed keeps graph construction reproducible across nodes and reloads.
const HNSW_SEED: u64 = 0x4550_4953_5445_4D49;
/// Threshold below which we use brute-force (HNSW overhead not worth it).
const BRUTE_FORCE_THRESHOLD: usize = 32;
/// Rebuild the index once tombstoned (superseded) points exceed this percent of
/// total inserts. The additive HNSW cannot remove a point, so an overwrite leaves the old
/// vector in the graph; rather than rebuild on every overwrite (the old O(n)
/// thrash), we tombstone + incrementally insert and rebuild only past this ratio,
/// which also bounds the recall drag from dead neighbors polluting the traversal.
const COMPACT_TOMBSTONE_PCT: usize = 30;

/// The lazily-built, incrementally-maintained HNSW index plus its idx→id map.
/// Held behind a `RwLock` inside `SemanticStore` for interior mutability so a
/// `&self` search can rebuild it once after load. NEVER serialized — reconstructed
/// from `embeddings`. (Phase C-D — HNSW-incremental; Phase B3 — tombstones.)
struct HnswIndex {
    /// The live index. `None` until first built / after a compaction invalidates
    /// it. `'static` is sound because `insert` copies the data into an owned `Vec`
    hnsw: Option<NativeHnswIndex>,
    /// HNSW internal id → node id. The internal id is the insertion ordinal;
    /// append-only, so it includes superseded (tombstoned) slots.
    order: Vec<String>,
    /// node id → its CURRENT live internal id (the latest insert for that node).
    id_to_internal: HashMap<String, usize>,
    /// Superseded internal ids (an overwrite re-inserts the node at a new id and
    /// tombstones the old one). Filtered out of search results.
    tombstones: std::collections::HashSet<usize>,
    /// Number of LIVE embeddings the current index reflects (staleness check).
    built_len: usize,
    /// Embedding dimensionality the index was built for.
    dim: usize,
}

impl HnswIndex {
    fn empty() -> Self {
        Self {
            hnsw: None,
            order: Vec::new(),
            id_to_internal: HashMap::new(),
            tombstones: std::collections::HashSet::new(),
            built_len: 0,
            dim: 0,
        }
    }
}

pub struct SemanticStore {
    embeddings: HashMap<String, Vec<f32>>,
    /// Exact model/preprocessing coordinate space for model-produced queries.
    /// `None` preserves legacy raw-vector stores without inventing an identity.
    space: Option<EmbeddingSpaceRef>,
    /// Incrementally-maintained HNSW index (Phase C-D). Skipped on (de)serialize
    /// and rebuilt lazily from `embeddings` on the first search after load — which
    /// also closes the pre-existing post-restore gap where the index metadata came
    /// back empty and HNSW search silently returned nothing.
    index: RwLock<HnswIndex>,
}

mod semantic_hnsw_index;
mod semantic_hnsw_lifecycle;
mod semantic_hnsw_mutation;
mod semantic_hnsw_persistence;
mod semantic_hnsw_query;

// The index is interior, non-Clone, non-Serialize → hand-roll the derives so the
// on-disk format is UNCHANGED (only `embeddings` is persisted, exactly as before).
impl Clone for SemanticStore {
    fn clone(&self) -> Self {
        Self {
            embeddings: self.embeddings.clone(),
            space: self.space.clone(),
            index: RwLock::new(HnswIndex::empty()),
        }
    }
}

impl std::fmt::Debug for SemanticStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SemanticStore")
            .field("embeddings", &self.embeddings.len())
            .field("space", &self.space.as_ref().map(|space| &space.digest))
            .finish()
    }
}

/// Apply the generic native width ceiling before delegating to the shared
/// legacy validator.  The shared validator intentionally remains wider for
/// numerical callers; this store is a model/vector boundary and must not pass
/// a >16k vector to either exact storage or the ANN constructor.
fn check_embedding_dimension_bounded(
    embedding: &[f32],
    store_dim: usize,
    maximum: usize,
) -> Result<usize, EmbeddingDimensionError> {
    if embedding.len() > maximum {
        return Err(EmbeddingDimensionError::Oversized {
            received: embedding.len(),
            max: maximum,
        });
    }
    check_embedding_dimension(embedding, store_dim)
}

/// Validate a raw query before any backend call.  Both native ANN libraries
/// assert on width; returning no hits from the legacy `Vec` API keeps malformed
/// caller input a rejection instead of allowing `zip` truncation or a panic.
fn valid_query(query: &[f32], store_dim: usize) -> bool {
    check_embedding_dimension_bounded(query, store_dim, MAX_GENERIC_DIMENSION).is_ok()
}

fn validate_persisted_embeddings(
    embeddings: &HashMap<String, Vec<f32>>,
    space: Option<&EmbeddingSpaceRef>,
) -> Result<(), String> {
    let expected = space.map(|value| value.dimensions).unwrap_or(0);
    let mut established = expected;
    for embedding in embeddings.values() {
        let next = check_embedding_dimension_bounded(embedding, established, MAX_GENERIC_DIMENSION)
            .map_err(|error| error.to_string())?;
        if established == 0 {
            established = next;
        }
    }
    if let Some(space) = space {
        if established != 0 && established != space.dimensions {
            return Err(format!(
                "semantic store space declares {} dimensions but persisted rows carry {established}",
                space.dimensions
            ));
        }
    }
    Ok(())
}

/// Pure-Rust dot product.
fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

/// Retain the exact best `limit` cosine hits without fully sorting the small-set
/// fallback. Selection is expected O(N), followed by O(limit log limit) ordering
/// of the returned prefix. Ids break equal-score ties and NaN is always last.
fn truncate_highest_similarity(scores: &mut Vec<(String, f32)>, limit: usize) {
    if limit == 0 {
        scores.clear();
        return;
    }
    if scores.len() > limit {
        scores.select_nth_unstable_by(limit, similarity_cmp);
        scores.truncate(limit);
    }
    scores.sort_unstable_by(similarity_cmp);
}

fn similarity_cmp(left: &(String, f32), right: &(String, f32)) -> std::cmp::Ordering {
    let score_order = match (left.1.is_nan(), right.1.is_nan()) {
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (true, true) => left.1.to_bits().cmp(&right.1.to_bits()),
        (false, false) => right.1.total_cmp(&left.1),
    };
    score_order.then_with(|| left.0.cmp(&right.0))
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
    fn serde_rejects_unknown_fields_and_mixed_width_rows() {
        let unknown = rmp_serde::to_vec_named(&serde_json::json!({
            "embeddings": {"a": [1.0, 0.0]},
            "unknown": true
        }))
        .unwrap();
        assert!(rmp_serde::from_slice::<SemanticStore>(&unknown).is_err());

        let mixed = rmp_serde::to_vec_named(&serde_json::json!({
            "embeddings": {"a": [1.0, 0.0], "b": [1.0, 0.0, 0.0]}
        }))
        .unwrap();
        assert!(rmp_serde::from_slice::<SemanticStore>(&mixed).is_err());

        #[derive(serde::Serialize)]
        struct Raw {
            embeddings: HashMap<String, Vec<f32>>,
        }
        let poisoned = rmp_serde::to_vec_named(&Raw {
            embeddings: HashMap::from([("a".to_string(), vec![1.0, f32::NAN])]),
        })
        .unwrap();
        assert!(rmp_serde::from_slice::<SemanticStore>(&poisoned).is_err());
    }

    #[test]
    fn partial_brute_force_selection_matches_total_full_sort() {
        let input: Vec<(String, f32)> = (0..257)
            .map(|index| (format!("node-{index:03}"), ((index * 37) % 29) as f32))
            .chain(std::iter::once(("malformed".into(), f32::NAN)))
            .collect();
        let mut expected = input.clone();
        expected.sort_unstable_by(similarity_cmp);
        expected.truncate(13);

        let mut selected = input;
        truncate_highest_similarity(&mut selected, 13);

        assert_eq!(selected, expected);
    }

    #[test]
    fn test_add_and_search_brute_force() {
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

    /// BUG-007 (P0, data-loss class) — this backend's analog of the arena-erasure
    /// bug fixed in `semantic_store_ann.rs`. This backend never CLEARED its
    /// `embeddings` map on a mismatched write, but it also never REJECTED one: a
    /// wrong-width vector was inserted right alongside the existing corpus (only
    /// the resident HNSW index quietly dropped it from search), silently violating
    /// this backend's own "never mixed-width" invariant. Proves the write is now
    /// rejected with a typed error and the corpus is byte-for-byte unchanged.
    #[test]
    fn mismatched_dimension_insert_does_not_corrupt_corpus() {
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
        let mut before = store.embeddings_snapshot();
        before.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let result = store.add_embedding("intruder".into(), vec![1.0, 2.0, 3.0, 4.0]);
        assert_eq!(
            result,
            Err(EmbeddingDimensionError::Mismatch {
                expected: 8,
                received: 4
            })
        );
        assert!(store.get_embedding("intruder").is_none());

        let mut after = store.embeddings_snapshot();
        after.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            store.len(),
            10,
            "existing corpus must not be touched (BUG-007)"
        );
        assert_eq!(
            before, after,
            "embeddings map must be byte-for-byte identical after a rejected write"
        );
    }

    /// GOC-08, this backend's analog of `semantic_store_ann`'s
    /// `non_finite_embedding_insert_does_not_erase_corpus`: a NaN/Inf component
    /// matches the store's established width byte-for-byte, so only a dedicated
    /// finite-value scan (not the length check) can catch it. Before this guard it
    /// would have landed in `embeddings` intact, same as the pre-fix mismatched-width
    /// case this backend used to silently accept.
    #[test]
    fn non_finite_embedding_insert_does_not_corrupt_corpus() {
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
        let mut before = store.embeddings_snapshot();
        before.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let mut poisoned = vec![0.0f32; 8];
        poisoned[5] = f32::NAN;
        let result = store.add_embedding("intruder".into(), poisoned);
        assert_eq!(result, Err(EmbeddingDimensionError::NonFinite { index: 5 }));
        assert!(store.get_embedding("intruder").is_none());

        let mut after = store.embeddings_snapshot();
        after.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(store.len(), 10, "existing corpus must not be touched");
        assert_eq!(
            before, after,
            "embeddings map must be byte-for-byte identical after a rejected write"
        );
    }

    /// Neighbouring hostile input: a zero-length embedding must be rejected.
    #[test]
    fn zero_dimension_embedding_is_rejected() {
        let mut store = SemanticStore::new();
        assert_eq!(
            store.add_embedding("a".into(), vec![]),
            Err(EmbeddingDimensionError::Empty)
        );
        assert!(store.is_empty());
    }

    /// Neighbouring hostile input: an embedding beyond the maximum dimension must
    /// be rejected before it is ever inserted.
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
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_hnsw_search_large_collection() {
        let mut store = SemanticStore::new();
        // Insert enough embeddings to trigger HNSW path
        for i in 0..50 {
            let mut emb = vec![0.0f32; 8];
            emb[i % 8] = 1.0;
            emb[(i + 1) % 8] = 0.5;
            store.add_embedding(format!("node_{}", i), emb).unwrap();
        }

        let query = vec![1.0, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let results = store.semantic_search(&query, 5);
        assert!(!results.is_empty());
        assert!(results.len() <= 5);
    }

    #[test]
    fn test_empty_store() {
        let store = SemanticStore::new();
        let results = store.semantic_search(&[1.0, 0.0], 5);
        assert!(results.is_empty());
    }

    #[test]
    fn hnsw_survives_serde_roundtrip() {
        // Regression for the PRE-EXISTING post-restore gap (Phase C-D): the index
        // metadata was #[serde(skip)] and never rebuilt, so HNSW search returned
        // NOTHING after a checkpoint reload. The index is now rebuilt lazily from
        // embeddings, so search works after a serialize/deserialize round-trip.
        let mut store = SemanticStore::new();
        for i in 0..50 {
            let mut emb = vec![0.0f32; 8];
            emb[i % 8] = 1.0;
            emb[(i + 1) % 8] = 0.5;
            store.add_embedding(format!("node_{}", i), emb).unwrap();
        }
        let bytes = rmp_serde::to_vec_named(&store).unwrap();
        let restored: SemanticStore = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(restored.len(), 50);

        let query = vec![1.0, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let results = restored.semantic_search(&query, 5);
        assert!(
            !results.is_empty(),
            "HNSW search must work after restore (index rebuilt lazily)"
        );
        assert!(results.len() <= 5);
    }

    #[test]
    fn overwrite_tombstones_reflect_latest_and_compact() {
        // Phase B3: an embedding overwrite must (a) be reflected in search (the
        // NEW vector wins, the stale one is tombstoned) and (b) NOT trigger a full
        // rebuild every time — many overwrites stay correct as compaction kicks in.
        let mut store = SemanticStore::new();
        for i in 0..40 {
            let mut emb = vec![0.1f32; 8];
            emb[i % 8] = (i as f32) / 40.0;
            store.add_embedding(format!("n{i}"), emb).unwrap();
        }
        let _ = store.semantic_search(&[0.1; 8], 5); // build the index

        // Overwrite n0 toward a brand-new direction → the NEW n0 is searchable.
        // (HNSW is approximate, so assert top-k membership, not exact rank.)
        let newdir = vec![5.0f32; 8];
        store.add_embedding("n0".into(), newdir.clone()).unwrap();

        let res = store.semantic_search(&newdir, 10);
        assert!(
            res.iter().any(|(id, _)| id == "n0"),
            "overwrite must be reflected in search: {res:?}"
        );

        // Hammer one node with overwrites to exceed the compaction ratio; results
        // must stay correct (latest embedding searchable) and free of dupes.
        for k in 0..40 {
            store
                .add_embedding("n1".into(), vec![k as f32 + 10.0; 8])
                .unwrap();
        }
        let target = vec![49.0f32; 8];
        let res = store.semantic_search(&target, 10);
        assert!(
            res.iter().any(|(id, _)| id == "n1"),
            "latest overwrite must be searchable after compaction: {res:?}"
        );
        // A tombstoned id must never surface as a duplicate of its live node.
        let mut ids: Vec<&String> = res.iter().map(|(id, _)| id).collect();
        let before = ids.len();
        ids.sort();
        ids.dedup();
        assert_eq!(before, ids.len(), "tombstoned dupes must not appear");
    }

    #[test]
    fn remove_embedding_drops_from_store_and_search() {
        // CONCEPT:EG-KG.storage.incremental-ann — brute-force regime: a removed embedding
        // vanishes from the store and from search; survivors remain.
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

        assert_eq!(store.len(), 3);

        assert!(store.remove_embedding("a"));
        assert!(!store.remove_embedding("a"), "second remove is a no-op");
        assert_eq!(store.len(), 2);
        assert!(store.get_embedding("a").is_none());

        let hits = store.semantic_search(&[1.0, 0.0, 0.0], 3);
        assert!(
            hits.iter().all(|(id, _)| id != "a"),
            "removed id must never surface: {hits:?}"
        );
    }

    #[test]
    fn remove_embedding_tombstones_in_built_index() {
        // Above the HNSW threshold + built: removing a node tombstones its internal
        // id so search filters it, with no full rebuild.
        let mut store = SemanticStore::new();
        for i in 0..40 {
            let mut emb = vec![0.1f32; 8];
            emb[i % 8] = (i as f32) / 40.0;
            store.add_embedding(format!("n{i}"), emb).unwrap();
        }
        let _ = store.semantic_search(&[0.1; 8], 5); // build the index

        let target = store.get_embedding("n3").unwrap();
        assert!(store
            .semantic_search(&target, 5)
            .iter()
            .any(|(id, _)| id == "n3"));

        assert!(store.remove_embedding("n3"));
        let after = store.semantic_search(&target, 10);
        assert!(
            after.iter().all(|(id, _)| id != "n3"),
            "tombstoned node must not surface: {after:?}"
        );
    }

    #[test]
    fn hnsw_incremental_insert_after_build_is_searchable() {
        // Build the index (cross the HNSW threshold), THEN add a new embedding —
        // it must be found via the incremental-insert path, no full rebuild needed.
        let mut store = SemanticStore::new();
        for i in 0..40 {
            let mut emb = vec![0.1f32; 8];
            emb[i % 8] = (i as f32) / 40.0;
            store.add_embedding(format!("n{}", i), emb).unwrap();
        }
        let _ = store.semantic_search(&[0.1; 8], 5); // triggers the initial build

        let distinct = vec![9.0f32; 8];
        store
            .add_embedding("late".into(), distinct.clone())
            .unwrap(); // incremental insert
        let results = store.semantic_search(&distinct, 3);
        assert!(
            results.iter().any(|(id, _)| id == "late"),
            "an embedding added after the build must be searchable: {:?}",
            results
        );
    }
}
