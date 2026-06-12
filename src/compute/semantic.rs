// CONCEPT:KG-2.22b — Semantic Embedding Store with HNSW Index
//
// High-performance embedding store using the hnsw_rs crate for
// O(log n) approximate nearest-neighbor search. Falls back to
// brute-force cosine for small collections (< 32 embeddings).

use hnsw_rs::hnsw::Hnsw;
use hnsw_rs::prelude::DistCosine;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Maximum number of connections per layer in the HNSW graph.
const HNSW_MAX_NB_CONN: usize = 16;
/// Number of layers in the HNSW index.
const HNSW_NB_LAYER: usize = 16;
/// Expansion factor during search (higher = more accurate, slower).
const HNSW_EF_SEARCH: usize = 64;
/// Threshold below which we use brute-force (HNSW overhead not worth it).
const BRUTE_FORCE_THRESHOLD: usize = 32;

/// The lazily-built, incrementally-maintained HNSW index plus its idx→id map.
/// Held behind a `RwLock` inside `SemanticStore` for interior mutability so a
/// `&self` search can rebuild it once after load. NEVER serialized — reconstructed
/// from `embeddings`. (Phase C-D — HNSW-incremental.)
struct HnswIndex {
    /// The live index. `None` until first built / after an in-place update
    /// invalidates it (hnsw_rs has no point removal, so an embedding overwrite
    /// forces one lazy rebuild). `'static` is sound because `insert` copies the
    /// data into an owned `Vec` (the crate's lifetime is for mmap-backed points).
    hnsw: Option<Hnsw<'static, f32, DistCosine>>,
    /// HNSW internal id → node id. The internal id is the insertion ordinal.
    order: Vec<String>,
    /// Number of embeddings the current index reflects (append staleness check).
    built_len: usize,
    /// Embedding dimensionality the index was built for.
    dim: usize,
}

impl HnswIndex {
    fn empty() -> Self {
        Self {
            hnsw: None,
            order: Vec::new(),
            built_len: 0,
            dim: 0,
        }
    }
}

pub struct SemanticStore {
    embeddings: HashMap<String, Vec<f32>>,
    /// Incrementally-maintained HNSW index (Phase C-D). Skipped on (de)serialize
    /// and rebuilt lazily from `embeddings` on the first search after load — which
    /// also closes the pre-existing post-restore gap where the index metadata came
    /// back empty and HNSW search silently returned nothing.
    index: RwLock<HnswIndex>,
}

// The index is interior, non-Clone, non-Serialize → hand-roll the derives so the
// on-disk format is UNCHANGED (only `embeddings` is persisted, exactly as before).
impl Clone for SemanticStore {
    fn clone(&self) -> Self {
        Self {
            embeddings: self.embeddings.clone(),
            index: RwLock::new(HnswIndex::empty()),
        }
    }
}

impl std::fmt::Debug for SemanticStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SemanticStore")
            .field("embeddings", &self.embeddings.len())
            .finish()
    }
}

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
            index: RwLock::new(HnswIndex::empty()),
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
            index: RwLock::new(HnswIndex::empty()),
        }
    }

    pub fn add_embedding(&mut self, node_id: String, embedding: Vec<f32>) {
        let is_update = self.embeddings.contains_key(&node_id);
        self.embeddings.insert(node_id.clone(), embedding.clone());
        let len = self.embeddings.len();

        let mut idx = self.index.write();
        if is_update {
            // hnsw_rs cannot remove/replace a point → invalidate; the next search
            // rebuilds a clean index. (Pure-append ingest never hits this path.)
            idx.hnsw = None;
        } else if idx.hnsw.is_some() {
            if embedding.len() == idx.dim {
                let internal = idx.order.len();
                idx.hnsw
                    .as_ref()
                    .unwrap()
                    .insert((&embedding[..], internal));
                idx.order.push(node_id);
                idx.built_len = len;
            } else {
                idx.hnsw = None; // dimension drift → rebuild
            }
        }
        // hnsw == None (not yet built): leave it; built lazily on next search.
    }

    pub fn semantic_search(&self, query_embedding: &[f32], n_results: usize) -> Vec<(String, f32)> {
        if self.embeddings.len() < BRUTE_FORCE_THRESHOLD {
            return self.brute_force_search(query_embedding, n_results);
        }
        self.ensure_index();
        let idx = self.index.read();
        self.hnsw_query(query_embedding, n_results, &idx)
    }

    /// Ensure the HNSW index reflects the current embeddings (double-checked).
    fn ensure_index(&self) {
        {
            let idx = self.index.read();
            if idx.hnsw.is_some() && idx.built_len == self.embeddings.len() {
                return;
            }
        }
        let mut idx = self.index.write();
        if idx.hnsw.is_some() && idx.built_len == self.embeddings.len() {
            return; // another thread rebuilt while we waited for the write lock
        }
        self.rebuild(&mut idx);
    }

    /// Build a fresh HNSW index from all embeddings (one-time after load / update).
    fn rebuild(&self, idx: &mut HnswIndex) {
        let dim = self
            .embeddings
            .values()
            .next()
            .map(|e| e.len())
            .unwrap_or(0);
        let hnsw: Hnsw<'static, f32, DistCosine> = Hnsw::new(
            HNSW_MAX_NB_CONN,
            self.embeddings.len().max(1),
            HNSW_NB_LAYER,
            HNSW_EF_SEARCH,
            DistCosine,
        );
        let mut order = Vec::with_capacity(self.embeddings.len());
        for (id, emb) in &self.embeddings {
            if emb.len() == dim {
                hnsw.insert((&emb[..], order.len()));
                order.push(id.clone());
            }
        }
        idx.hnsw = Some(hnsw);
        idx.order = order;
        idx.dim = dim;
        idx.built_len = self.embeddings.len();
    }

    /// Query the (already-current) HNSW index. hnsw_rs `DistCosine` returns a
    /// distance of `1 - cosine_similarity`, converted back to similarity here.
    fn hnsw_query(
        &self,
        query_embedding: &[f32],
        n_results: usize,
        idx: &HnswIndex,
    ) -> Vec<(String, f32)> {
        let hnsw = match &idx.hnsw {
            Some(h) => h,
            None => return Vec::new(),
        };
        hnsw.search(query_embedding, n_results, HNSW_EF_SEARCH)
            .iter()
            .filter_map(|nb| {
                idx.order
                    .get(nb.d_id)
                    .map(|id| (id.clone(), 1.0 - nb.distance))
            })
            .collect()
    }

    /// Brute-force cosine similarity search for small collections.
    fn brute_force_search(&self, query_embedding: &[f32], n_results: usize) -> Vec<(String, f32)> {
        let query_norm = dot_product(query_embedding, query_embedding).sqrt();
        if query_norm == 0.0 {
            return Vec::new();
        }

        let mut scores: Vec<(String, f32)> = self
            .embeddings
            .iter()
            .filter_map(|(node_id, emb)| {
                let emb_norm = dot_product(emb, emb).sqrt();
                if emb_norm == 0.0 {
                    None
                } else {
                    let similarity = dot_product(query_embedding, emb) / (query_norm * emb_norm);
                    Some((node_id.clone(), similarity))
                }
            })
            .collect();

        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.truncate(n_results);
        scores
    }

    /// Returns the number of stored embeddings.
    pub fn len(&self) -> usize {
        self.embeddings.len()
    }

    /// Returns true if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.embeddings.is_empty()
    }
}

/// Pure-Rust dot product.
fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_search_brute_force() {
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
    fn test_hnsw_search_large_collection() {
        let mut store = SemanticStore::new();
        // Insert enough embeddings to trigger HNSW path
        for i in 0..50 {
            let mut emb = vec![0.0f32; 8];
            emb[i % 8] = 1.0;
            emb[(i + 1) % 8] = 0.5;
            store.add_embedding(format!("node_{}", i), emb);
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
            store.add_embedding(format!("node_{}", i), emb);
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
    fn hnsw_incremental_insert_after_build_is_searchable() {
        // Build the index (cross the HNSW threshold), THEN add a new embedding —
        // it must be found via the incremental-insert path, no full rebuild needed.
        let mut store = SemanticStore::new();
        for i in 0..40 {
            let mut emb = vec![0.1f32; 8];
            emb[i % 8] = (i as f32) / 40.0;
            store.add_embedding(format!("n{}", i), emb);
        }
        let _ = store.semantic_search(&[0.1; 8], 5); // triggers the initial build

        let distinct = vec![9.0f32; 8];
        store.add_embedding("late".into(), distinct.clone()); // incremental insert
        let results = store.semantic_search(&distinct, 3);
        assert!(
            results.iter().any(|(id, _)| id == "late"),
            "an embedding added after the build must be searchable: {:?}",
            results
        );
    }
}
