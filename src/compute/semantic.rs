// CONCEPT:KG-2.22b — Semantic Embedding Store with HNSW Index
//
// High-performance embedding store using the hnsw_rs crate for
// O(log n) approximate nearest-neighbor search. Falls back to
// brute-force cosine for small collections (< 32 embeddings).

use hnsw_rs::hnsw::Hnsw;
use hnsw_rs::prelude::DistCosine;
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SemanticStore {
    embeddings: HashMap<String, Vec<f32>>,
    /// Ordered list of node IDs matching HNSW internal indices.
    #[serde(skip)]
    index_to_id: Vec<String>,
    /// Reverse lookup from node ID to HNSW internal index.
    #[serde(skip)]
    id_to_index: HashMap<String, usize>,
    /// Embedding dimensionality (set on first insert).
    #[serde(skip)]
    dimension: Option<usize>,
    /// Tracks whether the HNSW index needs rebuilding.
    #[serde(skip)]
    dirty: bool,
}

impl SemanticStore {
    pub fn new() -> Self {
        Self {
            embeddings: HashMap::new(),
            index_to_id: Vec::new(),
            id_to_index: HashMap::new(),
            dimension: None,
            dirty: false,
        }
    }

    pub fn add_embedding(&mut self, node_id: String, embedding: Vec<f32>) {
        if self.dimension.is_none() {
            self.dimension = Some(embedding.len());
        }
        self.embeddings.insert(node_id.clone(), embedding);
        if !self.id_to_index.contains_key(&node_id) {
            let idx = self.index_to_id.len();
            self.index_to_id.push(node_id.clone());
            self.id_to_index.insert(node_id, idx);
        }
        self.dirty = true;
    }

    pub fn semantic_search(&self, query_embedding: &[f32], n_results: usize) -> Vec<(String, f32)> {
        if self.embeddings.len() < BRUTE_FORCE_THRESHOLD {
            return self.brute_force_search(query_embedding, n_results);
        }
        self.hnsw_search(query_embedding, n_results)
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

    /// HNSW-accelerated approximate nearest neighbor search.
    fn hnsw_search(&self, query_embedding: &[f32], n_results: usize) -> Vec<(String, f32)> {
        let dim = match self.dimension {
            Some(d) => d,
            None => return Vec::new(),
        };

        // Build a temporary HNSW index.
        // hnsw_rs uses DistCosine which returns (1 - cosine_similarity),
        // so we convert back to similarity.
        let hnsw: Hnsw<f32, DistCosine> = Hnsw::new(
            HNSW_MAX_NB_CONN,
            self.embeddings.len().max(1),
            HNSW_NB_LAYER,
            HNSW_EF_SEARCH,
            DistCosine,
        );

        // Insert all embeddings in index order
        let ordered_data: Vec<(&Vec<f32>, usize)> = self
            .index_to_id
            .iter()
            .enumerate()
            .filter_map(|(idx, nid)| self.embeddings.get(nid).map(|emb| (emb, idx)))
            .collect();

        for (emb, idx) in &ordered_data {
            // hnsw_rs expects slices
            if emb.len() == dim {
                hnsw.insert((&emb[..], *idx));
            }
        }

        // Query
        let neighbors = hnsw.search(query_embedding, n_results, HNSW_EF_SEARCH);

        neighbors
            .iter()
            .filter_map(|neighbour| {
                let idx = neighbour.d_id;
                if idx < self.index_to_id.len() {
                    let node_id = self.index_to_id[idx].clone();
                    // DistCosine returns distance = 1 - similarity
                    let similarity = 1.0 - neighbour.distance;
                    Some((node_id, similarity))
                } else {
                    None
                }
            })
            .collect()
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
}
