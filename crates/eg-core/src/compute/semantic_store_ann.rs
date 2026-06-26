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
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Threshold below which we use brute-force (index overhead not worth it).
const BRUTE_FORCE_THRESHOLD: usize = 32;
/// Rebuild/compact the index once tombstoned rows exceed this fraction of total.
const COMPACT_TOMBSTONE_PCT: f32 = 0.30;

pub struct SemanticStore {
    embeddings: HashMap<String, Vec<f32>>,
    /// Lazily-built eg-ann IVF-PQ index. `None` until the store crosses
    /// `ANN_BUILD_THRESHOLD`; rebuilt lazily from `embeddings` after a snapshot
    /// load (the index is `#[serde(skip)]`, exactly like the HNSW backend).
    index: RwLock<Option<AnnIndex>>,
    /// LIVE embedding count the index reflects (staleness check after load).
    built_len: RwLock<usize>,
}

impl Clone for SemanticStore {
    fn clone(&self) -> Self {
        Self {
            embeddings: self.embeddings.clone(),
            index: RwLock::new(None),
            built_len: RwLock::new(0),
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
                    // Dimension drift → drop the index, rebuild on next search.
                    *idx = None;
                    *self.built_len.write() = 0;
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
        self.ensure_index();
        match self.index.read().as_ref() {
            Some(ann) => ann.search(query_embedding, n_results),
            None => self.brute_force_search(query_embedding, n_results),
        }
    }

    /// Ensure the index reflects the current embeddings (rebuilt lazily once after
    /// load / once the store crosses the build threshold).
    fn ensure_index(&self) {
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
        *idx = AnnIndex::build(&self.embeddings);
        *self.built_len.write() = if idx.is_some() {
            self.embeddings.len()
        } else {
            0
        };
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

    /// Persist the eg-ann index (codes + meta + id map) for a no-rebuild reopen.
    /// Builds the index first if it isn't resident. Errors if there is nothing to
    /// index (empty / below the build threshold).
    pub fn save_index(&self, dir: &Path) -> std::io::Result<()> {
        self.ensure_index();
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

/// Pure-Rust dot product.
fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // Force a search to build the index.
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
        };
        reloaded.load_index(&tmp).unwrap();
        let after = reloaded.semantic_search(q, 10);
        assert_eq!(
            before.iter().map(|r| r.0.clone()).collect::<Vec<_>>(),
            after.iter().map(|r| r.0.clone()).collect::<Vec<_>>(),
            "no-rebuild reload must match"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
