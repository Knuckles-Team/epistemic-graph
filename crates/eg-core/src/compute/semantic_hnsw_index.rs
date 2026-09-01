//! Derived HNSW lifecycle and query adapter.

use super::*;

impl SemanticStore {
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
            return;
        }
        self.rebuild(&mut idx);
    }

    /// Build a fresh HNSW index from all embeddings. This is called only after
    /// the query adapter has confirmed the maintained-dimension ceiling.
    fn rebuild(&self, idx: &mut HnswIndex) {
        let dim = self
            .embeddings
            .values()
            .next()
            .map(|e| e.len())
            .unwrap_or(0);
        if dim == 0 || dim > MAX_MAINTAINED_DIMENSION {
            *idx = HnswIndex::empty();
            return;
        }
        let mut hnsw = NativeHnswIndex::new(
            dim,
            Metric::Cosine,
            HNSW_MAX_NB_CONN,
            HNSW_EF_CONSTRUCTION,
            HNSW_SEED,
        );
        let mut order = Vec::with_capacity(self.embeddings.len());
        let mut id_to_internal = HashMap::with_capacity(self.embeddings.len());
        for (id, emb) in &self.embeddings {
            let internal = order.len();
            hnsw.insert(internal as u64, emb.clone());
            order.push(id.clone());
            id_to_internal.insert(id.clone(), internal);
        }
        idx.hnsw = Some(hnsw);
        idx.order = order;
        idx.id_to_internal = id_to_internal;
        idx.tombstones.clear();
        idx.dim = dim;
        idx.built_len = self.embeddings.len();
    }

    /// Query the already-current HNSW index. `Metric::Cosine` returns a distance
    /// of `1 - cosine_similarity`, converted back to similarity here.
    fn hnsw_query(
        &self,
        query_embedding: &[f32],
        n_results: usize,
        idx: &HnswIndex,
    ) -> Vec<(String, f32)> {
        let Some(hnsw) = idx.hnsw.as_ref() else {
            return Vec::new();
        };
        let want = if idx.tombstones.is_empty() {
            n_results
        } else {
            n_results
                .saturating_mul(2)
                .max(n_results.saturating_add(16))
                .min(idx.order.len())
        };
        let mut hits = Vec::new();
        for neighbor in hnsw.search(query_embedding, want, HNSW_EF_SEARCH) {
            let Ok(internal) = usize::try_from(neighbor.id) else {
                continue;
            };
            if idx.tombstones.contains(&internal) {
                continue;
            }
            let Some(id) = idx.order.get(internal) else {
                continue;
            };
            hits.push((id.clone(), 1.0 - neighbor.distance));
            if hits.len() == n_results {
                break;
            }
        }
        hits
    }
}
