//! Width-safe HNSW and exact query operations.

use super::*;

impl SemanticStore {
    pub fn semantic_search(&self, query_embedding: &[f32], n_results: usize) -> Vec<(String, f32)> {
        if n_results == 0 {
            return Vec::new();
        }
        let store_dim = self
            .space
            .as_ref()
            .map(|space| space.dimensions)
            .unwrap_or_else(|| self.dim());
        if !valid_query(query_embedding, store_dim) {
            return Vec::new();
        }
        // A wider generic vector is valid for exact retrieval but must never
        // cross the maintained HNSW artifact boundary.
        if self.embeddings.len() < BRUTE_FORCE_THRESHOLD || self.dim() > MAX_MAINTAINED_DIMENSION {
            return self.brute_force_search(query_embedding, n_results);
        }
        self.ensure_index();
        let idx = self.index.read();
        self.hnsw_query(query_embedding, n_results, &idx)
    }

    /// kNN search with a metadata predicate pushed into the HNSW walk.
    pub fn semantic_search_filtered(
        &self,
        query_embedding: &[f32],
        n_results: usize,
        allow: impl Fn(&str) -> bool + Sync,
    ) -> Vec<(String, f32)> {
        if n_results == 0 {
            return Vec::new();
        }
        let store_dim = self
            .space
            .as_ref()
            .map(|space| space.dimensions)
            .unwrap_or_else(|| self.dim());
        if !valid_query(query_embedding, store_dim) {
            return Vec::new();
        }
        if self.embeddings.len() < BRUTE_FORCE_THRESHOLD || self.dim() > MAX_MAINTAINED_DIMENSION {
            return self.brute_force_search_filtered(query_embedding, n_results, allow);
        }
        self.hnsw_filtered_search(query_embedding, n_results, allow)
            .unwrap_or_default()
    }

    /// Run a filtered query against the current HNSW artifact, if one is
    /// resident. A missing artifact is represented as `None` so the public
    /// legacy `Vec` API can preserve its empty-result behavior.
    fn hnsw_filtered_search(
        &self,
        query_embedding: &[f32],
        n_results: usize,
        allow: impl Fn(&str) -> bool + Sync,
    ) -> Option<Vec<(String, f32)>> {
        self.ensure_index();
        let idx = self.index.read();
        let hnsw = idx.hnsw.as_ref()?;
        let allow_internal = |internal_id: u64| hnsw_allows(&idx, internal_id, &allow);
        let ef = HNSW_EF_SEARCH_FILTERED.max(n_results);
        let mut hits = Vec::new();
        for neighbor in hnsw.search_filtered(query_embedding, n_results, ef, Some(&allow_internal))
        {
            let Ok(internal) = usize::try_from(neighbor.id) else {
                continue;
            };
            let Some(id) = idx.order.get(internal) else {
                continue;
            };
            hits.push((id.clone(), 1.0 - neighbor.distance));
        }
        Some(hits)
    }

    /// Search with the model-space identity preserved through the call.
    pub fn semantic_search_stamped_filtered(
        &self,
        query: &StampedVector,
        n_results: usize,
        allow: impl Fn(&str) -> bool + Sync,
    ) -> Result<Vec<(String, f32)>, SemanticQueryError> {
        query
            .validate()
            .map_err(SemanticQueryError::InvalidVector)?;
        let space = self
            .space
            .as_ref()
            .ok_or(SemanticQueryError::StoreSpaceUnbound)?;
        space
            .validate()
            .map_err(SemanticQueryError::InvalidStoreSpace)?;
        if query.space.digest != space.digest {
            return Err(SemanticQueryError::SpaceMismatch {
                expected: space.digest.clone(),
                received: query.space.digest.clone(),
            });
        }
        let resident_dim = self.dim();
        if resident_dim != 0 && resident_dim != query.values.len() {
            return Err(SemanticQueryError::DimensionMismatch {
                expected: resident_dim,
                received: query.values.len(),
            });
        }
        Ok(self.semantic_search_filtered(&query.values, n_results, allow))
    }

    fn brute_force_search_filtered(
        &self,
        query_embedding: &[f32],
        n_results: usize,
        allow: impl Fn(&str) -> bool,
    ) -> Vec<(String, f32)> {
        if self.embeddings.is_empty() {
            return Vec::new();
        }
        let query_norm = dot_product(query_embedding, query_embedding).sqrt();
        if query_norm == 0.0 {
            return Vec::new();
        }
        let mut scores: Vec<(String, f32)> = self
            .embeddings
            .iter()
            .filter(|(node_id, _)| allow(node_id.as_str()))
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
        truncate_highest_similarity(&mut scores, n_results);
        scores
    }

    fn brute_force_search(&self, query_embedding: &[f32], n_results: usize) -> Vec<(String, f32)> {
        self.brute_force_search_filtered(query_embedding, n_results, |_| true)
    }
}

fn hnsw_allows(idx: &HnswIndex, internal_id: u64, allow: &impl Fn(&str) -> bool) -> bool {
    let Ok(internal) = usize::try_from(internal_id) else {
        return false;
    };
    if idx.tombstones.contains(&internal) {
        return false;
    }
    idx.order
        .get(internal)
        .map(|node_id| allow(node_id.as_str()))
        .unwrap_or(false)
}
