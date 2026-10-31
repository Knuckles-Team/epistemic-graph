//! Width-safe exact and IVF-PQ query operations.

use super::*;

impl SemanticStore {
    pub fn semantic_search(&self, query_embedding: &[f32], n_results: usize) -> Vec<(String, f32)> {
        if !valid_query(query_embedding, query_dimension(self)) {
            return Vec::new();
        }
        if !maintained_query(self) {
            return self.brute_force_search(query_embedding, n_results);
        }
        match self.ready_search(query_embedding, n_results, &|_| true) {
            Some(hits) => hits,
            None => self.brute_force_search(query_embedding, n_results),
        }
    }

    /// kNN search with a node-id metadata pre-filter pushed into the ANN scan.
    pub fn semantic_search_filtered(
        &self,
        query_embedding: &[f32],
        n_results: usize,
        allow: impl Fn(&str) -> bool + Sync,
    ) -> Vec<(String, f32)> {
        if !valid_query(query_embedding, query_dimension(self)) {
            return Vec::new();
        }
        if !maintained_query(self) {
            return self.brute_force_search_filtered(query_embedding, n_results, allow);
        }
        match self.ready_search(query_embedding, n_results, &allow) {
            Some(hits) => hits,
            None => self.brute_force_search_filtered(query_embedding, n_results, allow),
        }
    }

    /// Search with a vector whose model-space identity is preserved through the call.
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
        if self.arena.dim != 0 && self.arena.dim != query.values.len() {
            return Err(SemanticQueryError::DimensionMismatch {
                expected: self.arena.dim,
                received: query.values.len(),
            });
        }
        Ok(self.semantic_search_filtered(&query.values, n_results, allow))
    }

    fn brute_force_search(&self, query_embedding: &[f32], n_results: usize) -> Vec<(String, f32)> {
        self.brute_force_search_filtered(query_embedding, n_results, |_| true)
    }

    fn brute_force_search_filtered(
        &self,
        query_embedding: &[f32],
        n_results: usize,
        allow: impl Fn(&str) -> bool + Sync,
    ) -> Vec<(String, f32)> {
        let arena = &self.arena;
        if arena.is_empty() || n_results == 0 || arena.dim == 0 {
            return Vec::new();
        }
        let query_norm = l2_norm(query_embedding);
        if query_norm == 0.0 {
            return Vec::new();
        }
        let inv_qnorm = 1.0 / query_norm;
        let query = CosineQuery {
            values: query_embedding,
            inverse_norm: inv_qnorm,
        };
        let mut scored = collect_scores(arena, &query, &allow);
        truncate_rows(&mut scored, n_results);
        scored
            .into_iter()
            .map(|(row, score)| (arena.ids[row].clone(), score))
            .collect()
    }
}

fn collect_scores(
    arena: &EmbeddingArena,
    query: &CosineQuery<'_>,
    allow: &(impl Fn(&str) -> bool + Sync),
) -> Vec<(usize, f32)> {
    let dim = arena.dim;
    let norms = &arena.norms;
    arena
        .data
        .par_chunks_exact(dim)
        .enumerate()
        .filter_map(|(row, embedding)| {
            if norms[row] == 0.0 || !allow(arena.ids[row].as_str()) {
                return None;
            }
            let score = cosine_score(query, embedding, norms[row])?;
            Some((row, score))
        })
        .collect()
}

struct CosineQuery<'a> {
    values: &'a [f32],
    inverse_norm: f32,
}

fn query_dimension(store: &SemanticStore) -> usize {
    store
        .space
        .as_ref()
        .map(|space| space.dimensions)
        .unwrap_or(store.arena.dim)
}

fn maintained_query(store: &SemanticStore) -> bool {
    store.arena.len() >= BRUTE_FORCE_THRESHOLD.max(ANN_BUILD_THRESHOLD)
        && store.arena.dim <= MAX_MAINTAINED_DIMENSION
}

fn cosine_score(query: &CosineQuery<'_>, embedding: &[f32], embedding_norm: f32) -> Option<f32> {
    if embedding_norm == 0.0 {
        None
    } else {
        Some(dot_product(query.values, embedding) * query.inverse_norm / embedding_norm)
    }
}

fn truncate_rows(scores: &mut Vec<(usize, f32)>, limit: usize) {
    let cmp_desc = |left: &(usize, f32), right: &(usize, f32)| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(std::cmp::Ordering::Equal)
    };
    let keep = limit.min(scores.len());
    if keep < scores.len() {
        scores.select_nth_unstable_by(keep.saturating_sub(1), cmp_desc);
        scores.truncate(keep);
    }
    scores.sort_by(cmp_desc);
}

impl SemanticStore {
    fn ready_search(
        &self,
        query_embedding: &[f32],
        n_results: usize,
        allow: &impl Fn(&str) -> bool,
    ) -> Option<Vec<(String, f32)>> {
        if self.state.load(Ordering::Acquire) != STATE_READY {
            return None;
        }
        let guard = self.index.try_read()?;
        let ann = guard.as_ref()?;
        if *self.built_len.read() != self.arena.len() {
            return None;
        }
        Some(ann.search_filtered(query_embedding, n_results, allow))
    }
}
