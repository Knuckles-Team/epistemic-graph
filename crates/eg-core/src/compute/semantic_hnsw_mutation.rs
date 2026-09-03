//! Validated embedding writes and derived-index maintenance for HNSW storage.

use super::*;

impl SemanticStore {
    /// Raw stored embedding for `node_id`, if present. The MMR reranker uses
    /// this owned value to compute pairwise diversity.
    pub fn get_embedding(&self, node_id: &str) -> Option<Vec<f32>> {
        self.embeddings.get(node_id).cloned()
    }

    /// Deterministic owned image used by the MutationBatch row-delta compiler.
    /// HNSW internals are derived and intentionally excluded.
    pub fn embeddings_snapshot(&self) -> Vec<(String, Vec<f32>)> {
        let mut rows: Vec<_> = self
            .embeddings
            .iter()
            .map(|(id, embedding)| (id.clone(), embedding.clone()))
            .collect();
        rows.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        rows
    }

    /// Reject malformed data before touching either resident rows or HNSW.
    pub fn add_embedding(
        &mut self,
        node_id: String,
        embedding: Vec<f32>,
    ) -> Result<(), EmbeddingDimensionError> {
        let expected_dim = self
            .space
            .as_ref()
            .map(|space| space.dimensions)
            .unwrap_or_else(|| self.dim());
        check_embedding_dimension_bounded(&embedding, expected_dim, MAX_GENERIC_DIMENSION)?;

        let is_update = self.embeddings.contains_key(&node_id);
        self.embeddings.insert(node_id.clone(), embedding.clone());
        let live_len = self.embeddings.len();

        let mut idx = self.index.write();
        if idx.hnsw.is_none() {
            return Ok(());
        }
        // The guard above guarantees that the resident HNSW dimension is
        // unchanged; a decline here would indicate an internal invariant break.
        let internal = idx.order.len();
        idx.hnsw
            .as_mut()
            .expect("resident HNSW checked above")
            .insert(internal as u64, embedding);
        idx.order.push(node_id.clone());
        if is_update {
            if let Some(&old) = idx.id_to_internal.get(&node_id) {
                idx.tombstones.insert(old);
            }
        }
        idx.id_to_internal.insert(node_id, internal);
        idx.built_len = live_len;

        if idx.order.len() >= BRUTE_FORCE_THRESHOLD
            && idx.tombstones.len() * 100 >= idx.order.len() * COMPACT_TOMBSTONE_PCT
        {
            idx.hnsw = None;
        }
        Ok(())
    }

    /// Incrementally remove `node_id`'s embedding and tombstone its HNSW row.
    pub fn remove_embedding(&mut self, node_id: &str) -> bool {
        if self.embeddings.remove(node_id).is_none() {
            return false;
        }
        let live_len = self.embeddings.len();
        let mut idx = self.index.write();
        if idx.hnsw.is_some() {
            if let Some(&internal) = idx.id_to_internal.get(node_id) {
                idx.tombstones.insert(internal);
            }
            idx.id_to_internal.remove(node_id);
            idx.built_len = live_len;
            if idx.order.len() >= BRUTE_FORCE_THRESHOLD
                && idx.tombstones.len() * 100 >= idx.order.len() * COMPACT_TOMBSTONE_PCT
            {
                idx.hnsw = None;
            }
        }
        true
    }

    /// Force a clean rebuild that drops all tombstones.
    pub fn force_compact(&self) {
        let mut idx = self.index.write();
        if idx.hnsw.is_some() {
            self.rebuild(&mut idx);
        }
    }
}
