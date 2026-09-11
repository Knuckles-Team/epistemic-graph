//! Validated arena writes and incremental IVF-PQ maintenance.

use super::*;

impl SemanticStore {
    /// Raw stored embedding for `node_id`, if present.
    pub fn get_embedding(&self, node_id: &str) -> Option<Vec<f32>> {
        self.arena.get(node_id).map(|row| row.to_vec())
    }

    /// Deterministic owned image used by the MutationBatch row-delta compiler.
    /// The ANN directory itself is derived and intentionally excluded.
    pub fn embeddings_snapshot(&self) -> Vec<(String, Vec<f32>)> {
        let mut rows: Vec<_> = self
            .arena
            .ids
            .iter()
            .enumerate()
            .map(|(row, id)| {
                let start = row * self.arena.dim;
                let end = start + self.arena.dim;
                (id.clone(), self.arena.data[start..end].to_vec())
            })
            .collect();
        rows.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        rows
    }

    /// Validate an embedding against this store's declared space and backend
    /// width before any resident state is touched. Callers that already hold a
    /// read guard can use this admission check without cloning the corpus.
    pub fn validate_embedding(&self, embedding: &[f32]) -> Result<(), EmbeddingDimensionError> {
        let expected_dim = self
            .space
            .as_ref()
            .map(|space| space.dimensions)
            .unwrap_or(self.arena.dim);
        if embedding.len() > MAX_GENERIC_DIMENSION {
            return Err(EmbeddingDimensionError::Oversized {
                received: embedding.len(),
                max: MAX_GENERIC_DIMENSION,
            });
        }
        check_embedding_dimension(embedding, expected_dim).map(|_| ())
    }

    /// Reject malformed data before touching either resident rows or the ANN.
    pub fn add_embedding(
        &mut self,
        node_id: String,
        embedding: Vec<f32>,
    ) -> Result<(), EmbeddingDimensionError> {
        self.validate_embedding(&embedding)?;
        self.arena.insert(node_id.clone(), &embedding)?;
        let live_len = self.arena.len();
        self.maintain_incremental_index(&node_id, &embedding, live_len);
        Ok(())
    }

    fn maintain_incremental_index(&self, node_id: &str, embedding: &[f32], live_len: usize) {
        let mut idx = self.index.write();
        let Some(ann) = idx.as_mut() else {
            return;
        };
        if update_ann(ann, node_id, embedding) {
            *self.built_len.write() = live_len;
            return;
        }
        *idx = None;
        *self.built_len.write() = 0;
        self.state.store(STATE_COLD, Ordering::Release);
    }

    /// Incrementally remove `node_id`'s embedding and tombstone its ANN row.
    pub fn remove_embedding(&mut self, node_id: &str) -> bool {
        if !self.arena.remove(node_id) {
            return false;
        }
        let live_len = self.arena.len();
        if let Some(ann) = self.index.write().as_mut() {
            ann.remove(node_id);
            *self.built_len.write() = live_len;
            if ann.tombstone_ratio() >= COMPACT_TOMBSTONE_PCT {
                ann.compact();
            }
        }
        true
    }

    /// Force a clean compaction that drops all tombstones.
    pub fn force_compact(&self) {
        if let Some(ann) = self.index.write().as_mut() {
            ann.compact();
        }
    }
}

fn update_ann(ann: &mut AnnIndex, node_id: &str, embedding: &[f32]) -> bool {
    if !ann.add(node_id, embedding) {
        return false;
    }
    if ann.tombstone_ratio() >= COMPACT_TOMBSTONE_PCT {
        ann.compact();
    }
    true
}
