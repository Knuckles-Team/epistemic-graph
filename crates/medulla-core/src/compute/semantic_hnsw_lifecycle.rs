//! Lifecycle and read-only shape operations for the default semantic store.

use super::*;

impl Default for SemanticStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SemanticStore {
    pub fn new() -> Self {
        Self {
            embeddings: HashMap::new(),
            space: None,
            index: RwLock::new(HnswIndex::empty()),
            generation: crate::compute::semantic::GenerationStamp::fresh(),
        }
    }

    pub(in crate::compute::semantic) fn mismatched_row_dim(
        &self,
        requested: usize,
    ) -> Option<usize> {
        self.embeddings
            .values()
            .map(Vec::len)
            .find(|&dim| dim != requested)
    }

    /// Returns the number of stored embeddings.
    pub fn len(&self) -> usize {
        self.embeddings.len()
    }

    /// Returns true if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.embeddings.is_empty()
    }

    /// The store's embedding dimensionality — `0` until the first vector is
    /// inserted. Lets callers reject a query vector of the wrong width before
    /// it reaches the HNSW constructor.
    pub fn dim(&self) -> usize {
        self.embeddings.values().next().map(Vec::len).unwrap_or(0)
    }

    /// Approximate resident bytes held by the embedding vectors.
    pub fn embedding_bytes(&self) -> u64 {
        self.embeddings
            .values()
            .map(|v| (v.len() * std::mem::size_of::<f32>()) as u64)
            .sum()
    }
}
