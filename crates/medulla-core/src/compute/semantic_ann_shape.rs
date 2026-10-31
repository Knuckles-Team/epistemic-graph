//! Resident arena shape and memory accounting for the IVF-PQ semantic store.

use super::*;

impl SemanticStore {
    /// Returns the number of stored embeddings.
    pub fn len(&self) -> usize {
        self.arena.len()
    }

    /// Returns true if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.arena.is_empty()
    }

    /// The store's embedding dimensionality — `0` until the first vector is
    /// inserted. This remains the resident-row width even when an exact-only
    /// generic space is wider than the maintained ANN ceiling.
    pub fn dim(&self) -> usize {
        self.arena.dim
    }

    /// Approximate resident bytes held by the embedding vectors.
    pub fn embedding_bytes(&self) -> u64 {
        self.arena.embedding_bytes()
    }
}
