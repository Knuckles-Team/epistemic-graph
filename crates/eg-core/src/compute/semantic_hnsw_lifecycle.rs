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
        }
    }

    /// Create an empty store pinned to one exact model/preprocessing space.
    pub fn new_in_space(space: EmbeddingSpaceRef) -> Result<Self, String> {
        let mut store = Self::new();
        store.declare_space(space)?;
        Ok(store)
    }

    /// Declare the immutable space used by model-produced queries. Existing raw
    /// rows may be adopted only when their width matches the declaration.
    pub fn declare_space(&mut self, space: EmbeddingSpaceRef) -> Result<(), String> {
        space.validate()?;
        if let Some(current) = self.space.as_ref() {
            if current.digest == space.digest {
                return Ok(());
            }
            return Err(format!(
                "semantic store already declares embedding space `{}`; cannot replace it with `{}`",
                current.digest, space.digest
            ));
        }
        if let Some(dim) = self
            .embeddings
            .values()
            .map(Vec::len)
            .find(|&dim| dim != space.dimensions)
        {
            return Err(format!(
                "semantic store rows carry {dim} dimensions; declared space `{}` requires {}",
                space.digest, space.dimensions
            ));
        }
        self.space = Some(space);
        Ok(())
    }

    /// Declared model/preprocessing space, if this is not a legacy raw store.
    pub fn space(&self) -> Option<&EmbeddingSpaceRef> {
        self.space.as_ref()
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
