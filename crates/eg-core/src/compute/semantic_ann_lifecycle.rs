//! Lifecycle and shape operations for the IVF-PQ semantic store.

use super::*;

impl Default for SemanticStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SemanticStore {
    pub fn new() -> Self {
        Self {
            arena: EmbeddingArena::default(),
            space: None,
            index: RwLock::new(None),
            built_len: RwLock::new(0),
            state: AtomicU8::new(STATE_COLD),
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
        if self.arena.dim != 0 && self.arena.dim != space.dimensions {
            return Err(format!(
                "semantic store rows carry {} dimensions; declared space `{}` requires {}",
                self.arena.dim, space.digest, space.dimensions
            ));
        }
        self.space = Some(space);
        Ok(())
    }

    /// Declared model/preprocessing space, if this is not a legacy raw store.
    pub fn space(&self) -> Option<&EmbeddingSpaceRef> {
        self.space.as_ref()
    }

    /// True once a fresh ANN index is resident and current.
    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_READY
            && *self.built_len.read() == self.arena.len()
    }

    /// True while a background `warm()` build is in flight for this store.
    pub fn is_warming(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_WARMING
    }

    /// True if a resident index reflects the current embedding count.
    pub fn index_matches_len(&self) -> bool {
        self.index.read().is_some() && *self.built_len.read() == self.arena.len()
    }
}
