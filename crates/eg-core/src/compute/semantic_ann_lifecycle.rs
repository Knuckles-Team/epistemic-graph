//! Lifecycle and shape operations for the IVF-PQ semantic store.

use super::*;

impl Default for SemanticStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SemanticStore {
    pub fn new() -> Self {
        Self::from_arena(
            EmbeddingArena::default(),
            None,
            crate::compute::semantic::GenerationStamp::fresh(),
        )
    }

    pub(super) fn from_arena(
        arena: EmbeddingArena,
        space: Option<EmbeddingSpaceRef>,
        generation: crate::compute::semantic::GenerationStamp,
    ) -> Self {
        Self {
            arena,
            space,
            index: RwLock::new(None),
            built_len: RwLock::new(0),
            state: AtomicU8::new(STATE_COLD),
            generation,
        }
    }

    pub(in crate::compute::semantic) fn mismatched_row_dim(
        &self,
        requested: usize,
    ) -> Option<usize> {
        (self.arena.dim != 0 && self.arena.dim != requested).then_some(self.arena.dim)
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
