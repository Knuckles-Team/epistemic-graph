use super::ResultCache;

impl ResultCache {
    /// `(hits, misses)` since construction — the proof counters a test reads to show
    /// a repeated query on an unchanged graph hit the cache (didn't recompute) and a
    /// query after a write missed (recomputed).
    pub fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(std::sync::atomic::Ordering::Relaxed),
            self.misses.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Current number of retained VERSION-KEYED entries (observability/tests).
    pub fn len(&self) -> usize {
        self.inner.lock().versioned.map.len()
    }

    /// Current number of retained DEPENDENCY-SCOPED entries (observability/tests, W1.6/P7).
    pub fn dep_len(&self) -> usize {
        self.inner.lock().dependency.map.len()
    }

    pub fn is_empty(&self) -> bool {
        let inner = self.inner.lock();
        inner.versioned.map.is_empty() && inner.dependency.map.is_empty()
    }
}
