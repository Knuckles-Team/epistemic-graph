//! Off-query-path IVF-PQ build and readiness lifecycle.

use super::*;

impl SemanticStore {
    /// Build the maintained IVF-PQ artifact off the query path. Wider generic
    /// vectors remain exact-only and intentionally do not allocate ANN state.
    pub fn warm(&self, label: &str) {
        if self.arena.len() < BRUTE_FORCE_THRESHOLD.max(ANN_BUILD_THRESHOLD)
            || self.arena.dim > MAX_MAINTAINED_DIMENSION
        {
            return;
        }
        self.ensure_index(label);
    }

    /// `pub(super)` — also called directly by `semantic_ann_persistence::save_index`
    /// (a sibling submodule of the shared `backend` parent) to guarantee a fresh
    /// index is resident before it is persisted.
    pub(super) fn ensure_index(&self, label: &str) {
        if self.arena.dim > MAX_MAINTAINED_DIMENSION {
            return;
        }
        if self.index_ready() {
            return;
        }
        if !self.claim_warm() {
            return;
        }
        self.build_index(label);
    }

    fn index_ready(&self) -> bool {
        let idx = self.index.read();
        idx.is_some() && *self.built_len.read() == self.arena.len()
    }

    fn claim_warm(&self) -> bool {
        self.state.swap(STATE_WARMING, Ordering::AcqRel) != STATE_WARMING
    }

    fn build_index(&self, label: &str) {
        let mut idx = self.index.write();
        let n = self.arena.len();
        let span = tracing::info_span!("ann_index_build", graph = label, n_vectors = n);
        let _guard = span.enter();
        let start = std::time::Instant::now();
        *idx = AnnIndex::build(&self.arena.ids, &self.arena.data, self.arena.dim);
        let built = idx.is_some();
        *self.built_len.write() = if built { n } else { 0 };
        self.state.store(
            if built { STATE_READY } else { STATE_COLD },
            Ordering::Release,
        );
        tracing::info!(
            graph = label,
            n_vectors = n,
            build_ms = start.elapsed().as_millis() as u64,
            ready = built,
            "semantic ANN index build complete"
        );
    }
}
