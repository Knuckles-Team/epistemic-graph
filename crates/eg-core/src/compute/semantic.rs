// CONCEPT:EG-KG.compute.rust-native-ml-estimators / KG-2.207 — SemanticStore backend selector.
//
// `compute::semantic::SemanticStore` is the embedding store the graph holds. Two
// interchangeable backends share one public API and one on-disk serde shape:
//   * DEFAULT — `semantic_hnsw`: the native eg-ann HNSW index (rebuilt from raw vectors on
//     first search after load).
//   * feature `ann` — `semantic_store_ann`: the native eg-ann IVF-PQ+OPQ+SQ8
//     index that reopens a persisted index WITHOUT rebuilding from raw vectors.
//
// Consumers `use crate::compute::semantic::SemanticStore` and never see which
// backend is active.

#[cfg(not(feature = "ann"))]
#[path = "semantic_hnsw.rs"]
mod backend;

#[cfg(feature = "ann")]
#[path = "semantic_store_ann.rs"]
mod backend;

pub use backend::SemanticStore;

/// Upper bound on a single embedding's dimensionality (CONCEPT:EG-KG.compute.rank-dim-mismatch-guard,
/// BUG-007). Shared by both `SemanticStore` backends' `add_embedding` chokepoint and
/// every batch-decode/validation layer above it, so an "oversized dimension" hostile
/// input is rejected at ONE authoritative bound rather than N independently-chosen
/// ones. 65_536 comfortably exceeds every real embedding model in use (the largest
/// production embedders top out in the low thousands of dimensions) while still
/// bounding a single vector's RAM/CPU cost.
pub const MAX_EMBEDDING_DIMENSION: usize = 65_536;

/// Typed rejection for `SemanticStore::add_embedding` (CONCEPT:EG-KG.compute.rank-dim-mismatch-guard,
/// BUG-007). A caller receiving this error is guaranteed the store was NOT mutated —
/// neither backend touches its arena/map until every hostile-input check below has
/// passed. Shared by both backends (`semantic_hnsw`/`semantic_store_ann`) so a
/// consumer written against `compute::semantic::SemanticStore` sees the identical
/// error type regardless of which backend the `ann` feature selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingDimensionError {
    /// A zero-length embedding vector.
    Empty,
    /// An embedding vector longer than [`MAX_EMBEDDING_DIMENSION`].
    Oversized { received: usize, max: usize },
    /// An embedding vector whose length doesn't match the store's already
    /// established dimension (the historical erase-the-corpus defect, BUG-007).
    Mismatch { expected: usize, received: usize },
}

impl std::fmt::Display for EmbeddingDimensionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "embedding must not be empty (dimension 0)"),
            Self::Oversized { received, max } => write!(
                f,
                "embedding dimension {received} exceeds the maximum of {max}"
            ),
            Self::Mismatch { expected, received } => write!(
                f,
                "embedding dimension mismatch: store expects {expected}, received {received}"
            ),
        }
    }
}

impl std::error::Error for EmbeddingDimensionError {}

/// Shared entry-point validation every `add_embedding` implementation runs BEFORE
/// touching any state (CONCEPT:EG-KG.compute.rank-dim-mismatch-guard, BUG-007). `store_dim == 0`
/// means the store has no established dimension yet (empty store — the first vector
/// establishes it). Returns the dimension the caller should now treat as established
/// (unchanged from `store_dim` unless it was `0`).
#[inline]
pub fn check_embedding_dimension(
    embedding_len: usize,
    store_dim: usize,
) -> Result<usize, EmbeddingDimensionError> {
    if embedding_len == 0 {
        return Err(EmbeddingDimensionError::Empty);
    }
    if embedding_len > MAX_EMBEDDING_DIMENSION {
        return Err(EmbeddingDimensionError::Oversized {
            received: embedding_len,
            max: MAX_EMBEDDING_DIMENSION,
        });
    }
    if store_dim == 0 {
        return Ok(embedding_len);
    }
    if embedding_len != store_dim {
        return Err(EmbeddingDimensionError::Mismatch {
            expected: store_dim,
            received: embedding_len,
        });
    }
    Ok(store_dim)
}

/// Build an OVERLAY embedding store for in-txn cross-modal read-your-own-writes
/// (CONCEPT:EG-KG.query.txn-cross-modal-ryow). Clones the committed `SemanticStore` (a point-in-time copy;
/// the live store is never touched) and folds each staged `(node_id, embedding)`
/// vector in via `add_embedding`, so the Rank/kNN leg of an in-txn unified query
/// ranks over the txn's own uncommitted embeddings alongside the committed ones.
/// A later `add_embedding` for a `node_id` overwrites an earlier vector for the
/// same node, matching upsert semantics. Off-txn queries clone the committed store
/// WITHOUT this overlay, so a staged vector stays invisible until commit.
///
/// A staged vector should already be dimension-valid — `stage_vector` records it
/// unconditionally, but the authoritative commit path (`commit_crossmodal`) rejects a
/// mismatched dimension before it can ever land durably, so reaching a mismatch here
/// means the txn is about to fail at commit anyway. This is a best-effort READ-time
/// overlay, not the authoritative write: a mismatched staged vector is skipped (never
/// silently merged into the wrong-dimension arena) and logged, so this query's other,
/// valid results are still served rather than failing the whole read.
pub fn semantic_overlay(committed: SemanticStore, staged: &[(String, Vec<f32>)]) -> SemanticStore {
    let mut store = committed;
    for (node_id, embedding) in staged {
        if let Err(error) = store.add_embedding(node_id.clone(), embedding.clone()) {
            tracing::warn!(
                node_id,
                %error,
                "skipping staged vector from the read-your-own-writes overlay (CONCEPT:EG-KG.compute.rank-dim-mismatch-guard) — \
                 the txn's commit will reject it authoritatively"
            );
        }
    }
    store
}
