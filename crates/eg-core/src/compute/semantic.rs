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

/// Build an OVERLAY embedding store for in-txn cross-modal read-your-own-writes
/// (CONCEPT:EG-KG.query.txn-cross-modal-ryow). Clones the committed `SemanticStore` (a point-in-time copy;
/// the live store is never touched) and folds each staged `(node_id, embedding)`
/// vector in via `add_embedding`, so the Rank/kNN leg of an in-txn unified query
/// ranks over the txn's own uncommitted embeddings alongside the committed ones.
/// A later `add_embedding` for a `node_id` overwrites an earlier vector for the
/// same node, matching upsert semantics. Off-txn queries clone the committed store
/// WITHOUT this overlay, so a staged vector stays invisible until commit.
pub fn semantic_overlay(committed: SemanticStore, staged: &[(String, Vec<f32>)]) -> SemanticStore {
    let mut store = committed;
    for (node_id, embedding) in staged {
        store.add_embedding(node_id.clone(), embedding.clone());
    }
    store
}
