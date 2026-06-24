// CONCEPT:KG-2.22b / KG-2.207 — SemanticStore backend selector.
//
// `compute::semantic::SemanticStore` is the embedding store the graph holds. Two
// interchangeable backends share one public API and one on-disk serde shape:
//   * DEFAULT — `semantic_hnsw`: the hnsw_rs index (rebuilt from raw vectors on
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
