// CONCEPT:KG-2.22 — Compute Modules
//
// Core compute primitives. `semantic` is the embedding store: a brute-force cosine
// path for tiny stores, and an ANN index for large ones — `hnsw_rs` by default, or
// the native eg-ann IVF-PQ+OPQ+SQ8-refine index (CONCEPT:KG-2.207) under the `ann`
// feature, which reopens a persisted index WITHOUT rebuilding from raw vectors.

pub mod semantic;

#[cfg(feature = "ann")]
pub mod semantic_ann;
