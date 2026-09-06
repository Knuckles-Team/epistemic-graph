// CONCEPT:EG-KG.compute.compute-modules — Compute Modules
//
// Core compute primitives. `semantic` is the embedding store: a brute-force cosine
// path for tiny stores, and the native `eg-ann` index for large ones, with
// the native eg-ann IVF-PQ+OPQ+SQ8-refine index (CONCEPT:EG-KG.sharding.semantic-embedding-store-backed) under the `ann`
// feature, which reopens a persisted index WITHOUT rebuilding from raw vectors.

pub mod semantic;

#[cfg(feature = "ann")]
pub mod semantic_ann;

/// Durable ANN code tier: one index generation's buffers in the `eg_ann` owner
/// table of `OwnerLayout::SemanticIndex`, written as one admitted
/// `Native(SemanticIndex)` mutation (RF-RULING-007).
#[cfg(feature = "ann-redb")]
pub mod semantic_ann_codes;
