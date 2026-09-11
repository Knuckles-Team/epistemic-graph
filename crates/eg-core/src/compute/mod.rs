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

/// RF-019's callable semantic service: the S1-S6 tiered ingestion orchestration
/// over the durable ANN code tier above. It carries its own
/// `#![cfg(feature = "ann-redb")]`, so it needs no gate here.
pub mod semantic_index_service;

/// RF-RULING-007 layer (c): the call-graph gate over `crates/eg-ann/src` and
/// this directory. Test-only and NOT feature-gated -- the whole point is that
/// it runs in every build that runs tests, including one with no storage
/// feature enabled at all.
#[cfg(test)]
mod semantic_arch_gate;
