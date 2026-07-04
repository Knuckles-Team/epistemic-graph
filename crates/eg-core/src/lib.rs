//! eg-core — the graph engine core: in-memory storage (`graph`), the multi-tenant
//! `registry`, the ACL/`isolation` layer, and HNSW `compute::semantic` search.
//! Depends only on `eg-types`. Everything graph-mutating lives here; the wire
//! protocol and server transport sit in higher crates.
//!
//! Re-export the `eg-types` modules under the historical `crate::` paths so the
//! moved bodies (`crate::protocol::`, `crate::types::`, `crate::acl::`) resolve
//! unchanged.
pub use eg_types::{acl, protocol, types, wire};

// CONCEPT:EG-KG.compute.message-broker-exchanges — message-broker exchanges/routing on top of the KG-2.303 queue.
#[cfg(feature = "broker")]
pub mod broker;
#[cfg(feature = "cold-tier")]
pub mod cold_tier;
pub mod compute;
pub mod decay;
pub mod graph;
pub mod index;
pub mod isolation;
/// CONCEPT:EG-KG.compute.jsonpath-evaluator — pure-Rust JSONPath evaluator + Postgres-`@>` containment.
pub mod jsonpath;
/// CONCEPT:EG-KG.storage.path-index-store — durable JSONPath index persistence. The dep-free
/// `PathIndexPersistence` seam + in-memory default are always compiled; the
/// redb-backed `RedbPathIndexStore` is gated behind the `path-persist` feature.
pub mod path_persist;
#[cfg(feature = "security")]
pub mod rbac;
/// CONCEPT:EG-KG.compute.durable-rbac-identity-persistence — durable RBAC/identity persistence (redb-backed, feature `security`).
#[cfg(feature = "security")]
pub mod rbac_persist;
pub mod read_through;
pub mod registry;
#[cfg(feature = "result-cache")]
pub mod result_cache;
/// CONCEPT:EG-KG.compute.scene-graph-primitives — scene-graph / 3D world-model primitives (poses, transform
/// composition, AABBs). Pure deterministic math; the `GraphCore` scene methods
/// (`add_scene_object`, `world_transform`, spatial relations) live in `graph`.
pub mod scene;
