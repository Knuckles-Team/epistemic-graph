//! eg-core — the graph engine core: in-memory storage (`graph`), the multi-tenant
//! `registry`, the ACL/`isolation` layer, and HNSW `compute::semantic` search.
//! Depends only on `eg-types`. Everything graph-mutating lives here; the wire
//! protocol and server transport sit in higher crates.
//!
//! Re-export the `eg-types` modules under the historical `crate::` paths so the
//! moved bodies (`crate::protocol::`, `crate::types::`, `crate::acl::`) resolve
//! unchanged.
pub use eg_types::{acl, protocol, types, wire};

// CONCEPT:EG-275 — message-broker exchanges/routing on top of the KG-2.303 queue.
#[cfg(feature = "broker")]
pub mod broker;
#[cfg(feature = "cold-tier")]
pub mod cold_tier;
pub mod compute;
pub mod decay;
pub mod graph;
pub mod index;
pub mod isolation;
/// CONCEPT:EG-084 — pure-Rust JSONPath evaluator + Postgres-`@>` containment.
pub mod jsonpath;
#[cfg(feature = "security")]
pub mod rbac;
pub mod read_through;
pub mod registry;
#[cfg(feature = "result-cache")]
pub mod result_cache;
/// CONCEPT:EG-087 — scene-graph / 3D world-model primitives (poses, transform
/// composition, AABBs). Pure deterministic math; the `GraphCore` scene methods
/// (`add_scene_object`, `world_transform`, spatial relations) live in `graph`.
pub mod scene;
