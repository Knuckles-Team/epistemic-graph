//! eg-core — the graph engine core: in-memory storage (`graph`), the multi-tenant
//! `registry`, the ACL/`isolation` layer, and HNSW `compute::semantic` search.
//! Depends only on `eg-types`. Everything graph-mutating lives here; the wire
//! protocol and server transport sit in higher crates.
//!
//! Re-export the `eg-types` modules under the historical `crate::` paths so the
//! moved bodies (`crate::protocol::`, `crate::types::`, `crate::acl::`) resolve
//! unchanged.
pub use eg_types::{acl, protocol, types, wire};

#[cfg(feature = "cold-tier")]
pub mod cold_tier;
pub mod compute;
pub mod decay;
pub mod graph;
pub mod index;
/// CONCEPT:EG-084 — pure-Rust JSONPath evaluator + Postgres-`@>` containment.
pub mod jsonpath;
pub mod isolation;
#[cfg(feature = "security")]
pub mod rbac;
pub mod read_through;
pub mod registry;
#[cfg(feature = "result-cache")]
pub mod result_cache;
