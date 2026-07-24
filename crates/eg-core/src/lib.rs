//! eg-core — the graph engine core: in-memory storage (`graph`), the multi-tenant
//! `registry`, the ACL/`isolation` layer, and HNSW `compute::semantic` search.
//! Depends only on `eg-types`. Everything graph-mutating lives here; the wire
//! protocol and server transport sit in higher crates.
//!
//! Re-export the `eg-types` modules under the historical `crate::` paths so the
//! moved bodies (`crate::protocol::`, `crate::types::`, `crate::acl::`) resolve
//! unchanged.
pub use eg_types::{acl, protocol, types, wire};

/// Shared test-only serialization lock for the process-global indexed-property env
/// vars (`EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES` / `EPISTEMIC_GRAPH_INDEXED_PROPERTIES`).
/// The `graph` property-index tests AND the `index` manager cap test both mutate these
/// globals, so both acquire THIS one lock — otherwise a cap-mutating test races a
/// default-cap test under parallel `--all-features` (a torn `nodes_by_property` read).
#[cfg(test)]
pub(crate) static PROP_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// CONCEPT:EG-KG.storage.bloom-negative-lookup-guard — lock-free per-graph Bloom
/// filter guarding the durable read-through negative-lookup path. Pure-Rust,
/// always compiled (no feature gate — the default/Pi build already links it).
pub mod bloom;
// CONCEPT:EG-KG.compute.message-broker-exchanges — message-broker exchanges/routing on top of the KG-2.303 queue.
#[cfg(feature = "broker")]
pub mod broker;
#[cfg(feature = "cold-tier")]
pub mod cold_tier;
pub mod compute;
/// ModalityContract retrofit (CONCEPT:E4): `impl ModalityContract for
/// index::NodeChange` + the `modality_conformance_tests!` battery. Behind the
/// crate's own opt-in `contract` feature (default OFF). See `src/contract.rs`.
#[cfg(feature = "contract")]
mod contract;
pub mod decay;
/// CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation (W1.6/P7) — the per-graph
/// dependency clock backing the result cache's dependency-scoped invalidation: a write
/// bumps only the label / property-key / node / edge dimensions it actually touched, so a
/// cached query result survives every write whose change-set is DISJOINT from the query's
/// dependency set. Gated with the result cache it serves.
#[cfg(feature = "result-cache")]
pub mod dep_scope;
pub mod graph;
pub mod index;
pub mod isolation;
/// CONCEPT:EG-KG.compute.jsonpath-evaluator — pure-Rust JSONPath evaluator + Postgres-`@>` containment.
pub mod jsonpath;
/// CONCEPT:EG-KG.storage.path-index-store — durable JSONPath index persistence. The dep-free
/// `PathIndexPersistence` seam + in-memory default are always compiled; the
/// redb-backed `RedbPathIndexStore` is gated behind the `path-persist` feature.
pub mod path_persist;
/// CONCEPT:EG-KG.query.named-graph-projection-catalog (W4.5 / N5) — the `gds.graph.project`-
/// equivalent named/materialized graph-projection catalog, invalidated via the SAME `dep_scope`
/// `DepClock` the result cache's dependency-scoped entries use. Gated with it (needs the clock).
#[cfg(feature = "result-cache")]
pub mod projection_catalog;
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
