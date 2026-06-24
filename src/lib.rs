#![allow(non_local_definitions)]
#![allow(dead_code)]
// No unsafe anywhere except the single tree-sitter FFI block (ast/parser.rs),
// which carries a scoped #[allow(unsafe_code)] with a soundness note. Any new
// unsafe is a compile error until it is explicitly justified the same way.
#![deny(unsafe_code)]

// CONCEPT:KG-2.16 - High-Performance Graph Compute Engine
// CONCEPT:ORCH-1.29 - Compiled Orchestration Kernel
// CONCEPT:KG-2.19 - Tokio Service Layer
//
// Tokio service layer handling MessagePack RPC.
// All logic delegated to graph, algorithms, and reasoning modules.
// Service-layer modules (protocol, registry, isolation, channels, server)
// are used by the epistemic-graph-server binary.

// Bottom-of-DAG wire types live in `eg-types`; the graph core (storage, registry,
// isolation, semantic search) lives in `eg-core`. Re-export both under the
// historical `crate::` paths so every module's `crate::protocol::` /
// `crate::graph::` / `crate::registry::` reference resolves unchanged.
pub use eg_core::{compute, decay, graph, index, isolation, registry};
pub use eg_types::{acl, protocol, types, wire};
// Compute domains live in `eg-compute`; re-export under the historical `crate::`
// paths. algorithms/ast/parser are always present; the feature-gated domains are
// re-exported only when their facade feature (→ eg-compute feature) is on.
#[cfg(feature = "datascience")]
pub use eg_compute::datascience;
#[cfg(feature = "finance")]
pub use eg_compute::finance;
#[cfg(feature = "reasoning")]
pub use eg_compute::reasoning;
pub use eg_compute::{algorithms, ast, parser, screen};

#[cfg(feature = "server")]
pub mod channels;
pub mod metrics;
#[cfg(feature = "server")]
pub mod persist;
#[cfg(feature = "server")]
pub mod persist_lock;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "server")]
pub mod wal;
#[cfg(feature = "server")]
pub mod wal_service;
// Per-graph write coalescer (CONCEPT:KG-2.182): batches concurrent single-op
// writes to one graph into a single topology-lock acquisition. Tokio-based, so it
// lives in the server-gated top-level crate alongside wal_service.
#[cfg(feature = "server")]
pub mod write_coalescer;
// In-engine Raft replication (CONCEPT:KG-2.188) — cluster tier only, behind the
// `raft` feature. A default/pi/full build links no openraft. The module is
// self-`#![cfg(feature = "raft")]`-gated; this `mod` line is also gated so it does
// not even appear in a non-raft build.
#[cfg(feature = "raft")]
pub mod raft;
