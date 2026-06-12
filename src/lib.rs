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

pub mod algorithms;
pub mod ast;
#[cfg(feature = "server")]
pub mod channels;
pub mod compute;
#[cfg(feature = "datascience")]
pub mod datascience;
#[cfg(feature = "finance")]
pub mod finance;
pub mod graph;
#[cfg(feature = "server")]
pub mod isolation;
pub mod metrics;
pub mod parser;
#[cfg(feature = "server")]
pub mod persist;
#[cfg(feature = "server")]
pub mod persist_lock;
#[cfg(feature = "server")]
pub mod protocol;
#[cfg(feature = "reasoning")]
mod reasoning;
#[cfg(feature = "server")]
pub mod registry;
#[cfg(feature = "server")]
pub mod server;
pub mod types;
#[cfg(feature = "server")]
pub mod wal;
#[cfg(feature = "server")]
pub mod wal_service;
