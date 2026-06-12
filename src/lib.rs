#![allow(non_local_definitions)]
#![allow(dead_code)]

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
pub mod datascience;
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
