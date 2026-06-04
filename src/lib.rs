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
pub mod graph;
mod reasoning;
pub mod types;
#[cfg(feature = "server")]
pub mod protocol;
#[cfg(feature = "server")]
pub mod registry;
#[cfg(feature = "server")]
pub mod isolation;
#[cfg(feature = "server")]
pub mod channels;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "kafka")]
pub mod event_bus;
pub mod parser;
pub mod compute;
pub mod finance;
pub mod ast;
pub mod datascience;
