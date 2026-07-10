// Compute primitives built ahead of their wire exposure (e.g. parametric_var,
// inv_norm) are kept; matches the facade's long-standing crate-level posture.
#![allow(dead_code)]

//! eg-compute — the compute domains layered above the graph core: always-on graph
//! `algorithms` + `ast`/`parser` (their tree-sitter parts gated by `ast`), plus the
//! feature-gated `finance`, `datascience`, and `reasoning` domains. Depends on
//! `eg-types` and `eg-core`, never on `eg-server`.
//!
//! Re-export the lower crates under the historical `crate::` paths so the moved
//! bodies (`crate::graph::`, `crate::types::`, `crate::wire::`) resolve unchanged.
pub use eg_core::{compute, graph, isolation, registry};
pub use eg_types::{acl, protocol, types, wire};

pub mod algorithms;
// CONCEPT:EG-KG.compute.graph-data-science-algorithms — standalone graph data-science algorithms (Neo4j GDS parity).
// Pure-Rust, deterministic, generic over node id; decoupled from the live engine
// graph so it is unit-testable in isolation. Always-on (no heavy deps).
pub mod ast;
pub mod graph_algos;
pub mod parser;
pub mod screen;

// ModalityContract retrofit (CONCEPT:E4): `impl ModalityContract for MergeProposal`,
// behind the crate's own opt-in `contract` feature (default OFF). See `src/contract.rs`.
#[cfg(feature = "contract")]
mod contract;

#[cfg(feature = "datascience")]
pub mod datascience;
#[cfg(feature = "finance")]
pub mod finance;
// CONCEPT:EG-KG.mining.frequent-itemset-mining — descriptive data-mining domain
// (frequent itemsets + association rules). Pure-Rust, dependency-light, batch;
// graph-agnostic (works over interned item ids), so it is unit-testable in
// isolation. Feature-gated like finance/datascience so a slim build drops it.
#[cfg(feature = "mining")]
pub mod mining;
// CONCEPT:EG-KG.graphlearn.link-predictor — graph-learning / neuro-symbolic domain.
// A pure-Rust KAN (Kolmogorov-Arnold) link-predictor over the resident graph: a
// polynomial-basis learnable edge function (`edge_fn`), a 1–2 layer KAN link-scorer
// over structural features (`link_predict`), and 1-hop neighbor aggregation
// (`neighbor_aggregate`). Graph-agnostic (works over `graph_algos::AdjacencyGraph`),
// so it is unit-testable in isolation. Feature-gated like mining; implies
// `datascience` for the shared Adam/SGD training kernels.
#[cfg(feature = "graphlearn")]
pub mod graphlearn;
// CONCEPT:EG-KG.compute.bayesian-fusion-helpers — Bayesian-update / mixture / fusion helpers over the
// `eg_types::Distribution` value. Conjugate posteriors are closed-form (no
// sampling), so this rides the pure `reasoning` feature (no heavy dep).
#[cfg(feature = "reasoning")]
pub mod probabilistic;
#[cfg(feature = "reasoning")]
pub mod reasoning;
