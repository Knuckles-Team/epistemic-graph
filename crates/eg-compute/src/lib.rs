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
pub mod ast;
pub mod parser;
pub mod screen;

#[cfg(feature = "datascience")]
pub mod datascience;
#[cfg(feature = "finance")]
pub mod finance;
// CONCEPT:EG-086 — Bayesian-update / mixture / fusion helpers over the
// `eg_types::Distribution` value. Conjugate posteriors are closed-form (no
// sampling), so this rides the pure `reasoning` feature (no heavy dep).
#[cfg(feature = "reasoning")]
pub mod probabilistic;
#[cfg(feature = "reasoning")]
pub mod reasoning;
