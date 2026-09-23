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

/// Advance a SplitMix64 state by one step.
///
/// Compute domains use the same mixing algorithm while retaining their own
/// seed conventions. Keeping only the state transition here preserves each
/// caller's stream and gives the implementation one canonical home.
pub(crate) fn splitmix64_next(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The dependency-free SplitMix64 stream shared by the always-on `graph_algos`
/// kernels (`louvain::visit_order`, `random_walk`, and the `similarity`
/// NN-descent sampler), each of which previously carried its own byte-identical
/// copy. `new` seeds the state directly and every draw advances via
/// [`splitmix64_next`], so each caller's stream is bit-for-bit what its local
/// copy produced — the seeded reproducibility those algorithms document is
/// preserved exactly.
pub(crate) struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub(crate) fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        splitmix64_next(&mut self.state)
    }

    /// A uniform `f64` in `[0, 1)`, via the top 53 bits (the standard
    /// integer-to-double technique — full `f64` mantissa precision).
    pub(crate) fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    /// Uniform integer in `[0, bound)`; `bound` must be positive.
    pub(crate) fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

pub mod algorithms;
pub(crate) mod node_labels;
// CONCEPT:EG-KG.compute.graph-data-science-algorithms — standalone graph data-science algorithms (Neo4j GDS parity).
// Pure-Rust, deterministic, generic over node id; decoupled from the live engine
// graph so it is unit-testable in isolation. Always-on (no heavy deps).
pub mod ast;
pub mod graph_algos;
pub mod parser;
pub mod screen;
// CONCEPT:EG-KG.compute.approximate-sketches (W4.5/N5) — HyperLogLog / Count-Min Sketch /
// MinHash. Pure-Rust (only `std`), deterministic given the same inserted items — the lowest
// common DAG ancestor of `eg-plan` (PlanStats cardinality inputs) and `eg-query` (SQL aggregate
// UDFs), which is why it lives here rather than in either consumer. Always-on (no heavy deps).
pub mod sketch;

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
// Decide layer package A3 — general bounded 0-1 integer programming: validated
// models, deterministic node-budgeted branch-and-bound, Lagrangian leaf-bound
// certificates and an independent verifier. Integer-only, no heavy deps.
#[cfg(feature = "solve")]
pub mod solve;
// Decide layer packages A2/A5 — the assembly decision function: step-1b
// eliminations, the typed model over the legal remainder, the solve with its
// no-good loop, why-not re-solves and the sealed, replayable record.
#[cfg(feature = "decide")]
pub mod assemble;
// CONCEPT:EG-KG.compute.reasoning-closure-gpu — semi-naive integer-interned rewrite of
// the `reasoning` fixpoint, with the transitive-closure join factored behind a
// `ClosureBackend` seam (CPU always-on + feature-gated CUDA kernel). Rides `reasoning`;
// the CUDA leg is further gated by `gpu-cuda`.
#[cfg(feature = "reasoning")]
pub mod reasoning_closure;
