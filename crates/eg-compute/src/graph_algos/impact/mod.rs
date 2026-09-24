// CONCEPT:EG-KG.mining.risk-propagation — probabilistic impact propagation (EH-526).
//
//! Probabilistic impact propagation: "if these nodes are hit, what is the
//! probability that each other node is hit?" (ANALYTICS-HARVEST AH-06).
//!
//! Personalised PageRank (`mining::risk_propagation`) answers a different
//! question — each node's *share* of propagated risk mass, which sums to one:
//! ten independent paths into a node divide the mass instead of raising the
//! node's probability toward one. The models here return a probability per node:
//!
//! * [`noisy_or`] — the noisy-OR recurrence
//!   `P(v) = 1 - (1 - s_v) * prod_{u -> v} (1 - p(u,v) P(u))` over the seeds'
//!   downstream cone. On a cone that is a polytree it is the exact live-edge
//!   reachability probability ([`Semantics::Exact`]); on any other DAG the
//!   parents are positively correlated (Harris/FKG), so it is an upper bound
//!   ([`Semantics::UpperBound`]); on a cyclic cone it is unrolled for at most
//!   `hops` rounds ([`Semantics::CyclicUnroll`], also an upper bound). The
//!   semantics travel with the result — nothing is silently approximate.
//! * [`independent_cascade`] — seeded Monte Carlo of the independent-cascade
//!   (live-edge) model: an unbiased estimate of the exact reachability
//!   probability on any graph, with Wilson intervals per node and a normal
//!   interval on the expected spread. Per-seed Shapley attribution is exact for
//!   the sampled game: in each world a target reached by the seed set `R`
//!   credits `1 / |R|` to each member, so the attributions sum to the node's
//!   probability for any number of seeds.
//! * [`strongest_paths`] — the most probable seed-to-node path within the hop
//!   bound (max-product, layered so the bound is honoured).
//! * [`noisy_or_attribution`] — exact Shapley values of the seeds for the
//!   noisy-OR value game (`2^s` coalitions, bounded by [`MAX_EXACT_SEEDS`]).
//!
//! Edge weights are transmission probabilities, clamped to `[0, 1]`; parallel
//! edges `u -> v` are independent channels and combine as `1 - prod(1 - p)`.
//! Every loop runs over sorted indices, and the only randomness is the seeded
//! SplitMix64 stream, so a result replays bit-identically.

mod cascade;
mod noisy_or;
mod paths;
mod shapley;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, VecDeque};

pub use cascade::{independent_cascade, CascadeResult, CascadeSpec};
pub use noisy_or::{noisy_or, NoisyOrResult};
pub use paths::{strongest_paths, ImpactPath};
pub use shapley::{noisy_or_attribution, MAX_EXACT_SEEDS};

/// Hop bound ceiling: a propagation never unrolls past this many rounds.
pub const MAX_HOPS: u32 = 64;

/// What a noisy-OR probability means on the cone it was computed over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Semantics {
    /// Polytree cone: the exact live-edge reachability probability.
    Exact,
    /// A DAG with shared ancestry: an upper bound on it.
    UpperBound,
    /// A cyclic cone, unrolled for the hop bound: an upper bound on it.
    CyclicUnroll,
}

/// One seed: a node already hit with `probability`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Seed {
    pub node: usize,
    pub probability: f64,
}

/// A directed graph of transmission probabilities over nodes `0..n`.
#[derive(Debug, Clone, PartialEq)]
pub struct ImpactGraph {
    out: Vec<Vec<(usize, f64)>>,
    inc: Vec<Vec<(usize, f64)>>,
}

impl ImpactGraph {
    /// Build from `(from, to, transmission)` triples. Out-of-range endpoints
    /// and self-loops are dropped; parallel edges combine as independent
    /// channels.
    pub fn new(n: usize, edges: &[(usize, usize, f64)]) -> Self {
        let mut merged: BTreeMap<(usize, usize), f64> = BTreeMap::new();
        for &(u, v, p) in edges {
            if u >= n || v >= n || u == v {
                continue;
            }
            let miss = merged.entry((u, v)).or_insert(1.0);
            *miss *= 1.0 - clamp_unit(p);
        }
        let mut out = vec![Vec::new(); n];
        let mut inc = vec![Vec::new(); n];
        for ((u, v), miss) in merged {
            let p = 1.0 - miss;
            if p > 0.0 {
                out[u].push((v, p));
                inc[v].push((u, p));
            }
        }
        Self { out, inc }
    }

    /// Number of nodes.
    pub fn len(&self) -> usize {
        self.out.len()
    }

    /// Whether the graph has no nodes.
    pub fn is_empty(&self) -> bool {
        self.out.is_empty()
    }

    /// Outgoing `(to, p)` of `u`, sorted by `to`.
    pub fn out_edges(&self, u: usize) -> &[(usize, f64)] {
        &self.out[u]
    }

    fn in_edges(&self, v: usize) -> &[(usize, f64)] {
        &self.inc[v]
    }
}

/// `p` clamped into `[0, 1]`; NaN counts as zero.
pub(crate) fn clamp_unit(p: f64) -> f64 {
    if p.is_nan() {
        0.0
    } else {
        p.clamp(0.0, 1.0)
    }
}

/// Seed probability per node (`0` for non-seeds); repeated seeds combine as
/// independent hits. Out-of-range seeds are dropped.
pub(crate) fn seed_vector(n: usize, seeds: &[Seed]) -> Vec<f64> {
    let mut miss = vec![1.0; n];
    for seed in seeds.iter().filter(|s| s.node < n) {
        miss[seed.node] *= 1.0 - clamp_unit(seed.probability);
    }
    miss.into_iter().map(|m| 1.0 - m).collect()
}

/// Breadth-first hop depth from every positive seed, `None` outside the cone
/// or beyond `hops`.
pub fn hop_depths(graph: &ImpactGraph, seeds: &[Seed], hops: u32) -> Vec<Option<u32>> {
    let hops = hops.min(MAX_HOPS);
    let prior = seed_vector(graph.len(), seeds);
    let mut depth: Vec<Option<u32>> = vec![None; graph.len()];
    let mut queue = VecDeque::new();
    for (node, p) in prior.iter().enumerate() {
        if *p > 0.0 {
            depth[node] = Some(0);
            queue.push_back(node);
        }
    }
    while let Some(u) = queue.pop_front() {
        let next = depth[u].unwrap_or(hops) + 1;
        if next > hops {
            continue;
        }
        for &(v, _) in graph.out_edges(u) {
            if depth[v].is_none() {
                depth[v] = Some(next);
                queue.push_back(v);
            }
        }
    }
    depth
}
