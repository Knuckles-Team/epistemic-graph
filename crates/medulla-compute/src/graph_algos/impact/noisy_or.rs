//! The noisy-OR impact model over the seeds' downstream cone.

use std::collections::VecDeque;

use super::{hop_depths, seed_vector, ImpactGraph, Seed, Semantics, MAX_HOPS};
use crate::union_find_root;

/// Per-node impact probabilities and what they mean.
#[derive(Debug, Clone, PartialEq)]
pub struct NoisyOrResult {
    /// `P(node is hit)`, zero outside the cone.
    pub probability: Vec<f64>,
    /// Breadth-first hop depth from the nearest seed; `None` outside the cone.
    pub depth: Vec<Option<u32>>,
    pub semantics: Semantics,
    /// Rounds the recurrence ran (the longest cone path on a single pass).
    pub rounds: u32,
}

/// Noisy-OR impact of `seeds` within `hops` (capped at [`MAX_HOPS`]).
///
/// An acyclic cone whose longest path fits the bound is evaluated in one
/// topological pass; otherwise the recurrence is unrolled round by round (each
/// round is one more hop), stopping early at its fixed point.
pub fn noisy_or(graph: &ImpactGraph, seeds: &[Seed], hops: u32) -> NoisyOrResult {
    let hops = hops.min(MAX_HOPS);
    let prior = seed_vector(graph.len(), seeds);
    let depth = hop_depths(graph, seeds, hops);
    let cone: Vec<bool> = depth.iter().map(Option::is_some).collect();
    let order = topological_cone(graph, &cone);
    let semantics = match &order {
        None => Semantics::CyclicUnroll,
        Some(_) if is_polytree(graph, &cone) => Semantics::Exact,
        Some(_) => Semantics::UpperBound,
    };
    let single_pass = order
        .map(|order| (longest_path(graph, &cone, &order), order))
        .filter(|(longest, _)| *longest <= hops);
    let (probability, rounds) = match single_pass {
        Some((longest, order)) => (topological_pass(graph, &prior, &cone, &order), longest),
        None => layered(graph, &prior, &cone, hops),
    };
    NoisyOrResult {
        probability,
        depth,
        semantics,
        rounds,
    }
}

/// `1 - (1 - prior) * prod_{u -> v, u in cone} (1 - p(u,v) value(u))`.
fn combine(graph: &ImpactGraph, cone: &[bool], prior: f64, v: usize, value: &[f64]) -> f64 {
    let miss: f64 = graph
        .in_edges(v)
        .iter()
        .filter(|(u, _)| cone[*u])
        .map(|&(u, p)| 1.0 - p * value[u])
        .product();
    1.0 - (1.0 - prior) * miss
}

/// Kahn's order over the cone's induced subgraph; `None` when it has a cycle.
fn topological_cone(graph: &ImpactGraph, cone: &[bool]) -> Option<Vec<usize>> {
    let mut indegree = vec![0usize; graph.len()];
    for v in (0..graph.len()).filter(|v| cone[*v]) {
        indegree[v] = graph.in_edges(v).iter().filter(|(u, _)| cone[*u]).count();
    }
    let mut ready: VecDeque<usize> = (0..graph.len())
        .filter(|v| cone[*v] && indegree[*v] == 0)
        .collect();
    let mut order = Vec::new();
    while let Some(u) = ready.pop_front() {
        order.push(u);
        for &(v, _) in graph.out_edges(u).iter().filter(|(v, _)| cone[*v]) {
            indegree[v] -= 1;
            if indegree[v] == 0 {
                ready.push_back(v);
            }
        }
    }
    let size = cone.iter().filter(|c| **c).count();
    (order.len() == size).then_some(order)
}

/// Whether the cone's induced subgraph has no undirected cycle (a polytree),
/// the condition under which every node's parents are independent.
fn is_polytree(graph: &ImpactGraph, cone: &[bool]) -> bool {
    let mut parent: Vec<usize> = (0..graph.len()).collect();
    for u in (0..graph.len()).filter(|u| cone[*u]) {
        for &(v, _) in graph.out_edges(u).iter().filter(|(v, _)| cone[*v]) {
            let (ru, rv) = (
                union_find_root(&mut parent, u),
                union_find_root(&mut parent, v),
            );
            if ru == rv {
                return false;
            }
            parent[ru] = rv;
        }
    }
    true
}

/// Edges on the longest path of the (acyclic) cone.
fn longest_path(graph: &ImpactGraph, cone: &[bool], order: &[usize]) -> u32 {
    let mut length = vec![0u32; graph.len()];
    for &u in order {
        for &(v, _) in graph.out_edges(u).iter().filter(|(v, _)| cone[*v]) {
            length[v] = length[v].max(length[u] + 1);
        }
    }
    order.iter().map(|&v| length[v]).max().unwrap_or(0)
}

fn topological_pass(
    graph: &ImpactGraph,
    prior: &[f64],
    cone: &[bool],
    order: &[usize],
) -> Vec<f64> {
    let mut value = vec![0.0; graph.len()];
    for &v in order {
        value[v] = combine(graph, cone, prior[v], v, &value);
    }
    value
}

/// Round-by-round unrolling: after round `h` a node holds the probability of
/// being hit within `h` hops (under the independence the model assumes).
fn layered(graph: &ImpactGraph, prior: &[f64], cone: &[bool], hops: u32) -> (Vec<f64>, u32) {
    let mut current: Vec<f64> = prior
        .iter()
        .zip(cone)
        .map(|(p, inside)| if *inside { *p } else { 0.0 })
        .collect();
    for round in 1..=hops {
        let next: Vec<f64> = (0..graph.len())
            .map(|v| {
                if cone[v] {
                    combine(graph, cone, prior[v], v, &current)
                } else {
                    0.0
                }
            })
            .collect();
        if next == current {
            return (current, round - 1);
        }
        current = next;
    }
    (current, hops)
}
