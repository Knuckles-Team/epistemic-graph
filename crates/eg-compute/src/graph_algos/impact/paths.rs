//! The most probable seed-to-node path within the hop bound.
//!
//! A path's probability is the seed's probability times each edge's
//! transmission. Layer `h` holds, per node, the best path of exactly `h`
//! edges (its probability and predecessor), so the bound is honoured exactly
//! and the path is rebuilt by walking the predecessors back down the layers.

use super::{seed_vector, ImpactGraph, Seed, MAX_HOPS};

/// One path, seed first.
#[derive(Debug, Clone, PartialEq)]
pub struct ImpactPath {
    pub nodes: Vec<usize>,
    pub probability: f64,
}

/// `(probability, predecessor)` per node in one layer.
type Layer = Vec<Option<(f64, usize)>>;

/// The strongest path into every node (`None` where no seed reaches it within
/// `hops`). A seed's own entry is the zero-edge path `[seed]`.
pub fn strongest_paths(graph: &ImpactGraph, seeds: &[Seed], hops: u32) -> Vec<Option<ImpactPath>> {
    let hops = hops.min(MAX_HOPS) as usize;
    let prior = seed_vector(graph.len(), seeds);
    let first: Layer = prior
        .iter()
        .enumerate()
        .map(|(node, p)| (*p > 0.0).then_some((*p, node)))
        .collect();
    let mut layers = vec![first];
    for _ in 0..hops {
        let next = relax(graph, &layers[layers.len() - 1]);
        if next.iter().all(Option::is_none) {
            break;
        }
        layers.push(next);
    }
    (0..graph.len()).map(|v| rebuild(&layers, v)).collect()
}

/// One more edge on every path of `layer`.
fn relax(graph: &ImpactGraph, layer: &Layer) -> Layer {
    let mut next: Layer = vec![None; graph.len()];
    for (u, entry) in layer.iter().enumerate() {
        let Some((pu, _)) = entry else { continue };
        for &(v, p) in graph.out_edges(u) {
            let candidate = pu * p;
            if next[v].is_none_or(|(best, _)| candidate > best) {
                next[v] = Some((candidate, u));
            }
        }
    }
    next
}

/// The best layer for `v` (ties go to the shorter path) and its path.
fn rebuild(layers: &[Layer], v: usize) -> Option<ImpactPath> {
    let (depth, probability) = layers
        .iter()
        .enumerate()
        .filter_map(|(h, layer)| layer[v].map(|(p, _)| (h, p)))
        .fold(None, |best: Option<(usize, f64)>, (h, p)| match best {
            Some((_, bp)) if bp >= p => best,
            _ => Some((h, p)),
        })?;
    let mut nodes = vec![v];
    let mut at = v;
    for h in (1..=depth).rev() {
        let (_, pred) = layers[h][at]?;
        nodes.push(pred);
        at = pred;
    }
    nodes.reverse();
    Some(ImpactPath { nodes, probability })
}
