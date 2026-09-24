//! The independent-cascade (live-edge) model by seeded Monte Carlo.
//!
//! Each sample draws one live-edge world: every seed is active with its own
//! probability and every edge is live with its transmission probability, each
//! coin flipped at most once per world (a generation-stamped memo, so a world
//! costs only the edges it touches). A node is hit when an active seed reaches
//! it over live edges within the hop bound. Averaging over worlds is an
//! unbiased estimate of the exact reachability probability on any graph.

use std::collections::VecDeque;

use super::{clamp_unit, ImpactGraph, Seed, MAX_HOPS};
use crate::SplitMix64;

/// The two-sided normal quantile of a 95% interval.
const Z95: f64 = 1.959_963_984_540_054;

/// How a cascade estimate is sampled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CascadeSpec {
    pub hops: u32,
    pub samples: u32,
    pub seed: u64,
}

/// Per-node hit probabilities with 95% Wilson intervals, the expected number
/// of hit nodes with a normal interval, and per-seed Shapley attribution for
/// the requested targets (`attribution[t][s]`, summing to the target's
/// probability over `s`).
#[derive(Debug, Clone, PartialEq)]
pub struct CascadeResult {
    pub probability: Vec<f64>,
    pub lower: Vec<f64>,
    pub upper: Vec<f64>,
    pub expected_spread: f64,
    pub spread_lower: f64,
    pub spread_upper: f64,
    pub samples: u32,
    pub attribution: Vec<Vec<f64>>,
}

/// Lazily flipped, per-world edge coins.
struct Coins {
    /// `(world, live)` per edge slot; a stale world means "not yet flipped".
    flips: Vec<(u32, bool)>,
    /// First slot of each node's out-edges.
    offset: Vec<usize>,
    world: u32,
}

impl Coins {
    fn new(graph: &ImpactGraph) -> Self {
        let mut offset = Vec::with_capacity(graph.len() + 1);
        let mut total = 0;
        for u in 0..graph.len() {
            offset.push(total);
            total += graph.out_edges(u).len();
        }
        offset.push(total);
        Self {
            flips: vec![(u32::MAX, false); total],
            offset,
            world: 0,
        }
    }

    fn live(&mut self, u: usize, k: usize, p: f64, rng: &mut SplitMix64) -> bool {
        let slot = &mut self.flips[self.offset[u] + k];
        if slot.0 != self.world {
            *slot = (self.world, rng.next_f64() < p);
        }
        slot.1
    }
}

/// Monte Carlo independent cascade from `seeds`.
pub fn independent_cascade(
    graph: &ImpactGraph,
    seeds: &[Seed],
    targets: &[usize],
    spec: &CascadeSpec,
) -> CascadeResult {
    let n = graph.len();
    let seeds: Vec<Seed> = seeds.iter().copied().filter(|s| s.node < n).collect();
    let samples = spec.samples.max(1);
    let mut rng = SplitMix64::new(spec.seed);
    let mut coins = Coins::new(graph);
    let mut hits = vec![0u64; n];
    let mut attribution = vec![vec![0.0; seeds.len()]; targets.len()];
    let mut spreads = Vec::with_capacity(samples as usize);
    for world in 0..samples {
        coins.world = world;
        let reach = one_world(graph, &seeds, spec.hops.min(MAX_HOPS), &mut coins, &mut rng);
        spreads.push(tally_world(&reach, &mut hits) as f64);
        credit_targets(&reach, targets, &mut attribution);
    }
    finish(hits, spreads, attribution, samples)
}

/// Which seeds (by position) reach each node in one world.
fn one_world(
    graph: &ImpactGraph,
    seeds: &[Seed],
    hops: u32,
    coins: &mut Coins,
    rng: &mut SplitMix64,
) -> Vec<Vec<usize>> {
    let mut reach: Vec<Vec<usize>> = vec![Vec::new(); graph.len()];
    for (index, seed) in seeds.iter().enumerate() {
        if rng.next_f64() < clamp_unit(seed.probability) {
            for node in reached_from(graph, seed.node, hops, coins, rng) {
                reach[node].push(index);
            }
        }
    }
    reach
}

/// Nodes reached from `source` over live edges within `hops`, source included.
fn reached_from(
    graph: &ImpactGraph,
    source: usize,
    hops: u32,
    coins: &mut Coins,
    rng: &mut SplitMix64,
) -> Vec<usize> {
    let mut depth: Vec<Option<u32>> = vec![None; graph.len()];
    depth[source] = Some(0);
    let mut queue = VecDeque::from([source]);
    let mut reached = vec![source];
    while let Some(u) = queue.pop_front() {
        let next = depth[u].unwrap_or(hops) + 1;
        if next > hops {
            continue;
        }
        for (k, &(v, p)) in graph.out_edges(u).iter().enumerate() {
            if depth[v].is_none() && coins.live(u, k, p, rng) {
                depth[v] = Some(next);
                queue.push_back(v);
                reached.push(v);
            }
        }
    }
    reached
}

/// Count this world's hits; returns the number of hit nodes.
fn tally_world(reach: &[Vec<usize>], hits: &mut [u64]) -> usize {
    let mut spread = 0;
    for (node, seeds) in reach.iter().enumerate() {
        if !seeds.is_empty() {
            hits[node] += 1;
            spread += 1;
        }
    }
    spread
}

/// Credit each target's reaching seeds `1 / |R|` (the world's exact Shapley
/// values of the OR game over the seeds).
fn credit_targets(reach: &[Vec<usize>], targets: &[usize], attribution: &mut [Vec<f64>]) {
    for (row, &target) in targets.iter().enumerate() {
        let Some(seeds) = reach.get(target).filter(|s| !s.is_empty()) else {
            continue;
        };
        let share = 1.0 / seeds.len() as f64;
        for &seed in seeds {
            attribution[row][seed] += share;
        }
    }
}

fn finish(
    hits: Vec<u64>,
    spreads: Vec<f64>,
    mut attribution: Vec<Vec<f64>>,
    samples: u32,
) -> CascadeResult {
    let total = f64::from(samples);
    let (lower, upper): (Vec<f64>, Vec<f64>) = hits.iter().map(|&k| wilson(k, samples)).unzip();
    let probability = hits.iter().map(|&k| k as f64 / total).collect();
    for row in &mut attribution {
        row.iter_mut().for_each(|value| *value /= total);
    }
    let mean = spreads.iter().sum::<f64>() / total;
    let variance = if samples > 1 {
        spreads.iter().map(|s| (s - mean) * (s - mean)).sum::<f64>() / (total - 1.0)
    } else {
        0.0
    };
    let half = Z95 * (variance / total).sqrt();
    CascadeResult {
        probability,
        lower,
        upper,
        expected_spread: mean,
        spread_lower: (mean - half).max(0.0),
        spread_upper: mean + half,
        samples,
        attribution,
    }
}

/// The 95% Wilson score interval of `k` successes in `n` trials.
pub(crate) fn wilson(k: u64, n: u32) -> (f64, f64) {
    let n = f64::from(n);
    let p = k as f64 / n;
    let z2 = Z95 * Z95;
    let denominator = 1.0 + z2 / n;
    let centre = (p + z2 / (2.0 * n)) / denominator;
    let half = Z95 * (p * (1.0 - p) / n + z2 / (4.0 * n * n)).sqrt() / denominator;
    ((centre - half).max(0.0), (centre + half).min(1.0))
}
