//! Oracles: brute-force enumeration of live-edge worlds (graphs of at most 12
//! edges), monotonicity, the hop bound, Monte Carlo convergence, strongest
//! paths and the Shapley axioms.

use super::*;

/// The exact probability that each node is reached within `hops`, by
/// enumerating every live-edge world and every seed activation.
fn brute_force(n: usize, edges: &[(usize, usize, f64)], seeds: &[Seed], hops: u32) -> Vec<f64> {
    let e = edges.len();
    let s = seeds.len();
    assert!(e + s <= 16, "oracle is exponential");
    let mut exact = vec![0.0; n];
    for world in 0..1usize << (e + s) {
        let (edge_weight, live) = live_edges(world, edges);
        let (weight, active) = active_seeds(world >> e, seeds, edge_weight);
        for (node, hit) in reached(n, &live, &active, hops).into_iter().enumerate() {
            if hit {
                exact[node] += weight;
            }
        }
    }
    exact
}

fn live_edges(world: usize, edges: &[(usize, usize, f64)]) -> (f64, Vec<(usize, usize)>) {
    let mut weight = 1.0;
    let mut live = Vec::new();
    for (i, &(u, v, p)) in edges.iter().enumerate() {
        let on = world & (1 << i) != 0;
        weight *= if on { p } else { 1.0 - p };
        if on {
            live.push((u, v));
        }
    }
    (weight, live)
}

fn active_seeds(world: usize, seeds: &[Seed], mut weight: f64) -> (f64, Vec<usize>) {
    let mut active = Vec::new();
    for (i, seed) in seeds.iter().enumerate() {
        let on = world & (1 << i) != 0;
        weight *= if on {
            seed.probability
        } else {
            1.0 - seed.probability
        };
        if on {
            active.push(seed.node);
        }
    }
    (weight, active)
}

fn reached(n: usize, live: &[(usize, usize)], active: &[usize], hops: u32) -> Vec<bool> {
    let mut hit = vec![false; n];
    let mut frontier: Vec<usize> = active.to_vec();
    frontier.iter().for_each(|&a| hit[a] = true);
    for _ in 0..hops {
        let mut next = Vec::new();
        for &(u, v) in live {
            if frontier.contains(&u) && !hit[v] {
                hit[v] = true;
                next.push(v);
            }
        }
        frontier = next;
    }
    hit
}

fn seed(node: usize, probability: f64) -> Seed {
    Seed { node, probability }
}

fn close(a: &[f64], b: &[f64], tol: f64) -> bool {
    a.iter().zip(b).all(|(x, y)| (x - y).abs() <= tol)
}

// 0 -> 1 -> 2, 1 -> 3, 4 -> 3 (a polytree), seeds 0 and 4.
const POLYTREE: [(usize, usize, f64); 4] = [(0, 1, 0.7), (1, 2, 0.5), (1, 3, 0.4), (4, 3, 0.6)];
// 0 -> 1 -> 3 and 0 -> 2 -> 3 (shared ancestry), seed 0.
const DIAMOND: [(usize, usize, f64); 4] = [(0, 1, 0.6), (0, 2, 0.5), (1, 3, 0.8), (2, 3, 0.7)];
// 0 -> 1 -> 2 -> 0, 2 -> 3.
const CYCLE: [(usize, usize, f64); 4] = [(0, 1, 0.9), (1, 2, 0.8), (2, 0, 0.5), (2, 3, 0.6)];

#[test]
fn noisy_or_is_exact_on_a_polytree() {
    let seeds = [seed(0, 0.9), seed(4, 0.5)];
    let graph = ImpactGraph::new(5, &POLYTREE);
    let out = noisy_or(&graph, &seeds, 8);
    assert_eq!(out.semantics, Semantics::Exact);
    assert!(close(
        &out.probability,
        &brute_force(5, &POLYTREE, &seeds, 8),
        1e-12
    ));
}

#[test]
fn noisy_or_bounds_a_diamond_from_above() {
    // An uncertain seed correlates the two branches.
    let seeds = [seed(0, 0.8)];
    let graph = ImpactGraph::new(4, &DIAMOND);
    let out = noisy_or(&graph, &seeds, 8);
    let exact = brute_force(4, &DIAMOND, &seeds, 8);
    assert_eq!(out.semantics, Semantics::UpperBound);
    assert!(close(&out.probability[..3], &exact[..3], 1e-12));
    assert!(
        out.probability[3] > exact[3] + 1e-6,
        "shared ancestry is counted twice"
    );
}

#[test]
fn a_cycle_is_unrolled_to_the_hop_bound() {
    let seeds = [seed(0, 1.0)];
    let graph = ImpactGraph::new(4, &CYCLE);
    let out = noisy_or(&graph, &seeds, 3);
    assert_eq!(out.semantics, Semantics::CyclicUnroll);
    assert!(out.rounds <= 3);
    let exact = brute_force(4, &CYCLE, &seeds, 3);
    assert!(out
        .probability
        .iter()
        .zip(&exact)
        .all(|(p, e)| p + 1e-12 >= *e));
}

#[test]
fn the_hop_bound_cuts_the_cone() {
    let chain = [(0, 1, 1.0), (1, 2, 1.0), (2, 3, 1.0)];
    let graph = ImpactGraph::new(4, &chain);
    let out = noisy_or(&graph, &[seed(0, 1.0)], 2);
    assert_eq!(out.probability, vec![1.0, 1.0, 1.0, 0.0]);
    assert_eq!(out.depth, vec![Some(0), Some(1), Some(2), None]);
    assert!(close(
        &out.probability,
        &brute_force(4, &chain, &[seed(0, 1.0)], 2),
        0.0
    ));
}

#[test]
fn impact_is_monotone_in_transmission_and_seeds() {
    let seeds = [seed(0, 0.9)];
    let base = noisy_or(&ImpactGraph::new(5, &POLYTREE), &seeds, 8).probability;
    let mut stronger = POLYTREE;
    stronger[1].2 = 0.9;
    let raised = noisy_or(&ImpactGraph::new(5, &stronger), &seeds, 8).probability;
    assert!(raised.iter().zip(&base).all(|(r, b)| r + 1e-15 >= *b));
    let more = noisy_or(
        &ImpactGraph::new(5, &POLYTREE),
        &[seed(0, 0.9), seed(4, 0.3)],
        8,
    )
    .probability;
    assert!(more.iter().zip(&base).all(|(m, b)| m + 1e-15 >= *b));
}

#[test]
fn independent_paths_raise_probability_unlike_mass_share() {
    // Ten independent seeds each reach node 10 with 0.3: probability ~0.97,
    // whereas a mass share would split one unit of risk across 11 nodes.
    let edges: Vec<(usize, usize, f64)> = (0..10).map(|u| (u, 10, 0.3)).collect();
    let seeds: Vec<Seed> = (0..10).map(|u| seed(u, 1.0)).collect();
    let out = noisy_or(&ImpactGraph::new(11, &edges), &seeds, 2);
    assert!((out.probability[10] - (1.0 - 0.7f64.powi(10))).abs() < 1e-12);
}

#[test]
fn cascade_converges_to_the_exact_live_edge_probability() {
    for (n, edges, seeds) in [
        (4, DIAMOND.to_vec(), vec![seed(0, 1.0)]),
        (4, CYCLE.to_vec(), vec![seed(0, 0.8)]),
        (5, POLYTREE.to_vec(), vec![seed(0, 0.9), seed(4, 0.5)]),
    ] {
        let graph = ImpactGraph::new(n, &edges);
        let spec = CascadeSpec {
            hops: 3,
            samples: 20_000,
            seed: 9,
        };
        let out = independent_cascade(&graph, &seeds, &[], &spec);
        let exact = brute_force(n, &edges, &seeds, 3);
        for (v, &want) in exact.iter().enumerate().take(n) {
            let se = (want * (1.0 - want) / 20_000.0).sqrt();
            assert!(
                (out.probability[v] - want).abs() <= 4.0 * se + 1e-9,
                "node {v}"
            );
            assert!(out.lower[v] <= out.probability[v] && out.probability[v] <= out.upper[v]);
        }
        let spread: f64 = exact.iter().sum();
        assert!(out.spread_lower - 0.05 <= spread && spread <= out.spread_upper + 0.05);
        assert_eq!(out, independent_cascade(&graph, &seeds, &[], &spec));
    }
}

#[test]
fn strongest_paths_honour_the_hop_bound() {
    // 0 -> 3 directly at 0.2, or 0 -> 1 -> 2 -> 3 at 0.9^3 = 0.729.
    let edges = [(0, 3, 0.2), (0, 1, 0.9), (1, 2, 0.9), (2, 3, 0.9)];
    let graph = ImpactGraph::new(4, &edges);
    let long = strongest_paths(&graph, &[seed(0, 1.0)], 3);
    let best = long[3].clone().unwrap();
    assert_eq!(best.nodes, vec![0, 1, 2, 3]);
    assert!((best.probability - 0.729).abs() < 1e-12);
    let short = strongest_paths(&graph, &[seed(0, 1.0)], 2);
    assert_eq!(short[3].clone().unwrap().nodes, vec![0, 3]);
    assert_eq!(short[0].clone().unwrap().nodes, vec![0]);
}

#[test]
fn shapley_axioms_hold_for_both_models() {
    // Seeds 0 and 4 reach node 3 symmetrically; seed 5 is isolated (null).
    let edges = [(0, 3, 0.5), (4, 3, 0.5), (3, 6, 1.0)];
    let seeds = [seed(0, 1.0), seed(4, 1.0), seed(5, 1.0)];
    let graph = ImpactGraph::new(7, &edges);
    let targets = [3, 6];
    let exact = noisy_or_attribution(&graph, &seeds, &targets, 4).unwrap();
    let probability = noisy_or(&graph, &seeds, 4).probability;
    for (row, &t) in exact.iter().zip(&targets) {
        assert!(
            (row.iter().sum::<f64>() - probability[t]).abs() < 1e-12,
            "efficiency"
        );
        assert!((row[0] - row[1]).abs() < 1e-12, "symmetry");
        assert_eq!(row[2], 0.0, "null player");
    }
    let spec = CascadeSpec {
        hops: 4,
        samples: 5_000,
        seed: 1,
    };
    let sampled = independent_cascade(&graph, &seeds, &targets, &spec);
    for (row, &t) in sampled.attribution.iter().zip(&targets) {
        assert!((row.iter().sum::<f64>() - sampled.probability[t]).abs() < 1e-12);
        assert_eq!(row[2], 0.0);
    }
    let too_many: Vec<Seed> = (0..=MAX_EXACT_SEEDS).map(|u| seed(u, 1.0)).collect();
    assert!(noisy_or_attribution(&graph, &too_many, &targets, 2).is_none());
}
