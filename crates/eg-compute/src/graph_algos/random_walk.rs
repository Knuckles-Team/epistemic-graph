// CONCEPT:EG-KG.compute.random-walk — Weighted random walk with restart, seeded and
// deterministic given the seed. Neo4j GDS `gds.randomWalk` parity.
//
// The one function in this crate whose whole POINT is randomness, so it is
// explicitly exempted from the rest of this module's "no RNG" contract (see
// `graph_algos`'s top-level doc) — but it is still fully deterministic for a
// fixed seed, via the SAME dependency-free splitmix64 stream
// [`super::louvain`]'s optional seeded shuffle uses (reimplemented locally so
// this module stays standalone, matching every other file in this crate).

use super::graph::AdjacencyGraph;
use std::hash::Hash;

/// A minimal, dependency-free splitmix64 PRNG stream.
struct SplitMix64 {
    state: u64,
}
impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// A uniform `f64` in `[0, 1)`, via the top 53 bits (the standard
    /// integer-to-double technique — full `f64` mantissa precision).
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

/// Configuration for [`random_walk`]. CONCEPT:EG-KG.compute.random-walk
#[derive(Debug, Clone, Copy)]
pub struct RandomWalkConfig {
    /// Number of steps to take (the returned walk has up to `steps + 1`
    /// nodes, including the start — fewer if a dead end is reached with no
    /// restart to recover it, see below).
    pub steps: usize,
    /// Probability, checked independently at EVERY step, of jumping back to
    /// `start` instead of moving to a neighbour. `0.0` ⇒ a plain random walk
    /// (never restarts, no RNG draw is even spent on the check). `1.0` ⇒
    /// every step returns to `start`. Clamped to `[0, 1]`.
    pub restart_probability: f64,
    /// PRNG seed — the walk is bit-reproducible for a fixed seed and graph.
    pub seed: u64,
}

impl Default for RandomWalkConfig {
    fn default() -> Self {
        Self {
            steps: 10,
            restart_probability: 0.0,
            seed: 0,
        }
    }
}

/// A weighted random walk starting at `start`: at each step, with probability
/// [`RandomWalkConfig::restart_probability`] jump back to `start`; otherwise
/// move to an out-neighbour chosen with probability proportional to edge
/// weight (uniform when unweighted). If the current node has no out-edges
/// (and this step didn't restart), the walk stops EARLY rather than looping
/// forever or panicking — a documented, honest degrade, not an error.
///
/// Returns the visited node labels in walk order (length `1..=steps+1`,
/// starting with `start`). Deterministic for a fixed `(graph, start, config)`.
///
/// Complexity: `O(steps · d̄)` for average degree `d̄` (each step scans one
/// node's out-edges once). CONCEPT:EG-KG.compute.random-walk
pub fn random_walk<N>(graph: &AdjacencyGraph<N>, start: usize, config: &RandomWalkConfig) -> Vec<N>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    if n == 0 || start >= n {
        return Vec::new();
    }
    let restart_p = config.restart_probability.clamp(0.0, 1.0);
    let mut rng = SplitMix64::new(config.seed);
    let mut walk: Vec<usize> = vec![start];
    let mut current = start;

    for _ in 0..config.steps {
        if restart_p > 0.0 && rng.next_f64() < restart_p {
            current = start;
            walk.push(current);
            continue;
        }
        let out = graph.out_edges(current);
        match choose_weighted_out_edge(out, &mut rng) {
            Some(chosen) => {
                current = chosen;
                walk.push(current);
            }
            None => break, // dead end, or degenerate all-zero weights ⇒ stop early
        }
    }

    walk.into_iter().map(|i| graph.node_at(i).clone()).collect()
}

/// Pick one of `out`'s targets with probability proportional to edge weight,
/// drawing exactly one `rng` value. `None` means there was no valid choice
/// (no out-edges, or all weights `<= 0.0`) -- the caller stops the walk.
/// Pure extraction from [`random_walk`]'s per-step body -- identical
/// sum/threshold/accumulate logic and RNG draw, no behaviour change; pulled
/// out solely to keep [`random_walk`]'s own cyclomatic complexity within the
/// repo's gate cap. CONCEPT:EG-KG.compute.random-walk
fn choose_weighted_out_edge(out: &[(usize, f64)], rng: &mut SplitMix64) -> Option<usize> {
    if out.is_empty() {
        return None;
    }
    let total_w: f64 = out.iter().map(|(_, w)| *w).sum();
    if total_w <= 0.0 {
        return None;
    }
    let threshold = rng.next_f64() * total_w;
    let mut acc = 0.0;
    let mut chosen = out[out.len() - 1].0; // float-rounding fallback: last edge
    for &(v, w) in out {
        acc += w;
        if threshold < acc {
            chosen = v;
            break;
        }
    }
    Some(chosen)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg144_random_walk_restart_probability_one_always_returns_to_start() {
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("b", "c", 1.0), ("c", "a", 1.0)]);
        let a = g.index_of(&"a").unwrap();
        let cfg = RandomWalkConfig {
            steps: 5,
            restart_probability: 1.0,
            seed: 7,
        };
        let walk = random_walk(&g, a, &cfg);
        assert_eq!(walk, vec!["a", "a", "a", "a", "a", "a"]);
    }

    #[test]
    fn eg144_random_walk_no_choice_path_is_seed_independent_and_stops_at_dead_end() {
        // a->b->c->d, each with exactly ONE out-edge (d has none): there is no
        // actual RANDOM choice anywhere, so the walk must be exactly this
        // chain regardless of seed, stopping early at d even though more
        // steps were requested.
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("b", "c", 1.0), ("c", "d", 1.0)]);
        let a = g.index_of(&"a").unwrap();
        for seed in [0u64, 1, 42, u64::MAX] {
            let cfg = RandomWalkConfig {
                steps: 10,
                restart_probability: 0.0,
                seed,
            };
            let walk = random_walk(&g, a, &cfg);
            assert_eq!(walk, vec!["a", "b", "c", "d"], "seed={seed}");
        }
    }

    #[test]
    fn eg144_random_walk_deterministic_for_a_fixed_seed_on_a_branching_graph() {
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("a", "c", 1.0),
            ("b", "d", 1.0),
            ("c", "d", 1.0),
            ("d", "a", 1.0),
        ]);
        let a = g.index_of(&"a").unwrap();
        let cfg = RandomWalkConfig {
            steps: 20,
            restart_probability: 0.2,
            seed: 12345,
        };
        let w1 = random_walk(&g, a, &cfg);
        let w2 = random_walk(&g, a, &cfg);
        assert_eq!(w1, w2);
    }

    #[test]
    fn eg144_random_walk_every_step_is_a_real_edge_or_a_restart() {
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("a", "c", 2.0),
            ("b", "d", 1.0),
            ("c", "d", 1.0),
            ("d", "b", 1.0),
        ]);
        let a = g.index_of(&"a").unwrap();
        let cfg = RandomWalkConfig {
            steps: 30,
            restart_probability: 0.3,
            seed: 99,
        };
        let walk = random_walk(&g, a, &cfg);
        assert_eq!(walk[0], "a");
        for w in walk.windows(2) {
            if w[1] == "a" {
                continue; // could be a restart jump
            }
            let u = g.index_of(&w[0]).unwrap();
            let v = g.index_of(&w[1]).unwrap();
            assert!(
                g.out_edges(u).iter().any(|&(t, _)| t == v),
                "{:?} -> {:?} is not a real edge",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn eg144_random_walk_dead_end_with_no_restart_stops_early() {
        let g = AdjacencyGraph::from_adjacency([("a", vec![("b", 1.0)]), ("b", vec![])]);
        let a = g.index_of(&"a").unwrap();
        let cfg = RandomWalkConfig {
            steps: 50,
            restart_probability: 0.0,
            seed: 3,
        };
        let walk = random_walk(&g, a, &cfg);
        assert_eq!(walk, vec!["a", "b"]);
    }

    #[test]
    fn eg144_random_walk_empty_graph_or_out_of_range_start_is_empty() {
        let g: AdjacencyGraph<&str> =
            AdjacencyGraph::from_adjacency(Vec::<(&str, Vec<(&str, f64)>)>::new());
        assert!(random_walk(&g, 0, &RandomWalkConfig::default()).is_empty());

        let g2 = AdjacencyGraph::from_edges([("a", "b", 1.0)]);
        assert!(random_walk(&g2, 99, &RandomWalkConfig::default()).is_empty());
    }
}
