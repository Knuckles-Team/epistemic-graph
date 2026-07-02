// CONCEPT:EG-144 — Community detection via Louvain modularity optimization
// (Neo4j GDS `gds.louvain` parity).

use super::graph::AdjacencyGraph;
use std::collections::HashMap;
use std::hash::Hash;

/// Configuration for [`louvain`]. CONCEPT:EG-144
#[derive(Debug, Clone, Copy)]
pub struct LouvainConfig {
    /// Resolution γ scaling the modularity null model (higher ⇒ more, smaller
    /// communities). GDS default 1.0.
    pub resolution: f64,
    /// Optional RNG seed. `None` ⇒ nodes are visited in ascending index order
    /// (fully deterministic, no RNG). `Some(seed)` ⇒ a *seeded* deterministic
    /// Fisher–Yates shuffle of the visit order (still reproducible run-to-run).
    pub seed: Option<u64>,
    /// Cap on local-moving sweeps per level (guards against slow convergence).
    pub max_sweeps: usize,
    /// Cap on aggregation levels.
    pub max_levels: usize,
}

impl Default for LouvainConfig {
    fn default() -> Self {
        Self {
            resolution: 1.0,
            seed: None,
            max_sweeps: 100,
            max_levels: 50,
        }
    }
}

/// Result of a Louvain run. CONCEPT:EG-144
#[derive(Debug, Clone)]
pub struct LouvainResult<N> {
    /// Communities: members sorted, communities ordered by smallest member.
    pub communities: Vec<Vec<N>>,
    /// Final modularity `Q` of the returned partition.
    pub modularity: f64,
}

/// Louvain community detection over the undirected symmetrisation of the graph.
///
/// Two-phase, multi-level: (1) greedy local modularity-gain moving until no node
/// improves, (2) aggregate each community into a super-node and recurse, until no
/// further gain. Deterministic for a fixed `seed` (or fully order-deterministic
/// when `seed = None`); ties are resolved in favour of a node's current community
/// (no oscillation).
///
/// Complexity: near-linear per level, `O(L · (V + E))` in practice for `L`
/// levels. CONCEPT:EG-144
pub fn louvain<N>(graph: &AdjacencyGraph<N>, config: &LouvainConfig) -> LouvainResult<N>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    if n == 0 {
        return LouvainResult {
            communities: Vec::new(),
            modularity: 0.0,
        };
    }
    let resolution = if config.resolution > 0.0 {
        config.resolution
    } else {
        1.0
    };

    let base_adj = graph.undirected_weighted_adjacency();
    let membership = louvain_partition(&base_adj, resolution, config.seed, config);
    let modularity = modularity_of(&base_adj, &membership, resolution);

    LouvainResult {
        communities: graph.label_partition(&membership),
        modularity,
    }
}

/// Core Louvain over a raw symmetric weighted adjacency. Returns the community of
/// each of the original `0..n` nodes (dense community ids).
fn louvain_partition(
    base_adj: &[Vec<(usize, f64)>],
    resolution: f64,
    seed: Option<u64>,
    config: &LouvainConfig,
) -> Vec<usize> {
    let n = base_adj.len();
    let m2: f64 = base_adj
        .iter()
        .flat_map(|row| row.iter().map(|(_, w)| *w))
        .sum(); // = 2m
    if m2 <= 0.0 {
        return (0..n).collect(); // no edges ⇒ every node isolated
    }

    // node_to_super[o] tracks which current-level super-node each ORIGINAL node
    // maps to; updated after each level.
    let mut node_to_super: Vec<usize> = (0..n).collect();
    let mut current: Vec<Vec<(usize, f64)>> = base_adj.to_vec();

    for _level in 0..config.max_levels {
        let (comm, improved, n_comms) =
            local_moving(&current, resolution, m2, seed, config.max_sweeps);
        if !improved {
            break;
        }
        // Fold this level's communities into the original mapping.
        for slot in node_to_super.iter_mut() {
            *slot = comm[*slot];
        }
        if n_comms == current.len() {
            break; // no coarsening possible
        }
        current = aggregate(&current, &comm, n_comms);
        if n_comms == 1 {
            break;
        }
    }

    // Densify community ids into 0..k in first-appearance order.
    let mut relabel: HashMap<usize, usize> = HashMap::new();
    let mut membership = vec![0usize; n];
    for (o, &c) in node_to_super.iter().enumerate() {
        let next = relabel.len();
        let dense = *relabel.entry(c).or_insert(next);
        membership[o] = dense;
    }
    membership
}

/// One level of local moving. Returns `(community_of_node, improved, n_comms)`.
fn local_moving(
    adj: &[Vec<(usize, f64)>],
    resolution: f64,
    m2: f64,
    seed: Option<u64>,
    max_sweeps: usize,
) -> (Vec<usize>, bool, usize) {
    let n = adj.len();
    let degree: Vec<f64> = adj
        .iter()
        .map(|row| row.iter().map(|(_, w)| *w).sum())
        .collect();

    let mut comm: Vec<usize> = (0..n).collect();
    let mut sigma_tot: Vec<f64> = degree.clone();

    let order = visit_order(n, seed);
    let mut improved = false;

    for _ in 0..max_sweeps {
        let mut moved = false;
        for &i in &order {
            let ci = comm[i];
            let ki = degree[i];

            // Weight from i to each neighbouring community (self-loop excluded).
            let mut to_comm: HashMap<usize, f64> = HashMap::new();
            for &(j, w) in &adj[i] {
                if j != i {
                    *to_comm.entry(comm[j]).or_insert(0.0) += w;
                }
            }

            // Detach i from its community.
            sigma_tot[ci] -= ki;

            // Baseline: rejoin ci. gain(c) = w_{i→c} − γ·Σtot(c)·k_i / 2m.
            let w_ci = *to_comm.get(&ci).unwrap_or(&0.0);
            let mut best_comm = ci;
            let mut best_gain = w_ci - resolution * sigma_tot[ci] * ki / m2;

            for (&c, &w_ic) in &to_comm {
                if c == ci {
                    continue;
                }
                let gain = w_ic - resolution * sigma_tot[c] * ki / m2;
                // Strictly-greater keeps ties with the current community (stable,
                // non-oscillating); deterministic smallest-id tie-break among
                // equally-better candidates.
                if gain > best_gain + 1e-12 || (gain > best_gain - 1e-12 && c < best_comm) {
                    best_gain = gain;
                    best_comm = c;
                }
            }

            sigma_tot[best_comm] += ki;
            comm[i] = best_comm;
            if best_comm != ci {
                moved = true;
                improved = true;
            }
        }
        if !moved {
            break;
        }
    }

    // Densify.
    let mut relabel: HashMap<usize, usize> = HashMap::new();
    for slot in comm.iter_mut() {
        let next = relabel.len();
        *slot = *relabel.entry(*slot).or_insert(next);
    }
    (comm, improved, relabel.len())
}

/// Aggregate communities into super-nodes; edge weights between communities sum.
fn aggregate(adj: &[Vec<(usize, f64)>], comm: &[usize], n_comms: usize) -> Vec<Vec<(usize, f64)>> {
    let mut maps: Vec<HashMap<usize, f64>> = vec![HashMap::new(); n_comms];
    for (i, row) in adj.iter().enumerate() {
        let ci = comm[i];
        for &(j, w) in row {
            let cj = comm[j];
            *maps[ci].entry(cj).or_insert(0.0) += w;
        }
    }
    maps.into_iter()
        .map(|m| {
            let mut v: Vec<(usize, f64)> = m.into_iter().collect();
            v.sort_unstable_by_key(|(i, _)| *i);
            v
        })
        .collect()
}

/// Modularity `Q = Σ_c [ in_c/2m − γ (Σtot_c/2m)² ]`.
fn modularity_of(adj: &[Vec<(usize, f64)>], membership: &[usize], resolution: f64) -> f64 {
    let m2: f64 = adj
        .iter()
        .flat_map(|row| row.iter().map(|(_, w)| *w))
        .sum();
    if m2 <= 0.0 {
        return 0.0;
    }
    let k = membership.iter().copied().max().map(|x| x + 1).unwrap_or(0);
    let mut internal = vec![0.0f64; k];
    let mut tot = vec![0.0f64; k];
    for (i, row) in adj.iter().enumerate() {
        let ci = membership[i];
        for &(j, w) in row {
            tot[ci] += w; // accumulates degree over all entries
            if membership[j] == ci {
                internal[ci] += w;
            }
        }
    }
    let mut q = 0.0;
    for c in 0..k {
        let frac = tot[c] / m2;
        q += internal[c] / m2 - resolution * frac * frac;
    }
    q
}

/// Deterministic visit order. `None` ⇒ ascending; `Some(seed)` ⇒ seeded
/// Fisher–Yates using a splitmix64 stream (dependency-free, reproducible).
fn visit_order(n: usize, seed: Option<u64>) -> Vec<usize> {
    let mut order: Vec<usize> = (0..n).collect();
    if let Some(seed) = seed {
        let mut state = seed;
        let mut next = || {
            state = state.wrapping_add(0x9E3779B97F4A7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            z ^ (z >> 31)
        };
        for i in (1..n).rev() {
            let j = (next() % (i as u64 + 1)) as usize;
            order.swap(i, j);
        }
    }
    order
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg144_louvain_finds_two_communities_in_two_cliques() {
        // Two 4-cliques {a,b,c,d} and {w,x,y,z} joined by a single bridge d–w.
        let mut edges: Vec<(&str, &str, f64)> = Vec::new();
        let clique1 = ["a", "b", "c", "d"];
        let clique2 = ["w", "x", "y", "z"];
        for c in [&clique1, &clique2] {
            for i in 0..c.len() {
                for j in (i + 1)..c.len() {
                    edges.push((c[i], c[j], 1.0));
                }
            }
        }
        edges.push(("d", "w", 1.0)); // weak bridge

        let g = AdjacencyGraph::from_edges(edges);
        let res = louvain(&g, &LouvainConfig::default());
        assert_eq!(
            res.communities.len(),
            2,
            "two cliques ⇒ two communities, got {:?}",
            res.communities
        );
        // Each clique lands wholly in one community.
        assert!(res.communities.iter().any(|c| c == &vec!["a", "b", "c", "d"]));
        assert!(res.communities.iter().any(|c| c == &vec!["w", "x", "y", "z"]));
        assert!(res.modularity > 0.3, "Q={}", res.modularity);
    }

    #[test]
    fn eg144_louvain_single_clique_is_one_community() {
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("b", "c", 1.0),
            ("a", "c", 1.0),
        ]);
        let res = louvain(&g, &LouvainConfig::default());
        assert_eq!(res.communities.len(), 1);
        assert_eq!(res.communities[0], vec!["a", "b", "c"]);
    }

    #[test]
    fn eg144_louvain_is_deterministic_across_runs() {
        let edges = [
            ("a", "b", 1.0),
            ("b", "c", 1.0),
            ("a", "c", 1.0),
            ("c", "d", 0.1),
            ("d", "e", 1.0),
            ("e", "f", 1.0),
            ("d", "f", 1.0),
        ];
        let g = AdjacencyGraph::from_edges(edges);
        let a = louvain(&g, &LouvainConfig::default());
        let b = louvain(&g, &LouvainConfig::default());
        assert_eq!(a.communities, b.communities);
        // Seeded runs are also reproducible.
        let cfg = LouvainConfig {
            seed: Some(42),
            ..Default::default()
        };
        let c1 = louvain(&g, &cfg);
        let c2 = louvain(&g, &cfg);
        assert_eq!(c1.communities, c2.communities);
    }

    #[test]
    fn eg144_louvain_disconnected_nodes_separate() {
        // Two disjoint edges ⇒ two communities.
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("x", "y", 1.0)]);
        let res = louvain(&g, &LouvainConfig::default());
        assert_eq!(res.communities.len(), 2);
    }
}
