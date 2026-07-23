// CONCEPT:EG-KG.compute.leiden-community-detection — Community detection via the Leiden algorithm
// (Traag, Waltman & van Eck 2019), Neo4j GDS `gds.leiden` parity.
//
// Leiden refines Louvain's own two-phase (local-move, aggregate) loop with a
// THIRD phase — refinement — run between local-moving and aggregation at every
// level. The paper "From Louvain to Leiden: guaranteeing well-connected
// communities" (Traag, V.A., Waltman, L. & van Eck, N.J., Sci Rep 9, 5233, 2019)
// proves that Louvain's own local-moving phase can, because of the ORDER nodes
// happen to be processed in within one sweep, leave a returned community
// internally disconnected or "badly connected" — a node can move away from a
// community mid-sweep, severing the only link between two remaining subsets,
// without Louvain's algorithm ever re-checking or repairing that.
//
// This module's refinement phase makes "every returned community induces a
// CONNECTED subgraph" a **structural guarantee**, not a typical outcome:
//
//   1. `local_moving` (reused verbatim from [`super::louvain`] — the SAME
//      unconstrained, tested routine) finds a coarse partition `P` of the
//      current (possibly already-aggregated) graph.
//   2. `refine` re-derives a partition WITHIN each `P` community (candidates
//      restricted to same-`P`-community neighbours, singleton-seeded, same
//      modularity-gain criterion) and then — the key correctness step —
//      explicitly recomputes the CONNECTED COMPONENTS of each resulting group's
//      *induced subgraph* and uses those components as the final refined
//      groups. Any group that local-moving's own churn left disconnected is
//      therefore always split back into genuinely connected pieces: the
//      guarantee holds by construction, independent of how the constrained
//      local-moving got there.
//   3. The current graph is aggregated by the REFINED partition (finer than
//      `P`), so the next level's unconstrained local-moving is free to re-merge
//      (or not) the pieces refinement split apart, based on real connectivity.
//
// Determinism: no RNG in the refinement phase itself (ascending index order,
// deterministic tie-break, matching this module's overall no-RNG contract);
// `LeidenConfig::seed` only affects the reused outer `local_moving` step's
// optional visit shuffle, exactly like [`super::louvain::LouvainConfig::seed`].

use super::graph::AdjacencyGraph;
use super::louvain::{aggregate, local_moving, modularity_of};
use std::collections::HashMap;
use std::hash::Hash;

/// Configuration for [`leiden`]. Mirrors [`super::louvain::LouvainConfig`]'s
/// shape so the two are drop-in comparable. CONCEPT:EG-KG.compute.leiden-community-detection
#[derive(Debug, Clone, Copy)]
pub struct LeidenConfig {
    /// Resolution γ scaling the modularity null model (higher ⇒ more, smaller
    /// communities). GDS default 1.0.
    pub resolution: f64,
    /// Optional RNG seed for the OUTER (per-level) local-moving pass's visit
    /// order — see [`super::louvain::LouvainConfig::seed`]. The refinement phase
    /// itself is always order-deterministic (ascending index).
    pub seed: Option<u64>,
    /// Cap on local-moving sweeps per level (guards against slow convergence).
    pub max_sweeps: usize,
    /// Cap on aggregation levels.
    pub max_levels: usize,
}

impl Default for LeidenConfig {
    fn default() -> Self {
        Self {
            resolution: 1.0,
            seed: None,
            max_sweeps: 100,
            max_levels: 50,
        }
    }
}

/// Result of a Leiden run. CONCEPT:EG-KG.compute.leiden-community-detection
#[derive(Debug, Clone)]
pub struct LeidenResult<N> {
    /// Communities: members sorted, communities ordered by smallest member.
    /// Every community's induced subgraph is connected — see the module doc.
    pub communities: Vec<Vec<N>>,
    /// Final modularity `Q` of the returned partition (same formula as
    /// [`super::louvain::louvain`], so the two are directly comparable).
    pub modularity: f64,
}

/// Leiden community detection over the undirected symmetrisation of the graph.
///
/// Same multi-level shape as [`super::louvain::louvain`] (local-moving,
/// aggregate, repeat), with a connectivity-guaranteeing refinement phase
/// between them — see the module doc for the exact mechanism and citation.
///
/// Complexity: `O(L · (V + E))`, the same asymptotic class as Louvain (the
/// refinement phase re-scans each level's edges a bounded constant number of
/// extra times). CONCEPT:EG-KG.compute.leiden-community-detection
pub fn leiden<N>(graph: &AdjacencyGraph<N>, config: &LeidenConfig) -> LeidenResult<N>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    if n == 0 {
        return LeidenResult {
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
    let membership = leiden_partition(&base_adj, resolution, config.seed, config);
    let modularity = modularity_of(&base_adj, &membership, resolution);

    LeidenResult {
        communities: graph.label_partition(&membership),
        modularity,
    }
}

/// Core Leiden over a raw symmetric weighted adjacency, mirroring
/// `louvain::louvain_partition`'s shape with a refinement step inserted between
/// local-moving and aggregation.
fn leiden_partition(
    base_adj: &[Vec<(usize, f64)>],
    resolution: f64,
    seed: Option<u64>,
    config: &LeidenConfig,
) -> Vec<usize> {
    let n = base_adj.len();
    let m2: f64 = base_adj
        .iter()
        .flat_map(|row| row.iter().map(|(_, w)| *w))
        .sum();
    if m2 <= 0.0 {
        return (0..n).collect(); // no edges ⇒ every node isolated
    }

    let mut node_to_super: Vec<usize> = (0..n).collect();
    let mut current: Vec<Vec<(usize, f64)>> = base_adj.to_vec();

    for _level in 0..config.max_levels {
        let (p, improved, _n_p) = local_moving(&current, resolution, m2, seed, config.max_sweeps);
        if !improved {
            break;
        }
        let refined = refine(&current, &p, resolution, m2);
        let n_refined = refined.iter().copied().max().map(|x| x + 1).unwrap_or(0);

        for slot in node_to_super.iter_mut() {
            *slot = refined[*slot];
        }
        if n_refined == current.len() {
            break; // refinement found no merges at all ⇒ stable
        }
        current = aggregate(&current, &refined, n_refined);
        if n_refined == 1 {
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

/// The refinement phase (CONCEPT:EG-KG.compute.leiden-community-detection). Starting from
/// singletons within each `p`-community, runs the SAME modularity-gain
/// local-moving restricted to same-`p`-community neighbours, then — the
/// correctness-critical step — recomputes the connected components of every
/// resulting group's induced subgraph and returns THOSE as the final refined
/// communities. See the module doc for why this makes connectivity a
/// structural guarantee rather than a typical outcome.
fn refine(adj: &[Vec<(usize, f64)>], p: &[usize], resolution: f64, m2: f64) -> Vec<usize> {
    let n = adj.len();
    let degree: Vec<f64> = adj
        .iter()
        .map(|row| row.iter().map(|(_, w)| *w).sum())
        .collect();

    // Step 1: constrained local-moving, singleton-seeded, restricted to
    // same-`p`-community candidates — otherwise identical to
    // `louvain::local_moving`'s single-node greedy reassignment.
    let mut comm: Vec<usize> = (0..n).collect();
    let mut sigma_tot: Vec<f64> = degree.clone();
    let order: Vec<usize> = (0..n).collect();

    for _ in 0..config_sweep_cap(n) {
        let mut moved = false;
        for &i in &order {
            let ci = comm[i];
            let ki = degree[i];

            let mut to_comm: HashMap<usize, f64> = HashMap::new();
            for &(j, w) in &adj[i] {
                if j != i && p[j] == p[i] {
                    *to_comm.entry(comm[j]).or_insert(0.0) += w;
                }
            }

            sigma_tot[ci] -= ki;
            let w_ci = *to_comm.get(&ci).unwrap_or(&0.0);
            let mut best_comm = ci;
            let mut best_gain = w_ci - resolution * sigma_tot[ci] * ki / m2;

            let mut keys: Vec<usize> = to_comm.keys().copied().collect();
            keys.sort_unstable();
            for c in keys {
                if c == ci {
                    continue;
                }
                let w_ic = to_comm[&c];
                let gain = w_ic - resolution * sigma_tot[c] * ki / m2;
                if gain > best_gain + 1e-12 || (gain > best_gain - 1e-12 && c < best_comm) {
                    best_gain = gain;
                    best_comm = c;
                }
            }

            sigma_tot[best_comm] += ki;
            comm[i] = best_comm;
            if best_comm != ci {
                moved = true;
            }
        }
        if !moved {
            break;
        }
    }

    // Step 2: split any group left disconnected by step 1 into its connected
    // components (union-find over ONLY edges joining two same-`comm` nodes).
    // This is what turns "typically connected" into "always connected".
    let mut uf_parent: Vec<usize> = (0..n).collect();
    for i in 0..n {
        for &(j, _w) in &adj[i] {
            if j != i && comm[i] == comm[j] {
                uf_union(&mut uf_parent, i, j);
            }
        }
    }
    let roots: Vec<usize> = (0..n).map(|i| uf_find(&mut uf_parent, i)).collect();

    // Densify (comm, connected-component-root) pairs into 0..k ids, in a fixed
    // ascending-`i` insertion order — deterministic regardless of hash
    // iteration, matching `louvain::local_moving`'s own densify idiom.
    let mut relabel: HashMap<(usize, usize), usize> = HashMap::new();
    let mut out = vec![0usize; n];
    for i in 0..n {
        let key = (comm[i], roots[i]);
        let next = relabel.len();
        out[i] = *relabel.entry(key).or_insert(next);
    }
    out
}

/// A bounded sweep cap for the refinement phase's own local-moving — proportional
/// to graph size like Louvain's `max_sweeps`, but derived locally so `refine`
/// does not need the full `LeidenConfig` threaded through it.
fn config_sweep_cap(n: usize) -> usize {
    n.clamp(1, 100)
}

fn uf_find(parent: &mut [usize], mut x: usize) -> usize {
    while parent[x] != x {
        parent[x] = parent[parent[x]]; // path halving
        x = parent[x];
    }
    x
}

fn uf_union(parent: &mut [usize], a: usize, b: usize) {
    let (ra, rb) = (uf_find(parent, a), uf_find(parent, b));
    if ra != rb {
        // Attach the larger root under the smaller for a deterministic result
        // independent of call order (both a<b and a>b callers converge).
        if ra < rb {
            parent[rb] = ra;
        } else {
            parent[ra] = rb;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::components::weakly_connected_components;
    use super::super::louvain::{louvain, LouvainConfig};
    use super::*;

    /// Every returned community's INDUCED subgraph must be connected — the
    /// guarantee this module exists to provide. Verified by rebuilding each
    /// community as its own small graph (edges filtered to both-endpoints-in)
    /// and cross-checking with the existing, independently-tested
    /// `weakly_connected_components` kernel.
    fn assert_all_communities_connected(edges: &[(&str, &str, f64)], communities: &[Vec<&str>]) {
        for community in communities {
            if community.len() <= 1 {
                continue;
            }
            let members: std::collections::BTreeSet<&str> = community.iter().copied().collect();
            let induced: Vec<(&str, &str, f64)> = edges
                .iter()
                .filter(|(s, t, _)| members.contains(s) && members.contains(t))
                .copied()
                .collect();
            let g = AdjacencyGraph::from_edges(induced);
            // Any member with no induced edge (e.g. only connected via a node
            // OUTSIDE this community — should not happen for a well-formed
            // community, but guard the fixture-construction assumption) must
            // still appear as a graph node so WCC sees it as its own island.
            let mut adjacency: Vec<(&str, Vec<(&str, f64)>)> =
                g.nodes().iter().map(|n| (*n, Vec::new())).collect();
            for m in &members {
                if g.index_of(m).is_none() {
                    adjacency.push((m, Vec::new()));
                }
            }
            let g = if adjacency.len() > g.node_count() {
                AdjacencyGraph::from_adjacency(adjacency)
            } else {
                g
            };
            let comps = weakly_connected_components(&g);
            assert_eq!(
                comps.len(),
                1,
                "community {community:?} induces a DISCONNECTED subgraph: {comps:?}"
            );
        }
    }

    #[test]
    fn leiden_finds_two_communities_in_two_cliques_matching_louvain() {
        // Same fixture as louvain's own test: two 4-cliques joined by one bridge.
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
        edges.push(("d", "w", 1.0));

        let g = AdjacencyGraph::from_edges(edges.clone());
        let leiden_res = leiden(&g, &LeidenConfig::default());
        let louvain_res = louvain(&g, &LouvainConfig::default());

        assert_eq!(
            leiden_res.communities.len(),
            2,
            "{:?}",
            leiden_res.communities
        );
        assert!(leiden_res
            .communities
            .iter()
            .any(|c| c == &vec!["a", "b", "c", "d"]));
        assert!(leiden_res
            .communities
            .iter()
            .any(|c| c == &vec!["w", "x", "y", "z"]));

        // The headline cross-check: Leiden's modularity is at least Louvain's on
        // this fixture (both find the same clean partition here).
        assert!(
            leiden_res.modularity >= louvain_res.modularity - 1e-9,
            "leiden Q={} should be >= louvain Q={}",
            leiden_res.modularity,
            louvain_res.modularity
        );

        let comm_refs: Vec<Vec<&str>> = leiden_res.communities.clone();
        assert_all_communities_connected(&edges, &comm_refs);
    }

    #[test]
    fn leiden_communities_stay_connected_on_a_ring_of_cliques() {
        // A trickier structure: three triangles connected in a ring by single
        // bridge edges (a-shape known to stress local-moving order-dependence).
        let mut edges: Vec<(&str, &str, f64)> = Vec::new();
        let triangles = [["a1", "a2", "a3"], ["b1", "b2", "b3"], ["c1", "c2", "c3"]];
        for t in &triangles {
            edges.push((t[0], t[1], 1.0));
            edges.push((t[1], t[2], 1.0));
            edges.push((t[0], t[2], 1.0));
        }
        edges.push(("a3", "b1", 0.5));
        edges.push(("b3", "c1", 0.5));
        edges.push(("c3", "a1", 0.5));

        let g = AdjacencyGraph::from_edges(edges.clone());
        let res = leiden(&g, &LeidenConfig::default());
        assert!(!res.communities.is_empty());
        let total: usize = res.communities.iter().map(Vec::len).sum();
        assert_eq!(total, 9, "every node must appear exactly once");
        assert_all_communities_connected(&edges, &res.communities);
    }

    #[test]
    fn leiden_single_clique_is_one_connected_community() {
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("b", "c", 1.0), ("a", "c", 1.0)]);
        let res = leiden(&g, &LeidenConfig::default());
        assert_eq!(res.communities.len(), 1);
        assert_eq!(res.communities[0], vec!["a", "b", "c"]);
    }

    #[test]
    fn leiden_is_deterministic_across_runs() {
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
        let a = leiden(&g, &LeidenConfig::default());
        let b = leiden(&g, &LeidenConfig::default());
        assert_eq!(a.communities, b.communities);
        assert!((a.modularity - b.modularity).abs() < 1e-12);

        let cfg = LeidenConfig {
            seed: Some(42),
            ..Default::default()
        };
        let c1 = leiden(&g, &cfg);
        let c2 = leiden(&g, &cfg);
        assert_eq!(c1.communities, c2.communities);
    }

    #[test]
    fn leiden_disconnected_nodes_separate() {
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("x", "y", 1.0)]);
        let res = leiden(&g, &LeidenConfig::default());
        assert_eq!(res.communities.len(), 2);
    }

    #[test]
    fn leiden_empty_graph_yields_empty_partition() {
        let g: AdjacencyGraph<&str> =
            AdjacencyGraph::from_adjacency(Vec::<(&str, Vec<(&str, f64)>)>::new());
        let res = leiden(&g, &LeidenConfig::default());
        assert!(res.communities.is_empty());
        assert_eq!(res.modularity, 0.0);
    }
}
