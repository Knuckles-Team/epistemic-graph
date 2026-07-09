// CONCEPT:EG-KG.compute.label-propagation — Community detection via synchronous
// Label Propagation (LPA). Neo4j GDS `gds.labelPropagation` parity.
//
// Every node starts in its own label (its compact index). Each SYNCHRONOUS sweep,
// every node adopts the label carrying the greatest total incident edge weight
// among its undirected neighbours' PREVIOUS-sweep labels (self excluded), ties
// broken toward the smallest label id. Runs until no label changes or
// `max_iterations` sweeps, whichever comes first.
//
// Deterministic (CONCEPT:EG-KG.compute.graph-data-science-algorithms's determinism
// contract): no RNG, synchronous (not asynchronous/random-order) updates, and a
// fixed ascending-label tie-break — a given graph + config always yields the same
// partition.

use super::graph::AdjacencyGraph;
use std::collections::HashMap;
use std::hash::Hash;

/// Config for [`label_propagation`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LabelPropagationConfig {
    /// Maximum synchronous sweeps (stops early if labels stop changing).
    pub max_iterations: usize,
    /// Weight neighbour votes by edge weight (`true`, GDS default) vs. a flat
    /// unweighted vote of `1.0` per neighbour edge (`false`).
    pub weighted: bool,
}

impl Default for LabelPropagationConfig {
    fn default() -> Self {
        Self {
            max_iterations: 10,
            weighted: true,
        }
    }
}

/// The community partition [`label_propagation`] converges to.
#[derive(Debug, Clone, PartialEq)]
pub struct LabelPropagationResult<N> {
    /// Communities, each a sorted member list; communities themselves ordered by
    /// their smallest member (via `AdjacencyGraph::label_partition`'s convention).
    pub communities: Vec<Vec<N>>,
}

/// Run label propagation over `graph` (CONCEPT:EG-KG.compute.label-propagation).
///
/// Complexity: `O(max_iterations · (V + E))`. An empty graph yields an empty
/// partition (never a panic).
pub fn label_propagation<N>(
    graph: &AdjacencyGraph<N>,
    config: &LabelPropagationConfig,
) -> LabelPropagationResult<N>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    if n == 0 {
        return LabelPropagationResult {
            communities: Vec::new(),
        };
    }
    let mut labels: Vec<usize> = (0..n).collect();
    for _ in 0..config.max_iterations.max(1) {
        let mut changed = false;
        let mut next = labels.clone();
        for u in 0..n {
            let neighbors = graph.undirected_neighbors(u);
            if neighbors.is_empty() {
                continue;
            }
            let mut votes: HashMap<usize, f64> = HashMap::new();
            for v in neighbors {
                let w = if config.weighted {
                    edge_weight_between(graph, u, v)
                } else {
                    1.0
                };
                *votes.entry(labels[v]).or_insert(0.0) += w;
            }
            // Ascending-label iteration order breaks ties toward the smallest id
            // deterministically, independent of HashMap iteration order.
            let mut ordered: Vec<usize> = votes.keys().copied().collect();
            ordered.sort_unstable();
            let mut best_label = labels[u];
            let mut best_weight = f64::MIN;
            for lbl in ordered {
                let w = votes[&lbl];
                if w > best_weight {
                    best_weight = w;
                    best_label = lbl;
                }
            }
            if next[u] != best_label {
                changed = true;
            }
            next[u] = best_label;
        }
        labels = next;
        if !changed {
            break;
        }
    }
    LabelPropagationResult {
        communities: graph.label_partition(&labels),
    }
}

/// Total (out + in) edge weight between `u` and `v` in `graph`.
fn edge_weight_between<N>(graph: &AdjacencyGraph<N>, u: usize, v: usize) -> f64
where
    N: Clone + Eq + Hash + Ord,
{
    let mut w = 0.0;
    for &(t, ww) in graph.out_edges(u) {
        if t == v {
            w += ww;
        }
    }
    for &(s, ww) in graph.in_edges(u) {
        if s == v {
            w += ww;
        }
    }
    w
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_dense_triangles_bridged_by_one_edge_split_into_two_communities() {
        // Two dense triangles {a,b,c} and {x,y,z}, bridged by one weak edge c-x.
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("b", "c", 1.0),
            ("a", "c", 1.0),
            ("x", "y", 1.0),
            ("y", "z", 1.0),
            ("x", "z", 1.0),
            ("c", "x", 1.0),
        ]);
        let res = label_propagation(&g, &LabelPropagationConfig::default());
        // Two communities total.
        assert_eq!(res.communities.len(), 2);
        let community_of = |id: &str| {
            res.communities
                .iter()
                .position(|c| c.iter().any(|m| *m == id))
                .unwrap()
        };
        assert_eq!(community_of("a"), community_of("b"));
        assert_eq!(community_of("b"), community_of("c"));
        assert_eq!(community_of("x"), community_of("y"));
        assert_eq!(community_of("y"), community_of("z"));
        assert_ne!(community_of("a"), community_of("x"));
    }

    #[test]
    fn isolated_nodes_stay_singleton_communities() {
        let g = AdjacencyGraph::from_adjacency([
            ("a".to_string(), vec![]),
            ("b".to_string(), vec![]),
        ]);
        let res = label_propagation(&g, &LabelPropagationConfig::default());
        assert_eq!(res.communities.len(), 2);
    }

    #[test]
    fn empty_graph_yields_empty_partition() {
        let g: AdjacencyGraph<String> = AdjacencyGraph::from_adjacency(Vec::<(String, Vec<(String, f64)>)>::new());
        let res = label_propagation(&g, &LabelPropagationConfig::default());
        assert!(res.communities.is_empty());
    }

    #[test]
    fn deterministic_across_repeated_runs() {
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("b", "c", 1.0),
            ("c", "d", 1.0),
            ("d", "a", 1.0),
            ("e", "f", 2.0),
        ]);
        let r1 = label_propagation(&g, &LabelPropagationConfig::default());
        let r2 = label_propagation(&g, &LabelPropagationConfig::default());
        assert_eq!(r1, r2);
    }
}
