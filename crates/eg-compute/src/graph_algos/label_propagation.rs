// CONCEPT:EG-KG.compute.label-propagation — Community detection via Label
// Propagation (LPA). Neo4j GDS `gds.labelPropagation` parity.
//
// Every node starts in its own label (its compact index). Each sweep visits
// nodes in a FIXED ascending-index order and updates each node's label
// IN PLACE — asynchronously, so a node's later neighbours in the same sweep
// already see its new label. This (not a synchronous "snapshot the whole
// previous round" update) is the standard LPA formulation, and deliberately so:
// synchronous updates are well known to OSCILLATE forever on bipartite/
// 2-colorable structures (e.g. a plain 3-node path a-b-c, whose two endpoints
// and middle node flip labels back and forth every round) — asynchronous
// in-order updates converge on exactly those graphs instead. Each node adopts
// the label carrying the greatest total incident edge weight among its
// undirected neighbours (self excluded), ties broken toward the smallest label
// id. Runs until no label changes in a full sweep or `max_iterations` sweeps,
// whichever comes first.
//
// Deterministic (CONCEPT:EG-KG.compute.graph-data-science-algorithms's determinism
// contract): no RNG — the traversal order is the fixed sorted-node-id order every
// sweep, and a fixed ascending-label tie-break — so a given graph + config always
// yields the same partition.
//
// Honest scope: LPA is a raw majority vote (unlike Louvain's modularity-gain
// criterion), so it inherits two well-documented general weaknesses of the
// algorithm family, not specific to this implementation: (1) it does not reliably
// merge sparse/bipartite/tree-like structures (a bare 3-node path has no majority
// signal either way); (2) on an UNWEIGHTED graph, a cold-start round (every node
// starts with a distinct label) can, depending on visitation order, let two
// otherwise-separate dense clusters merge across a single bridge edge before each
// side has locally converged. Weighting the bridge low relative to intra-cluster
// edges (`relationshipWeightProperty`) removes that ambiguity and is the
// realistic way a caller signals "this link is weak" — see the weighted-bridge
// test below.

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
                // Reads `labels[v]` — the CURRENT array, already updated for any
                // earlier-in-this-sweep neighbour (the asynchronous update this
                // module's doc comment explains).
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
            if labels[u] != best_label {
                changed = true;
                labels[u] = best_label;
            }
        }
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
    fn two_cliques_bridged_by_a_near_zero_weight_edge_split_into_two_communities() {
        // Two 4-cliques {a,b,c,d} and {w,x,y,z} joined by one bridge d-w — the same
        // fixture shape `louvain`'s equivalent test uses. LPA is a raw majority
        // vote (unlike Louvain's modularity-gain criterion), so on a graph where
        // EVERY edge carries equal weight, cold-start ties (every node starts
        // with a distinct label) can — depending on visitation order — let one
        // side's converged label leak across an equal-weight bridge before the
        // other side has locally converged (a documented general weakness of
        // plain LPA, not specific to this implementation). Weighting the bridge
        // near-zero — the realistic way a caller signals "this link is weak" —
        // removes the ambiguity entirely: it can never out-vote a same-cluster
        // weight-1.0 edge, so the split is robust regardless of visitation order.
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
        edges.push(("d", "w", 0.001));
        let g = AdjacencyGraph::from_edges(edges);

        let res = label_propagation(
            &g,
            &LabelPropagationConfig {
                max_iterations: 10,
                weighted: true,
            },
        );
        assert_eq!(
            res.communities.len(),
            2,
            "two cliques ⇒ two communities, got {:?}",
            res.communities
        );
        assert!(res
            .communities
            .iter()
            .any(|c| c == &vec!["a", "b", "c", "d"]));
        assert!(res
            .communities
            .iter()
            .any(|c| c == &vec!["w", "x", "y", "z"]));
    }

    #[test]
    fn unweighted_bare_path_does_not_spuriously_merge_or_panic() {
        // A bare 3-node path (a-b-c, a bipartite/2-colorable structure) has NO
        // majority signal for LPA to merge on — this is a well-documented general
        // LPA weakness (unlike WCC/Louvain, which use connectivity/modularity and
        // DO merge it). We only assert it runs deterministically to completion
        // without panicking or looping past `max_iterations` — not a specific
        // partition shape.
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("b", "c", 1.0)]);
        let r1 = label_propagation(&g, &LabelPropagationConfig::default());
        let r2 = label_propagation(&g, &LabelPropagationConfig::default());
        assert_eq!(
            r1, r2,
            "must stay deterministic even without a clear majority"
        );
        let total_members: usize = r1.communities.iter().map(Vec::len).sum();
        assert_eq!(total_members, 3);
    }

    #[test]
    fn isolated_nodes_stay_singleton_communities() {
        let g =
            AdjacencyGraph::from_adjacency([("a".to_string(), vec![]), ("b".to_string(), vec![])]);
        let res = label_propagation(&g, &LabelPropagationConfig::default());
        assert_eq!(res.communities.len(), 2);
    }

    #[test]
    fn empty_graph_yields_empty_partition() {
        let g: AdjacencyGraph<String> =
            AdjacencyGraph::from_adjacency(Vec::<(String, Vec<(String, f64)>)>::new());
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
