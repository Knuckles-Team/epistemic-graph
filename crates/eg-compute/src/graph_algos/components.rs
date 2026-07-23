// CONCEPT:EG-KG.compute.connected-components — Connected components: weakly-connected (union-find) and
// strongly-connected (Tarjan). Neo4j GDS `wcc` / `scc` parity.

use super::graph::AdjacencyGraph;
use std::hash::Hash;

/// Union-Find (disjoint-set) with union-by-rank + path compression.
/// `pub(crate)` — reused as-is by [`super::steiner`] for its metric-closure and
/// union-subgraph MST passes (Kruskal's algorithm).
pub(crate) struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<u32>,
}

impl UnionFind {
    pub(crate) fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    pub(crate) fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]]; // path halving
            x = self.parent[x];
        }
        x
    }

    pub(crate) fn union(&mut self, a: usize, b: usize) {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra == rb {
            return;
        }
        // Union by rank; on tie attach larger index under smaller for stability.
        match self.rank[ra].cmp(&self.rank[rb]) {
            std::cmp::Ordering::Less => self.parent[ra] = rb,
            std::cmp::Ordering::Greater => self.parent[rb] = ra,
            std::cmp::Ordering::Equal => {
                self.parent[rb] = ra;
                self.rank[ra] += 1;
            }
        }
    }
}

/// Weakly connected components via union-find: edges treated as undirected.
///
/// Returns components as `Vec<Vec<N>>` — members sorted, components ordered by
/// their smallest member (deterministic). Isolated nodes form singletons.
///
/// Complexity: `O((V + E) · α(V))`, effectively linear. CONCEPT:EG-KG.compute.connected-components
pub fn weakly_connected_components<N>(graph: &AdjacencyGraph<N>) -> Vec<Vec<N>>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let mut uf = UnionFind::new(n);
    for u in 0..n {
        for &(v, _) in graph.out_edges(u) {
            uf.union(u, v);
        }
    }
    let membership: Vec<usize> = (0..n).map(|i| uf.find(i)).collect();
    graph.label_partition(&membership)
}

/// Strongly connected components via **Tarjan's algorithm** (iterative, so deep
/// graphs cannot overflow the stack). Respects edge direction: two nodes share an
/// SCC iff each is reachable from the other.
///
/// Returns components as `Vec<Vec<N>>` — members sorted, components ordered by
/// their smallest member (deterministic; neighbour iteration follows sorted index
/// order so the discovery is reproducible).
///
/// Complexity: `O(V + E)`. CONCEPT:EG-KG.compute.connected-components
pub fn strongly_connected_components<N>(graph: &AdjacencyGraph<N>) -> Vec<Vec<N>>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    const UNVISITED: usize = usize::MAX;

    let mut index_of = vec![UNVISITED; n]; // discovery index
    let mut lowlink = vec![0usize; n];
    let mut on_stack = vec![false; n];
    let mut stack: Vec<usize> = Vec::new();
    let mut next_index = 0usize;
    let mut comp_id = vec![UNVISITED; n];
    let mut n_comps = 0usize;

    // Explicit DFS frame: (node, position in its out-edge list).
    let mut work: Vec<(usize, usize)> = Vec::new();

    for root in 0..n {
        if index_of[root] != UNVISITED {
            continue;
        }
        work.push((root, 0));
        while let Some(&mut (v, ref mut pos)) = work.last_mut() {
            if *pos == 0 {
                // First visit of v.
                index_of[v] = next_index;
                lowlink[v] = next_index;
                next_index += 1;
                stack.push(v);
                on_stack[v] = true;
            }
            let edges = graph.out_edges(v);
            if *pos < edges.len() {
                let w = edges[*pos].0;
                *pos += 1;
                if index_of[w] == UNVISITED {
                    work.push((w, 0));
                } else if on_stack[w] {
                    lowlink[v] = lowlink[v].min(index_of[w]);
                }
            } else {
                // Done with v: if it's a root of an SCC, pop the component.
                if lowlink[v] == index_of[v] {
                    loop {
                        let w = stack.pop().unwrap();
                        on_stack[w] = false;
                        comp_id[w] = n_comps;
                        if w == v {
                            break;
                        }
                    }
                    n_comps += 1;
                }
                work.pop();
                if let Some(&(parent, _)) = work.last() {
                    lowlink[parent] = lowlink[parent].min(lowlink[v]);
                }
            }
        }
    }

    graph.label_partition(&comp_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg144_wcc_partitions_two_islands() {
        // {a,b,c} and {x,y} — two weakly connected islands + edge direction
        // irrelevant (c→b reversed still connects).
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("c", "b", 1.0), ("x", "y", 1.0)]);
        let comps = weakly_connected_components(&g);
        assert_eq!(comps.len(), 2);
        assert_eq!(comps[0], vec!["a", "b", "c"]);
        assert_eq!(comps[1], vec!["x", "y"]);
    }

    #[test]
    fn eg144_wcc_isolated_node_is_singleton() {
        let g = AdjacencyGraph::from_adjacency([("a", vec![("b", 1.0)]), ("lonely", vec![])]);
        let comps = weakly_connected_components(&g);
        assert_eq!(comps.len(), 2);
        assert!(comps.iter().any(|c| c == &vec!["lonely"]));
    }

    #[test]
    fn eg144_scc_finds_cycle_vs_singletons() {
        // Known digraph: a→b→c→a (one 3-cycle) plus c→d, d is its own SCC.
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("b", "c", 1.0),
            ("c", "a", 1.0),
            ("c", "d", 1.0),
        ]);
        let sccs = strongly_connected_components(&g);
        // Two SCCs: {a,b,c} and {d}.
        assert_eq!(sccs.len(), 2);
        assert!(sccs.iter().any(|s| s == &vec!["a", "b", "c"]));
        assert!(sccs.iter().any(|s| s == &vec!["d"]));
    }

    #[test]
    fn eg144_scc_directed_chain_all_singletons() {
        // a→b→c with no back-edges: three separate SCCs.
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("b", "c", 1.0)]);
        let sccs = strongly_connected_components(&g);
        assert_eq!(sccs.len(), 3);
        for s in &sccs {
            assert_eq!(s.len(), 1);
        }
    }

    #[test]
    fn eg144_scc_two_cycles_bridged() {
        // Two 2-cycles a↔b and c↔d bridged by b→c (bridge is one-way).
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("b", "a", 1.0),
            ("b", "c", 1.0),
            ("c", "d", 1.0),
            ("d", "c", 1.0),
        ]);
        let sccs = strongly_connected_components(&g);
        assert_eq!(sccs.len(), 2);
        assert!(sccs.iter().any(|s| s == &vec!["a", "b"]));
        assert!(sccs.iter().any(|s| s == &vec!["c", "d"]));
    }
}
