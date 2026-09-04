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
    let mut state = TarjanState::for_node_count(n);

    for root in 0..n {
        if state.index_of[root] != UNVISITED {
            continue;
        }
        state.visit_root(graph, root);
    }

    graph.label_partition(&state.comp_id)
}

const UNVISITED: usize = usize::MAX;

struct TarjanState {
    index_of: Vec<usize>,
    lowlink: Vec<usize>,
    on_stack: Vec<bool>,
    stack: Vec<usize>,
    next_index: usize,
    comp_id: Vec<usize>,
    next_component: usize,
    work: Vec<(usize, usize)>,
}

impl TarjanState {
    fn for_node_count(n: usize) -> Self {
        Self {
            index_of: vec![UNVISITED; n],
            lowlink: vec![0; n],
            on_stack: vec![false; n],
            stack: Vec::new(),
            next_index: 0,
            comp_id: vec![UNVISITED; n],
            next_component: 0,
            work: Vec::new(),
        }
    }

    fn visit_root<N>(&mut self, graph: &AdjacencyGraph<N>, root: usize)
    where
        N: Clone + Eq + Hash + Ord,
    {
        self.work.push((root, 0));
        while !self.work.is_empty() {
            self.start_frame();
            if self.advance_edge(graph) {
                continue;
            }
            self.finish_frame();
        }
    }

    fn start_frame(&mut self) {
        let (node, position) = *self.work.last().expect("Tarjan frame must exist");
        if position != 0 {
            return;
        }
        self.index_of[node] = self.next_index;
        self.lowlink[node] = self.next_index;
        self.next_index += 1;
        self.stack.push(node);
        self.on_stack[node] = true;
    }

    fn advance_edge<N>(&mut self, graph: &AdjacencyGraph<N>) -> bool
    where
        N: Clone + Eq + Hash + Ord,
    {
        let frame = self.work.len() - 1;
        let (node, position) = self.work[frame];
        let edges = graph.out_edges(node);
        if position >= edges.len() {
            return false;
        }
        let next = edges[position].0;
        self.work[frame].1 += 1;
        if self.index_of[next] == UNVISITED {
            self.work.push((next, 0));
        } else if self.on_stack[next] {
            self.lowlink[node] = self.lowlink[node].min(self.index_of[next]);
        }
        true
    }

    fn finish_frame(&mut self) {
        let node = self.work.last().expect("Tarjan frame must exist").0;
        if self.lowlink[node] == self.index_of[node] {
            self.pop_component(node);
        }
        self.work.pop();
        if let Some(&(parent, _)) = self.work.last() {
            self.lowlink[parent] = self.lowlink[parent].min(self.lowlink[node]);
        }
    }

    fn pop_component(&mut self, root: usize) {
        loop {
            let node = self.stack.pop().expect("Tarjan stack must contain root");
            self.on_stack[node] = false;
            self.comp_id[node] = self.next_component;
            if node == root {
                break;
            }
        }
        self.next_component += 1;
    }
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
