// EH-285(c) — a standalone connectivity post-check: a free implementation-bug
// detector, not a new guarantee. The guarantee itself is the refinement
// phase (see the `leiden` module doc); this is what proves it held.

use std::collections::HashSet;
use std::hash::Hash;

use super::super::components::weakly_connected_components;
use super::super::graph::AdjacencyGraph;

/// Verify every community's induced subgraph is a single connected
/// component.
///
/// Under a correctly implemented Leiden this is a NO-OP: the refinement
/// phase already makes "every returned community induces a connected
/// subgraph" a STRUCTURAL guarantee (see the `leiden` module doc), so a
/// violation here means the guarantee itself has a regression, not that the
/// input graph is unusual. [`super::leiden`] and [`super::leiden_hierarchy`]
/// both `debug_assert!` this returns empty on every partition they build —
/// zero cost in a release build, and it fires under `cargo test` across
/// every existing fixture for free.
///
/// Returns the indices into `communities` of every community whose induced
/// subgraph is NOT one connected component (empty ⇒ fully connected, the
/// expected case). A community of 0 or 1 members is trivially connected and
/// never reported.
pub fn verify_communities_connected<N>(
    graph: &AdjacencyGraph<N>,
    communities: &[Vec<N>],
) -> Vec<usize>
where
    N: Clone + Eq + Hash + Ord,
{
    communities
        .iter()
        .enumerate()
        .filter(|(_, community)| community.len() > 1)
        .filter(|(_, community)| !induces_one_component(graph, community))
        .map(|(idx, _)| idx)
        .collect()
}

fn induces_one_component<N>(graph: &AdjacencyGraph<N>, community: &[N]) -> bool
where
    N: Clone + Eq + Hash + Ord,
{
    let members: HashSet<&N> = community.iter().collect();
    let adjacency: Vec<(N, Vec<(N, f64)>)> = community
        .iter()
        .map(|member| (member.clone(), induced_row(graph, &members, member)))
        .collect();
    let induced = AdjacencyGraph::from_adjacency(adjacency);
    weakly_connected_components(&induced).len() == 1
}

/// One community member's edges restricted to neighbours ALSO in the
/// community — a member absent from `graph` entirely (should not happen for
/// a partition this crate itself produced) contributes an empty row, which
/// still leaves it in the induced graph as its own island rather than
/// panicking.
fn induced_row<N>(graph: &AdjacencyGraph<N>, members: &HashSet<&N>, member: &N) -> Vec<(N, f64)>
where
    N: Clone + Eq + Hash + Ord,
{
    let Some(idx) = graph.index_of(member) else {
        return Vec::new();
    };
    graph
        .out_edges(idx)
        .iter()
        .filter_map(|&(neighbor_idx, weight)| {
            let neighbor = graph.node_at(neighbor_idx);
            members
                .contains(neighbor)
                .then(|| (neighbor.clone(), weight))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_correct_partition_reports_no_violations() {
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("b", "c", 1.0),
            ("a", "c", 1.0),
            ("x", "y", 1.0),
        ]);
        let communities = vec![vec!["a", "b", "c"], vec!["x", "y"]];
        assert!(verify_communities_connected(&g, &communities).is_empty());
    }

    /// The known-bad input: a hand-crafted partition (NOT produced by the
    /// algorithm) that groups two genuinely disconnected pairs into one
    /// claimed community, exactly the class of bug the refinement phase
    /// exists to prevent. A checker that cannot catch this proves nothing.
    #[test]
    fn a_deliberately_broken_partition_is_flagged() {
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("c", "d", 1.0)]);
        // "a","b","c","d" claimed as ONE community, but {a,b} and {c,d} share
        // no edge at all.
        let communities = vec![vec!["a", "b", "c", "d"]];
        let violations = verify_communities_connected(&g, &communities);
        assert_eq!(violations, vec![0]);
    }

    #[test]
    fn a_mix_of_good_and_bad_communities_flags_only_the_bad_one() {
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("b", "c", 1.0),
            ("a", "c", 1.0),
            ("x", "y", 1.0),
            ("p", "q", 1.0),
        ]);
        // Community 0 is a genuine triangle (connected). Community 1 falsely
        // claims "x","y" AND "p","q" as one community despite no edge
        // between the two pairs.
        let communities = vec![vec!["a", "b", "c"], vec!["x", "y", "p", "q"]];
        assert_eq!(verify_communities_connected(&g, &communities), vec![1]);
    }

    #[test]
    fn singleton_and_empty_communities_are_never_flagged() {
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0)]);
        let communities = vec![vec!["a"], Vec::new()];
        assert!(verify_communities_connected(&g, &communities).is_empty());
    }
}
