//! Deterministic code-subgraph hub ranking for the R654.2 behavior slice.
//!
//! The full code-context surface still needs symbol-index anchor resolution,
//! bounded caller/callee expansion, operations diagnosis and served wiring.
//! This module owns only the reusable hub-ranking seam: it converts the
//! caller/callee edges already collected by a future context planner into the
//! existing `eg-compute` graph representation and orders hubs by directed
//! betweenness centrality with a stable symbol tie-break.

use eg_compute::graph_algos::{betweenness_centrality, AdjacencyGraph};

/// A directed caller-to-callee edge in a resolved code subgraph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeEdge {
    pub caller: String,
    pub callee: String,
}

impl CodeEdge {
    /// Construct one edge from the canonical symbol identifiers.
    pub fn new(caller: impl Into<String>, callee: impl Into<String>) -> Self {
        Self {
            caller: caller.into(),
            callee: callee.into(),
        }
    }
}

/// A code symbol ranked as a hub of the supplied subgraph.
#[derive(Debug, Clone, PartialEq)]
pub struct CodeHub {
    /// Canonical symbol identifier.
    pub symbol: String,
    /// Directed betweenness centrality over the supplied code subgraph.
    pub centrality: f64,
}

/// Rank the most central symbols in a resolved caller/callee subgraph.
///
/// The graph kernel supplies sorted node identities and deterministic Brandes
/// traversal. Ranking then sorts by descending centrality and ascending symbol
/// identifier, so equal-score hubs do not change order between runs. `limit`
/// bounds the returned result and zero returns an empty vector without building
/// a graph. Duplicate edges are merged by [`AdjacencyGraph`].
pub fn rank_hubs(edges: &[CodeEdge], limit: usize) -> Vec<CodeHub> {
    if limit == 0 || edges.is_empty() {
        return Vec::new();
    }

    let graph = AdjacencyGraph::from_edges(
        edges
            .iter()
            .map(|edge| (edge.caller.clone(), edge.callee.clone(), 1.0)),
    );
    let mut hubs: Vec<CodeHub> = betweenness_centrality(&graph, true)
        .into_iter()
        .map(|(symbol, centrality)| CodeHub { symbol, centrality })
        .collect();
    hubs.sort_by(|left, right| {
        right
            .centrality
            .total_cmp(&left.centrality)
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
    hubs.truncate(limit);
    hubs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rank_hubs_is_deterministic_and_tie_breaks_symbols() {
        let edges = vec![
            CodeEdge::new("repo/a.rs::main", "repo/b.rs::parse"),
            CodeEdge::new("repo/b.rs::parse", "repo/c.rs::normalize"),
            CodeEdge::new("repo/c.rs::normalize", "repo/d.rs::emit"),
        ];

        let first = rank_hubs(&edges, 2);
        let second = rank_hubs(&edges, 2);

        assert_eq!(first, second);
        assert_eq!(
            first
                .iter()
                .map(|hub| hub.symbol.as_str())
                .collect::<Vec<_>>(),
            vec!["repo/b.rs::parse", "repo/c.rs::normalize"]
        );
        assert_eq!(first[0].centrality, first[1].centrality);
        assert!(first[0].centrality > 0.0);
    }

    #[test]
    fn rank_hubs_refuses_empty_or_zero_budget_without_partial_rows() {
        let edge = CodeEdge::new("repo/a.rs::main", "repo/b.rs::parse");

        assert!(rank_hubs(&[], 4).is_empty());
        assert!(rank_hubs(&[edge], 0).is_empty());
    }
}
