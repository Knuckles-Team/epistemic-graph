// CONCEPT:EG-KG.mining.community-writeback — Community detection as a mining family.
//
// THIS MODULE ADDS NO NEW ALGORITHM. The actual community-detection kernels —
// Louvain (modularity optimization) and Label Propagation — already exist as
// `eg_compute::graph_algos::{louvain, label_propagation}` (Neo4j GDS parity,
// exposed on the Cypher `CALL gds.*` surface in `eg-query`). This module is a
// THIN wrapper that (1) runs one of those two kernels, (2) uniformly scores
// every resulting community's INTERNAL EDGE DENSITY — `internal_edges /
// incident_edges`, the standard community-quality proxy computable from the
// SAME `AdjacencyGraph` public API for either algorithm (Louvain also reports a
// global modularity; label-propagation does not, so the per-community density
// score is the one signal available to BOTH, kept uniform on purpose) — so the
// `Mine*` family framing (one result shape, one epistemic writeback) covers
// community detection exactly like every other mining family, per the
// "wrap, don't reimplement" discipline.

use super::super::graph_algos::{
    label_propagation, louvain, AdjacencyGraph, LabelPropagationConfig, LouvainConfig,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    Louvain,
    LabelPropagation,
}

/// One detected community: its member node indices and its internal-density quality.
#[derive(Debug, Clone, PartialEq)]
pub struct Community {
    pub members: Vec<usize>,
    /// `internal_edges / incident_edges` over this community's members, `[0,1]`.
    /// A singleton (no incident edges at all) is vacuously `1.0` ("perfectly pure").
    pub density: f64,
}

/// The full community-detection result.
#[derive(Debug, Clone, PartialEq)]
pub struct CommunityResult {
    pub communities: Vec<Community>,
    /// Louvain's own global modularity; `None` for label-propagation (which has
    /// no modularity-gain objective).
    pub modularity: Option<f64>,
}

/// Run community detection over `graph` and score each resulting community's
/// internal density (CONCEPT:EG-KG.mining.community-writeback).
pub fn detect(
    graph: &AdjacencyGraph<usize>,
    algorithm: Algorithm,
    resolution: f64,
    max_iterations: usize,
    seed: u64,
    weighted: bool,
) -> CommunityResult {
    let (raw_communities, modularity): (Vec<Vec<usize>>, Option<f64>) = match algorithm {
        Algorithm::Louvain => {
            let cfg = LouvainConfig {
                resolution: if resolution > 0.0 { resolution } else { 1.0 },
                seed: Some(seed),
                max_sweeps: max_iterations.max(1),
                max_levels: 50,
            };
            let res = louvain(graph, &cfg);
            (res.communities, Some(res.modularity))
        }
        Algorithm::LabelPropagation => {
            let cfg = LabelPropagationConfig {
                max_iterations: max_iterations.max(1),
                weighted,
            };
            let res = label_propagation(graph, &cfg);
            (res.communities, None)
        }
    };

    let communities = raw_communities
        .into_iter()
        .map(|members| {
            let density = community_density(graph, &members);
            Community { members, density }
        })
        .collect();

    CommunityResult {
        communities,
        modularity,
    }
}

/// Internal edge density of one community: fraction of its members' incident
/// (directed) edges that land INSIDE the same community.
fn community_density(graph: &AdjacencyGraph<usize>, members: &[usize]) -> f64 {
    use std::collections::HashSet;
    let set: HashSet<usize> = members.iter().copied().collect();
    let mut internal = 0.0f64;
    let mut incident = 0.0f64;
    for &m in members {
        let Some(idx) = graph.index_of(&m) else {
            continue;
        };
        for &(v, w) in graph.out_edges(idx) {
            incident += w;
            let v_id = *graph.node_at(v);
            if set.contains(&v_id) {
                internal += w;
            }
        }
        for &(u, w) in graph.in_edges(idx) {
            incident += w;
            let u_id = *graph.node_at(u);
            if set.contains(&u_id) {
                internal += w;
            }
        }
    }
    if incident > 0.0 {
        (internal / incident).clamp(0.0, 1.0)
    } else {
        1.0 // no edges at all touching this community ⇒ vacuously "pure"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two tight triangles bridged by one weak edge — the textbook community fixture.
    fn two_triangles() -> AdjacencyGraph<usize> {
        AdjacencyGraph::from_adjacency(vec![
            (0usize, vec![(1, 1.0), (2, 1.0)]),
            (1, vec![(0, 1.0), (2, 1.0)]),
            (2, vec![(0, 1.0), (1, 1.0), (3, 0.01)]), // weak bridge
            (3, vec![(2, 0.01), (4, 1.0), (5, 1.0)]),
            (4, vec![(3, 1.0), (5, 1.0)]),
            (5, vec![(3, 1.0), (4, 1.0)]),
        ])
    }

    #[test]
    fn louvain_finds_two_dense_communities() {
        let g = two_triangles();
        let out = detect(&g, Algorithm::Louvain, 1.0, 100, 0, true);
        assert_eq!(out.communities.len(), 2);
        assert!(out.modularity.is_some());
        assert!(out.modularity.unwrap() > 0.0);
        for c in &out.communities {
            assert_eq!(c.members.len(), 3);
        }
    }

    #[test]
    fn label_propagation_has_no_modularity_but_still_scores_density() {
        let g = two_triangles();
        let out = detect(&g, Algorithm::LabelPropagation, 1.0, 20, 0, true);
        assert!(out.modularity.is_none());
        assert!(!out.communities.is_empty());
        for c in &out.communities {
            assert!((0.0..=1.0).contains(&c.density));
        }
    }

    #[test]
    fn tight_community_has_high_density() {
        let g = two_triangles();
        let out = detect(&g, Algorithm::Louvain, 1.0, 100, 0, true);
        // Each 3-clique community should have HIGH internal density (the one weak
        // bridge edge is the only thing pulling it below 1.0).
        for c in &out.communities {
            assert!(
                c.density > 0.9,
                "density {} too low for a tight triangle",
                c.density
            );
        }
    }

    #[test]
    fn isolated_singleton_has_vacuous_full_density() {
        let g: AdjacencyGraph<usize> =
            AdjacencyGraph::from_adjacency(vec![(0usize, Vec::<(usize, f64)>::new())]);
        let out = detect(&g, Algorithm::Louvain, 1.0, 10, 0, true);
        assert_eq!(out.communities.len(), 1);
        assert_eq!(out.communities[0].density, 1.0);
    }
}
