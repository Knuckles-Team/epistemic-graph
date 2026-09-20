// EH-285(a)(b) — a resolution sweep with a stability notion, and naming
// communities by their highest-centrality member instead of a positional
// index.

use std::hash::Hash;

use super::super::graph::AdjacencyGraph;
use super::{leiden, LeidenConfig};

/// One community found during a [`resolution_sweep`], annotated with how many
/// OTHER resolutions in the sweep also produced an IDENTICAL community (the
/// same member set) — EH-285(a)'s "keep communities that persist across
/// resolutions" taken literally: exact-set recurrence, not a full
/// co-clustering consensus matrix (see [`resolution_sweep`]'s doc for why
/// that's the deliberately cheaper choice here). `support == 0` means "only
/// this resolution found it this way"; a higher `support` is the confidence
/// signal the ledger row asks for.
// `PartialEq` only, deliberately not `Eq`: `resolution: f64` cannot implement
// `Eq` (NaN breaks reflexivity), so a derived `Eq` here would not compile.
#[derive(Debug, Clone, PartialEq)]
pub struct StableCommunity<N> {
    pub members: Vec<N>,
    /// The resolution this particular occurrence was found at.
    pub resolution: f64,
    pub support: usize,
}

/// EH-285(a): run [`leiden`] once per value in `resolutions` (holding every
/// other [`LeidenConfig`] field — quality function, seed, budget — fixed) and
/// annotate every resulting community with how many OTHER runs in the sweep
/// also produced an identical community.
///
/// This is a SIMPLER stability notion than a full co-clustering consensus
/// matrix (Lancichinetti & Fortunato): exact member-set recurrence across
/// sweep points, `O(R · (V+E) + R² · C)` for `R` resolutions and `C`
/// communities per run (dominated by the `R` Leiden runs themselves), not the
/// `O(V²)` pairwise co-clustering tally a full consensus matrix would need. A
/// community that shifts by even one member between two resolutions counts
/// as two DIFFERENT communities under this metric, not a near-match — a
/// known, documented limitation, not a hidden one.
///
/// Duplicate values in `resolutions` are each run independently (not
/// deduplicated first) — sweeping the same value twice is how a caller would
/// sanity-check this kernel's own determinism.
pub fn resolution_sweep<N>(
    graph: &AdjacencyGraph<N>,
    resolutions: &[f64],
    config: &LeidenConfig,
) -> Vec<StableCommunity<N>>
where
    N: Clone + Eq + Hash + Ord,
{
    let runs: Vec<(f64, Vec<Vec<N>>)> = resolutions
        .iter()
        .map(|&resolution| {
            let cfg = LeidenConfig {
                resolution,
                ..*config
            };
            (resolution, leiden(graph, &cfg).communities)
        })
        .collect();

    let mut out = Vec::new();
    for (run_index, (resolution, communities)) in runs.iter().enumerate() {
        for community in communities {
            let support = recurrence_count(&runs, run_index, community);
            out.push(StableCommunity {
                members: community.clone(),
                resolution: *resolution,
                support,
            });
        }
    }
    out
}

/// How many runs OTHER than `run_index` produced a community with exactly
/// `community`'s member set. Leiden's own contract sorts every community's
/// members (see [`super::LeidenResult`]'s doc), so this is a plain `Vec`
/// equality check, not a set comparison.
fn recurrence_count<N: Eq>(
    runs: &[(f64, Vec<Vec<N>>)],
    run_index: usize,
    community: &[N],
) -> usize {
    runs.iter()
        .enumerate()
        .filter(|(other_index, (_, other_communities))| {
            *other_index != run_index && other_communities.iter().any(|c| c == community)
        })
        .count()
}

/// One community, identified by its highest-centrality member instead of its
/// position in a returned `Vec` — EH-285(b). Leiden returns communities as
/// plain indices into a `Vec`; a graph that changed even slightly between two
/// ingests can renumber every community, churning every downstream reference
/// that named a community by index. Naming by the member with the highest
/// weighted degree (the SAME undirected symmetrisation Leiden itself
/// clusters over — see [`AdjacencyGraph::undirected_weighted_adjacency`])
/// gives each community a name that survives a re-run unless its actual
/// highest-degree member changes, a much rarer event than a `Vec` index
/// reshuffling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedCommunity<N> {
    /// The community's stable identity: its highest-weighted-degree member,
    /// ties broken toward the smaller member (by `Ord`) for determinism.
    pub name: N,
    pub members: Vec<N>,
}

/// Name every community in `communities` by its highest-weighted-degree
/// member in `graph` (EH-285(b)). `communities` need not come from
/// [`leiden`] directly — any partition of `graph`'s nodes works, including a
/// [`resolution_sweep`] result's `members`.
///
/// Panics if any community is empty — an empty community is not a partition
/// entry any caller in this crate ever produces (`leiden`/`leiden_hierarchy`
/// only ever push a community once it has at least one member), so this is a
/// documented precondition, not a runtime possibility this function absorbs
/// silently.
pub fn name_communities<N>(
    graph: &AdjacencyGraph<N>,
    communities: Vec<Vec<N>>,
) -> Vec<NamedCommunity<N>>
where
    N: Clone + Eq + Hash + Ord,
{
    let degree = degree_by_node(graph);
    communities
        .into_iter()
        .map(|members| {
            let name = highest_degree_member(graph, &degree, &members);
            NamedCommunity { name, members }
        })
        .collect()
}

/// Weighted degree of every node under the graph's undirected
/// symmetrisation, indexed the SAME way `graph.index_of` returns — computed
/// once for the whole graph rather than once per community member.
fn degree_by_node<N>(graph: &AdjacencyGraph<N>) -> Vec<f64>
where
    N: Clone + Eq + Hash + Ord,
{
    graph
        .undirected_weighted_adjacency()
        .iter()
        .map(|row| row.iter().map(|(_, w)| *w).sum())
        .collect()
}

/// The member of `members` with the highest `degree`, ties broken toward the
/// smaller member (`Ord`) — an explicit ascending scan that only replaces the
/// running best on a STRICT improvement, so ties always resolve to whichever
/// candidate was visited first in sorted order, independent of `members`'
/// own input order.
fn highest_degree_member<N>(graph: &AdjacencyGraph<N>, degree: &[f64], members: &[N]) -> N
where
    N: Clone + Eq + Hash + Ord,
{
    let mut sorted: Vec<&N> = members.iter().collect();
    sorted.sort();
    let mut candidates = sorted.into_iter();
    let mut best = candidates
        .next()
        .expect("name_communities: empty community");
    let mut best_degree = node_degree(graph, degree, best);
    for candidate in candidates {
        let candidate_degree = node_degree(graph, degree, candidate);
        if candidate_degree > best_degree {
            best = candidate;
            best_degree = candidate_degree;
        }
    }
    best.clone()
}

fn node_degree<N>(graph: &AdjacencyGraph<N>, degree: &[f64], node: &N) -> f64
where
    N: Clone + Eq + Hash + Ord,
{
    graph.index_of(node).map(|idx| degree[idx]).unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn bridged_four_cliques() -> AdjacencyGraph<&'static str> {
        let clique1 = ["a", "b", "c", "d"];
        let clique2 = ["w", "x", "y", "z"];
        let mut edges: Vec<(&str, &str, f64)> = Vec::new();
        for clique in [clique1, clique2] {
            for i in 0..clique.len() {
                for j in (i + 1)..clique.len() {
                    edges.push((clique[i], clique[j], 1.0));
                }
            }
        }
        edges.push(("d", "w", 1.0));
        AdjacencyGraph::from_edges(edges)
    }

    /// Leiden is deterministic (`leiden::tests::leiden_is_deterministic_across_runs`),
    /// so sweeping the SAME resolution three times must reproduce every
    /// community at FULL support (matched by both other runs) — a zero-risk
    /// way to prove the recurrence counting itself is correct, independent of
    /// how modularity happens to behave across genuinely DIFFERENT
    /// resolutions on any particular fixture (which this crate has no
    /// existing proven expectation for, unlike the single-resolution
    /// two-clique fixture `bridged_four_cliques` mirrors).
    #[test]
    fn identical_resolutions_in_a_sweep_recur_with_full_support() {
        let g = bridged_four_cliques();
        let cfg = LeidenConfig {
            budget: Duration::from_secs(5),
            ..LeidenConfig::default()
        };
        let stable = resolution_sweep(&g, &[1.0, 1.0, 1.0], &cfg);
        assert!(!stable.is_empty());
        for community in &stable {
            assert_eq!(
                community.support, 2,
                "identical resolutions must reproduce every community at full support: {community:?}"
            );
        }
    }

    #[test]
    fn resolution_sweep_covers_every_requested_resolution() {
        let g = bridged_four_cliques();
        let cfg = LeidenConfig {
            budget: Duration::from_secs(5),
            ..LeidenConfig::default()
        };
        let stable = resolution_sweep(&g, &[0.5, 1.0, 1.5], &cfg);
        for &resolution in &[0.5, 1.0, 1.5] {
            assert!(
                stable.iter().any(|s| s.resolution == resolution),
                "missing any community at resolution {resolution}"
            );
        }
    }

    #[test]
    fn resolution_sweep_is_deterministic() {
        let g = bridged_four_cliques();
        let cfg = LeidenConfig {
            budget: Duration::from_secs(5),
            ..LeidenConfig::default()
        };
        let a = resolution_sweep(&g, &[0.5, 1.0], &cfg);
        let b = resolution_sweep(&g, &[0.5, 1.0], &cfg);
        assert_eq!(a, b);
    }

    #[test]
    fn names_the_highest_degree_member_of_each_community() {
        // "b" is connected to every other member (degree 3); the rest have
        // degree 2 or less. "b" must be the name regardless of position.
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("b", "c", 1.0),
            ("b", "d", 1.0),
            ("c", "d", 1.0),
        ]);
        let named = name_communities(&g, vec![vec!["a", "b", "c", "d"]]);
        assert_eq!(named.len(), 1);
        assert_eq!(named[0].name, "b");
    }

    #[test]
    fn ties_break_toward_the_smaller_member() {
        // A symmetric 4-cycle: every member has the same weighted degree (2).
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("b", "c", 1.0),
            ("c", "d", 1.0),
            ("d", "a", 1.0),
        ]);
        let named = name_communities(&g, vec![vec!["a", "b", "c", "d"]]);
        assert_eq!(
            named[0].name, "a",
            "tie must break toward the smallest member"
        );
    }

    #[test]
    fn naming_a_singleton_names_it_after_itself() {
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0)]);
        let named = name_communities(&g, vec![vec!["a"]]);
        assert_eq!(named[0].name, "a");
    }
}
