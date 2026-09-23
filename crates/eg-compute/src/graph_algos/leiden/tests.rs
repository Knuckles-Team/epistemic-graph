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

/// Two 4-cliques (`a..d`, `w..z`) joined by one bridge edge (`d`-`w`) — the
/// shared fixture behind the plain Leiden/Louvain cross-check below and the
/// EH-283 CPM-large-gamma and default-quality-golden tests further down: one
/// definition, reused, rather than three hand-copies of the same nested
/// clique-edge loop (jscpd/dupehound).
pub(super) fn two_cliques_bridge_edges() -> Vec<(&'static str, &'static str, f64)> {
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
    edges
}

/// Three triangles joined in a ring by half-weight bridges -- the structure
/// that stresses local-moving order-dependence; shared with the hierarchy
/// tests.
pub(super) fn ring_of_triangles_edges() -> Vec<(&'static str, &'static str, f64)> {
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
    edges
}

/// A wall-clock budget over `n` nodes expired, was reported, and bounded the
/// kernel's run time.
pub(super) fn assert_budget_expired(
    deadline_hit: bool,
    elapsed: std::time::Duration,
    budget: std::time::Duration,
    n: usize,
) {
    assert!(
        deadline_hit,
        "a {budget:?} budget over {n} nodes must expire"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(3),
        "budget {budget:?} did not bound the kernel: elapsed={elapsed:?}"
    );
}

#[test]
fn leiden_finds_two_communities_in_two_cliques_matching_louvain() {
    // Same fixture as louvain's own test: two 4-cliques joined by one bridge.
    let edges = two_cliques_bridge_edges();

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
    let edges = ring_of_triangles_edges();

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
/// Leiden reuses `louvain::local_moving` verbatim, so it inherited that
/// module's MISSING wall-clock bound — Leiden never had one at all (this is
/// pre-existing, not caused by commit `a14b9c28`, but the shared kernel is
/// why it is fixed here). Same contract as Louvain's: the kernel stops on
/// TIME, returns the best partition so far, and says so.
#[test]
fn leiden_wall_clock_budget_truncates_large_graph_and_flags_it() {
    const N: usize = 20_000;
    let graph = budget_test_graph(N);
    let budget = std::time::Duration::from_millis(50);

    let started = Instant::now();
    let truncated = leiden(
        &graph,
        &LeidenConfig {
            budget,
            ..Default::default()
        },
    );
    let elapsed = started.elapsed();

    assert_budget_expired(truncated.deadline_hit, elapsed, budget, N);
    assert_covers(&truncated.communities, N);
}

/// A budget that does not fire must perturb nothing — Leiden's determinism
/// and its connectivity guarantee both have to survive the new field.
#[test]
fn leiden_budget_that_does_not_fire_changes_nothing() {
    const N: usize = 4_000;
    let graph = budget_test_graph(N);

    let converged = leiden(&graph, &LeidenConfig::default());
    assert!(
        !converged.deadline_hit,
        "the default 15s budget must not fire on a {N}-node fixture"
    );
    let generous = leiden(
        &graph,
        &LeidenConfig {
            budget: std::time::Duration::from_secs(600),
            ..Default::default()
        },
    );
    assert!(!generous.deadline_hit);
    assert_eq!(
        converged.communities, generous.communities,
        "a budget that does not fire must return the SAME partition"
    );
    assert_eq!(converged.modularity, generous.modularity);
    assert_covers(&converged.communities, N);

    let truncated = leiden(
        &graph,
        &LeidenConfig {
            budget: std::time::Duration::from_millis(1),
            ..Default::default()
        },
    );
    assert!(truncated.deadline_hit);
    assert_covers(&truncated.communities, N);
    assert!(
        truncated.communities.len() >= converged.communities.len(),
        "truncated={} converged={}",
        truncated.communities.len(),
        converged.communities.len()
    );
}

#[test]
fn leiden_small_graphs_never_report_a_deadline_hit() {
    let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("b", "c", 1.0), ("a", "c", 1.0)]);
    assert!(!leiden(&g, &LeidenConfig::default()).deadline_hit);
    let empty: AdjacencyGraph<&str> = AdjacencyGraph::from_edges([]);
    assert!(!leiden(&empty, &LeidenConfig::default()).deadline_hit);
}

/// Deterministic clustered fixture for the budget tests (blocks of 40 nodes
/// with 8 random intra-block edges each, joined in a ring).
fn budget_test_graph(n: usize) -> AdjacencyGraph<usize> {
    let mut rng = crate::SplitMix64::new(7);
    let mut adjacency: Vec<(usize, Vec<(usize, f64)>)> = (0..n).map(|i| (i, Vec::new())).collect();
    for (i, row) in adjacency.iter_mut().enumerate() {
        let start = (i / 40) * 40;
        let end = (start + 40).min(n);
        if end <= start + 1 {
            continue;
        }
        for _ in 0..8 {
            let j = start + rng.below(end - start);
            if j != i {
                row.1.push((j, 1.0));
            }
        }
    }
    let clusters = n.div_ceil(40);
    for cluster in 0..clusters {
        let a = cluster * 40;
        let b = ((cluster + 1) % clusters) * 40;
        if a < n && b < n && a != b {
            adjacency[a].1.push((b, 1.0));
        }
    }
    AdjacencyGraph::from_adjacency(adjacency)
}

// ── EH-283: CPM quality function ─────────────────────────────────────────

/// `num_cliques` triangles joined in a ring by single weight-1 bridge edges —
/// the classic Fortunato & Barthelemy (2007) resolution-limit fixture: every
/// triangle is a genuine clique, and the only inter-triangle edges are the
/// single ring bridges.
fn ring_of_triangles(num_cliques: usize) -> Vec<(String, String, f64)> {
    let mut edges = Vec::new();
    for c in 0..num_cliques {
        let (n0, n1, n2) = (format!("c{c}_0"), format!("c{c}_1"), format!("c{c}_2"));
        edges.push((n0.clone(), n1.clone(), 1.0));
        edges.push((n1, n2.clone(), 1.0));
        edges.push((n0, n2, 1.0));
    }
    for c in 0..num_cliques {
        let a = format!("c{c}_2");
        let b = format!("c{}_0", (c + 1) % num_cliques);
        edges.push((a, b, 1.0));
    }
    edges
}

/// Modularity's null model scales with the WHOLE graph's total edge weight
/// (`resolution · tot_c² / 2m`), which is exactly what gives it the
/// well-known RESOLUTION LIMIT: on a ring of enough triangles, modularity
/// optimization merges adjacent triangle PAIRS into one community even
/// though each triangle is a clique and the only inter-triangle edges are
/// single bridges. CPM's null model scales with each community's own SIZE
/// instead, so — tuned to this fixture's density — it resolves every
/// triangle as its own community where modularity cannot.
#[test]
fn cpm_resolves_cliques_where_modularity_merges_them_on_a_ring() {
    let g = AdjacencyGraph::from_edges(ring_of_triangles(20));

    let modularity_res = leiden(&g, &LeidenConfig::default());
    assert!(
        modularity_res.communities.len() < 20,
        "modularity should merge adjacent triangles on this ring (resolution limit): got {} communities: {:?}",
        modularity_res.communities.len(),
        modularity_res.communities
    );

    let cpm_res = leiden(
        &g,
        &LeidenConfig {
            quality: QualityFunction::Cpm,
            resolution: 0.5,
            ..Default::default()
        },
    );
    assert_eq!(
        cpm_res.communities.len(),
        20,
        "CPM at gamma=0.5 must resolve every triangle separately: {:?}",
        cpm_res.communities
    );
    for community in &cpm_res.communities {
        assert_eq!(
            community.len(),
            3,
            "every CPM community must be exactly one triangle: {community:?}"
        );
    }
}

/// As CPM's `resolution` (gamma) → 0, the size penalty vanishes entirely and
/// quality reduces to "more internal edges is always better" — maximized by
/// merging everything reachable into one community per connected component
/// (never merging ACROSS components: two nodes in different components share
/// no edge, so `w(v,C) = 0` for any cross-component move, and even a
/// near-zero positive gamma penalty makes that move strictly worse than
/// staying put).
#[test]
fn cpm_gamma_near_zero_gives_one_community_per_connected_component() {
    let mut edges = ring_of_triangles(6); // one 18-node connected component
    edges.push(("iso_a".to_string(), "iso_b".to_string(), 1.0));
    edges.push(("iso_b".to_string(), "iso_c".to_string(), 1.0));
    edges.push(("iso_a".to_string(), "iso_c".to_string(), 1.0));
    let g = AdjacencyGraph::from_edges(edges);

    let res = leiden(
        &g,
        &LeidenConfig {
            quality: QualityFunction::Cpm,
            resolution: 1e-6,
            ..Default::default()
        },
    );
    assert_eq!(
        res.communities.len(),
        2,
        "gamma near zero must yield exactly one community per connected component: {:?}",
        res.communities
    );
    let sizes: std::collections::BTreeSet<usize> = res.communities.iter().map(Vec::len).collect();
    assert_eq!(
        sizes,
        std::collections::BTreeSet::from([18, 3]),
        "each community must be exactly one whole connected component: {:?}",
        res.communities
    );
}

/// A large enough CPM `resolution` makes the size penalty dominate every
/// possible edge-weight gain, so no two nodes can ever gain by merging —
/// every community collapses to a singleton.
#[test]
fn cpm_gamma_large_gives_singletons() {
    let g = AdjacencyGraph::from_edges(two_cliques_bridge_edges());

    let res = leiden(
        &g,
        &LeidenConfig {
            quality: QualityFunction::Cpm,
            resolution: 10.0,
            ..Default::default()
        },
    );
    assert_eq!(res.communities.len(), 8, "{:?}", res.communities);
    for community in &res.communities {
        assert_eq!(community.len(), 1, "{:?}", res.communities);
    }
}

/// EH-283 must not move the DEFAULT (`QualityFunction::Modularity`) kernel's
/// output at all. Pins the exact modularity Leiden finds on the
/// two-4-cliques-bridge fixture (also covered by
/// `leiden_finds_two_communities_in_two_cliques_matching_louvain` above),
/// hand-derived from the standard modularity formula `Q = Σ_c [internal_c/2m
/// − (tot_c/2m)²]`: each clique has `internal_c = 12`, `tot_c = 13`
/// (weighted degree, including the bridge on one member), `2m = 26`, so
/// `tot_c/2m = 1/2` exactly and `Q = 2·(12/26 − 1/4) = 11/26`.
#[test]
fn default_modularity_quality_matches_pre_eh283_golden_value() {
    let g = AdjacencyGraph::from_edges(two_cliques_bridge_edges());

    assert_eq!(LeidenConfig::default().quality, QualityFunction::Modularity);
    let res = leiden(&g, &LeidenConfig::default());
    assert_eq!(res.communities.len(), 2);
    let golden = 11.0 / 26.0;
    assert!(
        (res.modularity - golden).abs() < 1e-9,
        "default modularity drifted from the pre-EH-283 golden: got {}, expected {golden}",
        res.modularity
    );
}

/// Every node in exactly one community — a truncated Leiden run must still
/// return a COMPLETE partition, not a partial one.
fn assert_covers(communities: &[Vec<usize>], n: usize) {
    let mut seen = vec![false; n];
    let mut total = 0usize;
    for community in communities {
        for &member in community {
            assert!(!seen[member], "node {member} appeared in two communities");
            seen[member] = true;
            total += 1;
        }
    }
    assert_eq!(total, n, "every node must appear in exactly one community");
}
