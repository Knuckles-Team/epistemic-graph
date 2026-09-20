use super::*;
use crate::SplitMix64;

/// Every level's communities must be a strict coarsening of the level
/// below: each parent's member set is EXACTLY the union of its children's
/// member sets (never a partial merge, never a member appearing under two
/// different parents).
fn assert_strict_nesting<N: Clone + Eq + StdHash + Ord + std::fmt::Debug>(
    hierarchy: &LeidenHierarchy<N>,
    leaf_nodes: &[N],
) {
    assert!(!hierarchy.levels.is_empty());
    // Level 1 must partition every leaf node exactly once.
    let mut seen = std::collections::BTreeSet::new();
    for c in &hierarchy.levels[0].communities {
        for m in c {
            assert!(
                seen.insert(m.clone()),
                "node {m:?} appears twice at level 1"
            );
        }
    }
    assert_eq!(seen, leaf_nodes.iter().cloned().collect());

    for w in hierarchy.levels.windows(2) {
        let (lower, upper) = (&w[0], &w[1]);
        assert_eq!(lower.parent.len(), lower.communities.len());
        for (c_idx, community) in lower.communities.iter().enumerate() {
            let parent_idx = lower.parent[c_idx].expect("non-top level must have a parent");
            let parent_members: std::collections::BTreeSet<N> =
                upper.communities[parent_idx].iter().cloned().collect();
            for m in community {
                assert!(
                    parent_members.contains(m),
                    "level member {m:?} of community {c_idx} is missing from its \
                     claimed parent {parent_idx}"
                );
            }
        }
    }
    // The top level's own parents must all be `None`.
    for p in &hierarchy.levels.last().unwrap().parent {
        assert!(p.is_none());
    }
}

#[test]
fn hierarchy_nests_strictly_on_two_bridged_cliques() {
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
    let g = AdjacencyGraph::from_edges(edges);
    let hierarchy = leiden_hierarchy(&g, &LeidenConfig::default());
    assert_strict_nesting(&hierarchy, g.nodes());

    // The FINAL level of the hierarchy must agree with `leiden`'s own flat
    // result (same underlying loop, same stopping condition).
    let flat = leiden(&g, &LeidenConfig::default());
    let top = hierarchy.levels.last().unwrap();
    let mut top_sorted = top.communities.clone();
    top_sorted.sort();
    let mut flat_sorted = flat.communities.clone();
    flat_sorted.sort();
    assert_eq!(top_sorted, flat_sorted);
    assert!((top.modularity - flat.modularity).abs() < 1e-9);
}

#[test]
fn hierarchy_ring_of_triangles_nests_strictly_and_is_deterministic() {
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
    let g = AdjacencyGraph::from_edges(edges);
    let h1 = leiden_hierarchy(&g, &LeidenConfig::default());
    assert_strict_nesting(&h1, g.nodes());

    let h2 = leiden_hierarchy(&g, &LeidenConfig::default());
    fn sig<'a>(h: &'a LeidenHierarchy<&'a str>) -> Vec<Vec<Vec<&'a str>>> {
        h.levels.iter().map(|l| l.communities.clone()).collect()
    }
    assert_eq!(sig(&h1), sig(&h2), "hierarchy must be deterministic");
}

#[test]
fn hierarchy_empty_graph_has_no_levels() {
    let g: AdjacencyGraph<&str> =
        AdjacencyGraph::from_adjacency(Vec::<(&str, Vec<(&str, f64)>)>::new());
    let h = leiden_hierarchy(&g, &LeidenConfig::default());
    assert!(h.levels.is_empty());
}

#[test]
fn hierarchy_no_edges_has_no_levels_above_leaves() {
    // Isolated nodes: `m2 <= 0.0` short-circuits `leiden_hierarchy_raw` to
    // no levels — callers treat the graph's own nodes as the implicit leaves.
    let g: AdjacencyGraph<&str> =
        AdjacencyGraph::from_adjacency([("a", Vec::<(&str, f64)>::new()), ("b", Vec::new())]);
    let h = leiden_hierarchy(&g, &LeidenConfig::default());
    assert!(h.levels.is_empty());
}

#[test]
fn hierarchy_single_clique_single_level_single_root() {
    let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("b", "c", 1.0), ("a", "c", 1.0)]);
    let h = leiden_hierarchy(&g, &LeidenConfig::default());
    assert_eq!(h.levels.len(), 1);
    assert_eq!(h.levels[0].communities.len(), 1);
    assert_eq!(h.levels[0].communities[0], vec!["a", "b", "c"]);
    assert_eq!(h.levels[0].parent, vec![None]);
}

/// A fixture large enough to reliably produce 2+ levels (unlike the small
/// hand-written fixtures above, which all happen to converge in exactly one
/// level — so `assert_strict_nesting`'s cross-level `parent` check never
/// actually ran on them). This is the regression test for a real bug caught
/// only at 25k-node benchmark scale: `parent` was built from the WRONG
/// iteration's `refined` array (this level's own `refined`, whose length is
/// the PREVIOUS level's community count) instead of the NEXT iteration's
/// (whose length matches THIS level's community count) — see the fix's
/// comment in `leiden_hierarchy`.
#[test]
fn hierarchy_multi_level_nests_strictly_on_a_synthetic_graph() {
    let (g, _edges) = synthetic_clustered_graph(3_000, 25, 8, 7);
    let hierarchy = leiden_hierarchy(&g, &LeidenConfig::default());
    assert!(
        hierarchy.levels.len() >= 2,
        "fixture must exercise multi-level nesting, got {} level(s)",
        hierarchy.levels.len()
    );
    assert_strict_nesting(&hierarchy, g.nodes());
}

// ── VIZ-1 scale benchmarks ──────────────────────────────────────────────
//
// NOT run by default (`#[ignore]`): these build synthetic graphs up to 1M
// nodes and are slow. Run explicitly with:
//   cargo test -p eg-compute --target-dir ./target-isolated -j 12 \
//     graph_algos::leiden::hierarchy_tests::bench_ -- --ignored --nocapture
// This is a `cargo test` (debug/`test`-profile) run, NOT `cargo bench` —
// the workspace build-discipline note forbids a `--release` target here
// (~97 GB/target, no budget), so these numbers are debug-profile timings:
// pessimistic vs. a release build, but still an honest, reproducible
// measurement of what this algorithm costs today, on this build tier.

/// A synthetic "planted-partition"-style graph resembling KG community
/// structure: `n` nodes grouped into dense clusters of `cluster_size`, each
/// cluster wired to its two ring-neighbours by a handful of sparse bridge
/// edges. Deterministic for a fixed `seed`. Returns `(adjacency, edge_count)`.
fn synthetic_clustered_graph(
    n: usize,
    cluster_size: usize,
    intra_degree: usize,
    seed: u64,
) -> (AdjacencyGraph<usize>, usize) {
    let num_clusters = n.div_ceil(cluster_size);
    let mut rng = SplitMix64::new(seed);
    let mut adjacency: Vec<(usize, Vec<(usize, f64)>)> = (0..n).map(|i| (i, Vec::new())).collect();
    let mut edge_count = 0usize;

    for cluster in 0..num_clusters {
        let start = cluster * cluster_size;
        let end = (start + cluster_size).min(n);
        append_cluster_edges(
            &mut adjacency,
            start,
            end,
            intra_degree,
            &mut rng,
            &mut edge_count,
        );
    }
    append_ring_bridges(
        &mut adjacency,
        num_clusters,
        cluster_size,
        n,
        &mut rng,
        &mut edge_count,
    );
    (AdjacencyGraph::from_adjacency(adjacency), edge_count)
}

fn append_cluster_edges(
    adjacency: &mut [(usize, Vec<(usize, f64)>)],
    start: usize,
    end: usize,
    intra_degree: usize,
    rng: &mut SplitMix64,
    edge_count: &mut usize,
) {
    if end <= start + 1 {
        return;
    }
    let members: Vec<usize> = (start..end).collect();
    for &source in &members {
        for _ in 0..intra_degree {
            let target = members[rng.below(members.len())];
            if target != source {
                adjacency[source].1.push((target, 1.0));
                *edge_count += 1;
            }
        }
    }
}

fn append_ring_bridges(
    adjacency: &mut [(usize, Vec<(usize, f64)>)],
    num_clusters: usize,
    cluster_size: usize,
    n: usize,
    rng: &mut SplitMix64,
    edge_count: &mut usize,
) {
    // Sparse ring bridges between adjacent clusters (2 edges each) so the
    // graph is one connected component, not `num_clusters` disjoint islands.
    for cluster in 0..num_clusters {
        let next_cluster = (cluster + 1) % num_clusters;
        let a_start = cluster * cluster_size;
        let a_end = (a_start + cluster_size).min(n);
        let b_start = next_cluster * cluster_size;
        let b_end = (b_start + cluster_size).min(n);
        if a_end <= a_start || b_end <= b_start {
            continue;
        }
        for _ in 0..2 {
            let source = a_start + rng.below(a_end - a_start);
            let target = b_start + rng.below(b_end - b_start);
            adjacency[source].1.push((target, 1.0));
            *edge_count += 1;
        }
    }
}

/// Resident set size in MB, read from `/proc/self/status` (Linux-only —
/// matches this whole homelab's build target; `None` off-Linux/if unreadable).
fn resident_memory_mb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.trim().trim_end_matches(" kB").trim().parse().ok()?;
            return Some(kb / 1024);
        }
    }
    None
}

fn run_bench(label: &str, n: usize, cluster_size: usize, intra_degree: usize) {
    let build_started = std::time::Instant::now();
    let (g, edge_count) = synthetic_clustered_graph(n, cluster_size, intra_degree, 42);
    let build_elapsed = build_started.elapsed();

    let cluster_started = std::time::Instant::now();
    let hierarchy = leiden_hierarchy(&g, &LeidenConfig::default());
    let cluster_elapsed = cluster_started.elapsed();

    let rss = resident_memory_mb();
    eprintln!(
        "[VIZ-1 bench] {label}: n={n} edges={edge_count} build={build_elapsed:?} \
         leiden_hierarchy={cluster_elapsed:?} levels={levels} top_level_clusters={top} \
         rss_mb={rss:?}",
        levels = hierarchy.levels.len(),
        top = hierarchy
            .levels
            .last()
            .map(|l| l.communities.len())
            .unwrap_or(0),
    );
    // Sanity: hierarchy must actually cover every node exactly once at
    // level 1 — a benchmark that silently degenerated (e.g. to all
    // singletons) would be a meaningless timing.
    if let Some(level1) = hierarchy.levels.first() {
        let covered: usize = level1.communities.iter().map(Vec::len).sum();
        assert_eq!(covered, n);
    }
}

/// The VIZ-1 cluster path (`algorithms::cluster_hierarchy` →
/// `ClusterHierarchyRefresh`) runs this kernel, and it had no wall-clock
/// bound at all. A TRUNCATED dendrogram is indistinguishable from a
/// converged one — coarser levels are simply absent — so the bound and the
/// flag matter more here than anywhere else in the family.
#[test]
fn leiden_hierarchy_wall_clock_budget_truncates_and_flags_it() {
    const N: usize = 20_000;
    let (graph, _edges) = synthetic_clustered_graph(N, 40, 8, 42);
    let budget = std::time::Duration::from_millis(50);

    let started = std::time::Instant::now();
    let truncated = leiden_hierarchy(
        &graph,
        &LeidenConfig {
            budget,
            ..Default::default()
        },
    );
    let elapsed = started.elapsed();

    assert!(
        truncated.deadline_hit,
        "a {budget:?} budget over {N} nodes must expire"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(3),
        "budget {budget:?} did not bound the kernel: elapsed={elapsed:?}"
    );
    // Whatever levels DID complete must still be a real, strictly nested
    // hierarchy — truncation drops coarser levels, it never corrupts the
    // ones already built.
    if let Some(level1) = truncated.levels.first() {
        let covered: usize = level1.communities.iter().map(Vec::len).sum();
        assert_eq!(covered, N);
    }
}

/// A budget that does not fire must leave the hierarchy byte-identical.
#[test]
fn leiden_hierarchy_budget_that_does_not_fire_changes_nothing() {
    const N: usize = 4_000;
    let (graph, _edges) = synthetic_clustered_graph(N, 40, 8, 42);

    let converged = leiden_hierarchy(&graph, &LeidenConfig::default());
    assert!(
        !converged.deadline_hit,
        "the default 15s budget must not fire on a {N}-node fixture"
    );
    let generous = leiden_hierarchy(
        &graph,
        &LeidenConfig {
            budget: std::time::Duration::from_secs(600),
            ..Default::default()
        },
    );
    assert!(!generous.deadline_hit);
    assert_eq!(converged.levels.len(), generous.levels.len());
    for (a, b) in converged.levels.iter().zip(&generous.levels) {
        assert_eq!(a.communities, b.communities);
        assert_eq!(a.parent, b.parent);
        assert_eq!(a.modularity, b.modularity);
    }
    assert_strict_nesting(&converged, &(0..N).collect::<Vec<_>>());
}

#[test]
#[ignore = "slow: run explicitly, see module doc"]
fn bench_hierarchy_synthetic_25k() {
    // Lower end of the live tenant graph's measured size (25k-57k nodes,
    // per the program charter's two disagreeing instruments).
    run_bench("25k", 25_000, 40, 8);
}

#[test]
#[ignore = "slow: run explicitly, see module doc"]
fn bench_hierarchy_synthetic_100k() {
    run_bench("100k", 100_000, 40, 8);
}

#[test]
#[ignore = "slow: run explicitly, see module doc"]
fn bench_hierarchy_synthetic_1m() {
    run_bench("1M", 1_000_000, 40, 8);
}
