// CONCEPT:EG-KG.compute.leiden-community-detection — Community detection via the Leiden algorithm
// (Traag, Waltman & van Eck 2019), Neo4j GDS `gds.leiden` parity.
//
// Leiden refines Louvain's own two-phase (local-move, aggregate) loop with a
// THIRD phase — refinement — run between local-moving and aggregation at every
// level. The paper "From Louvain to Leiden: guaranteeing well-connected
// communities" (Traag, V.A., Waltman, L. & van Eck, N.J., Sci Rep 9, 5233, 2019)
// proves that Louvain's own local-moving phase can, because of the ORDER nodes
// happen to be processed in within one sweep, leave a returned community
// internally disconnected or "badly connected" — a node can move away from a
// community mid-sweep, severing the only link between two remaining subsets,
// without Louvain's algorithm ever re-checking or repairing that.
//
// This module's refinement phase makes "every returned community induces a
// CONNECTED subgraph" a **structural guarantee**, not a typical outcome:
//
//   1. `local_moving` (reused verbatim from [`super::louvain`] — the SAME
//      unconstrained, tested routine) finds a coarse partition `P` of the
//      current (possibly already-aggregated) graph.
//   2. `refine` re-derives a partition WITHIN each `P` community (candidates
//      restricted to same-`P`-community neighbours, singleton-seeded, same
//      modularity-gain criterion) and then — the key correctness step —
//      explicitly recomputes the CONNECTED COMPONENTS of each resulting group's
//      *induced subgraph* and uses those components as the final refined
//      groups. Any group that local-moving's own churn left disconnected is
//      therefore always split back into genuinely connected pieces: the
//      guarantee holds by construction, independent of how the constrained
//      local-moving got there.
//   3. The current graph is aggregated by the REFINED partition (finer than
//      `P`), so the next level's unconstrained local-moving is free to re-merge
//      (or not) the pieces refinement split apart, based on real connectivity.
//
// Determinism: no RNG in the refinement phase itself (ascending index order,
// deterministic tie-break, matching this module's overall no-RNG contract);
// `LeidenConfig::seed` only affects the reused outer `local_moving` step's
// optional visit shuffle, exactly like [`super::louvain::LouvainConfig::seed`].

use super::graph::AdjacencyGraph;
use super::louvain::{aggregate, local_moving, modularity_of};
use std::collections::HashMap;
use std::hash::Hash;

/// Configuration for [`leiden`]. Mirrors [`super::louvain::LouvainConfig`]'s
/// shape so the two are drop-in comparable. CONCEPT:EG-KG.compute.leiden-community-detection
#[derive(Debug, Clone, Copy)]
pub struct LeidenConfig {
    /// Resolution γ scaling the modularity null model (higher ⇒ more, smaller
    /// communities). GDS default 1.0.
    pub resolution: f64,
    /// Optional RNG seed for the OUTER (per-level) local-moving pass's visit
    /// order — see [`super::louvain::LouvainConfig::seed`]. The refinement phase
    /// itself is always order-deterministic (ascending index).
    pub seed: Option<u64>,
    /// Cap on local-moving sweeps per level (guards against slow convergence).
    pub max_sweeps: usize,
    /// Cap on aggregation levels.
    pub max_levels: usize,
}

impl Default for LeidenConfig {
    fn default() -> Self {
        Self {
            resolution: 1.0,
            seed: None,
            max_sweeps: 100,
            max_levels: 50,
        }
    }
}

/// Result of a Leiden run. CONCEPT:EG-KG.compute.leiden-community-detection
#[derive(Debug, Clone)]
pub struct LeidenResult<N> {
    /// Communities: members sorted, communities ordered by smallest member.
    /// Every community's induced subgraph is connected — see the module doc.
    pub communities: Vec<Vec<N>>,
    /// Final modularity `Q` of the returned partition (same formula as
    /// [`super::louvain::louvain`], so the two are directly comparable).
    pub modularity: f64,
}

/// Leiden community detection over the undirected symmetrisation of the graph.
///
/// Same multi-level shape as [`super::louvain::louvain`] (local-moving,
/// aggregate, repeat), with a connectivity-guaranteeing refinement phase
/// between them — see the module doc for the exact mechanism and citation.
///
/// Complexity: `O(L · (V + E))`, the same asymptotic class as Louvain (the
/// refinement phase re-scans each level's edges a bounded constant number of
/// extra times). CONCEPT:EG-KG.compute.leiden-community-detection
pub fn leiden<N>(graph: &AdjacencyGraph<N>, config: &LeidenConfig) -> LeidenResult<N>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    if n == 0 {
        return LeidenResult {
            communities: Vec::new(),
            modularity: 0.0,
        };
    }
    let resolution = if config.resolution > 0.0 {
        config.resolution
    } else {
        1.0
    };

    let base_adj = graph.undirected_weighted_adjacency();
    let membership = leiden_partition(&base_adj, resolution, config.seed, config);
    let modularity = modularity_of(&base_adj, &membership, resolution);

    LeidenResult {
        communities: graph.label_partition(&membership),
        modularity,
    }
}

/// Core Leiden over a raw symmetric weighted adjacency, mirroring
/// `louvain::louvain_partition`'s shape with a refinement step inserted between
/// local-moving and aggregation.
fn leiden_partition(
    base_adj: &[Vec<(usize, f64)>],
    resolution: f64,
    seed: Option<u64>,
    config: &LeidenConfig,
) -> Vec<usize> {
    let n = base_adj.len();
    let m2: f64 = base_adj
        .iter()
        .flat_map(|row| row.iter().map(|(_, w)| *w))
        .sum();
    if m2 <= 0.0 {
        return (0..n).collect(); // no edges ⇒ every node isolated
    }

    let mut node_to_super: Vec<usize> = (0..n).collect();
    let mut current: Vec<Vec<(usize, f64)>> = base_adj.to_vec();

    for _level in 0..config.max_levels {
        let (p, improved, _n_p) = local_moving(&current, resolution, m2, seed, config.max_sweeps);
        if !improved {
            break;
        }
        let refined = refine(&current, &p, resolution, m2);
        let n_refined = refined.iter().copied().max().map(|x| x + 1).unwrap_or(0);

        for slot in node_to_super.iter_mut() {
            *slot = refined[*slot];
        }
        if n_refined == current.len() {
            break; // refinement found no merges at all ⇒ stable
        }
        current = aggregate(&current, &refined, n_refined);
        if n_refined == 1 {
            break;
        }
    }

    // Densify community ids into 0..k in first-appearance order.
    let mut relabel: HashMap<usize, usize> = HashMap::new();
    let mut membership = vec![0usize; n];
    for (o, &c) in node_to_super.iter().enumerate() {
        let next = relabel.len();
        let dense = *relabel.entry(c).or_insert(next);
        membership[o] = dense;
    }
    membership
}

/// The refinement phase (CONCEPT:EG-KG.compute.leiden-community-detection). Starting from
/// singletons within each `p`-community, runs the SAME modularity-gain
/// local-moving restricted to same-`p`-community neighbours, then — the
/// correctness-critical step — recomputes the connected components of every
/// resulting group's induced subgraph and returns THOSE as the final refined
/// communities. See the module doc for why this makes connectivity a
/// structural guarantee rather than a typical outcome.
fn refine(adj: &[Vec<(usize, f64)>], p: &[usize], resolution: f64, m2: f64) -> Vec<usize> {
    let n = adj.len();
    let degree: Vec<f64> = adj
        .iter()
        .map(|row| row.iter().map(|(_, w)| *w).sum())
        .collect();

    // Step 1: constrained local-moving, singleton-seeded, restricted to
    // same-`p`-community candidates — otherwise identical to
    // `louvain::local_moving`'s single-node greedy reassignment.
    let mut comm: Vec<usize> = (0..n).collect();
    let mut sigma_tot: Vec<f64> = degree.clone();
    let order: Vec<usize> = (0..n).collect();

    for _ in 0..config_sweep_cap(n) {
        let mut moved = false;
        for &i in &order {
            let ci = comm[i];
            let ki = degree[i];

            let mut to_comm: HashMap<usize, f64> = HashMap::new();
            for &(j, w) in &adj[i] {
                if j != i && p[j] == p[i] {
                    *to_comm.entry(comm[j]).or_insert(0.0) += w;
                }
            }

            sigma_tot[ci] -= ki;
            let w_ci = *to_comm.get(&ci).unwrap_or(&0.0);
            let mut best_comm = ci;
            let mut best_gain = w_ci - resolution * sigma_tot[ci] * ki / m2;

            let mut keys: Vec<usize> = to_comm.keys().copied().collect();
            keys.sort_unstable();
            for c in keys {
                if c == ci {
                    continue;
                }
                let w_ic = to_comm[&c];
                let gain = w_ic - resolution * sigma_tot[c] * ki / m2;
                if gain > best_gain + 1e-12 || (gain > best_gain - 1e-12 && c < best_comm) {
                    best_gain = gain;
                    best_comm = c;
                }
            }

            sigma_tot[best_comm] += ki;
            comm[i] = best_comm;
            if best_comm != ci {
                moved = true;
            }
        }
        if !moved {
            break;
        }
    }

    // Step 2: split any group left disconnected by step 1 into its connected
    // components (union-find over ONLY edges joining two same-`comm` nodes).
    // This is what turns "typically connected" into "always connected".
    let mut uf_parent: Vec<usize> = (0..n).collect();
    for i in 0..n {
        for &(j, _w) in &adj[i] {
            if j != i && comm[i] == comm[j] {
                uf_union(&mut uf_parent, i, j);
            }
        }
    }
    let roots: Vec<usize> = (0..n).map(|i| uf_find(&mut uf_parent, i)).collect();

    // Densify (comm, connected-component-root) pairs into 0..k ids, in a fixed
    // ascending-`i` insertion order — deterministic regardless of hash
    // iteration, matching `louvain::local_moving`'s own densify idiom.
    let mut relabel: HashMap<(usize, usize), usize> = HashMap::new();
    let mut out = vec![0usize; n];
    for i in 0..n {
        let key = (comm[i], roots[i]);
        let next = relabel.len();
        out[i] = *relabel.entry(key).or_insert(next);
    }
    out
}

/// A bounded sweep cap for the refinement phase's own local-moving — proportional
/// to graph size like Louvain's `max_sweeps`, but derived locally so `refine`
/// does not need the full `LeidenConfig` threaded through it.
fn config_sweep_cap(n: usize) -> usize {
    n.clamp(1, 100)
}

fn uf_find(parent: &mut [usize], mut x: usize) -> usize {
    while parent[x] != x {
        parent[x] = parent[parent[x]]; // path halving
        x = parent[x];
    }
    x
}

fn uf_union(parent: &mut [usize], a: usize, b: usize) {
    let (ra, rb) = (uf_find(parent, a), uf_find(parent, b));
    if ra != rb {
        // Attach the larger root under the smaller for a deterministic result
        // independent of call order (both a<b and a>b callers converge).
        if ra < rb {
            parent[rb] = ra;
        } else {
            parent[ra] = rb;
        }
    }
}

// ── Hierarchical Leiden (VIZ-1, CONCEPT:EG-KG.compute.leiden-hierarchy) ─────
//
// [`leiden`] above collapses the multi-level local-moving/refine/aggregate loop
// down to its FINAL flat partition — every intermediate level's own partition is
// computed and then thrown away. [`leiden_hierarchy`] is the same algorithm, run
// unchanged, with those intermediate levels KEPT instead of discarded: level 1 is
// the first coarsening of the original nodes, level 2 the coarsening of level 1's
// communities, and so on up to the root. This is a dendrogram in the same sense
// `python-louvain`'s `generate_dendrogram` is one — Leiden's own multi-level
// structure already IS a cluster hierarchy; nothing here changes what the
// algorithm computes, only what is returned.
//
// Why this, not a second bottom-up agglomerative pass: re-running clustering
// once per zoom level (or clustering the clusters again as a separate step)
// would (a) cost O(levels) full re-clusterings instead of one, and (b) risk the
// coarser levels disagreeing with the finer ones (a node landing in a level-2
// community whose members aren't a strict merge of its level-1 community) —
// the aggregate-and-recurse loop makes strict nesting a structural guarantee:
// `current` at level `L+1` is built by summing level `L`'s own communities
// together (`aggregate`), so a level-`(L+1)` community can only ever be a union
// of WHOLE level-`L` communities, never a partial one.
use std::hash::Hash as StdHash;

/// One level of a [`LeidenHierarchy`] (level 1 = first coarsening of the
/// original nodes; higher levels are coarser). CONCEPT:EG-KG.compute.leiden-hierarchy
#[derive(Debug, Clone)]
pub struct HierarchyLevel<N> {
    /// This level's communities, in DENSE INDEX ORDER (`communities[c]` is
    /// community `c`'s members — original-node ids, sorted). Same "array-local
    /// index" convention the level above and below both use for `parent`.
    pub communities: Vec<Vec<N>>,
    /// `parent[c]` is the index into the NEXT (coarser) level's `communities`
    /// that community `c` merges into. `None` only at the top (root) level.
    pub parent: Vec<Option<usize>>,
    /// This level's modularity, computed against the ORIGINAL base graph (same
    /// formula as [`LeidenResult::modularity`]) using this level's cumulative
    /// membership — directly comparable across levels and to [`leiden`]'s own
    /// final-level `modularity`.
    pub modularity: f64,
}

/// Full multi-level Leiden hierarchy (CONCEPT:EG-KG.compute.leiden-hierarchy). `levels[0]` is
/// level 1 (finest coarsening); `levels.last()` is the root. An empty graph or a
/// graph whose local-moving never improves (no edges) yields an empty `levels` —
/// callers should treat the ORIGINAL graph's own nodes as the (implicit) level 0.
#[derive(Debug, Clone)]
pub struct LeidenHierarchy<N> {
    pub levels: Vec<HierarchyLevel<N>>,
}

/// Run hierarchical Leiden over `graph`, keeping every intermediate level
/// instead of only the final flat partition — see the module section doc above.
/// Same complexity class as [`leiden`]: `O(L · (V + E))` plus one `O(V)`
/// membership-snapshot clone per level (levels are `O(log V)` in practice, so
/// this adds `O(V log V)` memory/work on top, not a new asymptotic class).
pub fn leiden_hierarchy<N>(graph: &AdjacencyGraph<N>, config: &LeidenConfig) -> LeidenHierarchy<N>
where
    N: Clone + Eq + StdHash + Ord,
{
    let n = graph.node_count();
    if n == 0 {
        return LeidenHierarchy { levels: Vec::new() };
    }
    let resolution = if config.resolution > 0.0 {
        config.resolution
    } else {
        1.0
    };
    let base_adj = graph.undirected_weighted_adjacency();
    let raw = leiden_hierarchy_raw(&base_adj, resolution, config.seed, config);

    let mut levels = Vec::with_capacity(raw.len());
    for (i, (snapshot, _refined_into_this_level)) in raw.iter().enumerate() {
        let communities = graph.label_partition(snapshot);
        let modularity = modularity_of(&base_adj, snapshot, resolution);
        let is_top = i + 1 == raw.len();
        // The parent pointer FROM level `i+1` (this loop's level) TO level
        // `i+2` is the `refined` array computed at the NEXT iteration
        // (`raw[i + 1].1`): that `refined` has length == this level's own
        // community count (it was built by locally-moving THIS level's
        // communities) and values in this level's next-coarser community
        // space — exactly `parent`. `raw[i].1` (this same iteration's
        // `refined`) instead has length == the PREVIOUS level's community
        // count (or the original node count at `i == 0`), which is why using
        // it directly here previously produced a `parent` array the wrong
        // length entirely (caught by the `debug_assert_eq!` below at 25k+
        // scale, where the mismatch is no longer masked by every fixture
        // converging in a single level).
        let parent: Vec<Option<usize>> = if is_top {
            vec![None; communities.len()]
        } else {
            raw[i + 1].1.iter().map(|&p| Some(p)).collect()
        };
        debug_assert_eq!(parent.len(), communities.len());
        levels.push(HierarchyLevel {
            communities,
            parent,
            modularity,
        });
    }
    LeidenHierarchy { levels }
}

/// The raw per-level bookkeeping [`leiden_hierarchy`] needs, computed by the
/// SAME local-moving/refine/aggregate loop [`leiden_partition`] runs — this is
/// that loop with each level's `(node_to_super snapshot, refined)` pair KEPT
/// (pushed to `out`) instead of discarded once folded into the next level.
///
/// Returns one `(node_to_super, refined)` pair per level, level 1 first:
/// - `node_to_super[o]` = this level's (dense) community id for ORIGINAL node
///   `o` — the cumulative mapping, exactly what [`leiden_partition`] itself
///   maintains internally, just captured before it's overwritten by the next
///   level's fold.
/// - `refined[c]` = the index, into the NEXT level's community space, that
///   THIS level's community `c` merges into (the same array `aggregate` uses to
///   build the next level's supernode graph — reused verbatim, not
///   recomputed, so it is guaranteed consistent with the actual aggregation).
fn leiden_hierarchy_raw(
    base_adj: &[Vec<(usize, f64)>],
    resolution: f64,
    seed: Option<u64>,
    config: &LeidenConfig,
) -> Vec<(Vec<usize>, Vec<usize>)> {
    let n = base_adj.len();
    let m2: f64 = base_adj
        .iter()
        .flat_map(|row| row.iter().map(|(_, w)| *w))
        .sum();
    if m2 <= 0.0 {
        return Vec::new(); // no edges ⇒ no coarsening ⇒ no levels above the leaves
    }

    let mut node_to_super: Vec<usize> = (0..n).collect();
    let mut current: Vec<Vec<(usize, f64)>> = base_adj.to_vec();
    let mut out: Vec<(Vec<usize>, Vec<usize>)> = Vec::new();

    for _level in 0..config.max_levels {
        let (p, improved, _n_p) = local_moving(&current, resolution, m2, seed, config.max_sweeps);
        if !improved {
            break;
        }
        let refined = refine(&current, &p, resolution, m2);
        let n_refined = refined.iter().copied().max().map(|x| x + 1).unwrap_or(0);

        for slot in node_to_super.iter_mut() {
            *slot = refined[*slot];
        }
        out.push((node_to_super.clone(), refined.clone()));

        if n_refined == current.len() {
            break; // refinement found no merges at all ⇒ stable
        }
        current = aggregate(&current, &refined, n_refined);
        if n_refined == 1 {
            break;
        }
    }
    out
}

#[cfg(test)]
mod hierarchy_tests {
    use super::*;

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
                assert!(seen.insert(m.clone()), "node {m:?} appears twice at level 1");
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
        let g: AdjacencyGraph<&str> = AdjacencyGraph::from_adjacency([
            ("a", Vec::<(&str, f64)>::new()),
            ("b", Vec::new()),
        ]);
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

    /// Deterministic splitmix64 stream (same construction [`leiden`]'s own
    /// visit-order shuffle uses) — a dependency-free, seeded bit source for
    /// synthetic graph generation. Not cryptographic.
    struct SplitMix64(u64);
    impl SplitMix64 {
        fn new(seed: u64) -> Self {
            Self(seed)
        }
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        fn next_range(&mut self, bound: usize) -> usize {
            if bound == 0 {
                0
            } else {
                (self.next_u64() % bound as u64) as usize
            }
        }
    }

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
        let num_clusters = (n + cluster_size - 1) / cluster_size;
        let mut rng = SplitMix64::new(seed);
        let mut adjacency: Vec<(usize, Vec<(usize, f64)>)> =
            (0..n).map(|i| (i, Vec::new())).collect();
        let mut edge_count = 0usize;

        for cluster in 0..num_clusters {
            let start = cluster * cluster_size;
            let end = (start + cluster_size).min(n);
            if end <= start + 1 {
                continue;
            }
            let members: Vec<usize> = (start..end).collect();
            for &u in &members {
                for _ in 0..intra_degree {
                    let v = members[rng.next_range(members.len())];
                    if v != u {
                        adjacency[u].1.push((v, 1.0));
                        edge_count += 1;
                    }
                }
            }
        }
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
                let u = a_start + rng.next_range(a_end - a_start);
                let v = b_start + rng.next_range(b_end - b_start);
                adjacency[u].1.push((v, 1.0));
                edge_count += 1;
            }
        }
        (AdjacencyGraph::from_adjacency(adjacency), edge_count)
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
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn leiden_finds_two_communities_in_two_cliques_matching_louvain() {
        // Same fixture as louvain's own test: two 4-cliques joined by one bridge.
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
}
