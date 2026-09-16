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
use super::louvain::{
    aggregate, local_moving, modularity_of, multilevel_partition, multilevel_start, run_community,
    LevelStep, MultilevelStart, DEADLINE_CHECK_STRIDE,
};
use std::cell::Cell;
use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

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
    /// Wall-clock budget for the WHOLE run — the exact counterpart of
    /// [`super::louvain::LouvainConfig::budget`], and for the same reason:
    /// `max_sweeps`/`max_levels` bound ITERATIONS, not TIME. Leiden reuses
    /// Louvain's `local_moving` verbatim, so it inherited that module's missing
    /// wall-clock bound too; this closes it for the flat [`leiden`] run AND for
    /// [`leiden_hierarchy`] (the VIZ-1 cluster path).
    ///
    /// On expiry the kernel returns the best partition/hierarchy found so far
    /// and sets [`LeidenResult::deadline_hit`] / [`LeidenHierarchy::deadline_hit`].
    /// A plain `Duration`, never an `Option`, so no caller can forget it.
    pub budget: Duration,
}

impl Default for LeidenConfig {
    fn default() -> Self {
        Self {
            resolution: 1.0,
            seed: None,
            max_sweeps: 100,
            max_levels: 50,
            budget: Duration::from_secs(15),
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
    /// `true` when [`LeidenConfig::budget`] expired before convergence. The
    /// partition is still valid and still connectivity-guaranteed, but it is the
    /// best one found so far, NOT a converged one. Never present a `true` here
    /// as a completed result.
    pub deadline_hit: bool,
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
    let deadline = Instant::now() + config.budget;
    let expired = Cell::new(false);
    let result = run_community(
        graph,
        config.resolution,
        |base_adj, resolution| {
            let (membership, hit) =
                leiden_partition(base_adj, resolution, config.seed, config, deadline);
            expired.set(hit);
            membership
        },
        |communities, modularity| LeidenResult {
            communities,
            modularity,
            deadline_hit: expired.get(),
        },
    );
    if result.deadline_hit {
        tracing::warn!(
            budget_ms = config.budget.as_millis() as u64,
            communities = result.communities.len(),
            "leiden: wall-clock budget expired; returning best partition so far (truncated)"
        );
    }
    result
}

/// Core Leiden over a raw symmetric weighted adjacency, mirroring
/// `louvain::louvain_partition`'s shape with a refinement step inserted between
/// local-moving and aggregation. Returns `(membership, deadline_hit)`.
fn leiden_partition(
    base_adj: &[Vec<(usize, f64)>],
    resolution: f64,
    seed: Option<u64>,
    config: &LeidenConfig,
    deadline: Instant,
) -> (Vec<usize>, bool) {
    multilevel_partition(base_adj, config.max_levels, |current, node_to_super, m2| {
        run_leiden_level(
            current,
            node_to_super,
            resolution,
            seed,
            config,
            m2,
            deadline,
        )
    })
}

/// Run one Leiden level (local-moving, then refinement, then — unless the level
/// decides to stop — aggregation) and fold its refined membership into
/// `node_to_super`. The per-level step [`leiden_partition`] hands to
/// [`multilevel_partition`].
fn run_leiden_level(
    current: &[Vec<(usize, f64)>],
    node_to_super: &mut [usize],
    resolution: f64,
    seed: Option<u64>,
    config: &LeidenConfig,
    m2: f64,
    deadline: Instant,
) -> (LevelStep, bool) {
    if Instant::now() >= deadline {
        return (LevelStep::Stop, true);
    }
    let (p, improved, _n_p, moving_expired) =
        local_moving(current, resolution, m2, seed, config.max_sweeps, deadline);
    if !improved {
        return (LevelStep::Stop, moving_expired);
    }
    let (refined, refine_expired) = refine(current, &p, resolution, m2, deadline);
    let deadline_hit = moving_expired || refine_expired;
    let n_refined = fold_refinement(node_to_super, &refined);
    if moving_expired || refine_expired || n_refined == current.len() {
        return (LevelStep::Stop, deadline_hit); // out of time, or refinement found no merges at all ⇒ stable
    }
    let next_current = aggregate(current, &refined, n_refined);
    if n_refined == 1 {
        return (LevelStep::Stop, deadline_hit);
    }
    (LevelStep::Continue { next_current }, deadline_hit)
}

/// Fold one level's `refined` partition into the cumulative original-node
/// mapping (`node_to_super[o] = refined[node_to_super[o]]`) and return the
/// number of refined communities (`max + 1`, or `0` when empty).
fn fold_refinement(node_to_super: &mut [usize], refined: &[usize]) -> usize {
    for slot in node_to_super.iter_mut() {
        *slot = refined[*slot];
    }
    refined.iter().copied().max().map(|x| x + 1).unwrap_or(0)
}

/// The refinement phase (CONCEPT:EG-KG.compute.leiden-community-detection). Starting from
/// singletons within each `p`-community, runs the SAME modularity-gain
/// local-moving restricted to same-`p`-community neighbours, then — the
/// correctness-critical step — recomputes the connected components of every
/// resulting group's induced subgraph and returns THOSE as the final refined
/// communities. See the module doc for why this makes connectivity a
/// structural guarantee rather than a typical outcome.
/// Returns `(refined_partition, deadline_hit)`.
fn refine(
    adj: &[Vec<(usize, f64)>],
    p: &[usize],
    resolution: f64,
    m2: f64,
    deadline: Instant,
) -> (Vec<usize>, bool) {
    let (comm, deadline_hit) = refinement_local_moving(adj, p, resolution, m2, deadline);
    // The component split and densify are single `O(V + E)` passes with no
    // convergence loop, so they need no deadline of their own — and they are
    // what makes the returned partition connectivity-guaranteed, so they must
    // run even on a truncated `comm`.
    let roots = refinement_components(adj, &comm);
    (densify_refined_partition(&comm, &roots), deadline_hit)
}

/// Returns `(community_of_node, deadline_hit)`.
fn refinement_local_moving(
    adj: &[Vec<(usize, f64)>],
    parent_comm: &[usize],
    resolution: f64,
    m2: f64,
    deadline: Instant,
) -> (Vec<usize>, bool) {
    let n = adj.len();
    let degree: Vec<f64> = adj
        .iter()
        .map(|row| row.iter().map(|(_, weight)| *weight).sum())
        .collect();
    let mut comm: Vec<usize> = (0..n).collect();
    let mut sigma_tot = degree.clone();

    let mut deadline_hit = false;

    'sweeps: for _ in 0..config_sweep_cap(n) {
        let mut moved = false;
        for node in 0..n {
            // Same stride discipline as `louvain::local_moving`: the refinement
            // phase runs up to `min(n, 100)` `O(V + E)` sweeps of its own, so a
            // per-sweep-only check would leave the same hole open here.
            if node % DEADLINE_CHECK_STRIDE == 0 && Instant::now() >= deadline {
                deadline_hit = true;
                break 'sweeps;
            }
            let inputs = RefinementInputs {
                adj,
                parent_comm,
                degree: &degree,
            };
            if move_refinement_node(&inputs, &mut comm, &mut sigma_tot, node, resolution, m2) {
                moved = true;
            }
        }
        if !moved {
            break;
        }
    }
    (comm, deadline_hit)
}

/// The read-only topology one refinement sweep reads: the level's adjacency,
/// the parent partition that bounds every move, and per-node degrees. Grouped
/// so the per-node move keeps its argument count inside the repo's cap.
struct RefinementInputs<'a> {
    adj: &'a [Vec<(usize, f64)>],
    parent_comm: &'a [usize],
    degree: &'a [f64],
}

fn move_refinement_node(
    inputs: &RefinementInputs<'_>,
    comm: &mut [usize],
    sigma_tot: &mut [f64],
    node: usize,
    resolution: f64,
    m2: f64,
) -> bool {
    let current = comm[node];
    let weights = refinement_weights(inputs.adj, inputs.parent_comm, comm, node);
    let node_degree = inputs.degree[node];
    sigma_tot[current] -= node_degree;
    let best = best_refinement_community(&weights, current, sigma_tot, node_degree, resolution, m2);
    sigma_tot[best] += node_degree;
    comm[node] = best;
    best != current
}

fn refinement_weights(
    adj: &[Vec<(usize, f64)>],
    parent_comm: &[usize],
    comm: &[usize],
    node: usize,
) -> HashMap<usize, f64> {
    let mut weights = HashMap::new();
    for &(neighbor, weight) in &adj[node] {
        if neighbor != node && parent_comm[neighbor] == parent_comm[node] {
            *weights.entry(comm[neighbor]).or_insert(0.0) += weight;
        }
    }
    weights
}

fn best_refinement_community(
    weights: &HashMap<usize, f64>,
    current: usize,
    sigma_tot: &[f64],
    degree: f64,
    resolution: f64,
    m2: f64,
) -> usize {
    let own_weight = *weights.get(&current).unwrap_or(&0.0);
    let mut best = current;
    let mut best_gain = own_weight - resolution * sigma_tot[current] * degree / m2;
    let mut candidates: Vec<usize> = weights.keys().copied().collect();
    candidates.sort_unstable();
    for candidate in candidates {
        if candidate == current {
            continue;
        }
        let weight = weights[&candidate];
        let gain = weight - resolution * sigma_tot[candidate] * degree / m2;
        if gain > best_gain + 1e-12 || (gain > best_gain - 1e-12 && candidate < best) {
            best_gain = gain;
            best = candidate;
        }
    }
    best
}

fn refinement_components(adj: &[Vec<(usize, f64)>], comm: &[usize]) -> Vec<usize> {
    let n = adj.len();
    let mut parent: Vec<usize> = (0..n).collect();
    for node in 0..n {
        for &(neighbor, _weight) in &adj[node] {
            if neighbor != node && comm[node] == comm[neighbor] {
                uf_union(&mut parent, node, neighbor);
            }
        }
    }
    (0..n).map(|node| uf_find(&mut parent, node)).collect()
}

fn densify_refined_partition(comm: &[usize], roots: &[usize]) -> Vec<usize> {
    let mut relabel = HashMap::new();
    let mut out = Vec::with_capacity(comm.len());
    for (&community, &root) in comm.iter().zip(roots) {
        let key = (community, root);
        let next = relabel.len();
        out.push(*relabel.entry(key).or_insert(next));
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
    /// `true` when [`LeidenConfig::budget`] expired before the hierarchy
    /// converged: the levels present are real and strictly nested, but the
    /// hierarchy is TRUNCATED — coarser levels the algorithm would have built
    /// are missing. A truncated dendrogram is indistinguishable from a converged
    /// one without this flag, so callers that render or persist it must surface
    /// it rather than presenting a partial hierarchy as the whole graph.
    pub deadline_hit: bool,
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
        return LeidenHierarchy {
            levels: Vec::new(),
            deadline_hit: false,
        };
    }
    let deadline = Instant::now() + config.budget;
    let resolution = if config.resolution > 0.0 {
        config.resolution
    } else {
        1.0
    };
    let base_adj = graph.undirected_weighted_adjacency();
    let (raw, deadline_hit) =
        leiden_hierarchy_raw(&base_adj, resolution, config.seed, config, deadline);

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
    if deadline_hit {
        tracing::warn!(
            budget_ms = config.budget.as_millis() as u64,
            levels = levels.len(),
            "leiden_hierarchy: wall-clock budget expired; hierarchy is TRUNCATED (coarser levels missing)"
        );
    }
    LeidenHierarchy {
        levels,
        deadline_hit,
    }
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
///
/// Returns `(levels, deadline_hit)`.
#[allow(clippy::type_complexity)]
fn leiden_hierarchy_raw(
    base_adj: &[Vec<(usize, f64)>],
    resolution: f64,
    seed: Option<u64>,
    config: &LeidenConfig,
    deadline: Instant,
) -> (Vec<(Vec<usize>, Vec<usize>)>, bool) {
    let Some(MultilevelStart {
        m2,
        mut node_to_super,
        mut current,
    }) = multilevel_start(base_adj)
    else {
        // no edges ⇒ no coarsening ⇒ no levels above the leaves
        return (Vec::new(), false);
    };
    let mut out: Vec<(Vec<usize>, Vec<usize>)> = Vec::new();
    let mut deadline_hit = false;

    for _level in 0..config.max_levels {
        if Instant::now() >= deadline {
            deadline_hit = true;
            break;
        }
        let (p, improved, _n_p, moving_expired) =
            local_moving(&current, resolution, m2, seed, config.max_sweeps, deadline);
        deadline_hit |= moving_expired;
        if !improved {
            break;
        }
        let (refined, refine_expired) = refine(&current, &p, resolution, m2, deadline);
        deadline_hit |= refine_expired;
        let n_refined = fold_refinement(&mut node_to_super, &refined);
        out.push((node_to_super.clone(), refined.clone()));

        if moving_expired || refine_expired || n_refined == current.len() {
            break; // out of time, or refinement found no merges at all ⇒ stable
        }
        current = aggregate(&current, &refined, n_refined);
        if n_refined == 1 {
            break;
        }
    }
    (out, deadline_hit)
}

#[cfg(test)]
mod hierarchy_tests {
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
        let mut adjacency: Vec<(usize, Vec<(usize, f64)>)> =
            (0..n).map(|i| (i, Vec::new())).collect();
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

        assert!(
            truncated.deadline_hit,
            "a {budget:?} budget over {N} nodes must expire"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(3),
            "budget {budget:?} did not bound the kernel: elapsed={elapsed:?}"
        );
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
        let mut adjacency: Vec<(usize, Vec<(usize, f64)>)> =
            (0..n).map(|i| (i, Vec::new())).collect();
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
}
