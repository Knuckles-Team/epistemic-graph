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
//   1. `local_moving` (the quality-generic kernel in [`local_moving`] — see
//      that module's doc for why it replaced a direct reuse of
//      [`super::louvain::local_moving`]) finds a coarse partition `P` of the
//      current (possibly already-aggregated) graph.
//   2. `refine` re-derives a partition WITHIN each `P` community (candidates
//      restricted to same-`P`-community neighbours, singleton-seeded, same
//      quality-gain criterion) and then — the key correctness step —
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
// [`connectivity::verify_communities_connected`] (EH-285(c)) is the free,
// standalone check that this guarantee actually held — a no-op under a
// correct implementation, wired in below as a `debug_assert!` so it runs
// across every existing test fixture for free without costing a release
// build anything.
//
// Determinism: no RNG in the refinement phase itself (ascending index order,
// deterministic tie-break, matching this module's overall no-RNG contract);
// `LeidenConfig::seed` only affects the reused outer local-moving step's
// optional visit shuffle, exactly like [`super::louvain::LouvainConfig::seed`].

mod connectivity;
mod local_moving;
mod quality;
mod refine;
mod stability;

pub use connectivity::verify_communities_connected;
pub use quality::QualityFunction;
pub use stability::{name_communities, resolution_sweep, NamedCommunity, StableCommunity};

use super::graph::AdjacencyGraph;
use super::louvain::{
    aggregate, multilevel_partition, multilevel_start, run_community, LevelStep, MultilevelStart,
};
use local_moving::{generic_local_moving, LocalMovingParams};
use quality::node_weight_vector;
use refine::refine;
use std::cell::Cell;
use std::hash::Hash;
use std::time::{Duration, Instant};

/// Configuration for [`leiden`]. Mirrors [`super::louvain::LouvainConfig`]'s
/// shape so the two are drop-in comparable. CONCEPT:EG-KG.compute.leiden-community-detection
#[derive(Debug, Clone, Copy)]
pub struct LeidenConfig {
    /// Resolution γ scaling the active [`QualityFunction`]'s null model
    /// (higher ⇒ more, smaller communities). GDS default 1.0. Under
    /// [`QualityFunction::Cpm`] this is CPM's own γ, a different scale than
    /// modularity's — see that variant's doc.
    pub resolution: f64,
    /// The objective local-moving and refinement optimize (EH-283). Defaults
    /// to [`QualityFunction::Modularity`], the pre-EH-283 behaviour.
    pub quality: QualityFunction,
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
    /// Louvain's shared multilevel driver, so it inherited that module's
    /// missing wall-clock bound too; this closes it for the flat [`leiden`]
    /// run AND for [`leiden_hierarchy`] (the VIZ-1 cluster path).
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
            quality: QualityFunction::default(),
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
    /// Final modularity `Q` of the returned partition, ALWAYS computed via
    /// the standard modularity formula regardless of the active
    /// [`QualityFunction`] — informational and comparable across runs, even
    /// though [`QualityFunction::Cpm`] optimizes a different objective while
    /// building the partition this score describes.
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
            let (membership, hit) = leiden_partition(base_adj, resolution, config, deadline);
            expired.set(hit);
            membership
        },
        |communities, modularity| LeidenResult {
            communities,
            modularity,
            deadline_hit: expired.get(),
        },
    );
    debug_assert!(
        verify_communities_connected(graph, &result.communities).is_empty(),
        "leiden (EH-285(c)): returned a disconnected community — the refinement \
         guarantee has a regression"
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
    config: &LeidenConfig,
    deadline: Instant,
) -> (Vec<usize>, bool) {
    multilevel_partition(base_adj, config.max_levels, |current, node_to_super, m2| {
        run_leiden_level(current, node_to_super, resolution, config, m2, deadline)
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
    config: &LeidenConfig,
    m2: f64,
    deadline: Instant,
) -> (LevelStep, bool) {
    if Instant::now() >= deadline {
        return (LevelStep::Stop, true);
    }
    let (_p, refined, improved, deadline_hit) =
        leiden_level_step(current, node_to_super, resolution, config, m2, deadline);
    if !improved {
        return (LevelStep::Stop, deadline_hit);
    }
    let n_refined = fold_refinement(node_to_super, &refined);
    if deadline_hit || n_refined == current.len() {
        return (LevelStep::Stop, deadline_hit); // out of time, or refinement found no merges at all ⇒ stable
    }
    let next_current = aggregate(current, &refined, n_refined);
    if n_refined == 1 {
        return (LevelStep::Stop, deadline_hit);
    }
    (LevelStep::Continue { next_current }, deadline_hit)
}

/// One full Leiden level at the CURRENT aggregation level: build the active
/// [`QualityFunction`]'s per-node weight vector, run the outer (unconstrained)
/// local-moving pass, then — unless it found no move at all — the
/// connectivity-guaranteeing refinement pass. Shared by [`run_leiden_level`]
/// (flat [`leiden`], only the final fold matters) and
/// [`leiden_hierarchy_raw`] (keeps `p`/`refined` for the per-level dendrogram
/// history) — EH-283 needed the quality-function plumbing in exactly one
/// place, which is also what removed the pre-existing duplication between
/// those two call sites.
///
/// Returns `(outer_partition, refined_partition, improved, deadline_hit)`.
/// `improved = false` means the outer pass made no move at all — the level is
/// stable and callers must stop without aggregating (the returned `refined`
/// is then empty and must not be used).
fn leiden_level_step(
    current: &[Vec<(usize, f64)>],
    node_to_super: &[usize],
    resolution: f64,
    config: &LeidenConfig,
    m2: f64,
    deadline: Instant,
) -> (Vec<usize>, Vec<usize>, bool, bool) {
    let node_weight = node_weight_vector(config.quality, current, node_to_super);
    let normalizer = config.quality.normalizer(m2);
    let outer_params = LocalMovingParams {
        node_weight: &node_weight,
        resolution,
        normalizer,
        restrict: None,
        seed: config.seed,
        max_sweeps: config.max_sweeps,
    };
    let (p, improved, moving_expired) = generic_local_moving(current, &outer_params, deadline);
    if !improved {
        return (p, Vec::new(), false, moving_expired);
    }
    let (refined, refine_expired) =
        refine(current, &p, resolution, &node_weight, normalizer, deadline);
    (p, refined, true, moving_expired || refine_expired)
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
use super::louvain::modularity_of;
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
    let (raw, deadline_hit) = leiden_hierarchy_raw(&base_adj, resolution, config, deadline);

    let mut levels = Vec::with_capacity(raw.len());
    for (i, (snapshot, _refined_into_this_level)) in raw.iter().enumerate() {
        let communities = graph.label_partition(snapshot);
        debug_assert!(
            verify_communities_connected(graph, &communities).is_empty(),
            "leiden_hierarchy (EH-285(c)): level {i} returned a disconnected community — \
             the refinement guarantee has a regression"
        );
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
/// SAME local-moving/refine/aggregate loop [`leiden_partition`] runs (via the
/// shared [`leiden_level_step`]) — this is that loop with each level's
/// `(node_to_super snapshot, refined)` pair KEPT (pushed to `out`) instead of
/// discarded once folded into the next level.
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
        let (_p, refined, improved, level_hit) =
            leiden_level_step(&current, &node_to_super, resolution, config, m2, deadline);
        deadline_hit |= level_hit;
        if !improved {
            break;
        }
        let n_refined = fold_refinement(&mut node_to_super, &refined);
        out.push((node_to_super.clone(), refined.clone()));

        if deadline_hit || n_refined == current.len() {
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
mod hierarchy_tests;
#[cfg(test)]
mod tests;
