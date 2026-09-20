// CONCEPT:EG-KG.compute.louvain-community-detection — Community detection via Louvain modularity optimization
// (Neo4j GDS `gds.louvain` parity).

use super::graph::AdjacencyGraph;
use crate::SplitMix64;
use std::cell::Cell;
use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

/// How many nodes one local-moving sweep visits between wall-clock checks.
///
/// A sweep is `O(V + E)`, so on a large graph a SINGLE sweep can outlast the
/// whole budget — checking only *between* sweeps would therefore not be a bound
/// at all. Checking every `DEADLINE_CHECK_STRIDE` nodes keeps the
/// `Instant::now()` cost immeasurable next to the per-node neighbour-weight
/// `HashMap` work while capping overshoot at that many node moves.
pub(crate) const DEADLINE_CHECK_STRIDE: usize = 1024;

/// Configuration for [`louvain`]. CONCEPT:EG-KG.compute.louvain-community-detection
#[derive(Debug, Clone, Copy)]
pub struct LouvainConfig {
    /// Resolution γ scaling the modularity null model (higher ⇒ more, smaller
    /// communities). GDS default 1.0.
    pub resolution: f64,
    /// Optional RNG seed. `None` ⇒ nodes are visited in ascending index order
    /// (fully deterministic, no RNG). `Some(seed)` ⇒ a *seeded* deterministic
    /// Fisher–Yates shuffle of the visit order (still reproducible run-to-run).
    pub seed: Option<u64>,
    /// Cap on local-moving sweeps per level (guards against slow convergence).
    pub max_sweeps: usize,
    /// Cap on aggregation levels.
    pub max_levels: usize,
    /// Wall-clock budget for the WHOLE run (CONCEPT:EG-KG.compute.louvain-community-detection).
    ///
    /// `max_sweeps`/`max_levels` bound ITERATIONS, not TIME, and per-iteration
    /// cost scales with the graph — so on a request path they are not a bound.
    /// This is. On expiry the kernel stops and returns the BEST PARTITION SO FAR
    /// (still a valid partition of every node) with
    /// [`LouvainResult::deadline_hit`] set.
    ///
    /// Deliberately a plain `Duration`, **not** an `Option<Duration>`: an
    /// optional budget invites the next caller to leave it unset, which is
    /// exactly the hole this field closes. Every struct-literal construction of
    /// this config is a compile error until it names a budget on purpose.
    pub budget: Duration,
}

impl Default for LouvainConfig {
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

/// Result of a Louvain run. CONCEPT:EG-KG.compute.louvain-community-detection
#[derive(Debug, Clone)]
pub struct LouvainResult<N> {
    /// Communities: members sorted, communities ordered by smallest member.
    pub communities: Vec<Vec<N>>,
    /// Final modularity `Q` of the returned partition.
    pub modularity: f64,
    /// `true` when [`LouvainConfig::budget`] expired before the algorithm
    /// converged. The partition is still VALID (every node is assigned) but it
    /// is the best one found so far, NOT a converged one — a truncated result
    /// that looks complete is precisely what this flag exists to prevent.
    /// Callers that persist or publish the partition must surface this.
    pub deadline_hit: bool,
}

/// Louvain community detection over the undirected symmetrisation of the graph.
///
/// Two-phase, multi-level: (1) greedy local modularity-gain moving until no node
/// improves, (2) aggregate each community into a super-node and recurse, until no
/// further gain. Deterministic for a fixed `seed` (or fully order-deterministic
/// when `seed = None`); ties are resolved in favour of a node's current community
/// (no oscillation).
///
/// Complexity: near-linear per level, `O(L · (V + E))` in practice for `L`
/// levels. CONCEPT:EG-KG.compute.louvain-community-detection
pub fn louvain<N>(graph: &AdjacencyGraph<N>, config: &LouvainConfig) -> LouvainResult<N>
where
    N: Clone + Eq + Hash + Ord,
{
    // The deadline is anchored ONCE, at entry, so it bounds the whole call
    // (projection included) rather than restarting per level.
    let deadline = Instant::now() + config.budget;
    // `run_community` calls `partition` exactly once and then `build` exactly
    // once, so a `Cell` hoists the truncation flag out of the `FnOnce` without
    // widening `run_community`'s shared signature (Leiden calls it too).
    let expired = Cell::new(false);
    let result = run_community(
        graph,
        config.resolution,
        |base_adj, resolution| {
            let (membership, hit) =
                louvain_partition(base_adj, resolution, config.seed, config, deadline);
            expired.set(hit);
            membership
        },
        |communities, modularity| LouvainResult {
            communities,
            modularity,
            deadline_hit: expired.get(),
        },
    );
    if result.deadline_hit {
        // Non-silent by construction: every caller of this kernel — including
        // ones that discard the typed flag — leaves a record that the partition
        // it published was truncated, not converged.
        tracing::warn!(
            budget_ms = config.budget.as_millis() as u64,
            communities = result.communities.len(),
            "louvain: wall-clock budget expired; returning best partition so far (truncated)"
        );
    }
    result
}

pub(crate) fn run_community<N, R, F, B>(
    graph: &AdjacencyGraph<N>,
    configured_resolution: f64,
    partition: F,
    build: B,
) -> R
where
    N: Clone + Eq + Hash + Ord,
    F: FnOnce(&[Vec<(usize, f64)>], f64) -> Vec<usize>,
    B: FnOnce(Vec<Vec<N>>, f64) -> R,
{
    let n = graph.node_count();
    if n == 0 {
        return build(Vec::new(), 0.0);
    }
    let resolution = positive_resolution(configured_resolution);
    let base_adj = graph.undirected_weighted_adjacency();
    let membership = partition(&base_adj, resolution);
    let modularity = modularity_of(&base_adj, &membership, resolution);
    build(graph.label_partition(&membership), modularity)
}

fn positive_resolution(configured_resolution: f64) -> f64 {
    if configured_resolution > 0.0 {
        configured_resolution
    } else {
        1.0
    }
}

/// Core Louvain over a raw symmetric weighted adjacency. Returns the community of
/// each of the original `0..n` nodes (dense community ids), plus whether
/// `deadline` expired before the algorithm converged.
fn louvain_partition(
    base_adj: &[Vec<(usize, f64)>],
    resolution: f64,
    seed: Option<u64>,
    config: &LouvainConfig,
    deadline: Instant,
) -> (Vec<usize>, bool) {
    multilevel_partition(base_adj, config.max_levels, |current, node_to_super, m2| {
        run_louvain_level(
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

/// Run one Louvain level (local-moving, then — unless the level decides to
/// stop — aggregation) and fold its communities into `node_to_super`.
fn run_louvain_level(
    current: &[Vec<(usize, f64)>],
    node_to_super: &mut [usize],
    resolution: f64,
    seed: Option<u64>,
    config: &LouvainConfig,
    m2: f64,
    deadline: Instant,
) -> (LevelStep, bool) {
    // Checked here AND inside the sweep loop: one level on a large graph can
    // exceed the budget by itself, so a per-level check alone is not a bound.
    if Instant::now() >= deadline {
        return (LevelStep::Stop, true);
    }
    let (comm, improved, n_comms, level_expired) =
        local_moving(current, resolution, m2, seed, config.max_sweeps, deadline);
    if !improved {
        return (LevelStep::Stop, level_expired);
    }
    // Fold this level's communities into the original mapping. A truncated
    // level's `comm` is still a complete, densified partition of `current`,
    // so folding it keeps the BEST PARTITION SO FAR rather than discarding
    // the work already done.
    for slot in node_to_super.iter_mut() {
        *slot = comm[*slot];
    }
    if level_expired || n_comms == current.len() {
        return (LevelStep::Stop, level_expired); // out of time, or no coarsening possible
    }
    let next_current = aggregate(current, &comm, n_comms);
    if n_comms == 1 {
        return (LevelStep::Stop, level_expired);
    }
    (LevelStep::Continue { next_current }, level_expired)
}

/// What one multilevel (Louvain / Leiden) level decided: stop (out of time, no
/// improvement, or the partition is already stable) or continue with the
/// aggregated next-level adjacency.
pub(crate) enum LevelStep {
    Stop,
    Continue {
        next_current: Vec<Vec<(usize, f64)>>,
    },
}

/// The multilevel driver shared by [`louvain_partition`] and
/// [`super::leiden`]'s partition: run up to `max_levels` levels of `run_level`
/// (given the current adjacency, the cumulative original-node mapping to fold
/// into, and `2m`), then densify the final mapping. A graph with no edge
/// weight is returned as all-singletons. Returns `(membership, deadline_hit)`,
/// where `deadline_hit` is the OR of every level's reported expiry.
pub(crate) fn multilevel_partition(
    base_adj: &[Vec<(usize, f64)>],
    max_levels: usize,
    mut run_level: impl FnMut(&[Vec<(usize, f64)>], &mut [usize], f64) -> (LevelStep, bool),
) -> (Vec<usize>, bool) {
    let Some(MultilevelStart {
        m2,
        mut node_to_super,
        mut current,
    }) = multilevel_start(base_adj)
    else {
        return ((0..base_adj.len()).collect(), false); // no edges ⇒ every node isolated
    };
    let mut deadline_hit = false;
    for _level in 0..max_levels {
        let (step, hit) = run_level(&current, &mut node_to_super, m2);
        deadline_hit |= hit;
        match step {
            LevelStep::Stop => break,
            LevelStep::Continue { next_current } => current = next_current,
        }
    }
    (densify_first_appearance(&node_to_super), deadline_hit)
}

/// Starting state of a multilevel (Louvain / Leiden) run over a raw symmetric
/// weighted adjacency.
pub(crate) struct MultilevelStart {
    /// Total adjacency weight, `= 2m`.
    pub(crate) m2: f64,
    /// `node_to_super[o]` tracks which current-level super-node each ORIGINAL
    /// node maps to; updated after each level. Starts as the identity.
    pub(crate) node_to_super: Vec<usize>,
    /// The current level's (super-node) adjacency. Starts as `base_adj`.
    pub(crate) current: Vec<Vec<(usize, f64)>>,
}

/// The shared starting state of [`louvain_partition`] and [`super::leiden`]'s
/// multilevel loops, or `None` when the graph has no edge weight at all
/// (every node isolated, so there is nothing to coarsen).
pub(crate) fn multilevel_start(base_adj: &[Vec<(usize, f64)>]) -> Option<MultilevelStart> {
    let m2: f64 = base_adj
        .iter()
        .flat_map(|row| row.iter().map(|(_, w)| *w))
        .sum(); // = 2m
    if m2 <= 0.0 {
        return None;
    }
    Some(MultilevelStart {
        m2,
        node_to_super: (0..base_adj.len()).collect(),
        current: base_adj.to_vec(),
    })
}

/// Densify community ids into `0..k` in first-appearance order. `pub(crate)` —
/// shared with [`super::leiden`], which densifies its final membership the same way.
pub(crate) fn densify_first_appearance(labels: &[usize]) -> Vec<usize> {
    let mut relabel: HashMap<usize, usize> = HashMap::new();
    let mut membership = vec![0usize; labels.len()];
    for (o, &c) in labels.iter().enumerate() {
        let next = relabel.len();
        let dense = *relabel.entry(c).or_insert(next);
        membership[o] = dense;
    }
    membership
}

/// One level of local moving. Returns
/// `(community_of_node, improved, n_comms, deadline_hit)`.
///
/// Stops as soon as `deadline` has passed, returning the partition reached so
/// far (always complete — every node carries a community at every instant) with
/// `deadline_hit = true`.
///
/// `pub(crate)` (not `pub`) — reused as-is by [`super::leiden`]'s outer per-level
/// pass (identical unconstrained local-moving), which layers its own restricted
/// refinement phase on top rather than re-deriving this routine. Threading the
/// deadline HERE is what gives Leiden a wall-clock bound too.
pub(crate) fn local_moving(
    adj: &[Vec<(usize, f64)>],
    resolution: f64,
    m2: f64,
    seed: Option<u64>,
    max_sweeps: usize,
    deadline: Instant,
) -> (Vec<usize>, bool, usize, bool) {
    let n = adj.len();
    let degree: Vec<f64> = adj
        .iter()
        .map(|row| row.iter().map(|(_, w)| *w).sum())
        .collect();

    let mut comm: Vec<usize> = (0..n).collect();
    let mut sigma_tot: Vec<f64> = degree.clone();

    let order = visit_order(n, seed);
    let mut improved = false;
    let mut deadline_hit = false;

    'sweeps: for _ in 0..max_sweeps {
        let mut moved = false;
        for (visited, &i) in order.iter().enumerate() {
            // `visited == 0` makes this also the top-of-sweep check.
            if visited % DEADLINE_CHECK_STRIDE == 0 && Instant::now() >= deadline {
                deadline_hit = true;
                break 'sweeps;
            }
            if move_louvain_node(adj, degree[i], &mut comm, &mut sigma_tot, i, resolution, m2) {
                moved = true;
                improved = true;
            }
        }
        if !moved {
            break;
        }
    }

    // Densify.
    let mut relabel: HashMap<usize, usize> = HashMap::new();
    for slot in comm.iter_mut() {
        let next = relabel.len();
        *slot = *relabel.entry(*slot).or_insert(next);
    }
    (comm, improved, relabel.len(), deadline_hit)
}

fn move_louvain_node(
    adj: &[Vec<(usize, f64)>],
    degree: f64,
    comm: &mut [usize],
    sigma_tot: &mut [f64],
    node: usize,
    resolution: f64,
    m2: f64,
) -> bool {
    let current = comm[node];
    let weights = neighboring_communities(adj, comm, node);
    sigma_tot[current] -= degree;
    let best = best_louvain_community(&weights, current, sigma_tot, degree, resolution, m2);
    sigma_tot[best] += degree;
    comm[node] = best;
    best != current
}

fn neighboring_communities(
    adj: &[Vec<(usize, f64)>],
    comm: &[usize],
    node: usize,
) -> HashMap<usize, f64> {
    let mut weights = HashMap::new();
    for &(neighbor, weight) in &adj[node] {
        if neighbor != node {
            *weights.entry(comm[neighbor]).or_insert(0.0) += weight;
        }
    }
    weights
}

fn best_louvain_community(
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
    for (&candidate, &weight) in weights {
        if candidate == current {
            continue;
        }
        let gain = weight - resolution * sigma_tot[candidate] * degree / m2;
        if gain > best_gain + 1e-12 || (gain > best_gain - 1e-12 && candidate < best) {
            best_gain = gain;
            best = candidate;
        }
    }
    best
}

/// Aggregate communities into super-nodes; edge weights between communities sum.
/// `pub(crate)` — reused by [`super::leiden`] to aggregate by its refined partition.
pub(crate) fn aggregate(
    adj: &[Vec<(usize, f64)>],
    comm: &[usize],
    n_comms: usize,
) -> Vec<Vec<(usize, f64)>> {
    let mut maps: Vec<HashMap<usize, f64>> = vec![HashMap::new(); n_comms];
    for (i, row) in adj.iter().enumerate() {
        let ci = comm[i];
        for &(j, w) in row {
            let cj = comm[j];
            *maps[ci].entry(cj).or_insert(0.0) += w;
        }
    }
    maps.into_iter()
        .map(|m| {
            let mut v: Vec<(usize, f64)> = m.into_iter().collect();
            v.sort_unstable_by_key(|(i, _)| *i);
            v
        })
        .collect()
}

/// Modularity `Q = Σ_c [ in_c/2m − γ (Σtot_c/2m)² ]`. `pub(crate)` — reused by
/// [`super::leiden`] to report its own final partition's modularity with the
/// SAME formula, so the two algorithms' `Q` values are directly comparable.
pub(crate) fn modularity_of(
    adj: &[Vec<(usize, f64)>],
    membership: &[usize],
    resolution: f64,
) -> f64 {
    let m2: f64 = adj.iter().flat_map(|row| row.iter().map(|(_, w)| *w)).sum();
    if m2 <= 0.0 {
        return 0.0;
    }
    let k = membership.iter().copied().max().map(|x| x + 1).unwrap_or(0);
    let mut internal = vec![0.0f64; k];
    let mut tot = vec![0.0f64; k];
    for (i, row) in adj.iter().enumerate() {
        let ci = membership[i];
        for &(j, w) in row {
            tot[ci] += w; // accumulates degree over all entries
            if membership[j] == ci {
                internal[ci] += w;
            }
        }
    }
    let mut q = 0.0;
    for c in 0..k {
        let frac = tot[c] / m2;
        q += internal[c] / m2 - resolution * frac * frac;
    }
    q
}

/// Deterministic visit order. `None` ⇒ ascending; `Some(seed)` ⇒ seeded
/// Fisher–Yates using a splitmix64 stream (dependency-free, reproducible).
///
/// `pub(crate)` (not `fn`-private) — reused verbatim by
/// [`super::leiden::local_moving`]'s quality-generic kernel (EH-283), which
/// needs the SAME outer-pass visit order Louvain uses so the default
/// (modularity, unrestricted) case stays bit-for-bit identical to this
/// kernel's own pre-existing behaviour. Widening this one function's
/// visibility (a pure additive, non-semantic change) is the dupehound-clean
/// alternative to Leiden re-deriving its own byte-identical copy.
pub(crate) fn visit_order(n: usize, seed: Option<u64>) -> Vec<usize> {
    let mut order: Vec<usize> = (0..n).collect();
    if let Some(seed) = seed {
        let mut rng = SplitMix64::new(seed);
        for i in (1..n).rev() {
            let j = rng.below(i + 1);
            order.swap(i, j);
        }
    }
    order
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A synthetic clustered graph: dense-ish blocks of `cluster_size` nodes,
    /// each node drawing `intra_degree` random intra-block edges, blocks joined
    /// in a ring so the graph is one component. Deterministic (fixed seed).
    fn deadline_test_graph(
        n: usize,
        cluster_size: usize,
        intra_degree: usize,
    ) -> AdjacencyGraph<usize> {
        let mut rng = SplitMix64::new(7);
        let mut adjacency: Vec<(usize, Vec<(usize, f64)>)> =
            (0..n).map(|i| (i, Vec::new())).collect();
        for (i, row) in adjacency.iter_mut().enumerate() {
            let start = (i / cluster_size) * cluster_size;
            let end = (start + cluster_size).min(n);
            if end <= start + 1 {
                continue;
            }
            for _ in 0..intra_degree {
                let j = start + rng.below(end - start);
                if j != i {
                    row.1.push((j, 1.0));
                }
            }
        }
        let clusters = n.div_ceil(cluster_size);
        for cluster in 0..clusters {
            let a = cluster * cluster_size;
            let b = ((cluster + 1) % clusters) * cluster_size;
            if a < n && b < n && a != b {
                adjacency[a].1.push((b, 1.0));
            }
        }
        AdjacencyGraph::from_adjacency(adjacency)
    }

    /// Assert `communities` is a COMPLETE partition of `0..n`: every node
    /// exactly once, none twice. A truncated run must still satisfy this — the
    /// budget returns the best partition so far, never a partial one.
    fn assert_partition_covers(communities: &[Vec<usize>], n: usize) {
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

    /// The wall-clock bound. Commit `a14b9c28` deleted a duplicate hand-rolled
    /// Louvain kernel and, with it, `COMMUNITY_DETECTION_BUDGET` — the only
    /// wall-clock bound in the whole community-detection family. `max_sweeps`
    /// and `max_levels` bound ITERATIONS, and per-iteration cost scales with the
    /// graph, so they never bounded a request path. This pins that the KERNEL
    /// itself stops on TIME (a handler-side timeout would bound the response
    /// while the compute thread kept burning CPU), and that the truncation is
    /// reported rather than passed off as a converged partition.
    #[test]
    fn eg144_louvain_wall_clock_budget_truncates_large_graph_and_flags_it() {
        const N: usize = 60_000;
        let graph = deadline_test_graph(N, 40, 8);
        let budget = Duration::from_millis(50);

        let started = Instant::now();
        let truncated = louvain(
            &graph,
            &LouvainConfig {
                budget,
                ..Default::default()
            },
        );
        let elapsed = started.elapsed();

        assert!(
            truncated.deadline_hit,
            "a {budget:?} budget over {N} nodes must expire — an unflagged \
             result here means a truncated partition is being presented as \
             converged"
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "budget {budget:?} did not bound the kernel: elapsed={elapsed:?}"
        );
        assert_partition_covers(&truncated.communities, N);
    }

    /// The negative direction: determinism is a documented guarantee of this
    /// kernel, so a budget that never fires must not perturb anything.
    #[test]
    fn eg144_louvain_budget_that_does_not_fire_changes_nothing() {
        const N: usize = 20_000;
        let graph = deadline_test_graph(N, 40, 8);

        let converged = louvain(&graph, &LouvainConfig::default());
        assert!(
            !converged.deadline_hit,
            "the default 15s budget must not fire on a {N}-node fixture"
        );
        let generous = louvain(
            &graph,
            &LouvainConfig {
                budget: Duration::from_secs(600),
                ..Default::default()
            },
        );
        assert!(!generous.deadline_hit);
        assert_eq!(
            converged.communities, generous.communities,
            "a budget that does not fire must return the SAME partition"
        );
        assert_eq!(converged.modularity, generous.modularity);
        assert_partition_covers(&converged.communities, N);

        // And truncation can only ever leave the graph LESS merged than the
        // converged run — never more. (A 1ms budget expires during projection,
        // so this also pins the degenerate "no level completed" case.)
        let truncated = louvain(
            &graph,
            &LouvainConfig {
                budget: Duration::from_millis(1),
                ..Default::default()
            },
        );
        assert!(truncated.deadline_hit);
        assert_partition_covers(&truncated.communities, N);
        assert!(
            truncated.communities.len() >= converged.communities.len(),
            "truncated={} converged={}",
            truncated.communities.len(),
            converged.communities.len()
        );
    }

    /// Every small fixture above runs under the DEFAULT budget; pin explicitly
    /// that none of them is silently truncated.
    #[test]
    fn eg144_louvain_small_graphs_never_report_a_deadline_hit() {
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("b", "c", 1.0), ("a", "c", 1.0)]);
        assert!(!louvain(&g, &LouvainConfig::default()).deadline_hit);
        let empty: AdjacencyGraph<&str> = AdjacencyGraph::from_edges([]);
        assert!(!louvain(&empty, &LouvainConfig::default()).deadline_hit);
    }

    #[test]
    fn eg144_louvain_finds_two_communities_in_two_cliques() {
        // Two 4-cliques {a,b,c,d} and {w,x,y,z} joined by a single bridge d–w.
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
        edges.push(("d", "w", 1.0)); // weak bridge

        let g = AdjacencyGraph::from_edges(edges);
        let res = louvain(&g, &LouvainConfig::default());
        assert_eq!(
            res.communities.len(),
            2,
            "two cliques ⇒ two communities, got {:?}",
            res.communities
        );
        // Each clique lands wholly in one community.
        assert!(res
            .communities
            .iter()
            .any(|c| c == &vec!["a", "b", "c", "d"]));
        assert!(res
            .communities
            .iter()
            .any(|c| c == &vec!["w", "x", "y", "z"]));
        assert!(res.modularity > 0.3, "Q={}", res.modularity);
    }

    #[test]
    fn eg144_louvain_single_clique_is_one_community() {
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("b", "c", 1.0), ("a", "c", 1.0)]);
        let res = louvain(&g, &LouvainConfig::default());
        assert_eq!(res.communities.len(), 1);
        assert_eq!(res.communities[0], vec!["a", "b", "c"]);
    }

    #[test]
    fn eg144_louvain_is_deterministic_across_runs() {
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
        let a = louvain(&g, &LouvainConfig::default());
        let b = louvain(&g, &LouvainConfig::default());
        assert_eq!(a.communities, b.communities);
        // Seeded runs are also reproducible.
        let cfg = LouvainConfig {
            seed: Some(42),
            ..Default::default()
        };
        let c1 = louvain(&g, &cfg);
        let c2 = louvain(&g, &cfg);
        assert_eq!(c1.communities, c2.communities);
    }

    #[test]
    fn eg144_louvain_disconnected_nodes_separate() {
        // Two disjoint edges ⇒ two communities.
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("x", "y", 1.0)]);
        let res = louvain(&g, &LouvainConfig::default());
        assert_eq!(res.communities.len(), 2);
    }
}
