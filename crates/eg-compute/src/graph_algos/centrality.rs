// CONCEPT:EG-KG.compute.graph-data-science-algorithms — Centrality: degree centrality + betweenness (Brandes'
// algorithm), eigenvector centrality, ArticleRank, and closeness/harmonic
// centrality. Neo4j GDS `gds.degree` / `gds.betweenness` / `gds.eigenvector` /
// `gds.articleRank` / `gds.closeness` / `gds.harmonic` parity.

use super::graph::AdjacencyGraph;
use super::shortest_path::dijkstra;
use std::collections::VecDeque;
use std::hash::Hash;

/// Which degree to score. CONCEPT:EG-KG.compute.graph-data-science-algorithms
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegreeKind {
    /// In-degree (number of incoming edges).
    In,
    /// Out-degree (number of outgoing edges).
    Out,
    /// In + out degree.
    Total,
}

/// Degree centrality: the chosen degree normalised by `(n − 1)` (so a node
/// adjacent to every other scores 1.0). Returns `(node, score)` in node order.
///
/// Complexity: `O(V + E)`. CONCEPT:EG-KG.compute.graph-data-science-algorithms
pub fn degree_centrality<N>(graph: &AdjacencyGraph<N>, kind: DegreeKind) -> Vec<(N, f64)>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let denom = if n > 1 { (n - 1) as f64 } else { 1.0 };
    let scores: Vec<f64> = (0..n)
        .map(|i| {
            let d = match kind {
                DegreeKind::In => graph.in_degree(i),
                DegreeKind::Out => graph.out_degree(i),
                DegreeKind::Total => graph.in_degree(i) + graph.out_degree(i),
            };
            d as f64 / denom
        })
        .collect();
    graph.label_scores(&scores)
}

/// Betweenness centrality via **Brandes' algorithm** (unweighted, BFS-based).
///
/// For each source, a BFS builds the shortest-path DAG (counting path
/// multiplicities `σ`), then dependencies are accumulated back-to-front. When
/// `directed` is false the graph is symmetrised and the raw scores are halved
/// (each unordered pair is otherwise counted twice).
///
/// Deterministic: neighbour iteration follows sorted index order. Returns
/// `(node, score)` in node order.
///
/// Complexity: `O(V · E)` time, `O(V + E)` space. CONCEPT:EG-KG.compute.graph-data-science-algorithms
pub fn betweenness_centrality<N>(graph: &AdjacencyGraph<N>, directed: bool) -> Vec<(N, f64)>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let mut bc = vec![0.0f64; n];

    // Neighbour list closure (directed out-edges, or undirected union).
    let neighbors = |v: usize| -> Vec<usize> {
        if directed {
            graph.out_edges(v).iter().map(|(t, _)| *t).collect()
        } else {
            graph.undirected_neighbors(v)
        }
    };

    for s in 0..n {
        let mut stack: Vec<usize> = Vec::new();
        let mut pred: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut sigma = vec![0.0f64; n];
        let mut dist = vec![-1i64; n];
        sigma[s] = 1.0;
        dist[s] = 0;

        let mut queue: VecDeque<usize> = VecDeque::new();
        queue.push_back(s);
        while let Some(v) = queue.pop_front() {
            stack.push(v);
            for w in neighbors(v) {
                if dist[w] < 0 {
                    dist[w] = dist[v] + 1;
                    queue.push_back(w);
                }
                if dist[w] == dist[v] + 1 {
                    sigma[w] += sigma[v];
                    pred[w].push(v);
                }
            }
        }

        let mut delta = vec![0.0f64; n];
        while let Some(w) = stack.pop() {
            for &v in &pred[w] {
                delta[v] += (sigma[v] / sigma[w]) * (1.0 + delta[w]);
            }
            if w != s {
                bc[w] += delta[w];
            }
        }
    }

    if !directed {
        for b in bc.iter_mut() {
            *b /= 2.0;
        }
    }
    graph.label_scores(&bc)
}

/// Configuration for [`eigenvector_centrality`]. CONCEPT:EG-KG.compute.eigenvector-centrality
#[derive(Debug, Clone, Copy)]
pub struct EigenvectorConfig {
    /// L2-norm convergence tolerance: stop when `‖x_new − x_old‖₂ ≤ tol`.
    pub tolerance: f64,
    /// Hard iteration cap.
    pub max_iterations: usize,
}

impl Default for EigenvectorConfig {
    fn default() -> Self {
        Self {
            tolerance: 1e-7,
            max_iterations: 100,
        }
    }
}

/// Result of an [`eigenvector_centrality`] run. CONCEPT:EG-KG.compute.eigenvector-centrality
#[derive(Debug, Clone)]
pub struct EigenvectorResult<N> {
    /// `(node, score)` pairs in node order; L2-normalised (`‖scores‖₂ ≈ 1`
    /// whenever the iteration doesn't degenerate to all-zero — see below).
    pub scores: Vec<(N, f64)>,
    /// Iterations actually performed.
    pub iterations: usize,
    /// Whether the L2 tolerance was reached before the cap.
    pub converged: bool,
}

/// Eigenvector centrality via power iteration: a node's score is proportional
/// to the sum of its IN-neighbours' scores (`x_new = Aᵀx`, weighted), the iterate
/// re-normalised to unit L2 norm every step so it neither blows up nor decays.
/// This converges to the dominant eigenvector of `Aᵀ` when the graph has a
/// unique dominant eigenvalue.
///
/// **Two honest, documented degenerate cases** (shared with any power-iteration
/// eigenvector method, GDS's own implementation included):
///
/// - A pure DAG/tree (no cycle at all) has an entirely NILPOTENT adjacency —
///   its only eigenvalue is `0` — so the iterate provably decays to the
///   all-zero vector within a few steps. `converged` still reports `true`
///   (the zero vector IS the fixed point reached).
/// - A graph with a "multiplicity of dominant eigenvalues" (e.g. a bipartite
///   or periodic structure) can make the iterate OSCILLATE indefinitely
///   instead of settling; `converged` honestly reports `false` once
///   `max_iterations` is hit rather than claiming a spurious fixed point.
///
/// (This is exactly why [`super::pagerank::pagerank`] adds a damping/teleport
/// term — it does not have either failure mode. Plain eigenvector centrality,
/// by design, has neither.)
///
/// Complexity: `O(k·(V+E))` for `k` iterations — same class as
/// [`super::pagerank::pagerank`]. CONCEPT:EG-KG.compute.eigenvector-centrality
pub fn eigenvector_centrality<N>(
    graph: &AdjacencyGraph<N>,
    config: &EigenvectorConfig,
) -> EigenvectorResult<N>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    if n == 0 {
        return EigenvectorResult {
            scores: Vec::new(),
            iterations: 0,
            converged: true,
        };
    }

    let init = 1.0 / (n as f64).sqrt();
    let mut x = vec![init; n];
    let mut next = vec![0.0f64; n];
    let mut iterations = 0;
    let mut converged = false;

    while iterations < config.max_iterations {
        iterations += 1;
        for slot in next.iter_mut() {
            *slot = 0.0;
        }
        for u in 0..n {
            for &(v, w) in graph.out_edges(u) {
                next[v] += w * x[u];
            }
        }
        let norm = next.iter().map(|v| v * v).sum::<f64>().sqrt();
        if norm > 0.0 {
            for slot in next.iter_mut() {
                *slot /= norm;
            }
        }
        let delta = x
            .iter()
            .zip(next.iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f64>()
            .sqrt();
        std::mem::swap(&mut x, &mut next);
        if delta <= config.tolerance {
            converged = true;
            break;
        }
    }

    EigenvectorResult {
        scores: graph.label_scores(&x),
        iterations,
        converged,
    }
}

/// Configuration for [`article_rank`]. Same shape/defaults as
/// [`super::pagerank::PageRankConfig`] since ArticleRank is a PageRank variant.
/// CONCEPT:EG-KG.compute.article-rank
#[derive(Debug, Clone, Copy)]
pub struct ArticleRankConfig {
    /// Damping factor `d`.
    pub damping: f64,
    /// L1 convergence tolerance.
    pub tolerance: f64,
    /// Hard iteration cap.
    pub max_iterations: usize,
}

impl Default for ArticleRankConfig {
    fn default() -> Self {
        Self {
            damping: 0.85,
            tolerance: 1e-7,
            max_iterations: 100,
        }
    }
}

/// Result of an [`article_rank`] run. CONCEPT:EG-KG.compute.article-rank
#[derive(Debug, Clone)]
pub struct ArticleRankResult<N> {
    /// `(node, rank)` pairs in node order.
    pub scores: Vec<(N, f64)>,
    pub iterations: usize,
    pub converged: bool,
}

/// **ArticleRank** — a PageRank variant (from the citation-ranking literature)
/// that divides a node `u`'s contribution to each out-neighbour by
/// `(u's own weighted out-degree + the graph's AVERAGE weighted out-degree)`
/// rather than by `u`'s out-degree alone. This discounts how much a
/// LOW-out-degree source can concentrate onto few targets — plain PageRank's
/// well-documented bias toward over-inflating the few things a sparse node
/// points to. Dangling-node handling (a node with zero out-weight spills its
/// rank uniformly, exactly as in [`super::pagerank::pagerank`]) is unchanged.
///
/// **Honest scope:** because the denominator is `outDegree + avgOutDegree`
/// rather than just `outDegree`, an active (non-dangling) node's rank is only
/// PARTIALLY redistributed each step (`outDegree/(outDegree+avgOutDegree)` of
/// it) — unlike PageRank, **ArticleRank's scores do not sum to 1** in general;
/// this is a documented, expected property of the algorithm (the discount IS
/// the point), not a bug. On a graph where every node has the SAME weighted
/// out-degree, the denominator becomes the same constant everywhere and the
/// scores are still all EQUAL at the fixed point (the ranking, if not the
/// absolute scale, degenerates to PageRank's own symmetric-graph behaviour).
///
/// Complexity: `O(k·(V+E))`. CONCEPT:EG-KG.compute.article-rank
pub fn article_rank<N>(
    graph: &AdjacencyGraph<N>,
    config: &ArticleRankConfig,
) -> ArticleRankResult<N>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    if n == 0 {
        return ArticleRankResult {
            scores: Vec::new(),
            iterations: 0,
            converged: true,
        };
    }

    let d = config.damping;
    let base = (1.0 - d) / n as f64;
    let inv_n = 1.0 / n as f64;

    let out_weight: Vec<f64> = (0..n).map(|i| graph.weighted_out_degree(i)).collect();
    let avg_out_weight = out_weight.iter().sum::<f64>() / n as f64;

    let mut rank = vec![inv_n; n];
    let mut next = vec![0.0f64; n];
    let mut iterations = 0;
    let mut converged = false;

    while iterations < config.max_iterations {
        iterations += 1;

        let dangling: f64 = (0..n)
            .filter(|&i| out_weight[i] <= 0.0)
            .map(|i| rank[i])
            .sum();
        let dangling_share = d * dangling * inv_n;

        for slot in next.iter_mut() {
            *slot = base + dangling_share;
        }
        for u in 0..n {
            let ow = out_weight[u];
            if ow <= 0.0 {
                continue;
            }
            let denom = ow + avg_out_weight;
            let ru = rank[u];
            for &(v, w) in graph.out_edges(u) {
                next[v] += d * ru * (w / denom);
            }
        }

        let delta: f64 = (0..n).map(|i| (next[i] - rank[i]).abs()).sum();
        std::mem::swap(&mut rank, &mut next);

        if delta <= config.tolerance {
            converged = true;
            break;
        }
    }

    ArticleRankResult {
        scores: graph.label_scores(&rank),
        iterations,
        converged,
    }
}

/// Configuration for [`closeness_centrality`]. CONCEPT:EG-KG.compute.closeness-centrality
#[derive(Debug, Clone, Copy, Default)]
pub struct ClosenessConfig {
    /// Apply the **Wasserman–Faust** "improved" correction (mirrors legacy
    /// GDS's `useWassermanFaust` flag): multiplies the classic Freeman
    /// closeness by an extra `reachable(v)/(N−1)` factor, so a node isolated
    /// in a small component doesn't score artificially high just because the
    /// FEW other nodes it can reach happen to be nearby. Default `false`
    /// (classic Freeman formula).
    pub improved: bool,
}

/// **Closeness centrality** (Freeman 1979): how close a node is, on average,
/// to every other REACHABLE node via directed out-paths —
/// `C(v) = reachable(v) / Σ_{u reachable} dist(v,u)`. An isolated node (no
/// reachable others) scores `0.0`. With [`ClosenessConfig::improved`], the
/// classic score is further scaled by `reachable(v)/(N−1)` (Wasserman–Faust),
/// penalising nodes that only reach a small fraction of the whole graph —
/// see the module tests for a worked before/after example.
///
/// Computed via one [`dijkstra`] run per node — `dijkstra`'s own distances are
/// what `Σ dist` and `reachable` are read from directly, so this is the same
/// "cross-check against an existing algorithm" the shortest-path kernel
/// already covers.
///
/// Complexity: `O(V·(V+E)logV)` — the same class as
/// [`super::shortest_path::all_pairs_shortest_paths`].
/// CONCEPT:EG-KG.compute.closeness-centrality
pub fn closeness_centrality<N>(graph: &AdjacencyGraph<N>, config: &ClosenessConfig) -> Vec<(N, f64)>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let denom_n = if n > 1 { (n - 1) as f64 } else { 1.0 };
    (0..n)
        .map(|v| {
            let dists = dijkstra(graph, v).distances(); // includes v itself at 0
            let reachable_others = dists.len().saturating_sub(1);
            let sum_dist: f64 = dists.iter().map(|(_, d)| *d).sum();
            let score = if reachable_others == 0 || sum_dist <= 0.0 {
                0.0
            } else {
                let classic = reachable_others as f64 / sum_dist;
                if config.improved {
                    classic * (reachable_others as f64 / denom_n)
                } else {
                    classic
                }
            };
            (graph.node_at(v).clone(), score)
        })
        .collect()
}

/// **Harmonic centrality** (Marchiori & Latora 2000):
/// `H(v) = (1/(N−1)) · Σ_{u≠v reachable} 1/dist(v,u)` — like closeness but
/// summing RECIPROCAL distances, so an unreachable node simply contributes
/// `0` (no `1/∞` blow-up) instead of being excluded from the sum/count —
/// handling disconnected graphs gracefully with no special-case correction,
/// unlike classic closeness (which needs [`ClosenessConfig::improved`] for the
/// same reason).
///
/// Complexity: `O(V·(V+E)logV)`, same as [`closeness_centrality`].
/// CONCEPT:EG-KG.compute.harmonic-centrality
pub fn harmonic_centrality<N>(graph: &AdjacencyGraph<N>) -> Vec<(N, f64)>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let denom_n = if n > 1 { (n - 1) as f64 } else { 1.0 };
    (0..n)
        .map(|v| {
            let sum_recip: f64 = dijkstra(graph, v)
                .distances()
                .into_iter()
                .filter(|(_, d)| *d > 0.0) // exclude v itself (distance 0)
                .map(|(_, d)| 1.0 / d)
                .sum();
            (graph.node_at(v).clone(), sum_recip / denom_n)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg144_degree_centrality_normalised() {
        // Undirected-style star: center 'c' out to 3 leaves.
        let g = AdjacencyGraph::from_edges([("c", "l1", 1.0), ("c", "l2", 1.0), ("c", "l3", 1.0)]);
        let out = degree_centrality(&g, DegreeKind::Out);
        let m: std::collections::HashMap<&str, f64> = out.iter().map(|(k, v)| (*k, *v)).collect();
        // n=4 ⇒ denom 3; center has out-degree 3 ⇒ 1.0.
        assert!((m["c"] - 1.0).abs() < 1e-9);
        assert!((m["l1"] - 0.0).abs() < 1e-9);

        let inc = degree_centrality(&g, DegreeKind::In);
        let mi: std::collections::HashMap<&str, f64> = inc.iter().map(|(k, v)| (*k, *v)).collect();
        assert!((mi["l1"] - (1.0 / 3.0)).abs() < 1e-9);
    }

    #[test]
    fn eg144_betweenness_star_center_is_high_leaves_zero() {
        // Undirected star: center on every shortest path between leaf pairs.
        let g =
            AdjacencyGraph::from_unweighted_edges([("c", "a"), ("c", "b"), ("c", "d"), ("c", "e")]);
        let bc = betweenness_centrality(&g, false);
        let m: std::collections::HashMap<&str, f64> = bc.iter().map(|(k, v)| (*k, *v)).collect();
        // 4 leaves ⇒ C(4,2)=6 unordered pairs all routed through center.
        assert!((m["c"] - 6.0).abs() < 1e-9, "center bc={}", m["c"]);
        for leaf in ["a", "b", "d", "e"] {
            assert!((m[leaf] - 0.0).abs() < 1e-9);
        }
    }

    #[test]
    fn eg144_betweenness_path_middle_is_highest() {
        // Undirected path a-b-c-d-e. Middle c should score highest.
        let g =
            AdjacencyGraph::from_unweighted_edges([("a", "b"), ("b", "c"), ("c", "d"), ("d", "e")]);
        let bc = betweenness_centrality(&g, false);
        let m: std::collections::HashMap<&str, f64> = bc.iter().map(|(k, v)| (*k, *v)).collect();
        // Analytic betweenness of an internal node at position i (1-based) in a
        // 5-path: b=3, c=4, d=3, endpoints 0.
        assert!((m["c"] - 4.0).abs() < 1e-9, "c={}", m["c"]);
        assert!((m["b"] - 3.0).abs() < 1e-9, "b={}", m["b"]);
        assert!((m["a"] - 0.0).abs() < 1e-9);
        assert!(m["c"] > m["b"]);
    }

    // ── eigenvector centrality ────────────────────────────────────────────

    #[test]
    fn eg144_eigenvector_symmetric_cycle_is_uniform_and_immediate() {
        // a->b->c->a: every node has exactly one in-edge of equal weight, so
        // the uniform starting vector IS already the fixed point.
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("b", "c", 1.0), ("c", "a", 1.0)]);
        let res = eigenvector_centrality(&g, &EigenvectorConfig::default());
        assert!(res.converged);
        let vals: Vec<f64> = res.scores.iter().map(|(_, v)| *v).collect();
        for w in vals.windows(2) {
            assert!((w[0] - w[1]).abs() < 1e-9, "{:?}", vals);
        }
        assert!(vals[0] > 0.0);
    }

    #[test]
    fn eg144_eigenvector_pure_dag_degenerates_to_zero() {
        // A star with all edges pointing AWAY from the center (a tree/DAG) has
        // a nilpotent adjacency matrix (no cycle anywhere) ⇒ the power
        // iteration provably decays to all-zero — a known, honest limitation
        // of plain eigenvector centrality (unlike damped PageRank).
        let g = AdjacencyGraph::from_edges([
            ("center", "l1", 1.0),
            ("center", "l2", 1.0),
            ("center", "l3", 1.0),
        ]);
        let res = eigenvector_centrality(&g, &EigenvectorConfig::default());
        assert!(res.converged);
        for (_, v) in res.scores {
            assert!(v.abs() < 1e-9, "expected decay to zero, got {v}");
        }
    }

    #[test]
    fn eg144_eigenvector_oscillating_structure_honestly_reports_not_converged() {
        // a<->c is a 2-cycle-like periodic pair (a->c->a) that a THIRD node b
        // only feeds INTO (b->c) with no return edge. b never receives any
        // inflow (deterministically 0 forever); a/c oscillate every step
        // instead of settling, so `converged` must honestly be false.
        let g = AdjacencyGraph::from_edges([("a", "c", 1.0), ("b", "c", 1.0), ("c", "a", 1.0)]);
        let cfg = EigenvectorConfig {
            tolerance: 1e-9,
            max_iterations: 50,
        };
        let res = eigenvector_centrality(&g, &cfg);
        assert!(!res.converged, "a periodic 2-cycle should not settle");
        assert_eq!(res.iterations, 50);
        let m: std::collections::HashMap<&str, f64> =
            res.scores.iter().map(|(k, v)| (*k, *v)).collect();
        assert!((m["b"] - 0.0).abs() < 1e-12, "b has no in-edges, ever");
    }

    #[test]
    fn eg144_eigenvector_empty_graph() {
        let g: AdjacencyGraph<&str> =
            AdjacencyGraph::from_adjacency(Vec::<(&str, Vec<(&str, f64)>)>::new());
        let res = eigenvector_centrality(&g, &EigenvectorConfig::default());
        assert!(res.scores.is_empty());
        assert!(res.converged);
    }

    // ── ArticleRank ───────────────────────────────────────────────────────

    #[test]
    fn eg144_article_rank_symmetric_ring_stays_uniform() {
        // Same fixture as pagerank's own "symmetric ring is uniform" test:
        // ArticleRank's fixed point is still uniform on a fully symmetric
        // graph (though, unlike PageRank, NOT necessarily summing to 1 — see
        // the function doc's honest-scope note).
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("b", "c", 1.0), ("c", "a", 1.0)]);
        let res = article_rank(&g, &ArticleRankConfig::default());
        assert!(res.converged);
        let vals: Vec<f64> = res.scores.iter().map(|(_, v)| *v).collect();
        for w in vals.windows(2) {
            assert!((w[0] - w[1]).abs() < 1e-6, "{:?}", vals);
        }
    }

    #[test]
    fn eg144_article_rank_ranks_known_four_node_graph_like_pagerank() {
        // Same fixture + qualitative ordering as pagerank's own test: c is the
        // strongest sink (3 inbound sources incl. d), d is source-only (no
        // inbound at all) ⇒ lowest.
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("a", "c", 1.0),
            ("b", "c", 1.0),
            ("c", "a", 1.0),
            ("d", "c", 1.0),
        ]);
        let res = article_rank(&g, &ArticleRankConfig::default());
        let m: std::collections::HashMap<&str, f64> =
            res.scores.iter().map(|(k, v)| (*k, *v)).collect();
        assert!(m["c"] > m["a"], "{m:?}");
        assert!(m["a"] > m["b"], "{m:?}");
        assert!(m["d"] < m["b"], "{m:?}");
        assert!(res.converged);
    }

    #[test]
    fn eg144_article_rank_dangling_sink_outranks_its_only_source() {
        // a->b, b dangling: b receives a's whole contribution PLUS its own
        // dangling self-spill, while a receives ONLY the dangling spillover
        // (no incoming edges at all) ⇒ b must outrank a. ArticleRank does NOT
        // conserve total mass to 1 (documented, unlike PageRank) so we assert
        // the ranking, not a sum.
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0)]);
        let res = article_rank(&g, &ArticleRankConfig::default());
        let m: std::collections::HashMap<&str, f64> =
            res.scores.iter().map(|(k, v)| (*k, *v)).collect();
        assert!(m["b"] > m["a"], "{m:?}");
        assert!(m["a"] > 0.0 && m["a"].is_finite());
        assert!(m["b"] > 0.0 && m["b"].is_finite());
        assert!(res.converged);
    }

    #[test]
    fn eg144_article_rank_empty_graph() {
        let g: AdjacencyGraph<&str> =
            AdjacencyGraph::from_adjacency(Vec::<(&str, Vec<(&str, f64)>)>::new());
        let res = article_rank(&g, &ArticleRankConfig::default());
        assert!(res.scores.is_empty());
        assert!(res.converged);
    }

    // ── closeness / harmonic centrality ────────────────────────────────────

    #[test]
    fn eg144_closeness_cross_checked_against_dijkstra_on_a_path() {
        // Bidirectional path a<->b<->c<->d (so directed dijkstra behaves like
        // an undirected path). Cross-check: independently recompute
        // reachable/sum-of-distances from the SAME `dijkstra` kernel and
        // assert it matches `closeness_centrality`'s output exactly.
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("b", "a", 1.0),
            ("b", "c", 1.0),
            ("c", "b", 1.0),
            ("c", "d", 1.0),
            ("d", "c", 1.0),
        ]);
        let closeness = closeness_centrality(&g, &ClosenessConfig::default());
        let m: std::collections::HashMap<&str, f64> = closeness.into_iter().collect();

        for &(id, expected) in &[("a", 3.0 / 6.0), ("b", 3.0 / 4.0)] {
            let idx = g.index_of(&id).unwrap();
            let dists = dijkstra(&g, idx).distances();
            let reachable = dists.len() - 1;
            let sum: f64 = dists.iter().map(|(_, d)| *d).sum();
            let cross_checked = reachable as f64 / sum;
            assert!((cross_checked - expected).abs() < 1e-9);
            assert!((m[id] - expected).abs() < 1e-9, "{id}: {}", m[id]);
        }
    }

    #[test]
    fn eg144_closeness_wasserman_faust_penalises_small_component() {
        // A 2-node component {x,y} plus a 4-node clique {w,v,u,t}. Classic
        // closeness gives x a PERFECT 1.0 (it only ever has to reach its one
        // neighbour) despite x reaching just 1 of the other 5 nodes total;
        // the Wasserman-Faust correction scales that down to 1.0 * (1/5) = 0.2.
        let mut edges: Vec<(&str, &str, f64)> = vec![("x", "y", 1.0), ("y", "x", 1.0)];
        let clique = ["w", "v", "u", "t"];
        for i in 0..clique.len() {
            for j in 0..clique.len() {
                if i != j {
                    edges.push((clique[i], clique[j], 1.0));
                }
            }
        }
        let g = AdjacencyGraph::from_edges(edges);

        let classic = closeness_centrality(&g, &ClosenessConfig::default());
        let improved = closeness_centrality(&g, &ClosenessConfig { improved: true });
        let mc: std::collections::HashMap<&str, f64> = classic.into_iter().collect();
        let mi: std::collections::HashMap<&str, f64> = improved.into_iter().collect();

        assert!((mc["x"] - 1.0).abs() < 1e-9, "classic: {}", mc["x"]);
        assert!((mi["x"] - 0.2).abs() < 1e-9, "improved: {}", mi["x"]);
        assert!(mi["x"] < mc["x"], "WF must penalise the small component");
    }

    #[test]
    fn eg144_closeness_isolated_node_is_zero() {
        let g = AdjacencyGraph::from_adjacency([("a", vec![("b", 1.0)]), ("lonely", vec![])]);
        let closeness = closeness_centrality(&g, &ClosenessConfig::default());
        let m: std::collections::HashMap<&str, f64> = closeness.into_iter().collect();
        assert_eq!(m["lonely"], 0.0);
    }

    #[test]
    fn eg144_harmonic_cross_checked_hand_computed_on_a_path() {
        // Same bidirectional path as the closeness cross-check test.
        let g = AdjacencyGraph::from_edges([
            ("a", "b", 1.0),
            ("b", "a", 1.0),
            ("b", "c", 1.0),
            ("c", "b", 1.0),
            ("c", "d", 1.0),
            ("d", "c", 1.0),
        ]);
        let harmonic = harmonic_centrality(&g);
        let m: std::collections::HashMap<&str, f64> = harmonic.into_iter().collect();
        // From b: dist to a=1,c=1,d=2 ⇒ (1/1+1/1+1/2)/3 = 2.5/3.
        assert!((m["b"] - 2.5 / 3.0).abs() < 1e-9, "{}", m["b"]);
        // From a: dist to b=1,c=2,d=3 ⇒ (1/1+1/2+1/3)/3.
        let expected_a = (1.0 + 0.5 + 1.0 / 3.0) / 3.0;
        assert!((m["a"] - expected_a).abs() < 1e-9, "{}", m["a"]);
    }

    #[test]
    fn eg144_harmonic_disconnected_pair_contributes_zero_no_blowup() {
        // Two isolated edges: harmonic centrality must not panic/NaN on the
        // unreachable far node — it just contributes 0 (unlike classic
        // closeness, no special-case correction needed).
        let g = AdjacencyGraph::from_edges([("a", "b", 1.0), ("x", "y", 1.0)]);
        let harmonic = harmonic_centrality(&g);
        for (_, v) in harmonic {
            assert!(v.is_finite());
        }
    }
}
