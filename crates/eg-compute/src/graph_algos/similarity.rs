// CONCEPT:EG-KG.compute.node-similarity — Node similarity: Jaccard + cosine over neighbour sets.
// Neo4j GDS `gds.nodeSimilarity` parity.

use super::graph::AdjacencyGraph;
use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::Hash;

/// A similarity edge between two nodes. CONCEPT:EG-KG.compute.node-similarity
#[derive(Debug, Clone)]
pub struct SimilarityPair<N> {
    /// First node (the smaller-id endpoint).
    pub a: N,
    /// Second node.
    pub b: N,
    /// Similarity score in `[0, 1]`.
    pub score: f64,
}

/// Which relationship set forms each node's "neighbour" vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Outgoing neighbours (GDS default).
    Out,
    /// Incoming neighbours.
    In,
    /// Union of both directions (undirected view).
    Undirected,
}

/// Sorted `(neighbor, weight)` list for a node under the chosen direction; the
/// undirected view sums both directions' weights.
fn neighbor_vec<N>(graph: &AdjacencyGraph<N>, i: usize, dir: Direction) -> Vec<(usize, f64)>
where
    N: Clone + Eq + Hash + Ord,
{
    match dir {
        Direction::Out => graph.out_edges(i).to_vec(),
        Direction::In => graph.in_edges(i).to_vec(),
        Direction::Undirected => {
            let mut m: HashMap<usize, f64> = HashMap::new();
            for &(t, w) in graph.out_edges(i) {
                *m.entry(t).or_insert(0.0) += w;
            }
            for &(s, w) in graph.in_edges(i) {
                *m.entry(s).or_insert(0.0) += w;
            }
            let mut v: Vec<(usize, f64)> = m.into_iter().collect();
            v.sort_unstable_by_key(|(k, _)| *k);
            v
        }
    }
}

/// Prepare each node's neighbor vector once for an all-pairs query. Directed
/// views borrow the graph's already-sorted adjacency without copying; only the
/// synthesized undirected view owns merged rows.
fn prepared_neighbors<N>(graph: &AdjacencyGraph<N>, dir: Direction) -> Vec<Cow<'_, [(usize, f64)]>>
where
    N: Clone + Eq + Hash + Ord,
{
    (0..graph.node_count())
        .map(|index| match dir {
            Direction::Out => Cow::Borrowed(graph.out_edges(index)),
            Direction::In => Cow::Borrowed(graph.in_edges(index)),
            Direction::Undirected => Cow::Owned(neighbor_vec(graph, index, dir)),
        })
        .collect()
}

fn neighbor_intersection(a: &[(usize, f64)], b: &[(usize, f64)]) -> usize {
    let (mut i, mut j, mut intersection) = (0, 0, 0);
    while i < a.len() && j < b.len() {
        match a[i].0.cmp(&b[j].0) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                intersection += 1;
                i += 1;
                j += 1;
            }
        }
    }
    intersection
}

fn jaccard_from_neighbors(a: &[(usize, f64)], b: &[(usize, f64)]) -> f64 {
    let intersection = neighbor_intersection(a, b);
    let union = a.len() + b.len() - intersection;
    if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    }
}

fn neighbor_norm(neighbors: &[(usize, f64)]) -> f64 {
    neighbors
        .iter()
        .map(|(_, weight)| weight * weight)
        .sum::<f64>()
        .sqrt()
}

fn cosine_from_neighbors(a: &[(usize, f64)], b: &[(usize, f64)], norm_a: f64, norm_b: f64) -> f64 {
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    let (mut i, mut j) = (0, 0);
    let mut dot = 0.0;
    while i < a.len() && j < b.len() {
        match a[i].0.cmp(&b[j].0) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                dot += a[i].1 * b[j].1;
                i += 1;
                j += 1;
            }
        }
    }
    dot / (norm_a * norm_b)
}

/// **Jaccard** similarity of two nodes' neighbour *sets* (weights ignored):
/// `|N(a) ∩ N(b)| / |N(a) ∪ N(b)|`. Two nodes with no neighbours score 0.
///
/// Complexity: `O(deg(a) + deg(b))`. CONCEPT:EG-KG.compute.node-similarity
pub fn jaccard_similarity<N>(graph: &AdjacencyGraph<N>, a: usize, b: usize, dir: Direction) -> f64
where
    N: Clone + Eq + Hash + Ord,
{
    let na = neighbor_vec(graph, a, dir);
    let nb = neighbor_vec(graph, b, dir);
    jaccard_from_neighbors(&na, &nb)
}

/// **Cosine** similarity of two nodes' weighted neighbour *vectors*:
/// `(a · b) / (‖a‖ ‖b‖)` over the shared neighbour space. For unit weights this
/// reduces to `|N(a) ∩ N(b)| / √(|N(a)|·|N(b)|)`.
///
/// Complexity: `O(deg(a) + deg(b))`. CONCEPT:EG-KG.compute.node-similarity
pub fn cosine_similarity<N>(graph: &AdjacencyGraph<N>, a: usize, b: usize, dir: Direction) -> f64
where
    N: Clone + Eq + Hash + Ord,
{
    let va = neighbor_vec(graph, a, dir);
    let vb = neighbor_vec(graph, b, dir);
    cosine_from_neighbors(&va, &vb, neighbor_norm(&va), neighbor_norm(&vb))
}

/// Which metric an all-pairs sweep uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    /// Set-based Jaccard.
    Jaccard,
    /// Weighted cosine.
    Cosine,
}

fn neighbor_score_cmp(left: &(usize, f64), right: &(usize, f64)) -> std::cmp::Ordering {
    right
        .1
        .partial_cmp(&left.1)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| left.0.cmp(&right.0))
}

/// All-pairs node similarity above a cutoff. Returns each unordered pair
/// `(a < b)` whose score `> cutoff`, sorted by descending score then ascending
/// node ids (deterministic).
///
/// Complexity: `O(V² · d̄)` naïvely. CONCEPT:EG-KG.compute.node-similarity
pub fn all_pairs_similarity<N>(
    graph: &AdjacencyGraph<N>,
    metric: Metric,
    dir: Direction,
    cutoff: f64,
) -> Vec<SimilarityPair<N>>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let neighbors = prepared_neighbors(graph, dir);
    let norms: Vec<f64> = if metric == Metric::Cosine {
        neighbors.iter().map(|row| neighbor_norm(row)).collect()
    } else {
        Vec::new()
    };
    let mut out: Vec<(usize, usize, f64)> = Vec::new();
    for a in 0..n {
        for b in (a + 1)..n {
            let s = match metric {
                Metric::Jaccard => jaccard_from_neighbors(&neighbors[a], &neighbors[b]),
                Metric::Cosine => {
                    cosine_from_neighbors(&neighbors[a], &neighbors[b], norms[a], norms[b])
                }
            };
            if s > cutoff {
                out.push((a, b, s));
            }
        }
    }
    // Descending score, then ascending (a, b) for stable ordering.
    out.sort_by(|x, y| {
        y.2.partial_cmp(&x.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| x.0.cmp(&y.0))
            .then_with(|| x.1.cmp(&y.1))
    });
    out.into_iter()
        .map(|(a, b, score)| SimilarityPair {
            a: graph.node_at(a).clone(),
            b: graph.node_at(b).clone(),
            score,
        })
        .collect()
}

/// Score node `a` against every other node under `metric`, keep only scores
/// `> cutoff`, and truncate to the top-`k` (descending score, ascending id).
/// Pure extraction from [`knn_similarity`]'s per-node loop body -- identical
/// scoring/filter/truncate/sort logic, no behaviour change; pulled out solely
/// to keep [`knn_similarity`]'s own cyclomatic complexity within the repo's
/// gate cap. CONCEPT:EG-KG.compute.node-similarity
fn top_k_for_node(
    a: usize,
    n: usize,
    k: usize,
    cutoff: f64,
    metric: Metric,
    neighbors: &[Cow<'_, [(usize, f64)]>],
    norms: &[f64],
) -> Vec<(usize, f64)> {
    let mut scored: Vec<(usize, f64)> = (0..n)
        .filter(|&b| b != a)
        .map(|b| {
            let s = match metric {
                Metric::Jaccard => jaccard_from_neighbors(&neighbors[a], &neighbors[b]),
                Metric::Cosine => {
                    cosine_from_neighbors(&neighbors[a], &neighbors[b], norms[a], norms[b])
                }
            };
            (b, s)
        })
        .filter(|&(_, s)| s > cutoff)
        .collect();
    if scored.len() > k {
        scored.select_nth_unstable_by(k, neighbor_score_cmp);
        scored.truncate(k);
    }
    // The public result is ordered, but the discarded V-k neighbors are not:
    // only sort the exact selected prefix under the established score/id order.
    scored.sort_by(neighbor_score_cmp);
    scored
}

/// Per-node top-`k` nearest-neighbour similarity edges (CONCEPT:EG-KG.compute.node-similarity),
/// `gds.knn` parity. Distinct from [`all_pairs_similarity`]'s GLOBAL cutoff sweep
/// (`gds.nodeSimilarity`): each node independently keeps its `top_k` best-scoring
/// OTHER nodes (score `> cutoff`), then the directed per-node results are folded
/// into undirected pairs (keeping the max of the two directional scores). This
/// engine computes the exact top-`k` via a full sweep rather than Neo4j's
/// approximate KNN-descent sampling — exact and deterministic, at `O(V²·d̄)`
/// instead of KNN-descent's sub-quadratic approximate cost; fine at the node
/// counts this engine targets.
///
/// Complexity: `O(V² · d̄)`. Returns pairs sorted by descending score then
/// ascending ids.
pub fn knn_similarity<N>(
    graph: &AdjacencyGraph<N>,
    metric: Metric,
    dir: Direction,
    top_k: usize,
    cutoff: f64,
) -> Vec<SimilarityPair<N>>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let k = top_k.max(1);
    let neighbors = prepared_neighbors(graph, dir);
    let norms: Vec<f64> = if metric == Metric::Cosine {
        neighbors.iter().map(|row| neighbor_norm(row)).collect()
    } else {
        Vec::new()
    };
    let mut pair_best: HashMap<(usize, usize), f64> = HashMap::new();
    for a in 0..n {
        let scored = top_k_for_node(a, n, k, cutoff, metric, &neighbors, &norms);
        for (b, s) in scored {
            let key = if a < b { (a, b) } else { (b, a) };
            let e = pair_best.entry(key).or_insert(f64::MIN);
            if s > *e {
                *e = s;
            }
        }
    }
    let mut out: Vec<(usize, usize, f64)> =
        pair_best.into_iter().map(|((a, b), s)| (a, b, s)).collect();
    out.sort_by(|x, y| {
        y.2.partial_cmp(&x.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| x.0.cmp(&y.0))
            .then_with(|| x.1.cmp(&y.1))
    });
    out.into_iter()
        .map(|(a, b, score)| SimilarityPair {
            a: graph.node_at(a).clone(),
            b: graph.node_at(b).clone(),
            score,
        })
        .collect()
}

/// A tiny deterministic SplitMix64 PRNG for reproducible NN-descent sampling
/// (CONCEPT:EG-KG.compute.node-similarity). Dep-free — the always-on `graph_algos` kernels take no
/// `rand` dependency, matching the same inline-hash approach the HNSW index uses
/// for its level assignment.
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

    /// Uniform integer in `[0, bound)`; `bound` must be positive.
    fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

/// One entry in a node's working K-NN list during NN-descent: the candidate node,
/// its similarity score, and whether it was added/updated since the last local join
/// (the `is_new` flag focuses each iteration on fresh information and guarantees
/// termination). CONCEPT:EG-KG.compute.node-similarity
struct DescNeighbor {
    node: usize,
    score: f64,
    is_new: bool,
}

/// Try to fold `(v, s)` into node `u`'s working top-`k` list, keeping it a deduped
/// max-`k` set of the highest scores. Returns `true` if the list changed (which the
/// caller counts toward the convergence test). A changed/added entry is marked
/// `is_new`. Deterministic: the evicted entry is the lowest score, breaking ties by
/// the largest node id, and an equal score never displaces an incumbent.
fn desc_try_update(list: &mut Vec<DescNeighbor>, u: usize, v: usize, s: f64, k: usize) -> bool {
    if u == v {
        return false;
    }
    if list.iter().any(|e| e.node == v) {
        return false;
    }
    if list.len() < k {
        list.push(DescNeighbor {
            node: v,
            score: s,
            is_new: true,
        });
        return true;
    }
    // Find the current worst entry (lowest score, then largest node id) to evict.
    let mut worst = 0usize;
    for (i, entry) in list.iter().enumerate().skip(1) {
        let cur = &list[worst];
        let better_evict =
            entry.score < cur.score || (entry.score == cur.score && entry.node > cur.node);
        if better_evict {
            worst = i;
        }
    }
    if s > list[worst].score {
        list[worst] = DescNeighbor {
            node: v,
            score: s,
            is_new: true,
        };
        return true;
    }
    false
}

/// Draw up to `sample` items from `pool` (all of it when smaller) into `dst`, via a
/// partial Fisher–Yates over a scratch copy so the draw is uniform and deterministic
/// under the seeded PRNG. Bounds the local-join fan-out (a high-in-degree hub's
/// reverse list is capped here) — the sampling that makes NN-descent sub-quadratic.
fn desc_sample_into(dst: &mut Vec<usize>, pool: &[usize], sample: usize, rng: &mut SplitMix64) {
    if pool.is_empty() {
        return;
    }
    if pool.len() <= sample {
        dst.extend_from_slice(pool);
        return;
    }
    let mut scratch = pool.to_vec();
    for i in 0..sample {
        let j = i + rng.below(scratch.len() - i);
        scratch.swap(i, j);
        dst.push(scratch[i]);
    }
}

/// Approximate per-node top-`k` node-similarity via NN-descent sampling
/// (CONCEPT:EG-KG.compute.node-similarity) — the APPROXIMATE, mode-selectable sibling of the exact
/// [`knn_similarity`]. Instead of the exact `O(V²·d̄)` full sweep, it seeds each node
/// with `k` random candidates and iteratively refines by joining each node's current
/// neighbours-of-neighbours (Dong, Charikar & Li, 2011), trading exactness for a
/// sub-quadratic `~O(V·k²·d̄·iters)` cost that dominates at large V. Knobs:
///
///   * `sample_rate` (ρ ∈ (0,1], GDS `sampleRate`) bounds the per-node join fan-out
///     to `⌈ρ·k⌉` new + `⌈ρ·k⌉` reverse candidates.
///   * `max_iters` caps refinement rounds; `delta` (GDS `deltaThreshold`) stops early
///     once the fraction of updated entries falls below it.
///   * `seed` (GDS `randomSeed`) makes the sampling reproducible — identical graphs
///     and knobs yield identical output.
///
/// The result shape + ordering are IDENTICAL to [`knn_similarity`] (undirected pairs,
/// max of the two directional scores, `score > cutoff`, sorted by descending score
/// then ascending ids), so the two are drop-in interchangeable behind `gds.knn`'s
/// `mode`. Falls back to the exact sweep when the candidate pool is at/below `k`
/// (nothing to approximate, and it dodges sampling artefacts on tiny graphs).
#[allow(clippy::too_many_arguments)]
pub fn knn_similarity_approx<N>(
    graph: &AdjacencyGraph<N>,
    metric: Metric,
    dir: Direction,
    top_k: usize,
    cutoff: f64,
    sample_rate: f64,
    max_iters: usize,
    delta: f64,
    seed: u64,
) -> Vec<SimilarityPair<N>>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let k = top_k.max(1);
    // Below this the exact sweep is already cheap and sampling only adds noise.
    if n <= k + 1 {
        return knn_similarity(graph, metric, dir, top_k, cutoff);
    }
    let neighbors = prepared_neighbors(graph, dir);
    let norms: Vec<f64> = if metric == Metric::Cosine {
        neighbors.iter().map(|row| neighbor_norm(row)).collect()
    } else {
        Vec::new()
    };
    let sim = |a: usize, b: usize| -> f64 {
        match metric {
            Metric::Jaccard => jaccard_from_neighbors(&neighbors[a], &neighbors[b]),
            Metric::Cosine => {
                cosine_from_neighbors(&neighbors[a], &neighbors[b], norms[a], norms[b])
            }
        }
    };
    let sample = (sample_rate.clamp(0.0, 1.0) * k as f64).ceil().max(1.0) as usize;
    let mut rng = SplitMix64::new(seed ^ 0x6B6E_6E5F_6465_7363); // "knn_desc"

    // Inverted index for SHARED-NEIGHBOUR (2-hop) candidate generation: target node
    // index → the nodes whose neighbour set contains it. Two nodes have nonzero
    // similarity ONLY if their neighbour sets overlap, so a node's real candidates are
    // exactly the nodes sharing one of its neighbours. Seeding NN-descent from a
    // SAMPLE of these (rather than uniform-random pairs, almost all of which score 0)
    // is what lets it bootstrap on local structure — the same shared-neighbour
    // sampling Neo4j's own approximate `gds.knn` uses. `neighbors` already encodes the
    // chosen `dir`, so this is direction-agnostic.
    let mut inverted: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (b, row) in neighbors.iter().enumerate() {
        for &(t, _) in row.iter() {
            if t < n {
                inverted[t].push(b);
            }
        }
    }

    // Seed: for each node, gather up to `cand_cap` sampled shared-neighbour candidates,
    // score them, and keep the top-`k`. If that pool is thin (a low-degree node), top
    // up with a few random draws so refinement still has somewhere to walk.
    let cand_cap = (k * 8).max(k + 1);
    let mut lists: Vec<Vec<DescNeighbor>> = (0..n).map(|_| Vec::with_capacity(k)).collect();
    let mut seen: Vec<bool> = vec![false; n];
    let mut touched: Vec<usize> = Vec::new();
    for a in 0..n {
        seen[a] = true;
        touched.push(a);
        let mut scored = 0;
        'gather: for &(t, _) in neighbors[a].iter() {
            let sources = &inverted[t];
            if sources.is_empty() {
                continue;
            }
            let mut drawn = 0;
            let mut tries = 0;
            while drawn < sample && tries < sample * 3 {
                tries += 1;
                let b = sources[rng.below(sources.len())];
                if !seen[b] {
                    seen[b] = true;
                    touched.push(b);
                    let s = sim(a, b);
                    desc_try_update(&mut lists[a], a, b, s, k);
                    drawn += 1;
                    scored += 1;
                    if scored >= cand_cap {
                        break 'gather;
                    }
                }
            }
        }
        let mut attempts = 0;
        while lists[a].len() < k && attempts < k * 4 + 8 {
            attempts += 1;
            let b = rng.below(n);
            if b != a && !lists[a].iter().any(|e| e.node == b) {
                let s = sim(a, b);
                desc_try_update(&mut lists[a], a, b, s, k);
            }
        }
        // Reset the per-node `seen` marks cheaply (only the touched entries).
        for &node in &touched {
            seen[node] = false;
        }
        touched.clear();
    }

    for round in 0..max_iters.max(1) {
        // Split each node's list into sampled NEW and OLD candidate sets, flipping the
        // drawn `new` entries to old so the next round only re-joins fresh information.
        let mut new_lists: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut old_lists: Vec<Vec<usize>> = vec![Vec::new(); n];
        for u in 0..n {
            let mut new_pool: Vec<usize> = Vec::new();
            for entry in lists[u].iter() {
                if !entry.is_new {
                    old_lists[u].push(entry.node);
                }
            }
            for entry in lists[u].iter().filter(|e| e.is_new) {
                new_pool.push(entry.node);
            }
            desc_sample_into(&mut new_lists[u], &new_pool, sample, &mut rng);
            // Mark the drawn `new` entries as old for the next iteration.
            let drawn: std::collections::HashSet<usize> = new_lists[u].iter().copied().collect();
            for entry in lists[u].iter_mut() {
                if entry.is_new && drawn.contains(&entry.node) {
                    entry.is_new = false;
                }
            }
        }

        // Reverse (in-)lists, sampled to bound hub fan-out.
        let mut r_new: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut r_old: Vec<Vec<usize>> = vec![Vec::new(); n];
        for u in 0..n {
            for &v in &new_lists[u] {
                r_new[v].push(u);
            }
            for &v in &old_lists[u] {
                r_old[v].push(u);
            }
        }

        let mut updates = 0usize;
        for u in 0..n {
            let mut nu = new_lists[u].clone();
            desc_sample_into(&mut nu, &r_new[u], sample, &mut rng);
            let mut ou = old_lists[u].clone();
            desc_sample_into(&mut ou, &r_old[u], sample, &mut rng);
            nu.sort_unstable();
            nu.dedup();
            ou.sort_unstable();
            ou.dedup();
            for i in 0..nu.len() {
                let p = nu[i];
                for &q in nu.iter().skip(i + 1) {
                    let s = sim(p, q);
                    updates += desc_try_update(&mut lists[p], p, q, s, k) as usize;
                    updates += desc_try_update(&mut lists[q], q, p, s, k) as usize;
                }
                for &q in &ou {
                    if p == q {
                        continue;
                    }
                    let s = sim(p, q);
                    updates += desc_try_update(&mut lists[p], p, q, s, k) as usize;
                    updates += desc_try_update(&mut lists[q], q, p, s, k) as usize;
                }
            }
        }
        tracing::trace!(
            target: "eg_compute::knn_descent",
            mode = "approximate",
            round,
            sample_rate,
            updates,
            "nn-descent refinement round",
        );
        if (updates as f64) <= delta * n as f64 * k as f64 {
            break;
        }
    }

    // Fold the directed working lists into undirected pairs (max of the two
    // directional scores), applying the `> cutoff` gate only now — the working lists
    // keep the best-k regardless of cutoff so the join always has neighbours to walk.
    let mut pair_best: HashMap<(usize, usize), f64> = HashMap::new();
    for (a, list) in lists.iter().enumerate() {
        for entry in list {
            if entry.score <= cutoff {
                continue;
            }
            let b = entry.node;
            let key = if a < b { (a, b) } else { (b, a) };
            let e = pair_best.entry(key).or_insert(f64::MIN);
            if entry.score > *e {
                *e = entry.score;
            }
        }
    }
    let mut out: Vec<(usize, usize, f64)> =
        pair_best.into_iter().map(|((a, b), s)| (a, b, s)).collect();
    out.sort_by(|x, y| {
        y.2.partial_cmp(&x.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| x.0.cmp(&y.0))
            .then_with(|| x.1.cmp(&y.1))
    });
    out.into_iter()
        .map(|(a, b, score)| SimilarityPair {
            a: graph.node_at(a).clone(),
            b: graph.node_at(b).clone(),
            score,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg144_jaccard_overlapping_neighbors() {
        // a→{x,y,z}, b→{y,z,w}. Intersection {y,z}=2, union {w,x,y,z}=4 ⇒ 0.5.
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 1.0),
            ("a", "z", 1.0),
            ("b", "y", 1.0),
            ("b", "z", 1.0),
            ("b", "w", 1.0),
        ]);
        let (a, b) = (g.index_of(&"a").unwrap(), g.index_of(&"b").unwrap());
        assert!((jaccard_similarity(&g, a, b, Direction::Out) - 0.5).abs() < 1e-9);
    }

    #[test]
    fn eg144_jaccard_identical_and_disjoint() {
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 1.0),
            ("b", "x", 1.0),
            ("b", "y", 1.0),
            ("c", "p", 1.0),
            ("c", "q", 1.0),
        ]);
        let (a, b, c) = (
            g.index_of(&"a").unwrap(),
            g.index_of(&"b").unwrap(),
            g.index_of(&"c").unwrap(),
        );
        assert!((jaccard_similarity(&g, a, b, Direction::Out) - 1.0).abs() < 1e-9);
        assert!((jaccard_similarity(&g, a, c, Direction::Out) - 0.0).abs() < 1e-9);
    }

    #[test]
    fn eg144_cosine_unit_weights_matches_formula() {
        // a→{x,y,z}, b→{y,z,w}: inter=2, |a|=|b|=3 ⇒ 2/√(3·3)=2/3.
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 1.0),
            ("a", "z", 1.0),
            ("b", "y", 1.0),
            ("b", "z", 1.0),
            ("b", "w", 1.0),
        ]);
        let (a, b) = (g.index_of(&"a").unwrap(), g.index_of(&"b").unwrap());
        assert!((cosine_similarity(&g, a, b, Direction::Out) - 2.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn eg144_cosine_respects_weights() {
        // Same target, proportional weight vectors ⇒ cosine 1.0.
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 2.0),
            ("b", "x", 2.0),
            ("b", "y", 4.0),
        ]);
        let (a, b) = (g.index_of(&"a").unwrap(), g.index_of(&"b").unwrap());
        assert!((cosine_similarity(&g, a, b, Direction::Out) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn prepared_pair_scores_match_pointwise_semantics() {
        let g = AdjacencyGraph::from_edges([
            ("a", "a", 0.5),
            ("a", "b", 1.0),
            ("b", "a", 2.0),
            ("b", "c", 3.0),
            ("c", "a", 4.0),
        ]);
        for direction in [Direction::Out, Direction::In, Direction::Undirected] {
            let prepared = prepared_neighbors(&g, direction);
            let norms: Vec<f64> = prepared.iter().map(|row| neighbor_norm(row)).collect();
            for a in 0..g.node_count() {
                for b in 0..g.node_count() {
                    assert_eq!(
                        jaccard_from_neighbors(&prepared[a], &prepared[b]),
                        jaccard_similarity(&g, a, b, direction)
                    );
                    let prepared_cosine =
                        cosine_from_neighbors(&prepared[a], &prepared[b], norms[a], norms[b]);
                    let pointwise_cosine = cosine_similarity(&g, a, b, direction);
                    assert!((prepared_cosine - pointwise_cosine).abs() < 1e-12);
                }
            }
        }
    }

    #[test]
    fn knn_similarity_keeps_top_k_per_node() {
        // a/b share {x,y} (jaccard 1.0); c only shares {x} with each (jaccard 0.5).
        // With top_k=1: a's + b's mutual best pick is each other (1.0); c's best
        // pick is a (0.5, ascending-id tie-break over the equally-scored b) — but
        // NOT b's pick (b's own top-1 is a, at a strictly higher score than c),
        // so (b, c) never appears.
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 1.0),
            ("b", "x", 1.0),
            ("b", "y", 1.0),
            ("c", "x", 1.0),
        ]);
        let pairs = knn_similarity(&g, Metric::Jaccard, Direction::Out, 1, 0.0);
        assert!(pairs
            .iter()
            .any(|p| p.a == "a" && p.b == "b" && (p.score - 1.0).abs() < 1e-9));
        assert!(pairs
            .iter()
            .any(|p| p.a == "a" && p.b == "c" && (p.score - 0.5).abs() < 1e-9));
        assert!(!pairs
            .iter()
            .any(|p| (p.a == "b" && p.b == "c") || (p.a == "c" && p.b == "b")));
        assert_eq!(pairs.len(), 2);
    }

    #[test]
    fn knn_similarity_empty_graph_is_empty() {
        let g: AdjacencyGraph<String> =
            AdjacencyGraph::from_adjacency(Vec::<(String, Vec<(String, f64)>)>::new());
        assert!(knn_similarity(&g, Metric::Jaccard, Direction::Out, 5, 0.0).is_empty());
    }

    /// A clustered similarity graph: `blocks` groups of `per_block` source nodes;
    /// every source in a block points at the SAME `feats` feature targets, so
    /// within-block nodes have near-identical neighbour sets (high similarity) and
    /// cross-block nodes share nothing — exactly the local structure NN-descent
    /// exploits. Feature targets are namespaced per block so blocks are disjoint.
    fn clustered_similarity_graph(
        blocks: usize,
        per_block: usize,
        feats: usize,
    ) -> AdjacencyGraph<String> {
        let mut edges: Vec<(String, String, f64)> = Vec::new();
        for blk in 0..blocks {
            for member in 0..per_block {
                let src = format!("n{blk}_{member}");
                for f in 0..feats {
                    edges.push((src.clone(), format!("f{blk}_{f}"), 1.0));
                }
            }
        }
        AdjacencyGraph::from_edges(edges)
    }

    /// Pair set (as ordered id tuples) for set-recall comparison.
    fn pair_set<N: Clone + Ord + std::hash::Hash>(
        pairs: &[SimilarityPair<N>],
    ) -> std::collections::HashSet<(N, N)> {
        pairs
            .iter()
            .map(|p| {
                if p.a <= p.b {
                    (p.a.clone(), p.b.clone())
                } else {
                    (p.b.clone(), p.a.clone())
                }
            })
            .collect()
    }

    #[test]
    fn knn_approx_recovers_most_of_exact_pairs() {
        // 30 blocks × 8 members = 240 sources, 6 shared features per block.
        let g = clustered_similarity_graph(30, 8, 6);
        let k = 8;
        let exact = knn_similarity(&g, Metric::Jaccard, Direction::Out, k, 0.0);
        let approx = knn_similarity_approx(
            &g,
            Metric::Jaccard,
            Direction::Out,
            k,
            0.0,
            0.5,
            30,
            0.001,
            42,
        );
        let (se, sa) = (pair_set(&exact), pair_set(&approx));
        let recovered = se.intersection(&sa).count();
        let recall = recovered as f64 / se.len().max(1) as f64;
        assert!(
            recall >= 0.9,
            "approx knn pair-recall = {recall:.4} (recovered {recovered}/{}) must be >= 0.9",
            se.len()
        );
        // Every emitted pair must clear the cutoff (approx applies the same gate).
        assert!(approx.iter().all(|p| p.score > 0.0));
    }

    #[test]
    fn knn_approx_is_deterministic_for_fixed_seed() {
        let g = clustered_similarity_graph(20, 6, 5);
        let run = || {
            knn_similarity_approx(
                &g,
                Metric::Cosine,
                Direction::Out,
                6,
                0.0,
                0.5,
                20,
                0.001,
                7,
            )
        };
        let a = run();
        let b = run();
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x.a, y.a);
            assert_eq!(x.b, y.b);
            assert!((x.score - y.score).abs() < 1e-12);
        }
    }

    #[test]
    fn knn_approx_falls_back_to_exact_on_tiny_graph() {
        // n = 3 sources ≤ k+1 ⇒ the approx entry point returns the exact sweep.
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 1.0),
            ("b", "x", 1.0),
            ("b", "y", 1.0),
            ("c", "x", 1.0),
        ]);
        let exact = knn_similarity(&g, Metric::Jaccard, Direction::Out, 5, 0.0);
        let approx = knn_similarity_approx(
            &g,
            Metric::Jaccard,
            Direction::Out,
            5,
            0.0,
            0.5,
            10,
            0.001,
            1,
        );
        assert_eq!(pair_set(&exact), pair_set(&approx));
    }

    #[test]
    fn knn_approx_respects_cutoff() {
        let g = clustered_similarity_graph(15, 6, 5);
        let approx = knn_similarity_approx(
            &g,
            Metric::Jaccard,
            Direction::Out,
            8,
            0.5,
            0.5,
            20,
            0.001,
            3,
        );
        assert!(
            approx.iter().all(|p| p.score > 0.5),
            "no pair may fall at/below the cutoff"
        );
    }

    #[test]
    fn eg144_all_pairs_similarity_ranked() {
        let g = AdjacencyGraph::from_edges([
            ("a", "x", 1.0),
            ("a", "y", 1.0),
            ("b", "x", 1.0),
            ("b", "y", 1.0),
            ("c", "y", 1.0),
        ]);
        let pairs = all_pairs_similarity(&g, Metric::Jaccard, Direction::Out, 0.0);
        // a & b share {x,y} ⇒ top pair with score 1.0.
        assert_eq!(pairs[0].a, "a");
        assert_eq!(pairs[0].b, "b");
        assert!((pairs[0].score - 1.0).abs() < 1e-9);
    }
}
