use super::super::graph::AdjacencyGraph;
use super::{
    cosine_from_neighbors, finish_similarity_pairs, jaccard_from_neighbors, knn_similarity,
    neighbor_norm, prepared_neighbors, KnnSimilarityApproxConfig, Metric, SimilarityPair,
};
use crate::SplitMix64;
use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::Hash;

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

struct SimilarityContext<'slice, 'row> {
    metric: Metric,
    neighbors: &'slice [Cow<'row, [(usize, f64)]>],
    norms: &'slice [f64],
}

fn similarity_score(context: &SimilarityContext<'_, '_>, a: usize, b: usize) -> f64 {
    match context.metric {
        Metric::Jaccard => jaccard_from_neighbors(&context.neighbors[a], &context.neighbors[b]),
        Metric::Cosine => cosine_from_neighbors(
            &context.neighbors[a],
            &context.neighbors[b],
            context.norms[a],
            context.norms[b],
        ),
    }
}

fn metric_norms(metric: Metric, neighbors: &[Cow<'_, [(usize, f64)]>]) -> Vec<f64> {
    if metric == Metric::Cosine {
        neighbors.iter().map(|row| neighbor_norm(row)).collect()
    } else {
        Vec::new()
    }
}

fn inverted_neighbor_index(neighbors: &[Cow<'_, [(usize, f64)]>], n: usize) -> Vec<Vec<usize>> {
    let mut inverted: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (b, row) in neighbors.iter().enumerate() {
        for &(t, _) in row.iter() {
            if t < n {
                inverted[t].push(b);
            }
        }
    }
    inverted
}

struct SeedState<'a> {
    list: &'a mut Vec<DescNeighbor>,
    seen: &'a mut [bool],
    touched: &'a mut Vec<usize>,
}

struct SeedConfig {
    node: usize,
    n: usize,
    k: usize,
    sample: usize,
    cand_cap: usize,
}

fn seed_shared_candidates(
    config: &SeedConfig,
    context: &SimilarityContext<'_, '_>,
    inverted: &[Vec<usize>],
    state: &mut SeedState<'_>,
    rng: &mut SplitMix64,
) {
    let mut scored = 0;
    'gather: for &(t, _) in context.neighbors[config.node].iter() {
        let sources = &inverted[t];
        if sources.is_empty() {
            continue;
        }
        let mut drawn = 0;
        let mut tries = 0;
        while drawn < config.sample && tries < config.sample * 3 {
            tries += 1;
            let b = sources[rng.below(sources.len())];
            if !state.seen[b] {
                state.seen[b] = true;
                state.touched.push(b);
                let s = similarity_score(context, config.node, b);
                desc_try_update(state.list, config.node, b, s, config.k);
                drawn += 1;
                scored += 1;
                if scored >= config.cand_cap {
                    break 'gather;
                }
            }
        }
    }
}

fn seed_random_candidates(
    config: &SeedConfig,
    context: &SimilarityContext<'_, '_>,
    list: &mut Vec<DescNeighbor>,
    rng: &mut SplitMix64,
) {
    let mut attempts = 0;
    while list.len() < config.k && attempts < config.k * 4 + 8 {
        attempts += 1;
        let b = rng.below(config.n);
        if b != config.node && !list.iter().any(|e| e.node == b) {
            let s = similarity_score(context, config.node, b);
            desc_try_update(list, config.node, b, s, config.k);
        }
    }
}

fn seed_descent_node(
    config: &SeedConfig,
    context: &SimilarityContext<'_, '_>,
    inverted: &[Vec<usize>],
    rng: &mut SplitMix64,
) -> Vec<DescNeighbor> {
    let mut list = Vec::with_capacity(config.k);
    let mut seen: Vec<bool> = vec![false; config.n];
    let mut touched: Vec<usize> = Vec::new();
    seen[config.node] = true;
    touched.push(config.node);
    {
        let mut state = SeedState {
            list: &mut list,
            seen: &mut seen,
            touched: &mut touched,
        };
        seed_shared_candidates(config, context, inverted, &mut state, rng);
        seed_random_candidates(config, context, state.list, rng);
    }
    for &node in &touched {
        seen[node] = false;
    }
    touched.clear();
    list
}

fn seed_descent_lists(
    n: usize,
    k: usize,
    sample: usize,
    cand_cap: usize,
    context: &SimilarityContext<'_, '_>,
    inverted: &[Vec<usize>],
    rng: &mut SplitMix64,
) -> Vec<Vec<DescNeighbor>> {
    let mut lists: Vec<Vec<DescNeighbor>> = Vec::with_capacity(n);
    for a in 0..n {
        let config = SeedConfig {
            node: a,
            n,
            k,
            sample,
            cand_cap,
        };
        lists.push(seed_descent_node(&config, context, inverted, rng));
    }
    lists
}

fn split_descent_lists(
    lists: &mut [Vec<DescNeighbor>],
    sample: usize,
    rng: &mut SplitMix64,
) -> (Vec<Vec<usize>>, Vec<Vec<usize>>) {
    let n = lists.len();
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
        desc_sample_into(&mut new_lists[u], &new_pool, sample, rng);
        let drawn: std::collections::HashSet<usize> = new_lists[u].iter().copied().collect();
        for entry in lists[u].iter_mut() {
            if entry.is_new && drawn.contains(&entry.node) {
                entry.is_new = false;
            }
        }
    }
    (new_lists, old_lists)
}

fn reverse_descent_lists(
    new_lists: &[Vec<usize>],
    old_lists: &[Vec<usize>],
    n: usize,
) -> (Vec<Vec<usize>>, Vec<Vec<usize>>) {
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
    (r_new, r_old)
}

fn update_descent_pair(
    lists: &mut [Vec<DescNeighbor>],
    p: usize,
    q: usize,
    k: usize,
    context: &SimilarityContext<'_, '_>,
) -> usize {
    let s = similarity_score(context, p, q);
    let mut updates = desc_try_update(&mut lists[p], p, q, s, k) as usize;
    updates += desc_try_update(&mut lists[q], q, p, s, k) as usize;
    updates
}

struct DescentCandidates<'a> {
    new_lists: &'a [Vec<usize>],
    old_lists: &'a [Vec<usize>],
    r_new: &'a [Vec<usize>],
    r_old: &'a [Vec<usize>],
}

fn refine_descent_node(
    u: usize,
    lists: &mut [Vec<DescNeighbor>],
    candidates: &DescentCandidates<'_>,
    sample: usize,
    k: usize,
    context: &SimilarityContext<'_, '_>,
    rng: &mut SplitMix64,
) -> usize {
    let mut nu = candidates.new_lists[u].clone();
    desc_sample_into(&mut nu, &candidates.r_new[u], sample, rng);
    let mut ou = candidates.old_lists[u].clone();
    desc_sample_into(&mut ou, &candidates.r_old[u], sample, rng);
    nu.sort_unstable();
    nu.dedup();
    ou.sort_unstable();
    ou.dedup();
    let mut updates = 0usize;
    for i in 0..nu.len() {
        let p = nu[i];
        for &q in nu.iter().skip(i + 1) {
            updates += update_descent_pair(lists, p, q, k, context);
        }
        for &q in &ou {
            if p == q {
                continue;
            }
            updates += update_descent_pair(lists, p, q, k, context);
        }
    }
    updates
}

fn refine_descent_round(
    lists: &mut [Vec<DescNeighbor>],
    sample: usize,
    k: usize,
    context: &SimilarityContext<'_, '_>,
    rng: &mut SplitMix64,
) -> usize {
    let (new_lists, old_lists) = split_descent_lists(lists, sample, rng);
    let (r_new, r_old) = reverse_descent_lists(&new_lists, &old_lists, lists.len());
    let candidates = DescentCandidates {
        new_lists: &new_lists,
        old_lists: &old_lists,
        r_new: &r_new,
        r_old: &r_old,
    };
    let mut updates = 0usize;
    for u in 0..lists.len() {
        updates += refine_descent_node(u, lists, &candidates, sample, k, context, rng);
    }
    updates
}

fn fold_descent_pairs(lists: &[Vec<DescNeighbor>], cutoff: f64) -> HashMap<(usize, usize), f64> {
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
    pair_best
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
pub fn knn_similarity_approx<N>(
    graph: &AdjacencyGraph<N>,
    config: KnnSimilarityApproxConfig,
) -> Vec<SimilarityPair<N>>
where
    N: Clone + Eq + Hash + Ord,
{
    let KnnSimilarityApproxConfig {
        metric,
        direction: dir,
        top_k,
        cutoff,
        sample_rate,
        max_iters,
        delta,
        seed,
    } = config;
    let n = graph.node_count();
    let k = top_k.max(1);
    // Below this the exact sweep is already cheap and sampling only adds noise.
    if n <= k + 1 {
        return knn_similarity(graph, metric, dir, top_k, cutoff);
    }
    let neighbors = prepared_neighbors(graph, dir);
    let norms = metric_norms(metric, &neighbors);
    let context = SimilarityContext {
        metric,
        neighbors: &neighbors,
        norms: &norms,
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
    let inverted = inverted_neighbor_index(&neighbors, n);

    // Seed: for each node, gather up to `cand_cap` sampled shared-neighbour candidates,
    // score them, and keep the top-`k`. If that pool is thin (a low-degree node), top
    // up with a few random draws so refinement still has somewhere to walk.
    let cand_cap = (k * 8).max(k + 1);
    let mut lists = seed_descent_lists(n, k, sample, cand_cap, &context, &inverted, &mut rng);

    for round in 0..max_iters.max(1) {
        let updates = refine_descent_round(&mut lists, sample, k, &context, &mut rng);
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
    finish_similarity_pairs(
        graph,
        fold_descent_pairs(&lists, cutoff)
            .into_iter()
            .map(|((a, b), score)| (a, b, score)),
    )
}
