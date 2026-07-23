// CONCEPT:EG-KG.graphlearn.node2vec — Node2Vec biased-walk + SGNS structural embeddings.
//
// Node2Vec (Grover & Leskovec 2016): sample second-order biased random walks over the
// graph (the `p`/`q` return/in-out knobs interpolate BFS-like structural-equivalence
// vs DFS-like homophily), treat each walk as a "sentence", and train skip-gram
// embeddings with negative sampling (SGNS) so nodes appearing in similar walk contexts
// land close in embedding space.
//
// Trained in the SAME idiom as the `graphlearn` KAN link-predictor: pure `Vec<f64>`,
// analytic SGNS gradients (no autodiff), the shared `datascience::training::adam_step`
// optimiser, and splitmix64 seeding — no torch/candle, no BLAS/GPU. Training is
// full-batch per epoch: each epoch STREAMS the walk corpus (walks are generated on the
// fly, their gradient accumulated, then discarded — never fully materialized), the
// mean gradient is taken, and ONE Adam step is applied. So memory is O(V·d) regardless
// of corpus size — the walk corpus is never held in memory.
//
// Determinism: walk sampling and negative sampling both run off a splitmix64 stream
// seeded from `config.seed` (+ the epoch), so a given graph + config is reproducible.
//
// Complexity: O(epochs · walks_per_node · V · walk_length · window · (1 + negatives) ·
// d) time; O(V · d) memory (embedding + context matrices and their Adam state).

use std::hash::Hash;

use crate::datascience::training::adam_step;
use crate::graph_algos::AdjacencyGraph;

use super::{l2_normalize_rows, SplitMix64};

/// Config for [`node2vec`] (CONCEPT:EG-KG.graphlearn.node2vec). Defaults are modest,
/// resident-graph-scale values (this is the lightweight in-engine embedder; heavy
/// large-corpus training is a data-science-mcp job).
#[derive(Debug, Clone)]
pub struct Node2VecConfig {
    /// Embedding dimension `d`.
    pub dim: usize,
    /// Steps per random walk.
    pub walk_length: usize,
    /// Walks started from each node per epoch.
    pub walks_per_node: usize,
    /// Skip-gram context radius (nodes within `±window` positions are context).
    pub window: usize,
    /// Return parameter `p`: `>1` discourages, `<1` encourages immediate backtracking.
    pub p: f64,
    /// In-out parameter `q`: `>1` biases toward local (BFS) structure, `<1` toward
    /// outward (DFS) exploration.
    pub q: f64,
    /// Negative samples drawn per positive (center, context) pair.
    pub negatives: usize,
    /// Full-batch training epochs (one streamed corpus pass + one Adam step each).
    pub epochs: usize,
    /// Adam learning rate.
    pub lr: f64,
    /// L2-normalize each final embedding row (so cosine/kNN is well-scaled).
    pub l2_normalize: bool,
    /// Deterministic seed for walk + negative sampling + initialisation.
    pub seed: u64,
}

impl Default for Node2VecConfig {
    fn default() -> Self {
        Self {
            dim: 128,
            walk_length: 40,
            walks_per_node: 10,
            window: 5,
            p: 1.0,
            q: 1.0,
            negatives: 5,
            epochs: 10,
            lr: 0.05,
            l2_normalize: true,
            seed: 42,
        }
    }
}

/// Compute Node2Vec embeddings for every node of `graph`, returned as rows in the
/// graph's compact-index order (`rows[i]` is the embedding of node index `i`).
/// CONCEPT:EG-KG.graphlearn.node2vec
pub fn node2vec<N>(graph: &AdjacencyGraph<N>, config: &Node2VecConfig) -> Vec<Vec<f32>>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let d = config.dim.max(1);
    if n == 0 {
        return Vec::new();
    }

    // Symmetric undirected adjacency (sorted per row) — the walk substrate.
    let adj = graph.undirected_weighted_adjacency();

    // Negative-sampling distribution ∝ degree^0.75 (word2vec unigram smoothing),
    // as a cumulative table for O(log V) sampling.
    let neg_cdf = negative_cdf(&adj);

    // Parameter matrices: input embeddings (the output) + context embeddings.
    let mut emb = init_matrix(n, d, config.seed ^ 0x1234_5678, 0.5);
    let mut ctx = vec![vec![0.0f64; d]; n];

    // Flattened Adam state over [emb ; ctx].
    let np = 2 * n * d;
    let mut m = vec![0.0f64; np];
    let mut vv = vec![0.0f64; np];

    for epoch in 0..config.epochs.max(1) {
        let mut g_emb = vec![vec![0.0f64; d]; n];
        let mut g_ctx = vec![vec![0.0f64; d]; n];
        let mut examples = 0u64;
        // Per-epoch RNG so re-sampled walks/negatives stay deterministic yet vary.
        let mut walk_rng = SplitMix64::new(
            config
                .seed
                .wrapping_add((epoch as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)),
        );
        let mut neg_rng = SplitMix64::new(config.seed ^ 0xD1B5_4A32_D192_ED03 ^ epoch as u64);

        // STREAM the corpus: generate each walk, accumulate its gradient, drop it.
        let mut walk = Vec::with_capacity(config.walk_length);
        for start in 0..n {
            if adj[start].is_empty() {
                continue; // no walk possible from an isolated node
            }
            for _ in 0..config.walks_per_node {
                sample_walk(&adj, start, config, &mut walk_rng, &mut walk);
                accumulate_walk_grad(
                    &walk,
                    &emb,
                    &ctx,
                    config,
                    &neg_cdf,
                    &mut neg_rng,
                    &mut g_emb,
                    &mut g_ctx,
                    &mut examples,
                );
            }
        }
        if examples == 0 {
            break;
        }

        // Mean gradient over the epoch's examples, then one Adam step on [emb ; ctx].
        let inv = 1.0 / examples as f64;
        let mut params = vec![0.0f64; np];
        let mut grads = vec![0.0f64; np];
        flatten_into(&emb, &ctx, &mut params);
        flatten_scaled_into(&g_emb, &g_ctx, inv, &mut grads);
        let step = adam_step(
            &params,
            &grads,
            &m,
            &vv,
            config.lr,
            0.9,
            0.999,
            1e-8,
            epoch as u64 + 1,
        );
        m = step.m;
        vv = step.v;
        unflatten(&step.params, &mut emb, &mut ctx);
    }

    if config.l2_normalize {
        l2_normalize_rows(&mut emb);
    }
    emb.into_iter()
        .map(|row| row.into_iter().map(|x| x as f32).collect())
        .collect()
}

// ─────────────────────────── biased walk ───────────────────────────

/// Sample one second-order biased walk of length `walk_length` starting at `start`
/// into `out` (cleared first). The Node2Vec transition from `prev`→`cur`→`x` weights
/// `x` by `w_cur,x · α`, where `α = 1/p` if `x==prev`, `1` if `x` neighbours `prev`
/// (distance 1), else `1/q`. CONCEPT:EG-KG.graphlearn.node2vec
fn sample_walk(
    adj: &[Vec<(usize, f64)>],
    start: usize,
    config: &Node2VecConfig,
    rng: &mut SplitMix64,
    out: &mut Vec<usize>,
) {
    out.clear();
    out.push(start);
    if config.walk_length <= 1 {
        return;
    }
    // First step: sample proportional to edge weight (no previous node yet).
    let mut cur = start;
    let mut prev: Option<usize> = None;
    for _ in 1..config.walk_length {
        let nbrs = &adj[cur];
        if nbrs.is_empty() {
            break;
        }
        let next = match prev {
            None => weighted_pick(nbrs, |_, w| w, rng),
            Some(t) => {
                let t_nbrs = &adj[t];
                weighted_pick(
                    nbrs,
                    |x, w| w * node2vec_alpha(x, t, t_nbrs, config.p, config.q),
                    rng,
                )
            }
        };
        match next {
            Some(x) => {
                out.push(x);
                prev = Some(cur);
                cur = x;
            }
            None => break,
        }
    }
}

/// The Node2Vec search-bias `α` for a candidate `x` given the walk's previous node
/// `t` (with `t`'s sorted neighbour list `t_nbrs`).
#[inline]
fn node2vec_alpha(x: usize, t: usize, t_nbrs: &[(usize, f64)], p: f64, q: f64) -> f64 {
    if x == t {
        1.0 / p
    } else if t_nbrs.binary_search_by_key(&x, |(i, _)| *i).is_ok() {
        1.0 // x is a neighbour of t (distance 1)
    } else {
        1.0 / q // distance 2
    }
}

/// Pick a neighbour index proportional to `weight(neighbor, edge_weight)`, seeded.
/// Returns `None` only if every weight is non-positive.
fn weighted_pick(
    nbrs: &[(usize, f64)],
    weight: impl Fn(usize, f64) -> f64,
    rng: &mut SplitMix64,
) -> Option<usize> {
    let mut total = 0.0;
    for &(x, w) in nbrs {
        let ww = weight(x, w);
        if ww > 0.0 {
            total += ww;
        }
    }
    if total <= 0.0 {
        return None;
    }
    let mut r = rng.next_f64() * total;
    for &(x, w) in nbrs {
        let ww = weight(x, w);
        if ww > 0.0 {
            r -= ww;
            if r <= 0.0 {
                return Some(x);
            }
        }
    }
    // Floating-point residue: fall back to the last positive-weight neighbour.
    nbrs.iter()
        .rev()
        .find(|&&(x, w)| weight(x, w) > 0.0)
        .map(|&(x, _)| x)
}

// ─────────────────────────── SGNS gradient ───────────────────────────

/// Accumulate the skip-gram-negative-sampling gradient of one walk into `g_emb`/
/// `g_ctx`. For each center `u` and each context `c` within `±window`, one positive
/// pair `(u, c)` plus `negatives` sampled non-context nodes contribute the analytic
/// SGNS gradient. CONCEPT:EG-KG.graphlearn.node2vec
#[allow(clippy::too_many_arguments)]
fn accumulate_walk_grad(
    walk: &[usize],
    emb: &[Vec<f64>],
    ctx: &[Vec<f64>],
    config: &Node2VecConfig,
    neg_cdf: &[f64],
    neg_rng: &mut SplitMix64,
    g_emb: &mut [Vec<f64>],
    g_ctx: &mut [Vec<f64>],
    examples: &mut u64,
) {
    let len = walk.len();
    for i in 0..len {
        let u = walk[i];
        let lo = i.saturating_sub(config.window);
        let hi = (i + config.window + 1).min(len);
        for (j, &c) in walk.iter().enumerate().take(hi).skip(lo) {
            if j == i {
                continue;
            }
            // Positive pair (u, c): dL/dz = σ(z) − 1.
            sgns_pair(u, c, 1.0, emb, ctx, g_emb, g_ctx);
            // Negatives: dL/dz = σ(z) (label 0).
            for _ in 0..config.negatives {
                let neg = sample_negative(neg_cdf, neg_rng);
                if neg == u {
                    continue;
                }
                sgns_pair(u, neg, 0.0, emb, ctx, g_emb, g_ctx);
            }
            *examples += 1;
        }
    }
}

/// One SGNS term: gradient of `−[label·log σ(z) + (1−label)·log σ(−z)]`, `z =
/// emb[u]·ctx[c]`. `dL/dz = σ(z) − label`; chain to `emb[u]` (·ctx[c]) and `ctx[c]`
/// (·emb[u]).
#[inline]
fn sgns_pair(
    u: usize,
    c: usize,
    label: f64,
    emb: &[Vec<f64>],
    ctx: &[Vec<f64>],
    g_emb: &mut [Vec<f64>],
    g_ctx: &mut [Vec<f64>],
) {
    let z: f64 = emb[u].iter().zip(ctx[c].iter()).map(|(a, b)| a * b).sum();
    let dz = sigmoid(z) - label;
    let (gu, gc) = (&mut g_emb[u], &mut g_ctx[c]);
    let (eu, ec) = (&emb[u], &ctx[c]);
    for ((gu_k, gc_k), (eu_k, ec_k)) in gu
        .iter_mut()
        .zip(gc.iter_mut())
        .zip(eu.iter().zip(ec.iter()))
    {
        *gu_k += dz * ec_k;
        *gc_k += dz * eu_k;
    }
}

// ─────────────────────────── helpers ───────────────────────────

#[inline]
fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

/// Cumulative `degree^0.75` table for negative sampling (unigram smoothing).
fn negative_cdf(adj: &[Vec<(usize, f64)>]) -> Vec<f64> {
    let mut cdf = Vec::with_capacity(adj.len());
    let mut acc = 0.0;
    for row in adj {
        let deg: f64 = row.iter().map(|(_, w)| *w).sum();
        acc += deg.max(0.0).powf(0.75);
        cdf.push(acc);
    }
    cdf
}

/// Sample a node index ∝ `degree^0.75` via binary search on the cumulative table.
fn sample_negative(cdf: &[f64], rng: &mut SplitMix64) -> usize {
    let total = cdf.last().copied().unwrap_or(0.0);
    if total <= 0.0 {
        return (rng.next_u64() % cdf.len().max(1) as u64) as usize;
    }
    let r = rng.next_f64() * total;
    match cdf.binary_search_by(|&c| c.partial_cmp(&r).unwrap_or(std::cmp::Ordering::Less)) {
        Ok(i) => i,
        Err(i) => i.min(cdf.len() - 1),
    }
}

/// Seeded `U(−scale/dim, scale/dim)` matrix init (word2vec-style small weights).
fn init_matrix(n: usize, d: usize, seed: u64, scale: f64) -> Vec<Vec<f64>> {
    let mut rng = SplitMix64::new(seed);
    let span = scale / d as f64;
    (0..n)
        .map(|_| {
            (0..d)
                .map(|_| (rng.next_f64() - 0.5) * 2.0 * span)
                .collect()
        })
        .collect()
}

fn flatten_into(emb: &[Vec<f64>], ctx: &[Vec<f64>], out: &mut [f64]) {
    let mut p = 0;
    for row in emb.iter().chain(ctx.iter()) {
        out[p..p + row.len()].copy_from_slice(row);
        p += row.len();
    }
}

fn flatten_scaled_into(g_emb: &[Vec<f64>], g_ctx: &[Vec<f64>], scale: f64, out: &mut [f64]) {
    let mut p = 0;
    for row in g_emb.iter().chain(g_ctx.iter()) {
        for (o, &g) in out[p..p + row.len()].iter_mut().zip(row.iter()) {
            *o = g * scale;
        }
        p += row.len();
    }
}

fn unflatten(flat: &[f64], emb: &mut [Vec<f64>], ctx: &mut [Vec<f64>]) {
    let mut p = 0;
    for row in emb.iter_mut().chain(ctx.iter_mut()) {
        let len = row.len();
        row.copy_from_slice(&flat[p..p + len]);
        p += len;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two disjoint triangles: same-triangle nodes co-occur in every walk, so their
    /// learned embeddings must be more cosine-similar than across triangles.
    #[test]
    fn node2vec_separates_two_triangles() {
        let g = AdjacencyGraph::from_unweighted_edges([
            (0u32, 1u32),
            (1, 2),
            (2, 0),
            (3, 4),
            (4, 5),
            (5, 3),
        ]);
        let cfg = Node2VecConfig {
            dim: 16,
            walk_length: 20,
            walks_per_node: 20,
            window: 3,
            epochs: 20,
            seed: 11,
            ..Default::default()
        };
        let emb = node2vec(&g, &cfg);
        assert_eq!(emb.len(), 6);
        let cos = |a: &[f32], b: &[f32]| super::super::cosine_f32(a, b);
        let intra = cos(&emb[0], &emb[1]);
        let inter = cos(&emb[0], &emb[3]);
        assert!(
            intra > inter,
            "intra-triangle cosine {intra} should exceed inter {inter}"
        );
    }

    /// Determinism: same seed ⇒ byte-identical embeddings.
    #[test]
    fn node2vec_is_deterministic() {
        let g = AdjacencyGraph::from_unweighted_edges([(0u32, 1u32), (1, 2), (2, 3), (3, 0)]);
        let cfg = Node2VecConfig {
            dim: 8,
            walks_per_node: 4,
            walk_length: 10,
            epochs: 3,
            seed: 5,
            ..Default::default()
        };
        assert_eq!(node2vec(&g, &cfg), node2vec(&g, &cfg));
    }

    /// The return parameter `p` biases the walk: a tiny `p` (cheap backtracking) makes
    /// walks revisit the previous node far more often than a large `p`.
    #[test]
    fn node2vec_return_param_changes_backtracking() {
        // Star: center 0 with leaves 1..4 — from a leaf the only non-return option is
        // back to the center, so backtracking pressure is observable on a path graph.
        let g = AdjacencyGraph::from_unweighted_edges([(0u32, 1u32), (1, 2), (2, 3), (3, 4)]);
        let count_backtracks = |p: f64| {
            let cfg = Node2VecConfig {
                walk_length: 30,
                walks_per_node: 50,
                p,
                q: 1.0,
                seed: 3,
                epochs: 0,
                ..Default::default()
            };
            let adj = g.undirected_weighted_adjacency();
            let mut rng = SplitMix64::new(cfg.seed);
            let mut walk = Vec::new();
            let mut back = 0u32;
            for start in 0..g.node_count() {
                for _ in 0..cfg.walks_per_node {
                    sample_walk(&adj, start, &cfg, &mut rng, &mut walk);
                    for w in walk.windows(3) {
                        if w[0] == w[2] {
                            back += 1;
                        }
                    }
                }
            }
            back
        };
        let low_p = count_backtracks(0.1); // cheap return ⇒ many backtracks
        let high_p = count_backtracks(10.0); // expensive return ⇒ few
        assert!(
            low_p > high_p,
            "small p should backtrack more: low_p={low_p} high_p={high_p}"
        );
    }
}
