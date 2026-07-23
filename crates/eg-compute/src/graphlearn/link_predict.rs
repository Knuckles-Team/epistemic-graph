// CONCEPT:EG-KG.graphlearn.link-predictor — a shallow KAN link-predictor.
//
// The realistic EpiGNN v1 (synergy report §3a-2): given a candidate node pair
// `(u, v)`, build a small feature vector from what is ALREADY resident — structural
// signals from `graph_algos` (common neighbours, Jaccard, Adamic-Adar, preferential
// attachment, PageRank product, neighbour cosine) plus a 1-hop-aggregated node
// feature dot (CONCEPT:EG-KG.graphlearn.neighbor-aggregation) — then score it with a
// 1–2 layer KAN: each layer output is a SUM of learnable `KanEdgeFn`s over its
// inputs (the exact KAN definition), down to a sigmoid link probability.
//
// Because the feature dimension is small (a handful of structural signals, not raw
// high-D embeddings) and the net is 1–2 layers, this is squarely the "small KANs"
// regime the KAN paper claims. Training is BATCH, one round-trip: observed edges are
// positives, sampled non-edges are negatives, K epochs of forward + analytic
// backward + Adam (reusing `datascience::training::adam_step`) run inside one engine
// call, returning the fitted [`KanLinkModel`] blob. In the default (hidden = 0)
// architecture there is exactly ONE `KanEdgeFn` per input feature, so the learned
// per-feature curve is directly inspectable — the interpretability differentiator.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::hash::Hash;

use crate::datascience::training::adam_step;
use crate::graph_algos::pagerank::{pagerank, PageRankConfig};
use crate::graph_algos::AdjacencyGraph;

use super::edge_fn::{Basis, KanEdgeFn};
use super::neighbor_aggregate::aggregate_1hop;

/// The structural features scored per candidate pair, in fixed order. The names are
/// serialized onto the `:EdgeFunction` write-back so each learned curve is queryable
/// by the feature it explains. CONCEPT:EG-KG.graphlearn.link-predictor
pub const FEATURE_NAMES: [&str; 7] = [
    "common_neighbors",
    "jaccard",
    "adamic_adar",
    "preferential_attachment",
    "pagerank_product",
    "neighbor_cosine",
    "node_feature_dot",
];

/// Number of per-pair structural features.
pub const N_FEATURES: usize = FEATURE_NAMES.len();

/// The two extra features contributed when a structural EMBEDDING channel is present
/// (CONCEPT:EG-KG.graphlearn.structural-embeddings): the dot product and cosine of the
/// candidate pair's embedding vectors. Appended after [`FEATURE_NAMES`] so the base
/// 7-feature model is byte-for-byte unchanged when no embeddings are supplied.
pub const EMBEDDING_FEATURE_NAMES: [&str; 2] = ["embedding_dot", "embedding_cosine"];

// ─────────────────────────── The model ───────────────────────────

/// One KAN layer: `out_dim` outputs, each `output[j] = bias[j] + Σ_i fns[j][i](x_i)`.
/// CONCEPT:EG-KG.graphlearn.link-predictor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KanLayer {
    pub in_dim: usize,
    pub out_dim: usize,
    /// `fns[j][i]` — the learnable edge function from input `i` to output `j`.
    pub fns: Vec<Vec<KanEdgeFn>>,
    pub bias: Vec<f64>,
}

impl KanLayer {
    fn new(in_dim: usize, out_dim: usize, basis: Basis, degree: usize) -> Self {
        let fns = (0..out_dim)
            .map(|_| {
                (0..in_dim)
                    .map(|_| KanEdgeFn::zeros(basis, degree))
                    .collect()
            })
            .collect();
        Self {
            in_dim,
            out_dim,
            fns,
            bias: vec![0.0; out_dim],
        }
    }

    fn forward(&self, x: &[f64]) -> Vec<f64> {
        (0..self.out_dim)
            .map(|j| {
                self.bias[j]
                    + (0..self.in_dim)
                        .map(|i| self.fns[j][i].eval(x[i]))
                        .sum::<f64>()
            })
            .collect()
    }
}

/// A fitted KAN link-predictor: a stack of `KanLayer`s (final `out_dim == 1`) over
/// z-standardised structural features, plus the standardisation stats. This is the
/// serializable blob `GraphLearnFit` returns and `GraphLearnPredict` consumes.
/// CONCEPT:EG-KG.graphlearn.link-predictor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KanLinkModel {
    pub basis: Basis,
    pub degree: usize,
    pub feature_names: Vec<String>,
    pub layers: Vec<KanLayer>,
    /// Per-feature mean used to z-standardise inputs before scoring.
    pub feat_mean: Vec<f64>,
    /// Per-feature std (floored at 1e-9) used to z-standardise inputs.
    pub feat_std: Vec<f64>,
    /// 1-hop neighbour-aggregation self-retention used at fit time — stored so
    /// `GraphLearnPredict` rebuilds the feature context identically.
    #[serde(default = "default_model_alpha")]
    pub alpha: f64,
    /// Training AUC recorded at fit time (positives vs sampled negatives).
    #[serde(default)]
    pub train_auc: f64,
}

fn default_model_alpha() -> f64 {
    0.5
}

impl KanLinkModel {
    /// Standardise a raw feature vector with the model's stored stats.
    fn standardize(&self, x: &[f64]) -> Vec<f64> {
        x.iter()
            .enumerate()
            .map(|(i, &v)| (v - self.feat_mean[i]) / self.feat_std[i])
            .collect()
    }

    /// Raw (pre-sigmoid) score of an already-standardised feature vector.
    fn score_std(&self, x_std: &[f64]) -> f64 {
        let mut a = x_std.to_vec();
        for layer in &self.layers {
            a = layer.forward(&a);
        }
        a[0]
    }

    /// Predicted link probability `σ(score)` for a RAW feature vector.
    pub fn predict_prob(&self, x_raw: &[f64]) -> f64 {
        sigmoid(self.score_std(&self.standardize(x_raw)))
    }
}

/// Training/config knobs for the KAN link-predictor.
/// CONCEPT:EG-KG.graphlearn.link-predictor
#[derive(Debug, Clone)]
pub struct KanLinkConfig {
    pub basis: Basis,
    pub degree: usize,
    /// Hidden width; `0` ⇒ a single layer (one `KanEdgeFn` per feature — the
    /// directly-interpretable default). `>0` ⇒ two layers.
    pub hidden: usize,
    pub epochs: usize,
    pub lr: f64,
    /// Negatives sampled per positive edge.
    pub neg_ratio: f64,
    pub seed: u64,
    /// 1-hop neighbour-aggregation self-retention for the node-feature channel.
    pub alpha: f64,
}

impl Default for KanLinkConfig {
    fn default() -> Self {
        Self {
            basis: Basis::Chebyshev,
            degree: 4,
            hidden: 0,
            epochs: 200,
            lr: 0.05,
            neg_ratio: 1.0,
            seed: 42,
            alpha: 0.5,
        }
    }
}

// ─────────────────────────── Feature context ───────────────────────────

/// Precomputed per-graph state for cheap per-pair feature construction: neighbour
/// sets, degrees, PageRank, and 1-hop-aggregated node features.
/// CONCEPT:EG-KG.graphlearn.link-predictor
pub struct FeatureCtx {
    /// Undirected neighbour set of each node index (sorted, deduped).
    neighbors: Vec<Vec<usize>>,
    /// Undirected degree of each node.
    degree: Vec<f64>,
    /// PageRank score of each node index.
    pr: Vec<f64>,
    /// 1-hop-aggregated node feature rows (parallel to node index).
    node_feats: Vec<Vec<f64>>,
    /// Optional per-node structural embedding rows (FastRP/Node2Vec). When present,
    /// `pair_features` appends the pair's embedding dot + cosine (2 extra features).
    /// CONCEPT:EG-KG.graphlearn.structural-embeddings
    embeddings: Option<Vec<Vec<f64>>>,
    n: usize,
}

impl FeatureCtx {
    /// Build the feature context over a graph. `alpha` controls the neighbour
    /// aggregation of the node-feature channel. CONCEPT:EG-KG.graphlearn.neighbor-aggregation
    pub fn build<N>(graph: &AdjacencyGraph<N>, alpha: f64) -> Self
    where
        N: Clone + Eq + Hash + Ord,
    {
        let n = graph.node_count();
        let neighbors: Vec<Vec<usize>> = (0..n).map(|i| graph.undirected_neighbors(i)).collect();
        let degree: Vec<f64> = neighbors.iter().map(|v| v.len() as f64).collect();
        let pr_res = pagerank(graph, &PageRankConfig::default());
        // pagerank returns (N, score) in node order; take scores by index.
        let pr: Vec<f64> = pr_res.scores.iter().map(|(_, s)| *s).collect();
        // Raw per-node features: [normalised degree, pagerank] → 1-hop aggregated.
        let max_deg = degree.iter().cloned().fold(1.0_f64, f64::max);
        let base: Vec<Vec<f64>> = (0..n).map(|i| vec![degree[i] / max_deg, pr[i]]).collect();
        let node_feats = aggregate_1hop(graph, &base, alpha);
        Self {
            neighbors,
            degree,
            pr,
            node_feats,
            embeddings: None,
            n,
        }
    }

    /// Build the feature context AND fold in a structural embedding channel: each
    /// pair additionally contributes its embedding dot + cosine (2 extra features).
    /// `embeddings` must be parallel to the graph's compact index order (one row per
    /// node). CONCEPT:EG-KG.graphlearn.structural-embeddings
    pub fn build_with_embeddings<N>(
        graph: &AdjacencyGraph<N>,
        alpha: f64,
        embeddings: Vec<Vec<f64>>,
    ) -> Self
    where
        N: Clone + Eq + Hash + Ord,
    {
        let mut ctx = Self::build(graph, alpha);
        if embeddings.len() == ctx.n {
            ctx.embeddings = Some(embeddings);
        }
        ctx
    }

    /// Number of per-pair features this context produces: 7 structural, or 9 when a
    /// structural embedding channel is present. CONCEPT:EG-KG.graphlearn.structural-embeddings
    pub fn n_features(&self) -> usize {
        if self.embeddings.is_some() {
            N_FEATURES + EMBEDDING_FEATURE_NAMES.len()
        } else {
            N_FEATURES
        }
    }

    /// The feature names in `pair_features` order (7 structural, + 2 embedding when a
    /// structural embedding channel is present).
    pub fn feature_names(&self) -> Vec<String> {
        let mut names: Vec<String> = FEATURE_NAMES.iter().map(|s| s.to_string()).collect();
        if self.embeddings.is_some() {
            names.extend(EMBEDDING_FEATURE_NAMES.iter().map(|s| s.to_string()));
        }
        names
    }

    /// The raw structural feature vector for the pair `(a, b)` (compact indices),
    /// in [`FEATURE_NAMES`] order. CONCEPT:EG-KG.graphlearn.link-predictor
    pub fn pair_features(&self, a: usize, b: usize) -> Vec<f64> {
        let na = &self.neighbors[a];
        let nb = &self.neighbors[b];
        let (common, union) = intersect_union(na, nb);
        let common_cnt = common.len() as f64;
        let jaccard = if union == 0 {
            0.0
        } else {
            common_cnt / union as f64
        };
        let adamic_adar: f64 = common
            .iter()
            .map(|&w| {
                let d = self.degree[w];
                if d > 1.0 {
                    1.0 / d.ln()
                } else {
                    0.0
                }
            })
            .sum();
        let pref_attach = self.degree[a] * self.degree[b];
        let pagerank_product = self.pr[a] * self.pr[b];
        let neighbor_cosine = {
            let denom = (self.degree[a] * self.degree[b]).sqrt();
            if denom > 0.0 {
                common_cnt / denom
            } else {
                0.0
            }
        };
        let node_feature_dot = self.node_feats[a]
            .iter()
            .zip(self.node_feats[b].iter())
            .map(|(x, y)| x * y)
            .sum();
        let mut feats = vec![
            common_cnt,
            jaccard,
            adamic_adar,
            pref_attach,
            pagerank_product,
            neighbor_cosine,
            node_feature_dot,
        ];
        // Structural-embedding channel (CONCEPT:EG-KG.graphlearn.structural-embeddings):
        // the pair's embedding dot + cosine, appended after the 7 structural features.
        if let Some(emb) = &self.embeddings {
            let (ea, eb) = (&emb[a], &emb[b]);
            let dot: f64 = ea.iter().zip(eb.iter()).map(|(x, y)| x * y).sum();
            let na: f64 = ea.iter().map(|x| x * x).sum::<f64>().sqrt();
            let nb: f64 = eb.iter().map(|x| x * x).sum::<f64>().sqrt();
            let cos = if na * nb > 0.0 { dot / (na * nb) } else { 0.0 };
            feats.push(dot);
            feats.push(cos);
        }
        feats
    }

    /// Node count of the underlying graph.
    pub fn node_count(&self) -> usize {
        self.n
    }
}

// ─────────────────────────── Fitting ───────────────────────────

/// Fit the KAN link-predictor over a graph: `positives` are observed undirected
/// edges (compact-index pairs); negatives are sampled non-edges. Returns the fitted
/// [`KanLinkModel`] (with its per-feature learned edge functions) + the recorded
/// training AUC. CONCEPT:EG-KG.graphlearn.link-predictor
pub fn fit_link_predictor(
    ctx: &FeatureCtx,
    positives: &[(usize, usize)],
    config: &KanLinkConfig,
) -> KanLinkModel {
    let n_nodes = ctx.node_count();
    // Canonicalise positives to (min, max) and dedupe.
    let pos_set: HashSet<(usize, usize)> = positives
        .iter()
        .filter(|(a, b)| a != b && *a < n_nodes && *b < n_nodes)
        .map(|&(a, b)| if a < b { (a, b) } else { (b, a) })
        .collect();
    let pos: Vec<(usize, usize)> = {
        let mut v: Vec<(usize, usize)> = pos_set.iter().copied().collect();
        v.sort_unstable();
        v
    };
    let n_neg = ((pos.len() as f64) * config.neg_ratio).round() as usize;
    let neg = sample_negatives(n_nodes, &pos_set, n_neg, config.seed);

    // Build the labelled training matrix.
    let mut raw: Vec<Vec<f64>> = Vec::with_capacity(pos.len() + neg.len());
    let mut labels: Vec<f64> = Vec::with_capacity(pos.len() + neg.len());
    for &(a, b) in &pos {
        raw.push(ctx.pair_features(a, b));
        labels.push(1.0);
    }
    for &(a, b) in &neg {
        raw.push(ctx.pair_features(a, b));
        labels.push(0.0);
    }

    // Standardisation stats.
    let (feat_mean, feat_std) = standardize_stats(&raw, ctx.n_features());
    let x_std: Vec<Vec<f64>> = raw
        .iter()
        .map(|r| {
            r.iter()
                .enumerate()
                .map(|(i, &v)| (v - feat_mean[i]) / feat_std[i])
                .collect()
        })
        .collect();

    // Build & init the layer stack (7 structural features, or 9 with embeddings).
    let mut layers = build_layers(ctx.n_features(), config);
    init_layers(&mut layers, config.seed);

    // Adam optimiser state over the flattened parameters.
    let mut params = flatten_params(&layers);
    let np = params.len();
    let mut m = vec![0.0; np];
    let mut v = vec![0.0; np];

    let batch = x_std.len().max(1) as f64;
    for epoch in 0..config.epochs {
        // Accumulate gradients over the full batch.
        let mut gacc = GradAccum::zeros(&layers);
        for (row, &y) in x_std.iter().zip(labels.iter()) {
            let (acts, score) = forward_all(&layers, row);
            let p = sigmoid(score);
            let dlds = p - y; // dL/dscore for sigmoid + BCE
            backward(&layers, &acts, dlds, &mut gacc);
        }
        let grad = gacc.flatten(1.0 / batch);
        let step = adam_step(
            &params,
            &grad,
            &m,
            &v,
            config.lr,
            0.9,
            0.999,
            1e-8,
            epoch as u64 + 1,
        );
        params = step.params;
        m = step.m;
        v = step.v;
        set_params(&mut layers, &params);
    }

    let mut model = KanLinkModel {
        basis: config.basis,
        degree: config.degree,
        feature_names: ctx.feature_names(),
        layers,
        feat_mean,
        feat_std,
        alpha: config.alpha,
        train_auc: 0.0,
    };
    // Record training AUC.
    let pos_scores: Vec<f64> = pos
        .iter()
        .map(|&(a, b)| model.predict_prob(&ctx.pair_features(a, b)))
        .collect();
    let neg_scores: Vec<f64> = neg
        .iter()
        .map(|&(a, b)| model.predict_prob(&ctx.pair_features(a, b)))
        .collect();
    model.train_auc = auc(&pos_scores, &neg_scores);
    model
}

/// Score a set of candidate pairs (compact indices) with a fitted model, returning
/// `(a, b, probability)` sorted by descending probability. CONCEPT:EG-KG.graphlearn.link-predictor
pub fn predict_pairs(
    model: &KanLinkModel,
    ctx: &FeatureCtx,
    pairs: &[(usize, usize)],
) -> Vec<(usize, usize, f64)> {
    let mut out: Vec<(usize, usize, f64)> = pairs
        .iter()
        .map(|&(a, b)| (a, b, model.predict_prob(&ctx.pair_features(a, b))))
        .collect();
    out.sort_by(|x, y| {
        y.2.partial_cmp(&x.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(x.0.cmp(&y.0))
            .then(x.1.cmp(&y.1))
    });
    out
}

/// Enumerate candidate non-edge pairs among all nodes, capped at `top_k` after
/// scoring (returns the highest-scoring missing links). A `top_k` of 0 scores every
/// non-edge. CONCEPT:EG-KG.graphlearn.predicted-edge-writeback
pub fn predict_missing_links(
    model: &KanLinkModel,
    ctx: &FeatureCtx,
    existing: &HashSet<(usize, usize)>,
    top_k: usize,
) -> Vec<(usize, usize, f64)> {
    let n = ctx.node_count();
    let mut candidates: Vec<(usize, usize)> = Vec::new();
    for a in 0..n {
        for b in (a + 1)..n {
            if !existing.contains(&(a, b)) {
                candidates.push((a, b));
            }
        }
    }
    let mut scored = predict_pairs(model, ctx, &candidates);
    if top_k > 0 && scored.len() > top_k {
        scored.truncate(top_k);
    }
    scored
}

// ─────────────────────────── Forward / backward ───────────────────────────

/// Forward pass caching each layer's input activation; returns `(activations, score)`
/// where `activations[l]` is the input to layer `l`.
fn forward_all(layers: &[KanLayer], input: &[f64]) -> (Vec<Vec<f64>>, f64) {
    let mut acts: Vec<Vec<f64>> = Vec::with_capacity(layers.len() + 1);
    acts.push(input.to_vec());
    let mut cur = input.to_vec();
    for layer in layers {
        cur = layer.forward(&cur);
        acts.push(cur.clone());
    }
    let score = acts.last().and_then(|a| a.first().copied()).unwrap_or(0.0);
    (acts, score)
}

/// Accumulated gradients mirroring the layer stack's parameter layout.
struct GradAccum {
    coeffs: Vec<Vec<Vec<Vec<f64>>>>, // [layer][j][i][k]
    bias: Vec<Vec<f64>>,             // [layer][j]
}

impl GradAccum {
    fn zeros(layers: &[KanLayer]) -> Self {
        let coeffs = layers
            .iter()
            .map(|l| {
                (0..l.out_dim)
                    .map(|j| {
                        (0..l.in_dim)
                            .map(|i| vec![0.0; l.fns[j][i].coeffs.len()])
                            .collect()
                    })
                    .collect()
            })
            .collect();
        let bias = layers.iter().map(|l| vec![0.0; l.out_dim]).collect();
        Self { coeffs, bias }
    }

    /// Flatten in the SAME order as [`flatten_params`], scaled by `scale`.
    fn flatten(&self, scale: f64) -> Vec<f64> {
        let mut out = Vec::new();
        for l in 0..self.coeffs.len() {
            for j in 0..self.coeffs[l].len() {
                for i in 0..self.coeffs[l][j].len() {
                    for &g in &self.coeffs[l][j][i] {
                        out.push(g * scale);
                    }
                }
            }
            for &gb in &self.bias[l] {
                out.push(gb * scale);
            }
        }
        out
    }
}

/// Backprop `dL/dscore` through the layer stack, accumulating into `gacc`.
fn backward(layers: &[KanLayer], acts: &[Vec<f64>], dlds: f64, gacc: &mut GradAccum) {
    // Output gradient for the last layer (out_dim == 1).
    let mut go = vec![dlds];
    for l in (0..layers.len()).rev() {
        let layer = &layers[l];
        let x_in = &acts[l];
        let mut grad_in = vec![0.0; layer.in_dim];
        for (j, &gj) in go.iter().enumerate() {
            gacc.bias[l][j] += gj;
            for i in 0..layer.in_dim {
                let f = &layer.fns[j][i];
                let bvals = f.grad_coeffs(x_in[i]);
                for (k, bv) in bvals.iter().enumerate() {
                    gacc.coeffs[l][j][i][k] += gj * bv;
                }
                grad_in[i] += gj * f.grad_input(x_in[i]);
            }
        }
        go = grad_in;
    }
}

// ─────────────────────────── Param (de)serialisation ───────────────────────────

fn build_layers(n_features: usize, config: &KanLinkConfig) -> Vec<KanLayer> {
    if config.hidden == 0 {
        vec![KanLayer::new(n_features, 1, config.basis, config.degree)]
    } else {
        vec![
            KanLayer::new(n_features, config.hidden, config.basis, config.degree),
            KanLayer::new(config.hidden, 1, config.basis, config.degree),
        ]
    }
}

/// Small seeded initialisation to break symmetry across edge functions.
fn init_layers(layers: &mut [KanLayer], seed: u64) {
    let mut rng = SplitMix64::new(seed ^ 0x9E37_79B9_7F4A_7C15);
    for layer in layers.iter_mut() {
        for j in 0..layer.out_dim {
            for i in 0..layer.in_dim {
                for c in layer.fns[j][i].coeffs.iter_mut() {
                    *c = (rng.next_f64() - 0.5) * 0.2; // U(-0.1, 0.1)
                }
            }
        }
    }
}

fn flatten_params(layers: &[KanLayer]) -> Vec<f64> {
    let mut out = Vec::new();
    for layer in layers {
        for j in 0..layer.out_dim {
            for i in 0..layer.in_dim {
                out.extend_from_slice(&layer.fns[j][i].coeffs);
            }
        }
        out.extend_from_slice(&layer.bias);
    }
    out
}

fn set_params(layers: &mut [KanLayer], flat: &[f64]) {
    let mut p = 0usize;
    for layer in layers.iter_mut() {
        for j in 0..layer.out_dim {
            for i in 0..layer.in_dim {
                let len = layer.fns[j][i].coeffs.len();
                layer.fns[j][i].coeffs.copy_from_slice(&flat[p..p + len]);
                p += len;
            }
        }
        for j in 0..layer.out_dim {
            layer.bias[j] = flat[p];
            p += 1;
        }
    }
}

// ─────────────────────────── Helpers ───────────────────────────

#[inline]
fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

/// Intersection (as an index list) + union size of two sorted, deduped index lists.
fn intersect_union(a: &[usize], b: &[usize]) -> (Vec<usize>, usize) {
    let (mut i, mut j) = (0, 0);
    let mut inter = Vec::new();
    let mut union = 0usize;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                union += 1;
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                union += 1;
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                inter.push(a[i]);
                union += 1;
                i += 1;
                j += 1;
            }
        }
    }
    union += (a.len() - i) + (b.len() - j);
    (inter, union)
}

/// Per-column mean and std (floored at 1e-9) of a feature matrix. `n_features` is the
/// expected column count, used only for the empty-input fallback.
fn standardize_stats(rows: &[Vec<f64>], n_features: usize) -> (Vec<f64>, Vec<f64>) {
    if rows.is_empty() {
        return (vec![0.0; n_features], vec![1.0; n_features]);
    }
    let d = rows[0].len();
    let n = rows.len() as f64;
    let mut mean = vec![0.0; d];
    for r in rows {
        for k in 0..d {
            mean[k] += r[k];
        }
    }
    for m in mean.iter_mut() {
        *m /= n;
    }
    let mut var = vec![0.0; d];
    for r in rows {
        for k in 0..d {
            let dv = r[k] - mean[k];
            var[k] += dv * dv;
        }
    }
    let std: Vec<f64> = var.iter().map(|v| (v / n).sqrt().max(1e-9)).collect();
    (mean, std)
}

/// Sample up to `count` unordered non-edge pairs not present in `existing`, seeded.
fn sample_negatives(
    n: usize,
    existing: &HashSet<(usize, usize)>,
    count: usize,
    seed: u64,
) -> Vec<(usize, usize)> {
    let mut rng = SplitMix64::new(seed);
    let mut out: Vec<(usize, usize)> = Vec::with_capacity(count);
    let mut seen: HashSet<(usize, usize)> = HashSet::new();
    if n < 2 {
        return out;
    }
    let max_tries = count.saturating_mul(50).max(1000);
    let mut tries = 0;
    while out.len() < count && tries < max_tries {
        tries += 1;
        let a = (rng.next_u64() % n as u64) as usize;
        let b = (rng.next_u64() % n as u64) as usize;
        if a == b {
            continue;
        }
        let key = if a < b { (a, b) } else { (b, a) };
        if existing.contains(&key) || seen.contains(&key) {
            continue;
        }
        seen.insert(key);
        out.push(key);
    }
    out
}

/// AUC (Mann-Whitney): probability a random positive outranks a random negative.
/// CONCEPT:EG-KG.graphlearn.link-predictor
pub fn auc(pos: &[f64], neg: &[f64]) -> f64 {
    if pos.is_empty() || neg.is_empty() {
        return 0.5;
    }
    let mut wins = 0.0;
    for &p in pos {
        for &q in neg {
            if p > q {
                wins += 1.0;
            } else if (p - q).abs() < 1e-12 {
                wins += 0.5;
            }
        }
    }
    wins / (pos.len() as f64 * neg.len() as f64)
}

/// Deterministic SplitMix64 PRNG (same family the mining GMM uses for k-means++).
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A seeded stochastic block model: `k` communities of `size` nodes, dense
    /// within, sparse across. Intra-community node pairs share many neighbours, so
    /// the planted latent rule ("links form between nodes sharing common neighbours")
    /// is recoverable. Returns the undirected edge list (compact indices) + node count.
    fn planted_sbm(
        k: usize,
        size: usize,
        p_in: f64,
        p_out: f64,
        seed: u64,
    ) -> (usize, Vec<(usize, usize)>) {
        let n = k * size;
        let mut rng = SplitMix64::new(seed);
        let community = |i: usize| i / size;
        let mut edges = Vec::new();
        for a in 0..n {
            for b in (a + 1)..n {
                let p = if community(a) == community(b) {
                    p_in
                } else {
                    p_out
                };
                if rng.next_f64() < p {
                    edges.push((a, b));
                }
            }
        }
        (n, edges)
    }

    #[test]
    fn planted_graph_auc_above_threshold() {
        // 4 communities of 10 (40 nodes), dense intra / sparse inter.
        let (n, all_edges) = planted_sbm(4, 10, 0.55, 0.02, 7);
        assert!(all_edges.len() > 50, "sbm too sparse: {}", all_edges.len());

        // Hold out ~20% of the edges as test positives.
        let mut rng = SplitMix64::new(999);
        let mut train: Vec<(usize, usize)> = Vec::new();
        let mut test_pos: Vec<(usize, usize)> = Vec::new();
        for &e in &all_edges {
            if rng.next_f64() < 0.2 {
                test_pos.push(e);
            } else {
                train.push(e);
            }
        }
        // Build the graph from TRAINING edges only (no test leakage).
        let g = AdjacencyGraph::from_adjacency(
            (0..n)
                .map(|i| {
                    let nbrs: Vec<(usize, f64)> = train
                        .iter()
                        .filter_map(|&(a, b)| {
                            if a == i {
                                Some((b, 1.0))
                            } else if b == i {
                                Some((a, 1.0))
                            } else {
                                None
                            }
                        })
                        .collect();
                    (i, nbrs)
                })
                .collect::<Vec<_>>(),
        );
        // The graph is over usize node ids 0..n in sorted order ⇒ index == id.
        let ctx = FeatureCtx::build(&g, 0.5);
        let model = fit_link_predictor(&ctx, &train, &KanLinkConfig::default());

        // Evaluate held-out positives vs random non-edge negatives.
        let train_set: HashSet<(usize, usize)> = train
            .iter()
            .map(|&(a, b)| if a < b { (a, b) } else { (b, a) })
            .collect();
        let all_set: HashSet<(usize, usize)> = all_edges
            .iter()
            .map(|&(a, b)| if a < b { (a, b) } else { (b, a) })
            .collect();
        let neg = sample_negatives(n, &all_set, test_pos.len().max(20), 12345);

        let pos_scores: Vec<f64> = test_pos
            .iter()
            .map(|&(a, b)| model.predict_prob(&ctx.pair_features(a, b)))
            .collect();
        let neg_scores: Vec<f64> = neg
            .iter()
            .map(|&(a, b)| model.predict_prob(&ctx.pair_features(a, b)))
            .collect();
        let test_auc = auc(&pos_scores, &neg_scores);
        assert!(
            test_auc > 0.7,
            "held-out AUC {test_auc} below 0.7 (train_auc {})",
            model.train_auc
        );
        // The training graph must not include the held-out edges (sanity).
        assert!(train_set.len() < all_set.len());
    }

    #[test]
    fn learned_edge_functions_are_recoverable() {
        // A tiny two-triangle graph; fit and confirm the per-feature edge functions
        // are queryable (one KanEdgeFn per feature in the default 1-layer model) and
        // carry non-trivial learned coefficients.
        let (n, edges) = planted_sbm(3, 8, 0.6, 0.03, 3);
        let g = AdjacencyGraph::from_adjacency(
            (0..n)
                .map(|i| {
                    let nbrs: Vec<(usize, f64)> = edges
                        .iter()
                        .filter_map(|&(a, b)| {
                            if a == i {
                                Some((b, 1.0))
                            } else if b == i {
                                Some((a, 1.0))
                            } else {
                                None
                            }
                        })
                        .collect();
                    (i, nbrs)
                })
                .collect::<Vec<_>>(),
        );
        let ctx = FeatureCtx::build(&g, 0.5);
        let model = fit_link_predictor(&ctx, &edges, &KanLinkConfig::default());

        // Default hidden=0 ⇒ exactly one layer, one edge fn per feature.
        assert_eq!(model.layers.len(), 1);
        assert_eq!(model.layers[0].fns[0].len(), N_FEATURES);
        assert_eq!(model.feature_names.len(), N_FEATURES);
        // At least one structural-overlap feature must carry a learned, non-zero curve.
        let mut max_norm = 0.0f64;
        for i in 0..N_FEATURES {
            let norm: f64 = model.layers[0].fns[0][i].coeffs.iter().map(|c| c * c).sum();
            max_norm = max_norm.max(norm);
        }
        assert!(max_norm > 1e-4, "no feature learned a non-trivial curve");
        // Serde round-trip proves the model (incl. edge functions) is a portable blob.
        let blob = serde_json::to_value(&model).unwrap();
        let back: KanLinkModel = serde_json::from_value(blob).unwrap();
        assert_eq!(back.feature_names, model.feature_names);
    }

    #[test]
    fn auc_perfect_and_random() {
        assert!((auc(&[0.9, 0.8], &[0.1, 0.2]) - 1.0).abs() < 1e-12);
        assert!((auc(&[0.5], &[0.5]) - 0.5).abs() < 1e-12);
    }
}
