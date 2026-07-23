// CONCEPT:EG-KG.graphlearn.structural-embeddings — native structural node embeddings.
//
// The engine's ANN vectors have historically come from EXTERNAL embedders. This module
// adds two dependency-free, deterministic STRUCTURAL embedders that run over the
// resident graph itself, so a graph can be vector-searched by topology alone:
//
//   * `fastrp`   (CONCEPT:EG-KG.graphlearn.fastrp)   — training-free iterated sparse
//     random projections with degree normalization (fast, the priority path).
//   * `node2vec` (CONCEPT:EG-KG.graphlearn.node2vec) — biased second-order walks + SGNS,
//     trained with the same analytic-gradient / batch-Adam idiom as the KAN
//     link-predictor.
//
// Both return rows in the graph's compact-index order and feed three consumers:
//   (a) the graph's `SemanticStore` (this module's [`NodeEmbeddings::write_to_store`] →
//       `add_embedding`), so kNN / vector search consume structural vectors;
//   (b) the KAN link-predictor feature builder (embedding dot + cosine — see
//       `super::link_predict::FeatureCtx::build_with_embeddings`);
//   (c) the `CALL gds.fastRP` / `CALL gds.node2vec` Cypher procedures (in `eg-query`).

pub mod fastrp;
pub mod node2vec;

pub use fastrp::{fastrp, FastRpConfig};
pub use node2vec::{node2vec, Node2VecConfig};

use std::fmt::Display;
use std::hash::Hash;

use crate::compute::semantic::SemanticStore;
use crate::graph_algos::AdjacencyGraph;

/// A batch of structural embeddings tied to their string node ids — the boundary
/// object between the pure index-ordered algorithm output and the id-keyed
/// `SemanticStore`. CONCEPT:EG-KG.graphlearn.structural-embeddings
#[derive(Debug, Clone)]
pub struct NodeEmbeddings {
    /// Node id per row (compact-index order).
    pub ids: Vec<String>,
    /// Embedding dimension.
    pub dim: usize,
    /// One `f32` embedding row per node (store-native precision).
    pub rows: Vec<Vec<f32>>,
}

impl NodeEmbeddings {
    /// Pair index-ordered embedding `rows` with the graph's node ids (stringified).
    /// CONCEPT:EG-KG.graphlearn.structural-embeddings
    pub fn from_graph<N>(graph: &AdjacencyGraph<N>, rows: Vec<Vec<f32>>) -> Self
    where
        N: Clone + Eq + Hash + Ord + Display,
    {
        let dim = rows.first().map(Vec::len).unwrap_or(0);
        let ids = graph.nodes().iter().map(|n| n.to_string()).collect();
        Self { ids, dim, rows }
    }

    /// Write every embedding into `store` via the `SemanticStore` write API
    /// (`add_embedding`), so subsequent `semantic_search` / kNN queries rank over these
    /// structural vectors. Returns the number of embeddings written.
    /// CONCEPT:EG-KG.graphlearn.structural-embeddings
    pub fn write_to_store(&self, store: &mut SemanticStore) -> usize {
        for (id, row) in self.ids.iter().zip(self.rows.iter()) {
            store.add_embedding(id.clone(), row.clone());
        }
        self.ids.len()
    }

    /// The embedding rows as `f64` (for the KAN feature builder).
    pub fn rows_f64(&self) -> Vec<Vec<f64>> {
        to_f64_rows(&self.rows)
    }
}

/// Convert `f32` embedding rows to `f64` (KAN features compute in `f64`).
pub fn to_f64_rows(rows: &[Vec<f32>]) -> Vec<Vec<f64>> {
    rows.iter()
        .map(|r| r.iter().map(|&x| x as f64).collect())
        .collect()
}

/// Cosine similarity of two `f32` vectors (0.0 when either is the zero vector).
pub fn cosine_f32(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    let denom = na.sqrt() * nb.sqrt();
    if denom > 0.0 {
        dot / denom
    } else {
        0.0
    }
}

/// Dot product of two `f64` vectors.
#[inline]
pub fn dot_f64(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

/// Cosine similarity of two `f64` vectors (0.0 when either is the zero vector).
pub fn cosine_f64(a: &[f64], b: &[f64]) -> f64 {
    let dot = dot_f64(a, b);
    let na: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
    let nb: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();
    let denom = na * nb;
    if denom > 0.0 {
        dot / denom
    } else {
        0.0
    }
}

/// L2-normalize each row in place (a zero row is left untouched).
pub(crate) fn l2_normalize_rows(rows: &mut [Vec<f64>]) {
    for row in rows.iter_mut() {
        let norm: f64 = row.iter().map(|x| x * x).sum::<f64>().sqrt();
        if norm > 0.0 {
            for x in row.iter_mut() {
                *x /= norm;
            }
        }
    }
}

/// Deterministic SplitMix64 PRNG — the same family the mining GMM and the KAN
/// link-predictor use, replicated here so the embedders share one seeding idiom.
pub(crate) struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub(crate) fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    pub(crate) fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    pub(crate) fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graphlearn::link_predict::{auc, fit_link_predictor, FeatureCtx, KanLinkConfig};
    use std::collections::HashSet;

    /// A seeded planted-partition (stochastic block model): `k` communities of `size`
    /// nodes, dense within (`p_in`) and sparse across (`p_out`). This is the track-g
    /// fixture style — reproduced here at 70 nodes (5×14, the track-g live-e2e size).
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

    /// Build an undirected `AdjacencyGraph<usize>` (index == id) from an edge list.
    fn graph_from(n: usize, edges: &[(usize, usize)]) -> AdjacencyGraph<usize> {
        AdjacencyGraph::from_adjacency(
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
        )
    }

    /// Sample `count` non-edge pairs not in `existing`, seeded (test-local).
    fn sample_negatives(
        n: usize,
        existing: &HashSet<(usize, usize)>,
        count: usize,
        seed: u64,
    ) -> Vec<(usize, usize)> {
        let mut rng = SplitMix64::new(seed);
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        let mut tries = 0;
        while out.len() < count && tries < count * 100 + 1000 {
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

    /// (train, test) edge partitions produced by `split_edges`.
    type EdgeSplit = (Vec<(usize, usize)>, Vec<(usize, usize)>);

    /// 80/20 train/test split of the edge set, seeded.
    fn split_edges(edges: &[(usize, usize)], seed: u64) -> EdgeSplit {
        let mut rng = SplitMix64::new(seed);
        let (mut train, mut test) = (Vec::new(), Vec::new());
        for &e in edges {
            if rng.next_f64() < 0.2 {
                test.push(e);
            } else {
                train.push(e);
            }
        }
        (train, test)
    }

    /// Held-out AUC of a fitted model over test positives vs random negatives.
    fn held_out_auc(
        model: &super::super::link_predict::KanLinkModel,
        ctx: &FeatureCtx,
        test_pos: &[(usize, usize)],
        neg: &[(usize, usize)],
    ) -> f64 {
        let pos: Vec<f64> = test_pos
            .iter()
            .map(|&(a, b)| model.predict_prob(&ctx.pair_features(a, b)))
            .collect();
        let ns: Vec<f64> = neg
            .iter()
            .map(|&(a, b)| model.predict_prob(&ctx.pair_features(a, b)))
            .collect();
        auc(&pos, &ns)
    }

    /// Held-out AUC averaged over several negative-sample seeds (variance reduction:
    /// a single small negative sample makes AUC coarse/noisy).
    fn avg_held_out_auc(
        model: &super::super::link_predict::KanLinkModel,
        ctx: &FeatureCtx,
        test_pos: &[(usize, usize)],
        n: usize,
        all_set: &HashSet<(usize, usize)>,
    ) -> f64 {
        let mut sum = 0.0;
        let seeds = [12345u64, 222, 9001];
        for &sd in &seeds {
            let neg = sample_negatives(n, all_set, test_pos.len() * 4, sd);
            sum += held_out_auc(model, ctx, test_pos, &neg);
        }
        sum / seeds.len() as f64
    }

    /// Held-out positives whose endpoints share ZERO common neighbours in the training
    /// graph: the structural overlap features (common-neighbours / Jaccard / Adamic-Adar
    /// / neighbour-cosine) are all 0 for these, so they are the HARD cases where the
    /// baseline is blind and a structural embedding channel is the only signal that can
    /// rank them. (`pair_features(a,b)[0]` is `common_neighbors`.)
    fn hard_pairs(ctx: &FeatureCtx, test_pos: &[(usize, usize)]) -> Vec<(usize, usize)> {
        test_pos
            .iter()
            .copied()
            .filter(|&(a, b)| ctx.pair_features(a, b)[0] == 0.0)
            .collect()
    }

    /// FastRP features for the KAN: computed on the TRAINING graph only (no test-edge
    /// leakage) and fed UN-normalized, so `embedding_dot` (magnitude-aware) and
    /// `embedding_cosine` (direction) are two distinct signals.
    fn feature_embeddings(g: &AdjacencyGraph<usize>) -> Vec<Vec<f64>> {
        to_f64_rows(&fastrp(
            g,
            &FastRpConfig {
                dim: 64,
                iterations: 4,
                seed: 7,
                l2_normalize: false,
                ..Default::default()
            },
        ))
    }

    /// THE VALIDATION (task acceptance): on the planted-partition fixture the KAN with
    /// FastRP embedding features beats the 7-structural-feature baseline's held-out
    /// link-prediction AUC. The fixture is deliberately SPARSE (low `p_in`) so most
    /// within-community node pairs share few/no direct neighbours — the structural
    /// overlap features are then weak, and FastRP's multi-hop community signal is what
    /// closes the gap. AUC is averaged over several negative-sample seeds (variance
    /// reduction). Reported on the FULL held-out set and the HARD (no-shared-neighbour)
    /// subset. Deterministic; all four numbers are printed.
    #[test]
    fn embedding_features_beat_structural_baseline_auc() {
        let (n, all_edges) = planted_sbm(6, 25, 0.17, 0.008, 7);
        assert!(all_edges.len() > 120, "sbm too sparse: {}", all_edges.len());
        let (train, test_pos) = split_edges(&all_edges, 999);
        let g = graph_from(n, &train);
        let all_set: HashSet<(usize, usize)> = all_edges
            .iter()
            .map(|&(a, b)| if a < b { (a, b) } else { (b, a) })
            .collect();

        // Baseline: the 7 structural features.
        let ctx_base = FeatureCtx::build(&g, 0.5);
        let model_base = fit_link_predictor(&ctx_base, &train, &KanLinkConfig::default());
        // Embeddings: 7 structural + FastRP dot/cosine (9 features).
        let ctx_emb = FeatureCtx::build_with_embeddings(&g, 0.5, feature_embeddings(&g));
        let model_emb = fit_link_predictor(&ctx_emb, &train, &KanLinkConfig::default());

        let base_full = avg_held_out_auc(&model_base, &ctx_base, &test_pos, n, &all_set);
        let emb_full = avg_held_out_auc(&model_emb, &ctx_emb, &test_pos, n, &all_set);
        let hard = hard_pairs(&ctx_base, &test_pos);
        assert!(
            hard.len() >= 10,
            "not enough hard test pairs: {}",
            hard.len()
        );
        let base_hard = avg_held_out_auc(&model_base, &ctx_base, &hard, n, &all_set);
        let emb_hard = avg_held_out_auc(&model_emb, &ctx_emb, &hard, n, &all_set);

        println!("[full] baseline_auc={base_full:.4} embedding_auc={emb_full:.4}");
        println!(
            "[hard n={}] baseline_auc={base_hard:.4} embedding_auc={emb_hard:.4}",
            hard.len()
        );
        assert!(
            base_full > 0.5,
            "baseline AUC {base_full} not better than chance"
        );
        // On the hard subset the structural overlap features are blind ⇒ embeddings win.
        assert!(
            emb_hard > base_hard,
            "on hard (no-shared-neighbour) pairs embeddings {emb_hard} must beat baseline {base_hard}"
        );
        // On the full held-out set embeddings beat the baseline too.
        assert!(
            emb_full > base_full,
            "embedding-augmented full-set AUC {emb_full} must beat baseline {base_full}"
        );
    }

    /// The Node2Vec feature variant also beats (or matches) the structural baseline on
    /// the same planted fixture — its biased walks capture the community signal.
    #[test]
    fn node2vec_features_match_or_beat_baseline_auc() {
        let (n, all_edges) = planted_sbm(6, 25, 0.17, 0.008, 7);
        let (train, test_pos) = split_edges(&all_edges, 999);
        let g = graph_from(n, &train);
        let all_set: HashSet<(usize, usize)> = all_edges
            .iter()
            .map(|&(a, b)| if a < b { (a, b) } else { (b, a) })
            .collect();

        let ctx_base = FeatureCtx::build(&g, 0.5);
        let model_base = fit_link_predictor(&ctx_base, &train, &KanLinkConfig::default());
        let base_full = avg_held_out_auc(&model_base, &ctx_base, &test_pos, n, &all_set);

        let emb = node2vec(
            &g,
            &Node2VecConfig {
                dim: 64,
                walk_length: 40,
                walks_per_node: 10,
                window: 5,
                epochs: 10,
                seed: 7,
                l2_normalize: false,
                ..Default::default()
            },
        );
        let ctx_emb = FeatureCtx::build_with_embeddings(&g, 0.5, to_f64_rows(&emb));
        let model_emb = fit_link_predictor(&ctx_emb, &train, &KanLinkConfig::default());
        let emb_full = avg_held_out_auc(&model_emb, &ctx_emb, &test_pos, n, &all_set);

        let hard = hard_pairs(&ctx_base, &test_pos);
        let base_hard = avg_held_out_auc(&model_base, &ctx_base, &hard, n, &all_set);
        let emb_hard = avg_held_out_auc(&model_emb, &ctx_emb, &hard, n, &all_set);
        println!("[full] baseline_auc={base_full:.4} node2vec_auc={emb_full:.4}");
        println!(
            "[hard n={}] baseline_auc={base_hard:.4} node2vec_auc={emb_hard:.4}",
            hard.len()
        );
        assert!(
            emb_full >= base_full,
            "node2vec-augmented AUC {emb_full} below baseline {base_full}"
        );
    }

    /// FastRP neighborhood-preservation sanity: on the planted graph, the mean cosine
    /// of intra-community node pairs is statistically higher than random pairs.
    #[test]
    fn fastrp_preserves_neighborhood_structure() {
        let (n, edges) = planted_sbm(5, 14, 0.32, 0.015, 7);
        let g = graph_from(n, &edges);
        let emb = fastrp(
            &g,
            &FastRpConfig {
                dim: 64,
                seed: 7,
                ..Default::default()
            },
        );
        let community = |i: usize| i / 14;

        // Mean cosine within communities vs across communities.
        let (mut intra_sum, mut intra_cnt) = (0.0f64, 0u32);
        let (mut inter_sum, mut inter_cnt) = (0.0f64, 0u32);
        for a in 0..n {
            for b in (a + 1)..n {
                let c = cosine_f32(&emb[a], &emb[b]) as f64;
                if community(a) == community(b) {
                    intra_sum += c;
                    intra_cnt += 1;
                } else {
                    inter_sum += c;
                    inter_cnt += 1;
                }
            }
        }
        let intra_mean = intra_sum / intra_cnt as f64;
        let inter_mean = inter_sum / inter_cnt as f64;
        println!("intra_mean_cos={intra_mean:.4} inter_mean_cos={inter_mean:.4}");
        assert!(
            intra_mean > inter_mean + 0.1,
            "nearby (same-community) pairs {intra_mean} must be clearly more similar than random {inter_mean}"
        );
    }

    /// INTEGRATION PROOF (a): FastRP embeddings written into a live `SemanticStore` are
    /// consumed by `semantic_search` (kNN) — a node's nearest neighbours by vector
    /// search are its own community, proving the write API round-trips into ANN search.
    #[test]
    fn embeddings_write_into_semantic_store_and_knn_search() {
        let (n, edges) = planted_sbm(5, 14, 0.35, 0.01, 7);
        let g = graph_from(n, &edges);
        let rows = fastrp(
            &g,
            &FastRpConfig {
                dim: 64,
                seed: 7,
                ..Default::default()
            },
        );
        let ne = NodeEmbeddings::from_graph(&g, rows);

        let mut store = SemanticStore::new();
        let written = ne.write_to_store(&mut store);
        assert_eq!(written, n);
        assert_eq!(store.len(), n);
        assert_eq!(store.dim(), 64);

        // kNN of node 0 (community 0 = ids 0..14): its top matches must be same-community.
        let query = store
            .get_embedding("0")
            .expect("embedding present in store");
        let hits = store.semantic_search(&query, 6);
        assert!(!hits.is_empty());
        let same_comm = hits
            .iter()
            .filter(|(id, _)| id.parse::<usize>().map(|i| i / 14 == 0).unwrap_or(false))
            .count();
        assert!(
            same_comm * 2 >= hits.len(),
            "most kNN hits for node 0 should be same-community; got {same_comm}/{}",
            hits.len()
        );
    }
}
