// CONCEPT:EG-KG.compute.node-similarity — Node similarity: Jaccard + cosine over neighbour sets.
// Neo4j GDS gds.nodeSimilarity parity.

#[path = "similarity/approximate.rs"]
mod approximate;
#[path = "similarity/exact.rs"]
mod exact;
#[path = "similarity/neighbors.rs"]
mod neighbors;
#[path = "similarity/types.rs"]
mod types;

pub use approximate::knn_similarity_approx;
pub use exact::{all_pairs_similarity, cosine_similarity, jaccard_similarity, knn_similarity};
pub use types::{Direction, KnnSimilarityApproxConfig, Metric, SimilarityPair};

use exact::finish_similarity_pairs;
use neighbors::{
    cosine_from_neighbors, jaccard_from_neighbors, neighbor_norm, neighbor_vec, prepared_neighbors,
};

#[cfg(test)]
mod tests {
    use super::super::graph::AdjacencyGraph;
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
            KnnSimilarityApproxConfig {
                metric: Metric::Jaccard,
                direction: Direction::Out,
                top_k: k,
                cutoff: 0.0,
                sample_rate: 0.5,
                max_iters: 30,
                delta: 0.001,
                seed: 42,
            },
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
                KnnSimilarityApproxConfig {
                    metric: Metric::Cosine,
                    direction: Direction::Out,
                    top_k: 6,
                    cutoff: 0.0,
                    sample_rate: 0.5,
                    max_iters: 20,
                    delta: 0.001,
                    seed: 7,
                },
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
            KnnSimilarityApproxConfig {
                metric: Metric::Jaccard,
                direction: Direction::Out,
                top_k: 5,
                cutoff: 0.0,
                sample_rate: 0.5,
                max_iters: 10,
                delta: 0.001,
                seed: 1,
            },
        );
        assert_eq!(pair_set(&exact), pair_set(&approx));
    }

    #[test]
    fn knn_approx_respects_cutoff() {
        let g = clustered_similarity_graph(15, 6, 5);
        let approx = knn_similarity_approx(
            &g,
            KnnSimilarityApproxConfig {
                metric: Metric::Jaccard,
                direction: Direction::Out,
                top_k: 8,
                cutoff: 0.5,
                sample_rate: 0.5,
                max_iters: 20,
                delta: 0.001,
                seed: 3,
            },
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
