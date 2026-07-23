// CONCEPT:EG-KG.graphlearn.fastrp — Fast Random Projection structural node embeddings.
//
// FastRP (Chen et al. 2019, "Fast and Accurate Network Embeddings via Very Sparse
// Random Projection") — a training-free structural embedding: seed every node with a
// very-sparse random projection row, then repeatedly diffuse it over the
// degree-normalized adjacency and take a per-iteration-weighted sum. Nodes with
// similar multi-hop neighbourhoods land close in embedding space, so kNN/cosine over
// the output recovers structural roles and community membership.
//
// Deliberately dependency-free and in the same pure-`Vec<f64>` + splitmix64 idiom as
// the rest of `graphlearn` (no ndarray/nalgebra, no BLAS/GPU) — the numeric work is a
// sparse mat-vec plus scaled vector adds, so it needs no linear-algebra kernel. The
// output rows are `f32` to feed the engine's `SemanticStore` directly.
//
// Determinism: the projection matrix is drawn from a per-node-seeded splitmix64
// stream (`seed ⊕ mix(node_index)`), so the result depends ONLY on the graph + config
// + seed — never on iteration order or parallelism.
//
// Complexity: O(iterations · (V + E) · d) time; O(V · d) memory (three V×d buffers:
// the current/previous propagation state and the weighted-sum accumulator).

use std::hash::Hash;

use crate::graph_algos::AdjacencyGraph;

use super::{l2_normalize_rows, SplitMix64};

/// Config for [`fastrp`] (CONCEPT:EG-KG.graphlearn.fastrp). Defaults follow the common
/// FastRP practice: 128-dim, 3 diffusion iterations at equal weight, GCN-style
/// symmetric degree normalization, sparsity-3 projection, L2-normalized output.
#[derive(Debug, Clone)]
pub struct FastRpConfig {
    /// Embedding dimension `d`.
    pub dim: usize,
    /// Number of diffusion iterations `k` (hops mixed in).
    pub iterations: usize,
    /// Per-iteration weights applied to `n_1 … n_k` in the final sum. If empty (the
    /// default), every iteration is weighted `1.0`. When non-empty its length must
    /// equal `iterations` (extra entries ignored, missing entries treated as `0`).
    pub iteration_weights: Vec<f64>,
    /// Symmetric degree-normalization exponent `s`: each neighbour contribution is
    /// scaled by `deg(v)^{-s} · deg(u)^{-s}`. `0.5` ⇒ GCN-style `D^{-1/2} A D^{-1/2}`
    /// (spectrally bounded); `0.0` ⇒ raw neighbour sum.
    pub normalization_strength: f64,
    /// Sparsity `s` of the random projection: each entry is `+√s` / `−√s` with
    /// probability `1/(2s)` each and `0` otherwise (Achlioptas very-sparse projection).
    pub sparsity: f64,
    /// L2-normalize each final embedding row (so cosine/kNN is well-scaled).
    pub l2_normalize: bool,
    /// Deterministic seed for the projection matrix.
    pub seed: u64,
}

impl Default for FastRpConfig {
    fn default() -> Self {
        Self {
            dim: 128,
            iterations: 3,
            iteration_weights: Vec::new(),
            normalization_strength: 0.5,
            sparsity: 3.0,
            l2_normalize: true,
            seed: 42,
        }
    }
}

/// Compute FastRP embeddings for every node of `graph`, returned as rows in the
/// graph's compact-index order (`rows[i]` is the embedding of node index `i`).
/// CONCEPT:EG-KG.graphlearn.fastrp
pub fn fastrp<N>(graph: &AdjacencyGraph<N>, config: &FastRpConfig) -> Vec<Vec<f32>>
where
    N: Clone + Eq + Hash + Ord,
{
    let n = graph.node_count();
    let d = config.dim.max(1);
    if n == 0 {
        return Vec::new();
    }

    // Symmetric undirected adjacency (each undirected edge's weight summed once) +
    // per-node weighted degree — the diffusion operator, built once. O(V + E).
    let adj = graph.undirected_weighted_adjacency();
    let deg: Vec<f64> = adj
        .iter()
        .map(|row| row.iter().map(|(_, w)| *w).sum())
        .collect();
    let s = config.normalization_strength;
    // Pre-raise degrees to the −s power once (guarded for isolated nodes).
    let deg_norm: Vec<f64> = deg
        .iter()
        .map(|&dv| if dv > 0.0 { dv.powf(-s) } else { 0.0 })
        .collect();

    // n_0 = the very-sparse random projection R (per-node-seeded ⇒ order-independent).
    let mut prev = random_projection(n, d, config.sparsity, config.seed);
    // Weighted-sum accumulator over n_1 … n_k (n_0 is not summed in — standard FastRP).
    let mut acc = vec![vec![0.0f64; d]; n];

    for iter in 1..=config.iterations.max(1) {
        let weight = iteration_weight(&config.iteration_weights, iter, config.iterations);
        // One diffusion step: cur[v] = Σ_{u∈N(v)} deg(v)^-s · w_uv · deg(u)^-s · prev[u].
        let mut cur = vec![vec![0.0f64; d]; n];
        for v in 0..n {
            let dv = deg_norm[v];
            if dv == 0.0 {
                continue; // isolated ⇒ empty sum ⇒ zero row
            }
            let out = &mut cur[v];
            for &(u, w) in &adj[v] {
                let c = dv * w * deg_norm[u];
                if c == 0.0 {
                    continue;
                }
                let src = &prev[u];
                for (o, s) in out.iter_mut().zip(src.iter()) {
                    *o += c * s;
                }
            }
        }
        if weight != 0.0 {
            for v in 0..n {
                let a = &mut acc[v];
                let c = &cur[v];
                for (av, cv) in a.iter_mut().zip(c.iter()) {
                    *av += weight * cv;
                }
            }
        }
        prev = cur;
    }

    if config.l2_normalize {
        l2_normalize_rows(&mut acc);
    }
    acc.into_iter()
        .map(|row| row.into_iter().map(|x| x as f32).collect())
        .collect()
}

/// The weight applied to iteration `iter` (1-based). Empty config weights ⇒ every
/// iteration weighted `1.0`; otherwise the `iter`-th entry (or `0.0` past the end).
fn iteration_weight(weights: &[f64], iter: usize, _iterations: usize) -> f64 {
    if weights.is_empty() {
        1.0
    } else {
        weights.get(iter - 1).copied().unwrap_or(0.0)
    }
}

/// Build the `V×d` very-sparse random projection matrix (Achlioptas): each entry is
/// `+√s` or `−√s` with probability `1/(2s)` each, else `0`. Each ROW is drawn from a
/// stream seeded by `seed ⊕ mix(row)`, so the matrix is identical regardless of the
/// order rows are produced in. CONCEPT:EG-KG.graphlearn.fastrp
fn random_projection(n: usize, d: usize, sparsity: f64, seed: u64) -> Vec<Vec<f64>> {
    let s = sparsity.max(1.0);
    let scale = s.sqrt();
    let p = 1.0 / (2.0 * s); // P(+√s) = P(−√s) = 1/(2s)
    (0..n)
        .map(|row| {
            let mut rng = SplitMix64::new(
                seed.wrapping_add((row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)),
            );
            (0..d)
                .map(|_| {
                    let r = rng.next_f64();
                    if r < p {
                        scale
                    } else if r < 2.0 * p {
                        -scale
                    } else {
                        0.0
                    }
                })
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two disjoint triangles (0-1-2) and (3-4-5): nodes in the same triangle share
    /// their whole neighbourhood, so FastRP embeddings within a triangle must be more
    /// similar (cosine) than across triangles.
    #[test]
    fn fastrp_separates_two_triangles() {
        let g = AdjacencyGraph::from_unweighted_edges([
            (0u32, 1u32),
            (1, 2),
            (2, 0),
            (3, 4),
            (4, 5),
            (5, 3),
        ]);
        let cfg = FastRpConfig {
            dim: 32,
            iterations: 3,
            seed: 7,
            ..Default::default()
        };
        let emb = fastrp(&g, &cfg);
        assert_eq!(emb.len(), 6);
        assert_eq!(emb[0].len(), 32);
        let cos = |a: &[f32], b: &[f32]| super::super::cosine_f32(a, b);
        let intra = cos(&emb[0], &emb[1]);
        let inter = cos(&emb[0], &emb[3]);
        assert!(
            intra > inter,
            "intra-triangle cosine {intra} should exceed inter {inter}"
        );
    }

    /// Determinism: same seed ⇒ byte-identical embeddings; the projection is
    /// per-node-seeded so it does not depend on node/iteration order.
    #[test]
    fn fastrp_is_deterministic() {
        let g = AdjacencyGraph::from_unweighted_edges([(0u32, 1u32), (1, 2), (2, 3), (3, 0)]);
        let cfg = FastRpConfig {
            dim: 16,
            seed: 123,
            ..Default::default()
        };
        let a = fastrp(&g, &cfg);
        let b = fastrp(&g, &cfg);
        assert_eq!(a, b);
    }

    /// L2-normalized rows have unit norm (zero rows — isolated nodes — stay zero).
    #[test]
    fn fastrp_rows_are_unit_norm() {
        let g = AdjacencyGraph::from_unweighted_edges([(0u32, 1u32), (1, 2), (2, 0)]);
        let cfg = FastRpConfig {
            dim: 8,
            l2_normalize: true,
            ..Default::default()
        };
        let emb = fastrp(&g, &cfg);
        for row in &emb {
            let norm: f32 = row.iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!((norm - 1.0).abs() < 1e-5, "row norm {norm} not unit");
        }
    }

    /// An isolated node yields the zero embedding (no neighbourhood to diffuse).
    #[test]
    fn fastrp_isolated_node_is_zero() {
        let g = AdjacencyGraph::from_adjacency([
            (0u32, vec![(1u32, 1.0)]),
            (1, vec![(0, 1.0)]),
            (2, vec![]), // isolated
        ]);
        let cfg = FastRpConfig {
            dim: 8,
            ..Default::default()
        };
        let emb = fastrp(&g, &cfg);
        assert!(emb[2].iter().all(|&x| x == 0.0));
    }
}
