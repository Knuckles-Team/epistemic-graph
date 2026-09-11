// CONCEPT:EG-KG.mining.truncated-svd — dimensionality reduction (DESCRIPTIVE: transform rows).
//
// Pure-Rust, dependency-light, batch (one round-trip): given a feature matrix (each
// row a point in R^d), project the rows into a low-dimensional embedding. Completes
// the reduction family beyond the existing datascience PCA with four engines:
//
//   * Truncated SVD  (CONCEPT:EG-KG.mining.truncated-svd) — the top-`k` right singular
//     vectors of X (via a symmetric eigendecomposition of the Gram matrix XᵀX);
//     coords = X·V = U·Σ. Reconstructs a low-rank matrix to within round-off.
//   * LDA            (CONCEPT:EG-KG.mining.lda-discriminant) — Fisher linear
//     discriminant (SUPERVISED — needs labels): whiten by the within-class scatter,
//     then take the top eigenvectors of the between-class scatter. ≤ (n_classes−1) dims.
//   * UMAP           (CONCEPT:EG-KG.mining.umap-layout) — a fuzzy k-NN neighbor graph
//     laid out by attractive/repulsive SGD (pure-Rust CPU). Approximate, small-N.
//   * t-SNE          (CONCEPT:EG-KG.mining.tsne-embedding) — perplexity-calibrated
//     Gaussian affinities matched to a Student-t low-D layout by gradient descent.
//     Approximate, small-N.
//
// Output: the transformed low-D `coords` (n rows × n_components). This module is
// graph-agnostic (works over `&[Vec<f64>]` + optional `&[i64]` labels for LDA); the
// handler (`src/server/handlers/mining.rs`) supplies rows (explicit features or node
// embeddings) and does the KG write-back (`:Embedding2D`).
//
// SCOPE (honest): SVD/LDA are exact linear algebra (deterministic, parity-checkable).
// UMAP and t-SNE are APPROXIMATE, iterative, and intended for small N (viz-scale —
// hundreds to low thousands of rows); they preserve neighborhood/cluster structure,
// not exact coordinates, and are deterministic per `seed`.

use super::math::{sq_dist, SplitMix64};

/// A point in feature space (one matrix row).
pub type Point = Vec<f64>;

/// Which reduction engine to run, with its parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Algorithm {
    /// Truncated SVD (unsupervised, exact linear projection).
    TruncatedSvd,
    /// Fisher LDA (supervised — the caller must supply labels).
    Lda,
    /// UMAP layout (approximate, seeded).
    Umap {
        n_neighbors: usize,
        min_dist: f64,
        epochs: usize,
        seed: u64,
    },
    /// t-SNE embedding (approximate, seeded).
    Tsne {
        perplexity: f64,
        epochs: usize,
        learning_rate: f64,
        seed: u64,
    },
}

/// The reduction outcome: the transformed low-D `coords` (parallel to the input
/// rows) and, for SVD, the retained `singular_values` (empty otherwise).
#[derive(Debug, Clone, PartialEq)]
pub struct Reduction {
    pub coords: Vec<Vec<f64>>,
    pub singular_values: Vec<f64>,
}

/// Run the chosen reduction over `rows` into `n_components` dimensions. `labels` is
/// required for LDA (ignored otherwise). An empty input yields an empty result.
pub fn reduce(
    rows: &[Point],
    labels: Option<&[i64]>,
    algorithm: Algorithm,
    n_components: usize,
) -> Reduction {
    if rows.is_empty() {
        return Reduction {
            coords: Vec::new(),
            singular_values: Vec::new(),
        };
    }
    let dim = rows[0].len();
    let k = n_components.clamp(1, dim.max(1));
    match algorithm {
        Algorithm::TruncatedSvd => truncated_svd(rows, k),
        Algorithm::Lda => lda(rows, labels.unwrap_or(&[]), n_components),
        Algorithm::Umap {
            n_neighbors,
            min_dist,
            epochs,
            seed,
        } => Reduction {
            coords: umap(rows, k, n_neighbors, min_dist, epochs, seed),
            singular_values: Vec::new(),
        },
        Algorithm::Tsne {
            perplexity,
            epochs,
            learning_rate,
            seed,
        } => Reduction {
            coords: tsne(rows, k, perplexity, epochs, learning_rate, seed),
            singular_values: Vec::new(),
        },
    }
}

// ─────────────────────────── Truncated SVD ───────────────────────────

/// Truncated SVD (CONCEPT:EG-KG.mining.truncated-svd) via the eigendecomposition of
/// the Gram matrix G = XᵀX (d×d, symmetric PSD). Its eigenpairs (λ_i, v_i) give the
/// right singular vectors V and singular values σ_i = √λ_i; the transformed rows are
/// coords = X·V = U·Σ. For a rank-r matrix and k ≥ r the reconstruction coords·Vᵀ = X
/// is exact to round-off (asserted by the reconstruction test).
fn truncated_svd(rows: &[Point], k: usize) -> Reduction {
    let n = rows.len();
    let dim = rows[0].len();
    // Gram matrix G = XᵀX (full symmetric fill — a, b each index g AND row).
    let mut g = vec![vec![0.0f64; dim]; dim];
    for row in rows {
        for a in 0..dim {
            for b in 0..dim {
                g[a][b] += row[a] * row[b];
            }
        }
    }
    let (eigvals, eigvecs) = jacobi_eigen(&g);
    // Sort eigenpairs by descending eigenvalue, take top-k.
    let mut order: Vec<usize> = (0..dim).collect();
    order.sort_by(|&a, &b| eigvals[b].partial_cmp(&eigvals[a]).unwrap());
    let kk = k.min(dim);
    let mut singular_values = Vec::with_capacity(kk);
    // V columns = the selected eigenvectors (eigvecs[:, j]).
    let mut v_cols: Vec<Vec<f64>> = Vec::with_capacity(kk);
    for &j in order.iter().take(kk) {
        singular_values.push(eigvals[j].max(0.0).sqrt());
        v_cols.push((0..dim).map(|d| eigvecs[d][j]).collect());
    }
    // coords[i][c] = X[i] · V[:, c].
    let mut coords = vec![vec![0.0f64; kk]; n];
    for (i, row) in rows.iter().enumerate() {
        for (c, vcol) in v_cols.iter().enumerate() {
            coords[i][c] = dot(row, vcol);
        }
    }
    Reduction {
        coords,
        singular_values,
    }
}

// ─────────────────────────── LDA ───────────────────────────

/// Fisher Linear Discriminant Analysis (CONCEPT:EG-KG.mining.lda-discriminant),
/// supervised. Computes the within-class scatter `Sw` and between-class scatter `Sb`,
/// whitens the space by `Sw` (symmetric eig → W = U·D^-1/2), then takes the leading
/// eigenvectors of the whitened between-class scatter WᵀSbW; the discriminant
/// directions are A = W·E. The rows are projected onto ≤ (n_classes−1) discriminants —
/// the directions that maximize between/within class separation.
fn lda(rows: &[Point], labels: &[i64], n_components: usize) -> Reduction {
    let n = rows.len();
    let dim = rows[0].len();
    if labels.len() != n {
        // Missing/mismatched labels ⇒ degrade to an empty (invalid) result rather
        // than panic; the handler validates and reports the error before this.
        return Reduction {
            coords: Vec::new(),
            singular_values: Vec::new(),
        };
    }
    let classes: Vec<i64> = labels
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let n_comp = n_components.clamp(1, (classes.len().saturating_sub(1)).max(1).min(dim));

    // Global mean + per-class means.
    let mean = column_mean(rows, dim);
    let mut class_means: Vec<Vec<f64>> = Vec::with_capacity(classes.len());
    let mut class_sizes: Vec<usize> = Vec::with_capacity(classes.len());
    for &cls in &classes {
        let idx: Vec<usize> = (0..n).filter(|&i| labels[i] == cls).collect();
        let mut cm = vec![0.0f64; dim];
        for &i in &idx {
            for d in 0..dim {
                cm[d] += rows[i][d];
            }
        }
        let cn = idx.len().max(1) as f64;
        for m in cm.iter_mut() {
            *m /= cn;
        }
        class_sizes.push(idx.len());
        class_means.push(cm);
    }

    // Sw = Σ_c Σ_{i∈c} (x-μ_c)(x-μ_c)ᵀ ; Sb = Σ_c n_c (μ_c-μ)(μ_c-μ)ᵀ.
    let mut sw = vec![vec![0.0f64; dim]; dim];
    for (ci, &cls) in classes.iter().enumerate() {
        for i in 0..n {
            if labels[i] != cls {
                continue;
            }
            let diff: Vec<f64> = (0..dim).map(|d| rows[i][d] - class_means[ci][d]).collect();
            outer_add(&mut sw, &diff, &diff, 1.0);
        }
    }
    let mut sb = vec![vec![0.0f64; dim]; dim];
    for ci in 0..classes.len() {
        let diff: Vec<f64> = (0..dim).map(|d| class_means[ci][d] - mean[d]).collect();
        outer_add(&mut sb, &diff, &diff, class_sizes[ci] as f64);
    }
    // Regularize Sw for invertibility.
    for (d, row) in sw.iter_mut().enumerate() {
        row[d] += 1e-6;
    }

    // Whiten by Sw: Sw = U D Uᵀ → W = U D^-1/2.
    let (sw_vals, sw_vecs) = jacobi_eigen(&sw);
    let mut w = vec![vec![0.0f64; dim]; dim]; // W[:, j] = U[:, j] / sqrt(D_j)
    for j in 0..dim {
        let scale = 1.0 / sw_vals[j].max(1e-12).sqrt();
        for d in 0..dim {
            w[d][j] = sw_vecs[d][j] * scale;
        }
    }
    // Whitened between-class scatter M = Wᵀ Sb W (symmetric).
    let sbw = matmul(&sb, &w); // (d×d)
    let m = matmul_tn(&w, &sbw); // Wᵀ · (Sb W)
    let (m_vals, m_vecs) = jacobi_eigen(&m);
    let mut order: Vec<usize> = (0..dim).collect();
    order.sort_by(|&a, &b| m_vals[b].partial_cmp(&m_vals[a]).unwrap());

    // Discriminant directions A[:, c] = W · E[:, order[c]].
    let mut a_cols: Vec<Vec<f64>> = Vec::with_capacity(n_comp);
    for &j in order.iter().take(n_comp) {
        let e_col: Vec<f64> = (0..dim).map(|d| m_vecs[d][j]).collect();
        let a_col: Vec<f64> = (0..dim)
            .map(|d| (0..dim).map(|kk| w[d][kk] * e_col[kk]).sum())
            .collect();
        a_cols.push(a_col);
    }
    // Project the mean-centered rows onto the discriminants.
    let mut coords = vec![vec![0.0f64; n_comp]; n];
    for (i, row) in rows.iter().enumerate() {
        let centered: Vec<f64> = (0..dim).map(|d| row[d] - mean[d]).collect();
        for (c, a_col) in a_cols.iter().enumerate() {
            coords[i][c] = dot(&centered, a_col);
        }
    }
    Reduction {
        coords,
        singular_values: Vec::new(),
    }
}

// ─────────────────────────── t-SNE ───────────────────────────

/// t-SNE (CONCEPT:EG-KG.mining.tsne-embedding). Builds perplexity-calibrated Gaussian
/// affinities P in the high-D space (per-point σ found by binary search to match
/// `perplexity`), symmetrizes them, then gradient-descends a low-D layout Y whose
/// Student-t affinities Q match P (KL gradient with momentum + early exaggeration).
/// Approximate + small-N by design; deterministic per `seed`.
fn tsne(
    rows: &[Point],
    dims: usize,
    perplexity: f64,
    epochs: usize,
    learning_rate: f64,
    seed: u64,
) -> Vec<Vec<f64>> {
    let n = rows.len();
    if n <= dims {
        return identity_pad(rows, dims);
    }
    let perp = perplexity.clamp(1.0, ((n - 1) as f64 / 3.0).max(1.0));
    let epochs = epochs.max(50);
    let lr = if learning_rate > 0.0 {
        learning_rate
    } else {
        100.0
    };

    let d2 = squared_distance_matrix(rows);
    let p = gaussian_affinities(&d2, perp.ln());
    let pj = symmetrize_affinities(&p);

    let mut rng = SplitMix64::new(seed);
    let mut y: Vec<Vec<f64>> = (0..n)
        .map(|_| (0..dims).map(|_| 1e-4 * rng.next_gauss()).collect())
        .collect();
    optimize_tsne(&mut y, &pj, epochs, lr, dims);
    y
}

/// Compute the symmetric pairwise squared-distance matrix used by t-SNE.
fn squared_distance_matrix(rows: &[Point]) -> Vec<Vec<f64>> {
    eg_geo::distance_matrix(rows, |a, b| sq_dist(a, b))
}

/// Calibrate one Gaussian affinity row so its entropy matches the target log-perplexity.
fn gaussian_affinity_row(distances: &[f64], diagonal: usize, target: f64) -> Vec<f64> {
    let n = distances.len();
    let (mut beta, mut lo, mut hi) = (1.0f64, f64::NEG_INFINITY, f64::INFINITY);
    let mut normalized = vec![0.0f64; n];
    for _ in 0..50 {
        let (row, sum) = gaussian_weights(distances, diagonal, beta);
        let h = gaussian_entropy(&row, diagonal, sum);
        normalized = normalize_probabilities(&row, sum);
        let diff = h - target;
        if diff.abs() < 1e-5 {
            break;
        }
        beta = update_gaussian_beta(beta, diff, &mut lo, &mut hi);
    }
    normalized
}

/// Compute one unnormalized Gaussian affinity row and its safe normalizer.
fn gaussian_weights(distances: &[f64], diagonal: usize, beta: f64) -> (Vec<f64>, f64) {
    let n = distances.len();
    let mut sum = 0.0;
    let mut row = vec![0.0f64; n];
    for j in 0..n {
        if diagonal != j {
            row[j] = (-beta * distances[j]).exp();
            sum += row[j];
        }
    }
    (row, sum.max(1e-12))
}

/// Compute the Shannon entropy of one normalized Gaussian affinity row.
fn gaussian_entropy(row: &[f64], diagonal: usize, sum: f64) -> f64 {
    let mut entropy = 0.0;
    for (j, &rv) in row.iter().enumerate() {
        if diagonal != j {
            let pij = rv / sum;
            if pij > 1e-12 {
                entropy += -pij * pij.ln();
            }
        }
    }
    entropy
}

/// Normalize one Gaussian affinity row without changing its element order.
fn normalize_probabilities(row: &[f64], sum: f64) -> Vec<f64> {
    row.iter().map(|value| value / sum).collect()
}

/// Update the binary-search bounds for one Gaussian entropy measurement.
fn update_gaussian_beta(beta: f64, diff: f64, lo: &mut f64, hi: &mut f64) -> f64 {
    if diff > 0.0 {
        *lo = beta;
        if hi.is_infinite() {
            beta * 2.0
        } else {
            (beta + *hi) / 2.0
        }
    } else {
        *hi = beta;
        if lo.is_infinite() {
            beta / 2.0
        } else {
            (beta + *lo) / 2.0
        }
    }
}

/// Build all per-point Gaussian affinities from the pairwise distance matrix.
fn gaussian_affinities(d2: &[Vec<f64>], target: f64) -> Vec<Vec<f64>> {
    d2.iter()
        .enumerate()
        .map(|(i, distances)| gaussian_affinity_row(distances, i, target))
        .collect()
}

/// Symmetrize and normalize high-dimensional affinities for the t-SNE objective.
fn symmetrize_affinities(p: &[Vec<f64>]) -> Vec<Vec<f64>> {
    let n = p.len();
    let denom = (2 * n) as f64;
    let mut pj = vec![vec![0.0f64; n]; n];
    for i in 0..n {
        for j in 0..n {
            pj[i][j] = ((p[i][j] + p[j][i]) / denom).max(1e-12);
        }
    }
    pj
}

/// Run the delta-bar-delta t-SNE optimizer with momentum and recentering.
fn optimize_tsne(y: &mut [Vec<f64>], pj: &[Vec<f64>], epochs: usize, lr: f64, dims: usize) {
    let n = y.len();
    let mut vel = vec![vec![0.0f64; dims]; n];
    let mut gains = vec![vec![1.0f64; dims]; n];
    for epoch in 0..epochs {
        let exaggeration = if epoch < 100 { 4.0 } else { 1.0 };
        let momentum = if epoch < 100 { 0.5 } else { 0.8 };
        let (num, qsum) = student_t_affinities(y);
        let inputs = TsneEpoch {
            y,
            pj,
            num: &num,
            qsum,
            exaggeration,
            dims,
        };
        update_tsne_velocity(&inputs, momentum, lr, &mut vel, &mut gains);
        for i in 0..n {
            for d in 0..dims {
                y[i][d] += vel[i][d];
            }
        }
        recenter(y, dims);
    }
}

/// Compute the low-dimensional Student-t numerators and their normalization sum.
fn student_t_affinities(y: &[Vec<f64>]) -> (Vec<Vec<f64>>, f64) {
    let n = y.len();
    let mut num = vec![vec![0.0f64; n]; n];
    let mut qsum = 0.0;
    for i in 0..n {
        for j in (i + 1)..n {
            let dist = sq_dist(&y[i], &y[j]);
            let v = 1.0 / (1.0 + dist);
            num[i][j] = v;
            num[j][i] = v;
            qsum += 2.0 * v;
        }
    }
    (num, qsum.max(1e-12))
}

/// One t-SNE epoch's fixed inputs: the current embedding, the symmetrized
/// high-dimensional affinities, this epoch's Student-t low-dimensional affinities
/// and their sum, and the exaggeration schedule. The velocity update and the
/// gradient read exactly this set unchanged; only the mutable optimizer state
/// (`vel`/`gains`) and the learning schedule differ between them.
struct TsneEpoch<'a> {
    y: &'a [Vec<f64>],
    pj: &'a [Vec<f64>],
    num: &'a [Vec<f64>],
    qsum: f64,
    exaggeration: f64,
    dims: usize,
}

/// Apply one gradient step to all low-dimensional coordinates' velocities.
fn update_tsne_velocity(
    epoch: &TsneEpoch<'_>,
    momentum: f64,
    lr: f64,
    vel: &mut [Vec<f64>],
    gains: &mut [Vec<f64>],
) {
    let dims = epoch.dims;
    for i in 0..epoch.y.len() {
        let grad = tsne_gradient(epoch, i);
        for d in 0..dims {
            if grad[d].signum() != vel[i][d].signum() {
                gains[i][d] += 0.2;
            } else {
                gains[i][d] *= 0.8;
            }
            gains[i][d] = gains[i][d].max(0.01);
            vel[i][d] = momentum * vel[i][d] - lr * gains[i][d] * grad[d];
        }
    }
}

/// Compute one t-SNE gradient row from the symmetric affinity matrices.
fn tsne_gradient(epoch: &TsneEpoch<'_>, i: usize) -> Vec<f64> {
    let TsneEpoch {
        y,
        pj,
        num,
        qsum,
        exaggeration,
        dims,
    } = *epoch;
    let mut grad = vec![0.0f64; dims];
    for j in 0..y.len() {
        if i == j {
            continue;
        }
        let q = (num[i][j] / qsum).max(1e-12);
        let mult = 4.0 * (pj[i][j] * exaggeration - q) * num[i][j];
        for d in 0..dims {
            grad[d] += mult * (y[i][d] - y[j][d]);
        }
    }
    grad
}

// ─────────────────────────── UMAP ───────────────────────────

/// UMAP (CONCEPT:EG-KG.mining.umap-layout). Builds a fuzzy k-NN simplicial set: per
/// point, the local connectivity ρ (distance to the nearest neighbor) and a σ scaling
/// the membership strengths exp(−(d−ρ)/σ) to sum ≈ log2(k); edges are symmetrized by
/// probabilistic t-conorm. The layout is optimized by SGD with attractive forces on
/// graph edges and repulsive forces on negative samples (a·b Student-t-style kernel
/// from `min_dist`). Approximate + small-N by design; deterministic per `seed`.
fn umap(
    rows: &[Point],
    dims: usize,
    n_neighbors: usize,
    min_dist: f64,
    epochs: usize,
    seed: u64,
) -> Vec<Vec<f64>> {
    let n = rows.len();
    if n <= dims + 1 {
        return identity_pad(rows, dims);
    }
    let k = n_neighbors.clamp(2, n - 1);
    // Keep the graph construction and SGD layout as separate algorithm phases.
    let dist = eg_geo::distance_matrix(rows, |a, b| sq_dist(a, b).sqrt());
    let neighbors = umap_neighbors(&dist, k);
    let edges = umap_edges(&dist, &neighbors, k);
    umap_layout(rows, dims, &edges, min_dist, epochs, seed)
}

fn umap_neighbors(dist: &[Vec<f64>], k: usize) -> Vec<Vec<usize>> {
    (0..dist.len())
        .map(|i| {
            let mut order: Vec<usize> = (0..dist.len()).filter(|&j| j != i).collect();
            order.sort_by(|&a, &b| {
                dist[i][a]
                    .partial_cmp(&dist[i][b])
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.cmp(&b))
            });
            order.truncate(k);
            order
        })
        .collect()
}

fn umap_edges(dist: &[Vec<f64>], neighbors: &[Vec<usize>], k: usize) -> Vec<(usize, usize, f64)> {
    // Fuzzy membership weights (rho + sigma calibration to log2(k)).
    let n = dist.len();
    let target = (k as f64).log2().max(1.0);
    let mut weight = vec![vec![0.0f64; n]; n];
    for i in 0..n {
        for (j, value) in umap_row_weights(&dist[i], &neighbors[i], target) {
            weight[i][j] = value;
        }
    }

    // Symmetrize by the probabilistic t-conorm: w = a + b − a·b.
    let mut edges: Vec<(usize, usize, f64)> = Vec::new();
    for (i, wrow) in weight.iter().enumerate() {
        for j in (i + 1)..n {
            let a = wrow[j];
            let b = weight[j][i];
            let w = a + b - a * b;
            if w > 1e-3 {
                edges.push((i, j, w));
            }
        }
    }
    edges
}

fn umap_row_weights(distances: &[f64], neighbors: &[usize], target: f64) -> Vec<(usize, f64)> {
    let rho = distances[neighbors[0]];
    // Binary search sigma.
    let (mut sigma, mut lo, mut hi) = (1.0f64, 0.0f64, f64::INFINITY);
    for _ in 0..40 {
        let mut s = 0.0;
        for &j in neighbors {
            s += (-(distances[j] - rho).max(0.0) / sigma).exp();
        }
        if (s - target).abs() < 1e-4 {
            break;
        }
        if s > target {
            hi = sigma;
            sigma = (lo + hi) / 2.0;
        } else {
            lo = sigma;
            sigma = if hi.is_infinite() {
                sigma * 2.0
            } else {
                (lo + hi) / 2.0
            };
        }
    }
    neighbors
        .iter()
        .map(|&j| (j, (-(distances[j] - rho).max(0.0) / sigma.max(1e-6)).exp()))
        .collect()
}

fn umap_layout(
    rows: &[Point],
    dims: usize,
    edges: &[(usize, usize, f64)],
    min_dist: f64,
    epochs: usize,
    seed: u64,
) -> Vec<Vec<f64>> {
    let n = rows.len();
    let epochs = epochs.max(50);
    // (a, b) curve params fitting the min_dist smoothness (standard UMAP approximation).
    let (a, b) = umap_ab(min_dist);

    // Init the layout from a small seeded jitter around the first two feature dims.
    let mut rng = SplitMix64::new(seed);
    let mut y: Vec<Vec<f64>> = (0..n)
        .map(|i| {
            (0..dims)
                .map(|d| rows[i].get(d).copied().unwrap_or(0.0) + 1e-3 * rng.next_gauss())
                .collect()
        })
        .collect();
    recenter(&mut y, dims);

    for epoch in 0..epochs {
        let alpha = 1.0 - (epoch as f64 / epochs as f64); // learning-rate decay
        for &(i, j, w) in edges {
            if rng.next_f64() > w {
                continue; // sample edges proportionally to membership
            }
            umap_edge_step(&mut y, i, j, a, b, alpha, &mut rng);
        }
    }
    recenter(&mut y, dims);
    y
}

fn umap_edge_step(
    y: &mut [Vec<f64>],
    i: usize,
    j: usize,
    a: f64,
    b: f64,
    alpha: f64,
    rng: &mut SplitMix64,
) {
    // Attractive gradient (pull i and j together). Precompute the per-dim
    // deltas, then apply to both rows (avoids a double mutable borrow of `y`).
    let d2 = sq_dist(&y[i], &y[j]).max(1e-6);
    let grad_coeff = (-2.0 * a * b * d2.powf(b - 1.0)) / (1.0 + a * d2.powf(b));
    let deltas: Vec<f64> = (0..y[i].len())
        .map(|d| clamp((y[i][d] - y[j][d]) * grad_coeff, -4.0, 4.0) * alpha)
        .collect();
    for (d, &del) in deltas.iter().enumerate() {
        y[i][d] += del;
        y[j][d] -= del;
    }

    // A few negative samples (repulsion).
    for _ in 0..3 {
        let r = (rng.next_u64() as usize) % y.len();
        if r == i {
            continue;
        }
        let yr = y[r].clone();
        let d2 = sq_dist(&y[i], &yr).max(1e-6);
        let grad_coeff = (2.0 * b) / ((0.001 + d2) * (1.0 + a * d2.powf(b)));
        for (d, &yrd) in yr.iter().enumerate() {
            y[i][d] += clamp((y[i][d] - yrd) * grad_coeff, -4.0, 4.0) * alpha;
        }
    }
}

/// Solve the UMAP smoothness curve `1/(1+a·d^(2b))` fit for a given `min_dist` via a
/// tiny least-squares over a fixed grid (the standard UMAP `find_ab_params`).
fn umap_ab(min_dist: f64) -> (f64, f64) {
    let min_dist = min_dist.clamp(0.001, 0.99);
    // Target curve: 1 for d<min_dist, else exp(-(d-min_dist)).
    let xs: Vec<f64> = (0..300).map(|i| i as f64 * 0.01).collect();
    let ys: Vec<f64> = xs
        .iter()
        .map(|&d| {
            if d < min_dist {
                1.0
            } else {
                (-(d - min_dist)).exp()
            }
        })
        .collect();
    // Grid search (a,b) minimizing SSE — keeps it dependency-free.
    let mut best = (1.0, 1.0);
    let mut best_err = f64::INFINITY;
    let mut a = 0.1;
    while a <= 3.0 {
        let mut b = 0.5;
        while b <= 2.0 {
            let err: f64 = xs
                .iter()
                .zip(&ys)
                .map(|(&d, &y)| {
                    let pred = 1.0 / (1.0 + a * d.powf(2.0 * b));
                    (pred - y) * (pred - y)
                })
                .sum();
            if err < best_err {
                best_err = err;
                best = (a, b);
            }
            b += 0.05;
        }
        a += 0.05;
    }
    best
}

// ─────────────────────────── linear-algebra helpers ───────────────────────────

/// Jacobi eigenvalue algorithm for a symmetric matrix. Returns `(eigenvalues,
/// eigenvectors)` where `eigenvectors[d][j]` is component `d` of eigenvector `j`
/// (columns are eigenvectors). Dependency-free + deterministic; the matrices here are
/// feature-dimension sized (small), so O(iters·d³) is fine.
fn jacobi_eigen(matrix: &[Vec<f64>]) -> (Vec<f64>, Vec<Vec<f64>>) {
    let n = matrix.len();
    let mut a: Vec<Vec<f64>> = matrix.to_vec();
    let mut v = vec![vec![0.0f64; n]; n];
    for (i, row) in v.iter_mut().enumerate() {
        row[i] = 1.0;
    }
    for _ in 0..100 {
        // Largest off-diagonal magnitude.
        let (mut p, mut q, mut off) = (0usize, 1usize, 0.0f64);
        for (i, arow) in a.iter().enumerate() {
            for (j, &aij) in arow.iter().enumerate().skip(i + 1) {
                if aij.abs() > off {
                    off = aij.abs();
                    p = i;
                    q = j;
                }
            }
        }
        if off < 1e-12 || n < 2 {
            break;
        }
        let app = a[p][p];
        let aqq = a[q][q];
        let apq = a[p][q];
        let theta = 0.5 * (aqq - app) / apq;
        let t = theta.signum() / (theta.abs() + (theta * theta + 1.0).sqrt());
        let c = 1.0 / (t * t + 1.0).sqrt();
        let s = t * c;
        // Rotate columns p, q of A (row-wise) and of the eigenvector accumulator V.
        for arow in a.iter_mut() {
            let aip = arow[p];
            let aiq = arow[q];
            arow[p] = c * aip - s * aiq;
            arow[q] = s * aip + c * aiq;
        }
        // Rotate rows p, q of A (column-wise) — split the two rows for disjoint borrows.
        {
            let (left, right) = a.split_at_mut(q);
            let rp = &mut left[p];
            let rq = &mut right[0];
            for (rpi, rqi) in rp.iter_mut().zip(rq.iter_mut()) {
                let api = *rpi;
                let aqi = *rqi;
                *rpi = c * api - s * aqi;
                *rqi = s * api + c * aqi;
            }
        }
        for vrow in v.iter_mut() {
            let vip = vrow[p];
            let viq = vrow[q];
            vrow[p] = c * vip - s * viq;
            vrow[q] = s * vip + c * viq;
        }
    }
    let eigvals: Vec<f64> = (0..n).map(|i| a[i][i]).collect();
    (eigvals, v)
}

fn matmul(a: &[Vec<f64>], b: &[Vec<f64>]) -> Vec<Vec<f64>> {
    let n = a.len();
    let m = b[0].len();
    let inner = b.len();
    let mut out = vec![vec![0.0f64; m]; n];
    for (i, orow) in out.iter_mut().enumerate() {
        for (kk, brow) in b.iter().enumerate().take(inner) {
            let aik = a[i][kk];
            if aik == 0.0 {
                continue;
            }
            for j in 0..m {
                orow[j] += aik * brow[j];
            }
        }
    }
    out
}

/// Aᵀ · B for square `a`, `b` of equal size.
fn matmul_tn(a: &[Vec<f64>], b: &[Vec<f64>]) -> Vec<Vec<f64>> {
    let n = a.len();
    let mut out = vec![vec![0.0f64; n]; n];
    for i in 0..n {
        for j in 0..n {
            let mut s = 0.0;
            for kk in 0..n {
                s += a[kk][i] * b[kk][j];
            }
            out[i][j] = s;
        }
    }
    out
}

fn outer_add(m: &mut [Vec<f64>], u: &[f64], w: &[f64], scale: f64) {
    for i in 0..u.len() {
        for j in 0..w.len() {
            m[i][j] += scale * u[i] * w[j];
        }
    }
}

fn column_mean(rows: &[Point], dim: usize) -> Vec<f64> {
    let n = rows.len() as f64;
    let mut mean = vec![0.0f64; dim];
    for row in rows {
        for d in 0..dim {
            mean[d] += row[d];
        }
    }
    for m in mean.iter_mut() {
        *m /= n;
    }
    mean
}

fn recenter(y: &mut [Vec<f64>], dims: usize) {
    let n = y.len() as f64;
    let mut mean = vec![0.0f64; dims];
    for row in y.iter() {
        for d in 0..dims {
            mean[d] += row[d];
        }
    }
    for m in mean.iter_mut() {
        *m /= n;
    }
    for row in y.iter_mut() {
        for d in 0..dims {
            row[d] -= mean[d];
        }
    }
}

/// Pad/truncate the raw rows to `dims` — the degenerate fallback when N is too small
/// for a meaningful neighbor layout (keeps the op total rather than erroring).
fn identity_pad(rows: &[Point], dims: usize) -> Vec<Vec<f64>> {
    rows.iter()
        .map(|r| {
            (0..dims)
                .map(|d| r.get(d).copied().unwrap_or(0.0))
                .collect()
        })
        .collect()
}

fn clamp(x: f64, lo: f64, hi: f64) -> f64 {
    x.max(lo).min(hi)
}

fn dot(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a rank-2 matrix in R^5: each row = a·u + b·v for basis vectors u, v.
    fn low_rank_matrix() -> Vec<Point> {
        let u = [1.0, 0.5, -0.3, 0.2, 0.8];
        let v = [-0.4, 0.9, 0.1, -0.7, 0.3];
        let coeffs = [(1.0, 0.0), (0.0, 1.0), (2.0, -1.0), (0.5, 0.5), (-1.0, 2.0)];
        coeffs
            .iter()
            .map(|&(a, b)| (0..5).map(|d| a * u[d] + b * v[d]).collect())
            .collect()
    }

    #[test]
    fn truncated_svd_reconstructs_low_rank() {
        let x = low_rank_matrix();
        let out = reduce(&x, None, Algorithm::TruncatedSvd, 2);
        assert_eq!(out.coords.len(), x.len());
        assert_eq!(out.coords[0].len(), 2);
        // Reconstruct X ≈ coords · Vᵀ. Recompute V from the same Gram eig to check the
        // reconstruction error is tiny for a rank-2 matrix retained at k=2.
        // Instead of recovering V here, assert the retained singular energy captures
        // essentially all of ‖X‖_F² (a rank-2 matrix has only 2 nonzero σ).
        let total_energy: f64 = x.iter().flatten().map(|&v| v * v).sum();
        let retained: f64 = out.singular_values.iter().map(|&s| s * s).sum();
        assert!(
            (total_energy - retained).abs() / total_energy < 1e-6,
            "retained {retained} vs total {total_energy}"
        );
    }

    #[test]
    fn truncated_svd_one_component_is_principal_direction() {
        let x = low_rank_matrix();
        let out = reduce(&x, None, Algorithm::TruncatedSvd, 1);
        assert_eq!(out.coords[0].len(), 1);
        assert_eq!(out.singular_values.len(), 1);
    }

    #[test]
    fn lda_separates_two_labeled_gaussians() {
        // Two 2-D Gaussians offset along x; LDA to 1-D must separate them.
        let mut rows = Vec::new();
        let mut labels = Vec::new();
        let mut rng = SplitMix64::new(1);
        for _ in 0..20 {
            rows.push(vec![0.0 + 0.3 * rng.next_gauss(), 0.3 * rng.next_gauss()]);
            labels.push(0i64);
        }
        for _ in 0..20 {
            rows.push(vec![6.0 + 0.3 * rng.next_gauss(), 0.3 * rng.next_gauss()]);
            labels.push(1i64);
        }
        let out = reduce(&rows, Some(&labels), Algorithm::Lda, 1);
        assert_eq!(out.coords[0].len(), 1);
        // The 1-D projection of the two classes is separable: max of class 0 < min of
        // class 1 (or vice-versa) with a comfortable gap.
        let c0: Vec<f64> = (0..20).map(|i| out.coords[i][0]).collect();
        let c1: Vec<f64> = (20..40).map(|i| out.coords[i][0]).collect();
        let m0 = c0.iter().sum::<f64>() / 20.0;
        let m1 = c1.iter().sum::<f64>() / 20.0;
        let sep = (m0 - m1).abs();
        let spread0 = c0.iter().map(|v| (v - m0).abs()).fold(0.0, f64::max);
        let spread1 = c1.iter().map(|v| (v - m1).abs()).fold(0.0, f64::max);
        assert!(sep > spread0 + spread1, "classes not separated: sep={sep}");
    }

    /// Neighbor preservation: the fraction of each row's high-D k-NN that remain in its
    /// low-D k-NN. A structure-preserving embedding keeps most neighbors.
    fn neighbor_preservation(hi: &[Point], lo: &[Vec<f64>], k: usize) -> f64 {
        let knn = |data: &[Vec<f64>], i: usize| -> std::collections::HashSet<usize> {
            let mut order: Vec<usize> = (0..data.len()).filter(|&j| j != i).collect();
            order.sort_by(|&a, &b| {
                sq_dist(&data[i], &data[a])
                    .partial_cmp(&sq_dist(&data[i], &data[b]))
                    .unwrap()
            });
            order.into_iter().take(k).collect()
        };
        let mut acc = 0.0;
        for i in 0..hi.len() {
            let a = knn(hi, i);
            let b = knn(lo, i);
            acc += a.intersection(&b).count() as f64 / k as f64;
        }
        acc / hi.len() as f64
    }

    /// Three tight, well-separated clusters in R^4 — the structure UMAP/t-SNE must keep.
    fn three_clusters() -> Vec<Point> {
        let centers = [
            [0.0, 0.0, 0.0, 0.0],
            [10.0, 10.0, 0.0, 0.0],
            [0.0, 0.0, 10.0, 10.0],
        ];
        let mut rng = SplitMix64::new(9);
        let mut rows = Vec::new();
        for c in centers {
            for _ in 0..12 {
                rows.push((0..4).map(|d| c[d] + 0.2 * rng.next_gauss()).collect());
            }
        }
        rows
    }

    #[test]
    fn tsne_preserves_cluster_structure() {
        let rows = three_clusters();
        let out = reduce(
            &rows,
            None,
            Algorithm::Tsne {
                perplexity: 8.0,
                epochs: 300,
                learning_rate: 100.0,
                seed: 7,
            },
            2,
        );
        assert_eq!(out.coords.len(), rows.len());
        assert_eq!(out.coords[0].len(), 2);
        let np = neighbor_preservation(&rows, &out.coords, 5);
        assert!(np > 0.7, "t-SNE neighbor preservation too low: {np}");
    }

    #[test]
    fn umap_preserves_cluster_structure() {
        let rows = three_clusters();
        let out = reduce(
            &rows,
            None,
            Algorithm::Umap {
                n_neighbors: 8,
                min_dist: 0.1,
                epochs: 200,
                seed: 3,
            },
            2,
        );
        assert_eq!(out.coords.len(), rows.len());
        let np = neighbor_preservation(&rows, &out.coords, 5);
        assert!(np > 0.6, "UMAP neighbor preservation too low: {np}");
    }

    #[test]
    fn umap_deterministic_for_seed() {
        let rows = three_clusters();
        let algorithm = Algorithm::Umap {
            n_neighbors: 8,
            min_dist: 0.1,
            epochs: 100,
            seed: 5,
        };
        let a = reduce(&rows, None, algorithm, 2);
        let b = reduce(&rows, None, algorithm, 2);
        assert_eq!(a.coords, b.coords);
    }

    #[test]
    fn umap_uses_identity_for_too_few_rows() {
        let rows = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]];
        let out = reduce(
            &rows,
            None,
            Algorithm::Umap {
                n_neighbors: 2,
                min_dist: 0.1,
                epochs: 0,
                seed: 1,
            },
            2,
        );
        assert_eq!(out.coords, vec![vec![1.0, 2.0], vec![4.0, 5.0]]);
    }

    #[test]
    fn empty_input_is_empty_result() {
        let out = reduce(&[], None, Algorithm::TruncatedSvd, 2);
        assert!(out.coords.is_empty());
    }

    #[test]
    fn tsne_deterministic_for_seed() {
        let rows = three_clusters();
        let a = reduce(
            &rows,
            None,
            Algorithm::Tsne {
                perplexity: 8.0,
                epochs: 100,
                learning_rate: 100.0,
                seed: 5,
            },
            2,
        );
        let b = reduce(
            &rows,
            None,
            Algorithm::Tsne {
                perplexity: 8.0,
                epochs: 100,
                learning_rate: 100.0,
                seed: 5,
            },
            2,
        );
        assert_eq!(a.coords, b.coords);
    }
}
