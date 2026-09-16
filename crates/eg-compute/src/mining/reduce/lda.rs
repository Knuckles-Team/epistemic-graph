//! Fisher linear discriminant analysis phases.

use super::{column_mean, dot, jacobi_eigen, matmul, matmul_tn, outer_add, Point, Reduction};

/// Fisher Linear Discriminant Analysis (CONCEPT:EG-KG.mining.lda-discriminant),
/// supervised. Computes the within-class scatter `Sw` and between-class scatter `Sb`,
/// whitens the space by `Sw` (symmetric eig → W = U·D^-1/2), then takes the leading
/// eigenvectors of the whitened between-class scatter WᵀSbW; the discriminant
/// directions are A = W·E. The rows are projected onto ≤ (n_classes−1) discriminants —
/// the directions that maximize between/within class separation.
pub(super) fn lda(rows: &[Point], labels: &[i64], n_components: usize) -> Reduction {
    let n = rows.len();
    let dim = rows[0].len();
    if labels.len() != n {
        return empty_reduction();
    }
    let classes = class_labels(labels);
    let n_comp = n_components.clamp(1, (classes.len().saturating_sub(1)).max(1).min(dim));
    let mean = column_mean(rows, dim);
    let (class_means, class_sizes) = class_statistics(rows, labels, &classes, dim);
    let mut sw = within_scatter(rows, labels, &classes, &class_means, dim);
    let sb = between_scatter(&class_means, &class_sizes, &mean, dim);
    regularize_within(&mut sw);
    let w = whiten(&sw);
    let directions = discriminant_columns(&w, &sb, n_comp);
    let coords = project_rows(rows, &mean, &directions, n_comp);
    Reduction {
        coords,
        singular_values: Vec::new(),
    }
}

fn empty_reduction() -> Reduction {
    Reduction {
        coords: Vec::new(),
        singular_values: Vec::new(),
    }
}

fn class_labels(labels: &[i64]) -> Vec<i64> {
    labels
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn class_statistics(
    rows: &[Point],
    labels: &[i64],
    classes: &[i64],
    dim: usize,
) -> (Vec<Vec<f64>>, Vec<usize>) {
    let mut class_means = Vec::with_capacity(classes.len());
    let mut class_sizes = Vec::with_capacity(classes.len());
    for &cls in classes {
        let idx: Vec<usize> = (0..rows.len()).filter(|&i| labels[i] == cls).collect();
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
    (class_means, class_sizes)
}

fn within_scatter(
    rows: &[Point],
    labels: &[i64],
    classes: &[i64],
    class_means: &[Vec<f64>],
    dim: usize,
) -> Vec<Vec<f64>> {
    let mut scatter = vec![vec![0.0f64; dim]; dim];
    for (ci, &cls) in classes.iter().enumerate() {
        for i in 0..rows.len() {
            if labels[i] != cls {
                continue;
            }
            let diff: Vec<f64> = (0..dim).map(|d| rows[i][d] - class_means[ci][d]).collect();
            outer_add(&mut scatter, &diff, &diff, 1.0);
        }
    }
    scatter
}

fn between_scatter(
    class_means: &[Vec<f64>],
    class_sizes: &[usize],
    mean: &[f64],
    dim: usize,
) -> Vec<Vec<f64>> {
    let mut scatter = vec![vec![0.0f64; dim]; dim];
    for ci in 0..class_means.len() {
        let diff: Vec<f64> = (0..dim).map(|d| class_means[ci][d] - mean[d]).collect();
        outer_add(&mut scatter, &diff, &diff, class_sizes[ci] as f64);
    }
    scatter
}

fn regularize_within(scatter: &mut [Vec<f64>]) {
    for (d, row) in scatter.iter_mut().enumerate() {
        row[d] += 1e-6;
    }
}

fn whiten(scatter: &[Vec<f64>]) -> Vec<Vec<f64>> {
    let dim = scatter.len();
    let (sw_vals, sw_vecs) = jacobi_eigen(scatter);
    let mut w = vec![vec![0.0f64; dim]; dim];
    for j in 0..dim {
        let scale = 1.0 / sw_vals[j].max(1e-12).sqrt();
        for d in 0..dim {
            w[d][j] = sw_vecs[d][j] * scale;
        }
    }
    w
}

/// `sum(row[kk] * e_col[kk])` over `kk in 0..width`, extracted out of
/// `discriminant_columns` so the eigenvector-back-projection loop nests only
/// two closures deep instead of three (BUG-KISS `nested_function_depth`).
fn weighted_row_sum(row: &[f64], e_col: &[f64], width: usize) -> f64 {
    (0..width).map(|kk| row[kk] * e_col[kk]).sum()
}

fn discriminant_columns(w: &[Vec<f64>], sb: &[Vec<f64>], n_comp: usize) -> Vec<Vec<f64>> {
    let sbw = matmul(sb, w);
    let m = matmul_tn(w, &sbw);
    let (m_vals, m_vecs) = jacobi_eigen(&m);
    let order = descending_order(&m_vals);
    order
        .iter()
        .take(n_comp)
        .map(|&j| {
            let e_col: Vec<f64> = (0..w.len()).map(|d| m_vecs[d][j]).collect();
            (0..w.len())
                .map(|d| weighted_row_sum(&w[d], &e_col, w.len()))
                .collect()
        })
        .collect()
}

fn descending_order(values: &[f64]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..values.len()).collect();
    order.sort_by(|&a, &b| values[b].partial_cmp(&values[a]).unwrap());
    order
}

fn project_rows(
    rows: &[Point],
    mean: &[f64],
    directions: &[Vec<f64>],
    n_comp: usize,
) -> Vec<Vec<f64>> {
    let mut coords = vec![vec![0.0f64; n_comp]; rows.len()];
    for (i, row) in rows.iter().enumerate() {
        let centered: Vec<f64> = (0..mean.len()).map(|d| row[d] - mean[d]).collect();
        for (c, direction) in directions.iter().enumerate() {
            coords[i][c] = dot(&centered, direction);
        }
    }
    coords
}
