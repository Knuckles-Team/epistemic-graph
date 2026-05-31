// CONCEPT:KG-2.22a — ML Primitives
//
// Core machine learning building blocks implemented in Rust.
// These replace scipy/sklearn hot paths and are exposed via PyO3.

use serde::{Deserialize, Serialize};

/// Result of a linear regression fit.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegressionResult {
    pub coefficients: Vec<f64>,
    pub intercept: f64,
    pub r_squared: f64,
    pub residuals: Vec<f64>,
}

/// Result of K-means clustering.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KMeansResult {
    pub labels: Vec<usize>,
    pub centroids: Vec<Vec<f64>>,
    pub inertia: f64,
    pub n_iterations: usize,
}

/// Result of PCA dimensionality reduction.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PCAResult {
    pub components: Vec<Vec<f64>>,
    pub explained_variance: Vec<f64>,
    pub explained_variance_ratio: Vec<f64>,
    pub transformed: Vec<Vec<f64>>,
}

/// Dataset statistics.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DatasetStats {
    pub means: Vec<f64>,
    pub stds: Vec<f64>,
    pub mins: Vec<f64>,
    pub maxs: Vec<f64>,
    pub correlation_matrix: Vec<Vec<f64>>,
    pub n_samples: usize,
    pub n_features: usize,
}

// ── Ordinary Least Squares Regression ─────────────────────────────────

/// Fit OLS linear regression: y = X @ β + intercept
///
/// Uses the normal equation: β = (X'X)^(-1) X'y
pub fn linear_regression(x: &[Vec<f64>], y: &[f64]) -> RegressionResult {
    let n = x.len();
    let p = if n > 0 { x[0].len() } else { 0 };

    if n == 0 || p == 0 || n != y.len() {
        return RegressionResult {
            coefficients: vec![],
            intercept: 0.0,
            r_squared: 0.0,
            residuals: vec![],
        };
    }

    // Add intercept column (augmented X)
    let p_aug = p + 1;
    let mut xtx = vec![vec![0.0; p_aug]; p_aug];
    let mut xty = vec![0.0; p_aug];

    for i in 0..n {
        let mut row = vec![1.0]; // intercept
        row.extend_from_slice(&x[i]);

        for j in 0..p_aug {
            xty[j] += row[j] * y[i];
            for k in 0..p_aug {
                xtx[j][k] += row[j] * row[k];
            }
        }
    }

    // Solve via Cholesky-like approach (simple Gauss elimination)
    let beta = solve_linear_system(&xtx, &xty);

    let intercept = beta[0];
    let coefficients: Vec<f64> = beta[1..].to_vec();

    // Compute R-squared
    let y_mean: f64 = y.iter().sum::<f64>() / n as f64;
    let mut ss_res = 0.0;
    let mut ss_tot = 0.0;
    let mut residuals = Vec::with_capacity(n);

    for i in 0..n {
        let y_pred: f64 = intercept + coefficients.iter().zip(x[i].iter()).map(|(c, xi)| c * xi).sum::<f64>();
        let res = y[i] - y_pred;
        residuals.push(res);
        ss_res += res * res;
        ss_tot += (y[i] - y_mean) * (y[i] - y_mean);
    }

    let r_squared = if ss_tot > 0.0 { 1.0 - ss_res / ss_tot } else { 0.0 };

    RegressionResult {
        coefficients,
        intercept,
        r_squared,
        residuals,
    }
}

// ── K-Means Clustering ───────────────────────────────────────────────

/// K-means clustering using Lloyd's algorithm.
pub fn kmeans(data: &[Vec<f64>], k: usize, max_iter: usize) -> KMeansResult {
    let n = data.len();
    let dim = if n > 0 { data[0].len() } else { 0 };

    if n == 0 || k == 0 || dim == 0 {
        return KMeansResult {
            labels: vec![],
            centroids: vec![],
            inertia: 0.0,
            n_iterations: 0,
        };
    }

    let k = k.min(n);

    // Initialize centroids using first k points (k-means++ would be better, but simpler for now)
    let mut centroids: Vec<Vec<f64>> = data[..k].to_vec();
    let mut labels = vec![0usize; n];
    let mut n_iter = 0;

    for iter in 0..max_iter {
        n_iter = iter + 1;
        let mut changed = false;

        // Assignment step
        for i in 0..n {
            let mut best_dist = f64::INFINITY;
            let mut best_label = 0;
            for (c_idx, centroid) in centroids.iter().enumerate() {
                let dist: f64 = data[i].iter()
                    .zip(centroid.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum();
                if dist < best_dist {
                    best_dist = dist;
                    best_label = c_idx;
                }
            }
            if labels[i] != best_label {
                labels[i] = best_label;
                changed = true;
            }
        }

        if !changed {
            break;
        }

        // Update step
        let mut new_centroids = vec![vec![0.0; dim]; k];
        let mut counts = vec![0usize; k];

        for i in 0..n {
            let label = labels[i];
            counts[label] += 1;
            for d in 0..dim {
                new_centroids[label][d] += data[i][d];
            }
        }

        for c in 0..k {
            if counts[c] > 0 {
                for d in 0..dim {
                    new_centroids[c][d] /= counts[c] as f64;
                }
            }
        }

        centroids = new_centroids;
    }

    // Compute inertia
    let mut inertia = 0.0;
    for i in 0..n {
        let dist: f64 = data[i].iter()
            .zip(centroids[labels[i]].iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum();
        inertia += dist;
    }

    KMeansResult {
        labels,
        centroids,
        inertia,
        n_iterations: n_iter,
    }
}

// ── PCA (Power Iteration Method) ─────────────────────────────────────

/// PCA via iterative eigendecomposition of the covariance matrix.
pub fn pca(data: &[Vec<f64>], n_components: usize) -> PCAResult {
    let n = data.len();
    let dim = if n > 0 { data[0].len() } else { 0 };

    if n == 0 || dim == 0 || n_components == 0 {
        return PCAResult {
            components: vec![],
            explained_variance: vec![],
            explained_variance_ratio: vec![],
            transformed: vec![],
        };
    }

    let n_comp = n_components.min(dim);

    // Center data
    let means: Vec<f64> = (0..dim)
        .map(|d| data.iter().map(|row| row[d]).sum::<f64>() / n as f64)
        .collect();

    let centered: Vec<Vec<f64>> = data.iter()
        .map(|row| row.iter().zip(means.iter()).map(|(x, m)| x - m).collect())
        .collect();

    // Covariance matrix
    let mut cov = vec![vec![0.0; dim]; dim];
    for row in &centered {
        for i in 0..dim {
            for j in i..dim {
                cov[i][j] += row[i] * row[j];
            }
        }
    }
    for i in 0..dim {
        for j in i..dim {
            cov[i][j] /= (n - 1).max(1) as f64;
            cov[j][i] = cov[i][j];
        }
    }

    // Power iteration for top eigenvectors
    let mut components = Vec::with_capacity(n_comp);
    let mut eigenvalues = Vec::with_capacity(n_comp);
    let mut deflated_cov = cov.clone();

    for _ in 0..n_comp {
        let (eigenvalue, eigenvector) = power_iteration(&deflated_cov, 200);
        eigenvalues.push(eigenvalue);
        components.push(eigenvector.clone());

        // Deflate: C = C - λ * v * v^T
        for i in 0..dim {
            for j in 0..dim {
                deflated_cov[i][j] -= eigenvalue * eigenvector[i] * eigenvector[j];
            }
        }
    }

    let total_variance: f64 = eigenvalues.iter().sum();
    let explained_variance_ratio: Vec<f64> = eigenvalues.iter()
        .map(|&ev| if total_variance > 0.0 { ev / total_variance } else { 0.0 })
        .collect();

    // Transform data
    let transformed: Vec<Vec<f64>> = centered.iter()
        .map(|row| {
            components.iter()
                .map(|comp| row.iter().zip(comp.iter()).map(|(x, c)| x * c).sum::<f64>())
                .collect()
        })
        .collect();

    PCAResult {
        components,
        explained_variance: eigenvalues,
        explained_variance_ratio,
        transformed,
    }
}

/// Compute dataset statistics.
pub fn compute_stats(data: &[Vec<f64>]) -> DatasetStats {
    let n = data.len();
    let dim = if n > 0 { data[0].len() } else { 0 };

    if n == 0 || dim == 0 {
        return DatasetStats {
            means: vec![], stds: vec![], mins: vec![], maxs: vec![],
            correlation_matrix: vec![], n_samples: 0, n_features: 0,
        };
    }

    let means: Vec<f64> = (0..dim)
        .map(|d| data.iter().map(|row| row[d]).sum::<f64>() / n as f64)
        .collect();

    let stds: Vec<f64> = (0..dim)
        .map(|d| {
            let var: f64 = data.iter()
                .map(|row| (row[d] - means[d]).powi(2))
                .sum::<f64>() / (n - 1).max(1) as f64;
            var.sqrt()
        })
        .collect();

    let mins: Vec<f64> = (0..dim)
        .map(|d| data.iter().map(|row| row[d]).fold(f64::INFINITY, f64::min))
        .collect();

    let maxs: Vec<f64> = (0..dim)
        .map(|d| data.iter().map(|row| row[d]).fold(f64::NEG_INFINITY, f64::max))
        .collect();

    // Correlation matrix
    let mut corr = vec![vec![0.0; dim]; dim];
    for i in 0..dim {
        for j in 0..dim {
            if stds[i] > 1e-12 && stds[j] > 1e-12 {
                let cov: f64 = data.iter()
                    .map(|row| (row[i] - means[i]) * (row[j] - means[j]))
                    .sum::<f64>() / (n - 1).max(1) as f64;
                corr[i][j] = cov / (stds[i] * stds[j]);
            } else if i == j {
                corr[i][j] = 1.0;
            }
        }
    }

    DatasetStats {
        means, stds, mins, maxs,
        correlation_matrix: corr,
        n_samples: n,
        n_features: dim,
    }
}

/// Train-test split (deterministic, no shuffle).
pub fn train_test_split(
    data: &[Vec<f64>],
    labels: &[f64],
    test_ratio: f64,
) -> (Vec<Vec<f64>>, Vec<Vec<f64>>, Vec<f64>, Vec<f64>) {
    let n = data.len();
    let split_idx = ((1.0 - test_ratio) * n as f64) as usize;

    let x_train = data[..split_idx].to_vec();
    let x_test = data[split_idx..].to_vec();
    let y_train = labels[..split_idx].to_vec();
    let y_test = labels[split_idx..].to_vec();

    (x_train, x_test, y_train, y_test)
}

// ── Internal Helpers ─────────────────────────────────────────────────

/// Solve Ax = b using Gaussian elimination with partial pivoting.
fn solve_linear_system(a: &[Vec<f64>], b: &[f64]) -> Vec<f64> {
    let n = a.len();
    let mut aug: Vec<Vec<f64>> = a.iter()
        .enumerate()
        .map(|(i, row)| {
            let mut r = row.clone();
            r.push(b[i]);
            r
        })
        .collect();

    // Forward elimination
    for col in 0..n {
        // Partial pivoting
        let mut max_row = col;
        let mut max_val = aug[col][col].abs();
        for row in (col + 1)..n {
            if aug[row][col].abs() > max_val {
                max_val = aug[row][col].abs();
                max_row = row;
            }
        }
        aug.swap(col, max_row);

        let pivot = aug[col][col];
        if pivot.abs() < 1e-15 {
            continue;
        }

        for row in (col + 1)..n {
            let factor = aug[row][col] / pivot;
            for j in col..=n {
                aug[row][j] -= factor * aug[col][j];
            }
        }
    }

    // Back substitution
    let mut result = vec![0.0; n];
    for i in (0..n).rev() {
        let mut sum = aug[i][n];
        for j in (i + 1)..n {
            sum -= aug[i][j] * result[j];
        }
        if aug[i][i].abs() > 1e-15 {
            result[i] = sum / aug[i][i];
        }
    }

    result
}

/// Power iteration to find the dominant eigenvector of a symmetric matrix.
fn power_iteration(matrix: &[Vec<f64>], max_iter: usize) -> (f64, Vec<f64>) {
    let n = matrix.len();
    let mut v: Vec<f64> = vec![1.0 / (n as f64).sqrt(); n];

    let mut eigenvalue = 0.0;
    for _ in 0..max_iter {
        // w = A * v
        let w: Vec<f64> = (0..n)
            .map(|i| matrix[i].iter().zip(v.iter()).map(|(a, b)| a * b).sum::<f64>())
            .collect();

        // Eigenvalue estimate = v^T * w
        eigenvalue = v.iter().zip(w.iter()).map(|(a, b)| a * b).sum::<f64>();

        // Normalize
        let norm: f64 = w.iter().map(|x| x * x).sum::<f64>().sqrt();
        if norm < 1e-15 {
            break;
        }
        v = w.iter().map(|x| x / norm).collect();
    }

    (eigenvalue, v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_regression() {
        // y = 2*x + 1
        let x = vec![vec![1.0], vec![2.0], vec![3.0], vec![4.0], vec![5.0]];
        let y = vec![3.0, 5.0, 7.0, 9.0, 11.0];
        let result = linear_regression(&x, &y);
        assert!((result.coefficients[0] - 2.0).abs() < 0.01);
        assert!((result.intercept - 1.0).abs() < 0.01);
        assert!(result.r_squared > 0.99);
    }

    #[test]
    fn test_kmeans() {
        let data = vec![
            vec![1.0, 1.0], vec![1.1, 1.1], vec![0.9, 0.9],
            vec![5.0, 5.0], vec![5.1, 5.1], vec![4.9, 4.9],
        ];
        let result = kmeans(&data, 2, 100);
        assert_eq!(result.labels.len(), 6);
        // First three should be same cluster, last three another
        assert_eq!(result.labels[0], result.labels[1]);
        assert_eq!(result.labels[3], result.labels[4]);
        assert_ne!(result.labels[0], result.labels[3]);
    }

    #[test]
    fn test_pca() {
        let data = vec![
            vec![1.0, 2.0], vec![3.0, 4.0], vec![5.0, 6.0], vec![7.0, 8.0],
        ];
        let result = pca(&data, 1);
        assert_eq!(result.components.len(), 1);
        assert_eq!(result.transformed.len(), 4);
    }

    #[test]
    fn test_dataset_stats() {
        let data = vec![vec![1.0, 2.0], vec![3.0, 4.0], vec![5.0, 6.0]];
        let stats = compute_stats(&data);
        assert_eq!(stats.n_samples, 3);
        assert_eq!(stats.n_features, 2);
        assert!((stats.means[0] - 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_train_test_split() {
        let data = vec![vec![1.0], vec![2.0], vec![3.0], vec![4.0], vec![5.0]];
        let labels = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let (x_train, x_test, y_train, y_test) = train_test_split(&data, &labels, 0.2);
        assert_eq!(x_train.len(), 4);
        assert_eq!(x_test.len(), 1);
        assert_eq!(y_train.len(), 4);
        assert_eq!(y_test.len(), 1);
    }
}
