use nalgebra::{DMatrix, DVector, SymmetricEigen};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct ClusterResult {
    pub cluster_id: String,
    pub label: String,
    pub indices: Vec<usize>,
    pub centroid: Vec<f64>,
    pub coherence: f64,
}

pub struct SpectralClusterNavigator {
    min_cluster_size: usize,
    max_depth: usize,
}

impl SpectralClusterNavigator {
    pub fn new(min_cluster_size: usize, max_depth: usize) -> Self {
        Self {
            min_cluster_size,
            max_depth,
        }
    }

    fn cosine_similarity_matrix(&self, vectors: &DMatrix<f64>) -> DMatrix<f64> {
        let (n, d) = vectors.shape();
        let mut normalized = DMatrix::zeros(n, d);
        for i in 0..n {
            let row = vectors.row(i);
            let norm = row.norm();
            let norm = if norm == 0.0 { 1.0 } else { norm };
            normalized.set_row(i, &(row / norm));
        }

        let mut similarity = &normalized * &normalized.transpose();
        
        // Clip to [0, 1]
        for v in similarity.iter_mut() {
            *v = v.clamp(0.0, 1.0);
        }
        similarity
    }

    fn normalized_laplacian(&self, mut affinity: DMatrix<f64>) -> DMatrix<f64> {
        let n = affinity.shape().0;
        for i in 0..n {
            affinity[(i, i)] = 0.0;
        }

        let mut degree_inv_sqrt = DMatrix::zeros(n, n);
        for i in 0..n {
            let sum: f64 = affinity.row(i).iter().sum();
            if sum > 0.0 {
                degree_inv_sqrt[(i, i)] = 1.0 / sum.sqrt();
            }
        }

        let i_mat = DMatrix::identity(n, n);
        i_mat - &degree_inv_sqrt * affinity * &degree_inv_sqrt
    }

    fn eigengap_k(&self, eigenvalues: &DVector<f64>, max_k: usize) -> usize {
        let n = eigenvalues.len();
        if n < 3 {
            return std::cmp::min(2, n);
        }

        let mut sorted_vals: Vec<f64> = eigenvalues.iter().cloned().collect();
        sorted_vals.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let upper = std::cmp::min(max_k, n - 1);
        if upper < 2 {
            return 2;
        }

        let mut max_gap = -1.0;
        let mut best_k = 2;
        for i in 1..upper {
            let gap = sorted_vals[i + 1] - sorted_vals[i];
            if gap > max_gap {
                max_gap = gap;
                best_k = i + 1; // +1 because we are looking at diffs, and skip evalue 0 (which is at index 0) wait, Python skips index 0 (which is 0.0), so diffs are from 1. 
                // Wait, Python: sorted_vals[1 : upper + 1], diffs between them.
                // In Python: `best_k = int(np.argmax(gaps)) + 2`.
            }
        }

        // Adjusting logic to match Python exactly:
        let mut best_k = 2;
        let mut max_gap = -1.0;
        // In python: gaps = np.diff(sorted_vals[1:upper+1])
        for i in 1..upper {
            let gap = sorted_vals[i + 1] - sorted_vals[i];
            if gap > max_gap {
                max_gap = gap;
                best_k = i - 1 + 2; // i=1 -> 0th gap -> best_k=2
            }
        }

        std::cmp::max(2, std::cmp::min(best_k, max_k))
    }

    fn cluster_coherence(&self, vectors: &DMatrix<f64>, indices: &[usize]) -> f64 {
        if indices.len() < 2 {
            return 1.0;
        }
        let n = indices.len();
        let d = vectors.shape().1;
        
        let mut cluster_vecs = DMatrix::zeros(n, d);
        for (i, &idx) in indices.iter().enumerate() {
            cluster_vecs.set_row(i, &vectors.row(idx));
        }

        let mut normalized = DMatrix::zeros(n, d);
        for i in 0..n {
            let row = cluster_vecs.row(i);
            let norm = row.norm();
            let norm = if norm == 0.0 { 1.0 } else { norm };
            normalized.set_row(i, &(row / norm));
        }

        let sims = &normalized * &normalized.transpose();
        let upper_sum = (sims.sum() - n as f64) / 2.0;
        let pair_count = (n * (n - 1)) as f64 / 2.0;

        if pair_count > 0.0 {
            upper_sum / pair_count
        } else {
            1.0
        }
    }

    fn kmeans(&self, data: &DMatrix<f64>, k: usize, max_iters: usize) -> Vec<usize> {
        let n = data.shape().0;
        if k >= n {
            return (0..n).collect();
        }

        let mut rng = rand::thread_rng();
        let mut centroids = DMatrix::zeros(k, data.shape().1);
        
        // k-means++ simplified
        let mut all_indices: Vec<usize> = (0..n).collect();
        all_indices.shuffle(&mut rng);
        for c in 0..k {
            centroids.set_row(c, &data.row(all_indices[c]));
        }

        let mut labels = vec![0; n];

        for _ in 0..max_iters {
            let mut new_labels = vec![0; n];
            for i in 0..n {
                let row = data.row(i);
                let mut min_dist = f64::MAX;
                let mut min_idx = 0;
                for j in 0..k {
                    let c_row = centroids.row(j);
                    let dist = (row - c_row).norm_squared();
                    if dist < min_dist {
                        min_dist = dist;
                        min_idx = j;
                    }
                }
                new_labels[i] = min_idx;
            }

            if new_labels == labels {
                break;
            }
            labels = new_labels;

            for j in 0..k {
                let mut sum = DVector::zeros(data.shape().1);
                let mut count = 0;
                for (i, &lbl) in labels.iter().enumerate() {
                    if lbl == j {
                        sum += data.row(i).transpose();
                        count += 1;
                    }
                }
                if count > 0 {
                    centroids.set_row(j, &(sum.transpose() / count as f64));
                }
            }
        }

        labels
    }

    pub fn cluster(&self, vectors: Vec<Vec<f64>>, max_k: usize, domain: &str) -> Vec<ClusterResult> {
        let n = vectors.len();
        if n == 0 {
            return vec![];
        }
        let d = vectors[0].len();
        let mut mat = DMatrix::zeros(n, d);
        for i in 0..n {
            for j in 0..d {
                mat[(i, j)] = vectors[i][j];
            }
        }

        if n < 2 {
            return vec![ClusterResult {
                cluster_id: format!("sc_{}", uuid::Uuid::new_v4().simple().to_string()[..8].to_string()),
                label: format!("{}_singleton", domain),
                indices: (0..n).collect(),
                centroid: if n > 0 { vectors[0].clone() } else { vec![] },
                coherence: 1.0,
            }];
        }

        let affinity = self.cosine_similarity_matrix(&mat);
        let laplacian = self.normalized_laplacian(affinity);

        let eig = SymmetricEigen::new(laplacian);
        let eigenvalues = eig.eigenvalues;
        let eigenvectors = eig.eigenvectors;

        // eigenvalues are not necessarily sorted by SymmetricEigen, so we need to sort them
        let mut eig_pairs: Vec<(f64, Vec<f64>)> = Vec::new();
        for i in 0..n {
            let mut col = Vec::new();
            for j in 0..n {
                col.push(eigenvectors[(j, i)]);
            }
            eig_pairs.push((eigenvalues[i], col));
        }
        eig_pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        let num_eigs = std::cmp::min(max_k + 1, n);
        let mut sorted_evals = DVector::zeros(num_eigs);
        let mut sorted_evecs = DMatrix::zeros(n, num_eigs);
        for i in 0..num_eigs {
            sorted_evals[i] = eig_pairs[i].0;
            for j in 0..n {
                sorted_evecs[(j, i)] = eig_pairs[i].1[j];
            }
        }

        let k = self.eigengap_k(&sorted_evals, max_k);
        let mut spectral_embedding = sorted_evecs.columns(0, k).into_owned();

        for i in 0..n {
            let row = spectral_embedding.row(i);
            let norm = row.norm();
            let norm = if norm == 0.0 { 1.0 } else { norm };
            spectral_embedding.set_row(i, &(row / norm));
        }

        let labels = self.kmeans(&spectral_embedding, k, 100);

        let mut results = Vec::new();
        for cluster_idx in 0..k {
            let mut member_indices = Vec::new();
            for (i, &lbl) in labels.iter().enumerate() {
                if lbl == cluster_idx {
                    member_indices.push(i);
                }
            }

            if member_indices.len() < self.min_cluster_size {
                continue;
            }

            let mut centroid = vec![0.0; d];
            for &idx in &member_indices {
                for j in 0..d {
                    centroid[j] += mat[(idx, j)];
                }
            }
            for j in 0..d {
                centroid[j] /= member_indices.len() as f64;
            }

            let coherence = self.cluster_coherence(&mat, &member_indices);
            
            results.push(ClusterResult {
                cluster_id: format!("sc_{}", uuid::Uuid::new_v4().simple().to_string()[..8].to_string()),
                label: format!("{}_cluster_{}", domain, cluster_idx),
                indices: member_indices,
                centroid,
                coherence,
            });
        }

        results.sort_by(|a, b| b.indices.len().cmp(&a.indices.len()));
        results
    }
}
