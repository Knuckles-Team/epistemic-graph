//! Unsupervised clustering — the `KMeans` surface (CONCEPT:EG-KG.query.kmeans-clustering-half-one). A slim, pure-Rust
//! Lloyd's-algorithm k-means (k-means++ seeding) over an `ndarray` matrix, using the
//! kernel's own seedable RNG ([`crate::random::Generator`], ChaCha20) so a run is
//! **deterministic for a given seed** — no `linfa`/BLAS dependency, keeping the Pi
//! contract (`cargo tree --features pi` links no eg-numeric/nalgebra/ndarray/linfa).
//!
//! This is the clustering kernel behind the Surface-B `kmeans(vec_col, k)` DataFusion
//! UDAF (`crates/eg-query/src/sql/numeric.rs`): a column of vectors is marshalled into an
//! `n×d` matrix and clustered IN-ENGINE (compute-near-data), returning one cluster label
//! per row — the "cluster the joined result in-engine" half of the cross-modal analytics
//! differentiator (CONCEPT:EG-KG.query.eg-3).

use crate::error::{NumericError, Result};
use crate::random::Generator;
use ndarray::{Array2, ArrayView2};

/// The result of a [`kmeans`] fit: a hard label per input row plus the fitted centroids
/// and the final inertia (within-cluster sum of squared distances).
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "contract", derive(serde::Serialize, serde::Deserialize))]
pub struct KMeansResult {
    /// Cluster index (`0..k`) for each input row, in input order.
    pub labels: Vec<usize>,
    /// The `k×d` centroid matrix (one centroid per row).
    pub centroids: Array2<f64>,
    /// Within-cluster sum of squared distances (Lloyd's objective) at convergence.
    pub inertia: f64,
    /// Number of Lloyd iterations actually run (≤ `max_iter`).
    pub n_iter: usize,
}

/// Squared Euclidean distance between a data row and a centroid row.
fn sq_dist(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// Nearest centroid (lowest index wins ties) + its squared distance, for `point`.
fn nearest(point: &[f64], centroids: &Array2<f64>) -> (usize, f64) {
    let mut best = 0usize;
    let mut best_d = f64::INFINITY;
    for (c, centroid) in centroids.rows().into_iter().enumerate() {
        let d = sq_dist(point, centroid.as_slice().unwrap());
        if d < best_d {
            best_d = d;
            best = c;
        }
    }
    (best, best_d)
}

/// Sample the next k-means++ centroid's row index, weighted by each point's squared
/// distance `d2[i]` to the nearest centroid chosen so far. Falls back to a uniform pick
/// when every point already coincides with a centroid (`total <= 0.0`).
fn sample_weighted_centroid_index(d2: &[f64], n: usize, gen: &mut Generator) -> usize {
    let total: f64 = d2.iter().sum();
    if total <= 0.0 {
        return gen.integers(0, n as i64, 1)[0] as usize;
    }
    let target = gen.uniform(0.0, total, 1)[0];
    let mut acc = 0.0;
    for (i, &w) in d2.iter().enumerate() {
        acc += w;
        if acc >= target {
            return i;
        }
    }
    n - 1
}

/// Update each point's running nearest-centroid squared distance after `new_centroid`
/// (row `d` values) joins the chosen set, in place.
fn update_nearest_sq_dist(data: ArrayView2<f64>, new_centroid: &[f64], d2: &mut [f64]) {
    for (i, slot) in d2.iter_mut().enumerate() {
        let dist = sq_dist(data.row(i).as_slice().unwrap(), new_centroid);
        if dist < *slot {
            *slot = dist;
        }
    }
}

/// k-means++ seeding — the deterministic-given-`seed` probabilistic init that spreads the
/// initial centroids proportionally to squared distance, yielding far better/stabler
/// clusters than a plain random pick. Returns a `k×d` centroid matrix.
fn kmeans_plus_plus(data: ArrayView2<f64>, k: usize, gen: &mut Generator) -> Array2<f64> {
    let (n, d) = data.dim();
    let mut centroids = Array2::<f64>::zeros((k, d));
    // First centroid: a uniformly-random row.
    let first = gen.integers(0, n as i64, 1)[0] as usize;
    centroids.row_mut(0).assign(&data.row(first));
    // Remaining centroids: sample row i with probability ∝ D(i)² (distance to the nearest
    // already-chosen centroid).
    let mut d2: Vec<f64> = (0..n)
        .map(|i| sq_dist(data.row(i).as_slice().unwrap(), &centroids.row(0).to_vec()))
        .collect();
    for c in 1..k {
        let chosen = sample_weighted_centroid_index(&d2, n, gen);
        centroids.row_mut(c).assign(&data.row(chosen));
        // Update each point's nearest-centroid squared distance with the new centroid.
        let new_c = centroids.row(c).to_vec();
        update_nearest_sq_dist(data, &new_c, &mut d2);
    }
    centroids
}

/// Assign every row to its nearest centroid, preserving input order and the
/// lowest-index tie break in [`nearest`].
fn assign_labels(data: ArrayView2<f64>, centroids: &Array2<f64>, labels: &mut [usize]) -> bool {
    let mut changed = false;
    for (index, label) in labels.iter_mut().enumerate() {
        let (cluster, _) = nearest(data.row(index).as_slice().unwrap(), centroids);
        if *label != cluster {
            *label = cluster;
            changed = true;
        }
    }
    changed
}

/// Accumulate centroid numerators in the same row and column order as the
/// original Lloyd update.
fn centroid_totals(
    data: ArrayView2<f64>,
    labels: &[usize],
    k: usize,
    dimensions: usize,
) -> (Array2<f64>, Vec<usize>) {
    let mut sums = Array2::<f64>::zeros((k, dimensions));
    let mut counts = vec![0usize; k];
    for (index, &cluster) in labels.iter().enumerate() {
        counts[cluster] += 1;
        let mut row = sums.row_mut(cluster);
        row += &data.row(index);
    }
    (sums, counts)
}

fn farthest_point(data: ArrayView2<f64>, centroids: &Array2<f64>) -> usize {
    let mut worst = 0usize;
    let mut worst_distance = f64::NEG_INFINITY;
    for index in 0..data.nrows() {
        let (_, distance) = nearest(data.row(index).as_slice().unwrap(), centroids);
        if distance > worst_distance {
            worst_distance = distance;
            worst = index;
        }
    }
    worst
}

/// Apply centroid means and the existing farthest-point empty-cluster repair
/// in ascending cluster order.
fn update_centroids(
    data: ArrayView2<f64>,
    centroids: &mut Array2<f64>,
    sums: &Array2<f64>,
    counts: &[usize],
) -> bool {
    let mut repaired_empty = false;
    for cluster in 0..centroids.nrows() {
        if counts[cluster] > 0 {
            let inverse = 1.0 / counts[cluster] as f64;
            for dimension in 0..centroids.ncols() {
                centroids[[cluster, dimension]] = sums[[cluster, dimension]] * inverse;
            }
        } else {
            let worst = farthest_point(data, centroids);
            centroids.row_mut(cluster).assign(&data.row(worst));
            repaired_empty = true;
        }
    }
    repaired_empty
}

fn lloyd_step(data: ArrayView2<f64>, centroids: &mut Array2<f64>, labels: &mut [usize]) -> bool {
    let labels_changed = assign_labels(data, centroids, labels);
    let (sums, counts) = centroid_totals(data, labels, centroids.nrows(), centroids.ncols());
    let repaired_empty = update_centroids(data, centroids, &sums, &counts);
    labels_changed || repaired_empty
}

fn settled_inertia(data: ArrayView2<f64>, labels: &[usize], centroids: &Array2<f64>) -> f64 {
    (0..data.nrows())
        .map(|index| {
            let cluster = labels[index];
            sq_dist(
                data.row(index).as_slice().unwrap(),
                centroids.row(cluster).as_slice().unwrap(),
            )
        })
        .sum()
}

/// Fit **k-means** on the `n×d` matrix `data` (rows = observations), returning a hard
/// cluster label per row (CONCEPT:EG-KG.query.kmeans-clustering-half-one). Lloyd's algorithm with k-means++ seeding; the
/// RNG is seeded from `seed` so the fit is reproducible. `k` is clamped to `n` (can't have
/// more clusters than points). Convergence = labels stop changing (or `max_iter`).
///
/// Errors ([`NumericError::Shape`]) on an empty matrix or `k == 0`. Empty clusters are
/// re-seeded to the point farthest from its centroid, so exactly `k` non-empty clusters
/// are returned whenever `k ≤ n`.
pub fn kmeans(data: ArrayView2<f64>, k: usize, max_iter: usize, seed: u64) -> Result<KMeansResult> {
    let (n, d) = data.dim();
    if n == 0 || d == 0 {
        return Err(NumericError::shape("kmeans: empty data matrix"));
    }
    if k == 0 {
        return Err(NumericError::shape("kmeans: k must be >= 1"));
    }
    let k = k.min(n);
    let mut gen = Generator::new(seed);
    let mut centroids = kmeans_plus_plus(data, k, &mut gen);
    let mut labels = vec![usize::MAX; n];
    let mut n_iter = 0;
    let max_iter = max_iter.max(1);

    for it in 0..max_iter {
        n_iter = it + 1;
        if !lloyd_step(data, &mut centroids, &mut labels) {
            break;
        }
    }

    // Final inertia (within-cluster sum of squared distances) over the settled labels.
    let inertia = settled_inertia(data, &labels, &centroids);

    Ok(KMeansResult {
        labels,
        centroids,
        inertia,
        n_iter,
    })
}

/// Convenience: the cluster labels only (CONCEPT:EG-KG.query.kmeans-clustering-half-one) — the shape the Surface-B
/// `kmeans(vec_col, k)` UDAF returns. Uses a fixed default `max_iter`/`seed` so a SQL
/// caller gets a deterministic assignment without threading extra arguments.
pub fn kmeans_labels(data: ArrayView2<f64>, k: usize) -> Result<Vec<usize>> {
    Ok(kmeans(data, k, 100, KMEANS_DEFAULT_SEED)?.labels)
}

/// The default RNG seed for the Surface-B `kmeans` UDAF, so in-engine clustering is
/// reproducible run-to-run (a SQL aggregate has nowhere to thread a seed).
pub const KMEANS_DEFAULT_SEED: u64 = 42;

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::array;

    #[test]
    fn two_well_separated_blobs() {
        // Two tight blobs around (0,0) and (10,10): k-means must split them cleanly.
        let data = array![
            [0.0, 0.0],
            [0.1, -0.1],
            [-0.1, 0.2],
            [10.0, 10.0],
            [10.1, 9.9],
            [9.8, 10.2],
        ];
        let res = kmeans(data.view(), 2, 100, 7).unwrap();
        // The three low points share a label; the three high points share the other.
        assert_eq!(res.labels[0], res.labels[1]);
        assert_eq!(res.labels[1], res.labels[2]);
        assert_eq!(res.labels[3], res.labels[4]);
        assert_eq!(res.labels[4], res.labels[5]);
        assert_ne!(res.labels[0], res.labels[3]);
        // Two distinct clusters, low inertia (points hug their centroids).
        assert!(res.inertia < 1.0, "inertia: {}", res.inertia);
    }

    #[test]
    fn deterministic_given_seed() {
        let data = array![[1.0, 1.0], [1.2, 0.9], [8.0, 8.0], [8.1, 7.8], [4.0, 9.0]];
        let a = kmeans(data.view(), 2, 100, 123).unwrap();
        let b = kmeans(data.view(), 2, 100, 123).unwrap();
        assert_eq!(a.labels, b.labels);
        assert_eq!(a.centroids, b.centroids);
    }

    #[test]
    fn fixed_single_cluster_numeric_oracle() {
        let data = array![[3.0, 4.0], [-1.0, 2.0], [5.0, -6.0], [1.0, 0.0]];
        let result = kmeans(data.view(), 1, 100, 987_654_321).unwrap();
        assert_eq!(result.labels, vec![0, 0, 0, 0]);
        assert_eq!(result.centroids, array![[2.0, 0.0]]);
        assert_eq!(result.inertia, 76.0);
        assert_eq!(result.n_iter, 2);
    }

    #[test]
    fn k_clamped_to_n() {
        // k > n: clamp to n, every point its own cluster, zero inertia.
        let data = array![[0.0], [5.0]];
        let res = kmeans(data.view(), 5, 100, 1).unwrap();
        assert_eq!(res.centroids.nrows(), 2);
        assert!((res.inertia).abs() < 1e-12);
        assert_ne!(res.labels[0], res.labels[1]);
    }

    #[test]
    fn empty_and_zero_k_error() {
        let empty = Array2::<f64>::zeros((0, 3));
        assert!(kmeans(empty.view(), 2, 10, 1).is_err());
        let data = array![[1.0, 2.0]];
        assert!(kmeans(data.view(), 0, 10, 1).is_err());
    }
}
