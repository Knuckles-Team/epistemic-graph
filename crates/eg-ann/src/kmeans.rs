//! Lloyd's k-means with k-means++ seeding. Shared by the IVF coarse quantizer,
//! the PQ subspace codebooks, and OPQ's per-iteration PQ retrain.
//!
//! The seeding matters for recall: the spike used a plain random-sample init and
//! deliberately-low iteration counts, which produced poor codebooks. k-means++
//! spreads the initial centroids by D² sampling, which converges to far tighter
//! clusters in the same iteration budget — the single biggest lever on raw-PQ
//! candidate quality, hence on the recall the refine tier can recover.

use rand::Rng;
use rand_chacha::ChaCha8Rng;
use rayon::prelude::*;

/// Squared Euclidean distance between two equal-length slices. CONCEPT:EG-KG.compute.lane-chunked-dot-product —
/// SIMD-friendly: 8 independent accumulator lanes over `chunks_exact(8)` (no FP
/// dependency chain, no per-element bounds check) so it autovectorizes to packed
/// AVX2 over the 1024-dim contiguous slices. Hot in the brute-force fallback, the
/// coarse cell scan, the SQ8 refine, and the worst-fit re-seed.
#[inline]
pub fn sq_dist(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = [0.0f32; 8];
    let mut ca = a.chunks_exact(8);
    let mut cb = b.chunks_exact(8);
    for (x, y) in ca.by_ref().zip(cb.by_ref()) {
        for l in 0..8 {
            let diff = x[l] - y[l];
            acc[l] += diff * diff;
        }
    }
    let mut d = ((acc[0] + acc[4]) + (acc[1] + acc[5])) + ((acc[2] + acc[6]) + (acc[3] + acc[7]));
    for (x, y) in ca.remainder().iter().zip(cb.remainder()) {
        let diff = x - y;
        d += diff * diff;
    }
    d
}

/// Squared distance with an early-out once the running sum exceeds `bound`. The
/// branch-per-element BLOCKS vectorization, so this is kept ONLY for the k-means
/// assignment loop (`nearest_centroid`), where pruning far centroids early wins more
/// than vectorization would; everything else uses the vectorized `sq_dist`.
#[inline]
fn sq_dist_bounded(v: &[f32], centroids: &[f32], base: usize, dim: usize, bound: f32) -> f32 {
    let mut d = 0.0f32;
    for j in 0..dim {
        let diff = v[j] - centroids[base + j];
        d += diff * diff;
        if d >= bound {
            return d;
        }
    }
    d
}

/// Index of the nearest of `k` flattened `dim`-vectors to `v`.
#[inline]
pub fn nearest_centroid(v: &[f32], centroids: &[f32], dim: usize, k: usize) -> usize {
    let mut best = 0usize;
    let mut bestd = f32::MAX;
    for c in 0..k {
        let d = sq_dist_bounded(v, centroids, c * dim, dim, bestd);
        if d < bestd {
            bestd = d;
            best = c;
        }
    }
    best
}

/// k-means++ initial centroid selection (D²-weighted sampling). Returns `k*dim`.
fn kmeanspp_init(data: &[Vec<f32>], dim: usize, k: usize, rng: &mut ChaCha8Rng) -> Vec<f32> {
    let n = data.len();
    let mut centroids = vec![0.0f32; k * dim];
    // First centroid: uniform random.
    let first = rng.gen_range(0..n);
    centroids[0..dim].copy_from_slice(&data[first]);

    // Running nearest-centroid² distance for every point.
    let mut d2: Vec<f32> = data.par_iter().map(|p| sq_dist(p, &data[first])).collect();

    for c in 1..k {
        // Sample the next centroid proportional to d2.
        let total: f64 = d2.iter().map(|&x| x as f64).sum();
        let next = if total <= 0.0 {
            rng.gen_range(0..n)
        } else {
            let mut target = rng.gen::<f64>() * total;
            let mut chosen = n - 1;
            for (i, &w) in d2.iter().enumerate() {
                target -= w as f64;
                if target <= 0.0 {
                    chosen = i;
                    break;
                }
            }
            chosen
        };
        centroids[c * dim..(c + 1) * dim].copy_from_slice(&data[next]);
        // Update d2 with the new centroid.
        let newc = data[next].clone();
        d2.par_iter_mut()
            .zip(data.par_iter())
            .for_each(|(dist, p)| {
                let ndist = sq_dist(p, &newc);
                if ndist < *dist {
                    *dist = ndist;
                }
            });
    }
    centroids
}

/// Train `k` centroids over `data` (each `dim`-long), `iters` Lloyd refinements
/// after a k-means++ seed. Returns flattened `k*dim` centroids. Empty clusters
/// are re-seeded from the point furthest from its assigned centroid (splits the
/// largest cluster), which keeps every codebook entry useful.
pub fn kmeans(
    data: &[Vec<f32>],
    dim: usize,
    k: usize,
    iters: usize,
    rng: &mut ChaCha8Rng,
) -> Vec<f32> {
    let n = data.len();
    let k = k.min(n.max(1));
    if n == 0 {
        return vec![0.0f32; k * dim];
    }
    let mut centroids = kmeanspp_init(data, dim, k, rng);

    for _ in 0..iters {
        // Assign every point to its nearest centroid (parallel).
        let assign: Vec<usize> = data
            .par_iter()
            .map(|v| nearest_centroid(v, &centroids, dim, k))
            .collect();

        // Accumulate sums + counts. CONCEPT:EG-KG.compute.kmeans-accumulation — the per-iteration accumulation
        // is O(n*dim) (the coarse quantizer at dim=1024, n≈16k is ~16M adds/iter);
        // fold per-thread partials then reduce so it runs across all cores instead
        // of a single-threaded scan.
        let (sums, counts) = data
            .par_iter()
            .zip(assign.par_iter())
            .fold(
                || (vec![0.0f64; k * dim], vec![0u64; k]),
                |(mut sums, mut counts), (v, &a)| {
                    counts[a] += 1;
                    let base = a * dim;
                    for d in 0..dim {
                        sums[base + d] += v[d] as f64;
                    }
                    (sums, counts)
                },
            )
            .reduce(
                || (vec![0.0f64; k * dim], vec![0u64; k]),
                |(mut s1, mut c1), (s2, c2)| {
                    for (a, b) in s1.iter_mut().zip(s2.iter()) {
                        *a += *b;
                    }
                    for (a, b) in c1.iter_mut().zip(c2.iter()) {
                        *a += *b;
                    }
                    (s1, c1)
                },
            );

        for c in 0..k {
            if counts[c] == 0 {
                // Re-seed an empty cluster from the worst-fit point so no codebook
                // slot is wasted (the spike re-seeded arbitrarily).
                if let Some(worst) = worst_fit_point(data, &centroids, dim, k, &assign) {
                    centroids[c * dim..(c + 1) * dim].copy_from_slice(&data[worst]);
                }
                continue;
            }
            let base = c * dim;
            let inv = 1.0 / counts[c] as f64;
            for d in 0..dim {
                centroids[base + d] = (sums[base + d] * inv) as f32;
            }
        }
    }
    centroids
}

/// The point with the largest distance to its assigned centroid (for empty-cell
/// re-seeding). `O(n*dim)` — only called when a cluster collapses.
fn worst_fit_point(
    data: &[Vec<f32>],
    centroids: &[f32],
    dim: usize,
    _k: usize,
    assign: &[usize],
) -> Option<usize> {
    data.iter()
        .zip(assign.iter())
        .enumerate()
        .map(|(i, (v, &a))| (i, sq_dist(v, &centroids[a * dim..(a + 1) * dim])))
        .max_by(|x, y| x.1.partial_cmp(&y.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(i, _)| i)
}
