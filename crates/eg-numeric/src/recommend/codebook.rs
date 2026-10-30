//! Residual quantization with seeded k-means per level.

use super::{sq_dist, RecommendError};

pub const MAX_LEVELS: usize = 4;
pub const MAX_CODES: usize = 256;
/// Lloyd iterations per level.
const ITERATIONS: usize = 16;

/// SplitMix64: the seeded, platform-independent generator behind centroid
/// initialisation.
struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// `levels` codebooks of up to `codes` centroids each, frozen once fitted:
/// a new item encodes against the same codebooks.
#[derive(Debug, Clone, PartialEq)]
pub struct ResidualQuantizer {
    dim: usize,
    /// `levels × codes × dim`.
    codebooks: Vec<Vec<Vec<f32>>>,
}

fn nearest(centroids: &[Vec<f32>], v: &[f32]) -> usize {
    let mut best = (0usize, f32::INFINITY);
    for (index, centroid) in centroids.iter().enumerate() {
        let distance = sq_dist(centroid, v);
        if distance < best.1 {
            best = (index, distance);
        }
    }
    best.0
}

/// Seeded k-means: `k` distinct initial points by a partial Fisher-Yates
/// shuffle, then fixed Lloyd iterations. An empty cluster keeps its centroid.
fn kmeans(points: &[Vec<f32>], k: usize, rng: &mut SplitMix) -> Vec<Vec<f32>> {
    let mut order: Vec<usize> = (0..points.len()).collect();
    for i in 0..k {
        let j = i + (rng.next() % (order.len() - i) as u64) as usize;
        order.swap(i, j);
    }
    let mut centroids: Vec<Vec<f32>> = order[..k].iter().map(|&i| points[i].clone()).collect();
    let dim = points[0].len();
    for _ in 0..ITERATIONS {
        let mut sums = vec![vec![0.0f32; dim]; k];
        let mut counts = vec![0usize; k];
        for point in points {
            let c = nearest(&centroids, point);
            counts[c] += 1;
            sums[c].iter_mut().zip(point).for_each(|(s, x)| *s += x);
        }
        for ((centroid, sum), count) in centroids.iter_mut().zip(sums).zip(counts) {
            if count > 0 {
                *centroid = sum.into_iter().map(|s| s / count as f32).collect();
            }
        }
    }
    centroids
}

impl ResidualQuantizer {
    /// Fit `levels` codebooks of at most `codes` centroids over `items`.
    pub fn fit(
        items: &[Vec<f32>],
        levels: usize,
        codes: usize,
        seed: u64,
    ) -> Result<Self, RecommendError> {
        if !(1..=MAX_LEVELS).contains(&levels) || !(1..=MAX_CODES).contains(&codes) {
            return Err(RecommendError::BadShape);
        }
        let dim = items.first().ok_or(RecommendError::EmptyCatalog)?.len();
        if let Some(bad) = items.iter().find(|item| item.len() != dim) {
            return Err(RecommendError::DimensionMismatch {
                expected: dim,
                found: bad.len(),
            });
        }
        let mut rng = SplitMix(seed);
        let mut residuals: Vec<Vec<f32>> = items.to_vec();
        let mut codebooks = Vec::with_capacity(levels);
        for _ in 0..levels {
            let centroids = kmeans(&residuals, codes.min(residuals.len()), &mut rng);
            for residual in &mut residuals {
                let c = nearest(&centroids, residual);
                residual
                    .iter_mut()
                    .zip(&centroids[c])
                    .for_each(|(r, x)| *r -= x);
            }
            codebooks.push(centroids);
        }
        Ok(Self { dim, codebooks })
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn levels(&self) -> usize {
        self.codebooks.len()
    }

    /// The code tuple of `v`, unused levels zero. O(L·C·d).
    pub fn encode(&self, v: &[f32]) -> Result<[u8; 4], RecommendError> {
        if v.len() != self.dim {
            return Err(RecommendError::DimensionMismatch {
                expected: self.dim,
                found: v.len(),
            });
        }
        let mut residual = v.to_vec();
        let mut codes = [0u8; 4];
        for (level, centroids) in self.codebooks.iter().enumerate() {
            let c = nearest(centroids, &residual);
            codes[level] = c as u8;
            residual
                .iter_mut()
                .zip(&centroids[c])
                .for_each(|(r, x)| *r -= x);
        }
        Ok(codes)
    }
}
