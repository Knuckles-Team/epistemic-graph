// CONCEPT:EG-KG.mining.isolation-forest — anomaly / outlier detection.
//
// Pure-Rust, dependency-light, batch (one round-trip): given a feature matrix
// (each row a point in R^d), score every row for how anomalous it is and flag the
// rows over a threshold. Four interchangeable detectors:
//
//   * z-score / MAD        (CONCEPT:EG-KG.mining.zscore-mad) — the robust modified
//     z-score (median + median-absolute-deviation), per-feature, aggregated.
//   * Isolation Forest     (CONCEPT:EG-KG.mining.isolation-forest) — a random tree
//     ensemble; anomalies isolate in short paths → high score.
//   * LOF                  (CONCEPT:EG-KG.mining.lof-local-density) — Local Outlier
//     Factor: a point in a sparser neighborhood than its k neighbors scores > 1.
//   * One-Class SVM        (CONCEPT:EG-KG.mining.oneclass-svm) — Schölkopf's ν-SVM
//     boundary fit by a lightweight SMO; points outside the boundary are anomalies.
//
// Every detector returns a per-row `score` where HIGHER = more anomalous, so a
// single `threshold` (or a per-algorithm default) yields `is_anomaly`. Isolation
// Forest and OCSVM (its SMO pass order) are seeded/deterministic; z-score and LOF
// are order-free. This module is graph/timeseries-agnostic: the handler
// (`src/server/handlers/mining.rs`) supplies rows (explicit features, a 1-D series
// window, or node embeddings) and does the KG write-back.

/// A point in feature space (one matrix row).
pub type Point = Vec<f64>;

/// Which detector to run, with its parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Algorithm {
    /// Robust modified z-score over each feature; the row score is the max over
    /// features (an anomaly in any one dimension flags the row).
    ZScoreMad,
    /// Isolation Forest: `n_trees` trees each on a `sample_size` subsample.
    IsolationForest {
        n_trees: usize,
        sample_size: usize,
        seed: u64,
    },
    /// Local Outlier Factor with `k` neighbors.
    Lof { k: usize },
    /// One-Class ν-SVM boundary (`nu` ∈ (0,1]) under `kernel`.
    OneClassSvm { kernel: Kernel, nu: f64 },
}

/// One-Class SVM kernel.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Kernel {
    Linear,
    /// RBF `exp(-gamma·‖x−y‖²)`; `gamma ≤ 0` ⇒ the `1/n_features` default.
    Rbf {
        gamma: f64,
    },
}

/// The detection outcome, parallel to the input rows: a per-row anomaly `score`
/// (higher = more anomalous) and the boolean `is_anomaly` at the applied
/// `threshold`.
#[derive(Debug, Clone, PartialEq)]
pub struct Anomalies {
    pub scores: Vec<f64>,
    pub is_anomaly: Vec<bool>,
    pub threshold: f64,
}

/// The default flag threshold for an algorithm when the caller passes none.
pub fn default_threshold(algorithm: Algorithm) -> f64 {
    match algorithm {
        // Iglewicz–Hoaglin: |modified z| > 3.5 is a potential outlier.
        Algorithm::ZScoreMad => 3.5,
        // Path-length anomaly score ∈ (0,1); > 0.6 is the common cutoff.
        Algorithm::IsolationForest { .. } => 0.6,
        // LOF ≈ 1 for inliers; > 1.5 marks a local outlier.
        Algorithm::Lof { .. } => 1.5,
        // OCSVM score = −f(x); a point outside the boundary has f(x) < 0 ⇒ score > 0.
        Algorithm::OneClassSvm { .. } => 0.0,
    }
}

/// Run the chosen detector over `points`. `threshold` overrides the per-algorithm
/// default. An empty input yields an empty result (never a panic).
pub fn detect(points: &[Point], algorithm: Algorithm, threshold: Option<f64>) -> Anomalies {
    let thr = threshold.unwrap_or_else(|| default_threshold(algorithm));
    if points.is_empty() {
        return Anomalies {
            scores: Vec::new(),
            is_anomaly: Vec::new(),
            threshold: thr,
        };
    }
    let scores = match algorithm {
        Algorithm::ZScoreMad => zscore_mad(points),
        Algorithm::IsolationForest {
            n_trees,
            sample_size,
            seed,
        } => isolation_forest(points, n_trees, sample_size, seed),
        Algorithm::Lof { k } => lof(points, k),
        Algorithm::OneClassSvm { kernel, nu } => one_class_svm(points, kernel, nu),
    };
    // OCSVM's boundary is a strict sign test (f(x) < 0); everything else is score ≥ thr.
    let is_anomaly = match algorithm {
        Algorithm::OneClassSvm { .. } => scores.iter().map(|&s| s > thr).collect(),
        _ => scores.iter().map(|&s| s >= thr).collect(),
    };
    Anomalies {
        scores,
        is_anomaly,
        threshold: thr,
    }
}

// ─────────────────────────── z-score / MAD ───────────────────────────

/// Robust modified z-score (CONCEPT:EG-KG.mining.zscore-mad). Per feature: the
/// modified z is `0.6745·(x − median) / MAD` (Iglewicz–Hoaglin); when MAD is 0 the
/// feature falls back to the classic `(x − mean) / std`. The row score is the max
/// absolute modified z over features (an anomaly in ANY dimension flags the row).
pub fn zscore_mad(points: &[Point]) -> Vec<f64> {
    let n = points.len();
    let dim = points[0].len();
    let mut med = vec![0.0f64; dim];
    let mut mad = vec![0.0f64; dim];
    let mut mean = vec![0.0f64; dim];
    let mut std = vec![0.0f64; dim];

    for d in 0..dim {
        let mut col: Vec<f64> = points.iter().map(|p| p[d]).collect();
        med[d] = median(&mut col);
        let mut dev: Vec<f64> = col.iter().map(|&x| (x - med[d]).abs()).collect();
        mad[d] = median(&mut dev);
        let m = col.iter().sum::<f64>() / n as f64;
        mean[d] = m;
        std[d] = (col.iter().map(|&x| (x - m) * (x - m)).sum::<f64>() / n as f64).sqrt();
    }

    points
        .iter()
        .map(|x| {
            let mut worst = 0.0f64;
            for d in 0..dim {
                let z = if mad[d] > 1e-12 {
                    0.6745 * (x[d] - med[d]) / mad[d]
                } else if std[d] > 1e-12 {
                    (x[d] - mean[d]) / std[d]
                } else {
                    0.0
                };
                worst = worst.max(z.abs());
            }
            worst
        })
        .collect()
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = v.len();
    if n == 0 {
        0.0
    } else if n % 2 == 1 {
        v[n / 2]
    } else {
        0.5 * (v[n / 2 - 1] + v[n / 2])
    }
}

// ─────────────────────────── Isolation Forest ───────────────────────────

enum ITree {
    Leaf {
        size: usize,
    },
    Split {
        feature: usize,
        value: f64,
        left: Box<ITree>,
        right: Box<ITree>,
    },
}

/// Isolation Forest (CONCEPT:EG-KG.mining.isolation-forest). Builds `n_trees` random
/// isolation trees, each on a `sample_size` subsample, splitting on a random
/// feature at a random value between the sample's min/max until points isolate or
/// the height limit is hit. The anomaly score is `2^(−E[h]/c(ψ))` where `E[h]` is
/// the mean path length and `c(ψ)` normalizes for the subsample size — anomalies
/// isolate faster (short paths) → score near 1. Deterministic per `seed`.
pub fn isolation_forest(
    points: &[Point],
    n_trees: usize,
    sample_size: usize,
    seed: u64,
) -> Vec<f64> {
    let n = points.len();
    let dim = points[0].len();
    let psi = sample_size.clamp(1, n);
    let n_trees = n_trees.max(1);
    let height_limit = ((psi as f64).log2().ceil() as usize).max(1);
    let mut rng = SplitMix64::new(seed);

    let mut trees = Vec::with_capacity(n_trees);
    for _ in 0..n_trees {
        // Subsample without replacement (index shuffle prefix).
        let mut idx: Vec<usize> = (0..n).collect();
        for i in 0..psi {
            let j = i + (rng.next_u64() as usize) % (n - i);
            idx.swap(i, j);
        }
        let sample: Vec<&Point> = idx[..psi].iter().map(|&i| &points[i]).collect();
        trees.push(build_itree(&sample, dim, 0, height_limit, &mut rng));
    }

    let cn = c_factor(psi);
    points
        .iter()
        .map(|x| {
            let avg_h: f64 =
                trees.iter().map(|t| path_length(t, x, 0)).sum::<f64>() / n_trees as f64;
            2f64.powf(-avg_h / cn.max(1e-12))
        })
        .collect()
}

fn build_itree(
    sample: &[&Point],
    dim: usize,
    depth: usize,
    limit: usize,
    rng: &mut SplitMix64,
) -> ITree {
    if depth >= limit || sample.len() <= 1 {
        return ITree::Leaf { size: sample.len() };
    }
    // Pick a feature with a non-degenerate range; if none, this is a leaf.
    let feature = (rng.next_u64() as usize) % dim;
    let mut lo = f64::INFINITY;
    let mut hi = f64::NEG_INFINITY;
    for p in sample {
        lo = lo.min(p[feature]);
        hi = hi.max(p[feature]);
    }
    if hi <= lo {
        return ITree::Leaf { size: sample.len() };
    }
    let value = lo + rng.next_f64() * (hi - lo);
    let (mut left, mut right): (Vec<&Point>, Vec<&Point>) = (Vec::new(), Vec::new());
    for &p in sample {
        if p[feature] < value {
            left.push(p);
        } else {
            right.push(p);
        }
    }
    ITree::Split {
        feature,
        value,
        left: Box::new(build_itree(&left, dim, depth + 1, limit, rng)),
        right: Box::new(build_itree(&right, dim, depth + 1, limit, rng)),
    }
}

fn path_length(tree: &ITree, x: &Point, depth: usize) -> f64 {
    match tree {
        ITree::Leaf { size } => depth as f64 + c_factor(*size),
        ITree::Split {
            feature,
            value,
            left,
            right,
        } => {
            if x[*feature] < *value {
                path_length(left, x, depth + 1)
            } else {
                path_length(right, x, depth + 1)
            }
        }
    }
}

/// c(n) = 2·H(n−1) − 2(n−1)/n, the average unsuccessful-BST-search path length —
/// the isolation-tree path-length normalizer.
fn c_factor(n: usize) -> f64 {
    if n <= 1 {
        0.0
    } else {
        let nf = n as f64;
        2.0 * ((nf - 1.0).ln() + 0.577_215_664_901_532_9) - 2.0 * (nf - 1.0) / nf
    }
}

// ─────────────────────────── LOF ───────────────────────────

/// Local Outlier Factor (CONCEPT:EG-KG.mining.lof-local-density). For each point:
/// the k-distance and k-neighbors, the reachability distance to each neighbor, the
/// local reachability density (inverse mean reachability distance), and finally
/// LOF = mean(lrd(neighbor) / lrd(point)) over the k neighbors. LOF ≈ 1 in a
/// uniform-density region; > 1 means the point sits in a sparser neighborhood than
/// its neighbors (a local outlier). Deterministic (distance sort, index tie-break).
pub fn lof(points: &[Point], k: usize) -> Vec<f64> {
    let n = points.len();
    let k = k.clamp(1, n.saturating_sub(1).max(1));

    // Pairwise distances.
    let mut dm = vec![vec![0.0f64; n]; n];
    for i in 0..n {
        for j in (i + 1)..n {
            let d = euclidean(&points[i], &points[j]);
            dm[i][j] = d;
            dm[j][i] = d;
        }
    }

    // k-neighbors (nearest k excluding self) and the k-distance per point.
    let mut neighbors: Vec<Vec<usize>> = Vec::with_capacity(n);
    let mut k_dist = vec![0.0f64; n];
    for i in 0..n {
        let mut order: Vec<usize> = (0..n).filter(|&j| j != i).collect();
        order.sort_by(|&a, &b| {
            dm[i][a]
                .partial_cmp(&dm[i][b])
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.cmp(&b))
        });
        let kk = k.min(order.len());
        let nbrs: Vec<usize> = order[..kk].to_vec();
        k_dist[i] = if kk == 0 { 0.0 } else { dm[i][nbrs[kk - 1]] };
        neighbors.push(nbrs);
    }

    // Local reachability density: lrd(i) = 1 / mean_o reach-dist_k(i, o),
    // reach-dist_k(i, o) = max(k-distance(o), d(i, o)).
    let mut lrd = vec![0.0f64; n];
    for i in 0..n {
        let nbrs = &neighbors[i];
        if nbrs.is_empty() {
            lrd[i] = 0.0;
            continue;
        }
        let sum: f64 = nbrs.iter().map(|&o| dm[i][o].max(k_dist[o])).sum();
        let mean = sum / nbrs.len() as f64;
        lrd[i] = if mean > 1e-12 {
            1.0 / mean
        } else {
            f64::INFINITY
        };
    }

    // LOF(i) = mean_o lrd(o) / lrd(i).
    (0..n)
        .map(|i| {
            let nbrs = &neighbors[i];
            if nbrs.is_empty() || !lrd[i].is_finite() || lrd[i] <= 0.0 {
                return 1.0;
            }
            let sum: f64 = nbrs.iter().map(|&o| lrd[o]).sum();
            (sum / nbrs.len() as f64) / lrd[i]
        })
        .collect()
}

// ─────────────────────────── One-Class SVM (SMO-lite) ───────────────────────────

/// One-Class ν-SVM (CONCEPT:EG-KG.mining.oneclass-svm), Schölkopf et al. Solves the
/// dual `min ½ αᵀKα  s.t. 0 ≤ αᵢ ≤ 1/(νl), Σαᵢ = 1` with a lightweight SMO that
/// optimizes coordinate PAIRS (holding `αᵢ+αⱼ` fixed to preserve the sum
/// constraint) for a bounded number of passes, then recovers the offset `ρ` from
/// the un-bounded support vectors. The decision `f(x) = Σ αᵢ K(xᵢ,x) − ρ`; a point
/// with `f(x) < 0` is outside the learned boundary → anomalous. We return
/// `score = −f(x)` so higher = more anomalous (boundary at 0). Deterministic.
pub fn one_class_svm(points: &[Point], kernel: Kernel, nu: f64) -> Vec<f64> {
    let n = points.len();
    let dim = points[0].len();
    let nu = nu.clamp(1e-6, 1.0);
    let upper = 1.0 / (nu * n as f64); // box bound C
    let kern = resolve_kernel(kernel, dim);

    // Precompute the kernel (Gram) matrix.
    let mut km = vec![vec![0.0f64; n]; n];
    for i in 0..n {
        for j in i..n {
            let v = kern(&points[i], &points[j]);
            km[i][j] = v;
            km[j][i] = v;
        }
    }

    // Feasible init: spread the unit mass, respecting the box bound.
    let mut alpha = vec![(1.0 / n as f64).min(upper); n];
    normalize_to_unit_sum(&mut alpha, upper);

    // Gradient Gᵢ = Σⱼ αⱼ K(i,j).
    let grad =
        |alpha: &[f64], i: usize| -> f64 { (0..n).map(|j| alpha[j] * km[i][j]).sum::<f64>() };

    // SMO-lite: pair sweeps. For a pair (i,j) with s = αᵢ+αⱼ held fixed, the
    // unconstrained optimum is αᵢ* = αᵢ + (Gⱼ − Gᵢ)/(Kᵢᵢ + Kⱼⱼ − 2Kᵢⱼ); clip to
    // the box AND to [max(0, s−C), min(C, s)] so αⱼ = s − αᵢ also stays feasible.
    let passes = 50;
    for _ in 0..passes {
        let mut changed = false;
        for i in 0..n {
            for j in (i + 1)..n {
                let eta = km[i][i] + km[j][j] - 2.0 * km[i][j];
                if eta <= 1e-12 {
                    continue;
                }
                let gi = grad(&alpha, i);
                let gj = grad(&alpha, j);
                let s = alpha[i] + alpha[j];
                let lo = (s - upper).max(0.0);
                let hi = s.min(upper);
                if hi - lo < 1e-15 {
                    continue;
                }
                let mut ai = alpha[i] + (gj - gi) / eta;
                ai = ai.clamp(lo, hi);
                if (ai - alpha[i]).abs() > 1e-12 {
                    alpha[i] = ai;
                    alpha[j] = s - ai;
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }

    // ρ = average decision sum over the un-bounded SVs (0 < αᵢ < C).
    let mut rho_acc = 0.0;
    let mut rho_cnt = 0usize;
    for i in 0..n {
        if alpha[i] > 1e-8 && alpha[i] < upper - 1e-8 {
            rho_acc += grad(&alpha, i);
            rho_cnt += 1;
        }
    }
    let rho = if rho_cnt > 0 {
        rho_acc / rho_cnt as f64
    } else {
        // Fall back to the mean decision sum when no free SV exists.
        (0..n).map(|i| grad(&alpha, i)).sum::<f64>() / n as f64
    };

    // score = −f(x) = ρ − Σ αᵢ K(xᵢ, x); higher ⇒ more anomalous.
    points
        .iter()
        .map(|x| {
            let f: f64 = (0..n).map(|i| alpha[i] * kern(&points[i], x)).sum::<f64>() - rho;
            -f
        })
        .collect()
}

/// A boxed kernel function `K(x, y)` (linear dot or RBF).
type KernelFn = Box<dyn Fn(&[f64], &[f64]) -> f64>;

fn resolve_kernel(kernel: Kernel, dim: usize) -> KernelFn {
    match kernel {
        Kernel::Linear => Box::new(|a: &[f64], b: &[f64]| dot(a, b)),
        Kernel::Rbf { gamma } => {
            let g = if gamma > 0.0 {
                gamma
            } else {
                1.0 / dim.max(1) as f64
            };
            Box::new(move |a: &[f64], b: &[f64]| (-g * sq_dist(a, b)).exp())
        }
    }
}

fn normalize_to_unit_sum(alpha: &mut [f64], upper: f64) {
    let sum: f64 = alpha.iter().sum();
    if sum <= 0.0 {
        return;
    }
    for a in alpha.iter_mut() {
        *a = (*a / sum).min(upper);
    }
    // Redistribute any deficit created by the box clamp back onto unsaturated coords.
    let mut deficit = 1.0 - alpha.iter().sum::<f64>();
    while deficit > 1e-9 {
        let open: Vec<usize> = (0..alpha.len())
            .filter(|&i| alpha[i] < upper - 1e-12)
            .collect();
        if open.is_empty() {
            break;
        }
        let share = deficit / open.len() as f64;
        for &i in &open {
            let room = upper - alpha[i];
            let add = share.min(room);
            alpha[i] += add;
        }
        let new_def = 1.0 - alpha.iter().sum::<f64>();
        if (deficit - new_def).abs() < 1e-12 {
            break;
        }
        deficit = new_def;
    }
}

// ─────────────────────────── shared helpers ───────────────────────────

fn euclidean(a: &[f64], b: &[f64]) -> f64 {
    sq_dist(a, b).sqrt()
}

fn sq_dist(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

fn dot(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// Deterministic splitmix64 — keeps the Isolation Forest dependency-free.
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64 {
            state: seed.wrapping_add(0x9E37_79B9_7F4A_7C15),
        }
    }
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A dense cluster around the origin plus two injected far outliers (rows 10, 11).
    fn dense_with_outliers() -> Vec<Point> {
        let mut pts = Vec::new();
        for i in 0..10 {
            let t = i as f64 * 0.1;
            pts.push(vec![t, -t + 0.05 * (i as f64 % 3.0)]);
        }
        pts.push(vec![100.0, 100.0]); // 10 — outlier
        pts.push(vec![-80.0, 90.0]); // 11 — outlier
        pts
    }

    fn top_two(scores: &[f64]) -> (usize, usize) {
        let mut idx: Vec<usize> = (0..scores.len()).collect();
        idx.sort_by(|&a, &b| scores[b].partial_cmp(&scores[a]).unwrap());
        (idx[0], idx[1])
    }

    #[test]
    fn zscore_mad_flags_injected_outliers() {
        let pts = dense_with_outliers();
        let out = detect(&pts, Algorithm::ZScoreMad, None);
        assert!(out.is_anomaly[10]);
        assert!(out.is_anomaly[11]);
        // The dense core is not flagged.
        assert!(out.is_anomaly[..10].iter().all(|&f| !f));
    }

    #[test]
    fn isolation_forest_ranks_outliers_highest() {
        let pts = dense_with_outliers();
        let out = detect(
            &pts,
            Algorithm::IsolationForest {
                n_trees: 128,
                sample_size: 8,
                seed: 7,
            },
            None,
        );
        // The two injected outliers get the two highest anomaly scores.
        let (a, b) = top_two(&out.scores);
        let top = [a, b];
        assert!(
            top.contains(&10),
            "outlier 10 not top-ranked: {:?}",
            out.scores
        );
        assert!(
            top.contains(&11),
            "outlier 11 not top-ranked: {:?}",
            out.scores
        );
    }

    #[test]
    fn isolation_forest_is_deterministic() {
        let pts = dense_with_outliers();
        let a = isolation_forest(&pts, 64, 8, 3);
        let b = isolation_forest(&pts, 64, 8, 3);
        assert_eq!(a, b);
    }

    #[test]
    fn lof_flags_the_sparse_point() {
        // A dense cluster plus one point sitting far from it — high LOF.
        let mut pts = vec![
            vec![0.0, 0.0],
            vec![0.1, 0.0],
            vec![0.0, 0.1],
            vec![0.1, 0.1],
            vec![0.05, 0.05],
            vec![0.2, 0.1],
            vec![0.1, 0.2],
        ];
        pts.push(vec![5.0, 5.0]); // 7 — local outlier
        let out = detect(&pts, Algorithm::Lof { k: 3 }, None);
        assert!(
            out.is_anomaly[7],
            "sparse point not flagged, scores={:?}",
            out.scores
        );
        // The dense members are inliers (LOF ≈ 1).
        assert!(out.scores[7] > out.scores[0]);
    }

    #[test]
    fn one_class_svm_separates_outliers() {
        let pts = dense_with_outliers();
        let out = detect(
            &pts,
            Algorithm::OneClassSvm {
                kernel: Kernel::Rbf { gamma: 0.001 },
                nu: 0.2,
            },
            None,
        );
        // The injected outliers score above the dense core (higher = more anomalous).
        let core_max = out.scores[..10]
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);
        assert!(out.scores[10] > core_max, "scores={:?}", out.scores);
        assert!(out.scores[11] > core_max, "scores={:?}", out.scores);
    }

    #[test]
    fn threshold_override_changes_flags() {
        let pts = dense_with_outliers();
        // An impossibly high z-score threshold flags nothing.
        let out = detect(&pts, Algorithm::ZScoreMad, Some(1e9));
        assert!(out.is_anomaly.iter().all(|&f| !f));
        assert_eq!(out.threshold, 1e9);
    }

    #[test]
    fn empty_input_is_empty_result() {
        let out = detect(&[], Algorithm::ZScoreMad, None);
        assert!(out.scores.is_empty());
        assert!(out.is_anomaly.is_empty());
    }
}
