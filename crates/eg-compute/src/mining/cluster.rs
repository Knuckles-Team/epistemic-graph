// CONCEPT:EG-KG.mining.dbscan-density — completing the clustering family.
//
// Pure-Rust, dependency-light, batch (one round-trip): given a feature matrix
// (each row a point in R^d), partition the rows into clusters. Four
// interchangeable engines beyond the existing k-Means/spectral:
//
//   * DBSCAN                (CONCEPT:EG-KG.mining.dbscan-density) — density-based,
//     `eps`+`min_pts`, labels un-dense points as noise (cluster id -1).
//   * Hierarchical          (CONCEPT:EG-KG.mining.hierarchical-linkage) — agglomerative
//     single/complete/average linkage, cut to a flat `k`-cluster partition.
//   * GMM (EM)              (CONCEPT:EG-KG.mining.gmm-em) — diagonal-covariance
//     Gaussian mixture fit by Expectation-Maximization; soft responsibilities +
//     an argmax hard label.
//   * k-Medoids (PAM)       (CONCEPT:EG-KG.mining.kmedoids-pam) — Partitioning Around
//     Medoids (greedy BUILD + SWAP); cluster centers are actual data points.
//
// All are deterministic: DBSCAN/hierarchical/PAM are seed-free (index-ordered,
// index tie-broken); GMM uses a seeded splitmix64 for k-means++ init. This module
// is graph-agnostic — it works over `&[Vec<f64>]`. The handler
// (`src/server/handlers/mining.rs`) supplies the rows (explicit or node embeddings)
// and does the KG write-back.

/// A point in feature space (one matrix row).
pub type Point = Vec<f64>;

/// Which clustering engine to run, with its parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Algorithm {
    /// Density clustering: a core point has ≥ `min_pts` points (incl. itself)
    /// within radius `eps`; non-core, non-reachable points are noise (id -1).
    Dbscan { eps: f64, min_pts: usize },
    /// Agglomerative clustering cut to `k` flat clusters under `linkage`.
    Hierarchical { k: usize, linkage: Linkage },
    /// `k`-component diagonal-covariance Gaussian mixture, `max_iter` EM steps.
    Gmm {
        k: usize,
        max_iter: usize,
        seed: u64,
    },
    /// Partitioning Around Medoids into `k` clusters, `max_iter` SWAP rounds.
    KMedoids { k: usize, max_iter: usize },
}

/// Linkage criterion for [`Algorithm::Hierarchical`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Linkage {
    Single,
    Complete,
    Average,
}

/// One cluster in the result: its id, the row indices that belong to it, the mean
/// of its members (centroid), and a compactness `score` (mean member→centroid
/// Euclidean distance — lower is tighter). The DBSCAN noise bucket carries id -1.
#[derive(Debug, Clone, PartialEq)]
pub struct Cluster {
    pub cluster_id: i64,
    pub members: Vec<usize>,
    pub centroid: Vec<f64>,
    pub score: f64,
}

/// The full clustering outcome: per-row `labels` (parallel to the input rows; -1 =
/// DBSCAN noise) and the grouped `clusters`. `responsibilities` is populated ONLY
/// by GMM — the soft `n × k` assignment matrix (row-stochastic).
#[derive(Debug, Clone, PartialEq)]
pub struct Clustering {
    pub labels: Vec<i64>,
    pub clusters: Vec<Cluster>,
    pub responsibilities: Option<Vec<Vec<f64>>>,
}

/// Run the chosen clustering engine over `points`. An empty input yields an empty
/// clustering (a valid empty result, never a panic).
pub fn cluster(points: &[Point], algorithm: Algorithm) -> Clustering {
    if points.is_empty() {
        return Clustering {
            labels: Vec::new(),
            clusters: Vec::new(),
            responsibilities: None,
        };
    }
    match algorithm {
        Algorithm::Dbscan { eps, min_pts } => {
            let labels = dbscan(points, eps, min_pts);
            group(points, labels, None)
        }
        Algorithm::Hierarchical { k, linkage } => {
            let labels = hierarchical(points, k, linkage);
            group(points, labels, None)
        }
        Algorithm::Gmm { k, max_iter, seed } => {
            let (labels, resp) = gmm(points, k, max_iter, seed);
            group(points, labels, Some(resp))
        }
        Algorithm::KMedoids { k, max_iter } => {
            let labels = kmedoids(points, k, max_iter);
            group(points, labels, None)
        }
    }
}

// ─────────────────────────── DBSCAN ───────────────────────────

const UNVISITED: i64 = -2;
const NOISE: i64 = -1;

/// Density-based clustering (CONCEPT:EG-KG.mining.dbscan-density). A core point has
/// ≥ `min_pts` points (INCLUDING itself) within `eps`; clusters grow from core
/// points through density-reachability. Points reachable from no core point stay
/// noise (label -1). Deterministic: points and neighbors are processed in index
/// order via a FIFO seed queue.
pub fn dbscan(points: &[Point], eps: f64, min_pts: usize) -> Vec<i64> {
    let n = points.len();
    let eps2 = eps * eps;
    let mut labels = vec![UNVISITED; n];
    let mut cid: i64 = 0;

    for p in 0..n {
        if labels[p] != UNVISITED {
            continue;
        }
        let neighbors = region_query(points, p, eps2);
        if neighbors.len() < min_pts {
            labels[p] = NOISE; // provisionally noise; may become a border point below
            continue;
        }
        // Start a new cluster and flood-fill density-reachable points.
        labels[p] = cid;
        let mut queue: std::collections::VecDeque<usize> = neighbors.into_iter().collect();
        while let Some(q) = queue.pop_front() {
            if labels[q] == NOISE {
                labels[q] = cid; // border point: reachable but not itself core
            }
            if labels[q] != UNVISITED {
                continue;
            }
            labels[q] = cid;
            let q_neighbors = region_query(points, q, eps2);
            if q_neighbors.len() >= min_pts {
                // q is core → its neighbors join the seed set.
                for r in q_neighbors {
                    if labels[r] == UNVISITED || labels[r] == NOISE {
                        queue.push_back(r);
                    }
                }
            }
        }
        cid += 1;
    }
    labels
}

/// All indices (including `p`) within squared radius `eps2` of point `p`.
fn region_query(points: &[Point], p: usize, eps2: f64) -> Vec<usize> {
    (0..points.len())
        .filter(|&q| sq_dist(&points[p], &points[q]) <= eps2)
        .collect()
}

// ─────────────────────────── Hierarchical agglomerative ───────────────────────────

/// Agglomerative clustering (CONCEPT:EG-KG.mining.hierarchical-linkage) cut to `k`
/// flat clusters. Starts with `n` singletons and repeatedly merges the two closest
/// clusters (by `linkage`) until `k` remain, maintaining the inter-cluster distance
/// matrix by the Lance-Williams update. Deterministic: the closest pair is chosen
/// with an (i, j) index tie-break. Labels are assigned 0..k-1 by ascending smallest
/// member index for a stable ordering.
pub fn hierarchical(points: &[Point], k: usize, linkage: Linkage) -> Vec<i64> {
    let n = points.len();
    let k = k.clamp(1, n);

    // Active clusters as member-index lists; `dist[i][j]` = current linkage distance.
    let mut members: Vec<Option<Vec<usize>>> = (0..n).map(|i| Some(vec![i])).collect();
    let mut dist = vec![vec![0.0f64; n]; n];
    for i in 0..n {
        for j in (i + 1)..n {
            let d = euclidean(&points[i], &points[j]);
            dist[i][j] = d;
            dist[j][i] = d;
        }
    }

    let mut active: usize = n;
    while active > k {
        // Find the closest active pair (a < b), index tie-broken.
        let mut best = f64::INFINITY;
        let (mut ba, mut bb) = (usize::MAX, usize::MAX);
        for i in 0..n {
            if members[i].is_none() {
                continue;
            }
            for j in (i + 1)..n {
                if members[j].is_none() {
                    continue;
                }
                if dist[i][j] < best {
                    best = dist[i][j];
                    ba = i;
                    bb = j;
                }
            }
        }
        if ba == usize::MAX {
            break;
        }
        // Merge bb into ba; Lance-Williams update of ba's distance to every other.
        let size_a = members[ba].as_ref().unwrap().len() as f64;
        let size_b = members[bb].as_ref().unwrap().len() as f64;
        for m in 0..n {
            if m == ba || m == bb || members[m].is_none() {
                continue;
            }
            let dam = dist[ba][m];
            let dbm = dist[bb][m];
            let new = match linkage {
                Linkage::Single => dam.min(dbm),
                Linkage::Complete => dam.max(dbm),
                Linkage::Average => (size_a * dam + size_b * dbm) / (size_a + size_b),
            };
            dist[ba][m] = new;
            dist[m][ba] = new;
        }
        let moved = members[bb].take().unwrap();
        members[ba].as_mut().unwrap().extend(moved);
        active -= 1;
    }

    // Assign labels by ascending smallest-member index.
    let mut surviving: Vec<(usize, Vec<usize>)> = members
        .into_iter()
        .filter_map(|m| m.map(|v| (v[0], v)))
        .collect();
    surviving.sort_by_key(|(min_idx, _)| *min_idx);
    let mut labels = vec![0i64; n];
    for (cid, (_, mem)) in surviving.into_iter().enumerate() {
        for idx in mem {
            labels[idx] = cid as i64;
        }
    }
    labels
}

// ─────────────────────────── GMM (EM) ───────────────────────────

/// Diagonal-covariance Gaussian mixture via EM (CONCEPT:EG-KG.mining.gmm-em).
/// Initializes `k` component means with seeded k-means++ (deterministic per
/// `seed`), diagonal covariances to the global per-feature variance, and uniform
/// weights, then alternates E (responsibilities) and M (weights/means/variances)
/// steps for `max_iter` iterations or until the log-likelihood barely moves.
/// Returns the argmax hard `labels` and the soft `n × k` responsibility matrix.
pub fn gmm(points: &[Point], k: usize, max_iter: usize, seed: u64) -> (Vec<i64>, Vec<Vec<f64>>) {
    let n = points.len();
    let dim = points[0].len();
    let k = k.clamp(1, n);
    const VAR_FLOOR: f64 = 1e-6;

    // Init means via k-means++; covariances to global variance; weights uniform.
    let mut means = kmeanspp_init(points, k, seed);
    let global_var = feature_variance(points, dim);
    let mut vars = vec![global_var.clone(); k];
    let mut weights = vec![1.0 / k as f64; k];

    let mut resp = vec![vec![0.0f64; k]; n];
    let mut prev_ll = f64::NEG_INFINITY;

    for _ in 0..max_iter.max(1) {
        // E-step: responsibilities via log-density for numerical stability.
        let mut ll = 0.0;
        for (i, x) in points.iter().enumerate() {
            let mut log_comp = vec![0.0f64; k];
            for c in 0..k {
                log_comp[c] =
                    weights[c].max(1e-300).ln() + log_gaussian_diag(x, &means[c], &vars[c]);
            }
            let max_lc = log_comp.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let mut sum = 0.0;
            for c in 0..k {
                let e = (log_comp[c] - max_lc).exp();
                resp[i][c] = e;
                sum += e;
            }
            for r in resp[i].iter_mut() {
                *r /= sum;
            }
            ll += max_lc + sum.ln();
        }

        // M-step: refit weights, means, diagonal variances.
        for c in 0..k {
            let nk: f64 = resp.iter().map(|r| r[c]).sum();
            let nk_safe = nk.max(1e-300);
            weights[c] = nk / n as f64;
            for d in 0..dim {
                let mut mean = 0.0;
                for (i, x) in points.iter().enumerate() {
                    mean += resp[i][c] * x[d];
                }
                means[c][d] = mean / nk_safe;
            }
            for d in 0..dim {
                let mut var = 0.0;
                for (i, x) in points.iter().enumerate() {
                    let diff = x[d] - means[c][d];
                    var += resp[i][c] * diff * diff;
                }
                vars[c][d] = (var / nk_safe).max(VAR_FLOOR);
            }
        }

        if (ll - prev_ll).abs() < 1e-9 * (1.0 + ll.abs()) {
            break;
        }
        prev_ll = ll;
    }

    let labels = resp.iter().map(|r| argmax(r) as i64).collect();
    (labels, resp)
}

/// log N(x | mean, diag(var)) for a diagonal-covariance Gaussian.
fn log_gaussian_diag(x: &[f64], mean: &[f64], var: &[f64]) -> f64 {
    const LOG_2PI: f64 = 1.837_877_066_409_345_6; // ln(2π)
    let mut acc = 0.0;
    for d in 0..x.len() {
        let v = var[d].max(1e-12);
        let diff = x[d] - mean[d];
        acc += -0.5 * (LOG_2PI + v.ln() + diff * diff / v);
    }
    acc
}

fn feature_variance(points: &[Point], dim: usize) -> Vec<f64> {
    let n = points.len() as f64;
    let mut mean = vec![0.0; dim];
    for x in points {
        for d in 0..dim {
            mean[d] += x[d];
        }
    }
    for m in mean.iter_mut() {
        *m /= n;
    }
    let mut var = vec![0.0; dim];
    for x in points {
        for d in 0..dim {
            let diff = x[d] - mean[d];
            var[d] += diff * diff;
        }
    }
    for v in var.iter_mut() {
        *v = (*v / n).max(1e-6);
    }
    var
}

// ─────────────────────────── k-Medoids (PAM) ───────────────────────────

/// Partitioning Around Medoids (CONCEPT:EG-KG.mining.kmedoids-pam). Classic PAM:
/// the greedy BUILD phase seeds `k` medoids that minimize total assignment cost,
/// then the SWAP phase repeatedly applies the single medoid↔non-medoid swap that
/// most reduces total cost, until no swap improves it or `max_iter` rounds pass.
/// Cluster centers are ACTUAL data points (robust to outliers, unlike k-Means).
/// Deterministic and seed-free (greedy, index tie-broken).
pub fn kmedoids(points: &[Point], k: usize, max_iter: usize) -> Vec<i64> {
    let n = points.len();
    let k = k.clamp(1, n);
    // Precompute the pairwise distance matrix (n is test/UI-scale here).
    let mut dm = vec![vec![0.0f64; n]; n];
    for i in 0..n {
        for j in (i + 1)..n {
            let d = euclidean(&points[i], &points[j]);
            dm[i][j] = d;
            dm[j][i] = d;
        }
    }

    // BUILD: first medoid minimizes total distance to all points; each subsequent
    // medoid greedily maximizes the reduction in total assignment cost.
    let mut medoids: Vec<usize> = Vec::with_capacity(k);
    {
        let first = (0..n)
            .min_by(|&a, &b| {
                total_to(a, &dm)
                    .partial_cmp(&total_to(b, &dm))
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.cmp(&b))
            })
            .unwrap();
        medoids.push(first);
        while medoids.len() < k {
            let mut best_gain = f64::NEG_INFINITY;
            let mut best_c = usize::MAX;
            for cand in 0..n {
                if medoids.contains(&cand) {
                    continue;
                }
                // Gain = Σ_j max(0, d(j, nearest_medoid) - d(j, cand)).
                let mut gain = 0.0;
                for j in 0..n {
                    let cur = nearest_medoid_dist(j, &medoids, &dm);
                    if dm[j][cand] < cur {
                        gain += cur - dm[j][cand];
                    }
                }
                if gain > best_gain || (gain == best_gain && cand < best_c) {
                    best_gain = gain;
                    best_c = cand;
                }
            }
            if best_c == usize::MAX {
                break;
            }
            medoids.push(best_c);
        }
    }

    // SWAP: best-improving medoid↔non-medoid swap until no improvement.
    for _ in 0..max_iter.max(1) {
        let base = total_cost(&medoids, &dm);
        let mut best_delta = -1e-12; // require a strict improvement
        let mut best_swap: Option<(usize, usize)> = None;
        for mi in 0..medoids.len() {
            for cand in 0..n {
                if medoids.contains(&cand) {
                    continue;
                }
                let mut trial = medoids.clone();
                trial[mi] = cand;
                let delta = total_cost(&trial, &dm) - base;
                if delta < best_delta {
                    best_delta = delta;
                    best_swap = Some((mi, cand));
                }
            }
        }
        match best_swap {
            Some((mi, cand)) => medoids[mi] = cand,
            None => break,
        }
    }

    // Assign each point to its nearest medoid; relabel medoids 0..k-1 by index.
    let mut medoid_order: Vec<usize> = (0..medoids.len()).collect();
    medoid_order.sort_by_key(|&i| medoids[i]);
    let mut medoid_label = vec![0i64; medoids.len()];
    for (new_id, &orig) in medoid_order.iter().enumerate() {
        medoid_label[orig] = new_id as i64;
    }
    (0..n)
        .map(|j| {
            let mut best = f64::INFINITY;
            let mut best_m = 0usize;
            for (mi, &m) in medoids.iter().enumerate() {
                if dm[j][m] < best {
                    best = dm[j][m];
                    best_m = mi;
                }
            }
            medoid_label[best_m]
        })
        .collect()
}

fn total_to(a: usize, dm: &[Vec<f64>]) -> f64 {
    dm[a].iter().sum()
}

fn nearest_medoid_dist(j: usize, medoids: &[usize], dm: &[Vec<f64>]) -> f64 {
    medoids
        .iter()
        .map(|&m| dm[j][m])
        .fold(f64::INFINITY, f64::min)
}

fn total_cost(medoids: &[usize], dm: &[Vec<f64>]) -> f64 {
    (0..dm.len())
        .map(|j| nearest_medoid_dist(j, medoids, dm))
        .sum()
}

// ─────────────────────────── shared helpers ───────────────────────────

/// Group per-row `labels` into [`Cluster`]s (centroid = member mean, score = mean
/// member→centroid distance). Clusters are ordered by ascending id; the DBSCAN
/// noise bucket (id -1) comes first when present.
fn group(points: &[Point], labels: Vec<i64>, resp: Option<Vec<Vec<f64>>>) -> Clustering {
    let dim = points[0].len();
    let mut ids: Vec<i64> = labels
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    ids.sort_unstable();

    let mut clusters = Vec::with_capacity(ids.len());
    for id in ids {
        let members: Vec<usize> = labels
            .iter()
            .enumerate()
            .filter_map(|(i, &l)| (l == id).then_some(i))
            .collect();
        let mut centroid = vec![0.0f64; dim];
        for &m in &members {
            for d in 0..dim {
                centroid[d] += points[m][d];
            }
        }
        let cnt = members.len().max(1) as f64;
        for c in centroid.iter_mut() {
            *c /= cnt;
        }
        let score = if members.is_empty() {
            0.0
        } else {
            members
                .iter()
                .map(|&m| euclidean(&points[m], &centroid))
                .sum::<f64>()
                / members.len() as f64
        };
        clusters.push(Cluster {
            cluster_id: id,
            members,
            centroid,
            score,
        });
    }
    Clustering {
        labels,
        clusters,
        responsibilities: resp,
    }
}

/// Seeded k-means++ center initialization (used by GMM). Deterministic per `seed`.
fn kmeanspp_init(points: &[Point], k: usize, seed: u64) -> Vec<Point> {
    let n = points.len();
    let mut rng = SplitMix64::new(seed);
    let mut centers: Vec<Point> = Vec::with_capacity(k);
    centers.push(points[(rng.next_u64() as usize) % n].clone());
    while centers.len() < k {
        // Weighted by squared distance to the nearest chosen center.
        let d2: Vec<f64> = points
            .iter()
            .map(|x| {
                centers
                    .iter()
                    .map(|c| sq_dist(x, c))
                    .fold(f64::INFINITY, f64::min)
            })
            .collect();
        let total: f64 = d2.iter().sum();
        if total <= 0.0 {
            // All remaining points coincide with a center — pad by index.
            for p in points.iter() {
                if centers.len() >= k {
                    break;
                }
                centers.push(p.clone());
            }
            break;
        }
        let mut target = rng.next_f64() * total;
        let mut chosen = n - 1;
        for (i, &w) in d2.iter().enumerate() {
            target -= w;
            if target <= 0.0 {
                chosen = i;
                break;
            }
        }
        centers.push(points[chosen].clone());
    }
    centers
}

fn argmax(v: &[f64]) -> usize {
    let mut best = 0;
    for i in 1..v.len() {
        if v[i] > v[best] {
            best = i;
        }
    }
    best
}

fn euclidean(a: &[f64], b: &[f64]) -> f64 {
    sq_dist(a, b).sqrt()
}

fn sq_dist(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// A tiny deterministic splitmix64 PRNG — keeps clustering dependency-free (no
/// `rand` crate pulled into the `mining` feature) while giving reproducible init.
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
        // 53-bit mantissa in [0, 1).
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two well-separated blobs plus one far outlier — the DBSCAN "two-moons-ish"
    /// fixture. Cluster A around (0,0), cluster B around (10,10), point 8 is noise.
    fn two_blobs() -> Vec<Point> {
        vec![
            vec![0.0, 0.0],   // 0 A
            vec![0.3, 0.1],   // 1 A
            vec![0.1, 0.4],   // 2 A
            vec![0.2, 0.2],   // 3 A
            vec![10.0, 10.0], // 4 B
            vec![10.3, 9.9],  // 5 B
            vec![9.8, 10.2],  // 6 B
            vec![10.1, 10.1], // 7 B
            vec![50.0, 50.0], // 8 noise
        ]
    }

    #[test]
    fn dbscan_finds_two_clusters_and_noise() {
        let pts = two_blobs();
        let labels = dbscan(&pts, 1.0, 3);
        // The two blobs are distinct clusters; point 8 is noise.
        assert_eq!(labels[8], NOISE);
        assert!(labels[0] >= 0 && labels[4] >= 0);
        assert_eq!(labels[0], labels[1]);
        assert_eq!(labels[0], labels[3]);
        assert_eq!(labels[4], labels[7]);
        assert_ne!(labels[0], labels[4]);
        // Exactly two dense clusters.
        let mut ids: Vec<i64> = labels.iter().copied().filter(|&l| l >= 0).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn dbscan_all_noise_when_eps_tiny() {
        let pts = two_blobs();
        let labels = dbscan(&pts, 0.01, 3);
        assert!(labels.iter().all(|&l| l == NOISE));
    }

    /// The two blobs WITHOUT the far outlier — the natural 2-partition fixture (an
    /// extreme outlier would legitimately claim its own cluster at k=2).
    fn two_blobs_no_noise() -> Vec<Point> {
        let mut p = two_blobs();
        p.truncate(8);
        p
    }

    #[test]
    fn hierarchical_recovers_two_groups_all_linkages() {
        let pts = two_blobs_no_noise();
        for linkage in [Linkage::Single, Linkage::Complete, Linkage::Average] {
            let labels = hierarchical(&pts, 2, linkage);
            // The four A points share a label; the four B points share another.
            assert_eq!(labels[0], labels[1], "linkage {linkage:?}");
            assert_eq!(labels[0], labels[2]);
            assert_eq!(labels[0], labels[3]);
            assert_eq!(labels[4], labels[5]);
            assert_eq!(labels[4], labels[7]);
            assert_ne!(labels[0], labels[4]);
        }
    }

    #[test]
    fn hierarchical_k_equals_n_is_all_singletons() {
        let pts = two_blobs();
        let labels = hierarchical(&pts, pts.len(), Linkage::Average);
        let mut uniq = labels.clone();
        uniq.sort_unstable();
        uniq.dedup();
        assert_eq!(uniq.len(), pts.len());
    }

    #[test]
    fn gmm_recovers_planted_gaussians() {
        // Two tight, well-separated Gaussians → EM must split them cleanly.
        let mut pts = Vec::new();
        for i in 0..10 {
            let t = i as f64 * 0.05;
            pts.push(vec![t, -t]); // near origin
        }
        for i in 0..10 {
            let t = i as f64 * 0.05;
            pts.push(vec![20.0 + t, 20.0 - t]); // near (20,20)
        }
        let (labels, resp) = gmm(&pts, 2, 100, 42);
        // Everything in the first blob shares a label distinct from the second.
        let a = labels[0];
        let b = labels[10];
        assert_ne!(a, b);
        assert!(labels[..10].iter().all(|&l| l == a));
        assert!(labels[10..].iter().all(|&l| l == b));
        // Responsibilities are row-stochastic and confident on this easy fixture.
        for r in &resp {
            let s: f64 = r.iter().sum();
            assert!((s - 1.0).abs() < 1e-9);
            assert!(r.iter().cloned().fold(0.0, f64::max) > 0.9);
        }
    }

    #[test]
    fn gmm_is_deterministic_for_a_seed() {
        let pts = two_blobs();
        let (l1, _) = gmm(&pts, 2, 50, 7);
        let (l2, _) = gmm(&pts, 2, 50, 7);
        assert_eq!(l1, l2);
    }

    #[test]
    fn kmedoids_partitions_two_blobs() {
        let pts = two_blobs_no_noise();
        let labels = kmedoids(&pts, 2, 100);
        // The A points cluster together, the B points together.
        assert_eq!(labels[0], labels[1]);
        assert_eq!(labels[0], labels[3]);
        assert_eq!(labels[4], labels[5]);
        assert_eq!(labels[4], labels[7]);
        assert_ne!(labels[0], labels[4]);
    }

    #[test]
    fn cluster_wrapper_groups_and_scores() {
        let pts = two_blobs();
        let out = cluster(
            &pts,
            Algorithm::Dbscan {
                eps: 1.0,
                min_pts: 3,
            },
        );
        assert_eq!(out.labels.len(), pts.len());
        // Noise bucket (-1) present + two clusters.
        assert!(out.clusters.iter().any(|c| c.cluster_id == -1));
        assert_eq!(out.clusters.iter().filter(|c| c.cluster_id >= 0).count(), 2);
        // Every centroid has the input dimensionality; scores are finite.
        for c in &out.clusters {
            assert_eq!(c.centroid.len(), 2);
            assert!(c.score.is_finite());
        }
        assert!(out.responsibilities.is_none());
    }

    #[test]
    fn gmm_populates_responsibilities() {
        let pts = two_blobs();
        let out = cluster(
            &pts,
            Algorithm::Gmm {
                k: 2,
                max_iter: 50,
                seed: 1,
            },
        );
        let resp = out.responsibilities.expect("gmm returns responsibilities");
        assert_eq!(resp.len(), pts.len());
        assert_eq!(resp[0].len(), 2);
    }
}
