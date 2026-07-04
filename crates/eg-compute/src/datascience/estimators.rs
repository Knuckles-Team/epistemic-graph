// CONCEPT:EG-KG.compute.rust-native-ml-estimators — Rust-Native ML Estimators (sklearn hard-cut)
//
// Regression estimators implemented in pure Rust to replace scikit-learn on the
// data-science-mcp hot path: Ridge / Lasso / ElasticNet (linear family),
// DecisionTree / RandomForest / GradientBoosting / AdaBoost.R2 (tree family),
// and epsilon-SVR with linear/RBF kernels via SMO.
//
// Design is STATELESS to match the rest of the engine: `fit_estimator` returns a
// serializable `FittedModel`; `predict` takes that model back plus a feature
// matrix. The Python client stores the model blob and ships it back for
// prediction (one round-trip each — never per-row over the wire).

use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use rayon::prelude::*;

use crate::datascience::primitives::solve_linear_system;

/// The estimator hyperparameters + serializable fitted-model types are defined
/// in `eg-types::wire` (the `protocol` enum embeds `EstimatorParams`/`FittedModel`
/// over the wire); re-exported here so the fit/predict code below is unchanged.
pub use crate::wire::{DecisionTree, EstimatorParams, FittedModel, TreeNode};

// ── Public entry points ────────────────────────────────────────────────────

/// Fit an estimator by (normalized) name. Returns an error string for unknown
/// estimators so the caller can fall back.
pub fn fit_estimator(
    estimator: &str,
    x: &[Vec<f64>],
    y: &[f64],
    params: &EstimatorParams,
) -> Result<FittedModel, String> {
    let key: String = estimator
        .to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric())
        .collect();
    match key.as_str() {
        "ridge" => Ok(fit_ridge(x, y, params.alpha.unwrap_or(1.0))),
        "lasso" => Ok(fit_elastic_net(
            x,
            y,
            params.alpha.unwrap_or(1.0),
            1.0,
            params,
        )),
        "elasticnet" => Ok(fit_elastic_net(
            x,
            y,
            params.alpha.unwrap_or(1.0),
            params.l1_ratio.unwrap_or(0.5),
            params,
        )),
        "decisiontree" | "decisiontreeregressor" => Ok(FittedModel::Tree(fit_tree(
            x,
            y,
            &(0..x.len()).collect::<Vec<_>>(),
            params,
            None,
            None,
        ))),
        "randomforest" | "randomforestregressor" => Ok(fit_random_forest(x, y, params)),
        "gradientboosting" | "gradientboostingregressor" => Ok(fit_gradient_boosting(x, y, params)),
        "adaboost" | "adaboostregressor" => Ok(fit_adaboost(x, y, params)),
        "svr" => Ok(fit_svr(x, y, params)),
        other => Err(format!("Unknown estimator '{}'", other)),
    }
}

/// Predict with a fitted model.
pub fn predict(model: &FittedModel, x: &[Vec<f64>]) -> Vec<f64> {
    match model {
        FittedModel::Linear {
            coefficients,
            intercept,
        } => x
            .iter()
            .map(|row| {
                intercept
                    + row
                        .iter()
                        .zip(coefficients.iter())
                        .map(|(a, b)| a * b)
                        .sum::<f64>()
            })
            .collect(),
        FittedModel::Tree(tree) => x.iter().map(|row| tree.predict_one(row)).collect(),
        FittedModel::Forest { trees } => x
            .iter()
            .map(|row| {
                if trees.is_empty() {
                    0.0
                } else {
                    trees.iter().map(|t| t.predict_one(row)).sum::<f64>() / trees.len() as f64
                }
            })
            .collect(),
        FittedModel::GradientBoosting {
            init,
            learning_rate,
            trees,
        } => x
            .iter()
            .map(|row| init + learning_rate * trees.iter().map(|t| t.predict_one(row)).sum::<f64>())
            .collect(),
        FittedModel::AdaBoost { trees, weights } => x
            .iter()
            .map(|row| {
                let preds: Vec<(f64, f64)> = trees
                    .iter()
                    .zip(weights.iter())
                    .map(|(t, &w)| (t.predict_one(row), w))
                    .collect();
                weighted_median(&preds)
            })
            .collect(),
        FittedModel::Svr {
            support_vectors,
            dual_coef,
            intercept,
            kernel,
            gamma,
        } => x
            .iter()
            .map(|row| {
                let mut s = *intercept;
                for (sv, &coef) in support_vectors.iter().zip(dual_coef.iter()) {
                    s += coef * kernel_fn(kernel, *gamma, row, sv);
                }
                s
            })
            .collect(),
    }
}

// ── Linear family ──────────────────────────────────────────────────────────

fn col_means(x: &[Vec<f64>], p: usize) -> Vec<f64> {
    let n = x.len() as f64;
    let mut m = vec![0.0; p];
    for row in x {
        for j in 0..p {
            m[j] += row[j];
        }
    }
    m.iter_mut().for_each(|v| *v /= n);
    m
}

/// Ridge regression via the regularized normal equation
/// (Xc'Xc + alpha*I) beta = Xc'yc on centered data; intercept recovered from means.
fn fit_ridge(x: &[Vec<f64>], y: &[f64], alpha: f64) -> FittedModel {
    let n = x.len();
    let p = if n > 0 { x[0].len() } else { 0 };
    if n == 0 || p == 0 {
        return FittedModel::Linear {
            coefficients: vec![0.0; p],
            intercept: 0.0,
        };
    }
    let xbar = col_means(x, p);
    let ybar = y.iter().sum::<f64>() / n as f64;

    // A = Xc'Xc + alpha*I  (p x p), b = Xc'yc
    let mut a = vec![vec![0.0; p]; p];
    let mut b = vec![0.0; p];
    for (row, &yi) in x.iter().zip(y.iter()) {
        let yc = yi - ybar;
        for j in 0..p {
            let xcj = row[j] - xbar[j];
            b[j] += xcj * yc;
            for k in j..p {
                a[j][k] += xcj * (row[k] - xbar[k]);
            }
        }
    }
    for j in 0..p {
        for k in 0..j {
            a[j][k] = a[k][j];
        }
        a[j][j] += alpha;
    }
    let coef = solve_linear_system(&a, &b);
    let intercept = ybar
        - xbar
            .iter()
            .zip(coef.iter())
            .map(|(m, c)| m * c)
            .sum::<f64>();
    FittedModel::Linear {
        coefficients: coef,
        intercept,
    }
}

#[inline]
fn soft_threshold(x: f64, t: f64) -> f64 {
    if x > t {
        x - t
    } else if x < -t {
        x + t
    } else {
        0.0
    }
}

/// ElasticNet / Lasso (l1_ratio=1) via cyclic coordinate descent, matching the
/// scikit-learn objective
/// 1/(2n)||y - Xb - b0||^2 + alpha*l1_ratio*||b||_1 + 0.5*alpha*(1-l1_ratio)||b||^2.
fn fit_elastic_net(
    x: &[Vec<f64>],
    y: &[f64],
    alpha: f64,
    l1_ratio: f64,
    params: &EstimatorParams,
) -> FittedModel {
    let n = x.len();
    let p = if n > 0 { x[0].len() } else { 0 };
    if n == 0 || p == 0 {
        return FittedModel::Linear {
            coefficients: vec![0.0; p],
            intercept: 0.0,
        };
    }
    let max_iter = params.max_iter.unwrap_or(1000);
    let tol = params.tol.unwrap_or(1e-4);
    let nf = n as f64;

    let xbar = col_means(x, p);
    let ybar = y.iter().sum::<f64>() / nf;

    // Centered design, stored column-major for cache-friendly coordinate sweeps.
    let mut xc: Vec<Vec<f64>> = vec![vec![0.0; n]; p];
    for (i, row) in x.iter().enumerate() {
        for j in 0..p {
            xc[j][i] = row[j] - xbar[j];
        }
    }
    let yc: Vec<f64> = y.iter().map(|&v| v - ybar).collect();
    let col_sq: Vec<f64> = (0..p)
        .map(|j| xc[j].iter().map(|v| v * v).sum::<f64>() / nf)
        .collect();

    let mut beta = vec![0.0; p];
    let mut resid = yc.clone(); // r = yc - Xc*beta  (beta starts at 0)

    let l1 = alpha * l1_ratio;
    for _ in 0..max_iter {
        let mut max_delta = 0.0_f64;
        for j in 0..p {
            if col_sq[j] <= 0.0 {
                continue;
            }
            let bj_old = beta[j];
            // rho = (1/n) * xc_j . (r + beta_j * xc_j)
            let mut rho = 0.0;
            for i in 0..n {
                rho += xc[j][i] * (resid[i] + bj_old * xc[j][i]);
            }
            rho /= nf;
            let denom = col_sq[j] + alpha * (1.0 - l1_ratio);
            let bj_new = soft_threshold(rho, l1) / denom;
            let delta = bj_new - bj_old;
            if delta != 0.0 {
                // r -= delta * xc_j
                for i in 0..n {
                    resid[i] -= delta * xc[j][i];
                }
                beta[j] = bj_new;
                max_delta = max_delta.max(delta.abs());
            }
        }
        if max_delta < tol {
            break;
        }
    }
    let intercept = ybar
        - xbar
            .iter()
            .zip(beta.iter())
            .map(|(m, c)| m * c)
            .sum::<f64>();
    FittedModel::Linear {
        coefficients: beta,
        intercept,
    }
}

// ── CART regression tree ───────────────────────────────────────────────────

fn fit_tree(
    x: &[Vec<f64>],
    y: &[f64],
    indices: &[usize],
    params: &EstimatorParams,
    max_features: Option<usize>,
    rng: Option<&mut ChaCha8Rng>,
) -> DecisionTree {
    let max_depth = params.max_depth.unwrap_or(usize::MAX);
    let min_samples_split = params.min_samples_split.unwrap_or(2).max(2);
    let min_samples_leaf = params.min_samples_leaf.unwrap_or(1).max(1);
    let p = if x.is_empty() { 0 } else { x[0].len() };

    let mut nodes: Vec<TreeNode> = Vec::new();
    // Own an optional RNG so recursion can borrow it mutably without lifetime knots.
    let mut owned_rng = rng.map(|r| r.clone());
    build_node(
        x,
        y,
        indices,
        0,
        max_depth,
        min_samples_split,
        min_samples_leaf,
        p,
        max_features,
        &mut owned_rng,
        &mut nodes,
    );
    DecisionTree { nodes }
}

#[allow(clippy::too_many_arguments)]
fn build_node(
    x: &[Vec<f64>],
    y: &[f64],
    indices: &[usize],
    depth: usize,
    max_depth: usize,
    min_samples_split: usize,
    min_samples_leaf: usize,
    p: usize,
    max_features: Option<usize>,
    rng: &mut Option<ChaCha8Rng>,
    nodes: &mut Vec<TreeNode>,
) -> i64 {
    let n = indices.len();
    let mean = indices.iter().map(|&i| y[i]).sum::<f64>() / n as f64;

    let make_leaf = |nodes: &mut Vec<TreeNode>| -> i64 {
        nodes.push(TreeNode {
            feature: -1,
            threshold: 0.0,
            left: -1,
            right: -1,
            value: mean,
        });
        (nodes.len() - 1) as i64
    };

    if n < min_samples_split || depth >= max_depth {
        return make_leaf(nodes);
    }
    // Pure node?
    let first = y[indices[0]];
    if indices.iter().all(|&i| (y[i] - first).abs() < 1e-12) {
        return make_leaf(nodes);
    }

    // Candidate features (optionally a random subset for forests).
    let feats: Vec<usize> = match (max_features, rng.as_mut()) {
        (Some(mf), Some(r)) if mf < p => sample_without_replacement(p, mf, r),
        _ => (0..p).collect(),
    };

    let mut best_feat: i64 = -1;
    let mut best_thresh = 0.0;
    let mut best_sse = f64::INFINITY;
    let mut best_left: Vec<usize> = Vec::new();
    let mut best_right: Vec<usize> = Vec::new();

    for &f in &feats {
        // Sort sample indices by feature f.
        let mut sorted: Vec<usize> = indices.to_vec();
        sorted.sort_by(|&a, &b| {
            x[a][f]
                .partial_cmp(&x[b][f])
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Prefix sums of y and y^2 for O(1) child SSE.
        let m = sorted.len();
        let mut psum = vec![0.0; m + 1];
        let mut psq = vec![0.0; m + 1];
        for k in 0..m {
            let yi = y[sorted[k]];
            psum[k + 1] = psum[k] + yi;
            psq[k + 1] = psq[k] + yi * yi;
        }
        let total = m;
        for k in min_samples_leaf..=(total - min_samples_leaf) {
            // Split between sorted[k-1] and sorted[k]; require a real value gap.
            let v_left = x[sorted[k - 1]][f];
            let v_right = x[sorted[k]][f];
            if (v_right - v_left).abs() < 1e-12 {
                continue;
            }
            let sse_l = psq[k] - psum[k] * psum[k] / k as f64;
            let nr = (total - k) as f64;
            let sse_r = (psq[total] - psq[k]) - (psum[total] - psum[k]).powi(2) / nr;
            let sse = sse_l + sse_r;
            if sse < best_sse {
                best_sse = sse;
                best_feat = f as i64;
                best_thresh = 0.5 * (v_left + v_right);
                best_left = sorted[..k].to_vec();
                best_right = sorted[k..].to_vec();
            }
        }
    }

    if best_feat < 0 {
        return make_leaf(nodes);
    }

    // Reserve this node's slot, then build children and patch links.
    let node_idx = nodes.len();
    nodes.push(TreeNode {
        feature: best_feat,
        threshold: best_thresh,
        left: -1,
        right: -1,
        value: mean,
    });
    let left_idx = build_node(
        x,
        y,
        &best_left,
        depth + 1,
        max_depth,
        min_samples_split,
        min_samples_leaf,
        p,
        max_features,
        rng,
        nodes,
    );
    let right_idx = build_node(
        x,
        y,
        &best_right,
        depth + 1,
        max_depth,
        min_samples_split,
        min_samples_leaf,
        p,
        max_features,
        rng,
        nodes,
    );
    nodes[node_idx].left = left_idx;
    nodes[node_idx].right = right_idx;
    node_idx as i64
}

fn sample_without_replacement(n: usize, k: usize, rng: &mut ChaCha8Rng) -> Vec<usize> {
    let mut pool: Vec<usize> = (0..n).collect();
    for i in 0..k {
        let j = i + (rng.gen::<u64>() as usize) % (n - i);
        pool.swap(i, j);
    }
    pool.truncate(k);
    pool
}

// ── Random forest (bagging, parallel) ──────────────────────────────────────

fn fit_random_forest(x: &[Vec<f64>], y: &[f64], params: &EstimatorParams) -> FittedModel {
    let n = x.len();
    let n_estimators = params.n_estimators.unwrap_or(100);
    let base_seed = params.random_state.unwrap_or(0);

    let trees: Vec<DecisionTree> = (0..n_estimators)
        .into_par_iter()
        .map(|t| {
            let mut rng =
                ChaCha8Rng::seed_from_u64(base_seed.wrapping_add(t as u64).wrapping_add(1));
            // Bootstrap sample (n draws with replacement).
            let boot: Vec<usize> = (0..n).map(|_| (rng.gen::<u64>() as usize) % n).collect();
            fit_tree(x, y, &boot, params, params.max_features, Some(&mut rng))
        })
        .collect();
    FittedModel::Forest { trees }
}

// ── Gradient boosting (squared-error) ──────────────────────────────────────

fn fit_gradient_boosting(x: &[Vec<f64>], y: &[f64], params: &EstimatorParams) -> FittedModel {
    let n = x.len();
    let n_estimators = params.n_estimators.unwrap_or(100);
    let lr = params.learning_rate.unwrap_or(0.1);
    // GBM uses shallow trees by default (sklearn default max_depth=3).
    let tree_params = EstimatorParams {
        max_depth: Some(params.max_depth.unwrap_or(3)),
        ..params.clone()
    };

    let init = y.iter().sum::<f64>() / n as f64;
    let mut f = vec![init; n];
    let mut trees = Vec::with_capacity(n_estimators);
    let all: Vec<usize> = (0..n).collect();

    for _ in 0..n_estimators {
        let resid: Vec<f64> = (0..n).map(|i| y[i] - f[i]).collect();
        let tree = fit_tree(x, &resid, &all, &tree_params, None, None);
        for i in 0..n {
            f[i] += lr * tree.predict_one(&x[i]);
        }
        trees.push(tree);
    }
    FittedModel::GradientBoosting {
        init,
        learning_rate: lr,
        trees,
    }
}

// ── AdaBoost.R2 (Drucker 1997, linear loss) ────────────────────────────────

fn fit_adaboost(x: &[Vec<f64>], y: &[f64], params: &EstimatorParams) -> FittedModel {
    let n = x.len();
    let n_estimators = params.n_estimators.unwrap_or(50);
    let lr = params.learning_rate.unwrap_or(1.0);
    let base_seed = params.random_state.unwrap_or(0);
    // AdaBoostRegressor default base estimator: DecisionTreeRegressor(max_depth=3).
    let tree_params = EstimatorParams {
        max_depth: Some(params.max_depth.unwrap_or(3)),
        ..params.clone()
    };

    let mut w = vec![1.0 / n as f64; n];
    let mut trees: Vec<DecisionTree> = Vec::new();
    let mut weights: Vec<f64> = Vec::new();
    let mut rng = ChaCha8Rng::seed_from_u64(base_seed.wrapping_add(1));

    for _ in 0..n_estimators {
        // Weighted bootstrap sample.
        let cdf = cumulative(&w);
        let boot: Vec<usize> = (0..n).map(|_| sample_cdf(&cdf, rng.gen::<f64>())).collect();
        let tree = fit_tree(x, y, &boot, &tree_params, None, None);

        // Linear loss over the full (weighted) training set.
        let preds: Vec<f64> = (0..n).map(|i| tree.predict_one(&x[i])).collect();
        let errs: Vec<f64> = (0..n).map(|i| (y[i] - preds[i]).abs()).collect();
        let dmax = errs.iter().cloned().fold(0.0_f64, f64::max);
        if dmax <= 0.0 {
            // Perfect fit — keep this single estimator with full weight.
            trees.push(tree);
            weights.push(1.0);
            break;
        }
        let loss: Vec<f64> = errs.iter().map(|e| e / dmax).collect();
        let avg_loss: f64 = (0..n).map(|i| w[i] * loss[i]).sum();
        if avg_loss >= 0.5 {
            // Too weak; stop (keep prior estimators).
            if trees.is_empty() {
                trees.push(tree);
                weights.push(1.0);
            }
            break;
        }
        let beta = avg_loss / (1.0 - avg_loss);
        let estimator_weight = lr * (1.0 / beta).ln();
        trees.push(tree);
        weights.push(estimator_weight);

        // Reweight and renormalize.
        for i in 0..n {
            w[i] *= beta.powf(lr * (1.0 - loss[i]));
        }
        let wsum: f64 = w.iter().sum();
        if wsum <= 0.0 {
            break;
        }
        w.iter_mut().for_each(|v| *v /= wsum);
    }
    FittedModel::AdaBoost { trees, weights }
}

fn cumulative(w: &[f64]) -> Vec<f64> {
    let mut c = Vec::with_capacity(w.len());
    let mut acc = 0.0;
    for &v in w {
        acc += v;
        c.push(acc);
    }
    c
}

fn sample_cdf(cdf: &[f64], u01: f64) -> usize {
    let total = *cdf.last().unwrap_or(&1.0);
    let target = u01 * total;
    match cdf.binary_search_by(|v| v.partial_cmp(&target).unwrap_or(std::cmp::Ordering::Equal)) {
        Ok(i) => i.min(cdf.len() - 1),
        Err(i) => i.min(cdf.len() - 1),
    }
}

/// Weighted median of (value, weight) pairs — used by AdaBoost.R2 prediction.
fn weighted_median(pairs: &[(f64, f64)]) -> f64 {
    if pairs.is_empty() {
        return 0.0;
    }
    let mut sorted: Vec<(f64, f64)> = pairs.iter().cloned().filter(|(_, w)| *w > 0.0).collect();
    if sorted.is_empty() {
        return pairs[0].0;
    }
    sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let total: f64 = sorted.iter().map(|(_, w)| *w).sum();
    let mut acc = 0.0;
    for (v, w) in &sorted {
        acc += *w;
        if acc >= 0.5 * total {
            return *v;
        }
    }
    sorted.last().unwrap().0
}

// ── epsilon-SVR via SMO ────────────────────────────────────────────────────

#[inline]
fn kernel_fn(kernel: &str, gamma: f64, a: &[f64], b: &[f64]) -> f64 {
    match kernel {
        "linear" => a.iter().zip(b.iter()).map(|(x, y)| x * y).sum(),
        _ => {
            // RBF: exp(-gamma * ||a-b||^2)
            let d2: f64 = a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum();
            (-gamma * d2).exp()
        }
    }
}

/// epsilon-SVR trained with a simplified SMO over the (alpha - alpha*) variables.
/// Converges to a KKT-satisfying solution; close to libsvm/sklearn but not
/// guaranteed bit-identical (documented as approximate-parity).
fn fit_svr(x: &[Vec<f64>], y: &[f64], params: &EstimatorParams) -> FittedModel {
    let n = x.len();
    let p = if n > 0 { x[0].len() } else { 0 };
    let c = params.c.unwrap_or(1.0);
    let epsilon = params.epsilon.unwrap_or(0.1);
    let kernel = params.kernel.clone().unwrap_or_else(|| "rbf".to_string());
    let gamma = params
        .gamma
        .unwrap_or_else(|| if p > 0 { 1.0 / p as f64 } else { 1.0 });
    let max_iter = params.max_iter.unwrap_or(1000);
    let tol = params.tol.unwrap_or(1e-3);

    if n == 0 {
        return FittedModel::Svr {
            support_vectors: vec![],
            dual_coef: vec![],
            intercept: 0.0,
            kernel,
            gamma,
        };
    }

    // Precompute kernel matrix (n is small on this hot path).
    let mut k = vec![vec![0.0; n]; n];
    for i in 0..n {
        for j in i..n {
            let v = kernel_fn(&kernel, gamma, &x[i], &x[j]);
            k[i][j] = v;
            k[j][i] = v;
        }
    }

    // beta_i = alpha_i - alpha_i*, constrained to [-C, C], sum(beta)=0.
    let mut beta = vec![0.0; n];
    let mut bias = 0.0;

    // Prediction f(x_i) = sum_j beta_j K(i,j) + bias.
    let f = |beta: &[f64], bias: f64, i: usize, k: &[Vec<f64>]| -> f64 {
        let mut s = bias;
        for j in 0..n {
            if beta[j] != 0.0 {
                s += beta[j] * k[j][i];
            }
        }
        s
    };

    let mut rng = ChaCha8Rng::seed_from_u64(params.random_state.unwrap_or(0).wrapping_add(7));
    for _ in 0..max_iter {
        let mut max_viol = 0.0_f64;
        for i in 0..n {
            // Pick a random partner j != i.
            let mut j = (rng.gen::<u64>() as usize) % n;
            if j == i {
                j = (j + 1) % n;
            }
            // Gradients of the epsilon-insensitive objective w.r.t beta on the
            // constraint beta_i + beta_j = const.
            let ei = f(&beta, bias, i, &k) - y[i];
            let ej = f(&beta, bias, j, &k) - y[j];
            // Subgradient including epsilon tube (push toward reducing |e|-eps).
            let gi = ei + epsilon * beta[i].signum().clamp(-1.0, 1.0);
            let gj = ej + epsilon * beta[j].signum().clamp(-1.0, 1.0);
            let eta = k[i][i] + k[j][j] - 2.0 * k[i][j];
            if eta <= 1e-12 {
                continue;
            }
            let delta = (gj - gi) / eta;
            let bi_old = beta[i];
            let bj_old = beta[j];
            // Move along beta_i += delta, beta_j -= delta (keeps sum constant),
            // then clip to box [-C, C] and re-impose sum preservation.
            let mut bi = (bi_old + delta).clamp(-c, c);
            let total = bi_old + bj_old;
            let mut bj = total - bi;
            if bj > c {
                bj = c;
                bi = total - bj;
            } else if bj < -c {
                bj = -c;
                bi = total - bj;
            }
            let change = (bi - bi_old).abs();
            if change > 1e-12 {
                beta[i] = bi;
                beta[j] = bj;
                max_viol = max_viol.max(change);
            }
        }

        // Update bias from average residual over free support vectors.
        let mut bsum = 0.0;
        let mut cnt = 0;
        for i in 0..n {
            if beta[i].abs() > 1e-8 && beta[i].abs() < c - 1e-8 {
                let pred_no_bias = f(&beta, 0.0, i, &k);
                let target = y[i] - epsilon * beta[i].signum();
                bsum += target - pred_no_bias;
                cnt += 1;
            }
        }
        if cnt > 0 {
            bias = bsum / cnt as f64;
        }
        if max_viol < tol {
            break;
        }
    }

    // Keep only support vectors.
    let mut svs = Vec::new();
    let mut coefs = Vec::new();
    for i in 0..n {
        if beta[i].abs() > 1e-8 {
            svs.push(x[i].clone());
            coefs.push(beta[i]);
        }
    }
    FittedModel::Svr {
        support_vectors: svs,
        dual_coef: coefs,
        intercept: bias,
        kernel,
        gamma,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rmse(a: &[f64], b: &[f64]) -> f64 {
        (a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum::<f64>() / a.len() as f64).sqrt()
    }

    #[test]
    fn ridge_recovers_linear() {
        // y = 2x0 + 3x1 + 1, alpha small -> near-OLS.
        let x = vec![
            vec![0.0, 0.0],
            vec![1.0, 0.0],
            vec![0.0, 1.0],
            vec![1.0, 1.0],
            vec![2.0, 1.0],
            vec![1.0, 2.0],
            vec![2.0, 2.0],
        ];
        let y: Vec<f64> = x.iter().map(|r| 2.0 * r[0] + 3.0 * r[1] + 1.0).collect();
        let m = fit_estimator(
            "ridge",
            &x,
            &y,
            &EstimatorParams {
                alpha: Some(1e-6),
                ..Default::default()
            },
        )
        .unwrap();
        let pred = predict(&m, &x);
        assert!(rmse(&pred, &y) < 1e-3, "ridge rmse {}", rmse(&pred, &y));
    }

    #[test]
    fn lasso_zeros_irrelevant_feature() {
        // y depends only on x0; x1 is noise-free irrelevant.
        let x: Vec<Vec<f64>> = (0..40)
            .map(|i| vec![i as f64 * 0.1, (i % 5) as f64])
            .collect();
        let y: Vec<f64> = x.iter().map(|r| 5.0 * r[0]).collect();
        let m = fit_estimator(
            "lasso",
            &x,
            &y,
            &EstimatorParams {
                alpha: Some(0.01),
                ..Default::default()
            },
        )
        .unwrap();
        if let FittedModel::Linear { coefficients, .. } = &m {
            assert!(coefficients[0] > 4.0, "x0 coef {}", coefficients[0]);
            assert!(coefficients[1].abs() < 0.5, "x1 coef {}", coefficients[1]);
        } else {
            panic!("expected linear model");
        }
    }

    #[test]
    fn tree_fits_step_function() {
        // Step: y=0 for x<0.5, y=10 for x>=0.5.
        let x: Vec<Vec<f64>> = (0..20).map(|i| vec![i as f64 / 20.0]).collect();
        let y: Vec<f64> = x
            .iter()
            .map(|r| if r[0] < 0.5 { 0.0 } else { 10.0 })
            .collect();
        let m = fit_estimator("decisiontree", &x, &y, &EstimatorParams::default()).unwrap();
        let pred = predict(&m, &x);
        assert!(rmse(&pred, &y) < 1e-6, "tree rmse {}", rmse(&pred, &y));
    }

    #[test]
    fn forest_and_gbm_fit_quadratic() {
        let x: Vec<Vec<f64>> = (0..60).map(|i| vec![(i as f64 - 30.0) / 10.0]).collect();
        let y: Vec<f64> = x.iter().map(|r| r[0] * r[0]).collect();
        let rf = fit_estimator(
            "randomforest",
            &x,
            &y,
            &EstimatorParams {
                n_estimators: Some(40),
                random_state: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        let gbm = fit_estimator(
            "gradientboosting",
            &x,
            &y,
            &EstimatorParams {
                n_estimators: Some(60),
                learning_rate: Some(0.1),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(rmse(&predict(&rf, &x), &y) < 1.0);
        assert!(rmse(&predict(&gbm, &x), &y) < 0.5);
    }

    #[test]
    fn adaboost_fits_linear() {
        let x: Vec<Vec<f64>> = (0..50).map(|i| vec![i as f64 * 0.1]).collect();
        let y: Vec<f64> = x.iter().map(|r| 2.0 * r[0] + 1.0).collect();
        let m = fit_estimator(
            "adaboost",
            &x,
            &y,
            &EstimatorParams {
                n_estimators: Some(30),
                random_state: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(rmse(&predict(&m, &x), &y) < 1.0);
    }

    #[test]
    fn svr_rbf_fits_sine() {
        let x: Vec<Vec<f64>> = (0..40).map(|i| vec![i as f64 * 0.15]).collect();
        let y: Vec<f64> = x.iter().map(|r| r[0].sin()).collect();
        let m = fit_estimator(
            "svr",
            &x,
            &y,
            &EstimatorParams {
                c: Some(10.0),
                epsilon: Some(0.05),
                gamma: Some(0.5),
                kernel: Some("rbf".into()),
                max_iter: Some(3000),
                ..Default::default()
            },
        )
        .unwrap();
        // Approximate fit; loose tolerance (SMO is not bit-identical to libsvm).
        assert!(
            rmse(&predict(&m, &x), &y) < 0.4,
            "svr rmse {}",
            rmse(&predict(&m, &x), &y)
        );
    }
}
