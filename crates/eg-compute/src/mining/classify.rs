// CONCEPT:EG-KG.mining.naive-bayes — classification (PREDICTIVE: fit → model blob → predict).
//
// Pure-Rust, dependency-light, batch. Completes the classifier family beyond the
// existing datascience tree/forest/boosting estimators with the four classical
// linear/probabilistic/instance classifiers, all as a fit→serializable-blob→predict
// pair (mirroring `datascience::estimators`):
//
//   * Gaussian / Multinomial Naive Bayes (CONCEPT:EG-KG.mining.naive-bayes) —
//     class-conditional feature likelihoods; Gaussian for continuous features,
//     Multinomial (Laplace-smoothed) for count features.
//   * k-NN                    (CONCEPT:EG-KG.mining.knn-classify) — lazy instance
//     classifier; a brute k-nearest-neighbor majority vote over the training rows.
//   * Logistic Regression     (CONCEPT:EG-KG.mining.logistic-regression) — one-vs-rest
//     linear classifier fit by batch gradient descent on the logistic loss (+L2).
//   * Linear SVM (SVC)        (CONCEPT:EG-KG.mining.linear-svc) — one-vs-rest hinge-loss
//     linear classifier fit by Pegasos-style sub-gradient descent.
//
// `fit` returns a serializable [`FittedClassifier`] (the wire blob, in eg-types so the
// protocol enum can embed it); `predict` takes that blob back plus a feature matrix
// and returns per-row labels + a per-class probability matrix. Everything is
// deterministic (GD/Pegasos are seed-free — zero init, index-ordered batch updates).
// The module is graph-agnostic (works over `&[Vec<f64>]` + `&[i64]` labels); the
// handler (`src/server/handlers/mining.rs`) supplies rows (explicit features or node
// embeddings), the labels, and does the KG write-back.

/// A point in feature space (one matrix row).
pub type Point = Vec<f64>;

/// The serializable fitted-model blob lives at the bottom of the DAG (eg-types) so
/// `Method::MineClassifyPredict` can embed it; re-exported here so the fit/predict
/// logic names it locally (mirrors `datascience::estimators`' `FittedModel` re-export).
pub use crate::wire::FittedClassifier;

/// Which classifier to fit, with its hyperparameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Algorithm {
    /// Gaussian Naive Bayes — continuous features, per-class Gaussian likelihood.
    GaussianNb,
    /// Multinomial Naive Bayes — non-negative count features, Laplace `alpha`.
    MultinomialNb { alpha: f64 },
    /// k-NN majority vote over the training rows (`k` neighbors).
    Knn { k: usize },
    /// One-vs-rest logistic regression: `epochs` of batch GD at `lr`, L2 `l2`.
    Logistic { lr: f64, epochs: usize, l2: f64 },
    /// One-vs-rest linear SVM (Pegasos sub-gradient): `epochs`, regularization `c`.
    LinearSvc { c: f64, epochs: usize, lr: f64 },
}

/// The prediction outcome, parallel to the input rows: a per-row `labels` value and
/// the per-row per-class `proba` matrix (columns ordered by `classes`).
#[derive(Debug, Clone, PartialEq)]
pub struct Classification {
    pub labels: Vec<i64>,
    pub proba: Vec<Vec<f64>>,
    pub classes: Vec<i64>,
}

/// Fit a classifier over `x` (feature rows) + `y` (integer labels, one per row).
/// Returns a serializable [`FittedClassifier`], or an error for a malformed dataset.
pub fn fit(x: &[Point], y: &[i64], algorithm: Algorithm) -> Result<FittedClassifier, String> {
    if x.is_empty() || y.is_empty() {
        return Err("classify: empty training set".into());
    }
    if x.len() != y.len() {
        return Err(format!(
            "classify: x has {} rows but y has {} labels",
            x.len(),
            y.len()
        ));
    }
    let dim = x[0].len();
    if dim == 0 {
        return Err("classify: feature rows must be non-empty".into());
    }
    if x.iter().any(|r| r.len() != dim) {
        return Err("classify: all feature rows must have the same dimensionality".into());
    }
    let classes = sorted_unique(y);
    Ok(match algorithm {
        Algorithm::GaussianNb => fit_gaussian_nb(x, y, &classes, dim),
        Algorithm::MultinomialNb { alpha } => fit_multinomial_nb(x, y, &classes, dim, alpha),
        Algorithm::Knn { k } => FittedClassifier::Knn {
            k: k.max(1),
            classes,
            x: x.to_vec(),
            y: y.to_vec(),
        },
        Algorithm::Logistic { lr, epochs, l2 } => {
            fit_linear_ovr(x, y, &classes, dim, "logistic", lr, epochs, l2, 0.0)
        }
        Algorithm::LinearSvc { c, epochs, lr } => {
            fit_linear_ovr(x, y, &classes, dim, "svc", lr, epochs, 0.0, c)
        }
    })
}

/// Predict labels + per-class probabilities for `x` using a fitted `model`.
pub fn predict(model: &FittedClassifier, x: &[Point]) -> Classification {
    match model {
        FittedClassifier::GaussianNb {
            classes,
            priors,
            means,
            vars,
        } => {
            let logs: Vec<Vec<f64>> = x
                .iter()
                .map(|row| {
                    (0..classes.len())
                        .map(|c| {
                            priors[c].max(1e-300).ln() + log_gaussian_diag(row, &means[c], &vars[c])
                        })
                        .collect()
                })
                .collect();
            finish(classes, logs, true)
        }
        FittedClassifier::MultinomialNb {
            classes,
            class_log_prior,
            feature_log_prob,
        } => {
            let logs: Vec<Vec<f64>> = x
                .iter()
                .map(|row| {
                    (0..classes.len())
                        .map(|c| {
                            let mut s = class_log_prior[c];
                            for (d, &xd) in row.iter().enumerate() {
                                s += xd * feature_log_prob[c][d];
                            }
                            s
                        })
                        .collect()
                })
                .collect();
            finish(classes, logs, true)
        }
        FittedClassifier::Knn {
            k,
            classes,
            x: xt,
            y: yt,
        } => knn_predict(*k, classes, xt, yt, x),
        FittedClassifier::LinearOvr {
            kind,
            classes,
            weights,
            biases,
        } => {
            let scores: Vec<Vec<f64>> = x
                .iter()
                .map(|row| {
                    (0..classes.len())
                        .map(|c| {
                            let raw = dot(&weights[c], row) + biases[c];
                            if kind == "logistic" {
                                sigmoid(raw)
                            } else {
                                raw
                            }
                        })
                        .collect()
                })
                .collect();
            // Labels = argmax score; proba = softmax over the per-class scores.
            let logs: Vec<Vec<f64>> = scores;
            finish(classes, logs, kind == "svc")
        }
    }
}

// ─────────────────────────── Naive Bayes ───────────────────────────

/// Gaussian Naive Bayes (CONCEPT:EG-KG.mining.naive-bayes): per class, the feature
/// mean + variance (variance-floored), and the class prior.
fn fit_gaussian_nb(x: &[Point], y: &[i64], classes: &[i64], dim: usize) -> FittedClassifier {
    const VAR_FLOOR: f64 = 1e-9;
    let n = x.len() as f64;
    let mut priors = Vec::with_capacity(classes.len());
    let mut means = Vec::with_capacity(classes.len());
    let mut vars = Vec::with_capacity(classes.len());
    for &cls in classes {
        let idx: Vec<usize> = (0..x.len()).filter(|&i| y[i] == cls).collect();
        let cn = idx.len().max(1) as f64;
        priors.push(idx.len() as f64 / n);
        let mut mean = vec![0.0; dim];
        for &i in &idx {
            for d in 0..dim {
                mean[d] += x[i][d];
            }
        }
        for m in mean.iter_mut() {
            *m /= cn;
        }
        let mut var = vec![0.0; dim];
        for &i in &idx {
            for d in 0..dim {
                let diff = x[i][d] - mean[d];
                var[d] += diff * diff;
            }
        }
        for v in var.iter_mut() {
            *v = (*v / cn).max(VAR_FLOOR);
        }
        means.push(mean);
        vars.push(var);
    }
    FittedClassifier::GaussianNb {
        classes: classes.to_vec(),
        priors,
        means,
        vars,
    }
}

/// Multinomial Naive Bayes (CONCEPT:EG-KG.mining.naive-bayes): Laplace-smoothed
/// class-conditional feature log-probabilities over non-negative count features.
fn fit_multinomial_nb(
    x: &[Point],
    y: &[i64],
    classes: &[i64],
    dim: usize,
    alpha: f64,
) -> FittedClassifier {
    let alpha = if alpha > 0.0 { alpha } else { 1.0 };
    let n = x.len() as f64;
    let mut class_log_prior = Vec::with_capacity(classes.len());
    let mut feature_log_prob = Vec::with_capacity(classes.len());
    for &cls in classes {
        let idx: Vec<usize> = (0..x.len()).filter(|&i| y[i] == cls).collect();
        class_log_prior.push(((idx.len() as f64) / n).max(1e-300).ln());
        // Per-feature summed counts (clamp negatives to 0 — Multinomial is count-based).
        let mut counts = vec![0.0f64; dim];
        for &i in &idx {
            for d in 0..dim {
                counts[d] += x[i][d].max(0.0);
            }
        }
        let total: f64 = counts.iter().sum::<f64>() + alpha * dim as f64;
        let logp: Vec<f64> = counts.iter().map(|&c| ((c + alpha) / total).ln()).collect();
        feature_log_prob.push(logp);
    }
    FittedClassifier::MultinomialNb {
        classes: classes.to_vec(),
        class_log_prior,
        feature_log_prob,
    }
}

// ─────────────────────────── k-NN ───────────────────────────

/// Brute k-NN majority vote (CONCEPT:EG-KG.mining.knn-classify). For each query row,
/// the `k` nearest training rows by Euclidean distance vote; ties break to the
/// smallest class label. `proba` = the per-class vote fraction. (A brute scan over the
/// feature rows — the ANN index would accelerate this at 1M+ scale, but the exact
/// brute vote keeps parity deterministic.)
fn knn_predict(k: usize, classes: &[i64], xt: &[Point], yt: &[i64], x: &[Point]) -> Classification {
    let k = k.clamp(1, xt.len().max(1));
    let class_index: std::collections::HashMap<i64, usize> =
        classes.iter().enumerate().map(|(i, &c)| (c, i)).collect();
    let mut labels = Vec::with_capacity(x.len());
    let mut proba = Vec::with_capacity(x.len());
    for row in x {
        // Nearest k by (distance, index) — deterministic tie-break.
        let mut order: Vec<usize> = (0..xt.len()).collect();
        order.sort_by(|&a, &b| {
            sq_dist(row, &xt[a])
                .partial_cmp(&sq_dist(row, &xt[b]))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.cmp(&b))
        });
        let mut votes = vec![0.0f64; classes.len()];
        for &nbr in order.iter().take(k) {
            if let Some(&ci) = class_index.get(&yt[nbr]) {
                votes[ci] += 1.0;
            }
        }
        let total: f64 = votes.iter().sum::<f64>().max(1.0);
        let p: Vec<f64> = votes.iter().map(|&v| v / total).collect();
        // argmax vote, tie → smallest class label (classes is sorted ascending).
        let best = (0..classes.len())
            .max_by(|&a, &b| {
                votes[a]
                    .partial_cmp(&votes[b])
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(b.cmp(&a)) // prefer smaller index on a tie
            })
            .unwrap_or(0);
        labels.push(classes[best]);
        proba.push(p);
    }
    Classification {
        labels,
        proba,
        classes: classes.to_vec(),
    }
}

// ─────────────────────────── Linear (logistic / SVC), one-vs-rest ───────────────────────────

/// Fit a one-vs-rest linear classifier (CONCEPT:EG-KG.mining.logistic-regression /
/// CONCEPT:EG-KG.mining.linear-svc). For each class it trains a binary linear model
/// (`logistic`: batch GD on the logistic loss with L2 `l2`; `svc`: Pegasos-style
/// sub-gradient hinge-loss with regularization derived from `c`), storing the weight
/// vector + bias. Deterministic: zero init, index-ordered full-batch updates.
#[allow(clippy::too_many_arguments)]
fn fit_linear_ovr(
    x: &[Point],
    y: &[i64],
    classes: &[i64],
    dim: usize,
    kind: &str,
    lr: f64,
    epochs: usize,
    l2: f64,
    c: f64,
) -> FittedClassifier {
    let n = x.len() as f64;
    let lr = if lr > 0.0 { lr } else { 0.1 };
    let epochs = epochs.max(1);
    let mut weights = Vec::with_capacity(classes.len());
    let mut biases = Vec::with_capacity(classes.len());
    for &cls in classes {
        let mut w = vec![0.0f64; dim];
        let mut b = 0.0f64;
        if kind == "logistic" {
            for _ in 0..epochs {
                let mut gw = vec![0.0f64; dim];
                let mut gb = 0.0f64;
                for (i, row) in x.iter().enumerate() {
                    let t = if y[i] == cls { 1.0 } else { 0.0 };
                    let p = sigmoid(dot(&w, row) + b);
                    let err = p - t;
                    for d in 0..dim {
                        gw[d] += err * row[d];
                    }
                    gb += err;
                }
                for d in 0..dim {
                    w[d] -= lr * (gw[d] / n + l2 * w[d]);
                }
                b -= lr * (gb / n);
            }
        } else {
            // SVC: minimize (lambda/2)||w||^2 + (1/n) Σ hinge(s_i (w·x+b)).
            let lambda = 1.0 / (c.max(1e-6) * n);
            for _ in 0..epochs {
                let mut gw: Vec<f64> = w.iter().map(|&wi| lambda * wi).collect();
                let mut gb = 0.0f64;
                for (i, row) in x.iter().enumerate() {
                    let s = if y[i] == cls { 1.0 } else { -1.0 };
                    let margin = s * (dot(&w, row) + b);
                    if margin < 1.0 {
                        for d in 0..dim {
                            gw[d] -= s * row[d] / n;
                        }
                        gb -= s / n;
                    }
                }
                for d in 0..dim {
                    w[d] -= lr * gw[d];
                }
                b -= lr * gb;
            }
        }
        weights.push(w);
        biases.push(b);
    }
    FittedClassifier::LinearOvr {
        kind: kind.to_string(),
        classes: classes.to_vec(),
        weights,
        biases,
    }
}

// ─────────────────────────── shared helpers ───────────────────────────

/// Turn per-class scores (`logs`) into labels (argmax) + a proba matrix. When
/// `softmax_scores` the scores are turned into probabilities by a numerically-stable
/// softmax (log-likelihoods for NB, raw margins for SVC); otherwise they are already
/// per-class probabilities (logistic sigmoids) and are just renormalized to sum 1.
fn finish(classes: &[i64], logs: Vec<Vec<f64>>, softmax_scores: bool) -> Classification {
    let mut labels = Vec::with_capacity(logs.len());
    let mut proba = Vec::with_capacity(logs.len());
    for row in &logs {
        let best = argmax(row);
        labels.push(classes[best]);
        let p = if softmax_scores {
            softmax(row)
        } else {
            let total: f64 = row.iter().sum::<f64>();
            if total > 0.0 {
                row.iter().map(|&v| v / total).collect()
            } else {
                softmax(row)
            }
        };
        proba.push(p);
    }
    Classification {
        labels,
        proba,
        classes: classes.to_vec(),
    }
}

fn softmax(v: &[f64]) -> Vec<f64> {
    let m = v.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let exps: Vec<f64> = v.iter().map(|&x| (x - m).exp()).collect();
    let sum: f64 = exps.iter().sum::<f64>().max(1e-300);
    exps.iter().map(|&e| e / sum).collect()
}

fn sigmoid(z: f64) -> f64 {
    1.0 / (1.0 + (-z).exp())
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

fn argmax(v: &[f64]) -> usize {
    let mut best = 0;
    for i in 1..v.len() {
        if v[i] > v[best] {
            best = i;
        }
    }
    best
}

fn dot(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

fn sq_dist(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

fn sorted_unique(y: &[i64]) -> Vec<i64> {
    let mut v: Vec<i64> = y
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    v.sort_unstable();
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A linearly-separable 2-class fixture: class 0 near the origin, class 1 near
    /// (10,10). Rows 0..4 = class 0, rows 5..9 = class 1.
    fn separable_2class() -> (Vec<Point>, Vec<i64>) {
        let x = vec![
            vec![0.0, 0.0],
            vec![0.5, 0.3],
            vec![0.2, 0.8],
            vec![0.9, 0.1],
            vec![0.3, 0.5],
            vec![10.0, 10.0],
            vec![10.5, 9.7],
            vec![9.8, 10.4],
            vec![10.2, 9.9],
            vec![9.6, 10.1],
        ];
        let y = vec![0, 0, 0, 0, 0, 1, 1, 1, 1, 1];
        (x, y)
    }

    fn accuracy(model: &FittedClassifier, x: &[Point], y: &[i64]) -> f64 {
        let out = predict(model, x);
        let correct = out.labels.iter().zip(y).filter(|(a, b)| a == b).count();
        correct as f64 / y.len() as f64
    }

    #[test]
    fn gaussian_nb_separates_two_classes() {
        let (x, y) = separable_2class();
        let m = fit(&x, &y, Algorithm::GaussianNb).unwrap();
        assert_eq!(accuracy(&m, &x, &y), 1.0);
        // A fresh point near the origin classifies as 0; near (10,10) as 1.
        let out = predict(&m, &[vec![0.4, 0.4], vec![9.9, 10.1]]);
        assert_eq!(out.labels, vec![0, 1]);
        // Probabilities are a valid distribution.
        for p in &out.proba {
            assert!((p.iter().sum::<f64>() - 1.0).abs() < 1e-9);
        }
    }

    #[test]
    fn multinomial_nb_classifies_count_features() {
        // Two "topics": topic 0 loads feature 0, topic 1 loads feature 1.
        let x = vec![
            vec![8.0, 1.0, 0.0],
            vec![9.0, 0.0, 1.0],
            vec![7.0, 2.0, 0.0],
            vec![0.0, 1.0, 8.0],
            vec![1.0, 0.0, 9.0],
            vec![0.0, 2.0, 7.0],
        ];
        let y = vec![0, 0, 0, 1, 1, 1];
        let m = fit(&x, &y, Algorithm::MultinomialNb { alpha: 1.0 }).unwrap();
        assert_eq!(accuracy(&m, &x, &y), 1.0);
        let out = predict(&m, &[vec![10.0, 0.0, 0.0], vec![0.0, 0.0, 10.0]]);
        assert_eq!(out.labels, vec![0, 1]);
    }

    #[test]
    fn knn_classifies_blobs() {
        let (x, y) = separable_2class();
        let m = fit(&x, &y, Algorithm::Knn { k: 3 }).unwrap();
        assert_eq!(accuracy(&m, &x, &y), 1.0);
        let out = predict(&m, &[vec![0.1, 0.2], vec![10.1, 9.8]]);
        assert_eq!(out.labels, vec![0, 1]);
        // Vote fractions sum to 1.
        for p in &out.proba {
            assert!((p.iter().sum::<f64>() - 1.0).abs() < 1e-9);
        }
    }

    #[test]
    fn logistic_recovers_linear_boundary() {
        let (x, y) = separable_2class();
        let m = fit(
            &x,
            &y,
            Algorithm::Logistic {
                lr: 0.5,
                epochs: 500,
                l2: 0.0,
            },
        )
        .unwrap();
        assert_eq!(accuracy(&m, &x, &y), 1.0);
        let out = predict(&m, &[vec![0.4, 0.4], vec![10.0, 10.0]]);
        assert_eq!(out.labels, vec![0, 1]);
    }

    #[test]
    fn linear_svc_recovers_linear_boundary() {
        let (x, y) = separable_2class();
        let m = fit(
            &x,
            &y,
            Algorithm::LinearSvc {
                c: 1.0,
                epochs: 500,
                lr: 0.1,
            },
        )
        .unwrap();
        assert_eq!(accuracy(&m, &x, &y), 1.0);
    }

    #[test]
    fn multiclass_one_vs_rest() {
        // Three well-separated blobs → OvR logistic must separate all three.
        let mut x = Vec::new();
        let mut y = Vec::new();
        for (cls, (cx, cy)) in [(0, (0.0, 0.0)), (1, (10.0, 0.0)), (2, (0.0, 10.0))]
            .into_iter()
            .enumerate()
            .map(|(_, v)| v)
        {
            for i in 0..5 {
                let t = i as f64 * 0.1;
                x.push(vec![cx + t, cy - t]);
                y.push(cls as i64);
            }
        }
        let m = fit(
            &x,
            &y,
            Algorithm::Logistic {
                lr: 0.3,
                epochs: 800,
                l2: 0.0,
            },
        )
        .unwrap();
        assert_eq!(accuracy(&m, &x, &y), 1.0);
        assert_eq!(predict(&m, &[vec![0.0, 9.0]]).labels, vec![2]);
    }

    #[test]
    fn fit_rejects_mismatched_lengths() {
        let x = vec![vec![0.0], vec![1.0]];
        let y = vec![0];
        assert!(fit(&x, &y, Algorithm::GaussianNb).is_err());
    }

    #[test]
    fn deterministic_fit() {
        let (x, y) = separable_2class();
        let a = fit(
            &x,
            &y,
            Algorithm::Logistic {
                lr: 0.5,
                epochs: 200,
                l2: 0.01,
            },
        )
        .unwrap();
        let b = fit(
            &x,
            &y,
            Algorithm::Logistic {
                lr: 0.5,
                epochs: 200,
                l2: 0.01,
            },
        )
        .unwrap();
        assert_eq!(predict(&a, &x).labels, predict(&b, &x).labels);
    }
}
