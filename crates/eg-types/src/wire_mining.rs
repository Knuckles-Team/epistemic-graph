//! Classification model wire DTOs.

#[cfg(feature = "mining")]
use serde::{Deserialize, Serialize};

// ── mining: classification (CONCEPT:EG-KG.mining.naive-bayes) ─────────────────────

/// Serializable fitted classifier returned by `mining::classify::fit` and consumed
/// back by `mining::classify::predict` (the PREDICTIVE fit→blob→predict pair). Lives
/// in eg-types so `Method::MineClassifyPredict` can embed the model over the wire —
/// exactly like `FittedModel` carries the datascience regressors. `classes` is the
/// sorted set of integer labels; every score/proba vector is column-ordered by it.
#[cfg(feature = "mining")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "model")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FittedClassifier {
    /// Gaussian Naive Bayes — per-class prior + per-feature (mean, variance).
    GaussianNb {
        classes: Vec<i64>,
        priors: Vec<f64>,
        means: Vec<Vec<f64>>,
        vars: Vec<Vec<f64>>,
    },
    /// Multinomial Naive Bayes — per-class log-prior + Laplace-smoothed feature log-probs.
    MultinomialNb {
        classes: Vec<i64>,
        class_log_prior: Vec<f64>,
        feature_log_prob: Vec<Vec<f64>>,
    },
    /// k-NN — the lazy classifier stores its training rows + labels for the vote.
    Knn {
        k: usize,
        classes: Vec<i64>,
        x: Vec<Vec<f64>>,
        y: Vec<i64>,
    },
    /// One-vs-rest linear classifier (`kind` = `logistic` | `svc`): a weight vector +
    /// bias per class. Prediction is argmax of the per-class score.
    LinearOvr {
        kind: String,
        classes: Vec<i64>,
        weights: Vec<Vec<f64>>,
        biases: Vec<f64>,
    },
}
