//! Data-science estimator wire DTOs.

#[cfg(feature = "datascience")]
use serde::{Deserialize, Serialize};

// ── datascience ──────────────────────────────────────────────────────────────

/// Hyperparameters for the estimators. All optional; per-estimator defaults are
/// applied at fit time to mirror scikit-learn's defaults.
// `skip_serializing_if = "Option::is_none"` on every field below is
// load-bearing, not cosmetic: the server's `eg2.` MAC covers a hash of
// `rmp_serde::to_vec_named(Method)` -- the DESERIALIZED, then RE-serialized
// typed value (`Method::canonical_body_bytes`), recomputed independently of
// whatever bytes actually rode the wire. The Python client independently
// hashes only the params it actually sent (never filling in the fields a
// caller omitted). Without `skip_serializing_if`, a caller-omitted `None`
// field still round-trips as an explicit `null` key once the server
// re-serializes the struct, so the two hashes silently diverge and every
// request carrying a PARTIAL `EstimatorParams` (i.e. every real caller: no
// Python `fit_estimator` call ever sets all sixteen hyperparameters) fails
// signature verification with the generic "Authentication failed" -- it
// never reaches `fit_estimator`'s own logic. Omitting a `None` field here
// makes the server's re-serialization match a client that also omitted it,
// which is exactly what every `epistemic_graph/client.py` caller does.
#[cfg(feature = "datascience")]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EstimatorParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alpha: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1_ratio: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_depth: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_samples_split: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_samples_leaf: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n_estimators: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub learning_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_features: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subsample: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub random_state: Option<u64>,
    // SVR
    #[serde(rename = "C", skip_serializing_if = "Option::is_none")]
    pub c: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epsilon: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gamma: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kernel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_iter: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tol: Option<f64>,
}

/// A flat regression tree: node `i` is a leaf when `feature < 0`.
#[cfg(feature = "datascience")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TreeNode {
    pub feature: i64,
    pub threshold: f64,
    pub left: i64,
    pub right: i64,
    pub value: f64,
}

#[cfg(feature = "datascience")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DecisionTree {
    pub nodes: Vec<TreeNode>,
}

#[cfg(feature = "datascience")]
impl DecisionTree {
    /// Walk the flat tree to the leaf for one feature row. `pub` so the estimator
    /// code in `eg-compute::datascience` can drive prediction across the crate
    /// boundary (the data lives here; the fit/predict logic stays upstream).
    pub fn predict_one(&self, x: &[f64]) -> f64 {
        if self.nodes.is_empty() {
            return 0.0;
        }
        let mut idx = 0usize;
        loop {
            let node = &self.nodes[idx];
            if node.feature < 0 {
                return node.value;
            }
            idx = if x[node.feature as usize] <= node.threshold {
                node.left as usize
            } else {
                node.right as usize
            };
        }
    }
}

/// Serializable fitted model returned by `fit_estimator`.
#[cfg(feature = "datascience")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "model")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FittedModel {
    Linear {
        coefficients: Vec<f64>,
        intercept: f64,
    },
    Tree(DecisionTree),
    Forest {
        trees: Vec<DecisionTree>,
    },
    GradientBoosting {
        init: f64,
        learning_rate: f64,
        trees: Vec<DecisionTree>,
    },
    AdaBoost {
        trees: Vec<DecisionTree>,
        weights: Vec<f64>,
    },
    Svr {
        support_vectors: Vec<Vec<f64>>,
        dual_coef: Vec<f64>,
        intercept: f64,
        kernel: String,
        gamma: f64,
    },
}
