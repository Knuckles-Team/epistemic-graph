//! Results of the data-science primitives and training kernels
//! (CONCEPT:EG-KG.compute.rust-native-training-loss).

use serde::{Deserialize, Serialize};

/// Result of an ordinary-least-squares regression.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RegressionResult {
    pub coefficients: Vec<f64>,
    pub intercept: f64,
    pub r_squared: f64,
    pub residuals: Vec<f64>,
}

/// Result of K-means clustering.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct KMeansResult {
    pub labels: Vec<usize>,
    pub centroids: Vec<Vec<f64>>,
    pub inertia: f64,
    pub n_iterations: usize,
}

/// Result of PCA dimensionality reduction.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PCAResult {
    pub components: Vec<Vec<f64>>,
    pub explained_variance: Vec<f64>,
    pub explained_variance_ratio: Vec<f64>,
    pub transformed: Vec<Vec<f64>>,
}

/// Dataset statistics.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DatasetStats {
    pub means: Vec<f64>,
    pub stds: Vec<f64>,
    pub mins: Vec<f64>,
    pub maxs: Vec<f64>,
    pub correlation_matrix: Vec<Vec<f64>>,
    pub n_samples: usize,
    pub n_features: usize,
}

/// The four partitions of a train/test split.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TrainTestSplitResult {
    pub x_train: Vec<Vec<f64>>,
    pub x_test: Vec<Vec<f64>>,
    pub y_train: Vec<f64>,
    pub y_test: Vec<f64>,
}

/// Mean categorical cross-entropy and its gradient w.r.t. the logits.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CrossEntropyResult {
    pub loss: f64,
    /// dL/dlogits, shape == logits (softmax - one_hot, averaged over the batch).
    pub grad: Vec<Vec<f64>>,
}

/// Bradley-Terry DPO loss and its gradients w.r.t. the chosen/rejected log-probs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DpoResult {
    pub loss: f64,
    pub grad_chosen: Vec<f64>,
    pub grad_rejected: Vec<f64>,
}

/// PPO/GRPO clipped surrogate loss and its gradient w.r.t. `logprob`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GrpoResult {
    pub loss: f64,
    pub grad: Vec<f64>,
}

/// One Adam step: the updated parameters and moment estimates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AdamResult {
    pub params: Vec<f64>,
    pub m: Vec<f64>,
    pub v: Vec<f64>,
}
