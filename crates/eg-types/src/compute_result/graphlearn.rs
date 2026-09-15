//! Results of the KAN link predictor (CONCEPT:EG-KG.graphlearn.link-predictor).

use serde::{Deserialize, Serialize};

/// One learned first-layer edge function.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EdgeFunctionRow {
    pub feature: String,
    pub hidden_output: usize,
    /// Polynomial basis family: `chebyshev` or `jacobi`.
    pub basis: String,
    pub coefficients: Vec<f64>,
}

/// `GraphLearnFit`: the fitted model and a summary of what it learned.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LinkPredictorFit {
    /// The fitted model, passed back verbatim to `GraphLearnPredict`. Opaque on the
    /// wire (the request carries it as JSON too), so no KAN type leaks here.
    pub model: serde_json::Value,
    pub n_nodes: usize,
    pub n_edges: usize,
    pub train_auc: f64,
    pub basis: String,
    pub degree: usize,
    pub edge_functions: Vec<EdgeFunctionRow>,
    pub edge_functions_written: usize,
}

/// One predicted link.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PredictedLink {
    pub src: String,
    pub dst: String,
    pub score: f64,
}

/// `GraphLearnPredict`: scored links and the model digest that scored them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LinkPrediction {
    pub predicted: Vec<PredictedLink>,
    pub n_predicted: usize,
    /// Digest of the model, `kanlink:<hex>`.
    pub model: String,
    pub written_back: usize,
}
