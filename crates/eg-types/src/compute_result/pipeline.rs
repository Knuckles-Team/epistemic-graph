//! Results of the composable ML pipeline (CONCEPT:EG-KG.mining.ml-pipeline).

use serde::{Deserialize, Serialize};

use super::graphlearn::PredictedLink;

/// `MiningPipelineTrain`: the trained version and its held-out metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PipelineTrainResult {
    pub name: String,
    /// The assigned version; 0 when not written back.
    pub version: u64,
    pub model_id: String,
    pub family: String,
    pub algorithm: String,
    /// Per-split metric objects; their keys depend on the model family.
    pub metrics: serde_json::Value,
    pub n_features: usize,
    pub n_train: usize,
    pub n_test: usize,
    /// The label classes of a classifier; `null` for other families.
    pub classes: serde_json::Value,
    pub written_back: bool,
}

/// `MiningPipelineServe`: the version now served for `name`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PipelineServeResult {
    pub name: String,
    pub version: u64,
    pub model_id: String,
    pub served: bool,
}

/// One classified row.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PipelineClassifiedRow {
    pub id: String,
    pub label: i64,
    pub proba: Vec<f64>,
}

/// One regressed row.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PipelineValueRow {
    pub id: String,
    pub value: f64,
}

/// Predictions of a `classify`-family model.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClassifyPrediction {
    pub model_id: String,
    pub rows: Vec<PipelineClassifiedRow>,
    pub classes: Vec<i64>,
    pub n_rows: usize,
    pub written_back: usize,
}

/// Predictions of an `estimator`-family model.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EstimatorPrediction {
    pub model_id: String,
    pub rows: Vec<PipelineValueRow>,
    pub n_rows: usize,
    pub written_back: usize,
}

/// The top missing links scored by a `graphlearn`-family model.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GraphlearnPrediction {
    pub model_id: String,
    pub predicted: Vec<PredictedLink>,
    pub n_predicted: usize,
}

/// `MiningPipelinePredict`: the body is selected by the served model's family,
/// published as the `family` field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(tag = "family", rename_all = "lowercase")]
pub enum PipelinePrediction {
    Classify(ClassifyPrediction),
    Estimator(EstimatorPrediction),
    Graphlearn(GraphlearnPrediction),
}

/// `MiningPipelineEvaluate`: metrics of a served tabular model on caller data.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PipelineEvaluation {
    pub name: String,
    pub version: u64,
    pub family: String,
    /// The family's metric object (accuracy/F1 for classify, R²/RMSE for estimator).
    pub metrics: serde_json::Value,
    pub n: usize,
}

/// `MiningPipelineCompare`: two versions' recorded metrics and their numeric diff.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PipelineComparison {
    pub name: String,
    pub version_a: u64,
    pub version_b: u64,
    pub algorithm_a: String,
    pub algorithm_b: String,
    pub metrics_a: serde_json::Value,
    pub metrics_b: serde_json::Value,
    /// `b - a` for every numeric primary metric both versions share; `null` when
    /// either version has no metric object.
    pub diff: serde_json::Value,
}
