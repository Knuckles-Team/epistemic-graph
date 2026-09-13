//! Results of the vector miners: cluster, anomaly, classify and reduce.

use serde::{Deserialize, Serialize};

use crate::wire::FittedClassifier;

/// A row of a vector-mining input: its node id when the rows came from a node
/// source, else its index in the explicit input matrix.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum RowRef {
    Id(String),
    Index(usize),
}

impl RowRef {
    /// The node id at `index` of `ids`, else the index itself.
    pub fn at(ids: &[String], index: usize) -> Self {
        match ids.get(index) {
            Some(id) => RowRef::Id(id.clone()),
            None => RowRef::Index(index),
        }
    }
}

/// One cluster; `cluster_id` is negative for a noise bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClusterRow {
    pub cluster_id: i64,
    pub members: Vec<RowRef>,
    pub centroid: Vec<f64>,
    pub score: f64,
}

/// `MineCluster`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClusterMiningResult {
    pub clusters: Vec<ClusterRow>,
    pub labels: Vec<i64>,
    pub n_rows: usize,
    /// Clusters with a non-negative id.
    pub n_clusters: usize,
    pub written_back: usize,
    /// Per-row soft assignments; present only for a mixture model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub responsibilities: Option<Vec<Vec<f64>>>,
}

/// One scored row of `MineAnomaly`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AnomalyRow {
    pub id: RowRef,
    pub anomaly_score: f64,
    pub is_anomaly: bool,
}

/// `MineAnomaly`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AnomalyMiningResult {
    pub rows: Vec<AnomalyRow>,
    pub n_rows: usize,
    pub n_anomalies: usize,
    pub threshold: f64,
    pub written_back: usize,
}

/// `MineClassifyFit`: the fitted classifier, passed back to `MineClassifyPredict`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClassifierFitResult {
    pub model: FittedClassifier,
    pub algorithm: String,
    pub n_samples: usize,
    pub classes: Vec<i64>,
}

/// One classified row of `MineClassifyPredict`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClassifiedRow {
    pub id: RowRef,
    pub label: i64,
    pub proba: Vec<f64>,
}

/// `MineClassifyPredict`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClassificationMiningResult {
    pub rows: Vec<ClassifiedRow>,
    pub classes: Vec<i64>,
    pub n_rows: usize,
    pub written_back: usize,
}

/// One embedded row of `MineReduce`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ReducedRow {
    pub id: RowRef,
    pub coords: Vec<f64>,
}

/// `MineReduce`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ReductionMiningResult {
    pub rows: Vec<ReducedRow>,
    pub algorithm: String,
    pub n_rows: usize,
    pub n_components: usize,
    pub written_back: usize,
    /// Present only for an algorithm that produces singular values.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub singular_values: Option<Vec<f64>>,
}
