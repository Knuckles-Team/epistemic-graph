//! Composable machine-learning pipeline wire DTOs.

#[cfg(feature = "ml-pipeline")]
use serde::{Deserialize, Serialize};

// ── ML pipeline (CONCEPT:EG-KG.mining.ml-pipeline) ────────────────────────────
// PURE-data wire DTOs for the composable `MiningPipeline*` surface: ordered feature
// steps → split → a pluggable model family (classify / estimator / graphlearn) → a
// versioned `:Model` artifact → serve/predict. It GENERALIZES the KAN one-off: the
// same train/eval/serve/predict lifecycle drives any registered family. The
// orchestration logic lives in the server handler (`handlers/pipeline.rs`); the ML
// primitives it composes (structural embeddings, classify, regression estimators, the
// KAN link-predictor, metrics) all already live in `eg-compute`. Free-form
// hyperparameters ride a `serde_json::Value` (`params`) so a new family needs no new
// wire field — the handler extracts what its family consumes, with per-family defaults.

/// One ordered feature-construction step. Steps run left→right: the first PRODUCING
/// step (`Embedding`/`NodeVector`) yields the `(node_id, row)` matrix; later transform
/// steps (`Normalize`) rewrite the rows in place. An empty step list ⇒ the caller's
/// explicit `x` matrix is used verbatim.
#[cfg(feature = "ml-pipeline")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "step", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FeatureStep {
    /// Structural node embeddings computed over the label subgraph — `method` is
    /// `fastrp` (default, training-free) or `node2vec` (biased walks + SGNS). The
    /// walk/epoch knobs apply to `node2vec`; `iterations` to `fastrp`.
    Embedding {
        #[serde(default = "default_embed_method")]
        method: String,
        #[serde(default = "default_embed_dim")]
        dim: usize,
        #[serde(default = "default_fastrp_iterations")]
        iterations: usize,
        #[serde(default = "default_n2v_walk_length")]
        walk_length: usize,
        #[serde(default = "default_n2v_walks_per_node")]
        walks_per_node: usize,
        #[serde(default = "default_n2v_window")]
        window: usize,
        #[serde(default = "default_n2v_epochs")]
        epochs: usize,
        #[serde(default = "default_embed_seed")]
        seed: u64,
    },
    /// Read each source node's pre-stored embedding vector from the SemanticStore
    /// (the "classify these nodes by their existing embeddings" path).
    NodeVector {},
    /// L2-normalize each feature row in place (a zero row is left untouched).
    Normalize {},
}

/// serde default: the default structural embedder.
#[cfg(feature = "ml-pipeline")]
fn default_embed_method() -> String {
    "fastrp".to_string()
}
/// serde default: embedding dimensionality.
#[cfg(feature = "ml-pipeline")]
fn default_embed_dim() -> usize {
    64
}
/// serde default: FastRP iterations.
#[cfg(feature = "ml-pipeline")]
fn default_fastrp_iterations() -> usize {
    3
}
/// serde default: Node2Vec walk length.
#[cfg(feature = "ml-pipeline")]
fn default_n2v_walk_length() -> usize {
    40
}
/// serde default: Node2Vec walks per node.
#[cfg(feature = "ml-pipeline")]
fn default_n2v_walks_per_node() -> usize {
    10
}
/// serde default: Node2Vec SGNS window.
#[cfg(feature = "ml-pipeline")]
fn default_n2v_window() -> usize {
    5
}
/// serde default: Node2Vec training epochs.
#[cfg(feature = "ml-pipeline")]
fn default_n2v_epochs() -> usize {
    5
}
/// serde default: embedding seed (deterministic).
#[cfg(feature = "ml-pipeline")]
fn default_embed_seed() -> u64 {
    7
}

/// Train/test split knobs (seeded, deterministic — reuses `datascience::primitives::
/// train_test_split`).
#[cfg(feature = "ml-pipeline")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SplitSpec {
    #[serde(default = "default_test_ratio")]
    pub test_ratio: f64,
    #[serde(default = "default_shuffle")]
    pub shuffle: bool,
    #[serde(default = "default_split_seed")]
    pub seed: u64,
}

#[cfg(feature = "ml-pipeline")]
fn default_test_ratio() -> f64 {
    0.25
}
#[cfg(feature = "ml-pipeline")]
fn default_shuffle() -> bool {
    true
}
#[cfg(feature = "ml-pipeline")]
fn default_split_seed() -> u64 {
    42
}

#[cfg(feature = "ml-pipeline")]
impl Default for SplitSpec {
    fn default() -> Self {
        Self {
            test_ratio: default_test_ratio(),
            shuffle: default_shuffle(),
            seed: default_split_seed(),
        }
    }
}

/// The model family + its algorithm + free-form hyperparameters. `family` is
/// `classify` (node classification), `estimator`/`regress` (regression), or
/// `graphlearn`/`kan` (link prediction — the KAN one-off, now a pipeline family).
/// `algorithm` selects within the family (e.g. classify → `logistic`; estimator →
/// `ridge`). `params` carries family-specific knobs the handler reads with defaults.
#[cfg(feature = "ml-pipeline")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ModelSpec {
    #[serde(default = "default_family")]
    pub family: String,
    #[serde(default)]
    pub algorithm: String,
    #[serde(default = "default_params")]
    pub params: serde_json::Value,
}

#[cfg(feature = "ml-pipeline")]
fn default_family() -> String {
    "classify".to_string()
}
#[cfg(feature = "ml-pipeline")]
fn default_params() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

/// The full composable pipeline recipe: ordered feature steps → split → model. Stored
/// (minus the split) inside the `:Model` artifact so eval/predict rebuild features
/// IDENTICALLY from the same recipe without the caller re-specifying it.
#[cfg(feature = "ml-pipeline")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PipelineSpec {
    #[serde(default)]
    pub features: Vec<FeatureStep>,
    #[serde(default)]
    pub split: SplitSpec,
    /// Node property holding the integer class label (node classification). Empty ⇒
    /// labels must be passed explicitly as `y`. Ignored by the `graphlearn` family
    /// (its labels are the observed edges).
    #[serde(default)]
    pub label_property: String,
    pub model: ModelSpec,
}
