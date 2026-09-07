use serde::{Deserialize, Serialize};

use super::digest::cbor;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum SemanticVectorMetric {
    Cosine,
    DotProduct,
    Euclidean,
}

impl SemanticVectorMetric {
    pub(crate) fn as_str(self) -> &'static str {
        const NAMES: [&str; 3] = ["cosine", "dot_product", "euclidean"];
        NAMES[self as usize]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum SemanticAnnIndexMethod {
    Hnsw,
    IvfPq,
}

impl SemanticAnnIndexMethod {
    pub(crate) fn as_str(self) -> &'static str {
        const NAMES: [&str; 2] = ["hnsw", "ivf_pq"];
        NAMES[self as usize]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum SemanticBindingState {
    Pending,
    Building,
    Live,
    Disabled,
    Failed,
    Dropping,
}

impl SemanticBindingState {
    pub fn as_str(self) -> &'static str {
        const NAMES: [&str; 6] = [
            "pending", "building", "live", "disabled", "failed", "dropping",
        ];
        NAMES[self as usize]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticModelIdentity {
    pub model_id: String,
    pub model_revision: String,
    pub preprocess_digest: String,
    pub model_digest: String,
}

/// Pre-binding lexical configuration. It intentionally has no binding or
/// final index digest, breaking the otherwise circular binding identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticLexicalIndexSpec {
    pub analyzer_id: String,
    pub analyzer_revision: String,
    pub analyzer_config_digest: String,
}

impl SemanticLexicalIndexSpec {
    pub(crate) fn canonical_cbor(&self) -> Vec<u8> {
        cbor::map([
            ("analyzer_id", cbor::text(&self.analyzer_id)),
            ("analyzer_revision", cbor::text(&self.analyzer_revision)),
            (
                "analyzer_config_digest",
                cbor::text(&self.analyzer_config_digest),
            ),
        ])
    }
}

/// Pre-binding ANN configuration. It intentionally has no binding or final
/// index digest; both are derived only after the binding digest exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticAnnIndexSpec {
    pub method: SemanticAnnIndexMethod,
    pub parameters_digest: String,
}

impl SemanticAnnIndexSpec {
    pub(crate) fn canonical_cbor(&self) -> Vec<u8> {
        cbor::map([
            ("method", cbor::text(self.method.as_str())),
            ("parameters_digest", cbor::text(&self.parameters_digest)),
        ])
    }
}
