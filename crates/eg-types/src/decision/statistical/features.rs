//! The versioned feature schema a statistical decision reads (EH-021).
//!
//! A feature is computed ONLY over the candidates the caller can see: library
//! candidates are tenant-visible by construction, and graph candidates are the
//! rows an RLS-filtered plan returned. Text statistics are candidate-local, so
//! adding an invisible document can never move a visible candidate's score.
//! Nothing here is caller-supplied code: every kind is a closed, engine-owned
//! computation.

use serde::{Deserialize, Serialize};

use super::super::numeric::QuantisedValue;
use crate::contract::BoundedVec;

/// Format identity of a feature schema body.
pub const FEATURE_SCHEMA_VERSION: u16 = 1;
/// Most features one schema may declare.
pub const MAX_FEATURES: usize = 32;

/// Which fact a feature reads. Closed: a new kind is a contract change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "feature", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FeatureKind {
    /// Share of the IRIs in the named `iri_list` parameter the candidate's
    /// classification covers by native subsumption. Exact integer arithmetic.
    CoverageFraction { param: String },
    /// Declared cost of one call, in micro-units.
    DeclaredCostMicros,
    /// Declared p95 latency, in milliseconds.
    DeclaredP95LatencyMs,
    /// How the declared cost is known: measured 3, estimated 2, declared 1,
    /// unavailable 0.
    CostQuality,
    /// Whole seconds between the revision's last update and the recorded `now`.
    AgeSeconds,
    /// A named numeric fact: a graph row's property (belief confidence,
    /// interval width, stored reliability).
    Number { key: String },
    /// Candidate-local BM25 of a named text field (`summary`, an NL template's
    /// `nl.utterances` / `nl.labels`, or a graph row's text property) against
    /// the named `text` parameter.
    TextBm25 { key: String, param: String },
}

/// What an absent fact means. Unknown is never silently zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "missing", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum MissingValue {
    /// The decision abstains with `UnknownFact` naming the candidate.
    Abstain,
    /// A declared, recorded imputation.
    Impute { value: QuantisedValue },
}

/// One named feature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FeatureSpec {
    pub name: String,
    pub kind: FeatureKind,
    pub missing: MissingValue,
}

/// The body of a published `FeatureSchema` component.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct FeatureSchemaBody {
    pub schema_version: u16,
    pub features: BoundedVec<FeatureSpec, 32>,
}

impl FeatureSchemaBody {
    /// The validating constructor: a known version, at least one feature, and
    /// unique non-empty names.
    pub fn checked(self) -> Result<Self, String> {
        if self.schema_version != FEATURE_SCHEMA_VERSION {
            return Err(format!(
                "feature schema version {} is not served",
                self.schema_version
            ));
        }
        if self.features.is_empty() {
            return Err("a feature schema declares at least one feature".to_string());
        }
        let mut names: Vec<&str> = self.features.iter().map(|f| f.name.as_str()).collect();
        names.sort_unstable();
        if names.iter().any(|name| name.is_empty()) || names.windows(2).any(|w| w[0] == w[1]) {
            return Err("feature names must be non-empty and unique".to_string());
        }
        Ok(self)
    }

    /// Feature names in declaration order: the column order of every matrix.
    pub fn names(&self) -> Vec<String> {
        self.features.iter().map(|f| f.name.clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decision::numeric::QuantScaleTag;
    use crate::decision::statistical::body::encode_body;

    /// Cross-language golden vector: `tests/test_decision_stat_client.py`
    /// builds the same body with `epistemic_graph.decision_stat` and asserts
    /// the same digest, so the Python codec and serde agree byte for byte.
    #[test]
    fn the_feature_schema_body_digest_matches_the_python_client() {
        let spec = |name: &str, kind, missing| FeatureSpec {
            name: name.to_string(),
            kind,
            missing,
        };
        let body = FeatureSchemaBody {
            schema_version: FEATURE_SCHEMA_VERSION,
            features: BoundedVec::new(vec![
                spec(
                    "text",
                    FeatureKind::TextBm25 {
                        key: "summary".to_string(),
                        param: "query".to_string(),
                    },
                    MissingValue::Abstain,
                ),
                spec(
                    "coverage",
                    FeatureKind::CoverageFraction {
                        param: "needs".to_string(),
                    },
                    MissingValue::Abstain,
                ),
                spec(
                    "cost",
                    FeatureKind::DeclaredCostMicros,
                    MissingValue::Impute {
                        value: QuantisedValue {
                            scale: QuantScaleTag::Q32,
                            value: 7 << 32,
                        },
                    },
                ),
            ])
            .expect("three features"),
        }
        .checked()
        .expect("valid");
        assert_eq!(
            encode_body(&body).expect("encodes").content_digest,
            "sha256:37a7e0b16650a73226cf326a4c9697ebdf03f027dad9585e852418d7da76ed98"
        );
    }
}
