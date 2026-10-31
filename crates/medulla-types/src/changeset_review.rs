//! Typed semantic changeset-review data (MEDULLA-DURABLE-KERNEL-R087.1).
//!
//! This module deliberately contains only the model: a graph mutation and
//! reviewer scope, a severity/class filter, and findings bound to exact code
//! evidence. Authentication, the probe producer, and the served
//! changeset-review route belong to R087.2 and remain above `medulla-types`.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// The five flaw classes the changeset review policy distinguishes.
#[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FlawClass {
    Injection,
    AuthBypass,
    DataExposure,
    Cryptography,
    BusinessLogic,
}

/// Ordered impact used by a review filter and each finding.
#[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ImpactSeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// A review policy's impact threshold and optional class allowlist.
///
/// An empty class set means every class is eligible; a non-empty set narrows
/// the review to the named classes. Severity is always bounded by the
/// threshold.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ImpactFilterPolicy {
    pub minimum_severity: ImpactSeverity,
    #[serde(default)]
    pub flaw_classes: BTreeSet<FlawClass>,
}

impl ImpactFilterPolicy {
    /// Whether a finding is in this policy's review set.
    pub fn allows(&self, finding: &ReviewFinding) -> bool {
        finding.severity >= self.minimum_severity
            && (self.flaw_classes.is_empty() || self.flaw_classes.contains(&finding.class))
    }
}

/// The graph mutation under review. The digest identifies the exact candidate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GraphMutation {
    pub graph: String,
    pub mutation_id: String,
    pub candidate_digest: String,
}

/// The reviewer's declared code scope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ReviewerScope {
    pub scope_id: String,
    #[serde(default)]
    pub paths: Vec<String>,
}

/// A request names the graph mutation, reviewer scope, and policy to apply.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ChangesetReviewRequest {
    pub mutation: GraphMutation,
    pub reviewer_scope: ReviewerScope,
    pub policy: ImpactFilterPolicy,
}

/// An exact source location supporting a finding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CodeLocation {
    pub path: String,
    pub start_line: u32,
    pub end_line: u32,
}

/// A source reference plus the exact location where it was observed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CodeEvidence {
    pub evidence_ref: String,
    pub location: CodeLocation,
}

/// One review finding bound to a class, impact, and code evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ReviewFinding {
    pub class: FlawClass,
    pub severity: ImpactSeverity,
    pub evidence: CodeEvidence,
}

/// Model-level refusal before a future served producer can consume a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangesetReviewError {
    EmptyGraph,
    EmptyMutationId,
    EmptyCandidateDigest,
    EmptyReviewerScope,
    EmptyReviewerPath,
    EmptyEvidenceReference,
    EmptyEvidencePath,
    InvalidEvidenceLines,
}

impl std::fmt::Display for ChangesetReviewError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Self::EmptyGraph => "graph must not be empty",
            Self::EmptyMutationId => "mutation id must not be empty",
            Self::EmptyCandidateDigest => "candidate digest must not be empty",
            Self::EmptyReviewerScope => "reviewer scope must not be empty",
            Self::EmptyReviewerPath => "reviewer path must not be empty",
            Self::EmptyEvidenceReference => "evidence reference must not be empty",
            Self::EmptyEvidencePath => "evidence path must not be empty",
            Self::InvalidEvidenceLines => "evidence lines must be positive and ordered",
        };
        f.write_str(text)
    }
}

impl std::error::Error for ChangesetReviewError {}

impl GraphMutation {
    pub fn validate(&self) -> Result<(), ChangesetReviewError> {
        if self.graph.trim().is_empty() {
            return Err(ChangesetReviewError::EmptyGraph);
        }
        if self.mutation_id.trim().is_empty() {
            return Err(ChangesetReviewError::EmptyMutationId);
        }
        if self.candidate_digest.trim().is_empty() {
            return Err(ChangesetReviewError::EmptyCandidateDigest);
        }
        Ok(())
    }
}

impl ReviewerScope {
    pub fn validate(&self) -> Result<(), ChangesetReviewError> {
        if self.scope_id.trim().is_empty() {
            return Err(ChangesetReviewError::EmptyReviewerScope);
        }
        if self.paths.iter().any(|path| path.trim().is_empty()) {
            return Err(ChangesetReviewError::EmptyReviewerPath);
        }
        Ok(())
    }
}

impl CodeEvidence {
    pub fn validate(&self) -> Result<(), ChangesetReviewError> {
        if self.evidence_ref.trim().is_empty() {
            return Err(ChangesetReviewError::EmptyEvidenceReference);
        }
        if self.location.path.trim().is_empty() {
            return Err(ChangesetReviewError::EmptyEvidencePath);
        }
        if self.location.start_line == 0 || self.location.end_line < self.location.start_line {
            return Err(ChangesetReviewError::InvalidEvidenceLines);
        }
        Ok(())
    }
}

impl ReviewFinding {
    pub fn validate(&self) -> Result<(), ChangesetReviewError> {
        self.evidence.validate()
    }
}

impl ChangesetReviewRequest {
    pub fn validate(&self) -> Result<(), ChangesetReviewError> {
        self.mutation.validate()?;
        self.reviewer_scope.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::DeserializeOwned;

    fn round_trip<T>(value: &T) -> T
    where
        T: DeserializeOwned + Serialize,
    {
        let bytes = serde_json::to_vec(value).expect("encode changeset review value");
        serde_json::from_slice(&bytes).expect("decode changeset review value")
    }

    fn evidence() -> CodeEvidence {
        CodeEvidence {
            evidence_ref: "probe:run-42:finding-1".into(),
            location: CodeLocation {
                path: "src/policy.rs".into(),
                start_line: 18,
                end_line: 24,
            },
        }
    }

    fn mutation() -> GraphMutation {
        GraphMutation {
            graph: "tenant-a".into(),
            mutation_id: "change-42".into(),
            candidate_digest: "sha256:abc123".into(),
        }
    }

    // spec: MEDULLA-DURABLE-KERNEL-R087.1
    #[test]
    fn every_flaw_class_and_evidence_round_trips() {
        let classes = [
            FlawClass::Injection,
            FlawClass::AuthBypass,
            FlawClass::DataExposure,
            FlawClass::Cryptography,
            FlawClass::BusinessLogic,
        ];
        for class in classes {
            let finding = ReviewFinding {
                class,
                severity: ImpactSeverity::High,
                evidence: evidence(),
            };
            assert_eq!(round_trip(&finding), finding);
            assert_eq!(finding.validate(), Ok(()));
        }
    }

    // spec: MEDULLA-DURABLE-KERNEL-R087.1
    #[test]
    fn request_round_trips_mutation_reviewer_scope_and_policy() {
        let request = ChangesetReviewRequest {
            mutation: mutation(),
            reviewer_scope: ReviewerScope {
                scope_id: "security-review".into(),
                paths: vec!["src/policy.rs".into(), "src/auth.rs".into()],
            },
            policy: ImpactFilterPolicy {
                minimum_severity: ImpactSeverity::Medium,
                flaw_classes: BTreeSet::from([FlawClass::AuthBypass, FlawClass::DataExposure]),
            },
        };
        assert_eq!(round_trip(&request), request);
        assert_eq!(request.validate(), Ok(()));
    }

    // spec: MEDULLA-DURABLE-KERNEL-R087.1
    #[test]
    fn impact_and_class_filters_are_applied_without_losing_evidence() {
        let policy = ImpactFilterPolicy {
            minimum_severity: ImpactSeverity::High,
            flaw_classes: BTreeSet::from([FlawClass::AuthBypass]),
        };
        let accepted = ReviewFinding {
            class: FlawClass::AuthBypass,
            severity: ImpactSeverity::Critical,
            evidence: evidence(),
        };
        let low = ReviewFinding {
            severity: ImpactSeverity::Medium,
            ..accepted.clone()
        };
        let wrong_class = ReviewFinding {
            class: FlawClass::Injection,
            ..accepted.clone()
        };
        assert!(policy.allows(&accepted));
        assert!(!policy.allows(&low));
        assert!(!policy.allows(&wrong_class));
        assert_eq!(accepted.evidence, evidence());
    }

    // spec: MEDULLA-DURABLE-KERNEL-R087.1
    #[test]
    fn malformed_mutation_scope_and_evidence_refuse() {
        let mut bad_mutation = mutation();
        bad_mutation.graph.clear();
        assert_eq!(
            bad_mutation.validate(),
            Err(ChangesetReviewError::EmptyGraph)
        );

        let mut bad_scope = ReviewerScope {
            scope_id: "scope".into(),
            paths: vec!["src/policy.rs".into()],
        };
        bad_scope.paths.push(" ".into());
        assert_eq!(
            bad_scope.validate(),
            Err(ChangesetReviewError::EmptyReviewerPath)
        );

        let mut bad_evidence = evidence();
        bad_evidence.location.start_line = 0;
        assert_eq!(
            bad_evidence.validate(),
            Err(ChangesetReviewError::InvalidEvidenceLines)
        );
        bad_evidence.location.start_line = 24;
        bad_evidence.location.end_line = 18;
        assert_eq!(
            bad_evidence.validate(),
            Err(ChangesetReviewError::InvalidEvidenceLines)
        );
    }
}
