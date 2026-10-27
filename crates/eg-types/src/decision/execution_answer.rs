//! The seven-part composed answer envelope for the orchestration competency.
//!
//! Each question keeps its own answer or typed abstention and evidence class.
//! Assembly wiring fills the envelope; this module only defines its stable
//! serde and digestable shape (EG-CQ-ORCH-R008.1).

use serde::{Deserialize, Serialize};

use super::digest::digest_text;
use super::record::{AbstainReason, EvidenceClass, PremiseRef};
use crate::contract::BoundedVec;

/// Format identity of the composed orchestration answer envelope.
pub const EXECUTION_ANSWER_SCHEMA_VERSION: u16 = 1;
/// Domain separator for the composed answer's stable digest.
pub const EXECUTION_ANSWER_DIGEST_DOMAIN: &str = "eg/orchestration-execution-answer/v1";

/// One part of the composed answer, preserving either a result or a typed
/// abstention together with the evidence that supports that part.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ExecutionAnswerPart {
    Answer {
        value: serde_json::Value,
        provenance: BoundedVec<PremiseRef, 64>,
        evidence_class: EvidenceClass,
    },
    Abstain {
        reason: AbstainReason,
        provenance: BoundedVec<PremiseRef, 64>,
        evidence_class: EvidenceClass,
    },
}

/// The seven separately answerable competency questions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ExecutionAnswerParts {
    pub steps: ExecutionAnswerPart,
    pub staffing: ExecutionAnswerPart,
    pub coverage: ExecutionAnswerPart,
    pub guardrails: ExecutionAnswerPart,
    pub bindings: ExecutionAnswerPart,
    pub workflow: ExecutionAnswerPart,
    pub dag: ExecutionAnswerPart,
}

/// The composed answer to "How can I execute task XYZ?".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ExecutionAnswer {
    pub schema_version: u16,
    pub task_iri: String,
    pub parts: ExecutionAnswerParts,
    pub assembled_by: String,
    pub certificate_digest: String,
}

impl ExecutionAnswer {
    /// Return the canonical digest used to pin this envelope across the wire.
    pub fn digest(&self) -> String {
        digest_text(EXECUTION_ANSWER_DIGEST_DOMAIN, self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answer(value: &str) -> ExecutionAnswerPart {
        ExecutionAnswerPart::Answer {
            value: serde_json::Value::String(value.to_owned()),
            provenance: BoundedVec::default(),
            evidence_class: EvidenceClass::Proof,
        }
    }

    fn abstain() -> ExecutionAnswerPart {
        ExecutionAnswerPart::Abstain {
            reason: AbstainReason::InsufficientConfidence,
            provenance: BoundedVec::default(),
            evidence_class: EvidenceClass::Claim,
        }
    }

    fn fixture() -> ExecutionAnswer {
        ExecutionAnswer {
            schema_version: EXECUTION_ANSWER_SCHEMA_VERSION,
            task_iri: "urn:task:demo".to_owned(),
            parts: ExecutionAnswerParts {
                steps: answer("steps"),
                staffing: answer("staffing"),
                coverage: abstain(),
                guardrails: answer("guardrails"),
                bindings: answer("bindings"),
                workflow: answer("workflow"),
                dag: answer("dag"),
            },
            assembled_by: "graph.assemble".to_owned(),
            certificate_digest: "sha256:certificate".to_owned(),
        }
    }

    #[test]
    fn execution_answer_round_trips_with_answer_and_abstention_parts() {
        let original = fixture();
        let encoded = serde_json::to_string(&original).unwrap();
        let decoded: ExecutionAnswer = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, original);
        assert!(encoded.contains("\"status\":\"answer\""));
        assert!(encoded.contains("\"status\":\"abstain\""));
    }

    #[test]
    fn execution_answer_digest_matches_golden_vector() {
        assert_eq!(
            fixture().digest(),
            "sha256:6a2533afa97694c00af1f8b7706bcc940cea77e0bf1d841acc07b17a5007d026"
        );
    }
}
