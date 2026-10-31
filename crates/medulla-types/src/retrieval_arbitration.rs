//! Typed arbitration values for the retrieval boundary.
//!
//! MEDULLA-FEDERATED-QUERY-R083.1 defines the value shared by a future planner
//! arbitration step and its caller: both the model prior and the retrieved
//! answer remain present in every verdict.  This module deliberately owns only
//! the serde data model.  Support classification, confidence comparison and
//! planner wiring belong to later R083 children.

use serde::{Deserialize, Serialize};

/// The provenance class of a candidate answer.
///
/// This is closed so a caller cannot silently introduce a third provenance
/// class that the R083 arbitration policy does not understand.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceClass {
    /// An answer produced without retrieved evidence.
    Parametric,
    /// An answer conditioned on retrieved evidence.
    Retrieved,
}

/// One answer considered by the retrieval arbitration policy.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Candidate {
    /// The candidate answer text carried through arbitration.
    pub answer: String,
    /// The producer's confidence in this candidate, before arbitration.
    pub confidence: f64,
    /// Whether the candidate is the model prior or retrieved evidence path.
    pub evidence_class: EvidenceClass,
}

/// The closed set of outcomes from comparing the prior and retrieved answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArbitrationChoice {
    Prior,
    Retrieved,
    Abstain,
}

/// A typed arbitration result retaining both candidates for every outcome.
///
/// The `margin` is carried even when the choice is `Abstain`, so a later
/// planner or evidence record can explain the policy threshold it applied.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ArbitrationVerdict {
    pub choice: ArbitrationChoice,
    pub margin: f64,
    pub prior: Candidate,
    pub retrieved: Candidate,
}

#[cfg(test)]
mod tests {
    use super::{ArbitrationChoice, ArbitrationVerdict, Candidate, EvidenceClass};

    #[test]
    fn verdict_carries_both_candidates() {
        let verdict = ArbitrationVerdict {
            choice: ArbitrationChoice::Abstain,
            margin: 0.10,
            prior: Candidate {
                answer: "prior answer".to_owned(),
                confidence: 0.91,
                evidence_class: EvidenceClass::Parametric,
            },
            retrieved: Candidate {
                answer: "retrieved answer".to_owned(),
                confidence: 0.88,
                evidence_class: EvidenceClass::Retrieved,
            },
        };

        let encoded = serde_json::to_string(&verdict).expect("verdict must serialize");
        assert!(encoded.contains("\"parametric\""));
        assert!(encoded.contains("\"retrieved\""));

        let decoded: ArbitrationVerdict =
            serde_json::from_str(&encoded).expect("verdict must round-trip");
        assert_eq!(decoded, verdict);
        assert_eq!(decoded.prior.answer, "prior answer");
        assert_eq!(decoded.retrieved.answer, "retrieved answer");
    }
}
