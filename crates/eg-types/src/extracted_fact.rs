//! Typed fact and claim extraction records (MEDULLA-INGEST-WORKER-R701.1).
//!
//! A fact always carries an evidence span; a fact that grounds to no ontology
//! class is held as a candidate claim. Grounding, direction repair and
//! deduplication are later rows (R701.2 onward).

use serde::{Deserialize, Serialize};

/// The source span a fact or claim was read from. `end` is exclusive.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "EvidenceSpanWire")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EvidenceSpan {
    pub source_ref: String,
    pub start: u64,
    pub end: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
struct EvidenceSpanWire {
    source_ref: String,
    start: u64,
    end: u64,
}

/// Why an evidence span is refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EvidenceSpanError {
    #[error("evidence span source_ref must not be blank")]
    BlankSource,
    #[error("evidence span must be non-empty (start < end)")]
    EmptySpan,
}

impl EvidenceSpan {
    pub fn new(
        source_ref: impl Into<String>,
        start: u64,
        end: u64,
    ) -> Result<Self, EvidenceSpanError> {
        let source_ref = source_ref.into();
        if source_ref.trim().is_empty() {
            return Err(EvidenceSpanError::BlankSource);
        }
        if start >= end {
            return Err(EvidenceSpanError::EmptySpan);
        }
        Ok(Self {
            source_ref,
            start,
            end,
        })
    }
}

impl TryFrom<EvidenceSpanWire> for EvidenceSpan {
    type Error = EvidenceSpanError;

    fn try_from(wire: EvidenceSpanWire) -> Result<Self, Self::Error> {
        Self::new(wire.source_ref, wire.start, wire.end)
    }
}

/// The outcome of grounding a fact against the compiled ontology.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum GroundingResult {
    Grounded {
        subject_class: String,
        property: String,
        object_class: String,
        /// True when subject and object were swapped to satisfy domain and range.
        direction_repaired: bool,
    },
    Ungrounded,
}

/// An atomic fact read from prose. The evidence span is mandatory.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ExtractedFact {
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub evidence: EvidenceSpan,
    pub confidence: f64,
}

/// A statement held as a claim: extracted, evidenced, but not admitted as a
/// grounded fact.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CandidateClaim {
    pub fact: ExtractedFact,
    pub grounding: GroundingResult,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn golden() -> serde_json::Value {
        json!({
            "fact": {
                "subject": "Ada Lovelace",
                "predicate": "wrote",
                "object": "Note G",
                "evidence": {"source_ref": "doc:ada", "start": 10, "end": 42},
                "confidence": 0.75
            },
            "grounding": {
                "status": "grounded",
                "subject_class": "Person",
                "property": "authorOf",
                "object_class": "Work",
                "direction_repaired": false
            }
        })
    }

    // spec: MEDULLA-INGEST-WORKER-R701.1
    #[test]
    fn golden_vector_round_trips_and_invalid_shapes_are_rejected() {
        let claim: CandidateClaim = serde_json::from_value(golden()).unwrap();
        assert_eq!(
            claim.fact.evidence,
            EvidenceSpan::new("doc:ada", 10, 42).unwrap()
        );
        assert_eq!(serde_json::to_value(&claim).unwrap(), golden());

        let ungrounded = CandidateClaim {
            grounding: GroundingResult::Ungrounded,
            ..claim.clone()
        };
        let encoded = serde_json::to_value(&ungrounded).unwrap();
        assert_eq!(encoded["grounding"], json!({"status": "ungrounded"}));
        assert_eq!(
            serde_json::from_value::<CandidateClaim>(encoded).unwrap(),
            ungrounded
        );

        // A fact without an evidence span is not representable.
        let mut no_span = golden();
        no_span["fact"].as_object_mut().unwrap().remove("evidence");
        assert!(serde_json::from_value::<CandidateClaim>(no_span).is_err());
        let mut empty_span = golden();
        empty_span["fact"]["evidence"]["end"] = json!(10);
        assert!(serde_json::from_value::<CandidateClaim>(empty_span).is_err());
        let mut blank_source = golden();
        blank_source["fact"]["evidence"]["source_ref"] = json!(" ");
        assert!(serde_json::from_value::<CandidateClaim>(blank_source).is_err());
        assert_eq!(
            EvidenceSpan::new("doc", 5, 5),
            Err(EvidenceSpanError::EmptySpan)
        );
    }
}
