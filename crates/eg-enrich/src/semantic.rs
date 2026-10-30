//! Semantic enrichment records: capability cards, enhancement candidates,
//! spec drafts, topic assignments and projection rules
//! (EG-DECISION-ENGINE-R614.1).
//!
//! The model layer of the enrichment family: typed, identifier-validated
//! records the behaviour (R614.2), the expected-value scheduling (R614.3),
//! the `CARD_UNGROUNDED` refusal (R614.4) and the per-card evidence (R614.5)
//! build on. Every identifier is an [`Id`], validated at construction and
//! on the wire; nothing here calls a model or schedules work.

use serde::{Deserialize, Serialize};

use crate::{EnrichError, Id};

/// The stable code of a refused classification confidence.
pub const INVALID_CONFIDENCE: &str = "INVALID_CONFIDENCE";

/// The largest confidence any topic assignment accepts.
pub const MAX_CONFIDENCE_BASIS_POINTS: u32 = 10_000;

/// A classification confidence in basis points of 1.0 (1 = 0.01%).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct Confidence(u32);

impl Confidence {
    pub fn new(basis_points: u32) -> Result<Self, EnrichError> {
        if basis_points > MAX_CONFIDENCE_BASIS_POINTS {
            return Err(EnrichError::new(
                INVALID_CONFIDENCE,
                format!(
                    "confidence {basis_points} exceeds {MAX_CONFIDENCE_BASIS_POINTS} basis points"
                ),
            ));
        }
        Ok(Self(basis_points))
    }

    /// The confidence as basis points of 1.0.
    pub fn basis_points(self) -> u32 {
        self.0
    }
}

impl TryFrom<u32> for Confidence {
    type Error = EnrichError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<Confidence> for u32 {
    fn from(confidence: Confidence) -> Self {
        confidence.0
    }
}

/// A model-produced summary of one symbol, file or feature, stored with the
/// model and the prompt that produced it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityCard {
    pub id: Id,
    /// The symbol, file or feature the card summarizes.
    pub subject: Id,
    /// The summary text; it cites the source fact ids below.
    pub summary: String,
    /// The model that produced the summary.
    pub model: Id,
    /// The digest of the prompt that produced the summary.
    pub prompt_digest: String,
    /// The stored fact ids the summary cites.
    pub source_fact_ids: Vec<Id>,
}

/// A research concept ranked into an enhancement proposal by its value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnhancementCandidate {
    pub id: Id,
    /// The research concept the candidate is.
    pub concept: Id,
    /// The value the ranking orders candidates by.
    pub value: u32,
}

/// A spec distilled from ranked research concepts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpecDraft {
    pub id: Id,
    pub title: String,
    pub body: String,
    /// The concepts the draft distills.
    pub source_concepts: Vec<Id>,
}

/// One document classified into the fixed subject taxonomy, linked to its
/// topic node with a confidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicAssignment {
    pub id: Id,
    /// The document the assignment classifies.
    pub document: Id,
    /// The topic node the document classifies into.
    pub topic: Id,
    /// How certain the classification is, in basis points.
    pub confidence: Confidence,
}

/// A declared rule projecting a node's own properties into the edges they
/// encode, under a declared taxonomy and reference rule.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionRule {
    pub id: Id,
    /// The taxonomy under which the projection is declared.
    pub taxonomy: Id,
    /// The node property the rule reads.
    pub property: Id,
    /// The edge relation the property encodes.
    pub relation: Id,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> Id {
        Id::new(value).unwrap()
    }

    fn card() -> CapabilityCard {
        CapabilityCard {
            id: id("card:summarize::main"),
            subject: id("src/lib.rs::main"),
            summary: "main wires the parser to the store".into(),
            model: id("eg:head/text-7b"),
            prompt_digest: "ab".repeat(32),
            source_fact_ids: vec![id("fact:1"), id("fact:2")],
        }
    }

    // spec: EG-DECISION-ENGINE-R614.1
    #[test]
    fn semantic_types_round_trip() {
        let card = card();
        let candidate = EnhancementCandidate {
            id: id("cand:1"),
            concept: id("concept:store-indexing"),
            value: 42,
        };
        let draft = SpecDraft {
            id: id("draft:1"),
            title: "Store indexing".into(),
            body: "Index the store by fact id.".into(),
            source_concepts: vec![id("concept:store-indexing")],
        };
        let assignment = TopicAssignment {
            id: id("assign:1"),
            document: id("doc:1"),
            topic: id("topic:storage"),
            confidence: Confidence::new(8_500).unwrap(),
        };
        let rule = ProjectionRule {
            id: id("rule:1"),
            taxonomy: id("taxonomy:code"),
            property: id("path"),
            relation: id("eg:edge/contains"),
        };
        macro_rules! round_trip {
            ($value:expr, $ty:ty) => {{
                let json = serde_json::to_string(&$value).unwrap();
                assert_eq!(serde_json::from_str::<$ty>(&json).unwrap(), $value);
            }};
        }
        round_trip!(card, CapabilityCard);
        round_trip!(candidate, EnhancementCandidate);
        round_trip!(draft, SpecDraft);
        round_trip!(assignment, TopicAssignment);
        round_trip!(rule, ProjectionRule);
        assert_eq!(
            serde_json::to_string(&assignment).unwrap(),
            r#"{"id":"assign:1","document":"doc:1","topic":"topic:storage","confidence":8500}"#
        );
    }

    // spec: EG-DECISION-ENGINE-R614.1
    #[test]
    fn semantic_types_reject_invalid_identifiers() {
        for bad in ["", "has space", &"x".repeat(crate::MAX_ID_BYTES + 1)] {
            assert_eq!(Id::new(bad).unwrap_err().code, crate::INVALID_IDENTIFIER);
        }
        let bad_card = serde_json::to_value(card()).unwrap();
        let mut card = bad_card;
        card["id"] = serde_json::json!("card 1");
        assert!(serde_json::from_value::<CapabilityCard>(card).is_err());
        assert!(serde_json::from_str::<EnhancementCandidate>(
            r#"{"id":"a","concept":"b","value":1,"extra":2}"#
        )
        .is_err());
        assert!(serde_json::from_str::<TopicAssignment>(
            r#"{"id":"a","document":"b","topic":"c","confidence":10001}"#
        )
        .is_err());
        assert_eq!(
            Confidence::new(10_001).unwrap_err().code,
            INVALID_CONFIDENCE
        );
        assert_eq!(Confidence::new(10_000).unwrap().basis_points(), 10_000);
    }
}
