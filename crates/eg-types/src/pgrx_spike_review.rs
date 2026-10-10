//! EG-UNIFIED-DATA-PLANE-R025.3.1 — adapter skeleton for reviewing the pgrx
//! spike's recorded decision into an approved architecture-decision record:
//! a typed review request, its mapping to the approved ADR record, and the
//! refusal path before any further pgrx work is authorized. Running the
//! live spike itself is EG-UNIFIED-DATA-PLANE-R025.2+; recording full-scope
//! evidence is R025.1. Filing the ADR document is EG-UNIFIED-DATA-PLANE-R025.3.2+.

use crate::pgrx_spike::{InvalidPgrxDecision, PgrxSpikeDecision, PgrxSpikeOutcome};
use serde::{Deserialize, Serialize};

/// A request to review one pgrx spike decision into an approved ADR.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgrxSpikeReviewRequest {
    pub decision: PgrxSpikeDecision,
    pub reviewer: String,
    pub adr_title: String,
}

/// The approved architecture-decision record this review produces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovedPgrxAdr {
    pub outcome: PgrxSpikeOutcome,
    pub reviewer: String,
    pub adr_title: String,
}

/// Why a [`PgrxSpikeReviewRequest`] was refused before any ADR was approved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgrxSpikeReviewRefusal {
    InvalidDecision(InvalidPgrxDecision),
    EmptyReviewer,
    EmptyAdrTitle,
}

impl std::fmt::Display for PgrxSpikeReviewRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidDecision(err) => write!(f, "cannot review an invalid decision: {err}"),
            Self::EmptyReviewer => write!(f, "review names no reviewer"),
            Self::EmptyAdrTitle => write!(f, "review names no ADR title"),
        }
    }
}

impl std::error::Error for PgrxSpikeReviewRefusal {}

impl PgrxSpikeReviewRequest {
    /// Validates the underlying decision and this review's own fields and,
    /// if well-formed, produces the approved ADR record. Performs NO filing
    /// of the ADR document itself: a refusal here never authorizes further
    /// pgrx work.
    pub fn approve(&self) -> Result<ApprovedPgrxAdr, PgrxSpikeReviewRefusal> {
        let outcome = self
            .decision
            .validate()
            .map_err(PgrxSpikeReviewRefusal::InvalidDecision)?;
        if self.reviewer.trim().is_empty() {
            return Err(PgrxSpikeReviewRefusal::EmptyReviewer);
        }
        if self.adr_title.trim().is_empty() {
            return Err(PgrxSpikeReviewRefusal::EmptyAdrTitle);
        }
        Ok(ApprovedPgrxAdr {
            outcome,
            reviewer: self.reviewer.clone(),
            adr_title: self.adr_title.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgrx_spike::{PgrxSpikeArea, PgrxSpikeEvidence};

    fn full_evidence() -> Vec<PgrxSpikeEvidence> {
        PgrxSpikeArea::ALL
            .into_iter()
            .map(|area| PgrxSpikeEvidence {
                area,
                finding: "evaluated".to_string(),
            })
            .collect()
    }

    fn valid_request() -> PgrxSpikeReviewRequest {
        PgrxSpikeReviewRequest {
            decision: PgrxSpikeDecision {
                evidence: full_evidence(),
                outcome: Some(PgrxSpikeOutcome::NoGo),
            },
            reviewer: "arch-review-board".to_string(),
            adr_title: "pgrx companion extension go/no-go".to_string(),
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R025.3.1
    #[test]
    fn well_formed_review_approves_the_adr() {
        let request = valid_request();
        let adr = request.approve().expect("well-formed review approves");
        assert_eq!(adr.outcome, PgrxSpikeOutcome::NoGo);
        assert_eq!(adr.reviewer, "arch-review-board");
    }

    // spec: EG-UNIFIED-DATA-PLANE-R025.3.1
    #[test]
    fn refuses_an_invalid_decision() {
        let mut request = valid_request();
        request.decision.outcome = None;
        assert_eq!(
            request.approve(),
            Err(PgrxSpikeReviewRefusal::InvalidDecision(
                InvalidPgrxDecision::NoOutcome
            ))
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R025.3.1
    #[test]
    fn refuses_an_empty_reviewer() {
        let mut request = valid_request();
        request.reviewer = "   ".to_string();
        assert_eq!(
            request.approve(),
            Err(PgrxSpikeReviewRefusal::EmptyReviewer)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R025.3.1
    #[test]
    fn refuses_an_empty_adr_title() {
        let mut request = valid_request();
        request.adr_title = "".to_string();
        assert_eq!(
            request.approve(),
            Err(PgrxSpikeReviewRefusal::EmptyAdrTitle)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R025.3.1
    #[test]
    fn approved_adr_serializes_round_trip() {
        let adr = valid_request().approve().unwrap();
        let encoded = serde_json::to_string(&adr).unwrap();
        let decoded: ApprovedPgrxAdr = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, adr);
    }
}
