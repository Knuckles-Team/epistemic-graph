//! Typed expected-value scheduling for enrichment work
//! (`EG-DECISION-ENGINE-R031`): a scheduling round selects candidate work by
//! comparing expected value against cost through the same typed decision
//! mechanism as any other declared-candidate question
//! (`QuestionKind::EnrichmentSchedule`, [`super::declared`]), never a fixed
//! priority order.
//!
//! This module fixes the SHAPE of one candidate: its expected value and
//! cost, both on the Q32 fixed-point scale the rest of the statistical
//! surface uses. The decision itself -- choosing among candidates for one
//! scheduling round -- runs through the ordinary `Decide` executor over
//! `CandidateSource::Declared` options built from
//! [`EnrichmentScheduleRequest::as_declared_options`]; it adds no
//! scheduling-specific comparison path.

use serde::{Deserialize, Serialize};

use super::declared::{DeclaredNumber, DeclaredOption};
use crate::contract::BoundedVec;

/// The declared-option fact key an enrichment candidate's expected value is
/// published under.
pub const EXPECTED_VALUE_KEY: &str = "enrichment.expected_value_q32";
/// The declared-option fact key an enrichment candidate's cost is published
/// under.
pub const COST_KEY: &str = "enrichment.cost_q32";

/// One candidate unit of enrichment work a scheduling round may choose, on
/// the Q32 fixed-point scale (value `n` means `n / 2^32`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EnrichmentCandidate {
    pub work_id: String,
    pub expected_value_q32: i64,
    pub cost_q32: i64,
}

impl EnrichmentCandidate {
    /// `expected_value - cost`: positive net value is what a scheduling
    /// decision should prefer, never priority order alone. Informational
    /// only -- the `Decide` executor scores the declared facts, not this.
    pub fn net_value_q32(&self) -> i64 {
        self.expected_value_q32.saturating_sub(self.cost_q32)
    }

    /// This candidate's facts as a declared option -- the only shape a
    /// scheduling decision reads it through.
    fn as_declared_option(&self) -> DeclaredOption {
        DeclaredOption {
            option_id: self.work_id.clone(),
            classification: BoundedVec::default(),
            numbers: BoundedVec::new(vec![
                DeclaredNumber {
                    key: EXPECTED_VALUE_KEY.to_string(),
                    q32: self.expected_value_q32,
                },
                DeclaredNumber {
                    key: COST_KEY.to_string(),
                    q32: self.cost_q32,
                },
            ])
            .expect("two facts is inside the declared-number bound"),
            texts: BoundedVec::default(),
        }
    }
}

/// One scheduling round's candidate work, checked before any decision runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EnrichmentScheduleRequest {
    pub candidates: Vec<EnrichmentCandidate>,
}

impl EnrichmentScheduleRequest {
    /// At least one candidate, unique non-empty work ids, and non-negative
    /// cost. A negative expected value is legal (a candidate that is a net
    /// loss); a negative COST is not -- it has no meaning on this scale and
    /// would let a candidate manufacture unbounded net value.
    pub fn check(&self) -> Result<(), String> {
        if self.candidates.is_empty() {
            return Err("an enrichment schedule request names at least one candidate".to_string());
        }
        let mut seen = std::collections::BTreeSet::new();
        for candidate in &self.candidates {
            if candidate.work_id.is_empty() || !seen.insert(candidate.work_id.as_str()) {
                return Err(
                    "enrichment candidate work ids must be non-empty and unique".to_string()
                );
            }
            if candidate.cost_q32 < 0 {
                return Err(format!(
                    "UNSUPPORTED_ENRICHMENT_COST: work '{}' names a negative cost",
                    candidate.work_id
                ));
            }
        }
        Ok(())
    }

    /// The candidates as declared options, in the request's order -- the
    /// `CandidateSource::Declared` set a `QuestionKind::EnrichmentSchedule`
    /// decision reads.
    pub fn as_declared_options(&self) -> Vec<DeclaredOption> {
        self.candidates
            .iter()
            .map(EnrichmentCandidate::as_declared_option)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: &str, value: i64, cost: i64) -> EnrichmentCandidate {
        EnrichmentCandidate {
            work_id: id.to_string(),
            expected_value_q32: value,
            cost_q32: cost,
        }
    }

    #[test]
    fn net_value_is_expected_value_minus_cost() {
        assert_eq!(candidate("a", 10, 3).net_value_q32(), 7);
        assert_eq!(candidate("a", 1, 3).net_value_q32(), -2);
    }

    #[test]
    fn a_request_with_unique_ids_and_non_negative_cost_is_accepted() {
        let request = EnrichmentScheduleRequest {
            candidates: vec![candidate("a", 10, 3), candidate("b", 2, 0)],
        };
        assert!(request.check().is_ok());
        assert_eq!(request.as_declared_options().len(), 2);
    }

    #[test]
    fn a_negative_cost_is_refused() {
        let request = EnrichmentScheduleRequest {
            candidates: vec![candidate("a", 10, -1)],
        };
        let error = request.check().expect_err("must be refused");
        assert!(
            error.contains("UNSUPPORTED_ENRICHMENT_COST"),
            "got: {error}"
        );
    }

    #[test]
    fn duplicate_work_ids_are_refused() {
        let request = EnrichmentScheduleRequest {
            candidates: vec![candidate("a", 1, 0), candidate("a", 2, 0)],
        };
        assert!(request.check().is_err());
    }

    #[test]
    fn an_empty_request_is_refused() {
        assert!(EnrichmentScheduleRequest { candidates: vec![] }
            .check()
            .is_err());
    }
}
