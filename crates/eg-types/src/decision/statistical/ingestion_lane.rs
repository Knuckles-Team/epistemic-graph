//! Typed ingestion-lane routing (`EG-DECISION-ENGINE-R030`): an ingestion
//! request is routed to a fast, medium or slow processing path through the
//! same typed decision mechanism as any other declared-candidate question
//! (`QuestionKind::IngestionLane`, [`super::declared`]), never a hard-coded
//! branch.
//!
//! This module fixes WHICH lanes exist. The decision itself -- choosing
//! among them for one request -- runs through the ordinary `Decide`
//! executor over `CandidateSource::Declared` options built from
//! [`IngestionLane::ALL`]; it adds no lane-specific scoring path. A caller
//! assembling that candidate set names its lanes by [`IngestionLane`]
//! option id, and [`IngestionLaneRequest::check`] refuses an unsupported
//! lane before any decision runs, rather than discovering it later as an
//! opaque abstention.

use serde::{Deserialize, Serialize};

/// The fixed set of ingestion processing paths a routing decision may choose
/// among. Adding a lane is a registry change here, not a new code path at
/// each caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum IngestionLane {
    Fast,
    Medium,
    Slow,
}

impl IngestionLane {
    /// Every registered lane, in a stable order.
    pub const ALL: [IngestionLane; 3] = [Self::Fast, Self::Medium, Self::Slow];

    /// The declared-option id a decision candidate for this lane carries.
    pub fn option_id(self) -> &'static str {
        match self {
            Self::Fast => "ingestion-lane:fast",
            Self::Medium => "ingestion-lane:medium",
            Self::Slow => "ingestion-lane:slow",
        }
    }

    /// The lane a declared-option id names, or `None` outside the registry.
    pub fn from_option_id(option_id: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|lane| lane.option_id() == option_id)
    }
}

/// One ingestion request's candidate lanes, as the option ids the caller
/// intends to offer a routing decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IngestionLaneRequest {
    pub candidate_lanes: Vec<String>,
}

impl IngestionLaneRequest {
    /// At least one candidate, and every candidate option id names a lane in
    /// [`IngestionLane::ALL`].
    pub fn check(&self) -> Result<(), String> {
        if self.candidate_lanes.is_empty() {
            return Err("an ingestion-lane request names at least one candidate lane".to_string());
        }
        for option_id in &self.candidate_lanes {
            if IngestionLane::from_option_id(option_id).is_none() {
                return Err(format!(
                    "UNSUPPORTED_INGESTION_LANE: '{option_id}' is not in the ingestion lane \
                     registry (fast, medium, slow)"
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-DECISION-ENGINE-R030
    #[test]
    fn every_lane_option_id_resolves_back_to_its_lane() {
        for lane in IngestionLane::ALL {
            assert_eq!(IngestionLane::from_option_id(lane.option_id()), Some(lane));
        }
    }

    // spec: EG-DECISION-ENGINE-R030
    #[test]
    fn a_request_naming_only_registered_lanes_is_accepted() {
        let request = IngestionLaneRequest {
            candidate_lanes: vec![
                IngestionLane::Fast.option_id().to_string(),
                IngestionLane::Slow.option_id().to_string(),
            ],
        };
        assert!(request.check().is_ok());
    }

    // spec: EG-DECISION-ENGINE-R030
    #[test]
    fn a_request_naming_an_unregistered_lane_is_refused() {
        let request = IngestionLaneRequest {
            candidate_lanes: vec!["ingestion-lane:glacial".to_string()],
        };
        let error = request.check().expect_err("must be refused");
        assert!(error.contains("UNSUPPORTED_INGESTION_LANE"), "got: {error}");
    }

    #[test]
    fn an_empty_request_is_refused() {
        assert!(IngestionLaneRequest {
            candidate_lanes: vec![]
        }
        .check()
        .is_err());
    }
}
