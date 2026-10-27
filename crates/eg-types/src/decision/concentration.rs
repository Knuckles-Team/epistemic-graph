//! Typed records for edit concentration policy and verdicts
//! (EG-DECISION-ENGINE-R173.1).

use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;

use crate::contract::OpaqueId;

/// Policy governing how many shipped edits may target one dimension in a
/// rolling window. A zero-length window has no meaningful interpretation and
/// is rejected during deserialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConcentrationPolicy {
    pub policy_iri: String,
    pub min_shipped: u32,
    pub window_rounds: NonZeroU32,
    pub revision: u64,
}

/// One edit fact considered by a concentration check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EditFact {
    pub edit_id: OpaqueId,
    pub dimension_iri: String,
    pub round: u64,
    pub status: String,
}

/// Closed action returned for one dimension's concentration window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ConcentrationAction {
    Keep,
    Revert,
}

/// Count and bounds supporting the action for a dimension.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ConcentrationVerdict {
    pub dimension_iri: String,
    pub count: u64,
    pub first_round: u64,
    pub last_round: u64,
    pub action: ConcentrationAction,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_round_trips_and_rejects_empty_window() {
        let policy = ConcentrationPolicy {
            policy_iri: "urn:policy:concentration".to_string(),
            min_shipped: 3,
            window_rounds: NonZeroU32::new(3).unwrap(),
            revision: 7,
        };
        let fact = EditFact {
            edit_id: OpaqueId::new("edit:42").unwrap(),
            dimension_iri: "urn:dimension:retrieval".to_string(),
            round: 12,
            status: "shipped".to_string(),
        };
        let verdict = ConcentrationVerdict {
            dimension_iri: fact.dimension_iri.clone(),
            count: 3,
            first_round: 10,
            last_round: 12,
            action: ConcentrationAction::Revert,
        };

        let policy_bytes = rmp_serde::to_vec_named(&policy).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<ConcentrationPolicy>(&policy_bytes).unwrap(),
            policy
        );
        let fact_bytes = rmp_serde::to_vec_named(&fact).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<EditFact>(&fact_bytes).unwrap(),
            fact
        );
        let verdict_bytes = rmp_serde::to_vec_named(&verdict).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<ConcentrationVerdict>(&verdict_bytes).unwrap(),
            verdict
        );

        let invalid = r#"{"policy_iri":"urn:policy:concentration","min_shipped":3,"window_rounds":0,"revision":7}"#;
        assert!(serde_json::from_str::<ConcentrationPolicy>(invalid).is_err());
    }
}
