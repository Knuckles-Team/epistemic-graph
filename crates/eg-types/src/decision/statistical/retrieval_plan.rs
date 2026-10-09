//! Typed retrieval-plan selection (`EG-DECISION-ENGINE-R029`): the decision
//! ladder selects among retrieval plans through the same typed decision
//! mechanism as any other declared-candidate question
//! (`QuestionKind::RetrievalPlan`, [`super::declared`]), never a hard-coded
//! rule naming one plan.
//!
//! This module fixes WHICH plan families a routing decision may choose
//! among. The decision itself -- choosing one plan for one request -- runs
//! through the ordinary `Decide` executor over `CandidateSource::Declared`
//! options built from [`RetrievalPlanKind::ALL`]; it adds no plan-specific
//! comparison path. A plan kind here names a retrieval STRATEGY a request
//! may route to (`crates/eg-plan`'s LeanRAG module, a reciprocal-rank-fusion
//! plan, or a direct SPARQL query); it is distinct from `Op::FuseRrf`, which
//! is an operator inside an already-chosen plan's query DAG, not a
//! candidate this decision ranks.
//!
//! [`super::retrieval`] records what a committed retrieval-plan decision's
//! run RETURNED and CITED after the fact; this module fixes what a request
//! may choose BEFORE that decision runs.

use serde::{Deserialize, Serialize};

/// The fixed set of retrieval-plan families a routing decision may choose
/// among. Adding a plan is a registry change here, not a new code path at
/// each caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum RetrievalPlanKind {
    LeanRag,
    ReciprocalRankFusion,
    DirectSparql,
}

impl RetrievalPlanKind {
    /// Every registered plan family, in a stable order.
    pub const ALL: [RetrievalPlanKind; 3] = [
        Self::LeanRag,
        Self::ReciprocalRankFusion,
        Self::DirectSparql,
    ];

    /// The declared-option id a decision candidate for this plan carries.
    pub fn option_id(self) -> &'static str {
        match self {
            Self::LeanRag => "retrieval-plan:leanrag",
            Self::ReciprocalRankFusion => "retrieval-plan:reciprocal-rank-fusion",
            Self::DirectSparql => "retrieval-plan:direct-sparql",
        }
    }

    /// The plan family an option id names, or `None` outside the registry.
    pub fn from_option_id(option_id: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|plan| plan.option_id() == option_id)
    }
}

/// One request's candidate retrieval plans, as the option ids the caller
/// intends to offer a routing decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RetrievalPlanRequest {
    pub candidate_plans: Vec<String>,
}

impl RetrievalPlanRequest {
    /// At least one candidate, and every candidate option id names a plan in
    /// [`RetrievalPlanKind::ALL`].
    pub fn check(&self) -> Result<(), String> {
        if self.candidate_plans.is_empty() {
            return Err(
                "a retrieval-plan request names at least one candidate plan".to_string(),
            );
        }
        for option_id in &self.candidate_plans {
            if RetrievalPlanKind::from_option_id(option_id).is_none() {
                return Err(format!(
                    "UNSUPPORTED_RETRIEVAL_PLAN: '{option_id}' is not in the retrieval plan \
                     registry (leanrag, reciprocal-rank-fusion, direct-sparql)"
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_plan_option_id_resolves_back_to_its_plan() {
        for plan in RetrievalPlanKind::ALL {
            assert_eq!(RetrievalPlanKind::from_option_id(plan.option_id()), Some(plan));
        }
    }

    #[test]
    fn a_request_naming_only_registered_plans_is_accepted() {
        let request = RetrievalPlanRequest {
            candidate_plans: vec![
                RetrievalPlanKind::LeanRag.option_id().to_string(),
                RetrievalPlanKind::DirectSparql.option_id().to_string(),
            ],
        };
        assert!(request.check().is_ok());
    }

    #[test]
    fn a_request_naming_an_unregistered_plan_is_refused() {
        let request = RetrievalPlanRequest {
            candidate_plans: vec!["retrieval-plan:vector-only".to_string()],
        };
        let error = request.check().expect_err("must be refused");
        assert!(error.contains("UNSUPPORTED_RETRIEVAL_PLAN"), "got: {error}");
    }

    #[test]
    fn an_empty_request_is_refused() {
        assert!(RetrievalPlanRequest {
            candidate_plans: vec![]
        }
        .check()
        .is_err());
    }
}
