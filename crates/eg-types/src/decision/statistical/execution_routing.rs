//! Typed execution-configuration routing (`EG-DECISION-ENGINE-R035`):
//! `graph.decide()` accepts requests to select among candidate models,
//! prompts, skills, tools, harness modes, account modes, or sandbox
//! configurations through the same typed decision mechanism as any other
//! declared-candidate question (`QuestionKind::Route`, [`super::declared`]),
//! never a hard-coded branch per category.
//!
//! This module names the 7 routing categories the requirement enumerates.
//! Model, prompt, skill and tool already have a native component kind
//! ([`crate::agent_component::AgentComponentKind`]); harness mode, account
//! mode and sandbox configuration had no type anywhere, so a routing
//! decision could not name which of them it was choosing among. The
//! decision itself still runs through the ordinary `Decide` executor over
//! `CandidateSource::Declared` options; this module adds no
//! category-specific comparison path.

use serde::{Deserialize, Serialize};

/// The 7 kinds of execution configuration a routing decision may choose
/// among (EG-DECISION-ENGINE-R035). Adding an eighth is a registry change
/// here, not a new code path at each caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ExecutionRoutingCategory {
    Model,
    Prompt,
    Skill,
    Tool,
    HarnessMode,
    AccountMode,
    SandboxConfiguration,
}

impl ExecutionRoutingCategory {
    /// Every registered category, in the requirement's own order.
    pub const ALL: [ExecutionRoutingCategory; 7] = [
        Self::Model,
        Self::Prompt,
        Self::Skill,
        Self::Tool,
        Self::HarnessMode,
        Self::AccountMode,
        Self::SandboxConfiguration,
    ];
}

/// One execution-routing request's candidate option ids for a single
/// category, checked before any decision runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ExecutionRoutingRequest {
    pub category: ExecutionRoutingCategory,
    pub candidate_option_ids: Vec<String>,
}

impl ExecutionRoutingRequest {
    /// At least one candidate, and every candidate option id is non-empty
    /// and named once.
    pub fn check(&self) -> Result<(), String> {
        if self.candidate_option_ids.is_empty() {
            return Err(
                "an execution-routing request names at least one candidate option".to_string(),
            );
        }
        let mut seen = std::collections::BTreeSet::new();
        for option_id in &self.candidate_option_ids {
            if option_id.is_empty() || !seen.insert(option_id.as_str()) {
                return Err(
                    "execution-routing candidate option ids must be non-empty and unique"
                        .to_string(),
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_category_is_registered_exactly_once() {
        let mut seen = std::collections::BTreeSet::new();
        for category in ExecutionRoutingCategory::ALL {
            assert!(seen.insert(format!("{category:?}")), "duplicate category");
        }
        assert_eq!(ExecutionRoutingCategory::ALL.len(), 7);
    }

    #[test]
    fn a_request_with_unique_non_empty_ids_is_accepted() {
        let request = ExecutionRoutingRequest {
            category: ExecutionRoutingCategory::HarnessMode,
            candidate_option_ids: vec!["harness:autonomous".to_string(), "harness:guided".to_string()],
        };
        assert!(request.check().is_ok());
    }

    #[test]
    fn an_empty_option_id_is_refused() {
        let request = ExecutionRoutingRequest {
            category: ExecutionRoutingCategory::AccountMode,
            candidate_option_ids: vec!["".to_string()],
        };
        assert!(request.check().is_err());
    }

    #[test]
    fn duplicate_option_ids_are_refused() {
        let request = ExecutionRoutingRequest {
            category: ExecutionRoutingCategory::SandboxConfiguration,
            candidate_option_ids: vec!["sandbox:strict".to_string(), "sandbox:strict".to_string()],
        };
        assert!(request.check().is_err());
    }

    #[test]
    fn an_empty_request_is_refused() {
        assert!(ExecutionRoutingRequest {
            category: ExecutionRoutingCategory::Model,
            candidate_option_ids: vec![],
        }
        .check()
        .is_err());
    }
}
