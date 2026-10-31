//! Typed connector prompt metadata used by agent assembly (MEDULLA-DECISION-ENGINE-R814.1.2).

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::orchestration::HarnessComponentKind;

/// The packaged prompt role that determines its harness-component kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PromptRole {
    /// A prompt eligible to frame an agent as its system prompt.
    System,
    /// Task-specific prompts are guidance, not system-prompt candidates.
    Task,
    /// Template prompts are guidance, not system-prompt candidates.
    Template,
}

/// A connector prompt and the tasks it declares itself able to serve.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PromptCandidate {
    /// The stable component identity in the connector catalog.
    pub component_id: String,
    /// The connector that supplied this prompt.
    pub connector: String,
    /// Its declared role; task and template prompts remain tool guidance.
    pub role: PromptRole,
    /// Task IRIs declared by a system prompt candidate.
    pub serves: BTreeSet<String>,
    /// The catalog's lowercase SHA-256 digest (64 hexadecimal characters).
    pub digest: String,
}

impl PromptCandidate {
    /// Map the declared role to the harness component category it belongs to.
    pub fn component_kind(&self) -> HarnessComponentKind {
        match self.role {
            PromptRole::System => HarnessComponentKind::SystemPrompt,
            PromptRole::Task | PromptRole::Template => HarnessComponentKind::ToolGuidance,
        }
    }

    /// Only a system-role prompt can be considered by system-prompt assembly.
    pub fn is_system_candidate(&self) -> bool {
        self.role == PromptRole::System
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-DECISION-ENGINE-R814.1.2
    #[test]
    fn candidate_round_trips_and_kind_follows_role() {
        let candidate = PromptCandidate {
            component_id: "urn:prompt:triage".into(),
            connector: "urn:connector:service-desk".into(),
            role: PromptRole::System,
            serves: BTreeSet::from(["urn:task:incident-triage".into()]),
            digest: "a".repeat(64),
        };
        let encoded = serde_json::to_string(&candidate).unwrap();
        let decoded: PromptCandidate = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, candidate);
        assert_eq!(decoded.component_kind(), HarnessComponentKind::SystemPrompt);
        assert!(decoded.is_system_candidate());

        for role in [PromptRole::Task, PromptRole::Template] {
            let guidance = PromptCandidate {
                role,
                ..candidate.clone()
            };
            assert_eq!(
                guidance.component_kind(),
                HarnessComponentKind::ToolGuidance
            );
            assert!(!guidance.is_system_candidate());
        }
    }
}
