//! Natural language without an LLM (EH-028, EH-064).
//!
//! An `NlTemplate` component pairs example utterances and ontology labels with
//! a typed target and typed slots. Routing an utterance is itself a statistical
//! decision (`QuestionKind::TemplateChoice`) over the tenant's templates;
//! slots are filled by exact matching against the native ontology's labels and
//! the utterance's own tokens, never by generation. When routing abstains, a
//! configured LLM planner may propose a template -- and that proposal is
//! recorded as a CLAIM, never as the decision.

use serde::{Deserialize, Serialize};

use super::{StatisticalQuestion, TypedParam};
use crate::agent_component::ComponentDependency;
use crate::contract::BoundedVec;

/// Format identity of an NL template body.
pub const NL_TEMPLATE_SCHEMA_VERSION: u16 = 1;

/// What one slot accepts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "slot_type", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum NlSlotType {
    /// A native ontology term under `under`, matched by its label.
    Iri { under: String },
    /// A decimal integer token.
    Int,
    /// The utterance text after a marker word.
    Text { after: String },
}

/// One typed slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct NlSlot {
    pub name: String,
    pub slot_type: NlSlotType,
    pub required: bool,
}

/// Which request a template fills.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "target", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum NlTarget {
    /// A statistical decision question.
    Decide { question: StatisticalQuestion },
    /// An agent assembly.
    AgentAssemble,
    /// A named, operator-published query.
    NamedQuery { query_id: String },
}

/// The body of a published `NlTemplate` component.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct NlTemplateBody {
    pub schema_version: u16,
    pub utterances: BoundedVec<String, 64>,
    /// Ontology labels and alternative labels this template answers to.
    #[serde(default)]
    pub labels: BoundedVec<String, 16>,
    #[serde(default)]
    pub slots: BoundedVec<NlSlot, 16>,
    pub target: NlTarget,
}

impl NlTemplateBody {
    /// The validating constructor.
    pub fn checked(self) -> Result<Self, String> {
        if self.schema_version != NL_TEMPLATE_SCHEMA_VERSION {
            return Err(format!(
                "NL template version {} is not served",
                self.schema_version
            ));
        }
        if self.utterances.is_empty() || self.utterances.iter().any(|u| u.trim().is_empty()) {
            return Err("an NL template carries at least one non-empty utterance".to_string());
        }
        let mut names: Vec<&str> = self.slots.iter().map(|s| s.name.as_str()).collect();
        names.sort_unstable();
        if names.windows(2).any(|w| w[0] == w[1]) {
            return Err("NL template slot names must be unique".to_string());
        }
        Ok(self)
    }
}

/// Who produced a template choice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "by", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum NlChoiceSource {
    /// The engine's own routing decision.
    Engine,
    /// An LLM planner's proposal after the engine abstained: a claim.
    LlmProposal {
        producer: String,
        prompt_digest: String,
    },
}

/// A routed utterance: the template chosen and the typed slot values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct NlBinding {
    pub template: ComponentDependency,
    pub target: NlTarget,
    pub params: BoundedVec<TypedParam, 16>,
    /// Required slots the utterance did not fill.
    #[serde(default)]
    pub unfilled: BoundedVec<String, 16>,
    pub source: NlChoiceSource,
}
