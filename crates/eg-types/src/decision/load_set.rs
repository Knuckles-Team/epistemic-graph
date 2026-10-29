//! Job-scoped load set question and answer (MEDULLA-DECISION-ENGINE-R176.1).

use serde::{Deserialize, Deserializer, Serialize};

use super::PremiseRef;

pub const DEFAULT_LOAD_SET_MAX_TOOLS: u16 = 32;
pub const LOAD_SET_MAX_TOOLS_LIMIT: u16 = 128;
pub const DEFAULT_LOAD_SET_MAX_AGENTS: u16 = 4;

fn default_max_tools() -> u16 {
    DEFAULT_LOAD_SET_MAX_TOOLS
}

fn default_max_agents() -> u16 {
    DEFAULT_LOAD_SET_MAX_AGENTS
}

fn bounded_max_tools<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u16, D::Error> {
    let value = u16::deserialize(deserializer)?;
    if value == 0 || value > LOAD_SET_MAX_TOOLS_LIMIT {
        return Err(serde::de::Error::custom(format!(
            "max_tools must be 1 to {LOAD_SET_MAX_TOOLS_LIMIT}"
        )));
    }
    Ok(value)
}

/// What a job needs loaded. `job_embedding` is optional; the text alone is
/// enough to derive capabilities.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LoadSetQuestion {
    pub job_text: String,
    #[serde(default)]
    pub job_embedding: Vec<f32>,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    #[serde(default = "default_max_tools", deserialize_with = "bounded_max_tools")]
    pub max_tools: u16,
    #[serde(default = "default_max_agents")]
    pub max_agents: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentStart {
    pub agent_iri: String,
    pub prompt_iri: String,
    pub model_profile: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LoadSetAnswer {
    pub tools: Vec<String>,
    pub skills: Vec<String>,
    pub agents: Vec<AgentStart>,
    pub graph_revision: u64,
    pub premises: Vec<PremiseRef>,
}

#[cfg(test)]
mod tests;
