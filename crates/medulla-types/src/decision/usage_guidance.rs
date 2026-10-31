//! Connector usage guidance answered from the graph (MEDULLA-DECISION-ENGINE-R177.1).

use serde::{Deserialize, Deserializer, Serialize};

fn required_tool<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let tool = String::deserialize(deserializer)?;
    if tool.trim().is_empty() {
        return Err(serde::de::Error::custom("a usage step names a tool"));
    }
    Ok(tool)
}

/// One ordered step: the tool, the steps whose outputs feed it and the steps
/// it precedes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct UsageStep {
    #[serde(deserialize_with = "required_tool")]
    pub tool: String,
    #[serde(default)]
    pub inputs_from: Vec<String>,
    #[serde(default)]
    pub precedes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct UsageGuidance {
    pub connector: String,
    pub job_class: String,
    pub conditions: Vec<String>,
    pub steps: Vec<UsageStep>,
    pub prompts: Vec<String>,
    pub cautions: Vec<String>,
    pub graph_revision: u64,
}

#[cfg(test)]
mod tests;
