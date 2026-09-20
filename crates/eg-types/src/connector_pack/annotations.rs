//! What a pack entry DECLARES about itself.
//!
//! Every value here is a publisher claim. The engine records it as one: an
//! imported cost becomes [`crate::agent_component::FactQuality::Declared`] with
//! a [`crate::agent_component::PriceSource::ConnectorPack`] naming the entry it
//! came from, so a decision that used it can say exactly whose number it was.

use serde::{Deserialize, Serialize};

use crate::agent_component::{DeclaredCost, DeclaredLatency};
use crate::contract::BoundedVec;

/// Declared per-invocation prices, in integer micros — the same shape an
/// [`crate::agent_component::CostFacts`] carries once the engine has recorded
/// who declared it, so a pack entry's raw claim and the digested fact can
/// never drift into two different ideas of "cost".
pub type PackCost = DeclaredCost;

/// A model profile a pack publishes.
///
/// The three `supports_*` flags are tri-state here and `bool` on the component
/// facts, because absent is not false: a pack that leaves one out has said
/// nothing, and importing that as `false` would silently make the model
/// ineligible. An entry with any of them absent is refused `INVALID_FACTS`
/// rather than imported with an invented answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackModelFacts {
    pub provider: String,
    pub model_identity: String,
    pub context_window_tokens: u64,
    pub max_output_tokens: u64,
    #[serde(default)]
    pub supports_tools: Option<bool>,
    #[serde(default)]
    pub supports_structured_output: Option<bool>,
    #[serde(default)]
    pub supports_vision: Option<bool>,
}

/// Everything one entry declares beyond its bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PackAnnotations {
    #[serde(default)]
    pub provides: BoundedVec<String, 64>,
    #[serde(default)]
    pub requires_capabilities: BoundedVec<String, 64>,
    #[serde(default)]
    pub modalities_in: BoundedVec<String, 64>,
    #[serde(default)]
    pub modalities_out: BoundedVec<String, 64>,
    #[serde(default)]
    pub required_scopes: BoundedVec<String, 64>,
    #[serde(default)]
    pub read_only_hint: Option<bool>,
    #[serde(default)]
    pub destructive_hint: Option<bool>,
    #[serde(default)]
    pub idempotent_hint: Option<bool>,
    #[serde(default)]
    pub open_world_hint: Option<bool>,
    #[serde(default)]
    pub contract_version: Option<String>,
    #[serde(default)]
    pub cost: Option<PackCost>,
    #[serde(default)]
    pub latency_declared: Option<DeclaredLatency>,
    #[serde(default)]
    pub model: Option<PackModelFacts>,
    /// The SDK contract the publisher pinned. Stored as a claim ATTRIBUTE
    /// rather than a typed fact: the engine cannot recompute the SDK's own
    /// normalization, so it can record the string and must not pretend to have
    /// checked it.
    #[serde(default)]
    pub sdk_contract_pin: Option<String>,
}
