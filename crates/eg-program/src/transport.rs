//! Provider-neutral transport boundary for model-dependent program modules.
//!
//! The durable contract cannot express URLs, credentials, provider names, prompt
//! bodies, or response bodies. An engine adapter resolves governed opaque content
//! references at execution time and applies its existing egress, auth, timeout,
//! retry, redaction, and tracing policy. This keeps `eg-program` independent from
//! LiteLLM and from any second provider stack.

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;

use eg_modality::OpaqueRef;
use serde::{Deserialize, Serialize};

use crate::{ProgramError, ProgramModality};

pub const MAX_MODEL_INPUTS: usize = 256;
pub const MAX_MODEL_OUTPUTS: usize = 128;
pub const MAX_MODEL_TOKENS: u32 = 1_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelTier {
    CheapestEligible,
    Balanced,
    HighestQuality,
    LocalOnly,
}

/// A governed reference resolved only inside the engine's trusted execution
/// boundary. It is safe to checkpoint because it carries no raw content or source
/// locator.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GovernedContentRef {
    pub content_ref: OpaqueRef,
    pub modality: ProgramModality,
    pub access_policy_ref: OpaqueRef,
    #[serde(default)]
    pub evidence_locus_refs: Vec<OpaqueRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCallRequest {
    pub request_ref: OpaqueRef,
    pub program_ref: OpaqueRef,
    pub model_profile_ref: OpaqueRef,
    pub tier: ModelTier,
    pub inputs: Vec<GovernedContentRef>,
    pub output_schema_refs: Vec<OpaqueRef>,
    pub max_output_tokens: u32,
    pub trace_ref: OpaqueRef,
}

impl ModelCallRequest {
    pub fn validate(&self) -> Result<(), ProgramError> {
        if self.inputs.is_empty()
            || self.inputs.len() > MAX_MODEL_INPUTS
            || self.output_schema_refs.is_empty()
            || self.output_schema_refs.len() > MAX_MODEL_OUTPUTS
            || self.max_output_tokens == 0
            || self.max_output_tokens > MAX_MODEL_TOKENS
            || self.inputs.iter().any(|input| {
                input.evidence_locus_refs.is_empty()
                    || input.evidence_locus_refs.len() > crate::MAX_EVIDENCE_PER_EXAMPLE
            })
        {
            return Err(ProgramError::ResourceLimit);
        }
        if self
            .inputs
            .iter()
            .map(|input| &input.content_ref)
            .collect::<BTreeSet<_>>()
            .len()
            != self.inputs.len()
            || self
                .output_schema_refs
                .iter()
                .collect::<BTreeSet<_>>()
                .len()
                != self.output_schema_refs.len()
            || self.inputs.iter().any(|input| {
                input
                    .evidence_locus_refs
                    .iter()
                    .collect::<BTreeSet<_>>()
                    .len()
                    != input.evidence_locus_refs.len()
            })
            || self
                .inputs
                .iter()
                .any(|input| input.access_policy_ref != self.inputs[0].access_policy_ref)
        {
            return Err(ProgramError::InvalidProgram);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelFinishReason {
    Completed,
    Length,
    ToolCall,
    Refused,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_input_tokens: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCallResponse {
    pub response_ref: OpaqueRef,
    pub output_refs: Vec<OpaqueRef>,
    pub trace_ref: OpaqueRef,
    pub usage: ModelUsage,
    pub finish_reason: ModelFinishReason,
}

impl ModelCallResponse {
    pub fn validate(&self) -> Result<(), ProgramError> {
        if self.output_refs.is_empty() || self.output_refs.len() > MAX_MODEL_OUTPUTS {
            return Err(ProgramError::ResourceLimit);
        }
        if self.output_refs.iter().collect::<BTreeSet<_>>().len() != self.output_refs.len() {
            return Err(ProgramError::InvalidProgram);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelTransportError {
    PolicyDenied,
    Unavailable,
    Timeout,
    InvalidResponse,
    ResourceLimit,
    Cancelled,
}

impl std::fmt::Display for ModelTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::PolicyDenied => "model call denied by policy",
            Self::Unavailable => "model transport unavailable",
            Self::Timeout => "model call timed out",
            Self::InvalidResponse => "model response failed validation",
            Self::ResourceLimit => "model call exceeded its resource budget",
            Self::Cancelled => "model call cancelled",
        })
    }
}

impl std::error::Error for ModelTransportError {}

/// Injected by the engine. Implementations must resolve all references through the
/// verified policy context and return only newly issued opaque references.
pub trait ModelTransport: Send + Sync {
    fn execute<'a>(
        &'a self,
        request: &'a ModelCallRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ModelCallResponse, ModelTransportError>> + Send + 'a>>;
}
