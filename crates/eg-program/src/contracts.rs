//! Durable LM-program contracts.
//!
//! These types intentionally store only opaque references, numeric coordinates,
//! bounded schema identifiers, and policy metadata. Prompt bodies, source bytes,
//! identities, endpoints, credentials, and filesystem paths are ephemeral data and
//! are not representable in this module.

use std::collections::BTreeSet;

use eg_modality::{EvidenceAddress, EvidenceLocus, OpaqueRef, PolicyEnvelope, PrivacyAttestation};
use serde::{Deserialize, Serialize};

pub const PROGRAM_SCHEMA_VERSION: u16 = 1;
pub const MAX_SIGNATURE_FIELDS: usize = 128;
pub const MAX_TOOL_REFS: usize = 128;
pub const MAX_PURPOSE_REFS: usize = 128;
pub const MAX_EXAMPLES: usize = 100_000;
pub const MAX_INPUT_REFS: usize = 256;
pub const MAX_EVIDENCE_PER_EXAMPLE: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgramModality {
    Text,
    Document,
    Image,
    Audio,
    Video,
    Graph,
    Table,
    TimeSeries,
    Vector,
    Spatial,
    Tensor,
    Code,
    Trace,
    Binary,
}

impl ProgramModality {
    pub const ALL: [Self; 14] = [
        Self::Text,
        Self::Document,
        Self::Image,
        Self::Audio,
        Self::Video,
        Self::Graph,
        Self::Table,
        Self::TimeSeries,
        Self::Vector,
        Self::Spatial,
        Self::Tensor,
        Self::Code,
        Self::Trace,
        Self::Binary,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Document => "document",
            Self::Image => "image",
            Self::Audio => "audio",
            Self::Video => "video",
            Self::Graph => "graph",
            Self::Table => "table",
            Self::TimeSeries => "time_series",
            Self::Vector => "vector",
            Self::Spatial => "spatial",
            Self::Tensor => "tensor",
            Self::Code => "code",
            Self::Trace => "trace",
            Self::Binary => "binary",
        }
    }

    pub fn accepts(self, address: &EvidenceAddress) -> bool {
        match (self, address) {
            (Self::Text | Self::Document, EvidenceAddress::CharacterRange { start, end }) => {
                end > start
            }
            (
                Self::Image,
                EvidenceAddress::ImageRegion {
                    x,
                    y,
                    width,
                    height,
                },
            ) => {
                x.is_finite()
                    && y.is_finite()
                    && width.is_finite()
                    && height.is_finite()
                    && *width > 0.0
                    && *height > 0.0
            }
            (Self::Audio, EvidenceAddress::AudioRange { start_ms, end_ms }) => end_ms > start_ms,
            (Self::Video, EvidenceAddress::VideoTimeRange { start_ms, end_ms }) => {
                end_ms > start_ms
            }
            (Self::TimeSeries, EvidenceAddress::MetricWindow { start_ms, end_ms }) => {
                end_ms > start_ms
            }
            (
                Self::Video,
                EvidenceAddress::FrameRange {
                    start_frame,
                    end_frame,
                },
            ) => end_frame >= start_frame,
            (
                Self::Graph | Self::Table | Self::Vector | Self::Tensor | Self::Binary,
                EvidenceAddress::RowVersion { version, .. },
            ) => *version > 0,
            (
                Self::Table | Self::Tensor,
                EvidenceAddress::TableCellRange {
                    row_start,
                    row_end,
                    col_start,
                    col_end,
                },
            ) => row_end >= row_start && col_end >= col_start,
            (Self::Spatial, EvidenceAddress::Point { x, y }) => x.is_finite() && y.is_finite(),
            (
                Self::Code,
                EvidenceAddress::CodeSymbol {
                    start_line,
                    end_line,
                    ..
                },
            ) => end_line >= start_line,
            (Self::Trace, EvidenceAddress::TraceSpan { .. }) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldRole {
    Input,
    Output,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldSpec {
    /// Bounded machine identifier, never a display name.
    pub name: String,
    pub role: FieldRole,
    pub schema_ref: OpaqueRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description_ref: Option<OpaqueRef>,
    #[serde(default)]
    pub required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignatureSpec {
    pub signature_ref: OpaqueRef,
    pub instruction_ref: OpaqueRef,
    pub fields: Vec<FieldSpec>,
}

impl SignatureSpec {
    pub fn validate(&self) -> Result<(), ProgramError> {
        if self.fields.is_empty() || self.fields.len() > MAX_SIGNATURE_FIELDS {
            return Err(ProgramError::InvalidSignature);
        }
        let mut names = BTreeSet::new();
        let mut inputs = 0usize;
        let mut outputs = 0usize;
        for field in &self.fields {
            if !is_identifier(&field.name) || !names.insert(field.name.as_str()) {
                return Err(ProgramError::InvalidSignature);
            }
            match field.role {
                FieldRole::Input => inputs += 1,
                FieldRole::Output => outputs += 1,
            }
        }
        if inputs == 0 || outputs == 0 {
            return Err(ProgramError::InvalidSignature);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModuleKind {
    Predict,
    ChainOfThought,
    React,
    ProgramOfThought,
    BestOfN,
    Refine,
    RecursiveLanguageModel,
    Parallel,
    MultiChainComparison,
    CodeAct,
    Custom,
}

impl ModuleKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Predict => "predict",
            Self::ChainOfThought => "chain_of_thought",
            Self::React => "react",
            Self::ProgramOfThought => "program_of_thought",
            Self::BestOfN => "best_of_n",
            Self::Refine => "refine",
            Self::RecursiveLanguageModel => "recursive_language_model",
            Self::Parallel => "parallel",
            Self::MultiChainComparison => "multi_chain_comparison",
            Self::CodeAct => "code_act",
            Self::Custom => "custom",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterKind {
    Chat,
    Json,
    Xml,
    TwoStep,
    Document,
    Citations,
}

impl AdapterKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Json => "json",
            Self::Xml => "xml",
            Self::TwoStep => "two_step",
            Self::Document => "document",
            Self::Citations => "citations",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramRevision {
    pub schema_version: u16,
    pub program_ref: OpaqueRef,
    pub revision: u64,
    pub parent_ref: Option<OpaqueRef>,
    pub signature: SignatureSpec,
    pub module: ModuleKind,
    pub adapter: AdapterKind,
    #[serde(default)]
    pub tool_refs: Vec<OpaqueRef>,
    pub policy: PolicyEnvelope,
}

impl ProgramRevision {
    pub fn validate(&self) -> Result<(), ProgramError> {
        if self.schema_version != PROGRAM_SCHEMA_VERSION || self.revision == 0 {
            return Err(ProgramError::UnsupportedVersion);
        }
        self.signature.validate()?;
        if self.tool_refs.len() > MAX_TOOL_REFS || self.policy.purpose_refs.len() > MAX_PURPOSE_REFS
        {
            return Err(ProgramError::ResourceLimit);
        }
        if self.parent_ref.as_ref() == Some(&self.program_ref) {
            return Err(ProgramError::InvalidProgram);
        }
        let unique = self.tool_refs.iter().collect::<BTreeSet<_>>();
        let purposes = self.policy.purpose_refs.iter().collect::<BTreeSet<_>>();
        if unique.len() != self.tool_refs.len() || purposes.len() != self.policy.purpose_refs.len()
        {
            return Err(ProgramError::InvalidProgram);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceBinding {
    pub modality: ProgramModality,
    pub locus: EvidenceLocus,
    pub policy: PolicyEnvelope,
}

impl EvidenceBinding {
    pub fn validate(&self) -> Result<(), ProgramError> {
        if self.locus.policy_ref != self.policy.access_policy_ref {
            return Err(ProgramError::PolicyMismatch);
        }
        if !self.modality.accepts(&self.locus.address) {
            return Err(ProgramError::ModalityMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExampleOutcome {
    Success,
    Failure,
    Abstained,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExampleSplit {
    #[default]
    Train,
    Validation,
    Test,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrainingExample {
    pub example_ref: OpaqueRef,
    pub input_refs: Vec<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_output_ref: Option<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_output_ref: Option<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback_ref: Option<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_ref: Option<OpaqueRef>,
    #[serde(default)]
    pub split: ExampleSplit,
    pub outcome: ExampleOutcome,
    /// Held-out or online metric in `[0, 1]`.
    pub score: f64,
    /// Non-negative evaluation weight.
    #[serde(default = "one")]
    pub weight: f64,
    pub evidence: Vec<EvidenceBinding>,
}

impl TrainingExample {
    pub fn validate(&self, policy: &PolicyEnvelope) -> Result<(), ProgramError> {
        if self.input_refs.is_empty()
            || self.input_refs.len() > MAX_INPUT_REFS
            || self.evidence.is_empty()
            || self.evidence.len() > MAX_EVIDENCE_PER_EXAMPLE
            || !self.score.is_finite()
            || !(0.0..=1.0).contains(&self.score)
            || !self.weight.is_finite()
            || self.weight < 0.0
        {
            return Err(ProgramError::InvalidExample);
        }
        if self.input_refs.iter().collect::<BTreeSet<_>>().len() != self.input_refs.len() {
            return Err(ProgramError::InvalidExample);
        }
        if matches!(self.outcome, ExampleOutcome::Success) && self.observed_output_ref.is_none() {
            return Err(ProgramError::InvalidExample);
        }
        for binding in &self.evidence {
            binding.validate()?;
            if &binding.policy != policy {
                return Err(ProgramError::PolicyMismatch);
            }
        }
        Ok(())
    }

    pub fn modalities(&self) -> BTreeSet<ProgramModality> {
        self.evidence.iter().map(|item| item.modality).collect()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrainingCorpus {
    pub corpus_ref: OpaqueRef,
    pub snapshot_version: u64,
    pub privacy: PrivacyAttestation,
    pub examples: Vec<TrainingExample>,
}

impl TrainingCorpus {
    pub fn validate(&self, program: &ProgramRevision) -> Result<(), ProgramError> {
        self.privacy
            .validate()
            .map_err(|_| ProgramError::PrivacyAttestation)?;
        if self.snapshot_version == 0
            || self.examples.is_empty()
            || self.examples.len() > MAX_EXAMPLES
        {
            return Err(ProgramError::ResourceLimit);
        }
        let mut ids = BTreeSet::new();
        let mut loci = BTreeSet::new();
        for example in &self.examples {
            if !ids.insert(example.example_ref.as_str()) {
                return Err(ProgramError::InvalidExample);
            }
            for binding in &example.evidence {
                if !loci.insert(binding.locus.id.as_ref().as_str()) {
                    return Err(ProgramError::InvalidExample);
                }
            }
            example.validate(&program.policy)?;
        }
        Ok(())
    }

    pub fn modalities(&self) -> BTreeSet<ProgramModality> {
        self.examples
            .iter()
            .flat_map(TrainingExample::modalities)
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProgramError {
    UnsupportedVersion,
    InvalidProgram,
    InvalidSignature,
    InvalidExample,
    PolicyMismatch,
    PrivacyAttestation,
    ModalityMismatch,
    ResourceLimit,
    InvalidArtifact,
    InvalidPlan,
    NoEligibleExamples,
    Cancelled,
    InvalidPromotion,
    InvalidCommit,
}

impl std::fmt::Display for ProgramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::UnsupportedVersion => "unsupported program contract version",
            Self::InvalidProgram => "program contract is invalid",
            Self::InvalidSignature => "program signature is invalid",
            Self::InvalidExample => "training example is invalid",
            Self::PolicyMismatch => "program policy scope does not match evidence",
            Self::PrivacyAttestation => "program privacy attestation failed",
            Self::ModalityMismatch => "evidence address does not match its modality",
            Self::ResourceLimit => "program operation exceeds resource limits",
            Self::InvalidArtifact => "optimizer artifact is invalid or out of policy scope",
            Self::InvalidPlan => "optimizer execution plan is invalid",
            Self::NoEligibleExamples => "no governed examples satisfy the optimizer",
            Self::Cancelled => "program optimization cancelled",
            Self::InvalidPromotion => "program candidate failed the promotion contract",
            Self::InvalidCommit => "program promotion commit is invalid",
        })
    }
}

impl std::error::Error for ProgramError {}

fn one() -> f64 {
    1.0
}

fn is_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}
