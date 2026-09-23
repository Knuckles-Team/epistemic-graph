//! Capture-first open-weight policy evolution (EH-346, EH-347): the wire
//! contract for the five records EG owns.
//!
//! EG records and relates; it never trains. AU captures and dispatches, an
//! external trainer differentiates, graph-os authorizes and promotes. Every
//! record here is **immutable and content-addressed**: its id is a digest of
//! the tenant and its typed body, so an identical write is a replay and a
//! different body is a different record. Nothing can be overwritten in place,
//! and a read re-derives the id to prove the stored body is still the one
//! that was committed.
//!
//! * [`records`] -- capability, model-policy version, training run, evaluation.
//! * [`capture`] -- the trajectory capture and its held Blob-CAS arrays.

use serde::{Deserialize, Serialize};

pub mod capture;
pub mod records;

pub use capture::{
    ArrayEncoding, CaptureCompletion, CaptureEligibility, CaptureTraceFidelity, HeldBlobRef,
    PolicyCapture, RewardEvidenceSource, RewardRecord,
};
pub use records::{
    AdapterSpec, ControlSetting, EvaluationMetrics, EvaluationVerdict, KlEstimator, LogprobSupport,
    ModelPolicyVersion, OpenWeightPolicyCapability, PolicyControls, PolicyEvaluation,
    ResourceObservation, SafetyOutcome, TrainingMethod, TrainingOutput, TrainingRun,
    TrainingRunStatus, VersionOrigin,
};

use crate::contract::{Digest256, OpaqueId};

/// Format identity (RF-ADR-006) of every policy-evolution record.
pub const POLICY_EVOLUTION_SCHEMA_VERSION: u16 = 1;
/// Most captures one training run may name.
pub const MAX_TRAINING_INPUTS: usize = 256;
/// Parts-per-million denominator of every ratio.
pub const PPM_SCALE: u32 = 1_000_000;
const RECORD_DOMAIN: &[u8] = b"eg/policy-evolution-record/v1";

/// The five record kinds, and the record-id prefix each carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PolicyRecordKind {
    Capability,
    Capture,
    ModelPolicyVersion,
    TrainingRun,
    PolicyEvaluation,
}

const KIND_PREFIXES: [(PolicyRecordKind, &str); 5] = [
    (PolicyRecordKind::Capability, "polcap"),
    (PolicyRecordKind::Capture, "polcapture"),
    (PolicyRecordKind::ModelPolicyVersion, "polver"),
    (PolicyRecordKind::TrainingRun, "poltrain"),
    (PolicyRecordKind::PolicyEvaluation, "poleval"),
];

impl PolicyRecordKind {
    /// The record-id prefix of this kind.
    pub fn prefix(self) -> &'static str {
        KIND_PREFIXES
            .iter()
            .find(|(kind, _)| *kind == self)
            .map_or("", |(_, prefix)| prefix)
    }

    /// The kind a record id names, or `None` for any other id.
    pub fn of_record_id(record_id: &str) -> Option<Self> {
        let (prefix, digest) = record_id.split_once(':')?;
        Digest256::parse(digest).ok()?;
        KIND_PREFIXES
            .iter()
            .find(|(_, candidate)| *candidate == prefix)
            .map(|(kind, _)| *kind)
    }
}

/// One stored record, tagged by kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PolicyEvolutionRecord {
    Capability { record: OpenWeightPolicyCapability },
    Capture { record: PolicyCapture },
    ModelPolicyVersion { record: ModelPolicyVersion },
    TrainingRun { record: TrainingRun },
    PolicyEvaluation { record: PolicyEvaluation },
}

impl PolicyEvolutionRecord {
    pub fn kind(&self) -> PolicyRecordKind {
        match self {
            Self::Capability { .. } => PolicyRecordKind::Capability,
            Self::Capture { .. } => PolicyRecordKind::Capture,
            Self::ModelPolicyVersion { .. } => PolicyRecordKind::ModelPolicyVersion,
            Self::TrainingRun { .. } => PolicyRecordKind::TrainingRun,
            Self::PolicyEvaluation { .. } => PolicyRecordKind::PolicyEvaluation,
        }
    }

    /// Structural validation of the body, before any stored state is read.
    pub fn validate(&self) -> Result<(), PolicyRefusal> {
        match self {
            Self::Capability { record } => record.validate(),
            Self::Capture { record } => record.validate(),
            Self::ModelPolicyVersion { record } => record.validate(),
            Self::TrainingRun { record } => record.validate(),
            Self::PolicyEvaluation { record } => record.validate(),
        }
    }

    /// The record's immutable id: its kind prefix and a framed digest of the
    /// tenant and the typed body. The same body under the same tenant always
    /// yields the same id; any change yields a different one.
    pub fn record_id(&self, tenant_id: &str) -> Result<String, String> {
        let encoded = rmp_serde::to_vec_named(self)
            .map_err(|error| format!("policy record encoding failed: {error}"))?;
        let digest = Digest256::framed(RECORD_DOMAIN, &[tenant_id.as_bytes(), &encoded])?;
        Ok(format!("{}:{}", self.kind().prefix(), digest.to_hex()))
    }
}

/// A typed record body that can be taken back out of a [`PolicyEvolutionRecord`].
pub trait PolicyEvolutionRecordBody: Sized {
    /// The body, or `None` when the record is of another kind.
    fn from_record(record: PolicyEvolutionRecord) -> Option<Self>;
}

macro_rules! policy_evolution_record_bodies {
    ($($variant:ident => $body:ty),* $(,)?) => {
        $(
            impl PolicyEvolutionRecordBody for $body {
                fn from_record(record: PolicyEvolutionRecord) -> Option<Self> {
                    match record {
                        PolicyEvolutionRecord::$variant { record } => Some(record),
                        _ => None,
                    }
                }
            }
        )*
    };
}

policy_evolution_record_bodies! {
    Capability => OpenWeightPolicyCapability,
    Capture => PolicyCapture,
    ModelPolicyVersion => ModelPolicyVersion,
    TrainingRun => TrainingRun,
    PolicyEvaluation => PolicyEvaluation,
}

/// Read one record by id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PolicyRecordGetRequest {
    pub record_id: OpaqueId,
}

/// Every policy-evolution operation. The read/write split and the
/// authorization action live on the op, so the capability ledger and the
/// server's write classifier cannot disagree about an operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PolicyEvolutionOp {
    /// `PolicyCapabilityPut`: record one attested capability.
    PutCapability { request: OpenWeightPolicyCapability },
    /// `PolicyCaptureCommit`: record one terminal trajectory capture.
    CommitCapture { request: PolicyCapture },
    /// `ModelPolicyVersionRegister`: register one immutable version identity.
    RegisterModelPolicyVersion { request: ModelPolicyVersion },
    /// `TrainingRunCommit`: record one external training run receipt.
    CommitTrainingRun { request: TrainingRun },
    /// `PolicyEvaluationCommit`: record one held-out evaluation receipt.
    CommitPolicyEvaluation { request: PolicyEvaluation },
    /// `PolicyRecordGet`: read one record, re-verified against its id.
    Get { request: PolicyRecordGetRequest },
}

impl PolicyEvolutionOp {
    pub fn is_mutation(&self) -> bool {
        !matches!(self, Self::Get { .. })
    }

    /// The authorization action each operation needs. Capture is the only
    /// write an ordinary service identity may hold; the rest change what may
    /// be trained on or served, so they are `admin:` gated.
    pub fn authz_action(&self) -> &'static str {
        match self {
            Self::PutCapability { .. } => "admin:policy-capability",
            Self::CommitCapture { .. } => "policy:capture-write",
            Self::RegisterModelPolicyVersion { .. } => "admin:model-policy-register",
            Self::CommitTrainingRun { .. } => "admin:training-run-write",
            Self::CommitPolicyEvaluation { .. } => "admin:policy-evaluation-write",
            Self::Get { .. } => "policy:read",
        }
    }

    /// The op's wire tag, for audit lines and diagnostics.
    pub fn name(&self) -> &'static str {
        match self {
            Self::PutCapability { .. } => "put_capability",
            Self::CommitCapture { .. } => "commit_capture",
            Self::RegisterModelPolicyVersion { .. } => "register_model_policy_version",
            Self::CommitTrainingRun { .. } => "commit_training_run",
            Self::CommitPolicyEvaluation { .. } => "commit_policy_evaluation",
            Self::Get { .. } => "get",
        }
    }

    /// The record a write op commits, or the read request.
    pub fn into_record(self) -> Result<PolicyEvolutionRecord, PolicyRecordGetRequest> {
        match self {
            Self::PutCapability { request } => {
                Ok(PolicyEvolutionRecord::Capability { record: request })
            }
            Self::CommitCapture { request } => {
                Ok(PolicyEvolutionRecord::Capture { record: request })
            }
            Self::RegisterModelPolicyVersion { request } => {
                Ok(PolicyEvolutionRecord::ModelPolicyVersion { record: request })
            }
            Self::CommitTrainingRun { request } => {
                Ok(PolicyEvolutionRecord::TrainingRun { record: request })
            }
            Self::CommitPolicyEvaluation { request } => {
                Ok(PolicyEvolutionRecord::PolicyEvaluation { record: request })
            }
            Self::Get { request } => Err(request),
        }
    }
}

/// Whether a write created the record or found it already committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PolicyWriteDisposition {
    Written,
    Replayed,
}

/// The receipt of every policy-evolution write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PolicyRecordReceipt {
    pub record_id: String,
    pub kind: PolicyRecordKind,
    pub disposition: PolicyWriteDisposition,
    /// Set for a capture: whether it may be a training input.
    #[serde(default)]
    pub eligibility: Option<CaptureEligibility>,
    pub observed_at_ms: u64,
}

/// One record as read back, with the server-stamped writer and time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PolicyRecordView {
    pub record_id: String,
    pub recorded_by: String,
    pub recorded_at_ms: u64,
    pub record: PolicyEvolutionRecord,
}

/// Every typed refusal. On the wire it is the error text `"<CODE>: <detail>"`,
/// so a client can branch on the code without parsing prose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyRefusal {
    InvalidRecord(String),
    CapabilityMissing,
    CaptureDisabled,
    TrainDisabled,
    LogprobsUnsupported,
    SamplerMismatch,
    TrajectoryMissing,
    TrajectoryLengthMismatch,
    BlobMissing(String),
    RecordMissing(String),
    RunNotSucceeded,
    CaptureIneligible(String),
    RecordTampered(String),
}

impl PolicyRefusal {
    pub(crate) fn invalid(detail: impl Into<String>) -> Self {
        Self::InvalidRecord(detail.into())
    }

    /// The stable refusal code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidRecord(_) => "POLICY_INVALID_RECORD",
            Self::CapabilityMissing => "POLICY_CAPABILITY_MISSING",
            Self::CaptureDisabled => "POLICY_CAPTURE_DISABLED",
            Self::TrainDisabled => "POLICY_TRAIN_DISABLED",
            Self::LogprobsUnsupported => "POLICY_LOGPROBS_UNSUPPORTED",
            Self::SamplerMismatch => "POLICY_SAMPLER_MISMATCH",
            Self::TrajectoryMissing => "POLICY_TRAJECTORY_MISSING",
            Self::TrajectoryLengthMismatch => "POLICY_TRAJECTORY_LENGTH_MISMATCH",
            Self::BlobMissing(_) => "POLICY_BLOB_MISSING",
            Self::RecordMissing(_) => "POLICY_RECORD_MISSING",
            Self::RunNotSucceeded => "POLICY_RUN_NOT_SUCCEEDED",
            Self::CaptureIneligible(_) => "POLICY_CAPTURE_INELIGIBLE",
            Self::RecordTampered(_) => "POLICY_RECORD_TAMPERED",
        }
    }

    fn detail(&self) -> &str {
        match self {
            Self::InvalidRecord(detail)
            | Self::BlobMissing(detail)
            | Self::RecordMissing(detail)
            | Self::CaptureIneligible(detail)
            | Self::RecordTampered(detail) => detail,
            Self::CapabilityMissing
            | Self::CaptureDisabled
            | Self::TrainDisabled
            | Self::LogprobsUnsupported
            | Self::SamplerMismatch
            | Self::TrajectoryMissing
            | Self::TrajectoryLengthMismatch
            | Self::RunNotSucceeded => "",
        }
    }
}

impl std::fmt::Display for PolicyRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code(), self.detail())
    }
}

#[cfg(test)]
mod tests;
