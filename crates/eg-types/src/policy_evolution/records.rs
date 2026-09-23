//! The policy-evolution records other than the capture itself: the attested
//! open-weight capability, the immutable model-policy identity, the external
//! training run receipt and the held-out evaluation receipt.
//!
//! Pure serde plus structural validation. Cross-record rules (a capture needs
//! an enabled capability, a trained version needs a succeeded run) need the
//! stored records and live in the server's gate.

use serde::{Deserialize, Serialize};

use super::capture::HeldBlobRef;
use super::{PolicyRefusal, MAX_TRAINING_INPUTS, PPM_SCALE};
use crate::contract::{BoundedVec, Digest256, OpaqueId, ResourceId};

/// What an endpoint's sampler reports about the tokens it chose.
///
/// Attested by the capability probe, never inferred from a provider or host
/// name. `top_k == 0` means no alternative-token records are available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LogprobSupport {
    /// The log-probability of each sampled token. Capture fails closed without it.
    pub chosen_token: bool,
    #[serde(default)]
    pub top_k: u16,
    #[serde(default)]
    pub auxiliary_draws: bool,
}

/// One of the three independent controls. Disabled unless a record says so.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ControlSetting {
    #[serde(default)]
    pub enabled: bool,
    /// The authorization scope allowed to request this control. An enabled
    /// control must name one.
    #[serde(default)]
    pub scope: Option<ResourceId>,
}

impl ControlSetting {
    fn validate(&self, control: &str) -> Result<(), PolicyRefusal> {
        if self.enabled && self.scope.is_none() {
            return Err(PolicyRefusal::invalid(format!(
                "enabled {control} control must name its authorization scope"
            )));
        }
        Ok(())
    }
}

/// `capture`, `train` and `promote` are independent: enabling one never
/// implies another. All three default to off.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PolicyControls {
    #[serde(default)]
    pub capture: ControlSetting,
    #[serde(default)]
    pub train: ControlSetting,
    #[serde(default)]
    pub promote: ControlSetting,
}

/// `OpenWeightPolicyCapability/v1`: the attested serving endpoint policy
/// evolution may use. Endpoint credentials stay in graph-os's secret provider;
/// `endpoint_ref` is only its opaque id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OpenWeightPolicyCapability {
    pub provider: ResourceId,
    pub endpoint_ref: OpaqueId,
    pub base_checkpoint_digest: Digest256,
    #[serde(default)]
    pub adapter_digest: Option<Digest256>,
    pub tokenizer_digest: Digest256,
    pub decode_params_digest: Digest256,
    /// Where a trained artifact is written. Always a NEW artifact: the live
    /// one is never overwritten.
    pub artifact_destination_ref: OpaqueId,
    pub logprobs: LogprobSupport,
    #[serde(default)]
    pub controls: PolicyControls,
    /// Digest of the probe evidence that attested this record.
    pub probe_digest: Digest256,
    pub probed_at_ms: u64,
}

impl OpenWeightPolicyCapability {
    /// Structural validation. Capture without chosen-token log-probabilities
    /// fails closed here, before anything is recorded.
    pub fn validate(&self) -> Result<(), PolicyRefusal> {
        self.controls.capture.validate("capture")?;
        self.controls.train.validate("train")?;
        self.controls.promote.validate("promote")?;
        if self.controls.capture.enabled && !self.logprobs.chosen_token {
            return Err(PolicyRefusal::LogprobsUnsupported);
        }
        Ok(())
    }
}

/// How a model-policy version came to exist.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "origin", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum VersionOrigin {
    /// A served base checkpoint (optionally with an existing adapter).
    Base,
    /// The output of one succeeded training run.
    Trained { training_run_id: OpaqueId },
}

/// `ModelPolicyVersion/v1`: an immutable artifact identity. Bytes stay in the
/// external artifact store; EG holds identity and lineage. The record id the
/// engine returns IS the version id every capture and route refers to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ModelPolicyVersion {
    pub checkpoint_digest: Digest256,
    #[serde(default)]
    pub adapter_digest: Option<Digest256>,
    pub tokenizer_digest: Digest256,
    pub artifact_ref: OpaqueId,
    #[serde(default)]
    pub parent_version_id: Option<OpaqueId>,
    pub origin: VersionOrigin,
}

impl ModelPolicyVersion {
    /// A trained version is a new adapter on a named parent.
    pub fn validate(&self) -> Result<(), PolicyRefusal> {
        let trained = matches!(self.origin, VersionOrigin::Trained { .. });
        if trained && (self.adapter_digest.is_none() || self.parent_version_id.is_none()) {
            return Err(PolicyRefusal::invalid(
                "a trained version needs an adapter digest and a parent version",
            ));
        }
        Ok(())
    }
}

/// The KL estimator a KLPO job uses. Chosen by measured cost; there is no
/// platform default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "estimator", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum KlEstimator {
    SampledToken,
    TopK { k: u16 },
    Binary,
    FullVocabulary,
    MonteCarlo { draws: u16 },
}

/// The external optimisation method. KLPO is one option beside the existing
/// GRPO/DPO/SFT choices.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum TrainingMethod {
    Sft,
    Dpo,
    Grpo,
    Klpo { estimator: KlEstimator },
}

/// What the run trains. Adapter-only: full-weight mutation is not expressible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "adapter", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AdapterSpec {
    Lora { rank: u16 },
}

/// Observed resource use of one run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ResourceObservation {
    pub wall_ms: u64,
    pub gpu_seconds: u64,
    pub peak_memory_bytes: u64,
}

/// The new artifact a succeeded run produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TrainingOutput {
    pub artifact_digest: Digest256,
    pub artifact_ref: OpaqueId,
}

/// Terminal status. Only a succeeded run carries an output, so a failed or
/// cancelled run has nothing a version could name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum TrainingRunStatus {
    Succeeded { output: TrainingOutput },
    Failed,
    Cancelled,
}

/// `TrainingRun/v1`: the receipt of one external training job. EG records it;
/// it never optimises weights.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TrainingRun {
    pub capability_id: OpaqueId,
    /// The native WorkItem that owned the attempt.
    pub work_item_id: OpaqueId,
    pub base_version_id: OpaqueId,
    pub input_capture_ids: BoundedVec<OpaqueId, MAX_TRAINING_INPUTS>,
    pub method: TrainingMethod,
    pub adapter: AdapterSpec,
    pub trainer_image_digest: Digest256,
    pub hyperparameters_digest: Digest256,
    #[serde(default)]
    pub resources: ResourceObservation,
    pub status: TrainingRunStatus,
    #[serde(default)]
    pub log: Option<HeldBlobRef>,
}

impl TrainingRun {
    pub fn validate(&self) -> Result<(), PolicyRefusal> {
        if self.input_capture_ids.is_empty() {
            return Err(PolicyRefusal::invalid(
                "a training run names its input captures",
            ));
        }
        if let Some(log) = &self.log {
            log.validate_bytes("log")?;
        }
        Ok(())
    }
}

/// Whether the safety policy passed on the frozen set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SafetyOutcome {
    Passed,
    Failed,
}

/// The evaluator's decision. Loss movement is diagnostic, never a verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum EvaluationVerdict {
    Accepted,
    Rejected,
}

/// Held-out measurements. Ratios are parts-per-million, never floats, so the
/// record digest is exact.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EvaluationMetrics {
    pub task_success_ppm: u32,
    pub baseline_task_success_ppm: u32,
    pub cost_micros: u64,
    pub latency_p50_ms: u32,
    pub latency_p95_ms: u32,
    pub gpu_seconds: u64,
    pub trace_completeness_ppm: u32,
    pub unsupported_replay_mass_ppm: u32,
}

impl EvaluationMetrics {
    fn validate(&self) -> Result<(), PolicyRefusal> {
        let ratios = [
            self.task_success_ppm,
            self.baseline_task_success_ppm,
            self.trace_completeness_ppm,
            self.unsupported_replay_mass_ppm,
        ];
        if ratios.iter().any(|ratio| *ratio > PPM_SCALE) {
            return Err(PolicyRefusal::invalid("a ppm ratio exceeds 1000000"));
        }
        Ok(())
    }
}

/// `PolicyEvaluation/v1`: an independent held-out evaluation of one version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PolicyEvaluation {
    pub version_id: OpaqueId,
    #[serde(default)]
    pub baseline_version_id: Option<OpaqueId>,
    pub evaluation_set_digest: Digest256,
    pub evaluator_digest: Digest256,
    pub metrics: EvaluationMetrics,
    pub safety: SafetyOutcome,
    pub verdict: EvaluationVerdict,
    #[serde(default)]
    pub report: Option<HeldBlobRef>,
}

impl PolicyEvaluation {
    /// An accepted verdict needs a passed safety policy and no regression
    /// against the baseline.
    pub fn validate(&self) -> Result<(), PolicyRefusal> {
        self.metrics.validate()?;
        let regressed = self.metrics.task_success_ppm < self.metrics.baseline_task_success_ppm;
        let accepted = self.verdict == EvaluationVerdict::Accepted;
        if accepted && (self.safety == SafetyOutcome::Failed || regressed) {
            return Err(PolicyRefusal::invalid(
                "an accepted evaluation needs passed safety and no regression",
            ));
        }
        if let Some(report) = &self.report {
            report.validate_bytes("report")?;
        }
        Ok(())
    }
}
