//! `PolicyCapture/v1`: the bounded summary relating one durable trajectory to
//! the Blob-CAS arrays a trainer needs.
//!
//! Token ids, the action mask and the frozen sampler `log_q` are large, so
//! they live in the engine's Blob CAS and the record carries only their held
//! references. The stored `q` facts are immutable: they are never recomputed
//! under a newer checkpoint.

use serde::{Deserialize, Serialize};

use super::PolicyRefusal;
use crate::contract::{Digest256, OpaqueId, ResourceId};

/// How the elements of a referenced array are encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ArrayEncoding {
    /// Little-endian `u32` (token ids, sampler Top-K / auxiliary ids).
    U32Le,
    /// Little-endian IEEE-754 `f32` (log-probabilities).
    F32Le,
    /// One byte per token, `1` = policy token (prompt, padding and tool
    /// output tokens are `0` and masked from the loss).
    U8Mask,
    /// Opaque bytes (logs, reports). Carries no element width.
    Bytes,
}

impl ArrayEncoding {
    fn element_width(self) -> Option<u64> {
        match self {
            Self::U32Le | Self::F32Le => Some(4),
            Self::U8Mask => Some(1),
            Self::Bytes => None,
        }
    }
}

/// A Blob-CAS object this record depends on. The engine checks, at commit,
/// that the caller holds a blob with this digest and exact length.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HeldBlobRef {
    /// The Blob CAS digest `BlobCommit` returned.
    pub digest: Digest256,
    pub length: u64,
    pub encoding: ArrayEncoding,
    /// Element count; `0` for [`ArrayEncoding::Bytes`].
    #[serde(default)]
    pub elements: u32,
}

impl HeldBlobRef {
    /// `length` agrees with `elements` for a typed array.
    fn validate_array(&self, what: &str, elements: u32) -> Result<(), PolicyRefusal> {
        let width = self.encoding.element_width();
        let consistent = width
            .is_some_and(|width| self.length == u64::from(self.elements).saturating_mul(width));
        if !consistent || self.elements != elements {
            return Err(PolicyRefusal::invalid(format!(
                "{what} must hold exactly {elements} typed elements"
            )));
        }
        Ok(())
    }

    /// An opaque byte object: non-empty and untyped.
    pub(crate) fn validate_bytes(&self, what: &str) -> Result<(), PolicyRefusal> {
        if self.encoding != ArrayEncoding::Bytes || self.length == 0 || self.elements != 0 {
            return Err(PolicyRefusal::invalid(format!(
                "{what} must be a non-empty opaque byte blob"
            )));
        }
        Ok(())
    }
}

/// How the episode ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CaptureCompletion {
    Terminal,
    Truncated,
    Aborted,
}

/// Where a reward's evidence came from. `self_reported` is the acting
/// agent's own claim: kept as evidence, never trained on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum RewardEvidenceSource {
    IndependentVerifier,
    Rubric,
    SelfReported,
}

/// The terminal reward and the verifier that produced it. Micros, never a
/// float, so the record digest is exact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RewardRecord {
    pub value_micros: i64,
    pub verifier_id: ResourceId,
    pub verifier_digest: Digest256,
    pub evidence: RewardEvidenceSource,
}

/// How much of the trace the capture kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CaptureTraceFidelity {
    Full,
    Redacted,
}

/// Whether a capture may be a training input, and if not, why.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CaptureEligibility {
    Eligible,
    NotTerminal,
    NoReward,
    UnverifiedReward,
}

/// `PolicyCapture/v1`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PolicyCapture {
    /// The attested capability the sampler ran under; its `capture` control
    /// must be enabled.
    pub capability_id: OpaqueId,
    /// The exact behaviour-policy version that sampled every policy token.
    pub sampler_version_id: OpaqueId,
    /// The durable `:Trajectory` in the request graph (`StartTrajectory`).
    pub trajectory_id: OpaqueId,
    /// The trajectory's step count at capture. Later appends never change
    /// what this capture describes.
    pub trajectory_steps: u32,
    pub completion: CaptureCompletion,
    pub token_count: u32,
    pub policy_token_count: u32,
    pub token_ids: HeldBlobRef,
    /// Frozen chosen-token log-probabilities, one per policy token.
    pub log_q: HeldBlobRef,
    pub action_mask: HeldBlobRef,
    #[serde(default)]
    pub sampler_top_k: Option<HeldBlobRef>,
    #[serde(default)]
    pub auxiliary_draws: Option<HeldBlobRef>,
    #[serde(default)]
    pub reward: Option<RewardRecord>,
    pub purpose: ResourceId,
    pub trace_fidelity: CaptureTraceFidelity,
    pub captured_at_ms: u64,
}

impl PolicyCapture {
    /// Dimension and completeness checks that need no stored state.
    pub fn validate(&self) -> Result<(), PolicyRefusal> {
        let policy_tokens = self.policy_token_count;
        if self.trajectory_steps == 0 || policy_tokens == 0 || policy_tokens > self.token_count {
            return Err(PolicyRefusal::invalid(
                "a capture needs steps and 0 < policy tokens <= tokens",
            ));
        }
        expect_encoding(&self.token_ids, ArrayEncoding::U32Le, "token_ids")?;
        expect_encoding(&self.log_q, ArrayEncoding::F32Le, "log_q")?;
        expect_encoding(&self.action_mask, ArrayEncoding::U8Mask, "action_mask")?;
        self.token_ids
            .validate_array("token_ids", self.token_count)?;
        self.action_mask
            .validate_array("action_mask", self.token_count)?;
        self.log_q.validate_array("log_q", policy_tokens)?;
        for (what, blob) in self.sampler_records() {
            expect_encoding(blob, ArrayEncoding::U32Le, what)?;
            blob.validate_array(what, blob.elements)?;
        }
        Ok(())
    }

    fn sampler_records(&self) -> impl Iterator<Item = (&'static str, &HeldBlobRef)> {
        [
            ("sampler_top_k", self.sampler_top_k.as_ref()),
            ("auxiliary_draws", self.auxiliary_draws.as_ref()),
        ]
        .into_iter()
        .filter_map(|(what, blob)| blob.map(|blob| (what, blob)))
    }

    /// Every Blob-CAS object the record depends on.
    pub fn held_blobs(&self) -> Vec<&HeldBlobRef> {
        let mut blobs = vec![&self.token_ids, &self.log_q, &self.action_mask];
        blobs.extend(self.sampler_records().map(|(_, blob)| blob));
        blobs
    }

    /// A capture is a trainer input only when it is terminal and carries an
    /// independently evidenced reward. Incomplete captures stay trace evidence.
    pub fn eligibility(&self) -> CaptureEligibility {
        match (&self.completion, &self.reward) {
            (CaptureCompletion::Truncated | CaptureCompletion::Aborted, _) => {
                CaptureEligibility::NotTerminal
            }
            (CaptureCompletion::Terminal, None) => CaptureEligibility::NoReward,
            (CaptureCompletion::Terminal, Some(reward)) => match reward.evidence {
                RewardEvidenceSource::SelfReported => CaptureEligibility::UnverifiedReward,
                RewardEvidenceSource::IndependentVerifier | RewardEvidenceSource::Rubric => {
                    CaptureEligibility::Eligible
                }
            },
        }
    }
}

fn expect_encoding(
    blob: &HeldBlobRef,
    encoding: ArrayEncoding,
    what: &str,
) -> Result<(), PolicyRefusal> {
    if blob.encoding != encoding {
        return Err(PolicyRefusal::invalid(format!(
            "{what} must be encoded as {encoding:?}"
        )));
    }
    Ok(())
}
