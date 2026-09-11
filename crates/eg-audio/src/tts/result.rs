use serde::{Deserialize, Serialize};

use super::audio::{OutputFormat, QualitySummary, TtsChunk};
use super::request::VoiceModelRef;
use super::{JobId, RequestId};

/// Whether a run's output is reproducible. piper-rs's inference path carries
/// no explicit random seed (source audit: `open-source-libraries/piper-rs`),
/// so this contract's only pure-module-producible value is `Unverified` — a
/// real worker must actually reproduce a run and carry the repeated digest
/// to ever report `VerifiedDeterministic`; nothing here fabricates that
/// claim.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeterminismClaim {
    Unverified,
    VerifiedDeterministic { repeated_output_digest: String },
    NonDeterministic,
}

/// `tts.status.v1` — the durable job's lifecycle state. Progress is bounded
/// and never implies completion without a durable [`TtsResult`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    Accepted,
    Queued,
    Running,
    Chunking,
    Cancelling,
    Cancelled,
    Succeeded,
    Degraded,
    Failed,
}

impl JobStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JobStatus::Cancelled | JobStatus::Succeeded | JobStatus::Degraded | JobStatus::Failed
        )
    }
}

/// `tts.result.v1` — terminal. Constructible only via [`crate::tts::finalize_result`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TtsResult {
    pub request_id: RequestId,
    pub job_id: JobId,
    pub status: JobStatus,
    pub chunks: Vec<TtsChunk>,
    pub voice: VoiceModelRef,
    pub output_format: OutputFormat,
    pub total_sample_count: u64,
    pub total_duration_ms: u64,
    pub quality: QualitySummary,
    pub deterministic: DeterminismClaim,
}
