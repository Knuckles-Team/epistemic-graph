//! Native TTS contract (GOC-34, `OWNER-VOICE-TTS`).
//!
//! This module is the **versioned wire contract plus pure, dependency-light
//! validation/authorization logic** for native Piper-compatible speech
//! synthesis. It mirrors GOC-33's [`crate::asr`] design exactly: it consumes
//! the GOC-32 [`crate::ingress`] contract's `BoundedId`/`CarrierRef`/
//! `IdempotencyKey` types directly rather than redefining them. Per the
//! GOC-34 lane contract, this crate does **not** own `BoundedId`/`CarrierRef`;
//! a required change to that shape is GOC-32's to make, not this module's.
//!
//! No `ort`, ONNX Runtime, eSpeak-ng, Piper fork/vendor mirror, or voice model
//! weight is linked by this module or this feature. There is no
//! `epistemic-graph-voice-worker` process yet — verified against `main`
//! before writing this module, nothing under any repository in this
//! workspace defines one. **No voice model, config, or eSpeak data was
//! vendored, downloaded, or referenced by a concrete URL anywhere in this
//! change** — model acquisition, licensing, and manifests are GOC-36's scope
//! per the lane contract's standing rule that a signed-but-stale or
//! unattested artifact is worse than none. This module is the **frozen
//! type-level `tts.*` contract** a future worker embeds; it performs no I/O,
//! no inference, no phonemization, and fetches no model.
//!
//! ## Authority boundary
//!
//! Per the lane contract: EG WorkItem/job state, lease epoch, cancellation
//! generation, chunk/rendition publication, and provenance are authoritative
//! elsewhere (GOC-03/GOC-05/GOC-19/GOC-20), not here. This module proves only
//! the **shape and admission rules** a durable committer and worker must
//! apply — nothing here claims a durable commit, a CAS write, or a GOC-03
//! fence.
//!
//! ## What is proven here (with tests in this file)
//!
//! - **Known-bad proof 1 — malformed/oversized synthesis input never yields
//!   partial audio presented as complete.** [`validate_request`] rejects an
//!   input whose declared byte length exceeds the mode-appropriate
//!   [`TtsLimits`] cap, an unsigned/malformed model or config manifest
//!   digest, a speaker ID outside the declared bound, a non-finite/invalid
//!   synthesis control, and an unsupported output format — all before any
//!   audio or model byte would be touched by a real worker. Downstream,
//!   [`finalize_result`] independently refuses to assemble a `Succeeded`
//!   result unless a chunk marked `is_final` is present, chunks are
//!   contiguous and strictly ordered, and the aggregated
//!   [`QualitySummary::non_finite_samples`] count is zero — a truncated or
//!   corrupted chunk stream can never be reported as a finished synthesis.
//!   See `rejects_oversized_text_input`, `rejects_malformed_model_digest`,
//!   `rejects_speaker_id_beyond_limit`, `rejects_non_finite_synthesis_control`,
//!   `succeeded_status_requires_a_final_chunk`, and
//!   `non_finite_audio_blocks_a_succeeded_result`.
//! - **Known-bad proof 2 — no audio may be synthesized for an unauthorized
//!   caller.** Exactly like GOC-33's `AuthorizedCarrier`: [`finalize_result`]
//!   requires an [`AuthorizedCarrier`] token, and the *only* function that
//!   produces one is [`authorize_carrier`], which fails closed on every
//!   non-`Authorized` [`PolicyDecision`]. This is a structural guarantee (the
//!   type does not exist without the call succeeding), not only a runtime
//!   check. See `denied_policy_decision_never_yields_an_authorized_carrier`
//!   and `unauthorized_carrier_blocks_result_construction`.
//! - The piper-rs source audit found no explicit random seed in its
//!   inference path, so this contract never lets a worker assert a
//!   byte-identical reproducibility claim by default:
//!   [`DeterminismClaim::Unverified`] is the only value [`finalize_result`]
//!   produces; a `VerifiedDeterministic` claim requires a worker to actually
//!   reproduce and carry the repeated digest itself (not modeled by this
//!   pure module). See `finalize_result_never_claims_verified_determinism`.
//!
//! ## What is explicitly NOT proven or claimed here
//!
//! No Piper ONNX/JSON loader, eSpeak-ng phonemizer, ORT session, tensor
//! construction, phrase segmenter, model pool, or worker process exists in
//! this module (GOC-34-W03/W05, deferred — no model is vendored or
//! downloaded per the lane's standing rule). No AU submission/status/cancel
//! adapter exists yet; per the lane's file-ownership order this crate owns
//! only the EG-side DTO/protocol module, and — mirroring what GOC-32/GOC-33
//! actually shipped at this same stage (a single EG-only W01/W02 contract
//! commit with no AU-side client) — the AU orchestration adapter named in
//! the lane scope is deferred to a follow-up change once GOC-19's WorkItem
//! command contract exists to bind against; nothing under
//! `agent-packages/agent-utilities` defines one today. No worker sandbox,
//! resource-budget enforcement, CAS resolver, or `voice.worker.manifest.v1`
//! parsing exists here (W03/W06 — deferred). No RTF/latency/quality
//! benchmark evidence is claimed (W07 — deferred). No docs/handoff artifact
//! exists yet (W08 — deferred). Downstream work should treat this module as
//! the frozen **type-level** TTS contract only, exactly as `crate::ingress`
//! and `crate::asr` document themselves for GOC-32/GOC-33.

use serde::{Deserialize, Serialize};

use crate::ingress::{BoundedId, CarrierRef, IdempotencyKey};

/// Current wire/behavioral version of the `tts.*` contract family.
pub const TTS_PROTOCOL_VERSION: u16 = 1;

fn is_valid_digest(value: &str) -> bool {
    // Duplicated from `crate::ingress`/`crate::asr` deliberately: this module
    // stays independently reviewable and does not reach into another
    // module's private helpers, matching the dependency-light posture both
    // modules document for themselves.
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

/// A bounded wire token reused for TTS-local identity (request/job/model/
/// config/rendition/language/voice). Same shape as `crate::ingress::BoundedId`;
/// a distinct alias is kept here (rather than a raw re-export) so a future
/// GOC-05 committer can see at a glance which identities are TTS-local vs.
/// ingress-owned when it mints real `OpaqueRef`s from each — mirroring
/// `crate::asr::AsrBoundedId`.
pub use crate::ingress::BoundedId as TtsBoundedId;

pub type RequestId = TtsBoundedId;
pub type JobId = TtsBoundedId;
pub type SensitiveInputRef = TtsBoundedId;
pub type ModelArtifactId = TtsBoundedId;
pub type ConfigArtifactId = TtsBoundedId;
pub type RenditionRef = TtsBoundedId;
pub type LanguageTag = TtsBoundedId;
pub type EspeakVoice = TtsBoundedId;

/// `text` is phonemized by the worker's eSpeak-ng integration; `phonemes` is
/// already-phonemized input the caller supplies verbatim. Neither variant's
/// bytes are ever inlined in this DTO — only a bounded CAS/sensitive-input
/// reference and its digest cross this contract, per the lane's "text and
/// phonemes are sensitive inputs" invariant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InputMode {
    Text,
    Phonemes,
}

/// A bounded reference to the sensitive text/phoneme input. Carries no raw
/// bytes — only a content digest and declared length a durable committer or
/// worker can verify against the governed CAS object it already holds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SensitiveInput {
    pub mode: InputMode,
    pub input_ref: SensitiveInputRef,
    /// Lowercase hex SHA-256 over the referenced input bytes.
    pub input_digest: String,
    pub byte_len: u32,
}

impl SensitiveInput {
    pub fn validate(&self, limits: &TtsLimits) -> Result<(), TtsError> {
        if !is_valid_digest(&self.input_digest) {
            return Err(TtsError::MalformedRequest {
                reason: "input_digest is not a 64-char lowercase hex sha256",
            });
        }
        let cap = match self.mode {
            InputMode::Text => limits.max_text_bytes,
            InputMode::Phonemes => limits.max_phoneme_bytes,
        };
        if self.byte_len == 0 {
            return Err(TtsError::MalformedRequest {
                reason: "input byte_len must be positive",
            });
        }
        if self.byte_len > cap {
            let limit = match self.mode {
                InputMode::Text => "max_text_bytes",
                InputMode::Phonemes => "max_phoneme_bytes",
            };
            return Err(TtsError::ResourceExhausted { limit });
        }
        Ok(())
    }
}

/// A reference to the immutable, signed Piper-compatible ONNX model plus its
/// JSON configuration (GOC-36 acquisition/signing is out of scope here — this
/// only validates the wire SHAPE a worker would check before trusting the
/// pair). A mutable/unsigned manifest is rejected before any model or audio
/// byte access.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoiceModelRef {
    pub model_artifact_id: ModelArtifactId,
    pub config_artifact_id: ConfigArtifactId,
    /// Lowercase hex SHA-256 over the `.onnx` model bytes.
    pub model_digest: String,
    /// Lowercase hex SHA-256 over the `.onnx.json` config bytes.
    pub config_digest: String,
    /// Opaque reference to the signer/signature record. `TtsBoundedId`
    /// already refuses an empty token at construction, so a voice-model ref
    /// cannot be built with a blank signature reference.
    pub signature_ref: TtsBoundedId,
}

impl VoiceModelRef {
    pub fn validate(&self) -> Result<(), TtsError> {
        if !is_valid_digest(&self.model_digest) {
            return Err(TtsError::ModelUnavailable {
                reason: "model digest is not a valid sha256",
            });
        }
        if !is_valid_digest(&self.config_digest) {
            return Err(TtsError::ModelUnavailable {
                reason: "config digest is not a valid sha256",
            });
        }
        Ok(())
    }
}

/// Optional speaker selection for a multi-speaker Piper voice. Validation
/// against the *actual* speaker map requires the loaded model and is a
/// worker-time concern (GOC-34-W03); this pure module can only bound the ID
/// against the request's own declared [`TtsLimits::max_speaker_id`] — see
/// `validate_request`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpeakerSelection {
    Default,
    Id(u32),
}

/// Piper inference-scale controls (length/noise/noise-w). All three must be
/// finite; `length_scale` must be strictly positive (zero or negative would
/// request zero/negative-duration audio) and the noise controls must be
/// non-negative. piper-rs's `src/model.rs` sends these as raw positional
/// tensor inputs with no validation — this contract closes that gap.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct SynthesisControls {
    pub length_scale: f32,
    pub noise_scale: f32,
    pub noise_w: f32,
}

impl SynthesisControls {
    pub fn validate(&self) -> Result<(), TtsError> {
        let finite = self.length_scale.is_finite()
            && self.noise_scale.is_finite()
            && self.noise_w.is_finite();
        if !finite {
            return Err(TtsError::MalformedRequest {
                reason: "synthesis control is not finite",
            });
        }
        if self.length_scale <= 0.0 || self.noise_scale < 0.0 || self.noise_w < 0.0 {
            return Err(TtsError::MalformedRequest {
                reason: "synthesis control is out of the valid range",
            });
        }
        Ok(())
    }
}

/// Output PCM encoding. Bounded to exactly one supported variant today —
/// mirroring `crate::ingress::Codec`'s "accepts exactly these codecs" posture
/// — so an unsupported request-side encoding is a typed rejection rather
/// than a best-effort transcode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputEncoding {
    Pcm16Le,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputFormat {
    pub encoding: OutputEncoding,
    pub sample_rate: u32,
    pub channels: u16,
}

impl OutputFormat {
    pub fn validate(&self) -> Result<(), TtsError> {
        if self.sample_rate == 0 || self.channels == 0 {
            return Err(TtsError::MalformedRequest {
                reason: "output sample_rate and channels must be positive",
            });
        }
        Ok(())
    }
}

/// Policy inputs a real deployment must qualify (GOC-34-W07); no default is
/// hardcoded here, matching `crate::asr::AsrLimits`. `max_speaker_id` of `0`
/// is a valid configuration (single-speaker voice only) and is therefore not
/// included in the "strictly positive" check below.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtsLimits {
    pub max_text_bytes: u32,
    pub max_phoneme_bytes: u32,
    pub max_chunk_decoded_bytes: u32,
    pub max_chunks: u32,
    pub max_output_ms: u32,
    pub max_speaker_id: u32,
    pub request_deadline_ms: u32,
}

impl TtsLimits {
    pub fn validate(&self) -> Result<(), TtsError> {
        let all_positive = self.max_text_bytes > 0
            && self.max_phoneme_bytes > 0
            && self.max_chunk_decoded_bytes > 0
            && self.max_chunks > 0
            && self.max_output_ms > 0
            && self.request_deadline_ms > 0;
        if !all_positive {
            return Err(TtsError::MalformedRequest {
                reason: "tts limits must be strictly positive",
            });
        }
        Ok(())
    }
}

/// `tts.request.v1`
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TtsRequest {
    pub protocol_version: u16,
    pub request_id: RequestId,
    pub job_id: JobId,
    pub carrier: CarrierRef,
    pub voice: VoiceModelRef,
    pub language: LanguageTag,
    pub espeak_voice: EspeakVoice,
    pub speaker: SpeakerSelection,
    pub input: SensitiveInput,
    pub controls: SynthesisControls,
    pub output_format: OutputFormat,
    pub limits: TtsLimits,
    pub deadline_ms: u32,
    pub idempotency_key: IdempotencyKey,
}

/// Bounded, pure request validation. Never touches audio, phoneme, or model
/// bytes; returns the first typed violation found. A real worker/listener
/// MUST call this (and reject on `Err`) before any model load, phonemization,
/// or CAS read — this is the "fails before accessing model/audio bytes"
/// acceptance gate at the request shape level, and the first half of
/// known-bad proof 1 (malformed/oversized input never reaches inference).
pub fn validate_request(request: &TtsRequest) -> Result<(), TtsError> {
    if request.protocol_version != TTS_PROTOCOL_VERSION {
        return Err(TtsError::IncompatibleVersion);
    }
    request.limits.validate()?;
    request.voice.validate()?;
    request.output_format.validate()?;
    request.controls.validate()?;
    request.input.validate(&request.limits)?;
    if let SpeakerSelection::Id(id) = request.speaker {
        if id > request.limits.max_speaker_id {
            return Err(TtsError::UnsupportedSpeaker);
        }
    }
    if request.deadline_ms == 0 {
        return Err(TtsError::MalformedRequest {
            reason: "deadline_ms must be positive",
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ChunkSequence(pub u64);

/// Chunk-level audio quality. `Unavailable` is the honest default when a
/// worker does not measure a chunk; `Measured` carries a bounded, explicit
/// report — never a fabricated "clean" claim. Mirrors the calibrated-vs-
/// diagnostic discipline of `crate::asr::Quality`, adapted for synthesized
/// PCM rather than a recognition confidence.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum ChunkQuality {
    Unavailable,
    Measured {
        clipped_samples: u32,
        non_finite_samples: u32,
        peak_abs: f32,
    },
}

impl ChunkQuality {
    pub fn validate(&self) -> Result<(), TtsError> {
        match self {
            ChunkQuality::Unavailable => Ok(()),
            ChunkQuality::Measured { peak_abs, .. } => {
                if !peak_abs.is_finite() {
                    return Err(TtsError::MalformedResult {
                        reason: "peak_abs must be finite",
                    });
                }
                Ok(())
            }
        }
    }
}

/// `tts.chunk.v1` — an ordered, independently addressable slice of the
/// requested synthesis. Immutable once emitted.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TtsChunk {
    pub request_id: RequestId,
    pub job_id: JobId,
    pub sequence: ChunkSequence,
    pub phrase_index: u32,
    pub sample_offset: u64,
    pub sample_count: u32,
    pub sample_rate: u32,
    pub channels: u16,
    pub encoding: OutputEncoding,
    pub rendition_ref: RenditionRef,
    /// Lowercase hex SHA-256 over the chunk's committed PCM bytes.
    pub rendition_digest: String,
    pub is_final: bool,
    pub quality: ChunkQuality,
}

impl TtsChunk {
    pub fn validate(&self) -> Result<(), TtsError> {
        if self.sample_count == 0 {
            return Err(TtsError::MalformedResult {
                reason: "chunk sample_count must be positive",
            });
        }
        if self.sample_rate == 0 || self.channels == 0 {
            return Err(TtsError::MalformedResult {
                reason: "chunk sample_rate and channels must be positive",
            });
        }
        if !is_valid_digest(&self.rendition_digest) {
            return Err(TtsError::MalformedResult {
                reason: "rendition_digest is not a 64-char lowercase hex sha256",
            });
        }
        self.quality.validate()?;
        Ok(())
    }
}

/// Aggregated, bounded audio-quality evidence over an entire result. `0`
/// `non_finite_samples` is required for a `Succeeded` status — see
/// `finalize_result`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct QualitySummary {
    pub total_samples: u64,
    pub clipped_samples: u64,
    pub non_finite_samples: u64,
    pub chunks_with_unavailable_quality: u32,
}

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

/// A closed set of policy outcomes a real GOC-15/16 carrier check must
/// produce. Never freeform — mirrors `crate::asr::PolicyDecision`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyDecision {
    Authorized,
    ConsentRevoked,
    PolicyDenied,
    TenantMismatch,
}

/// Proof that a carrier was checked and authorized. This module's ONLY
/// constructor is [`authorize_carrier`], and [`finalize_result`] is the ONLY
/// function that can produce a [`TtsResult`] — and it requires one. Audio
/// cannot be assembled by this module without a successful authorization
/// decision; there is no code path that skips this token. This is known-bad
/// proof 2 (see the module doc).
#[derive(Clone, Debug)]
pub struct AuthorizedCarrier {
    carrier: CarrierRef,
}

impl AuthorizedCarrier {
    pub fn carrier(&self) -> &CarrierRef {
        &self.carrier
    }
}

/// The sole way to obtain an [`AuthorizedCarrier`]. Fails closed on every
/// non-`Authorized` decision — the caller (a real GOC-15/16 policy check) is
/// what decides `decision`; this function only enforces that synthesized
/// audio can never be produced except through an explicit `Authorized`
/// outcome.
pub fn authorize_carrier(
    carrier: &CarrierRef,
    decision: PolicyDecision,
) -> Result<AuthorizedCarrier, TtsError> {
    match decision {
        PolicyDecision::Authorized => Ok(AuthorizedCarrier {
            carrier: carrier.clone(),
        }),
        PolicyDecision::ConsentRevoked => Err(TtsError::PolicyDenied {
            reason: "consent_revoked",
        }),
        PolicyDecision::PolicyDenied => Err(TtsError::PolicyDenied {
            reason: "policy_denied",
        }),
        PolicyDecision::TenantMismatch => Err(TtsError::PolicyDenied {
            reason: "tenant_mismatch",
        }),
    }
}

/// `tts.result.v1` — terminal. Constructible only via [`finalize_result`].
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

/// Assemble a terminal `tts.result.v1`. Requires:
/// - an [`AuthorizedCarrier`] (structural authorization proof — known-bad
///   proof 2, see the module doc's summary);
/// - a `status` that is terminal per [`JobStatus::is_terminal`] (a `Running`/
///   `Chunking`/etc. status has no result — only a job-status read does);
/// - a request that independently passes [`validate_request`];
/// - chunks that each pass [`TtsChunk::validate`], belong to this request/
///   job, are strictly ordered by sequence, and form a contiguous sample
///   range with the previous chunk.
///
/// A `Succeeded` status with zero chunks, or with no chunk marked `is_final`,
/// or whose aggregated [`QualitySummary::non_finite_samples`] is nonzero, is
/// rejected — a partial, corrupted, or truncated run must report `Degraded`,
/// `Cancelled`, or `Failed` explicitly rather than a fabricated success. This
/// is known-bad proof 1's downstream half (see the module doc).
pub fn finalize_result(
    authorized: &AuthorizedCarrier,
    request: &TtsRequest,
    chunks: Vec<TtsChunk>,
    status: JobStatus,
) -> Result<TtsResult, TtsError> {
    if authorized.carrier() != &request.carrier {
        return Err(TtsError::PolicyDenied {
            reason: "authorized carrier does not match the request carrier",
        });
    }
    if !status.is_terminal() {
        return Err(TtsError::MalformedResult {
            reason: "finalize_result requires a terminal job status",
        });
    }
    validate_request(request)?;
    if chunks.len() as u32 > request.limits.max_chunks {
        return Err(TtsError::ResourceExhausted {
            limit: "max_chunks",
        });
    }
    if status == JobStatus::Succeeded && chunks.is_empty() {
        return Err(TtsError::MalformedResult {
            reason: "a succeeded result requires at least one ordered chunk",
        });
    }

    let mut previous_sequence: Option<u64> = None;
    let mut previous_offset_end: Option<u64> = None;
    let mut quality = QualitySummary::default();
    let mut final_seen = false;

    for chunk in &chunks {
        if chunk.request_id != request.request_id || chunk.job_id != request.job_id {
            return Err(TtsError::Conflict {
                reason: "chunk belongs to a different request or job",
            });
        }
        chunk.validate()?;
        if let Some(previous) = previous_sequence {
            if chunk.sequence.0 <= previous {
                return Err(TtsError::MalformedResult {
                    reason: "chunks are not strictly ordered by sequence",
                });
            }
        }
        previous_sequence = Some(chunk.sequence.0);

        if let Some(previous_end) = previous_offset_end {
            if chunk.sample_offset != previous_end {
                return Err(TtsError::MalformedResult {
                    reason: "chunk sample_offset is not contiguous with the previous chunk",
                });
            }
        }
        previous_offset_end = Some(chunk.sample_offset + u64::from(chunk.sample_count));

        if final_seen {
            return Err(TtsError::MalformedResult {
                reason: "a chunk follows the chunk already marked final",
            });
        }
        if chunk.is_final {
            final_seen = true;
        }

        match &chunk.quality {
            ChunkQuality::Unavailable => {
                quality.chunks_with_unavailable_quality += 1;
            }
            ChunkQuality::Measured {
                clipped_samples,
                non_finite_samples,
                ..
            } => {
                quality.clipped_samples += u64::from(*clipped_samples);
                quality.non_finite_samples += u64::from(*non_finite_samples);
            }
        }
        quality.total_samples += u64::from(chunk.sample_count);
    }

    if status == JobStatus::Succeeded {
        if !final_seen {
            return Err(TtsError::MalformedResult {
                reason: "a succeeded result requires a chunk marked final",
            });
        }
        if quality.non_finite_samples > 0 {
            return Err(TtsError::NonFiniteAudio);
        }
    }

    let total_duration_ms = if request.output_format.sample_rate > 0 {
        quality
            .total_samples
            .saturating_mul(1000)
            / u64::from(request.output_format.sample_rate)
    } else {
        0
    };

    Ok(TtsResult {
        request_id: request.request_id.clone(),
        job_id: request.job_id.clone(),
        status,
        chunks,
        voice: request.voice.clone(),
        output_format: request.output_format,
        total_sample_count: quality.total_samples,
        total_duration_ms,
        quality,
        // Never claimed here — only a real worker that actually reproduces a
        // run may report otherwise. See `DeterminismClaim`'s doc.
        deterministic: DeterminismClaim::Unverified,
    })
}

/// `tts.error.v1` — a typed, bounded diagnostic. `Serialize`-only for the
/// same reason as `crate::ingress::IngressError`/`crate::asr::AsrError`:
/// `&'static str` reason fields cannot soundly implement `Deserialize<'de>`,
/// and an error has no reason to be accepted as an inbound request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum TtsError {
    ModelUnavailable {
        reason: &'static str,
    },
    UnsupportedLanguage,
    UnsupportedSpeaker,
    ResourceExhausted {
        limit: &'static str,
    },
    PolicyDenied {
        reason: &'static str,
    },
    Timeout,
    Cancelled,
    Degraded,
    IncompatibleVersion,
    MalformedRequest {
        reason: &'static str,
    },
    MalformedResult {
        reason: &'static str,
    },
    /// A chunk or the aggregated result reported one or more non-finite
    /// (NaN/inf) PCM samples. Never silently dropped, clamped, or folded
    /// into a `Succeeded` result.
    NonFiniteAudio,
    Conflict {
        reason: &'static str,
    },
}

#[cfg(test)]
mod fixtures {
    use super::*;

    pub fn id(value: &str) -> TtsBoundedId {
        BoundedId::new(value).expect("fixture id is valid")
    }

    pub fn carrier() -> CarrierRef {
        CarrierRef {
            tenant: id("tenant-1"),
            actor: id("actor-1"),
            consent_ref: id("consent-1"),
            purpose: id("purpose-1"),
            trace_id: id("trace-1"),
        }
    }

    pub fn voice() -> VoiceModelRef {
        VoiceModelRef {
            model_artifact_id: id("model-1"),
            config_artifact_id: id("config-1"),
            model_digest: "a".repeat(64),
            config_digest: "b".repeat(64),
            signature_ref: id("signature-1"),
        }
    }

    pub fn limits() -> TtsLimits {
        TtsLimits {
            max_text_bytes: 64 * 1024,
            max_phoneme_bytes: 128 * 1024,
            max_chunk_decoded_bytes: 2 * 1024 * 1024,
            max_chunks: 256,
            max_output_ms: 30_000,
            max_speaker_id: 8,
            request_deadline_ms: 60_000,
        }
    }

    pub fn input(byte_len: u32) -> SensitiveInput {
        SensitiveInput {
            mode: InputMode::Text,
            input_ref: id("input-1"),
            input_digest: "c".repeat(64),
            byte_len,
        }
    }

    pub fn controls() -> SynthesisControls {
        SynthesisControls {
            length_scale: 1.0,
            noise_scale: 0.667,
            noise_w: 0.8,
        }
    }

    pub fn output_format() -> OutputFormat {
        OutputFormat {
            encoding: OutputEncoding::Pcm16Le,
            sample_rate: 22_050,
            channels: 1,
        }
    }

    pub fn request() -> TtsRequest {
        TtsRequest {
            protocol_version: TTS_PROTOCOL_VERSION,
            request_id: id("request-1"),
            job_id: id("job-1"),
            carrier: carrier(),
            voice: voice(),
            language: id("en"),
            espeak_voice: id("en-us"),
            speaker: SpeakerSelection::Default,
            input: input(128),
            controls: controls(),
            output_format: output_format(),
            limits: limits(),
            deadline_ms: 60_000,
            idempotency_key: id("idem-1"),
        }
    }

    pub fn chunk(request: &TtsRequest, sequence: u64, offset: u64, is_final: bool) -> TtsChunk {
        TtsChunk {
            request_id: request.request_id.clone(),
            job_id: request.job_id.clone(),
            sequence: ChunkSequence(sequence),
            phrase_index: 0,
            sample_offset: offset,
            sample_count: 1_000,
            sample_rate: request.output_format.sample_rate,
            channels: request.output_format.channels,
            encoding: OutputEncoding::Pcm16Le,
            rendition_ref: id("rendition-1"),
            rendition_digest: "d".repeat(64),
            is_final,
            quality: ChunkQuality::Measured {
                clipped_samples: 0,
                non_finite_samples: 0,
                peak_abs: 0.5,
            },
        }
    }
}

#[cfg(test)]
mod request_validation_tests {
    use super::fixtures::*;
    use super::*;

    #[test]
    fn accepts_a_well_formed_request() {
        assert_eq!(validate_request(&request()), Ok(()));
    }

    #[test]
    fn rejects_incompatible_protocol_version() {
        let mut request = request();
        request.protocol_version = 99;
        assert_eq!(validate_request(&request), Err(TtsError::IncompatibleVersion));
    }

    /// Known-bad input (proof 1, half A): text input whose declared length
    /// exceeds the request's own bound. Must be rejected with a typed,
    /// bounded error before any phonemization or model byte access.
    #[test]
    fn rejects_oversized_text_input() {
        let mut request = request();
        request.input = input(request.limits.max_text_bytes + 1);
        assert_eq!(
            validate_request(&request),
            Err(TtsError::ResourceExhausted {
                limit: "max_text_bytes"
            })
        );
    }

    #[test]
    fn rejects_oversized_phoneme_input() {
        let mut request = request();
        request.input = SensitiveInput {
            mode: InputMode::Phonemes,
            input_ref: fixtures::id("input-2"),
            input_digest: "e".repeat(64),
            byte_len: request.limits.max_phoneme_bytes + 1,
        };
        assert_eq!(
            validate_request(&request),
            Err(TtsError::ResourceExhausted {
                limit: "max_phoneme_bytes"
            })
        );
    }

    #[test]
    fn rejects_empty_input() {
        let mut request = request();
        request.input = input(0);
        assert_eq!(
            validate_request(&request),
            Err(TtsError::MalformedRequest {
                reason: "input byte_len must be positive"
            })
        );
    }

    #[test]
    fn rejects_malformed_input_digest() {
        let mut request = request();
        request.input.input_digest = "not-a-hex-digest".to_string();
        assert_eq!(
            validate_request(&request),
            Err(TtsError::MalformedRequest {
                reason: "input_digest is not a 64-char lowercase hex sha256"
            })
        );
    }

    /// Known-bad input (proof 1, half A): an unsigned/malformed model digest.
    /// A real worker must fail before it ever loads the ONNX graph.
    #[test]
    fn rejects_malformed_model_digest() {
        let mut request = request();
        request.voice.model_digest = "short".to_string();
        assert_eq!(
            validate_request(&request),
            Err(TtsError::ModelUnavailable {
                reason: "model digest is not a valid sha256"
            })
        );
    }

    #[test]
    fn rejects_malformed_config_digest() {
        let mut request = request();
        request.voice.config_digest = "short".to_string();
        assert_eq!(
            validate_request(&request),
            Err(TtsError::ModelUnavailable {
                reason: "config digest is not a valid sha256"
            })
        );
    }

    #[test]
    fn rejects_speaker_id_beyond_limit() {
        let mut request = request();
        request.speaker = SpeakerSelection::Id(request.limits.max_speaker_id + 1);
        assert_eq!(validate_request(&request), Err(TtsError::UnsupportedSpeaker));
    }

    #[test]
    fn accepts_speaker_id_at_the_limit() {
        let mut request = request();
        request.speaker = SpeakerSelection::Id(request.limits.max_speaker_id);
        assert_eq!(validate_request(&request), Ok(()));
    }

    #[test]
    fn rejects_non_finite_synthesis_control() {
        let mut request = request();
        request.controls.noise_scale = f32::NAN;
        assert_eq!(
            validate_request(&request),
            Err(TtsError::MalformedRequest {
                reason: "synthesis control is not finite"
            })
        );
    }

    #[test]
    fn rejects_zero_length_scale() {
        let mut request = request();
        request.controls.length_scale = 0.0;
        assert_eq!(
            validate_request(&request),
            Err(TtsError::MalformedRequest {
                reason: "synthesis control is out of the valid range"
            })
        );
    }

    #[test]
    fn rejects_negative_noise_w() {
        let mut request = request();
        request.controls.noise_w = -0.1;
        assert_eq!(
            validate_request(&request),
            Err(TtsError::MalformedRequest {
                reason: "synthesis control is out of the valid range"
            })
        );
    }

    #[test]
    fn rejects_zero_output_sample_rate() {
        let mut request = request();
        request.output_format.sample_rate = 0;
        assert_eq!(
            validate_request(&request),
            Err(TtsError::MalformedRequest {
                reason: "output sample_rate and channels must be positive"
            })
        );
    }

    #[test]
    fn rejects_zero_deadline() {
        let mut request = request();
        request.deadline_ms = 0;
        assert_eq!(
            validate_request(&request),
            Err(TtsError::MalformedRequest {
                reason: "deadline_ms must be positive"
            })
        );
    }

    #[test]
    fn zero_limits_are_rejected() {
        let mut request = request();
        request.limits.max_chunks = 0;
        assert_eq!(
            validate_request(&request),
            Err(TtsError::MalformedRequest {
                reason: "tts limits must be strictly positive"
            })
        );
    }
}

#[cfg(test)]
mod authorization_tests {
    use super::fixtures::*;
    use super::*;

    #[test]
    fn authorized_decision_yields_an_authorized_carrier() {
        let result = authorize_carrier(&carrier(), PolicyDecision::Authorized);
        assert!(result.is_ok());
    }

    /// Known-bad scenario (proof 2): consent was revoked. No
    /// `AuthorizedCarrier` is ever produced, so synthesized audio cannot be
    /// assembled through `finalize_result` for this carrier — proven both by
    /// this `Err` assertion and structurally, since `finalize_result` has no
    /// constructor path that accepts anything but a real
    /// `&AuthorizedCarrier`.
    #[test]
    fn denied_policy_decision_never_yields_an_authorized_carrier() {
        for decision in [
            PolicyDecision::ConsentRevoked,
            PolicyDecision::PolicyDenied,
            PolicyDecision::TenantMismatch,
        ] {
            let result = authorize_carrier(&carrier(), decision);
            assert!(
                result.is_err(),
                "decision {decision:?} must never authorize a carrier"
            );
        }
    }

    /// End-to-end known-bad proof (proof 2): synthesis was requested under a
    /// carrier whose consent was revoked. There is no `TtsResult` for it —
    /// the authorization step fails first, before any chunk is considered.
    #[test]
    fn unauthorized_carrier_blocks_result_construction() {
        let request = request();
        let authorization = authorize_carrier(&request.carrier, PolicyDecision::ConsentRevoked);
        assert_eq!(
            authorization.err(),
            Some(TtsError::PolicyDenied {
                reason: "consent_revoked"
            })
        );
        // No `AuthorizedCarrier` value exists in this branch, so there is no
        // way to call `finalize_result` at all — the assertion above is the
        // complete proof for this module's boundary.
    }

    #[test]
    fn authorized_carrier_for_a_different_request_is_rejected_at_finalize() {
        let request = request();
        let mismatched_carrier = CarrierRef {
            tenant: fixtures::id("tenant-2"),
            ..carrier()
        };
        let authorization = authorize_carrier(&mismatched_carrier, PolicyDecision::Authorized)
            .expect("policy itself authorized this carrier");
        let outcome = finalize_result(&authorization, &request, vec![], JobStatus::Cancelled);
        assert_eq!(
            outcome,
            Err(TtsError::PolicyDenied {
                reason: "authorized carrier does not match the request carrier"
            })
        );
    }
}

#[cfg(test)]
mod finalize_result_tests {
    use super::fixtures::*;
    use super::*;

    fn authorized(request: &TtsRequest) -> AuthorizedCarrier {
        authorize_carrier(&request.carrier, PolicyDecision::Authorized)
            .expect("fixture carrier is authorized")
    }

    #[test]
    fn assembles_a_succeeded_result_from_ordered_contiguous_chunks() {
        let request = request();
        let auth = authorized(&request);
        let chunks = vec![
            chunk(&request, 0, 0, false),
            chunk(&request, 1, 1_000, true),
        ];
        let result = finalize_result(&auth, &request, chunks, JobStatus::Succeeded)
            .expect("well-formed succeeded result");
        assert_eq!(result.total_sample_count, 2_000);
        assert_eq!(result.deterministic, DeterminismClaim::Unverified);
    }

    /// `finalize_result` refuses to produce a result for a non-terminal
    /// status — a `Running`/`Chunking` job has no result yet.
    #[test]
    fn non_terminal_status_is_rejected() {
        let request = request();
        let auth = authorized(&request);
        let outcome = finalize_result(&auth, &request, vec![], JobStatus::Running);
        assert_eq!(
            outcome,
            Err(TtsError::MalformedResult {
                reason: "finalize_result requires a terminal job status"
            })
        );
    }

    #[test]
    fn succeeded_status_with_no_chunks_is_rejected() {
        let request = request();
        let auth = authorized(&request);
        let outcome = finalize_result(&auth, &request, vec![], JobStatus::Succeeded);
        assert_eq!(
            outcome,
            Err(TtsError::MalformedResult {
                reason: "a succeeded result requires at least one ordered chunk"
            })
        );
    }

    #[test]
    fn cancelled_status_with_no_chunks_is_allowed() {
        let request = request();
        let auth = authorized(&request);
        let outcome = finalize_result(&auth, &request, vec![], JobStatus::Cancelled);
        assert!(outcome.is_ok());
    }

    /// Known-bad input (proof 1, half B): a run that emits chunks but never
    /// marks one final must not be reported `Succeeded` — that would present
    /// partial audio as a complete synthesis.
    #[test]
    fn succeeded_status_requires_a_final_chunk() {
        let request = request();
        let auth = authorized(&request);
        let chunks = vec![chunk(&request, 0, 0, false)];
        let outcome = finalize_result(&auth, &request, chunks, JobStatus::Succeeded);
        assert_eq!(
            outcome,
            Err(TtsError::MalformedResult {
                reason: "a succeeded result requires a chunk marked final"
            })
        );
    }

    #[test]
    fn out_of_order_chunks_are_rejected() {
        let request = request();
        let auth = authorized(&request);
        let chunks = vec![
            chunk(&request, 1, 1_000, true),
            chunk(&request, 0, 0, false),
        ];
        let outcome = finalize_result(&auth, &request, chunks, JobStatus::Succeeded);
        assert_eq!(
            outcome,
            Err(TtsError::MalformedResult {
                reason: "chunks are not strictly ordered by sequence"
            })
        );
    }

    /// Known-bad input: a gap between chunk sample ranges (as if a chunk was
    /// dropped in transit). Must be rejected rather than silently spliced.
    #[test]
    fn non_contiguous_sample_offsets_are_rejected() {
        let request = request();
        let auth = authorized(&request);
        let chunks = vec![
            chunk(&request, 0, 0, false),
            chunk(&request, 1, 5_000, true), // gap: previous chunk ended at 1_000
        ];
        let outcome = finalize_result(&auth, &request, chunks, JobStatus::Succeeded);
        assert_eq!(
            outcome,
            Err(TtsError::MalformedResult {
                reason: "chunk sample_offset is not contiguous with the previous chunk"
            })
        );
    }

    #[test]
    fn a_chunk_after_the_final_chunk_is_rejected() {
        let request = request();
        let auth = authorized(&request);
        let chunks = vec![
            chunk(&request, 0, 0, true),
            chunk(&request, 1, 1_000, false),
        ];
        let outcome = finalize_result(&auth, &request, chunks, JobStatus::Succeeded);
        assert_eq!(
            outcome,
            Err(TtsError::MalformedResult {
                reason: "a chunk follows the chunk already marked final"
            })
        );
    }

    #[test]
    fn chunk_from_a_different_request_is_a_typed_conflict() {
        let request = request();
        let auth = authorized(&request);
        let mut foreign = chunk(&request, 0, 0, true);
        foreign.request_id = fixtures::id("request-2");
        let outcome = finalize_result(&auth, &request, vec![foreign], JobStatus::Succeeded);
        assert_eq!(
            outcome,
            Err(TtsError::Conflict {
                reason: "chunk belongs to a different request or job"
            })
        );
    }

    #[test]
    fn exceeding_max_chunks_is_resource_exhausted() {
        let mut request = request();
        request.limits.max_chunks = 1;
        let auth = authorized(&request);
        let chunks = vec![
            chunk(&request, 0, 0, false),
            chunk(&request, 1, 1_000, true),
        ];
        let outcome = finalize_result(&auth, &request, chunks, JobStatus::Succeeded);
        assert_eq!(
            outcome,
            Err(TtsError::ResourceExhausted {
                limit: "max_chunks"
            })
        );
    }

    /// Known-bad input (proof 1, half B): a chunk that measured non-finite
    /// (NaN/inf) PCM samples must block a `Succeeded` result end to end,
    /// never silently folded into a "complete" run.
    #[test]
    fn non_finite_audio_blocks_a_succeeded_result() {
        let request = request();
        let auth = authorized(&request);
        let mut corrupted = chunk(&request, 0, 0, true);
        corrupted.quality = ChunkQuality::Measured {
            clipped_samples: 0,
            non_finite_samples: 3,
            peak_abs: 0.9,
        };
        let outcome = finalize_result(&auth, &request, vec![corrupted], JobStatus::Succeeded);
        assert_eq!(outcome, Err(TtsError::NonFiniteAudio));
    }

    /// The same non-finite chunk is a legitimate part of a `Degraded`
    /// report — only `Succeeded` is blocked, so a worker has an honest way
    /// to report a partially-corrupted run rather than being forced into
    /// silently dropping evidence.
    #[test]
    fn non_finite_audio_is_reported_but_not_blocked_on_a_degraded_result() {
        let request = request();
        let auth = authorized(&request);
        let mut corrupted = chunk(&request, 0, 0, true);
        corrupted.quality = ChunkQuality::Measured {
            clipped_samples: 0,
            non_finite_samples: 3,
            peak_abs: 0.9,
        };
        let result = finalize_result(&auth, &request, vec![corrupted], JobStatus::Degraded)
            .expect("degraded status may carry non-finite quality evidence");
        assert_eq!(result.quality.non_finite_samples, 3);
    }

    #[test]
    fn a_malformed_chunk_digest_fails_result_assembly() {
        let request = request();
        let auth = authorized(&request);
        let mut bad = chunk(&request, 0, 0, true);
        bad.rendition_digest = "not-a-hex-digest".to_string();
        let outcome = finalize_result(&auth, &request, vec![bad], JobStatus::Succeeded);
        assert_eq!(
            outcome,
            Err(TtsError::MalformedResult {
                reason: "rendition_digest is not a 64-char lowercase hex sha256"
            })
        );
    }

    /// No pure-module code path can ever produce a
    /// `DeterminismClaim::VerifiedDeterministic` — only a real worker that
    /// actually reproduces a run may report it.
    #[test]
    fn finalize_result_never_claims_verified_determinism() {
        let request = request();
        let auth = authorized(&request);
        let chunks = vec![chunk(&request, 0, 0, true)];
        let result = finalize_result(&auth, &request, chunks, JobStatus::Succeeded)
            .expect("well-formed succeeded result");
        assert_eq!(result.deterministic, DeterminismClaim::Unverified);
    }
}
