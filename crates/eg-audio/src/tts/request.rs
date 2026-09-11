use serde::{Deserialize, Serialize};

use crate::ingress::{CarrierRef, IdempotencyKey};

use super::audio::OutputFormat;
use super::{
    is_valid_digest, ConfigArtifactId, EspeakVoice, JobId, LanguageTag, ModelArtifactId, RequestId,
    SensitiveInputRef, TtsBoundedId, TtsError, TTS_PROTOCOL_VERSION,
};

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
        let positive_limits = [
            self.max_text_bytes,
            self.max_phoneme_bytes,
            self.max_chunk_decoded_bytes,
            self.max_chunks,
            self.max_output_ms,
            self.request_deadline_ms,
        ];
        if positive_limits.contains(&0) {
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
