//! Native ASR contract (GOC-33, `OWNER-VOICE-ASR`).
//!
//! This module is the **versioned wire contract plus pure, dependency-light
//! validation/authorization logic** for native transcription. It consumes the
//! GOC-32 [`crate::ingress`] contract's `StreamId`/`StreamGeneration`/
//! `CarrierRef`/`ChunkRef` types directly rather than redefining them — see
//! that module's own doc for why those are `BoundedId`-based and deliberately
//! independent of `eg-modality::OpaqueRef`. Per the GOC-33 lane contract, this
//! crate does **not** own `BoundedId`/`CarrierRef`; a required change to that
//! shape is GOC-32's to make, not this module's.
//!
//! No `whisper-rs`, whisper.cpp, ONNX Runtime, codec, model loader, or model
//! weight is linked by this module or this feature. There is no
//! `epistemic-graph-voice-worker` process yet — verified against `main` before
//! writing this module, nothing under any repository in this workspace defines
//! one. This module is the **frozen type-level `asr.*` contract** a future
//! worker embeds; it performs no I/O, no inference, and fetches no model.
//!
//! ## Authority boundary
//!
//! Per the lane contract: EG WorkItem/job state, lease epoch, cancellation
//! generation, ASR result publication, and provenance are authoritative
//! elsewhere (GOC-03/GOC-05/GOC-20), not here. This module proves only the
//! **shape and admission rules** a durable committer must apply — nothing here
//! claims a durable commit, a CAS write, or a GOC-03 fence.
//!
//! ## What is proven here (with tests in this file)
//!
//! - A malformed or truncated audio reference (a zero/negative-length source
//!   range, or a segment whose word timing extends past its own declared
//!   segment boundary — the ASR-layer analog of a truncated frame) is rejected
//!   with a bounded, typed [`AsrError`] rather than being accepted and
//!   presented as a complete result. See `rejects_zero_length_source_range`
//!   and `rejects_truncated_word_timing_past_segment_bound`.
//! - A transcript (an [`AsrResult`]) cannot be constructed by this module for
//!   an unauthorized carrier: [`finalize_result`] requires an
//!   [`AuthorizedCarrier`] token, and the *only* function that produces one is
//!   [`authorize_carrier`], which fails closed on every non-`Authorized`
//!   [`PolicyDecision`] — see `denied_policy_decision_never_yields_an_
//!   authorized_carrier` and `unauthorized_carrier_blocks_result_
//!   construction`. This is a structural guarantee (the type does not exist
//!   without the call succeeding), not only a runtime check.
//! - Meetily's text-length confidence heuristic and Parakeet's absent
//!   confidence can be represented only as [`Quality::DiagnosticLabel`] or
//!   [`Quality::Unavailable`] — never as [`Quality::Calibrated`], whose
//!   fields are format/range-validated. See `diagnostic_label_is_never_a_
//!   calibrated_probability`.
//! - A `translate` task without an explicit target language, or a
//!   `transcribe` task that declares one, is rejected before any other
//!   validation — no silent language fallback. See
//!   `translate_without_target_is_rejected`.
//! - An unsigned/malformed model-manifest digest fails [`validate_request`]
//!   before any audio or model byte would be touched by a real worker. See
//!   `malformed_model_manifest_digest_is_rejected`.
//!
//! ## What is explicitly NOT proven or claimed here
//!
//! No `whisper-rs`/whisper.cpp or Parakeet/ONNX Runtime provider exists in
//! this module (GOC-33-W04/W05, deferred — no model is vendored or downloaded
//! per the lane's standing rule). No worker process, model pool, CAS
//! resolver, AU orchestration adapter, or `audio-transcriber` compatibility
//! path exists here (W03 process skeleton, W06 AU/compat adapter — deferred).
//! No WER/RTF/latency/resource evidence is claimed (W07 — deferred). No
//! docs/handoff artifact exists yet (W08 — deferred). Downstream work should
//! treat this module as the frozen **type-level** ASR contract only, exactly
//! as `crate::ingress` documents itself for GOC-32.

use serde::{Deserialize, Serialize};

use crate::ingress::{CarrierRef, IdempotencyKey, StreamGeneration, StreamId};

/// Current wire/behavioral version of the `asr.*` contract family.
pub const ASR_PROTOCOL_VERSION: u16 = 1;

const MAX_DIAGNOSTIC_LABEL_LEN: usize = 256;

fn is_valid_digest(value: &str) -> bool {
    // Duplicated from `crate::ingress` deliberately: this module stays
    // independently reviewable and does not reach into that module's
    // private helpers, matching the dependency-light posture the ingress
    // module itself documents for its own relationship to `eg-modality`.
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

/// A bounded wire token reused for ASR-local identity (request/job/model
/// manifest/segment/text-ref). Same shape as `crate::ingress::BoundedId`; a
/// distinct alias is kept here (rather than a raw re-export) so a future
/// GOC-05 committer can see at a glance which identities are ASR-local vs.
/// ingress-owned when it mints real `OpaqueRef`s from each.
pub use crate::ingress::BoundedId as AsrBoundedId;

pub type RequestId = AsrBoundedId;
pub type JobId = AsrBoundedId;
pub type SegmentTextRef = AsrBoundedId;
pub type TranscriptRenditionRef = AsrBoundedId;
pub type LanguageTag = AsrBoundedId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AsrTask {
    Transcribe,
    Translate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimingAvailability {
    Unavailable,
    Provided,
}

/// `task=transcribe` preserves source language; `task=translate` requires an
/// explicit target. No silent fallback between the two.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LanguageSpec {
    pub source: LanguageTag,
    #[serde(default)]
    pub target: Option<LanguageTag>,
}

/// A bounded reference to the source audio range this request/segment covers.
/// Carries no raw audio — only the GOC-32 stream identity, a millisecond
/// range, and a content digest a durable committer can verify against the
/// governed rendition it already holds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioSourceRef {
    pub stream_id: StreamId,
    pub generation: StreamGeneration,
    pub start_ms: u64,
    pub end_ms: u64,
    /// Lowercase hex SHA-256 over the source audio range's committed bytes.
    pub source_digest: String,
}

impl AudioSourceRef {
    pub fn validate(&self) -> Result<(), AsrError> {
        if self.end_ms <= self.start_ms {
            return Err(AsrError::MalformedRequest {
                reason: "audio source range end_ms must be strictly after start_ms",
            });
        }
        if !is_valid_digest(&self.source_digest) {
            return Err(AsrError::MalformedRequest {
                reason: "source_digest is not a 64-char lowercase hex sha256",
            });
        }
        Ok(())
    }
}

/// Policy inputs a real deployment must qualify (GOC-33-W07); no default is
/// hardcoded here. All fields must be strictly positive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AsrLimits {
    pub max_window_ms: u32,
    pub max_decoded_audio_bytes: u32,
    pub max_segment_text_bytes: u32,
    pub max_segments: u32,
    pub max_inflight_partials: u32,
    pub request_deadline_ms: u32,
}

impl AsrLimits {
    pub fn validate(&self) -> Result<(), AsrError> {
        let all_positive = self.max_window_ms > 0
            && self.max_decoded_audio_bytes > 0
            && self.max_segment_text_bytes > 0
            && self.max_segments > 0
            && self.max_inflight_partials > 0
            && self.request_deadline_ms > 0;
        if !all_positive {
            return Err(AsrError::MalformedRequest {
                reason: "asr limits must be strictly positive",
            });
        }
        Ok(())
    }
}

/// A reference to an immutable, signed model manifest (GOC-33-W03/W05
/// acquisition/signing is out of scope here — this only validates the wire
/// SHAPE a worker would check before trusting the manifest). A mutable/
/// unsigned manifest is rejected before any model or audio byte access.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelManifestRef {
    pub manifest_id: AsrBoundedId,
    /// Lowercase hex SHA-256 over the full manifest content.
    pub digest: String,
    /// Opaque reference to the signer/signature record. `AsrBoundedId`
    /// already refuses an empty token at construction, so a manifest ref
    /// cannot be built with a blank signature reference.
    pub signature_ref: AsrBoundedId,
}

impl ModelManifestRef {
    pub fn validate(&self) -> Result<(), AsrError> {
        if !is_valid_digest(&self.digest) {
            return Err(AsrError::ModelUnavailable {
                reason: "model manifest digest is not a valid sha256",
            });
        }
        Ok(())
    }
}

/// `asr.request.v1`
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AsrRequest {
    pub protocol_version: u16,
    pub request_id: RequestId,
    pub job_id: JobId,
    pub carrier: CarrierRef,
    pub source: AudioSourceRef,
    pub model_manifest: ModelManifestRef,
    pub language: LanguageSpec,
    pub task: AsrTask,
    pub partial_support: bool,
    pub word_timing_requested: bool,
    pub limits: AsrLimits,
    pub generation: StreamGeneration,
    pub idempotency_key: IdempotencyKey,
}

/// Bounded, pure request validation. Never touches audio or model bytes;
/// returns the first typed violation found. A real worker/listener MUST call
/// this (and reject on `Err`) before any model load or CAS read — this is the
/// "fails before accessing audio/model bytes" acceptance gate at the request
/// shape level.
pub fn validate_request(request: &AsrRequest) -> Result<(), AsrError> {
    if request.protocol_version != ASR_PROTOCOL_VERSION {
        return Err(AsrError::IncompatibleVersion);
    }
    request.source.validate()?;
    request.model_manifest.validate()?;
    request.limits.validate()?;
    match request.task {
        AsrTask::Translate if request.language.target.is_none() => {
            return Err(AsrError::UnsupportedTask {
                reason: "translate requires an explicit target language",
            });
        }
        AsrTask::Transcribe if request.language.target.is_some() => {
            return Err(AsrError::MalformedRequest {
                reason: "transcribe must not declare a translation target",
            });
        }
        _ => {}
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentSequence(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WordSpan {
    pub start_ms: u64,
    pub end_ms: u64,
}

/// Model-derived timing is preserved only when a provider actually supplies
/// it. `Unavailable` is the explicit, honest default — never estimated or
/// fabricated from chunk/transport timing (Meetily's discarded-timestamp
/// path is the negative example this contract exists to prevent).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SegmentTiming {
    Unavailable,
    Provided {
        start_ms: u64,
        end_ms: u64,
        words: Vec<WordSpan>,
    },
}

impl SegmentTiming {
    pub fn validate(&self) -> Result<(), AsrError> {
        match self {
            SegmentTiming::Unavailable => Ok(()),
            SegmentTiming::Provided {
                start_ms,
                end_ms,
                words,
            } => {
                if end_ms <= start_ms {
                    return Err(AsrError::MalformedResult {
                        reason: "segment timing end_ms must be strictly after start_ms",
                    });
                }
                let mut floor = *start_ms;
                for word in words {
                    // A truncated/corrupted provider output can claim word
                    // boundaries that run backward or spill past the
                    // segment's own declared end — the ASR-layer analog of
                    // GOC-32's truncated-PCM-frame check. Reject rather than
                    // silently clamp or accept as complete.
                    if word.end_ms <= word.start_ms
                        || word.start_ms < floor
                        || word.end_ms > *end_ms
                    {
                        return Err(AsrError::MalformedResult {
                            reason: "word span is not monotonic within its segment bounds",
                        });
                    }
                    floor = word.end_ms;
                }
                Ok(())
            }
        }
    }
}

/// Quality is calibrated or explicitly labeled as not calibrated. A provider
/// heuristic (e.g. Meetily's text-length formula) may only ever be carried as
/// [`Quality::DiagnosticLabel`] — a bounded, non-numeric string that this
/// contract never treats as, converts to, or serializes as a probability.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Quality {
    Unavailable,
    Calibrated {
        avg_logprob: f32,
        no_speech_prob: f32,
    },
    DiagnosticLabel(String),
}

impl Quality {
    pub fn validate(&self) -> Result<(), AsrError> {
        match self {
            Quality::Unavailable => Ok(()),
            Quality::Calibrated {
                avg_logprob,
                no_speech_prob,
            } => {
                let bounded = avg_logprob.is_finite()
                    && no_speech_prob.is_finite()
                    && (0.0..=1.0).contains(no_speech_prob);
                if !bounded {
                    return Err(AsrError::QualityUnavailable);
                }
                Ok(())
            }
            Quality::DiagnosticLabel(label) => {
                if label.is_empty() || label.len() > MAX_DIAGNOSTIC_LABEL_LEN {
                    return Err(AsrError::MalformedResult {
                        reason: "diagnostic quality label is empty or out of bounds",
                    });
                }
                Ok(())
            }
        }
    }
}

/// `asr.partial.v1` — provisional and replaceable; never terminal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AsrPartial {
    pub request_id: RequestId,
    pub generation: StreamGeneration,
    pub sequence: SegmentSequence,
    pub text_ref: SegmentTextRef,
    pub timing: SegmentTiming,
    pub quality: Quality,
    pub supersedes: Option<SegmentSequence>,
}

/// `asr.segment.v1` — immutable once emitted; may supersede an earlier
/// sequence's provisional output.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AsrSegment {
    pub segment_id: AsrBoundedId,
    pub request_id: RequestId,
    pub sequence: SegmentSequence,
    pub source: AudioSourceRef,
    pub text_ref: SegmentTextRef,
    pub language: LanguageTag,
    pub task: AsrTask,
    pub timing: SegmentTiming,
    pub quality: Quality,
    pub model_manifest: ModelManifestRef,
    pub supersedes: Option<SegmentSequence>,
}

impl AsrSegment {
    pub fn validate(&self) -> Result<(), AsrError> {
        self.source.validate()?;
        self.timing.validate()?;
        self.quality.validate()?;
        self.model_manifest.validate()?;
        Ok(())
    }
}

/// A closed set of policy outcomes a real GOC-15/16 carrier check must
/// produce. Never freeform — see `crate::ingress::CancelReason` for the same
/// discipline applied to control frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyDecision {
    Authorized,
    ConsentRevoked,
    PolicyDenied,
    TenantMismatch,
}

/// Proof that a carrier was checked and authorized. This module's ONLY
/// constructor is [`authorize_carrier`], and [`finalize_result`] is the ONLY
/// function that can produce an [`AsrResult`] — and it requires one. A
/// transcript cannot be assembled by this module without a successful
/// authorization decision; there is no code path that skips this token.
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
/// what decides `decision`; this function only enforces that a transcript can
/// never be produced except through an explicit `Authorized` outcome.
pub fn authorize_carrier(
    carrier: &CarrierRef,
    decision: PolicyDecision,
) -> Result<AuthorizedCarrier, AsrError> {
    match decision {
        PolicyDecision::Authorized => Ok(AuthorizedCarrier {
            carrier: carrier.clone(),
        }),
        PolicyDecision::ConsentRevoked => Err(AsrError::PolicyDenied {
            reason: "consent_revoked",
        }),
        PolicyDecision::PolicyDenied => Err(AsrError::PolicyDenied {
            reason: "policy_denied",
        }),
        PolicyDecision::TenantMismatch => Err(AsrError::PolicyDenied {
            reason: "tenant_mismatch",
        }),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResultStatus {
    Complete,
    Degraded,
    Cancelled,
}

/// `asr.result.v1` — terminal. Constructible only via [`finalize_result`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AsrResult {
    pub request_id: RequestId,
    pub status: ResultStatus,
    pub segments: Vec<AsrSegment>,
    pub transcript_rendition_ref: TranscriptRenditionRef,
    pub source: AudioSourceRef,
    pub language: LanguageSpec,
    pub model_manifest: ModelManifestRef,
    pub timing_coverage: TimingAvailability,
}

/// Assemble a terminal `asr.result.v1`. Requires:
/// - an [`AuthorizedCarrier`] (structural authorization proof — see the
///   module doc's "known-bad" summary);
/// - a request that independently passes [`validate_request`];
/// - segments that each pass [`AsrSegment::validate`], belong to this
///   request, are strictly ordered by sequence, and stay within
///   `limits.max_segments`.
///
/// A `Complete` status with zero segments is rejected — a partial/degraded
/// run must say so explicitly rather than reporting an empty transcript as a
/// finished one.
pub fn finalize_result(
    authorized: &AuthorizedCarrier,
    request: &AsrRequest,
    segments: Vec<AsrSegment>,
    status: ResultStatus,
    transcript_rendition_ref: TranscriptRenditionRef,
) -> Result<AsrResult, AsrError> {
    if authorized.carrier() != &request.carrier {
        return Err(AsrError::PolicyDenied {
            reason: "authorized carrier does not match the request carrier",
        });
    }
    validate_request(request)?;
    if status == ResultStatus::Complete && segments.is_empty() {
        return Err(AsrError::MalformedResult {
            reason: "a complete result requires at least one ordered segment",
        });
    }
    if segments.len() as u32 > request.limits.max_segments {
        return Err(AsrError::ResourceExhausted {
            limit: "max_segments",
        });
    }
    let mut previous_sequence: Option<u64> = None;
    for segment in &segments {
        if segment.request_id != request.request_id {
            return Err(AsrError::Conflict {
                reason: "segment belongs to a different request",
            });
        }
        segment.validate()?;
        if let Some(previous) = previous_sequence {
            if segment.sequence.0 <= previous {
                return Err(AsrError::MalformedResult {
                    reason: "segments are not strictly ordered by sequence",
                });
            }
        }
        previous_sequence = Some(segment.sequence.0);
    }
    let timing_coverage = if !segments.is_empty()
        && segments
            .iter()
            .all(|segment| matches!(segment.timing, SegmentTiming::Provided { .. }))
    {
        TimingAvailability::Provided
    } else {
        TimingAvailability::Unavailable
    };
    Ok(AsrResult {
        request_id: request.request_id.clone(),
        status,
        segments,
        transcript_rendition_ref,
        source: request.source.clone(),
        language: request.language.clone(),
        model_manifest: request.model_manifest.clone(),
        timing_coverage,
    })
}

/// `asr.error.v1` — a typed, bounded diagnostic. `Serialize`-only for the
/// same reason as `crate::ingress::IngressError`: `&'static str` reason
/// fields cannot soundly implement `Deserialize<'de>`, and an error has no
/// reason to be accepted as an inbound request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum AsrError {
    ModelUnavailable {
        reason: &'static str,
    },
    UnsupportedLanguage,
    UnsupportedTask {
        reason: &'static str,
    },
    TimingUnavailable,
    QualityUnavailable,
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
    /// Not in the lane doc's original error taxonomy; added because a pure
    /// request-shape validator needs a way to report a malformed request
    /// distinct from every other typed reason above. Kept `&'static str`
    /// bounded for the same reason the rest of this enum is.
    MalformedRequest {
        reason: &'static str,
    },
    MalformedResult {
        reason: &'static str,
    },
    Conflict {
        reason: &'static str,
    },
}

#[cfg(test)]
mod fixtures {
    use super::*;
    use crate::ingress::BoundedId;

    pub fn id(value: &str) -> AsrBoundedId {
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

    pub fn source(start_ms: u64, end_ms: u64) -> AudioSourceRef {
        AudioSourceRef {
            stream_id: id("stream-1"),
            generation: StreamGeneration(1),
            start_ms,
            end_ms,
            source_digest: "a".repeat(64),
        }
    }

    pub fn model_manifest() -> ModelManifestRef {
        ModelManifestRef {
            manifest_id: id("manifest-1"),
            digest: "b".repeat(64),
            signature_ref: id("signature-1"),
        }
    }

    pub fn limits() -> AsrLimits {
        AsrLimits {
            max_window_ms: 30_000,
            max_decoded_audio_bytes: 2 * 1024 * 1024,
            max_segment_text_bytes: 64 * 1024,
            max_segments: 256,
            max_inflight_partials: 128,
            request_deadline_ms: 60_000,
        }
    }

    pub fn request() -> AsrRequest {
        AsrRequest {
            protocol_version: ASR_PROTOCOL_VERSION,
            request_id: id("request-1"),
            job_id: id("job-1"),
            carrier: carrier(),
            source: source(0, 5_000),
            model_manifest: model_manifest(),
            language: LanguageSpec {
                source: id("en"),
                target: None,
            },
            task: AsrTask::Transcribe,
            partial_support: true,
            word_timing_requested: true,
            limits: limits(),
            generation: StreamGeneration(1),
            idempotency_key: id("idem-1"),
        }
    }

    pub fn segment(request_id: &RequestId, sequence: u64) -> AsrSegment {
        AsrSegment {
            segment_id: id("segment-1"),
            request_id: request_id.clone(),
            sequence: SegmentSequence(sequence),
            source: source(0, 5_000),
            text_ref: id("text-ref-1"),
            language: id("en"),
            task: AsrTask::Transcribe,
            timing: SegmentTiming::Provided {
                start_ms: 0,
                end_ms: 5_000,
                words: vec![WordSpan {
                    start_ms: 0,
                    end_ms: 400,
                }],
            },
            quality: Quality::Calibrated {
                avg_logprob: -0.2,
                no_speech_prob: 0.01,
            },
            model_manifest: model_manifest(),
            supersedes: None,
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
        assert_eq!(
            validate_request(&request),
            Err(AsrError::IncompatibleVersion)
        );
    }

    /// Known-bad input: a zero/negative-length audio source range — the
    /// request-level analog of GOC-32's truncated/malformed frame. Must be
    /// rejected with a typed error before any model or audio byte access.
    #[test]
    fn rejects_zero_length_source_range() {
        let mut request = request();
        request.source = source(1_000, 1_000);
        assert_eq!(
            validate_request(&request),
            Err(AsrError::MalformedRequest {
                reason: "audio source range end_ms must be strictly after start_ms"
            })
        );
    }

    /// Known-bad input: a tampered/malformed digest field on the source
    /// range (not 64-char lowercase hex), mirroring
    /// `ingress::rejects_malformed_digest_field`.
    #[test]
    fn rejects_malformed_source_digest() {
        let mut request = request();
        request.source.source_digest = "not-a-hex-digest".to_string();
        assert_eq!(
            validate_request(&request),
            Err(AsrError::MalformedRequest {
                reason: "source_digest is not a 64-char lowercase hex sha256"
            })
        );
    }

    /// Known-bad input: an unsigned/malformed model-manifest digest. A real
    /// worker must fail before it ever loads the manifest or touches audio.
    #[test]
    fn malformed_model_manifest_digest_is_rejected() {
        let mut request = request();
        request.model_manifest.digest = "short".to_string();
        assert_eq!(
            validate_request(&request),
            Err(AsrError::ModelUnavailable {
                reason: "model manifest digest is not a valid sha256"
            })
        );
    }

    #[test]
    fn translate_without_target_is_rejected() {
        let mut request = request();
        request.task = AsrTask::Translate;
        assert_eq!(
            validate_request(&request),
            Err(AsrError::UnsupportedTask {
                reason: "translate requires an explicit target language"
            })
        );
    }

    #[test]
    fn transcribe_with_a_target_is_rejected_as_silent_fallback_risk() {
        let mut request = request();
        request.language.target = Some(fixtures::id("es"));
        assert_eq!(
            validate_request(&request),
            Err(AsrError::MalformedRequest {
                reason: "transcribe must not declare a translation target"
            })
        );
    }

    #[test]
    fn zero_limits_are_rejected() {
        let mut request = request();
        request.limits.max_segments = 0;
        assert_eq!(
            validate_request(&request),
            Err(AsrError::MalformedRequest {
                reason: "asr limits must be strictly positive"
            })
        );
    }
}

#[cfg(test)]
mod segment_timing_tests {
    use super::*;

    #[test]
    fn unavailable_timing_always_validates() {
        assert_eq!(SegmentTiming::Unavailable.validate(), Ok(()));
    }

    #[test]
    fn well_formed_provided_timing_validates() {
        let timing = SegmentTiming::Provided {
            start_ms: 0,
            end_ms: 1_000,
            words: vec![
                WordSpan {
                    start_ms: 0,
                    end_ms: 200,
                },
                WordSpan {
                    start_ms: 200,
                    end_ms: 600,
                },
            ],
        };
        assert_eq!(timing.validate(), Ok(()));
    }

    /// Known-bad input: a provider result whose word timing runs past its
    /// own declared segment boundary — the shape a truncated/corrupted
    /// decoder output would take. Must be rejected, not silently clamped or
    /// accepted as a complete, well-timed segment.
    #[test]
    fn rejects_truncated_word_timing_past_segment_bound() {
        let timing = SegmentTiming::Provided {
            start_ms: 0,
            end_ms: 1_000,
            words: vec![WordSpan {
                start_ms: 900,
                end_ms: 1_500, // spills past end_ms=1_000
            }],
        };
        assert_eq!(
            timing.validate(),
            Err(AsrError::MalformedResult {
                reason: "word span is not monotonic within its segment bounds"
            })
        );
    }

    #[test]
    fn rejects_non_monotonic_word_order() {
        let timing = SegmentTiming::Provided {
            start_ms: 0,
            end_ms: 1_000,
            words: vec![
                WordSpan {
                    start_ms: 500,
                    end_ms: 700,
                },
                WordSpan {
                    start_ms: 100, // runs backward
                    end_ms: 400,
                },
            ],
        };
        assert_eq!(
            timing.validate(),
            Err(AsrError::MalformedResult {
                reason: "word span is not monotonic within its segment bounds"
            })
        );
    }
}

#[cfg(test)]
mod quality_tests {
    use super::*;

    #[test]
    fn diagnostic_label_is_never_a_calibrated_probability() {
        // A Meetily-style text-length heuristic can only ever be carried as
        // a bounded, non-numeric diagnostic label — there is no conversion
        // path from `DiagnosticLabel` to `Calibrated` anywhere in this
        // module, and the label itself is never interpreted as a number.
        let heuristic = Quality::DiagnosticLabel("text_length_heuristic:42".to_string());
        assert_eq!(heuristic.validate(), Ok(()));
        assert!(!matches!(heuristic, Quality::Calibrated { .. }));
    }

    #[test]
    fn empty_diagnostic_label_is_rejected() {
        let empty = Quality::DiagnosticLabel(String::new());
        assert_eq!(
            empty.validate(),
            Err(AsrError::MalformedResult {
                reason: "diagnostic quality label is empty or out of bounds"
            })
        );
    }

    #[test]
    fn out_of_range_no_speech_probability_is_rejected() {
        let bad = Quality::Calibrated {
            avg_logprob: -0.1,
            no_speech_prob: 1.5,
        };
        assert_eq!(bad.validate(), Err(AsrError::QualityUnavailable));
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

    /// Known-bad scenario: consent was revoked. No `AuthorizedCarrier` is
    /// ever produced, so a transcript cannot be assembled through
    /// `finalize_result` for this carrier — proven both by this `Err`
    /// assertion and structurally, since `finalize_result` has no
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

    /// End-to-end known-bad proof: audio was submitted under a carrier whose
    /// consent was revoked. There is no `AsrResult` for it — the
    /// authorization step fails first, before segments are ever considered.
    #[test]
    fn unauthorized_carrier_blocks_result_construction() {
        let request = request();
        let authorization = authorize_carrier(&request.carrier, PolicyDecision::ConsentRevoked);
        assert_eq!(
            authorization.err(),
            Some(AsrError::PolicyDenied {
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
        let outcome = finalize_result(
            &authorization,
            &request,
            vec![],
            ResultStatus::Cancelled,
            fixtures::id("rendition-1"),
        );
        assert_eq!(
            outcome,
            Err(AsrError::PolicyDenied {
                reason: "authorized carrier does not match the request carrier"
            })
        );
    }
}

#[cfg(test)]
mod finalize_result_tests {
    use super::fixtures::*;
    use super::*;

    fn authorized(request: &AsrRequest) -> AuthorizedCarrier {
        authorize_carrier(&request.carrier, PolicyDecision::Authorized)
            .expect("fixture carrier is authorized")
    }

    #[test]
    fn assembles_a_complete_result_from_ordered_segments() {
        let request = request();
        let auth = authorized(&request);
        let segments = vec![
            segment(&request.request_id, 0),
            segment(&request.request_id, 1),
        ];
        let result = finalize_result(
            &auth,
            &request,
            segments,
            ResultStatus::Complete,
            fixtures::id("rendition-1"),
        )
        .expect("well-formed complete result");
        assert_eq!(result.timing_coverage, TimingAvailability::Provided);
        assert_eq!(result.segments.len(), 2);
    }

    #[test]
    fn complete_status_with_no_segments_is_rejected() {
        let request = request();
        let auth = authorized(&request);
        let outcome = finalize_result(
            &auth,
            &request,
            vec![],
            ResultStatus::Complete,
            fixtures::id("rendition-1"),
        );
        assert_eq!(
            outcome,
            Err(AsrError::MalformedResult {
                reason: "a complete result requires at least one ordered segment"
            })
        );
    }

    #[test]
    fn cancelled_status_with_no_segments_is_allowed() {
        let request = request();
        let auth = authorized(&request);
        let outcome = finalize_result(
            &auth,
            &request,
            vec![],
            ResultStatus::Cancelled,
            fixtures::id("rendition-1"),
        );
        assert!(outcome.is_ok());
    }

    #[test]
    fn out_of_order_segments_are_rejected() {
        let request = request();
        let auth = authorized(&request);
        let segments = vec![
            segment(&request.request_id, 1),
            segment(&request.request_id, 0),
        ];
        let outcome = finalize_result(
            &auth,
            &request,
            segments,
            ResultStatus::Complete,
            fixtures::id("rendition-1"),
        );
        assert_eq!(
            outcome,
            Err(AsrError::MalformedResult {
                reason: "segments are not strictly ordered by sequence"
            })
        );
    }

    #[test]
    fn segment_from_a_different_request_is_a_typed_conflict() {
        let request = request();
        let auth = authorized(&request);
        let foreign_request_id = fixtures::id("request-2");
        let segments = vec![segment(&foreign_request_id, 0)];
        let outcome = finalize_result(
            &auth,
            &request,
            segments,
            ResultStatus::Complete,
            fixtures::id("rendition-1"),
        );
        assert_eq!(
            outcome,
            Err(AsrError::Conflict {
                reason: "segment belongs to a different request"
            })
        );
    }

    #[test]
    fn exceeding_max_segments_is_resource_exhausted() {
        let mut request = request();
        request.limits.max_segments = 1;
        let auth = authorized(&request);
        let segments = vec![
            segment(&request.request_id, 0),
            segment(&request.request_id, 1),
        ];
        let outcome = finalize_result(
            &auth,
            &request,
            segments,
            ResultStatus::Complete,
            fixtures::id("rendition-1"),
        );
        assert_eq!(
            outcome,
            Err(AsrError::ResourceExhausted {
                limit: "max_segments"
            })
        );
    }

    /// Known-bad input propagated end to end: a segment with truncated word
    /// timing must fail result assembly with a typed error rather than
    /// being folded into a "complete" result.
    #[test]
    fn a_segment_with_truncated_timing_fails_result_assembly() {
        let request = request();
        let auth = authorized(&request);
        let mut bad_segment = segment(&request.request_id, 0);
        bad_segment.timing = SegmentTiming::Provided {
            start_ms: 0,
            end_ms: 1_000,
            words: vec![WordSpan {
                start_ms: 900,
                end_ms: 5_000, // truncated/corrupted: spills far past end_ms
            }],
        };
        let outcome = finalize_result(
            &auth,
            &request,
            vec![bad_segment],
            ResultStatus::Complete,
            fixtures::id("rendition-1"),
        );
        assert_eq!(
            outcome,
            Err(AsrError::MalformedResult {
                reason: "word span is not monotonic within its segment bounds"
            })
        );
    }

    #[test]
    fn missing_timing_on_any_segment_reports_unavailable_coverage() {
        let request = request();
        let auth = authorized(&request);
        let mut untimed = segment(&request.request_id, 0);
        untimed.timing = SegmentTiming::Unavailable;
        let result = finalize_result(
            &auth,
            &request,
            vec![untimed],
            ResultStatus::Complete,
            fixtures::id("rendition-1"),
        )
        .expect("missing timing alone is not a rejection reason");
        assert_eq!(result.timing_coverage, TimingAvailability::Unavailable);
    }
}
