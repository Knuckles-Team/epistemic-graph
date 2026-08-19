//! `Method::Asr { op }` (GOC-33, `OWNER-VOICE-ASR`, feature `asr-whisper`): a
//! direct, non-durable batch-file transcription call over the whisper-rs/
//! whisper.cpp provider in `eg-asr-whisper`. This is the wire surface
//! `audio-transcriber`'s pluggable `TranscriptionProvider` seam reaches over
//! the existing `epistemic_graph.client` MessagePack/UDS transport — no
//! second transport is introduced.
//!
//! NOT graph-row-scoped: a transcription reads no persisted graph state and
//! commits no durable `asr.result.v1` here (that governed, `CarrierRef`-bound
//! commit is future worker/AU-orchestration work — W03/W06 in the GOC-33 lane
//! doc; see `crates/eg-audio/src/asr.rs`'s module doc for the authority
//! boundary this handler deliberately stays on the near side of). This module
//! therefore self-routes in `dispatch.rs`, ahead of the per-graph
//! `dispatch_graph_op` chain, exactly like `handlers::quantum`/
//! `handlers::viz`, and needs no `state`/`GraphCore` at all. The verified
//! request gate still runs before this arm: because `AsrOp` carries no graph,
//! tenant, or server-side result handle, there is no cross-tenant row target
//! for this pure caller-supplied computation to address or disclose.
//!
//! Every [`eg_audio::asr::AsrSegment`] the provider constructs is validated
//! through that frozen contract's own `AsrSegment::validate()` before this
//! handler will include it in a response (see `eg-asr-whisper`'s module doc)
//! — this handler only serializes an already-validated outcome, it performs
//! no additional inference or timing/quality logic of its own.
//!
//! Model acquisition/verification governance is explicitly NOT this
//! surface's job (GOC-36): `model_path`/`model_sha256` in [`AsrOp::
//! TranscribeFile`] must already name a caller-resolved, digest-declared
//! model file. `eg_asr_whisper::verify_model` fails closed — distinctly from
//! every other rejection reason — when that file is absent or the digest
//! does not match; this handler never falls back to a network fetch.

use eg_asr_whisper::{
    decode_wav_16k_mono, verify_model, CancelFlag, TranscribeOptions, WhisperAsrProvider,
};
use eg_audio::asr::AsrError;
use eg_types::asr_wire::AsrOp;

use crate::protocol::{Response, ResultPayload};

const DEFAULT_WINDOW_MS: u32 = 30_000;

pub(crate) async fn handle(req_id: u64, op: AsrOp) -> Response {
    match op {
        AsrOp::TranscribeFile {
            model_path,
            model_sha256,
            audio_wav,
            language,
            translate,
            word_timing,
            window_ms,
        } => {
            // whisper.cpp is a blocking, CPU-bound C library call — run it on
            // a blocking-safe thread rather than the async reactor, exactly
            // as any long CPU-bound handler in this codebase must.
            let outcome = tokio::task::spawn_blocking(move || {
                transcribe_file(
                    &model_path,
                    &model_sha256,
                    &audio_wav,
                    language.as_deref(),
                    translate,
                    word_timing,
                    if window_ms == 0 {
                        DEFAULT_WINDOW_MS
                    } else {
                        window_ms
                    },
                )
            })
            .await;
            match outcome {
                Ok(Ok(payload)) => Response::ok(req_id, ResultPayload::Json(payload)),
                Ok(Err(err)) => Response::err(req_id, asr_error_message(&err)),
                Err(_) => Response::err(req_id, "asr worker task panicked".to_string()),
            }
        }
    }
}

fn transcribe_file(
    model_path: &str,
    model_sha256: &str,
    audio_wav: &[u8],
    language: Option<&str>,
    translate: bool,
    word_timing: bool,
    window_ms: u32,
) -> Result<serde_json::Value, AsrError> {
    let verified = verify_model(model_path, model_sha256)?;
    let provider = WhisperAsrProvider::load(&verified, "eg-asr-whisper-rpc", false)?;
    let audio = decode_wav_16k_mono(audio_wav)?;
    let opts = TranscribeOptions {
        language: language.map(str::to_string),
        translate,
        word_timing,
        window_ms,
    };
    let cancel = CancelFlag::new();
    let outcome = provider.transcribe_streaming(&audio, &opts, &cancel, |_partial| {})?;

    let segments: Vec<serde_json::Value> = outcome
        .segments
        .iter()
        .map(|s| {
            let (start_ms, end_ms) = match &s.segment.timing {
                eg_audio::asr::SegmentTiming::Provided {
                    start_ms, end_ms, ..
                } => (*start_ms, *end_ms),
                eg_audio::asr::SegmentTiming::Unavailable => (0, 0),
            };
            let (avg_logprob, no_speech_prob) = match &s.segment.quality {
                eg_audio::asr::Quality::Calibrated {
                    avg_logprob,
                    no_speech_prob,
                } => (Some(*avg_logprob), Some(*no_speech_prob)),
                _ => (None, None),
            };
            serde_json::json!({
                "start": start_ms as f64 / 1000.0,
                "end": end_ms as f64 / 1000.0,
                "text": s.text,
                "avg_logprob": avg_logprob,
                "no_speech_prob": no_speech_prob,
            })
        })
        .collect();
    let text = outcome
        .segments
        .iter()
        .map(|s| s.text.as_str())
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string();

    Ok(serde_json::json!({
        "text": text,
        "language": outcome.language,
        "segments": segments,
        "timing_available": outcome.timing_available,
    }))
}

/// A stable, readable message per typed [`AsrError`] variant — never a bare
/// `{:?}` dump of a value that might carry caller-sensitive detail, and never
/// a generic "failed" that loses the distinction between e.g. `Cancelled` and
/// `ModelUnavailable`.
fn asr_error_message(err: &AsrError) -> String {
    match err {
        AsrError::ModelUnavailable { reason } => format!("model_unavailable: {reason}"),
        AsrError::UnsupportedLanguage => "unsupported_language".to_string(),
        AsrError::UnsupportedTask { reason } => format!("unsupported_task: {reason}"),
        AsrError::TimingUnavailable => "timing_unavailable".to_string(),
        AsrError::QualityUnavailable => {
            "quality_unavailable: no accepted segments (e.g. silence/no speech)".to_string()
        }
        AsrError::ResourceExhausted { limit } => format!("resource_exhausted: {limit}"),
        AsrError::PolicyDenied { reason } => format!("policy_denied: {reason}"),
        AsrError::Timeout => "timeout".to_string(),
        AsrError::Cancelled => "cancelled".to_string(),
        AsrError::Degraded => "degraded".to_string(),
        AsrError::IncompatibleVersion => "incompatible_version".to_string(),
        AsrError::MalformedRequest { reason } => format!("malformed_request: {reason}"),
        AsrError::MalformedResult { reason } => format!("malformed_result: {reason}"),
        AsrError::Conflict { reason } => format!("conflict: {reason}"),
    }
}
