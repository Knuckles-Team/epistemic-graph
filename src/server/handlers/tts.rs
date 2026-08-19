//! Native TTS synthesis (GOC-34, `OWNER-VOICE-TTS`). Stateless — no graph core, runs
//! inline, mirroring `finance.rs`'s pure-compute shape (Method::TtsSynthesize is
//! `mutates: false` in `eg-capabilities`'s policy ledger — see that crate). The
//! verified caller is required before inference, but the request contains no
//! graph-row selector or server-side result handle; it therefore has no
//! cross-tenant row read to project or shape.
//!
//! ## What this handler honestly does and does not do
//!
//! - **Does**: decode+validate the frozen `tts.request.v1` contract
//!   (`eg_audio::tts::validate_request`), re-hash the caller-supplied sensitive
//!   input bytes against the request's declared digest, resolve+digest-verify the
//!   voice model/config pair against an operator-configured directory (GOC-36
//!   placeholder — see `eg_tts_piper::voice`'s doc), run real streaming ONNX
//!   inference via `eg-tts-piper`, and assemble a terminal `tts.result.v1` via
//!   `eg_audio::tts::finalize_result` — the SAME structural authorization/ordering/
//!   non-finite-audio guarantees the frozen contract's own tests prove.
//! - **Does NOT (documented gaps, not silently papered over)**:
//!   - **Authorization** maps "the eg2 transport already authenticated this
//!     caller" (the `caller` identity dispatch already verified before any handler
//!     runs) straight to [`tts::PolicyDecision::Authorized`]. A real GOC-15/16
//!     consent/classification decision is NOT integrated — an absent verified
//!     caller fails closed (`PolicyDenied`), but there is no richer per-tenant
//!     consent/classification check yet.
//!   - **Durable CAS/rendition publication** (GOC-05/eg-jobs territory) does not
//!     exist. Raw PCM bytes are returned INLINE alongside the typed result rather
//!     than published to a durable content store — `TtsChunk.rendition_ref`/
//!     `rendition_digest` describe THESE inline bytes, not a durable object.
//!   - **The durable WorkItem/job/lease plane** (GOC-19/20) is not wired: this is
//!     one synchronous request/response, not an async submit/status/cancel job.
//!     Cancellation is real INSIDE `eg-tts-piper`'s streaming synthesis (see that
//!     crate), but nothing external can cancel an in-flight call over the wire yet.

#![allow(clippy::result_large_err)]

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::protocol::{Method, Response, ResultPayload};
use eg_audio::tts::{self, PolicyDecision};
use eg_tts_piper::{
    resolve_voice_paths, synthesize_streaming, verify_voice_digests, CancellationToken,
    LoadedVoice, SynthesizeRequest,
};

#[derive(Serialize)]
struct TtsSynthesizeResponse {
    result: tts::TtsResult,
    /// Raw PCM16LE bytes, one entry per `result.chunks[i]`, in the SAME order — the
    /// inline carrier documented in this module's doc.
    chunk_pcm: Vec<Vec<u8>>,
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

fn synthesize(
    req_id: u64,
    caller: Option<&str>,
    request_msgpack: &[u8],
    input_bytes: Vec<u8>,
) -> Result<Response, tts::TtsError> {
    let request: tts::TtsRequest =
        rmp_serde::from_slice(request_msgpack).map_err(|_| tts::TtsError::MalformedRequest {
            reason: "request_msgpack does not decode as a tts.request.v1 TtsRequest",
        })?;
    tts::validate_request(&request)?;

    if input_bytes.len() as u32 != request.input.byte_len
        || sha256_hex(&input_bytes) != request.input.input_digest
    {
        return Err(tts::TtsError::MalformedRequest {
            reason: "input_bytes does not match the request's declared input_digest/byte_len",
        });
    }
    let input_text =
        String::from_utf8(input_bytes).map_err(|_| tts::TtsError::MalformedRequest {
            reason: "input_bytes is not valid UTF-8",
        })?;

    // See this module's doc: the eg2 transport already authenticated `caller`
    // before any handler runs. An absent verified caller fails closed; a present
    // one maps to Authorized (a real GOC-15/16 consent/classification decision is
    // a documented, not-yet-built gap — see the doc above).
    let decision = if caller.is_some() {
        PolicyDecision::Authorized
    } else {
        PolicyDecision::PolicyDenied
    };
    let authorized = tts::authorize_carrier(&request.carrier, decision)?;

    let model_dir_raw = std::env::var(eg_tts_piper::voice::VOICE_MODEL_DIR_ENV).map_err(|_| {
        tts::TtsError::ModelUnavailable {
            reason: "EPISTEMIC_GRAPH_VOICE_MODEL_DIR is not configured (GOC-36 real \
                artifact resolution is a follow-up; this operator-configured directory \
                is the documented interim seam)",
        }
    })?;
    let model_dir = std::path::PathBuf::from(model_dir_raw);
    let paths = resolve_voice_paths(&model_dir, &request.voice)?;
    verify_voice_digests(&paths, &request.voice)?;
    let voice = LoadedVoice::load(&paths)?;

    let stream = synthesize_streaming(
        voice,
        SynthesizeRequest {
            request_id: request.request_id.clone(),
            job_id: request.job_id.clone(),
            input_mode: request.input.mode,
            input_text,
            espeak_voice: request.espeak_voice.as_str().to_string(),
            speaker: request.speaker,
            controls: request.controls,
            output_format: request.output_format,
            limits: request.limits,
        },
        CancellationToken::new(),
    )?;

    let mut chunks = Vec::new();
    let mut chunk_pcm = Vec::new();
    let mut status = tts::JobStatus::Succeeded;
    for item in stream {
        match item {
            Ok(synthesized) => {
                chunks.push(synthesized.chunk);
                chunk_pcm.push(synthesized.pcm);
            }
            Err(tts::TtsError::Cancelled) => {
                status = tts::JobStatus::Cancelled;
                break;
            }
            Err(e) => {
                if chunks.is_empty() {
                    return Err(e);
                }
                // Partial output with at least one good chunk is honestly
                // Degraded, never fabricated Succeeded — mirrors
                // `finalize_result`'s own "partial output is RUNNING/CANCELLED/
                // DEGRADED/FAILED, never fabricated success" rule.
                status = tts::JobStatus::Degraded;
                break;
            }
        }
    }

    let result = tts::finalize_result(&authorized, &request, chunks, status)?;
    Ok(Response::ok(
        req_id,
        ResultPayload::raw(&TtsSynthesizeResponse { result, chunk_pcm }),
    ))
}

/// Handle a `TtsSynthesize` method. `Err(method)` hands a non-TTS method back to the
/// dispatcher (routing fall-through), matching `handlers::finance::try_handle`'s
/// convention.
pub(crate) fn try_handle(
    req_id: u64,
    caller: Option<&str>,
    method: Method,
) -> Result<Response, Method> {
    let (request_msgpack, input_bytes) = match method {
        Method::TtsSynthesize {
            request_msgpack,
            input_bytes,
        } => (request_msgpack, input_bytes),
        other => return Err(other),
    };
    Ok(synthesize(req_id, caller, &request_msgpack, input_bytes)
        .unwrap_or_else(|e| Response::err(req_id, format!("{e:?}"))))
}
