//! `AsrOp` — the wire op `Method::Asr` carries (GOC-33, `OWNER-VOICE-ASR`),
//! gated `asr-native`. Mirrors `quantum::QuantumOp`/`jobs::JobOp`'s "one
//! `Method` variant, one internal op enum" shape so the native-ASR surface
//! costs the wire protocol exactly ONE new `Method` variant.
//!
//! This is a direct, non-durable, non-GOC-32-bound request/response
//! transcription call — the batch-compatibility surface `audio-transcriber`'s
//! pluggable `TranscriptionProvider` seam calls over the existing
//! `epistemic_graph.client` MessagePack/UDS transport (no second transport).
//! It is NOT the future governed streaming worker (`epistemic-graph-voice-
//! worker`, GOC-33-W03) and does not construct a durable `asr.result.v1` — see
//! `crates/eg-audio/src/asr.rs`'s module doc and `crates/eg-asr-whisper/src/
//! lib.rs`'s module doc for that authority boundary.
//!
//! Pure serde — no dependency on `eg-audio`/`eg-asr-whisper` (both sit ABOVE
//! this crate in the DAG; `eg-types` is the bottom of the whole engine DAG).
//! The facade (`src/server/handlers/asr.rs`) is the one place that calls into
//! the real whisper-rs provider and maps its typed
//! `eg_audio::asr::AsrSegment`/`AsrError` back onto this wire shape; this
//! module only shapes what crosses the wire.

use serde::{Deserialize, Serialize};

/// `Method::Asr { op }`'s request shape. `audio_wav` carries the actual PCM
/// bytes over the wire (MessagePack handles binary natively — no base64
/// inflation); `model_path`/`model_sha256` are a caller-resolved,
/// already-verified model reference (GOC-36 owns acquisition; this crate
/// never resolves a bare model name into a path or URL).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AsrOp {
    TranscribeFile {
        model_path: String,
        model_sha256: String,
        audio_wav: Vec<u8>,
        #[serde(default)]
        language: Option<String>,
        #[serde(default)]
        translate: bool,
        #[serde(default)]
        word_timing: bool,
        /// Bounded streaming window, milliseconds. `0`/absent ⇒ handler
        /// default (30_000 — see `handlers::asr`).
        #[serde(default)]
        window_ms: u32,
    },
}
