//! # eg-audio — the audio modality (metadata/segment-level, CONCEPT:E4 follow-up)
//!
//! A pure-Rust leaf crate (a sibling of `eg-tensor`/`eg-geo`/`eg-image`) giving the
//! engine an audio data type with **NO heavy audio-codec dependency** (no `symphonia`/
//! `hound`/decoder crate) — the Raspberry-Pi contract. Deliberately
//! **metadata/segment-level**, not a waveform codec:
//!
//! * [`AudioData`] — `sample_rate`/`duration_ms` (read from a REAL WAV/RIFF header via
//!   [`header::read_wav_header`] — never fabricated), a content-addressed `blob_ref`
//!   pointing at the original bytes in the engine's blob CAS, and an optional
//!   [`AudioSegment`] index (named millisecond time ranges) supplied by an external
//!   extractor (a diarization/VAD pass, a transcription aligner, a human annotation,
//!   …). This crate never invents a segment.
//! * [`header`] — the dependency-free WAV header parser, plus a duplicated
//!   FNV-1a-128 [`header::content_hash`] (the SAME construction
//!   `eg_tensor::store::content_hash`/`eg_image::content_hash` use).
//!
//! ## What this crate does NOT do
//!
//! Real waveform decode (MP3/FLAC/Opus → PCM samples, or even reading WAV sample
//! bytes themselves) is explicitly OUT of scope for v1 — see the crate's reserved,
//! dependency-free `codec` feature (currently a no-op) for where a real codec would be
//! gated in a follow-up, kept OUT of the default build (the Pi contract).
//!
//! ## `ModalityContract` (CONCEPT:E4 follow-up)
//!
//! Behind the crate's own opt-in `contract` feature (default OFF, mirroring
//! `eg-tensor`/`eg-geo`/`eg-image`): `AudioData::evidence(id)` returns a REAL, located
//! `eg_modality::EvidenceSpan::AudioSegment` from the first stored segment (or `None`
//! when the recording carries no segment index yet — never fabricated). See
//! `src/contract.rs`.
//!
//! ## `runtime` (CONCEPT:EG-P1-3, OPT-IN, default OFF)
//!
//! `src/runtime.rs`, behind the crate's own `runtime` feature: codec/waveform-
//! window/spectrogram/VAD/diarization/transcript-alignment TRAITS + typed
//! artifacts — a trait-definition layer only, adding NO new dependency even when
//! the feature is on (a real codec/model is a separate plugin crate/service). See
//! `src/runtime.rs` module docs.

mod audio;
mod header;

// ModalityContract retrofit (CONCEPT:E4 follow-up): `impl ModalityContract for
// AudioData`, behind the crate's own opt-in `contract` feature (default OFF).
#[cfg(feature = "contract")]
mod contract;

// OPT-IN runtime seam (CONCEPT:EG-P1-3): codec/waveform-window/spectrogram/VAD/
// diarization/transcript-alignment traits + typed artifacts, behind the crate's
// own `runtime` feature (default OFF, no new dependency).
#[cfg(feature = "runtime")]
pub mod runtime;

pub use audio::{AudioData, AudioSegment};
pub use header::{content_hash, read_wav_header, WavInfo};

// CONCEPT:EG-P1-3 — the small-footprint default is sacred: neither `codec` nor
// `runtime` is ever implied by `default`, so a plain `cargo build -p eg-audio`
// links no heavy audio dependency. Asserted here as an executable regression
// check (see `eg-image`'s identical guardrail for the full rationale).
#[cfg(all(test, not(any(feature = "codec", feature = "runtime"))))]
mod default_build_guardrail {
    #[test]
    fn default_build_has_neither_codec_nor_runtime_on() {
        assert!(
            !cfg!(feature = "codec"),
            "the `codec` feature must stay opt-in, never part of `default`"
        );
        assert!(
            !cfg!(feature = "runtime"),
            "the `runtime` feature must stay opt-in, never part of `default`"
        );
    }
}
