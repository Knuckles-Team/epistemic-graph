//! # eg-audio — native governed audio serving
//!
//! The current pure-Rust runtime strictly decodes bounded 8/16-bit PCM/WAV into a
//! request-local mono buffer, then materializes complete-coverage waveform windows,
//! RMS/peak statistics, bounded spectral-centroid features, energy VAD segments,
//! opaque channel grouping, and reference-only transcript alignment. Durable
//! [`AudioData`] contains SHA-256 content identity and normalized features, never PCM
//! samples, transcript text, or speaker identity. Other encodings are rejected by the
//! current codec contract.
//!
//! ## `ModalityContract`
//!
//! Behind the crate's `contract` feature, `AudioData::evidence_address()` returns a real
//! `eg_modality::EvidenceAddress::AudioRange` from the first stored segment (or `None`
//! when the recording carries no segment index yet — never fabricated). See
//! `src/contract.rs`.
//!
//! ## `runtime` (CONCEPT:EG-P1-3, OPT-IN, default OFF)
//!
//! `src/runtime.rs`, behind `runtime`, provides the native codec, temporal posting
//! index, and exact time/RMS predicates. Governed persistence is exposed through
//! [`AudioServingRuntime`].

mod audio;
mod header;

// Governed contract implementation.
#[cfg(feature = "contract")]
mod contract;

// Pure-Rust native runtime.
#[cfg(feature = "runtime")]
pub mod runtime;

pub use audio::{AudioData, AudioFeatureWindow, AudioSegment};

#[cfg(feature = "serving")]
pub type AudioServingRuntime = eg_modality::ServedModalityRuntime<AudioData>;
pub use header::{content_hash, read_wav_header, WavInfo};

// The leaf default stays value-only; the main facade enables the native runtime.
#[cfg(all(test, not(feature = "runtime")))]
mod default_build_guardrail {
    #[test]
    fn default_build_has_no_runtime() {
        assert!(
            !cfg!(feature = "runtime"),
            "the `runtime` feature must stay opt-in, never part of `default`"
        );
    }
}
