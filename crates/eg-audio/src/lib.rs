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
//!
//! ## `ingress` (GOC-32, OPT-IN, default OFF)
//!
//! `src/ingress.rs`, behind `ingress`, is the versioned realtime audio-ingress
//! wire contract (`audio.ingress.*`) plus a pure, dependency-light in-process
//! runtime: frame shape/integrity validation, sequence/watermark tracking with a
//! bounded reorder window, and a bounded admission queue that proves backpressure
//! rather than unbounded growth. It adds no codec, transport, device, or CAS
//! dependency — see the module doc for the exact authority boundary and what is
//! (and is not) proven there.

mod audio;
mod header;

// Governed contract implementation.
#[cfg(feature = "contract")]
mod contract;

// Pure-Rust native runtime.
#[cfg(feature = "runtime")]
pub mod runtime;

// Realtime audio-ingress contract + bounded in-process runtime (GOC-32).
#[cfg(feature = "ingress")]
pub mod ingress;

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

// The realtime ingress contract stays opt-in until GOC-32-W03..W07 wire a real
// worker/transport around it — see crates/eg-audio/src/ingress.rs.
#[cfg(all(test, not(feature = "ingress")))]
mod default_build_guardrail_ingress {
    #[test]
    fn default_build_has_no_ingress() {
        assert!(
            !cfg!(feature = "ingress"),
            "the `ingress` feature must stay opt-in, never part of `default`"
        );
    }
}
