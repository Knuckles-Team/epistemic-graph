//! # eg-video — native governed video serving
//!
//! The current pure-Rust runtime strictly walks ISOBMFF movie/track/sample tables,
//! validates media-data byte ranges, derives exact frame timing and keyframe identity,
//! calculates frame rate and temporal signatures, and decodes current uncompressed
//! 24-bit `raw ` RGB samples. Other codecs remain exact encoded frame values and are never
//! reported as decoded pixels. Durable [`VideoData`] contains SHA-256 content identity,
//! normalized tracks/frames/shots, and no source bytes.
//!
//! ## `ModalityContract`
//!
//! Behind the crate's `contract` feature, `VideoData::evidence_address()` returns a
//! real `eg_modality::EvidenceAddress::VideoTimeRange` from the first stored shot (or
//! `None` when the video carries no shot index yet — never fabricated). See
//! `src/contract.rs`.
//!
//! ## `runtime` (CONCEPT:EG-P1-3, OPT-IN, default OFF)
//!
//! `src/runtime.rs`, behind `runtime`, provides native parsing, sample access,
//! temporal postings, and exact frame-window predicates. Governed persistence is
//! exposed through [`VideoServingRuntime`].

mod header;
mod video;

// Governed contract implementation.
#[cfg(feature = "contract")]
mod contract;

// Pure-Rust native runtime.
#[cfg(feature = "runtime")]
pub mod runtime;

pub use header::{content_hash, read_mp4_duration_ms};
pub use video::{TrackKind, VideoData, VideoFrame, VideoShot, VideoTrack};

#[cfg(feature = "serving")]
pub type VideoServingRuntime = eg_modality::ServedModalityRuntime<VideoData>;

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
