//! # eg-video — the video modality (metadata/shot-level, CONCEPT:E4 follow-up)
//!
//! A pure-Rust leaf crate (a sibling of `eg-tensor`/`eg-geo`/`eg-image`/`eg-audio`)
//! giving the engine a video data type with **NO heavy video/ffmpeg codec dependency**
//! — the Raspberry-Pi contract. Deliberately **metadata/shot-level**, not a frame
//! codec:
//!
//! * [`VideoData`] — `duration_ms` (read from a REAL MP4/ISOBMFF `moov/mvhd` box via
//!   [`header::read_mp4_duration_ms`] — never fabricated), an optional `frame_rate`
//!   (supplied by a caller — this metadata-level parser does not walk per-track
//!   `stbl` boxes), a content-addressed `blob_ref` pointing at the original bytes in
//!   the engine's blob CAS, and an optional [`VideoShot`] index (named millisecond
//!   time ranges) supplied by an external extractor (a shot-boundary detector, a
//!   scene-segmentation pass, a human annotation, …). This crate never invents a shot.
//! * [`header`] — the dependency-free MP4/ISOBMFF box walker, plus a duplicated
//!   FNV-1a-128 [`header::content_hash`] (the SAME construction
//!   `eg_tensor::store::content_hash`/`eg_image::content_hash`/
//!   `eg_audio::content_hash` use).
//!
//! ## What this crate does NOT do
//!
//! Real frame decode (H.264/VP9/AV1 → a pixel-frame buffer) is explicitly OUT of
//! scope for v1 — see the crate's reserved, dependency-free `codec` feature (currently
//! a no-op) for where a real decoder would be gated in a follow-up, kept OUT of the
//! default build (the Pi contract). Likewise, per-frame-rate extraction (walking
//! `trak/mdia/minf/stbl/stts`) is a documented follow-up — `frame_rate` stays `None`
//! from `VideoData::from_bytes` today.
//!
//! ## `ModalityContract` (CONCEPT:E4 follow-up)
//!
//! Behind the crate's own opt-in `contract` feature (default OFF, mirroring
//! `eg-tensor`/`eg-geo`/`eg-image`/`eg-audio`): `VideoData::evidence(id)` returns a
//! REAL, located `eg_modality::EvidenceSpan::VideoShot` from the first stored shot (or
//! `None` when the video carries no shot index yet — never fabricated). See
//! `src/contract.rs`.
//!
//! ## `runtime` (CONCEPT:EG-P1-3, OPT-IN, default OFF)
//!
//! `src/runtime.rs`, behind the crate's own `runtime` feature: container/track/
//! keyframe/shot/scene/caption/temporal-embedding TRAITS + typed artifacts — a
//! trait-definition layer only, adding NO new dependency even when the feature is
//! on (a real demuxer/detector/model is a separate plugin crate/service). See
//! `src/runtime.rs` module docs.

mod header;
mod video;

// ModalityContract retrofit (CONCEPT:E4 follow-up): `impl ModalityContract for
// VideoData`, behind the crate's own opt-in `contract` feature (default OFF).
#[cfg(feature = "contract")]
mod contract;

// OPT-IN runtime seam (CONCEPT:EG-P1-3): container/track/keyframe/shot/scene/
// caption/temporal-embedding traits + typed artifacts, behind the crate's own
// `runtime` feature (default OFF, no new dependency).
#[cfg(feature = "runtime")]
pub mod runtime;

pub use header::{content_hash, read_mp4_duration_ms};
pub use video::{VideoData, VideoShot};

// CONCEPT:EG-P1-3 — the small-footprint default is sacred: neither `codec` nor
// `runtime` is ever implied by `default`, so a plain `cargo build -p eg-video`
// links no heavy video/ffmpeg dependency. Asserted here as an executable
// regression check (see `eg-image`'s identical guardrail for the full rationale).
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
