//! The video modality's stored value model (CONCEPT:E4 follow-up — image/audio/video
//! evidence) — metadata + a content-addressed blob reference + an optional shot/scene
//! index, deliberately NOT decoded frames.
//!
//! [`VideoData`] mirrors `eg-image::ImageData`/`eg-audio::AudioData`'s shape:
//! `duration_ms` is read from a REAL MP4/ISOBMFF `moov/mvhd` box
//! (`crate::header::read_mp4_duration_ms`) — never fabricated — and `shots` is an
//! OPTIONAL index of named time ranges supplied by an external extractor (a
//! shot-boundary detector, a scene-segmentation pass, a human annotation, …); this
//! crate never invents one.

use serde::{Deserialize, Serialize};

/// One named shot/scene time range inside a video (milliseconds from the start), as
/// supplied by an external extractor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VideoShot {
    /// A caller-supplied label ("scene-1", a shot-detector index, …). Optional.
    #[serde(default)]
    pub label: Option<String>,
    pub start_ms: u64,
    pub end_ms: u64,
}

impl VideoShot {
    pub fn new(start_ms: u64, end_ms: u64) -> Self {
        Self {
            label: None,
            start_ms,
            end_ms,
        }
    }

    pub fn labeled(label: impl Into<String>, start_ms: u64, end_ms: u64) -> Self {
        Self {
            label: Some(label.into()),
            start_ms,
            end_ms,
        }
    }
}

/// The video modality's stored value: real duration + a content-addressed blob
/// reference + an optional, extractor-supplied shot/scene index. No decoded frames —
/// see module docs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VideoData {
    pub duration_ms: u64,
    /// Frames per second, when known. `None` when not supplied/derivable from the
    /// header this crate reads (frame rate lives in per-track `stbl`/`stts` boxes,
    /// which this metadata-level parser does not walk — a documented follow-up, see
    /// crate docs).
    #[serde(default)]
    pub frame_rate: Option<f64>,
    /// Content address of the ORIGINAL video bytes, resolvable through the engine's
    /// blob CAS. Opaque to this crate.
    pub blob_ref: String,
    /// Named shot/scene time-range boundaries, in milliseconds. May be empty (a video
    /// with no detected/known shots yet).
    #[serde(default)]
    pub shots: Vec<VideoShot>,
}

impl VideoData {
    /// Construct directly from an already-known duration (e.g. supplied by an
    /// upstream extractor/pipeline).
    pub fn new(duration_ms: u64, blob_ref: impl Into<String>) -> Self {
        Self {
            duration_ms,
            frame_rate: None,
            blob_ref: blob_ref.into(),
            shots: Vec::new(),
        }
    }

    pub fn with_shots(mut self, shots: Vec<VideoShot>) -> Self {
        self.shots = shots;
        self
    }

    pub fn with_frame_rate(mut self, frame_rate: f64) -> Self {
        self.frame_rate = Some(frame_rate);
        self
    }

    /// Build a `VideoData` from REAL MP4/ISOBMFF bytes: parses the `moov/mvhd` box for
    /// `duration_ms` (never fabricated), content-addresses the bytes for `blob_ref`,
    /// and attaches the given (externally supplied, possibly empty) shot index
    /// verbatim. `frame_rate` stays `None` (this metadata-level parser does not walk
    /// per-track `stbl` boxes — see the struct docs). Returns `None` if no `moov/
    /// mvhd` box is found — a caller that already knows the duration from elsewhere
    /// can still build one directly via [`VideoData::new`].
    pub fn from_bytes(bytes: &[u8], shots: Vec<VideoShot>) -> Option<Self> {
        let duration_ms = crate::header::read_mp4_duration_ms(bytes)?;
        Some(Self {
            duration_ms,
            frame_rate: None,
            blob_ref: crate::header::content_hash(bytes),
            shots,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_defaults_to_no_shots_and_no_frame_rate() {
        let v = VideoData::new(5000, "abc");
        assert!(v.shots.is_empty());
        assert_eq!(v.frame_rate, None);
    }

    #[test]
    fn serde_round_trips() {
        let v = VideoData::new(9000, "hash1")
            .with_frame_rate(29.97)
            .with_shots(vec![VideoShot::labeled("scene-1", 0, 3000)]);
        let json = serde_json::to_string(&v).unwrap();
        let back: VideoData = serde_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn from_bytes_never_fabricates_duration_for_unrecognized_bytes() {
        assert_eq!(VideoData::from_bytes(b"not an mp4", Vec::new()), None);
    }
}
