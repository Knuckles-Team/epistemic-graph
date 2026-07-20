//! Source-free normalized video tracks, frames, and governed shot ranges.

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrackKind {
    Video,
    Audio,
    Subtitle,
    Metadata,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoTrack {
    pub track_id: u32,
    pub kind: TrackKind,
    pub codec_fourcc: [u8; 4],
    pub timescale: u32,
    pub width: u16,
    pub height: u16,
    /// Visual sample-entry pixel depth. Non-video tracks use zero.
    pub pixel_depth: u16,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VideoFrame {
    pub track_id: u32,
    pub frame_number: u64,
    pub start_ms: u64,
    pub end_ms: u64,
    pub byte_offset: u64,
    pub byte_length: u32,
    pub keyframe: bool,
}

/// One named shot/scene time range inside a video (milliseconds from the start), as
/// supplied by an external extractor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VideoShot {
    /// Optional opaque label reference. Governed serving rejects display text.
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

/// The video modality's stored value. Encoded bytes and decoded pixel buffers remain
/// outside this serializable record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VideoData {
    pub duration_ms: u64,
    /// Frames per second, derived by the native runtime or supplied with a governed
    /// directly constructed value.
    #[serde(default)]
    pub frame_rate: Option<f64>,
    /// Content address of the ORIGINAL video bytes, resolvable through the engine's
    /// blob CAS. Opaque to this crate.
    pub blob_ref: String,
    /// Named shot/scene time-range boundaries, in milliseconds. May be empty (a video
    /// with no detected/known shots yet).
    #[serde(default)]
    pub shots: Vec<VideoShot>,
    pub tracks: Vec<VideoTrack>,
    pub frames: Vec<VideoFrame>,
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
            tracks: Vec::new(),
            frames: Vec::new(),
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

    pub fn with_native_index(mut self, tracks: Vec<VideoTrack>, frames: Vec<VideoFrame>) -> Self {
        self.tracks = tracks;
        self.frames = frames;
        self
    }

    /// Header-only value construction. Production serving uses
    /// `NativeVideoRuntime` so tracks and frames are mandatory.
    pub fn from_bytes(bytes: &[u8], shots: Vec<VideoShot>) -> Option<Self> {
        let duration_ms = crate::header::read_mp4_duration_ms(bytes)?;
        Some(Self {
            duration_ms,
            frame_rate: None,
            blob_ref: crate::header::content_hash(bytes),
            shots,
            tracks: Vec::new(),
            frames: Vec::new(),
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
            .with_shots(vec![VideoShot::labeled(
                "eg:label:0000000000000001",
                0,
                3000,
            )]);
        let json = serde_json::to_string(&v).unwrap();
        let back: VideoData = serde_json::from_str(&json).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn from_bytes_never_fabricates_duration_for_unrecognized_bytes() {
        assert_eq!(VideoData::from_bytes(b"not an mp4", Vec::new()), None);
    }
}
