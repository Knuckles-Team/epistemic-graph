//! The audio modality's stored value model (CONCEPT:E4 follow-up — image/audio/video
//! evidence) — metadata + a content-addressed blob reference + an optional segment
//! index, deliberately NOT a decoded PCM sample buffer.
//!
//! [`AudioData`] mirrors `eg-image::ImageData`'s shape: `sample_rate`/`duration_ms` are
//! read from a REAL WAV header (`crate::header::read_wav_header`) — never fabricated —
//! and `segments` is an OPTIONAL index of named time ranges supplied by an external
//! extractor (a diarization/VAD pass, a transcription aligner, a human annotation, …);
//! this crate never invents one.

use serde::{Deserialize, Serialize};

/// One named time segment inside an audio recording (milliseconds from the start), as
/// supplied by an external extractor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AudioSegment {
    /// A caller-supplied label ("speaker-1", "music", a VAD/ASR label, …). Optional.
    #[serde(default)]
    pub label: Option<String>,
    pub start_ms: u64,
    pub end_ms: u64,
}

impl AudioSegment {
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

/// The audio modality's stored value: real sample rate/duration + a content-addressed
/// blob reference + an optional, extractor-supplied segment index. No PCM sample
/// buffer — see module docs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AudioData {
    pub sample_rate: u32,
    pub duration_ms: u64,
    /// Content address of the ORIGINAL audio bytes, resolvable through the engine's
    /// blob CAS. Opaque to this crate.
    pub blob_ref: String,
    /// Named time-range segments inside the recording, in milliseconds. May be empty
    /// (a recording with no detected/known segments yet).
    #[serde(default)]
    pub segments: Vec<AudioSegment>,
}

impl AudioData {
    /// Construct directly from already-known sample rate/duration (e.g. supplied by
    /// an upstream extractor/pipeline).
    pub fn new(sample_rate: u32, duration_ms: u64, blob_ref: impl Into<String>) -> Self {
        Self {
            sample_rate,
            duration_ms,
            blob_ref: blob_ref.into(),
            segments: Vec::new(),
        }
    }

    pub fn with_segments(mut self, segments: Vec<AudioSegment>) -> Self {
        self.segments = segments;
        self
    }

    /// Build an `AudioData` from REAL WAV bytes: parses the RIFF/WAV header for
    /// `sample_rate`/`duration_ms` (never fabricated), content-addresses the bytes for
    /// `blob_ref`, and attaches the given (externally supplied, possibly empty)
    /// segment index verbatim. Returns `None` if the bytes aren't a recognized WAV
    /// header — a caller that already knows sample_rate/duration from elsewhere can
    /// still build one directly via [`AudioData::new`].
    pub fn from_bytes(bytes: &[u8], segments: Vec<AudioSegment>) -> Option<Self> {
        let info = crate::header::read_wav_header(bytes)?;
        Some(Self {
            sample_rate: info.sample_rate,
            duration_ms: info.duration_ms,
            blob_ref: crate::header::content_hash(bytes),
            segments,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_defaults_to_no_segments() {
        let a = AudioData::new(44_100, 1000, "abc");
        assert!(a.segments.is_empty());
    }

    #[test]
    fn serde_round_trips() {
        let a = AudioData::new(16_000, 2000, "hash1").with_segments(vec![AudioSegment::labeled(
            "speaker-1",
            0,
            1000,
        )]);
        let json = serde_json::to_string(&a).unwrap();
        let back: AudioData = serde_json::from_str(&json).unwrap();
        assert_eq!(a, back);
    }

    #[test]
    fn from_bytes_never_fabricates_duration_for_unrecognized_bytes() {
        assert_eq!(AudioData::from_bytes(b"not a wav", Vec::new()), None);
    }
}
