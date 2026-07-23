//! Source-free decoded audio facts, bounded waveform features, and governed time
//! ranges. The SHA-256 `blob_ref` binds them to request-local PCM samples.

use serde::{Deserialize, Serialize};

/// One named time segment inside an audio recording (milliseconds from the start), as
/// supplied by an external extractor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AudioSegment {
    /// Optional opaque label reference. Governed serving rejects display text.
    #[serde(default)]
    pub label: Option<String>,
    pub start_ms: u64,
    pub end_ms: u64,
}

/// Bounded native waveform summary for one exact window. Samples themselves never
/// leave the request-local decoder.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AudioFeatureWindow {
    pub start_ms: u64,
    pub end_ms: u64,
    pub peak: f32,
    pub rms: f32,
    pub spectral_centroid_bin: f32,
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

/// The audio modality's stored value. PCM samples remain request-local.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AudioData {
    pub sample_rate: u32,
    pub channels: u16,
    pub bits_per_sample: u16,
    pub duration_ms: u64,
    /// Content address of the ORIGINAL audio bytes, resolvable through the engine's
    /// blob CAS. Opaque to this crate.
    pub blob_ref: String,
    /// Named time-range segments inside the recording, in milliseconds. May be empty
    /// (a recording with no detected/known segments yet).
    #[serde(default)]
    pub segments: Vec<AudioSegment>,
    pub feature_windows: Vec<AudioFeatureWindow>,
}

impl AudioData {
    /// Construct directly from already-known sample rate/duration (e.g. supplied by
    /// an upstream extractor/pipeline).
    pub fn new(sample_rate: u32, duration_ms: u64, blob_ref: impl Into<String>) -> Self {
        Self {
            sample_rate,
            channels: 1,
            bits_per_sample: 16,
            duration_ms,
            blob_ref: blob_ref.into(),
            segments: Vec::new(),
            feature_windows: Vec::new(),
        }
    }

    pub fn with_segments(mut self, segments: Vec<AudioSegment>) -> Self {
        self.segments = segments;
        self
    }

    pub fn with_native_features(
        mut self,
        channels: u16,
        bits_per_sample: u16,
        windows: Vec<AudioFeatureWindow>,
    ) -> Self {
        self.channels = channels;
        self.bits_per_sample = bits_per_sample;
        self.feature_windows = windows;
        self
    }

    /// Build a header-only `AudioData`: parses the RIFF/WAV header for
    /// `sample_rate`/`duration_ms` (never fabricated), content-addresses the bytes for
    /// `blob_ref`, and attaches the given (externally supplied, possibly empty)
    /// segment index verbatim. Returns `None` if the bytes aren't a recognized WAV
    /// header — a caller that already knows sample_rate/duration from elsewhere can
    /// still build one directly via [`AudioData::new`]. Production serving uses
    /// `NativeAudioRuntime` so waveform features are mandatory.
    pub fn from_bytes(bytes: &[u8], segments: Vec<AudioSegment>) -> Option<Self> {
        let info = crate::header::read_wav_header(bytes)?;
        Some(Self {
            sample_rate: info.sample_rate,
            channels: info.channels,
            bits_per_sample: info.bits_per_sample,
            duration_ms: info.duration_ms,
            blob_ref: crate::header::content_hash(bytes),
            segments,
            feature_windows: Vec::new(),
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
            "eg:label:0000000000000001",
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
