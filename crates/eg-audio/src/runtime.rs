//! OPT-IN audio runtime seam (CONCEPT:EG-P1-3), behind this crate's own `runtime`
//! feature (default OFF, no new dependency).
//!
//! `eg-audio`'s default build is deliberately metadata/segment-level (see the
//! crate's module docs) — no waveform decode, ever, in the default build. Real
//! audio-runtime work (codec decode, windowed waveform stats, spectrograms, voice
//! activity detection, speaker diarization, transcript-to-audio alignment) needs
//! exactly the heavy native dependencies (`symphonia`/an FFT crate/a VAD or
//! diarization model, …) this workspace's Pi contract keeps out of the default
//! build. So this module, gated behind `runtime`, defines ONLY the typed
//! artifacts + the plugin TRAITS a real codec/model would implement — it adds no
//! dependency at all, even with `runtime` on, because it contains no
//! implementation to depend on anything with.
//!
//! * [`WaveformWindow`] / [`AudioCodecPlugin`] — decode a time window to summary
//!   waveform stats (peak/RMS) — never full PCM samples (that would need a real
//!   codec's buffer type).
//! * [`SpectrogramFrame`] / [`SpectrogramPlugin`] — a time-frequency analysis hook.
//! * [`VadSegment`] / [`VadPlugin`] — voice-activity detection.
//! * [`DiarizationSegment`] / [`DiarizationPlugin`] — speaker diarization.
//! * [`TranscriptAlignment`] / [`TranscriptAlignmentPlugin`] — align an existing
//!   transcript's text to audio time ranges (forced alignment).
//!
//! [`NoopAudioRuntime`] implements every trait as a trivial, always-empty
//! placeholder — enough to prove the trait seam end-to-end without claiming any
//! real decode/analysis occurred. Real codec/VAD/diarization/alignment models are
//! explicitly OUT of scope — documented follow-ups, implemented as external
//! plugins.

use crate::AudioData;

/// Summary waveform statistics for one time window — never full PCM samples.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WaveformWindow {
    pub start_ms: u64,
    pub end_ms: u64,
    pub peak: f32,
    pub rms: f32,
}

/// One frame of a time-frequency (spectrogram) analysis.
#[derive(Clone, Debug, PartialEq)]
pub struct SpectrogramFrame {
    pub start_ms: u64,
    pub end_ms: u64,
    pub bins: Vec<f32>,
}

/// One voice-activity-detection verdict over a time range.
#[derive(Clone, Debug, PartialEq)]
pub struct VadSegment {
    pub start_ms: u64,
    pub end_ms: u64,
    pub is_speech: bool,
}

/// One speaker-diarization segment.
#[derive(Clone, Debug, PartialEq)]
pub struct DiarizationSegment {
    pub start_ms: u64,
    pub end_ms: u64,
    pub speaker: String,
}

/// One forced-alignment result: a transcript text slice aligned to a time range.
#[derive(Clone, Debug, PartialEq)]
pub struct TranscriptAlignment {
    pub start_ms: u64,
    pub end_ms: u64,
    pub text: String,
}

/// Decode a `[start_ms, end_ms)` window to summary waveform stats. `None` means
/// "cannot decode this window" (e.g. out of range, unsupported codec).
pub trait AudioCodecPlugin {
    fn decode_window(
        &self,
        audio: &AudioData,
        start_ms: u64,
        end_ms: u64,
    ) -> Option<WaveformWindow>;
}

/// Compute a spectrogram over the whole recording.
pub trait SpectrogramPlugin {
    fn compute(&self, audio: &AudioData) -> Vec<SpectrogramFrame>;
}

/// Detect voice-activity segments over the whole recording.
pub trait VadPlugin {
    fn detect(&self, audio: &AudioData) -> Vec<VadSegment>;
}

/// Diarize speakers over the whole recording.
pub trait DiarizationPlugin {
    fn diarize(&self, audio: &AudioData) -> Vec<DiarizationSegment>;
}

/// Force-align an existing `transcript` to the recording's time axis.
pub trait TranscriptAlignmentPlugin {
    fn align(&self, audio: &AudioData, transcript: &str) -> Vec<TranscriptAlignment>;
}

/// A trivial, dependency-free placeholder implementing EVERY runtime trait: never
/// fabricates a result (empty `Vec`s, or `None`/a zeroed [`WaveformWindow`] on the
/// scalar hook) — proving the trait wiring compiles and runs end-to-end without
/// claiming any real decode/analysis occurred. Real implementations are
/// documented, out-of-scope follow-ups.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopAudioRuntime;

impl AudioCodecPlugin for NoopAudioRuntime {
    fn decode_window(
        &self,
        audio: &AudioData,
        start_ms: u64,
        end_ms: u64,
    ) -> Option<WaveformWindow> {
        if end_ms <= start_ms || end_ms > audio.duration_ms {
            return None;
        }
        Some(WaveformWindow {
            start_ms,
            end_ms,
            peak: 0.0,
            rms: 0.0,
        })
    }
}

impl SpectrogramPlugin for NoopAudioRuntime {
    fn compute(&self, _audio: &AudioData) -> Vec<SpectrogramFrame> {
        Vec::new()
    }
}

impl VadPlugin for NoopAudioRuntime {
    fn detect(&self, _audio: &AudioData) -> Vec<VadSegment> {
        Vec::new()
    }
}

impl DiarizationPlugin for NoopAudioRuntime {
    fn diarize(&self, _audio: &AudioData) -> Vec<DiarizationSegment> {
        Vec::new()
    }
}

impl TranscriptAlignmentPlugin for NoopAudioRuntime {
    fn align(&self, _audio: &AudioData, _transcript: &str) -> Vec<TranscriptAlignment> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_runtime_decodes_a_window_within_range() {
        let audio = AudioData::new(16_000, 5000, "blob-1");
        let window = NoopAudioRuntime.decode_window(&audio, 0, 1000).unwrap();
        assert_eq!((window.start_ms, window.end_ms), (0, 1000));
    }

    #[test]
    fn noop_runtime_rejects_a_window_past_the_known_duration() {
        let audio = AudioData::new(16_000, 1000, "blob-1");
        assert_eq!(NoopAudioRuntime.decode_window(&audio, 0, 5000), None);
    }

    #[test]
    fn noop_runtime_never_fabricates_analysis_results() {
        let audio = AudioData::new(16_000, 1000, "blob-1");
        assert!(SpectrogramPlugin::compute(&NoopAudioRuntime, &audio).is_empty());
        assert!(VadPlugin::detect(&NoopAudioRuntime, &audio).is_empty());
        assert!(DiarizationPlugin::diarize(&NoopAudioRuntime, &audio).is_empty());
        assert!(TranscriptAlignmentPlugin::align(&NoopAudioRuntime, &audio, "hello").is_empty());
    }
}
