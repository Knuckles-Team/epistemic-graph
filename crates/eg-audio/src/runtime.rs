//! Dependency-light native PCM/WAV audio runtime.
//!
//! The runtime decodes 8/16-bit PCM to an ephemeral mono buffer and performs real
//! waveform statistics, a small DFT spectrogram, energy-based voice activity, and
//! deterministic segment grouping. Transcript alignment accepts only an opaque
//! transcript reference plus unit count, so raw transcript or speaker identity is
//! never retained by this layer.

use std::f32::consts::TAU;

use eg_modality::{
    temporal_buckets, GovernedModality, NativePredicate, NativeProductionProbe, OpaqueRef,
};

use crate::{content_hash, read_wav_header, AudioData, AudioFeatureWindow, AudioSegment};

const MAX_DECODED_SAMPLES: usize = 16_777_216;
const MAX_SOURCE_BYTES: usize = 256 * 1024 * 1024;
const MAX_FEATURE_WINDOWS: usize = 4_096;
const MAX_ALIGNMENT_UNITS: u32 = 65_536;
const MAX_CHANNELS: u16 = 256;
const MAX_DFT_SAMPLES: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WaveformWindow {
    pub start_ms: u64,
    pub end_ms: u64,
    pub peak: f32,
    pub rms: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SpectrogramFrame {
    pub start_ms: u64,
    pub end_ms: u64,
    pub bins: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VadSegment {
    pub start_ms: u64,
    pub end_ms: u64,
    pub is_speech: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DiarizationSegment {
    pub start_ms: u64,
    pub end_ms: u64,
    pub speaker_ref: OpaqueRef,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TranscriptAlignment {
    pub start_ms: u64,
    pub end_ms: u64,
    pub transcript_ref: OpaqueRef,
    pub unit_start: u32,
    pub unit_end: u32,
}

pub trait AudioCodecPlugin {
    fn decode_window(
        &self,
        audio: &AudioData,
        start_ms: u64,
        end_ms: u64,
    ) -> Option<WaveformWindow>;
}

pub trait SpectrogramPlugin {
    fn compute(&self, audio: &AudioData) -> Vec<SpectrogramFrame>;
}

pub trait VadPlugin {
    fn detect(&self, audio: &AudioData) -> Vec<VadSegment>;
}

pub trait DiarizationPlugin {
    fn diarize(&self, audio: &AudioData) -> Vec<DiarizationSegment>;
}

pub trait TranscriptAlignmentPlugin {
    fn align(
        &self,
        audio: &AudioData,
        transcript_ref: &OpaqueRef,
        unit_count: u32,
    ) -> Vec<TranscriptAlignment>;
}

/// Concrete PCM runtime. Decoded samples are ephemeral and omitted from every
/// governed snapshot; only `AudioData.blob_ref` is durable.
#[derive(Clone, Debug)]
pub struct NativeAudioRuntime {
    samples: Vec<f32>,
    sample_rate: u32,
    channels: u16,
    bits_per_sample: u16,
    content_ref: String,
}

impl NativeAudioRuntime {
    pub fn from_wav(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > MAX_SOURCE_BYTES {
            return None;
        }
        let info = read_wav_header(bytes)?;
        let (channels, bits_per_sample, pcm) = pcm_payload(bytes)?;
        if channels == 0 || channels > MAX_CHANNELS || !matches!(bits_per_sample, 8 | 16) {
            return None;
        }
        let frame_bytes = usize::from(channels).checked_mul(usize::from(bits_per_sample / 8))?;
        if info.channels != channels
            || info.bits_per_sample != bits_per_sample
            || pcm.len() % frame_bytes != 0
            || pcm.len() / frame_bytes > MAX_DECODED_SAMPLES
        {
            return None;
        }
        let samples = decode_mono(pcm, channels, bits_per_sample)?;
        if samples.is_empty() {
            return None;
        }
        Some(Self {
            samples,
            sample_rate: info.sample_rate,
            channels,
            bits_per_sample,
            content_ref: content_hash(bytes),
        })
    }

    /// Materialize the bounded, source-free normalized audio value used by the
    /// served runtime.
    pub fn normalized_data(&self) -> Option<AudioData> {
        let duration_ms = self.samples.len() as u64 * 1_000 / u64::from(self.sample_rate);
        if duration_ms == 0 {
            return None;
        }
        let window_samples = self
            .samples
            .len()
            .div_ceil(MAX_FEATURE_WINDOWS)
            .max((self.sample_rate as usize / 50).max(1));
        let mut windows = Vec::new();
        for (index, samples) in self.samples.chunks(window_samples).enumerate() {
            let start_sample = index.checked_mul(window_samples)?;
            let end_sample = start_sample.checked_add(samples.len())?;
            let start_ms = start_sample as u64 * 1_000 / u64::from(self.sample_rate);
            let end_ms =
                (end_sample as u64 * 1_000 / u64::from(self.sample_rate)).max(start_ms + 1);
            let peak = samples
                .iter()
                .fold(0.0f32, |value, sample| value.max(sample.abs()));
            let rms = (samples.iter().map(|sample| sample * sample).sum::<f32>()
                / samples.len() as f32)
                .sqrt();
            let spectrum = bounded_spectral_bins(samples);
            let magnitude = spectrum.iter().sum::<f32>();
            let spectral_centroid_bin = if magnitude > 0.0 {
                spectrum
                    .iter()
                    .enumerate()
                    .map(|(bin, value)| bin as f32 * value)
                    .sum::<f32>()
                    / magnitude
            } else {
                0.0
            };
            windows.push(AudioFeatureWindow {
                start_ms,
                end_ms: end_ms.min(duration_ms),
                peak,
                rms,
                spectral_centroid_bin,
            });
        }
        if windows.is_empty()
            || windows
                .iter()
                .any(|window| temporal_buckets(window.start_ms, window.end_ms).is_err())
        {
            return None;
        }
        let whole_rms = (self.samples.iter().map(|value| value * value).sum::<f32>()
            / self.samples.len() as f32)
            .sqrt();
        let threshold = (whole_rms * 0.5).max(0.01);
        let segments = windows
            .iter()
            .filter(|window| window.rms >= threshold)
            .map(|window| AudioSegment::new(window.start_ms, window.end_ms))
            .collect();
        Some(
            AudioData::new(self.sample_rate, duration_ms, self.content_ref.clone())
                .with_native_features(self.channels, self.bits_per_sample, windows)
                .with_segments(segments),
        )
    }

    fn is_bound_to(&self, audio: &AudioData) -> bool {
        let duration_ms = self.samples.len() as u64 * 1_000 / u64::from(self.sample_rate);
        self.content_ref == audio.blob_ref
            && self.sample_rate == audio.sample_rate
            && self.channels == audio.channels
            && self.bits_per_sample == audio.bits_per_sample
            && duration_ms == audio.duration_ms
    }

    fn sample_range(&self, start_ms: u64, end_ms: u64) -> Option<&[f32]> {
        if end_ms <= start_ms {
            return None;
        }
        let start =
            usize::try_from(u128::from(start_ms) * u128::from(self.sample_rate) / 1_000).ok()?;
        let end =
            usize::try_from(u128::from(end_ms) * u128::from(self.sample_rate) / 1_000).ok()?;
        (start < end && end <= self.samples.len()).then(|| &self.samples[start..end])
    }

    fn stats(&self, start_ms: u64, end_ms: u64) -> Option<WaveformWindow> {
        let samples = self.sample_range(start_ms, end_ms)?;
        let peak = samples
            .iter()
            .fold(0.0f32, |max, value| max.max(value.abs()));
        let mean_square =
            samples.iter().map(|value| value * value).sum::<f32>() / samples.len() as f32;
        Some(WaveformWindow {
            start_ms,
            end_ms,
            peak,
            rms: mean_square.sqrt(),
        })
    }
}

pub fn production_probe() -> NativeProductionProbe {
    let bytes = probe_wav(16, 1);
    let runtime = NativeAudioRuntime::from_wav(&bytes);
    let value = runtime
        .as_ref()
        .and_then(NativeAudioRuntime::normalized_data);
    let codec = runtime.is_some();
    let normalized_payload = value
        .as_ref()
        .is_some_and(GovernedModality::validate_governed_payload);
    let secondary_index = value
        .as_ref()
        .is_some_and(|value| !value.native_index_keys().is_empty());
    let typed_query = value.as_ref().is_some_and(|value| {
        value.matches_native_predicate(&NativePredicate::AudioWindow {
            start_ms: 0,
            end_ms: value.duration_ms,
            minimum_rms: 0.0,
        })
    });
    let malformed_and_resource_bounds = NativeAudioRuntime::from_wav(b"invalid").is_none()
        && NativeAudioRuntime::from_wav(&probe_wav(24, 1)).is_none()
        && NativeAudioRuntime::from_wav(&probe_wav(16, MAX_CHANNELS + 1)).is_none();
    NativeProductionProbe {
        codec,
        normalized_payload,
        secondary_index,
        typed_query,
        malformed_and_resource_bounds,
    }
}

fn probe_wav(bits_per_sample: u16, channels: u16) -> Vec<u8> {
    let samples: [i16; 8] = [0, 8_000, 16_000, 8_000, 0, -8_000, -16_000, -8_000];
    let bytes_per_sample = u32::from(bits_per_sample / 8);
    let frame_bytes = u32::from(channels) * bytes_per_sample;
    let data_len = samples.len() as u32 * frame_bytes;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"RIFF");
    bytes.extend_from_slice(&(36 + data_len).to_le_bytes());
    bytes.extend_from_slice(b"WAVEfmt ");
    bytes.extend_from_slice(&16u32.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&channels.to_le_bytes());
    bytes.extend_from_slice(&8u32.to_le_bytes());
    bytes.extend_from_slice(&(8 * frame_bytes).to_le_bytes());
    bytes.extend_from_slice(&(frame_bytes as u16).to_le_bytes());
    bytes.extend_from_slice(&bits_per_sample.to_le_bytes());
    bytes.extend_from_slice(b"data");
    bytes.extend_from_slice(&data_len.to_le_bytes());
    for sample in samples {
        let raw = sample.to_le_bytes();
        for _ in 0..channels {
            for index in 0..bytes_per_sample as usize {
                bytes.push(raw[index.min(1)]);
            }
        }
    }
    bytes
}

impl AudioCodecPlugin for NativeAudioRuntime {
    fn decode_window(
        &self,
        audio: &AudioData,
        start_ms: u64,
        end_ms: u64,
    ) -> Option<WaveformWindow> {
        if !self.is_bound_to(audio) || end_ms > audio.duration_ms {
            return None;
        }
        self.stats(start_ms, end_ms)
    }
}

impl SpectrogramPlugin for NativeAudioRuntime {
    fn compute(&self, audio: &AudioData) -> Vec<SpectrogramFrame> {
        if !self.is_bound_to(audio) {
            return Vec::new();
        }
        let frame_len = self
            .samples
            .len()
            .div_ceil(MAX_FEATURE_WINDOWS)
            .max((self.sample_rate as usize / 50).clamp(32, 512));
        self.samples
            .chunks(frame_len)
            .enumerate()
            .map(|(frame_index, frame)| {
                let bins = bounded_spectral_bins(frame).to_vec();
                let start_sample = frame_index * frame_len;
                let end_sample = start_sample + frame.len();
                SpectrogramFrame {
                    start_ms: start_sample as u64 * 1_000 / self.sample_rate as u64,
                    end_ms: end_sample as u64 * 1_000 / self.sample_rate as u64,
                    bins,
                }
            })
            .collect()
    }
}

impl VadPlugin for NativeAudioRuntime {
    fn detect(&self, audio: &AudioData) -> Vec<VadSegment> {
        if !self.is_bound_to(audio) {
            return Vec::new();
        }
        let whole_rms =
            (self.samples.iter().map(|v| v * v).sum::<f32>() / self.samples.len() as f32).sqrt();
        let threshold = (whole_rms * 0.5).max(0.01);
        let window_ms = audio
            .duration_ms
            .div_ceil(MAX_FEATURE_WINDOWS as u64)
            .max(100);
        (0..audio.duration_ms)
            .step_by(window_ms as usize)
            .filter_map(|start_ms| {
                let end_ms = (start_ms + window_ms).min(audio.duration_ms);
                self.stats(start_ms, end_ms).map(|stats| VadSegment {
                    start_ms,
                    end_ms,
                    is_speech: stats.rms >= threshold,
                })
            })
            .collect()
    }
}

impl DiarizationPlugin for NativeAudioRuntime {
    /// Dependency-light diarization groups contiguous detected speech under one
    /// opaque channel identity; it never claims to identify a person.
    fn diarize(&self, audio: &AudioData) -> Vec<DiarizationSegment> {
        let speaker_ref = OpaqueRef::scoped("speaker", "0000000000000000")
            .expect("static opaque speaker reference is valid");
        let mut output: Vec<DiarizationSegment> = Vec::new();
        for segment in self
            .detect(audio)
            .into_iter()
            .filter(|segment| segment.is_speech)
        {
            if let Some(last) = output.last_mut() {
                if last.end_ms == segment.start_ms {
                    last.end_ms = segment.end_ms;
                    continue;
                }
            }
            output.push(DiarizationSegment {
                start_ms: segment.start_ms,
                end_ms: segment.end_ms,
                speaker_ref: speaker_ref.clone(),
            });
        }
        output
    }
}

impl TranscriptAlignmentPlugin for NativeAudioRuntime {
    fn align(
        &self,
        audio: &AudioData,
        transcript_ref: &OpaqueRef,
        unit_count: u32,
    ) -> Vec<TranscriptAlignment> {
        if !self.is_bound_to(audio) || unit_count == 0 || unit_count > MAX_ALIGNMENT_UNITS {
            return Vec::new();
        }
        (0..unit_count)
            .map(|unit| TranscriptAlignment {
                start_ms: audio.duration_ms * unit as u64 / unit_count as u64,
                end_ms: audio.duration_ms * (unit + 1) as u64 / unit_count as u64,
                transcript_ref: transcript_ref.clone(),
                unit_start: unit,
                unit_end: unit + 1,
            })
            .collect()
    }
}

fn dft_magnitude(samples: &[f32], bin: usize) -> f32 {
    if samples.is_empty() {
        return 0.0;
    }
    let (real, imag) =
        samples
            .iter()
            .enumerate()
            .fold((0.0f32, 0.0f32), |(real, imag), (index, sample)| {
                let phase = TAU * bin as f32 * index as f32 / samples.len() as f32;
                (real + sample * phase.cos(), imag - sample * phase.sin())
            });
    (real.mul_add(real, imag * imag)).sqrt() / samples.len() as f32
}

/// Produce the current eight-bin summary with at most 512 trigonometric samples.
/// Large windows are mean-pooled once, keeping extraction O(N) with a small fixed
/// spectral term instead of performing eight full scans with transcendental work.
fn bounded_spectral_bins(samples: &[f32]) -> [f32; 8] {
    if samples.is_empty() {
        return [0.0; 8];
    }
    let bucket_size = samples.len().div_ceil(MAX_DFT_SAMPLES).max(1);
    let pooled: Vec<f32> = samples
        .chunks(bucket_size)
        .take(MAX_DFT_SAMPLES)
        .map(|bucket| bucket.iter().sum::<f32>() / bucket.len() as f32)
        .collect();
    std::array::from_fn(|bin| dft_magnitude(&pooled, bin))
}

fn pcm_payload(bytes: &[u8]) -> Option<(u16, u16, &[u8])> {
    if bytes.len() < 12
        || &bytes[..4] != b"RIFF"
        || &bytes[8..12] != b"WAVE"
        || usize::try_from(u32::from_le_bytes(bytes[4..8].try_into().ok()?))
            .ok()?
            .checked_add(8)?
            != bytes.len()
    {
        return None;
    }
    let mut pos = 12usize;
    let mut format = None;
    let mut data = None;
    while pos + 8 <= bytes.len() {
        let id = &bytes[pos..pos + 4];
        let len = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().ok()?) as usize;
        let start = pos + 8;
        let end = start.checked_add(len)?;
        if end > bytes.len() {
            return None;
        }
        if id == b"fmt " && len >= 16 {
            if format.is_some() {
                return None;
            }
            let encoding = u16::from_le_bytes(bytes[start..start + 2].try_into().ok()?);
            if encoding != 1 {
                return None;
            }
            let channels = u16::from_le_bytes(bytes[start + 2..start + 4].try_into().ok()?);
            let bits = u16::from_le_bytes(bytes[start + 14..start + 16].try_into().ok()?);
            format = Some((channels, bits));
        } else if id == b"data" {
            if data.is_some() {
                return None;
            }
            data = Some(&bytes[start..end]);
        }
        pos = end.checked_add(len & 1)?;
        if pos > bytes.len() {
            return None;
        }
    }
    if pos != bytes.len() {
        return None;
    }
    let (channels, bits) = format?;
    Some((channels, bits, data?))
}

fn decode_mono(bytes: &[u8], channels: u16, bits: u16) -> Option<Vec<f32>> {
    let channels = channels as usize;
    match bits {
        8 => Some(
            bytes
                .chunks_exact(channels)
                .map(|frame| {
                    frame
                        .iter()
                        .map(|sample| (*sample as f32 - 128.0) / 128.0)
                        .sum::<f32>()
                        / channels as f32
                })
                .collect(),
        ),
        16 => {
            let frame_bytes = channels.checked_mul(2)?;
            Some(
                bytes
                    .chunks_exact(frame_bytes)
                    .map(|frame| {
                        frame
                            .chunks_exact(2)
                            .map(|sample| {
                                i16::from_le_bytes([sample[0], sample[1]]) as f32 / 32768.0
                            })
                            .sum::<f32>()
                            / channels as f32
                    })
                    .collect(),
            )
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wav_fixture() -> Vec<u8> {
        let samples: [i16; 8] = [0, 8_000, 16_000, 8_000, 0, -8_000, -16_000, -8_000];
        let data_len = (samples.len() * 2) as u32;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"RIFF");
        bytes.extend_from_slice(&(36 + data_len).to_le_bytes());
        bytes.extend_from_slice(b"WAVEfmt ");
        bytes.extend_from_slice(&16u32.to_le_bytes());
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(&8u32.to_le_bytes());
        bytes.extend_from_slice(&16u32.to_le_bytes());
        bytes.extend_from_slice(&2u16.to_le_bytes());
        bytes.extend_from_slice(&16u16.to_le_bytes());
        bytes.extend_from_slice(b"data");
        bytes.extend_from_slice(&data_len.to_le_bytes());
        for sample in samples {
            bytes.extend_from_slice(&sample.to_le_bytes());
        }
        bytes
    }

    #[test]
    fn native_runtime_computes_real_waveform_and_spectrum() {
        let bytes = wav_fixture();
        let audio = AudioData::from_bytes(&bytes, Vec::new()).unwrap();
        let runtime = NativeAudioRuntime::from_wav(&bytes).unwrap();
        let stats = runtime.decode_window(&audio, 0, audio.duration_ms).unwrap();
        assert!(stats.peak > 0.4);
        assert!(stats.rms > 0.2);
        let spectrum = runtime.compute(&audio);
        assert!(!spectrum.is_empty());
        assert_eq!(spectrum[0].bins.len(), 8);
    }

    #[test]
    fn native_runtime_returns_opaque_speaker_and_transcript_references() {
        let bytes = wav_fixture();
        let audio = AudioData::from_bytes(&bytes, Vec::new()).unwrap();
        let runtime = NativeAudioRuntime::from_wav(&bytes).unwrap();
        assert!(runtime
            .diarize(&audio)
            .iter()
            .all(|segment| segment.speaker_ref.namespace() == "speaker"));
        let transcript_ref = OpaqueRef::scoped("transcript", "0000000000000001").unwrap();
        let aligned = runtime.align(&audio, &transcript_ref, 2);
        assert_eq!(aligned.len(), 2);
        assert_eq!(aligned[0].transcript_ref, transcript_ref);
    }

    #[test]
    fn malformed_or_unsupported_audio_is_rejected() {
        assert!(NativeAudioRuntime::from_wav(b"not-a-wave").is_none());
    }
}
