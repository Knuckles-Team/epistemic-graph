use serde::{Deserialize, Serialize};

use super::{is_valid_digest, JobId, RenditionRef, RequestId, TtsError};

/// Output PCM encoding. Bounded to exactly one supported variant today —
/// mirroring `crate::ingress::Codec`'s "accepts exactly these codecs" posture
/// — so an unsupported request-side encoding is a typed rejection rather
/// than a best-effort transcode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputEncoding {
    Pcm16Le,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputFormat {
    pub encoding: OutputEncoding,
    pub sample_rate: u32,
    pub channels: u16,
}

impl OutputFormat {
    pub fn validate(&self) -> Result<(), TtsError> {
        if self.sample_rate == 0 || self.channels == 0 {
            return Err(TtsError::MalformedRequest {
                reason: "output sample_rate and channels must be positive",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ChunkSequence(pub u64);

/// Chunk-level audio quality. `Unavailable` is the honest default when a
/// worker does not measure a chunk; `Measured` carries a bounded, explicit
/// report — never a fabricated "clean" claim. Mirrors the calibrated-vs-
/// diagnostic discipline of `crate::asr::Quality`, adapted for synthesized
/// PCM rather than a recognition confidence.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum ChunkQuality {
    Unavailable,
    Measured {
        clipped_samples: u32,
        non_finite_samples: u32,
        peak_abs: f32,
    },
}

impl ChunkQuality {
    pub fn validate(&self) -> Result<(), TtsError> {
        match self {
            ChunkQuality::Unavailable => Ok(()),
            ChunkQuality::Measured { peak_abs, .. } => {
                if !peak_abs.is_finite() {
                    return Err(TtsError::MalformedResult {
                        reason: "peak_abs must be finite",
                    });
                }
                Ok(())
            }
        }
    }
}

/// `tts.chunk.v1` — an ordered, independently addressable slice of the
/// requested synthesis. Immutable once emitted.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TtsChunk {
    pub request_id: RequestId,
    pub job_id: JobId,
    pub sequence: ChunkSequence,
    pub phrase_index: u32,
    pub sample_offset: u64,
    pub sample_count: u32,
    pub sample_rate: u32,
    pub channels: u16,
    pub encoding: OutputEncoding,
    pub rendition_ref: RenditionRef,
    /// Lowercase hex SHA-256 over the chunk's committed PCM bytes.
    pub rendition_digest: String,
    pub is_final: bool,
    pub quality: ChunkQuality,
}

impl TtsChunk {
    pub fn validate(&self) -> Result<(), TtsError> {
        if self.sample_count == 0 {
            return Err(TtsError::MalformedResult {
                reason: "chunk sample_count must be positive",
            });
        }
        if self.sample_rate == 0 || self.channels == 0 {
            return Err(TtsError::MalformedResult {
                reason: "chunk sample_rate and channels must be positive",
            });
        }
        if !is_valid_digest(&self.rendition_digest) {
            return Err(TtsError::MalformedResult {
                reason: "rendition_digest is not a 64-char lowercase hex sha256",
            });
        }
        self.quality.validate()?;
        Ok(())
    }
}

/// Aggregated, bounded audio-quality evidence over an entire result. `0`
/// `non_finite_samples` is required for a `Succeeded` status — see
/// `finalize_result`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct QualitySummary {
    pub total_samples: u64,
    pub clipped_samples: u64,
    pub non_finite_samples: u64,
    pub chunks_with_unavailable_quality: u32,
}
