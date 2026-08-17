//! Bounded, dependency-free 16 kHz mono 16-bit PCM WAV decode.
//!
//! Deliberately narrow: this provider's qualified input is exactly 16 kHz mono
//! 16-bit PCM (whisper.cpp's native sample rate), matching the platform
//! constraint that a provider must FAIL CLOSED on an unsupported sample rate or
//! format rather than silently resampling or mixing channels — resampling
//! quality/latency is unqualified here (GOC-33-W07) and channel mixing would
//! fabricate audio content. `eg_audio::header::read_wav_header` proves the
//! header facts; this module additionally locates the `data` chunk's byte
//! range so the PCM samples themselves can be decoded.

use eg_audio::asr::AsrError;

/// Decoded, provider-ready audio: 16 kHz mono `f32` PCM samples in whisper.cpp's
/// expected `[-1.0, 1.0]` range.
#[derive(Debug, PartialEq)]
pub struct DecodedAudio {
    pub samples: Vec<f32>,
    pub duration_ms: u64,
}

const REQUIRED_SAMPLE_RATE: u32 = 16_000;

/// Decode a WAV byte buffer, rejecting anything that is not exactly 16 kHz mono
/// 16-bit PCM. A malformed header, a truncated `data` chunk (declared length
/// exceeds the bytes actually present), or an odd number of PCM bytes are all
/// distinct, typed rejections — never a best-effort partial decode.
pub fn decode_wav_16k_mono(bytes: &[u8]) -> Result<DecodedAudio, AsrError> {
    let info = eg_audio::read_wav_header(bytes).ok_or(AsrError::MalformedRequest {
        reason: "not a well-formed RIFF/WAVE PCM header",
    })?;
    if info.bits_per_sample != 16 {
        return Err(AsrError::MalformedRequest {
            reason: "unsupported sample format: only 16-bit PCM is accepted",
        });
    }
    if info.channels != 1 {
        return Err(AsrError::MalformedRequest {
            reason: "unsupported channel layout: only mono is accepted",
        });
    }
    if info.sample_rate != REQUIRED_SAMPLE_RATE {
        return Err(AsrError::MalformedRequest {
            reason: "unsupported sample rate: only 16000 Hz is accepted",
        });
    }

    let data = locate_data_chunk(bytes).ok_or(AsrError::MalformedRequest {
        reason: "data chunk could not be relocated after header validation",
    })?;
    if data.len() % 2 != 0 {
        return Err(AsrError::MalformedRequest {
            reason: "truncated PCM16 data: odd byte length",
        });
    }
    if data.is_empty() {
        return Err(AsrError::MalformedRequest {
            reason: "zero-length audio data",
        });
    }
    let samples: Vec<f32> = data
        .chunks_exact(2)
        .map(|pair| i16::from_le_bytes([pair[0], pair[1]]) as f32 / 32_768.0)
        .collect();
    Ok(DecodedAudio {
        samples,
        duration_ms: info.duration_ms,
    })
}

/// Re-walk the RIFF chunk list to find the `data` chunk's byte range. Mirrors
/// `eg_audio::header::read_wav_header`'s own walk (that function proves the
/// facts but does not expose chunk offsets); kept intentionally separate and
/// re-validated rather than reaching into that module's private state, the
/// same dependency-light posture `eg_audio::asr` documents for itself.
fn locate_data_chunk(bytes: &[u8]) -> Option<&[u8]> {
    let mut pos = 12usize;
    while pos + 8 <= bytes.len() {
        let chunk_id = &bytes[pos..pos + 4];
        let chunk_size = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().ok()?) as usize;
        let body_start = pos + 8;
        let body_end = body_start.checked_add(chunk_size)?;
        if body_end > bytes.len() {
            return None;
        }
        if chunk_id == b"data" {
            return Some(&bytes[body_start..body_end]);
        }
        pos = body_end.checked_add(chunk_size % 2)?;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wav_fixture(sample_rate: u32, channels: u16, bits: u16, samples: &[i16]) -> Vec<u8> {
        let bytes_per_sample = (bits / 8) as u32;
        let block_align = bytes_per_sample * channels as u32;
        let data_size = samples.len() as u32 * bytes_per_sample;
        let byte_rate = sample_rate * block_align;
        let mut b = Vec::new();
        b.extend_from_slice(b"RIFF");
        b.extend_from_slice(&(36u32 + data_size).to_le_bytes());
        b.extend_from_slice(b"WAVE");
        b.extend_from_slice(b"fmt ");
        b.extend_from_slice(&16u32.to_le_bytes());
        b.extend_from_slice(&1u16.to_le_bytes());
        b.extend_from_slice(&channels.to_le_bytes());
        b.extend_from_slice(&sample_rate.to_le_bytes());
        b.extend_from_slice(&byte_rate.to_le_bytes());
        b.extend_from_slice(&(block_align as u16).to_le_bytes());
        b.extend_from_slice(&bits.to_le_bytes());
        b.extend_from_slice(b"data");
        b.extend_from_slice(&data_size.to_le_bytes());
        for s in samples {
            b.extend_from_slice(&s.to_le_bytes());
        }
        b
    }

    #[test]
    fn decodes_16k_mono_pcm16() {
        let samples = [0i16, 16_384, -16_384, 32_767, -32_768];
        let bytes = wav_fixture(16_000, 1, 16, &samples);
        let decoded = decode_wav_16k_mono(&bytes).expect("valid fixture decodes");
        assert_eq!(decoded.samples.len(), samples.len());
        assert!((decoded.samples[1] - 0.5).abs() < 0.001);
    }

    #[test]
    fn rejects_unsupported_sample_rate() {
        let bytes = wav_fixture(44_100, 1, 16, &[0, 0]);
        assert_eq!(
            decode_wav_16k_mono(&bytes),
            Err(AsrError::MalformedRequest {
                reason: "unsupported sample rate: only 16000 Hz is accepted"
            })
        );
    }

    #[test]
    fn rejects_stereo() {
        let bytes = wav_fixture(16_000, 2, 16, &[0, 0, 0, 0]);
        assert_eq!(
            decode_wav_16k_mono(&bytes),
            Err(AsrError::MalformedRequest {
                reason: "unsupported channel layout: only mono is accepted"
            })
        );
    }

    #[test]
    fn rejects_non_wav_bytes() {
        let bytes = b"definitely not a wav file".to_vec();
        assert_eq!(
            decode_wav_16k_mono(&bytes),
            Err(AsrError::MalformedRequest {
                reason: "not a well-formed RIFF/WAVE PCM header"
            })
        );
    }

    #[test]
    fn rejects_truncated_data_chunk() {
        // A header that CLAIMS more data than is actually present must fail the
        // header-level check before this module's own re-walk ever runs.
        let mut bytes = wav_fixture(16_000, 1, 16, &[0, 0, 0, 0]);
        // Declare the RIFF size as if 4 extra data bytes exist, but don't append
        // them: read_wav_header's own bytes.len() cross-check catches this.
        let claimed_extra = 4u32;
        let new_riff_size = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) + claimed_extra;
        bytes[4..8].copy_from_slice(&new_riff_size.to_le_bytes());
        let data_len_pos = bytes.len() - 8; // "data" + old size precede the samples
        let old_data_len =
            u32::from_le_bytes(bytes[data_len_pos..data_len_pos + 4].try_into().unwrap());
        bytes[data_len_pos..data_len_pos + 4]
            .copy_from_slice(&(old_data_len + claimed_extra).to_le_bytes());
        assert_eq!(
            decode_wav_16k_mono(&bytes),
            Err(AsrError::MalformedRequest {
                reason: "not a well-formed RIFF/WAVE PCM header"
            })
        );
    }
}
