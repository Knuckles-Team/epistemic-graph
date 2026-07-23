//! Dependency-free WAV/RIFF header parsing: real `sample_rate`/`channels`/
//! `bits_per_sample`/`duration_ms` computed straight from the `fmt ` + `data` chunk
//! sizes — NOT a waveform decode. WAV (uncompressed PCM in a RIFF container) is the
//! one audio format simple enough to read honestly with no codec at all: duration
//! falls out of arithmetic over the declared byte rate and the `data` chunk's byte
//! length, no entropy decoding involved.

use sha2::{Digest, Sha256};

/// The real facts read out of a WAV file's header — no samples are decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WavInfo {
    pub sample_rate: u32,
    pub channels: u16,
    pub bits_per_sample: u16,
    pub duration_ms: u64,
}

/// Parse a WAV/RIFF header: walks the RIFF chunk list for `fmt ` (channels/sample
/// rate/bit depth) and `data` (byte length), then computes `duration_ms` from the byte
/// rate implied by `fmt `. Returns `None` if the bytes aren't a `RIFF....WAVE`
/// container, `fmt ` is missing/malformed, or no `data` chunk was found.
pub fn read_wav_header(bytes: &[u8]) -> Option<WavInfo> {
    if bytes.len() < 12
        || &bytes[0..4] != b"RIFF"
        || &bytes[8..12] != b"WAVE"
        || usize::try_from(u32::from_le_bytes(bytes[4..8].try_into().ok()?))
            .ok()?
            .checked_add(8)?
            != bytes.len()
    {
        return None;
    }
    let mut pos = 12usize;
    let mut sample_rate = 0u32;
    let mut channels = 0u16;
    let mut bits_per_sample = 0u16;
    let mut block_align = 0u16;
    let mut byte_rate = 0u32;
    let mut saw_format = false;
    let mut data_len: Option<u64> = None;

    while pos + 8 <= bytes.len() {
        let chunk_id = &bytes[pos..pos + 4];
        let chunk_size = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().ok()?) as usize;
        let body_start = pos + 8;
        let body_end = match body_start.checked_add(chunk_size) {
            Some(e) if e <= bytes.len() => e,
            _ => return None,
        };
        match chunk_id {
            b"fmt " => {
                if saw_format || chunk_size < 16 {
                    return None;
                }
                let body = &bytes[body_start..body_end];
                if u16::from_le_bytes(body[0..2].try_into().ok()?) != 1 {
                    return None;
                }
                channels = u16::from_le_bytes(body[2..4].try_into().ok()?);
                sample_rate = u32::from_le_bytes(body[4..8].try_into().ok()?);
                byte_rate = u32::from_le_bytes(body[8..12].try_into().ok()?);
                block_align = u16::from_le_bytes(body[12..14].try_into().ok()?);
                bits_per_sample = u16::from_le_bytes(body[14..16].try_into().ok()?);
                saw_format = true;
            }
            b"data" => {
                if data_len.is_some() {
                    return None;
                }
                data_len = Some(chunk_size as u64);
            }
            _ => {}
        }
        // RIFF chunks are word-aligned: an odd chunk_size has one pad byte after it.
        pos = body_end.checked_add(chunk_size % 2)?;
        if pos > bytes.len() {
            return None;
        }
    }

    if pos != bytes.len() {
        return None;
    }

    let data_len = data_len?;
    if sample_rate == 0 || channels == 0 || !matches!(bits_per_sample, 8 | 16) || block_align == 0 {
        return None;
    }
    let expected_align = u32::from(channels).checked_mul(u32::from(bits_per_sample / 8))?;
    let expected_rate = sample_rate.checked_mul(expected_align)?;
    if u32::from(block_align) != expected_align
        || byte_rate != expected_rate
        || data_len % u64::from(block_align) != 0
    {
        return None;
    }
    let duration_ms = data_len.checked_mul(1_000)? / u64::from(byte_rate);
    Some(WavInfo {
        sample_rate,
        channels,
        bits_per_sample,
        duration_ms,
    })
}

/// SHA-256 content address rendered as 64 lowercase hexadecimal characters.
pub fn content_hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal, structurally valid WAV: RIFF/WAVE + a 16-byte PCM `fmt ` chunk + a
    /// `data` chunk of `num_samples` (per channel) silent frames.
    fn wav_fixture(
        sample_rate: u32,
        channels: u16,
        bits_per_sample: u16,
        num_frames: u32,
    ) -> Vec<u8> {
        let bytes_per_sample = (bits_per_sample / 8) as u32;
        let block_align = bytes_per_sample * channels as u32;
        let data_size = num_frames * block_align;
        let byte_rate = sample_rate * block_align;

        let mut b = Vec::new();
        b.extend_from_slice(b"RIFF");
        b.extend_from_slice(&(36u32 + data_size).to_le_bytes());
        b.extend_from_slice(b"WAVE");
        // fmt chunk
        b.extend_from_slice(b"fmt ");
        b.extend_from_slice(&16u32.to_le_bytes());
        b.extend_from_slice(&1u16.to_le_bytes()); // PCM
        b.extend_from_slice(&channels.to_le_bytes());
        b.extend_from_slice(&sample_rate.to_le_bytes());
        b.extend_from_slice(&byte_rate.to_le_bytes());
        b.extend_from_slice(&(block_align as u16).to_le_bytes());
        b.extend_from_slice(&bits_per_sample.to_le_bytes());
        // data chunk
        b.extend_from_slice(b"data");
        b.extend_from_slice(&data_size.to_le_bytes());
        b.extend(std::iter::repeat_n(0u8, data_size as usize));
        b
    }

    #[test]
    fn wav_header_reports_real_sample_rate_and_duration() {
        // 1 second of 16-bit mono @ 44100 Hz.
        let bytes = wav_fixture(44_100, 1, 16, 44_100);
        let info = read_wav_header(&bytes).expect("valid WAV header");
        assert_eq!(info.sample_rate, 44_100);
        assert_eq!(info.channels, 1);
        assert_eq!(info.bits_per_sample, 16);
        assert_eq!(info.duration_ms, 1000);
    }

    #[test]
    fn wav_header_handles_stereo_and_partial_seconds() {
        // 500ms of 16-bit stereo @ 8000 Hz = 4000 frames.
        let bytes = wav_fixture(8_000, 2, 16, 4_000);
        let info = read_wav_header(&bytes).expect("valid WAV header");
        assert_eq!(info.channels, 2);
        assert_eq!(info.duration_ms, 500);
    }

    #[test]
    fn rejects_non_riff_bytes() {
        assert_eq!(read_wav_header(b"not a wav file at all!!"), None);
    }

    #[test]
    fn rejects_missing_data_chunk() {
        let mut b = Vec::new();
        b.extend_from_slice(b"RIFF");
        b.extend_from_slice(&20u32.to_le_bytes());
        b.extend_from_slice(b"WAVE");
        b.extend_from_slice(b"fmt ");
        b.extend_from_slice(&16u32.to_le_bytes());
        b.extend_from_slice(&1u16.to_le_bytes());
        b.extend_from_slice(&1u16.to_le_bytes());
        b.extend_from_slice(&44_100u32.to_le_bytes());
        b.extend_from_slice(&88_200u32.to_le_bytes());
        b.extend_from_slice(&2u16.to_le_bytes());
        b.extend_from_slice(&16u16.to_le_bytes());
        assert_eq!(read_wav_header(&b), None);
    }

    #[test]
    fn content_hash_is_deterministic() {
        assert_eq!(content_hash(b"abc"), content_hash(b"abc"));
        assert_ne!(content_hash(b"abc"), content_hash(b"abd"));
    }
}
