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

const RIFF_HEADER_LEN: usize = 12;
const CHUNK_HEADER_LEN: usize = 8;
const PCM_FORMAT_LEN: usize = 16;

#[derive(Clone, Copy)]
struct PcmFormat {
    channels: u16,
    sample_rate: u32,
    byte_rate: u32,
    block_align: u16,
    bits_per_sample: u16,
}

#[derive(Clone, Copy)]
struct RiffChunk<'a> {
    id: &'a [u8],
    body: &'a [u8],
    next: usize,
}

#[derive(Default)]
struct ParsedChunks<'a> {
    format: Option<PcmFormat>,
    payload: Option<&'a [u8]>,
}

/// Parse a WAV/RIFF header: walks the RIFF chunk list for `fmt ` (channels/sample
/// rate/bit depth) and `data` (byte length), then computes `duration_ms` from the byte
/// rate implied by `fmt `. Returns `None` if the bytes aren't a `RIFF....WAVE`
/// container, `fmt ` is missing/malformed, or no `data` chunk was found.
pub fn read_wav_header(bytes: &[u8]) -> Option<WavInfo> {
    let riff_end = riff_end(bytes)?;
    let chunks = parse_chunks(bytes, riff_end)?;
    let format = chunks.format?;
    let payload = chunks.payload?;
    format.info(payload.len())
}

/// Same walk as [`read_wav_header`], but also returns the `data` chunk's raw
/// PCM bytes. Behind `runtime`: [`crate::runtime`]'s codec path is the only
/// consumer that needs the actual payload bytes rather than merely their
/// length (which `read_wav_header` above already covers via `format.info`).
/// Kept as its own function -- rather than a shared struct carrying a
/// `payload` field -- so a `runtime`-off build never declares a field nothing
/// in that configuration reads (that shared-struct shape previously tripped
/// `dead_code` under a feature set, such as `asr`, that never enables
/// `runtime`).
#[cfg(feature = "runtime")]
pub(crate) fn parse_wav_pcm(bytes: &[u8]) -> Option<(WavInfo, &[u8])> {
    let riff_end = riff_end(bytes)?;
    let chunks = parse_chunks(bytes, riff_end)?;
    let format = chunks.format?;
    let payload = chunks.payload?;
    Some((format.info(payload.len())?, payload))
}

fn riff_end(bytes: &[u8]) -> Option<usize> {
    if bytes.get(..4)? != b"RIFF" || bytes.get(8..12)? != b"WAVE" {
        return None;
    }
    usize::try_from(read_u32(bytes, 4)?)
        .ok()?
        .checked_add(8)
        .filter(|end| *end == bytes.len())
}

fn parse_chunks<'a>(bytes: &'a [u8], limit: usize) -> Option<ParsedChunks<'a>> {
    let mut chunks = ParsedChunks::default();
    let mut offset = RIFF_HEADER_LEN;
    while offset < limit {
        let chunk = next_chunk(bytes, offset, limit)?;
        chunks.accept(chunk)?;
        offset = chunk.next;
    }
    Some(chunks)
}

fn next_chunk<'a>(bytes: &'a [u8], offset: usize, limit: usize) -> Option<RiffChunk<'a>> {
    let header_end = offset.checked_add(CHUNK_HEADER_LEN)?;
    if header_end > limit {
        return None;
    }
    let chunk_len = usize::try_from(read_u32(bytes, offset.checked_add(4)?)?).ok()?;
    let body_end = header_end.checked_add(chunk_len)?;
    let next = body_end.checked_add(chunk_len % 2)?;
    if next > limit {
        return None;
    }
    Some(RiffChunk {
        id: bytes.get(offset..offset.checked_add(4)?)?,
        body: bytes.get(header_end..body_end)?,
        next,
    })
}

impl<'a> ParsedChunks<'a> {
    fn accept(&mut self, chunk: RiffChunk<'a>) -> Option<()> {
        if chunk.id == b"fmt " {
            if self.format.is_some() {
                return None;
            }
            self.format = Some(PcmFormat::parse(chunk.body)?);
        } else if chunk.id == b"data" {
            if self.payload.is_some() {
                return None;
            }
            self.payload = Some(chunk.body);
        }
        Some(())
    }
}

impl PcmFormat {
    fn parse(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < PCM_FORMAT_LEN || read_u16(bytes, 0)? != 1 {
            return None;
        }
        Some(Self {
            channels: read_u16(bytes, 2)?,
            sample_rate: read_u32(bytes, 4)?,
            byte_rate: read_u32(bytes, 8)?,
            block_align: read_u16(bytes, 12)?,
            bits_per_sample: read_u16(bytes, 14)?,
        })
    }

    fn info(self, data_len: usize) -> Option<WavInfo> {
        if self.sample_rate == 0
            || self.channels == 0
            || self.block_align == 0
            || !matches!(self.bits_per_sample, 8 | 16)
        {
            return None;
        }
        let expected_align =
            u32::from(self.channels).checked_mul(u32::from(self.bits_per_sample / 8))?;
        let expected_rate = self.sample_rate.checked_mul(expected_align)?;
        let data_len = u64::try_from(data_len).ok()?;
        if u32::from(self.block_align) != expected_align
            || self.byte_rate != expected_rate
            || data_len % u64::from(self.block_align) != 0
        {
            return None;
        }
        Some(WavInfo {
            sample_rate: self.sample_rate,
            channels: self.channels,
            bits_per_sample: self.bits_per_sample,
            duration_ms: data_len.checked_mul(1_000)? / u64::from(self.byte_rate),
        })
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        bytes.get(offset..offset.checked_add(2)?)?.try_into().ok()?,
    ))
}

fn read_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        bytes.get(offset..offset.checked_add(4)?)?.try_into().ok()?,
    ))
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
