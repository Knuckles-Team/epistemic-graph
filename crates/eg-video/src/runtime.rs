//! Bounded native ISOBMFF track/sample extraction and raw-frame decode.

use std::collections::BTreeSet;

use crate::{content_hash, read_mp4_duration_ms, TrackKind, VideoData, VideoFrame, VideoTrack};
use eg_modality::{GovernedModality, NativeProductionProbe};

mod parser;
mod probe;

use parser::{parse_brands, parse_tracks_and_frames, validate_box_sequence};

const MAX_FRAMES: usize = 200_000;
const MAX_SOURCE_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerInfo {
    pub compatible_brands: Vec<[u8; 4]>,
    pub tracks: Vec<VideoTrack>,
    pub frame_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedFrame {
    pub width: u16,
    pub height: u16,
    pub rgb: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TemporalSignature {
    pub start_ms: u64,
    pub end_ms: u64,
    pub vector: [f32; 4],
}

/// Request-local decoder. Source bytes are retained only long enough to serve exact
/// frame samples and are absent from `VideoData` and every durable snapshot.
#[derive(Clone, Debug)]
pub struct NativeVideoRuntime {
    source: Vec<u8>,
    value: VideoData,
    brands: Vec<[u8; 4]>,
}

impl NativeVideoRuntime {
    pub fn decode_isobmff(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > MAX_SOURCE_BYTES {
            return None;
        }
        validate_box_sequence(bytes)?;
        let duration_ms = read_mp4_duration_ms(bytes)?;
        if duration_ms == 0 {
            return None;
        }
        let brands = parse_brands(bytes)?;
        let (tracks, frames) = parse_tracks_and_frames(bytes)?;
        if frames.is_empty() || !tracks.iter().any(|track| track.kind == TrackKind::Video) {
            return None;
        }
        let video_frames = frames
            .iter()
            .filter(|frame| {
                tracks
                    .iter()
                    .any(|track| track.track_id == frame.track_id && track.kind == TrackKind::Video)
            })
            .count();
        let frame_rate = video_frames as f64 * 1_000.0 / duration_ms as f64;
        let value = VideoData::new(duration_ms, content_hash(bytes))
            .with_frame_rate(frame_rate)
            .with_native_index(tracks, frames);
        if !value.validate_governed_payload() {
            return None;
        }
        Some(Self {
            source: bytes.to_vec(),
            value,
            brands,
        })
    }

    pub fn inspect(&self) -> ContainerInfo {
        ContainerInfo {
            compatible_brands: self.brands.clone(),
            tracks: self.value.tracks.clone(),
            frame_count: self.value.frames.len(),
        }
    }

    pub fn normalized_data(&self) -> VideoData {
        self.value.clone()
    }

    pub fn encoded_frame(&self, track_id: u32, frame_number: u64) -> Option<&[u8]> {
        let frame = self
            .value
            .frames
            .iter()
            .find(|frame| frame.track_id == track_id && frame.frame_number == frame_number)?;
        let start = usize::try_from(frame.byte_offset).ok()?;
        let end = start.checked_add(frame.byte_length as usize)?;
        self.source.get(start..end)
    }

    /// Decode the current uncompressed 24-bit `raw ` RGB sample entry. Compressed codecs
    /// remain exact encoded-frame values and are never misreported as pixels.
    pub fn decode_raw_rgb(&self, track_id: u32, frame_number: u64) -> Option<DecodedFrame> {
        let track = self.value.tracks.iter().find(|track| {
            track.track_id == track_id && track.codec_fourcc == *b"raw " && track.pixel_depth == 24
        })?;
        let rgb = self.encoded_frame(track_id, frame_number)?.to_vec();
        let expected = usize::from(track.width)
            .checked_mul(usize::from(track.height))?
            .checked_mul(3)?;
        (rgb.len() == expected).then_some(DecodedFrame {
            width: track.width,
            height: track.height,
            rgb,
        })
    }

    pub fn keyframes(&self) -> Vec<&VideoFrame> {
        let video_tracks: BTreeSet<u32> = self
            .value
            .tracks
            .iter()
            .filter(|track| track.kind == TrackKind::Video)
            .map(|track| track.track_id)
            .collect();
        self.value
            .frames
            .iter()
            .filter(|frame| frame.keyframe && video_tracks.contains(&frame.track_id))
            .collect()
    }

    pub fn temporal_signatures(&self) -> Vec<TemporalSignature> {
        let duration = self.value.duration_ms as f32;
        let video_tracks: BTreeSet<u32> = self
            .value
            .tracks
            .iter()
            .filter(|track| track.kind == TrackKind::Video)
            .map(|track| track.track_id)
            .collect();
        self.value
            .frames
            .iter()
            .filter(|frame| video_tracks.contains(&frame.track_id))
            .map(|frame| TemporalSignature {
                start_ms: frame.start_ms,
                end_ms: frame.end_ms,
                vector: [
                    frame.start_ms as f32 / duration,
                    frame.end_ms as f32 / duration,
                    frame.byte_length as f32 / self.source.len().max(1) as f32,
                    if frame.keyframe { 1.0 } else { 0.0 },
                ],
            })
            .collect()
    }
}

pub fn production_probe() -> NativeProductionProbe {
    probe::production_probe()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_mp4_fixture() -> Vec<u8> {
        probe::probe_container()
    }

    #[test]
    fn runtime_extracts_track_timing_samples_keyframes_and_raw_pixels() {
        let bytes = raw_mp4_fixture();
        let runtime = NativeVideoRuntime::decode_isobmff(&bytes).unwrap();
        let info = runtime.inspect();
        assert_eq!(info.tracks.len(), 1);
        assert_eq!(info.frame_count, 2);
        assert_eq!(runtime.keyframes().len(), 1);
        let frame = runtime.decode_raw_rgb(1, 1).unwrap();
        assert_eq!((frame.width, frame.height), (2, 1));
        assert_eq!(frame.rgb, (0..6).collect::<Vec<_>>());
        assert_eq!(runtime.temporal_signatures().len(), 2);
    }

    #[test]
    fn malformed_or_metadata_only_container_is_rejected() {
        assert!(NativeVideoRuntime::decode_isobmff(b"not-a-container").is_none());
        let metadata_only = probe::encode_box(b"ftyp", b"isom\0\0\0\0");
        assert!(NativeVideoRuntime::decode_isobmff(&metadata_only).is_none());
        let mut ambiguous = raw_mp4_fixture();
        ambiguous.extend_from_slice(&probe::encode_box(b"ftyp", b"isom\0\0\0\0"));
        assert!(NativeVideoRuntime::decode_isobmff(&ambiguous).is_none());
    }
}
