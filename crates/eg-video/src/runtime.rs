//! Bounded native ISOBMFF track/sample extraction and raw-frame decode.

use std::collections::BTreeSet;

use crate::{content_hash, read_mp4_duration_ms, TrackKind, VideoData, VideoFrame, VideoTrack};
use eg_modality::{GovernedModality, NativePredicate, NativeProductionProbe};

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

fn parse_brands(bytes: &[u8]) -> Option<Vec<[u8; 4]>> {
    let ftyp = one_box(bytes, b"ftyp")?;
    if ftyp.len() < 8 || (ftyp.len() - 8) % 4 != 0 {
        return None;
    }
    let mut brands = Vec::new();
    brands.push(ftyp.get(0..4)?.try_into().ok()?);
    for brand in ftyp.get(8..)?.chunks_exact(4) {
        brands.push(brand.try_into().ok()?);
    }
    Some(brands)
}

fn parse_tracks_and_frames(bytes: &[u8]) -> Option<(Vec<VideoTrack>, Vec<VideoFrame>)> {
    let media_ranges = media_data_ranges(bytes)?;
    let moov = one_box(bytes, b"moov")?;
    validate_box_sequence(moov)?;
    validate_versioned_minimum(one_box(moov, b"mvhd")?, 100, 112)?;
    let mut tracks = Vec::new();
    let mut frames = Vec::new();
    for trak in boxes_named(moov, b"trak") {
        validate_box_sequence(trak)?;
        let tkhd = one_box(trak, b"tkhd")?;
        validate_versioned_minimum(tkhd, 84, 96)?;
        let track_id = parse_versioned_u32(tkhd, 12, 20)?;
        if track_id == 0
            || tracks
                .iter()
                .any(|track: &VideoTrack| track.track_id == track_id)
        {
            return None;
        }
        let mdia = one_box(trak, b"mdia")?;
        validate_box_sequence(mdia)?;
        let mdhd = one_box(mdia, b"mdhd")?;
        validate_versioned_minimum(mdhd, 24, 36)?;
        let timescale = parse_versioned_u32(mdhd, 12, 20)?;
        if timescale == 0 {
            return None;
        }
        let hdlr = one_box(mdia, b"hdlr")?;
        if hdlr.len() < 24 {
            return None;
        }
        let handler: [u8; 4] = hdlr.get(8..12)?.try_into().ok()?;
        let kind = match &handler {
            b"vide" => TrackKind::Video,
            b"soun" => TrackKind::Audio,
            b"subt" | b"text" => TrackKind::Subtitle,
            b"meta" => TrackKind::Metadata,
            _ => TrackKind::Unknown,
        };
        let minf = one_box(mdia, b"minf")?;
        validate_box_sequence(minf)?;
        let stbl = one_box(minf, b"stbl")?;
        validate_box_sequence(stbl)?;
        let (codec_fourcc, width, height, pixel_depth) =
            parse_sample_description(one_box(stbl, b"stsd")?, kind)?;
        let sizes = parse_sample_sizes(one_box(stbl, b"stsz")?)?;
        if sizes.is_empty() || frames.len().checked_add(sizes.len())? > MAX_FRAMES {
            return None;
        }
        let deltas = parse_time_to_sample(one_box(stbl, b"stts")?, sizes.len())?;
        let chunks = parse_chunk_offsets(stbl)?;
        let samples_per_chunk = parse_sample_to_chunk(one_box(stbl, b"stsc")?, chunks.len())?;
        let offsets = expand_offsets(&chunks, &samples_per_chunk, &sizes, &media_ranges)?;
        let sync = match optional_one_box(stbl, b"stss")? {
            Some(bytes) => Some(parse_sync_samples(bytes, sizes.len())?),
            None => None,
        };
        let mut timestamp = 0u64;
        for (index, ((byte_offset, byte_length), delta)) in
            offsets.into_iter().zip(deltas).enumerate()
        {
            let start_ms = timestamp.checked_mul(1_000)? / u64::from(timescale);
            timestamp = timestamp.checked_add(u64::from(delta))?;
            let end_ms = (timestamp.checked_mul(1_000)? / u64::from(timescale))
                .max(start_ms.checked_add(1)?);
            let sample_number = index as u64 + 1;
            frames.push(VideoFrame {
                track_id,
                frame_number: sample_number,
                start_ms,
                end_ms,
                byte_offset,
                byte_length,
                keyframe: sync
                    .as_ref()
                    .is_none_or(|samples| samples.contains(&(sample_number as u32))),
            });
        }
        tracks.push(VideoTrack {
            track_id,
            kind,
            codec_fourcc,
            timescale,
            width,
            height,
            pixel_depth,
        });
    }
    let mut byte_ranges: Vec<(u64, u64)> = frames
        .iter()
        .map(|frame| {
            Some((
                frame.byte_offset,
                frame
                    .byte_offset
                    .checked_add(u64::from(frame.byte_length))?,
            ))
        })
        .collect::<Option<_>>()?;
    byte_ranges.sort_unstable();
    if byte_ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return None;
    }
    Some((tracks, frames))
}

fn parse_sample_description(bytes: &[u8], kind: TrackKind) -> Option<([u8; 4], u16, u16, u16)> {
    if fullbox_u32(bytes, 4)? != 1 {
        return None;
    }
    let entry_size = u32::from_be_bytes(bytes.get(8..12)?.try_into().ok()?) as usize;
    let minimum_size = match kind {
        TrackKind::Video => 86,
        TrackKind::Audio => 36,
        _ => 16,
    };
    if entry_size < minimum_size || entry_size.checked_add(8)? != bytes.len() {
        return None;
    }
    let codec = bytes.get(12..16)?.try_into().ok()?;
    if u16::from_be_bytes(bytes.get(22..24)?.try_into().ok()?) == 0 {
        return None;
    }
    let (width, height, pixel_depth) = if kind == TrackKind::Video {
        (
            u16::from_be_bytes(bytes.get(40..42)?.try_into().ok()?),
            u16::from_be_bytes(bytes.get(42..44)?.try_into().ok()?),
            u16::from_be_bytes(bytes.get(90..92)?.try_into().ok()?),
        )
    } else {
        (0, 0, 0)
    };
    Some((codec, width, height, pixel_depth))
}

fn parse_sample_sizes(bytes: &[u8]) -> Option<Vec<u32>> {
    let uniform = fullbox_u32(bytes, 4)?;
    let count = fullbox_u32(bytes, 8)? as usize;
    if count == 0 || count > MAX_FRAMES {
        return None;
    }
    if uniform != 0 {
        return (bytes.len() == 12).then(|| vec![uniform; count]);
    }
    if bytes.len() != 12usize.checked_add(count.checked_mul(4)?)? {
        return None;
    }
    (0..count)
        .map(|index| fullbox_u32(bytes, 12 + index * 4))
        .collect()
}

fn parse_time_to_sample(bytes: &[u8], expected: usize) -> Option<Vec<u32>> {
    let entries = fullbox_u32(bytes, 4)? as usize;
    if entries == 0 || entries > MAX_FRAMES {
        return None;
    }
    if bytes.len() != 8usize.checked_add(entries.checked_mul(8)?)? {
        return None;
    }
    let mut output = Vec::with_capacity(expected);
    for index in 0..entries {
        let offset = 8usize.checked_add(index.checked_mul(8)?)?;
        let count = fullbox_u32(bytes, offset)? as usize;
        let delta = fullbox_u32(bytes, offset + 4)?;
        if count == 0 || delta == 0 || output.len().checked_add(count)? > expected {
            return None;
        }
        output.extend(std::iter::repeat_n(delta, count));
    }
    (output.len() == expected).then_some(output)
}

fn parse_chunk_offsets(stbl: &[u8]) -> Option<Vec<u64>> {
    let stco = optional_one_box(stbl, b"stco")?;
    let co64 = optional_one_box(stbl, b"co64")?;
    if stco.is_some() == co64.is_some() {
        return None;
    }
    if let Some(stco) = stco {
        let count = fullbox_u32(stco, 4)? as usize;
        if count == 0 || count > MAX_FRAMES {
            return None;
        }
        if stco.len() != 8usize.checked_add(count.checked_mul(4)?)? {
            return None;
        }
        return (0..count)
            .map(|index| fullbox_u32(stco, 8 + index * 4).map(u64::from))
            .collect();
    }
    let co64 = co64?;
    let count = fullbox_u32(co64, 4)? as usize;
    if count == 0 || count > MAX_FRAMES {
        return None;
    }
    if co64.len() != 8usize.checked_add(count.checked_mul(8)?)? {
        return None;
    }
    (0..count)
        .map(|index| {
            Some(u64::from_be_bytes(
                co64.get(8 + index * 8..16 + index * 8)?.try_into().ok()?,
            ))
        })
        .collect()
}

fn parse_sample_to_chunk(bytes: &[u8], chunk_count: usize) -> Option<Vec<u32>> {
    let count = fullbox_u32(bytes, 4)? as usize;
    if count == 0 || count > MAX_FRAMES || chunk_count == 0 || chunk_count > MAX_FRAMES {
        return None;
    }
    if bytes.len() != 8usize.checked_add(count.checked_mul(12)?)? {
        return None;
    }
    let mut entries = Vec::with_capacity(count);
    for index in 0..count {
        let offset = 8 + index * 12;
        let first_chunk = fullbox_u32(bytes, offset)? as usize;
        let samples = fullbox_u32(bytes, offset + 4)?;
        let description = fullbox_u32(bytes, offset + 8)?;
        if first_chunk == 0 || samples == 0 || description != 1 {
            return None;
        }
        entries.push((first_chunk, samples));
    }
    if entries.first()?.0 != 1 || !entries.windows(2).all(|pair| pair[0].0 < pair[1].0) {
        return None;
    }
    Some(
        (1..=chunk_count)
            .map(|chunk| {
                entries
                    .iter()
                    .rev()
                    .find(|(first, _)| *first <= chunk)
                    .map(|(_, samples)| *samples)
                    .unwrap_or(0)
            })
            .collect(),
    )
}

fn expand_offsets(
    chunks: &[u64],
    samples_per_chunk: &[u32],
    sizes: &[u32],
    media_ranges: &[(u64, u64)],
) -> Option<Vec<(u64, u32)>> {
    let mut output = Vec::with_capacity(sizes.len());
    let mut sample = 0usize;
    for (chunk_offset, count) in chunks.iter().zip(samples_per_chunk) {
        let mut offset = *chunk_offset;
        for _ in 0..*count {
            let size = *sizes.get(sample)?;
            let end = offset.checked_add(u64::from(size))?;
            if !media_ranges
                .iter()
                .any(|(start, range_end)| offset >= *start && end <= *range_end)
            {
                return None;
            }
            output.push((offset, size));
            offset = end;
            sample += 1;
        }
    }
    (sample == sizes.len()).then_some(output)
}

fn parse_sync_samples(bytes: &[u8], sample_count: usize) -> Option<BTreeSet<u32>> {
    let count = fullbox_u32(bytes, 4)? as usize;
    if count > MAX_FRAMES {
        return None;
    }
    if bytes.len() != 8usize.checked_add(count.checked_mul(4)?)? {
        return None;
    }
    let samples: BTreeSet<u32> = (0..count)
        .map(|index| fullbox_u32(bytes, 8 + index * 4))
        .collect::<Option<_>>()?;
    (samples.len() == count
        && samples.iter().all(|sample| {
            *sample > 0 && usize::try_from(*sample).is_ok_and(|sample| sample <= sample_count)
        }))
    .then_some(samples)
}

fn fullbox_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_be_bytes(
        bytes.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn parse_versioned_u32(
    bytes: &[u8],
    version_zero_offset: usize,
    version_one_offset: usize,
) -> Option<u32> {
    match bytes.first()? {
        0 => fullbox_u32(bytes, version_zero_offset),
        1 => fullbox_u32(bytes, version_one_offset),
        _ => None,
    }
}

fn validate_versioned_minimum(bytes: &[u8], version_zero: usize, version_one: usize) -> Option<()> {
    let minimum = match bytes.first()? {
        0 => version_zero,
        1 => version_one,
        _ => return None,
    };
    (bytes.len() >= minimum).then_some(())
}

fn box_header(bytes: &[u8]) -> Option<([u8; 4], usize, usize)> {
    let size = u32::from_be_bytes(bytes.get(0..4)?.try_into().ok()?) as usize;
    let kind = bytes.get(4..8)?.try_into().ok()?;
    if size == 0 {
        Some((kind, 8, bytes.len().checked_sub(8)?))
    } else if size == 1 {
        let extended =
            usize::try_from(u64::from_be_bytes(bytes.get(8..16)?.try_into().ok()?)).ok()?;
        Some((kind, 16, extended.checked_sub(16)?))
    } else {
        Some((kind, 8, size.checked_sub(8)?))
    }
}

fn validate_box_sequence(bytes: &[u8]) -> Option<()> {
    let mut position = 0usize;
    while position < bytes.len() {
        let (_, header, body_len) = box_header(bytes.get(position..)?)?;
        position = position.checked_add(header)?.checked_add(body_len)?;
        if position > bytes.len() {
            return None;
        }
    }
    (position == bytes.len()).then_some(())
}

fn media_data_ranges(bytes: &[u8]) -> Option<Vec<(u64, u64)>> {
    let mut ranges = Vec::new();
    let mut position = 0usize;
    while position < bytes.len() {
        let (kind, header, body_len) = box_header(bytes.get(position..)?)?;
        let start = position.checked_add(header)?;
        let end = start.checked_add(body_len)?;
        if end > bytes.len() {
            return None;
        }
        if &kind == b"mdat" && start < end {
            ranges.push((start as u64, end as u64));
        }
        position = end;
    }
    (!ranges.is_empty()).then_some(ranges)
}

fn boxes(bytes: &[u8]) -> impl Iterator<Item = ([u8; 4], &[u8])> {
    let mut position = 0usize;
    std::iter::from_fn(move || {
        let (kind, header, body_len) = box_header(bytes.get(position..)?)?;
        let start = position.checked_add(header)?;
        let end = start.checked_add(body_len)?;
        let body = bytes.get(start..end)?;
        position = end;
        Some((kind, body))
    })
}

fn one_box<'a>(bytes: &'a [u8], target: &[u8; 4]) -> Option<&'a [u8]> {
    let mut matches = boxes_named(bytes, target);
    let value = matches.next()?;
    matches.next().is_none().then_some(value)
}

fn optional_one_box<'a>(bytes: &'a [u8], target: &[u8; 4]) -> Option<Option<&'a [u8]>> {
    let mut matches = boxes_named(bytes, target);
    let value = matches.next();
    if matches.next().is_some() {
        return None;
    }
    Some(value)
}

fn boxes_named<'a>(bytes: &'a [u8], target: &[u8; 4]) -> impl Iterator<Item = &'a [u8]> {
    let target = *target;
    boxes(bytes).filter_map(move |(kind, body)| (kind == target).then_some(body))
}

pub fn production_probe() -> NativeProductionProbe {
    let bytes = probe_container();
    let runtime = NativeVideoRuntime::decode_isobmff(&bytes);
    let value = runtime.as_ref().map(NativeVideoRuntime::normalized_data);
    let codec = runtime
        .as_ref()
        .and_then(|runtime| runtime.decode_raw_rgb(1, 1))
        .is_some();
    let normalized_payload = value
        .as_ref()
        .is_some_and(GovernedModality::validate_governed_payload);
    let secondary_index = value
        .as_ref()
        .is_some_and(|value| !value.native_index_keys().is_empty());
    let typed_query = value.as_ref().is_some_and(|value| {
        value.matches_native_predicate(&NativePredicate::VideoWindow {
            start_ms: 0,
            end_ms: value.duration_ms,
            keyframes_only: true,
        })
    });
    let mut oversized_stsz = vec![0; 4];
    oversized_stsz.extend_from_slice(&0u32.to_be_bytes());
    oversized_stsz.extend_from_slice(&(MAX_FRAMES as u32 + 1).to_be_bytes());
    let malformed_and_resource_bounds = NativeVideoRuntime::decode_isobmff(b"invalid").is_none()
        && parse_sample_sizes(&oversized_stsz).is_none();
    NativeProductionProbe {
        codec,
        normalized_payload,
        secondary_index,
        typed_query,
        malformed_and_resource_bounds,
    }
}

fn encode_box(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(&(body.len() as u32 + 8).to_be_bytes());
    output.extend_from_slice(kind);
    output.extend_from_slice(body);
    output
}

fn encode_fullbox(entries: &[u8]) -> Vec<u8> {
    let mut output = vec![0; 4];
    output.extend_from_slice(entries);
    output
}

fn probe_container() -> Vec<u8> {
    let mut ftyp = Vec::new();
    ftyp.extend_from_slice(b"isom");
    ftyp.extend_from_slice(&0u32.to_be_bytes());
    ftyp.extend_from_slice(b"isom");
    let ftyp = encode_box(b"ftyp", &ftyp);
    let samples: Vec<u8> = (0..12).collect();
    let mdat = encode_box(b"mdat", &samples);
    let sample_offset = (ftyp.len() + 8) as u32;
    let mut mvhd = vec![0; 100];
    mvhd[12..16].copy_from_slice(&1_000u32.to_be_bytes());
    mvhd[16..20].copy_from_slice(&2_000u32.to_be_bytes());
    let mut tkhd = vec![0; 84];
    tkhd[12..16].copy_from_slice(&1u32.to_be_bytes());
    let mut mdhd = vec![0; 24];
    mdhd[12..16].copy_from_slice(&1_000u32.to_be_bytes());
    mdhd[16..20].copy_from_slice(&2_000u32.to_be_bytes());
    let mut hdlr = vec![0; 24];
    hdlr[8..12].copy_from_slice(b"vide");
    let mut sample_body = vec![0; 78];
    sample_body[6..8].copy_from_slice(&1u16.to_be_bytes());
    sample_body[24..26].copy_from_slice(&2u16.to_be_bytes());
    sample_body[26..28].copy_from_slice(&1u16.to_be_bytes());
    sample_body[40..42].copy_from_slice(&1u16.to_be_bytes());
    sample_body[74..76].copy_from_slice(&24u16.to_be_bytes());
    sample_body[76..78].copy_from_slice(&u16::MAX.to_be_bytes());
    let mut sample_entry = Vec::new();
    sample_entry.extend_from_slice(&(sample_body.len() as u32 + 8).to_be_bytes());
    sample_entry.extend_from_slice(b"raw ");
    sample_entry.extend_from_slice(&sample_body);
    let mut stsd = encode_fullbox(&1u32.to_be_bytes());
    stsd.extend_from_slice(&sample_entry);
    let mut stts_entries = Vec::new();
    stts_entries.extend_from_slice(&1u32.to_be_bytes());
    stts_entries.extend_from_slice(&2u32.to_be_bytes());
    stts_entries.extend_from_slice(&1_000u32.to_be_bytes());
    let stts = encode_fullbox(&stts_entries);
    let mut stsz_entries = Vec::new();
    stsz_entries.extend_from_slice(&6u32.to_be_bytes());
    stsz_entries.extend_from_slice(&2u32.to_be_bytes());
    let stsz = encode_fullbox(&stsz_entries);
    let mut stco_entries = Vec::new();
    stco_entries.extend_from_slice(&1u32.to_be_bytes());
    stco_entries.extend_from_slice(&sample_offset.to_be_bytes());
    let stco = encode_fullbox(&stco_entries);
    let mut stsc_entries = Vec::new();
    stsc_entries.extend_from_slice(&1u32.to_be_bytes());
    stsc_entries.extend_from_slice(&1u32.to_be_bytes());
    stsc_entries.extend_from_slice(&2u32.to_be_bytes());
    stsc_entries.extend_from_slice(&1u32.to_be_bytes());
    let stsc = encode_fullbox(&stsc_entries);
    let mut stss_entries = Vec::new();
    stss_entries.extend_from_slice(&1u32.to_be_bytes());
    stss_entries.extend_from_slice(&1u32.to_be_bytes());
    let stss = encode_fullbox(&stss_entries);
    let mut stbl = encode_box(b"stsd", &stsd);
    stbl.extend_from_slice(&encode_box(b"stts", &stts));
    stbl.extend_from_slice(&encode_box(b"stsz", &stsz));
    stbl.extend_from_slice(&encode_box(b"stco", &stco));
    stbl.extend_from_slice(&encode_box(b"stsc", &stsc));
    stbl.extend_from_slice(&encode_box(b"stss", &stss));
    let minf = encode_box(b"minf", &encode_box(b"stbl", &stbl));
    let mut mdia = encode_box(b"mdhd", &mdhd);
    mdia.extend_from_slice(&encode_box(b"hdlr", &hdlr));
    mdia.extend_from_slice(&minf);
    let mut trak = encode_box(b"tkhd", &tkhd);
    trak.extend_from_slice(&encode_box(b"mdia", &mdia));
    let mut moov = encode_box(b"mvhd", &mvhd);
    moov.extend_from_slice(&encode_box(b"trak", &trak));
    let mut output = ftyp;
    output.extend_from_slice(&mdat);
    output.extend_from_slice(&encode_box(b"moov", &moov));
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boxed(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
        let mut output = Vec::new();
        output.extend_from_slice(&(body.len() as u32 + 8).to_be_bytes());
        output.extend_from_slice(kind);
        output.extend_from_slice(body);
        output
    }

    fn raw_mp4_fixture() -> Vec<u8> {
        probe_container()
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
        let metadata_only = boxed(b"ftyp", b"isom\0\0\0\0");
        assert!(NativeVideoRuntime::decode_isobmff(&metadata_only).is_none());
        let mut ambiguous = raw_mp4_fixture();
        ambiguous.extend_from_slice(&boxed(b"ftyp", b"isom\0\0\0\0"));
        assert!(NativeVideoRuntime::decode_isobmff(&ambiguous).is_none());
    }
}
