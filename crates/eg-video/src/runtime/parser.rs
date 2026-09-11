use std::collections::BTreeSet;

use crate::{TrackKind, VideoFrame, VideoTrack};

use super::MAX_FRAMES;

pub(super) fn parse_brands(bytes: &[u8]) -> Option<Vec<[u8; 4]>> {
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

pub(super) fn parse_tracks_and_frames(bytes: &[u8]) -> Option<(Vec<VideoTrack>, Vec<VideoFrame>)> {
    let media_ranges = media_data_ranges(bytes)?;
    let moov = one_box(bytes, b"moov")?;
    validate_box_sequence(moov)?;
    validate_versioned_minimum(one_box(moov, b"mvhd")?, 100, 112)?;
    let mut tracks = Vec::new();
    let mut frames = Vec::new();
    for trak in boxes_named(moov, b"trak") {
        let track = parse_track(trak, &media_ranges, &tracks, frames.len(), &mut frames)?;
        tracks.push(track);
    }
    validate_frame_ranges(&frames)?;
    Some((tracks, frames))
}

struct TrackLayout {
    track_id: u32,
    kind: TrackKind,
    codec_fourcc: [u8; 4],
    width: u16,
    height: u16,
    pixel_depth: u16,
    timescale: u32,
    offsets: Vec<(u64, u32)>,
    deltas: Vec<u32>,
    sync: Option<BTreeSet<u32>>,
}

fn parse_track(
    trak: &[u8],
    media_ranges: &[(u64, u64)],
    existing_tracks: &[VideoTrack],
    existing_frame_count: usize,
    frames: &mut Vec<VideoFrame>,
) -> Option<VideoTrack> {
    let track_id = parse_track_id(trak)?;
    if existing_tracks
        .iter()
        .any(|track| track.track_id == track_id)
    {
        return None;
    }
    let layout = parse_track_layout(trak, media_ranges, track_id, existing_frame_count)?;
    append_track_frames(&layout, frames)?;
    Some(VideoTrack {
        track_id: layout.track_id,
        kind: layout.kind,
        codec_fourcc: layout.codec_fourcc,
        timescale: layout.timescale,
        width: layout.width,
        height: layout.height,
        pixel_depth: layout.pixel_depth,
    })
}

fn parse_track_id(trak: &[u8]) -> Option<u32> {
    validate_box_sequence(trak)?;
    let tkhd = one_box(trak, b"tkhd")?;
    validate_versioned_minimum(tkhd, 84, 96)?;
    let track_id = parse_versioned_u32(tkhd, 12, 20)?;
    (track_id > 0).then_some(track_id)
}

fn parse_track_layout(
    trak: &[u8],
    media_ranges: &[(u64, u64)],
    track_id: u32,
    existing_frame_count: usize,
) -> Option<TrackLayout> {
    let mdia = one_box(trak, b"mdia")?;
    let (kind, timescale, stbl) = parse_media_layout(mdia)?;
    let (codec_fourcc, width, height, pixel_depth) =
        parse_sample_description(one_box(stbl, b"stsd")?, kind)?;
    let sizes = parse_sample_sizes(one_box(stbl, b"stsz")?)?;
    if sizes.is_empty() || existing_frame_count.checked_add(sizes.len())? > MAX_FRAMES {
        return None;
    }
    let deltas = parse_time_to_sample(one_box(stbl, b"stts")?, sizes.len())?;
    let chunks = parse_chunk_offsets(stbl)?;
    let samples_per_chunk = parse_sample_to_chunk(one_box(stbl, b"stsc")?, chunks.len())?;
    let offsets = expand_offsets(&chunks, &samples_per_chunk, &sizes, media_ranges)?;
    let sync = match optional_one_box(stbl, b"stss")? {
        Some(bytes) => Some(parse_sync_samples(bytes, sizes.len())?),
        None => None,
    };
    Some(TrackLayout {
        track_id,
        kind,
        codec_fourcc,
        width,
        height,
        pixel_depth,
        timescale,
        offsets,
        deltas,
        sync,
    })
}

fn parse_media_layout(mdia: &[u8]) -> Option<(TrackKind, u32, &[u8])> {
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
    let kind = track_kind(handler);
    let minf = one_box(mdia, b"minf")?;
    validate_box_sequence(minf)?;
    let stbl = one_box(minf, b"stbl")?;
    validate_box_sequence(stbl)?;
    Some((kind, timescale, stbl))
}

fn track_kind(handler: [u8; 4]) -> TrackKind {
    match &handler {
        b"vide" => TrackKind::Video,
        b"soun" => TrackKind::Audio,
        b"subt" | b"text" => TrackKind::Subtitle,
        b"meta" => TrackKind::Metadata,
        _ => TrackKind::Unknown,
    }
}

fn append_track_frames(layout: &TrackLayout, frames: &mut Vec<VideoFrame>) -> Option<()> {
    frames.reserve(layout.offsets.len());
    let mut timestamp = 0u64;
    for (index, ((byte_offset, byte_length), delta)) in
        layout.offsets.iter().zip(&layout.deltas).enumerate()
    {
        let start_ms = timestamp.checked_mul(1_000)? / u64::from(layout.timescale);
        timestamp = timestamp.checked_add(u64::from(*delta))?;
        let end_ms = (timestamp.checked_mul(1_000)? / u64::from(layout.timescale))
            .max(start_ms.checked_add(1)?);
        let sample_number = index as u64 + 1;
        frames.push(VideoFrame {
            track_id: layout.track_id,
            frame_number: sample_number,
            start_ms,
            end_ms,
            byte_offset: *byte_offset,
            byte_length: *byte_length,
            keyframe: layout
                .sync
                .as_ref()
                .is_none_or(|samples| samples.contains(&(sample_number as u32))),
        });
    }
    Some(())
}

fn validate_frame_ranges(frames: &[VideoFrame]) -> Option<()> {
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
    Some(())
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

pub(super) fn parse_sample_sizes(bytes: &[u8]) -> Option<Vec<u32>> {
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
    match stco {
        Some(bytes) => parse_stco_offsets(bytes),
        None => parse_co64_offsets(co64?),
    }
}

fn parse_stco_offsets(bytes: &[u8]) -> Option<Vec<u64>> {
    let count = fullbox_u32(bytes, 4)? as usize;
    if count == 0 || count > MAX_FRAMES {
        return None;
    }
    if bytes.len() != 8usize.checked_add(count.checked_mul(4)?)? {
        return None;
    }
    (0..count)
        .map(|index| fullbox_u32(bytes, 8 + index * 4).map(u64::from))
        .collect()
}

fn parse_co64_offsets(bytes: &[u8]) -> Option<Vec<u64>> {
    let count = fullbox_u32(bytes, 4)? as usize;
    if count == 0 || count > MAX_FRAMES {
        return None;
    }
    if bytes.len() != 8usize.checked_add(count.checked_mul(8)?)? {
        return None;
    }
    (0..count)
        .map(|index| {
            Some(u64::from_be_bytes(
                bytes.get(8 + index * 8..16 + index * 8)?.try_into().ok()?,
            ))
        })
        .collect()
}

fn parse_sample_to_chunk(bytes: &[u8], chunk_count: usize) -> Option<Vec<u32>> {
    if chunk_count == 0 || chunk_count > MAX_FRAMES {
        return None;
    }
    let entries = parse_sample_to_chunk_entries(bytes)?;
    if entries.first()?.0 != 1 || !entries.windows(2).all(|pair| pair[0].0 < pair[1].0) {
        return None;
    }
    Some(
        (1..=chunk_count)
            .map(|chunk| samples_for_chunk(&entries, chunk))
            .collect(),
    )
}

fn parse_sample_to_chunk_entries(bytes: &[u8]) -> Option<Vec<(usize, u32)>> {
    let count = fullbox_u32(bytes, 4)? as usize;
    if count == 0 || count > MAX_FRAMES {
        return None;
    }
    if bytes.len() != 8usize.checked_add(count.checked_mul(12)?)? {
        return None;
    }
    (0..count)
        .map(|index| parse_sample_to_chunk_entry(bytes, index))
        .collect()
}

fn parse_sample_to_chunk_entry(bytes: &[u8], index: usize) -> Option<(usize, u32)> {
    let offset = 8usize.checked_add(index.checked_mul(12)?)?;
    let first_chunk = fullbox_u32(bytes, offset)? as usize;
    let samples = fullbox_u32(bytes, offset + 4)?;
    let description = fullbox_u32(bytes, offset + 8)?;
    (first_chunk > 0 && samples > 0 && description == 1).then_some((first_chunk, samples))
}

fn samples_for_chunk(entries: &[(usize, u32)], chunk: usize) -> u32 {
    entries
        .iter()
        .rev()
        .find(|(first, _)| *first <= chunk)
        .map(|(_, samples)| *samples)
        .unwrap_or(0)
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

pub(super) fn validate_box_sequence(bytes: &[u8]) -> Option<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_to_chunk_box(entries: &[(u32, u32, u32)]) -> Vec<u8> {
        let mut bytes = vec![0; 8];
        bytes[4..8].copy_from_slice(&(entries.len() as u32).to_be_bytes());
        for &(first_chunk, samples, description) in entries {
            bytes.extend_from_slice(&first_chunk.to_be_bytes());
            bytes.extend_from_slice(&samples.to_be_bytes());
            bytes.extend_from_slice(&description.to_be_bytes());
        }
        bytes
    }

    #[test]
    fn sample_to_chunk_expands_latest_entry_per_chunk() {
        let table = sample_to_chunk_box(&[(1, 2, 1), (3, 1, 1)]);
        assert_eq!(parse_sample_to_chunk(&table, 4), Some(vec![2, 2, 1, 1]));
    }

    #[test]
    fn sample_to_chunk_rejects_invalid_order_and_description() {
        let unordered = sample_to_chunk_box(&[(1, 2, 1), (1, 1, 1)]);
        let wrong_description = sample_to_chunk_box(&[(1, 2, 2)]);
        let mut truncated = sample_to_chunk_box(&[(1, 2, 1)]);
        truncated.pop();
        let mut oversized = vec![0; 8];
        oversized[4..8].copy_from_slice(&(MAX_FRAMES as u32 + 1).to_be_bytes());
        assert_eq!(parse_sample_to_chunk(&unordered, 2), None);
        assert_eq!(parse_sample_to_chunk(&wrong_description, 1), None);
        assert_eq!(parse_sample_to_chunk(&truncated, 1), None);
        assert_eq!(parse_sample_to_chunk(&oversized, 1), None);
    }
}
