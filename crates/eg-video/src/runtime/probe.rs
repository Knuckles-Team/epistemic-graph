use eg_modality::{GovernedModality, NativePredicate, NativeProductionProbe};

use super::parser::parse_sample_sizes;
use super::{NativeVideoRuntime, MAX_FRAMES};

pub(super) fn production_probe() -> NativeProductionProbe {
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

pub(super) fn probe_container() -> Vec<u8> {
    let ftyp = probe_file_type();
    let samples: Vec<u8> = (0..12).collect();
    let mdat = encode_box(b"mdat", &samples);
    let sample_offset = (ftyp.len() + 8) as u32;
    let moov = probe_movie(sample_offset);
    let mut output = ftyp;
    output.extend_from_slice(&mdat);
    output.extend_from_slice(&moov);
    output
}

fn probe_file_type() -> Vec<u8> {
    let mut ftyp = Vec::new();
    ftyp.extend_from_slice(b"isom");
    ftyp.extend_from_slice(&0u32.to_be_bytes());
    ftyp.extend_from_slice(b"isom");
    encode_box(b"ftyp", &ftyp)
}

fn probe_movie(sample_offset: u32) -> Vec<u8> {
    let mut mvhd = vec![0; 100];
    mvhd[12..16].copy_from_slice(&1_000u32.to_be_bytes());
    mvhd[16..20].copy_from_slice(&2_000u32.to_be_bytes());
    let mut moov = encode_box(b"mvhd", &mvhd);
    moov.extend_from_slice(&probe_track(sample_offset));
    encode_box(b"moov", &moov)
}

fn probe_track(sample_offset: u32) -> Vec<u8> {
    let mut tkhd = vec![0; 84];
    tkhd[12..16].copy_from_slice(&1u32.to_be_bytes());
    let mut trak = encode_box(b"tkhd", &tkhd);
    trak.extend_from_slice(&probe_media(sample_offset));
    encode_box(b"trak", &trak)
}

fn probe_media(sample_offset: u32) -> Vec<u8> {
    let mut mdhd = vec![0; 24];
    mdhd[12..16].copy_from_slice(&1_000u32.to_be_bytes());
    mdhd[16..20].copy_from_slice(&2_000u32.to_be_bytes());
    let mut hdlr = vec![0; 24];
    hdlr[8..12].copy_from_slice(b"vide");
    let minf = encode_box(
        b"minf",
        &encode_box(b"stbl", &probe_sample_table(sample_offset)),
    );
    let mut mdia = encode_box(b"mdhd", &mdhd);
    mdia.extend_from_slice(&encode_box(b"hdlr", &hdlr));
    mdia.extend_from_slice(&minf);
    encode_box(b"mdia", &mdia)
}

fn probe_sample_table(sample_offset: u32) -> Vec<u8> {
    let mut stbl = encode_box(b"stsd", &probe_sample_description());
    stbl.extend_from_slice(&encode_box(b"stts", &probe_time_to_sample()));
    stbl.extend_from_slice(&encode_box(b"stsz", &probe_sample_sizes()));
    stbl.extend_from_slice(&encode_box(b"stco", &probe_chunk_offsets(sample_offset)));
    stbl.extend_from_slice(&encode_box(b"stsc", &probe_sample_to_chunk()));
    stbl.extend_from_slice(&encode_box(b"stss", &probe_sync_samples()));
    stbl
}

fn probe_sample_description() -> Vec<u8> {
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
    stsd
}

fn probe_time_to_sample() -> Vec<u8> {
    let mut stts_entries = Vec::new();
    stts_entries.extend_from_slice(&1u32.to_be_bytes());
    stts_entries.extend_from_slice(&2u32.to_be_bytes());
    stts_entries.extend_from_slice(&1_000u32.to_be_bytes());
    encode_fullbox(&stts_entries)
}

fn probe_sample_sizes() -> Vec<u8> {
    let mut stsz_entries = Vec::new();
    stsz_entries.extend_from_slice(&6u32.to_be_bytes());
    stsz_entries.extend_from_slice(&2u32.to_be_bytes());
    encode_fullbox(&stsz_entries)
}

fn probe_chunk_offsets(sample_offset: u32) -> Vec<u8> {
    let mut stco_entries = Vec::new();
    stco_entries.extend_from_slice(&1u32.to_be_bytes());
    stco_entries.extend_from_slice(&sample_offset.to_be_bytes());
    encode_fullbox(&stco_entries)
}

fn probe_sample_to_chunk() -> Vec<u8> {
    let mut stsc_entries = Vec::new();
    stsc_entries.extend_from_slice(&1u32.to_be_bytes());
    stsc_entries.extend_from_slice(&1u32.to_be_bytes());
    stsc_entries.extend_from_slice(&2u32.to_be_bytes());
    stsc_entries.extend_from_slice(&1u32.to_be_bytes());
    encode_fullbox(&stsc_entries)
}

fn probe_sync_samples() -> Vec<u8> {
    let mut stss_entries = Vec::new();
    stss_entries.extend_from_slice(&1u32.to_be_bytes());
    stss_entries.extend_from_slice(&1u32.to_be_bytes());
    encode_fullbox(&stss_entries)
}

/// Frame one ISOBMFF box: a big-endian `u32` total length (body + the 8-byte
/// header), the four-character kind, then the body.
///
/// `pub(super)` so the runtime's own tests build their fixture containers with
/// the SAME encoder the probe fixture uses, rather than a second copy that
/// could drift from it.
pub(super) fn encode_box(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
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
