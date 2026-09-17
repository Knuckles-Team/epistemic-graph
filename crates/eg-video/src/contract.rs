//! Governed modality contract for [`VideoData`].

use eg_modality::{
    encode_staged, temporal_buckets, ConformanceTestable, EvidenceAddress, GovernedModality,
    ModalityContract, NativeIndexKey, NativePredicate, OpaqueRef, Provenance, RowSetShape,
    StagedWrite,
};
use std::collections::{BTreeMap, BTreeSet};

use crate::video::{TrackKind, VideoData, VideoFrame, VideoShot, VideoTrack};

const MAX_TRACKS: usize = 1_024;
const MAX_FRAMES: usize = 200_000;
const MAX_SHOTS: usize = 65_536;
const MAX_VIDEO_PIXELS: u64 = 8_388_608;

fn opaque(value: &str) -> bool {
    OpaqueRef::new(value.to_string()).is_ok()
}

fn validate_video_header(video: &VideoData) -> bool {
    video.duration_ms > 0
        && eg_modality::content_address(&video.blob_ref)
        && video
            .frame_rate
            .is_none_or(|rate| rate.is_finite() && rate > 0.0)
        && video.shots.len() <= MAX_SHOTS
}

fn validate_shots(video: &VideoData) -> bool {
    video.shots.iter().all(|shot| {
        shot.end_ms > shot.start_ms
            && shot.end_ms <= video.duration_ms
            && shot.label.as_deref().is_none_or(opaque)
    })
}

fn validate_codec(track: &VideoTrack) -> bool {
    track
        .codec_fourcc
        .iter()
        .all(|byte| byte.is_ascii_graphic() || *byte == b' ')
        && track.codec_fourcc != *b"    "
}

fn validate_video_dimensions(track: &VideoTrack) -> bool {
    track.kind != TrackKind::Video
        || (track.width > 0
            && track.height > 0
            && track.pixel_depth > 0
            && (track.codec_fourcc != *b"raw " || track.pixel_depth == 24)
            && u64::from(track.width) * u64::from(track.height) <= MAX_VIDEO_PIXELS)
}

fn validate_track(track: &VideoTrack, track_ids: &mut BTreeSet<u32>) -> bool {
    track.track_id > 0
        && track_ids.insert(track.track_id)
        && track.timescale > 0
        && validate_codec(track)
        && validate_video_dimensions(track)
        && (track.kind == TrackKind::Video || track.pixel_depth == 0)
}

fn validate_tracks(video: &VideoData, track_ids: &mut BTreeSet<u32>) -> bool {
    !video.tracks.is_empty()
        && video.tracks.len() <= MAX_TRACKS
        && video
            .tracks
            .iter()
            .all(|track| validate_track(track, track_ids))
}

fn validate_frame_identity(
    frame: &VideoFrame,
    track_ids: &BTreeSet<u32>,
    frame_ids: &mut BTreeSet<(u32, u64)>,
) -> bool {
    track_ids.contains(&frame.track_id)
        && frame.frame_number > 0
        && frame_ids.insert((frame.track_id, frame.frame_number))
}

/// Continuity facts about one frame, computed by its caller: whether it
/// picks up immediately after the previous frame on its track, and whether
/// its byte range end is representable without overflow. Grouped into one
/// type (rather than two bare `bool` parameters) so the call site cannot
/// transpose them.
struct FrameContinuity {
    sequential: bool,
    has_byte_end: bool,
}

fn validate_frame_timing(
    frame: &VideoFrame,
    duration_ms: u64,
    continuity: FrameContinuity,
) -> bool {
    continuity.sequential
        && frame.end_ms > frame.start_ms
        && frame.end_ms <= duration_ms
        && frame.byte_length > 0
        && continuity.has_byte_end
        && temporal_buckets(frame.start_ms, frame.end_ms).is_ok()
}

/// What frame validation accumulates across one payload's frames: the frame
/// identities seen, each track's last frame (number and end time), and every
/// accepted frame's payload byte range.
struct FrameLedger {
    frame_ids: BTreeSet<(u32, u64)>,
    last_frame: BTreeMap<u32, (u64, u64)>,
    byte_ranges: Vec<(u64, u64)>,
}

fn validate_frame(
    frame: &VideoFrame,
    video: &VideoData,
    track_ids: &BTreeSet<u32>,
    ledger: &mut FrameLedger,
) -> bool {
    let previous = ledger.last_frame.get(&frame.track_id).copied();
    let sequential = previous.map_or(
        frame.frame_number == 1 && frame.start_ms == 0,
        |(number, end_ms)| {
            number.checked_add(1) == Some(frame.frame_number) && end_ms == frame.start_ms
        },
    );
    let byte_end = frame.byte_offset.checked_add(u64::from(frame.byte_length));
    let valid = validate_frame_identity(frame, track_ids, &mut ledger.frame_ids)
        && validate_frame_timing(
            frame,
            video.duration_ms,
            FrameContinuity {
                sequential,
                has_byte_end: byte_end.is_some(),
            },
        );
    if valid {
        ledger
            .last_frame
            .insert(frame.track_id, (frame.frame_number, frame.end_ms));
        ledger
            .byte_ranges
            .push((frame.byte_offset, byte_end.unwrap_or_default()));
    }
    valid
}

fn validate_frames(video: &VideoData, track_ids: &BTreeSet<u32>, ledger: &mut FrameLedger) -> bool {
    !video.frames.is_empty()
        && video.frames.len() <= MAX_FRAMES
        && video
            .frames
            .iter()
            .all(|frame| validate_frame(frame, video, track_ids, ledger))
}

fn has_video_track(video: &VideoData) -> bool {
    video
        .tracks
        .iter()
        .any(|track| track.kind == TrackKind::Video)
}

fn has_video_frame(video: &VideoData) -> bool {
    video.frames.iter().any(|frame| {
        video
            .tracks
            .iter()
            .any(|track| track.track_id == frame.track_id && track.kind == TrackKind::Video)
    })
}

fn payload_ranges_are_disjoint(byte_ranges: &mut [(u64, u64)]) -> bool {
    byte_ranges.sort_unstable();
    byte_ranges.windows(2).all(|pair| pair[0].1 <= pair[1].0)
}

/// Element count for `modality_contract_runtime_hooks!` — passed as a function
/// path rather than an inline `self`-bearing expression; see that macro's docs
/// for why (the macro is invoked at item position, where `self` has no binding).
fn element_count(video: &VideoData) -> u64 {
    video.frames.len() as u64
}

/// Secondary-index flag for `modality_contract_runtime_hooks!`.
fn has_secondary_index(video: &VideoData) -> bool {
    !video.native_index_keys().is_empty()
}

impl ModalityContract for VideoData {
    fn storage_kind(&self) -> &'static str {
        "video"
    }

    /// A video is a FILTER/SOURCE candidate, not intrinsically ranked — unranked
    /// until a RANK op imposes a score.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    fn cdc_topic(&self) -> Option<&'static str> {
        Some("modality.video.v1")
    }

    fn provenance(&self, _id: &str) -> Option<Provenance> {
        Some(Provenance::asserted())
    }

    /// The X1 evidence resolver: the FIRST shot in this video's (extractor-supplied)
    /// shot index, as an exact video time range. `None` when there are no shots —
    /// never fabricated.
    fn evidence_address(&self) -> Option<EvidenceAddress> {
        let shot = self.shots.first()?;
        Some(EvidenceAddress::VideoTimeRange {
            start_ms: shot.start_ms,
            end_ms: shot.end_ms,
        })
    }

    fn analytics_ops(&self) -> Vec<&'static str> {
        vec![
            "duration",
            "shot_index",
            "frame_index",
            "raw_frame_decode",
            "scene_window",
            "temporal_signature",
        ]
    }

    fn policy_labels(&self, _id: &str) -> Vec<String> {
        eg_modality::policy_labels()
    }

    eg_modality::modality_contract_runtime_hooks!(VideoData, element_count, has_secondary_index);
}

impl GovernedModality for VideoData {
    fn validate_governed_payload(&self) -> bool {
        let mut track_ids = BTreeSet::new();
        let mut ledger = FrameLedger {
            frame_ids: BTreeSet::new(),
            last_frame: BTreeMap::new(),
            byte_ranges: Vec::with_capacity(self.frames.len().min(MAX_FRAMES)),
        };
        validate_video_header(self)
            && validate_shots(self)
            && validate_tracks(self, &mut track_ids)
            && has_video_track(self)
            && validate_frames(self, &track_ids, &mut ledger)
            && has_video_frame(self)
            && payload_ranges_are_disjoint(&mut ledger.byte_ranges)
    }

    fn native_index_keys(&self) -> Vec<NativeIndexKey> {
        let video_tracks: std::collections::BTreeSet<u32> = self
            .tracks
            .iter()
            .filter(|track| track.kind == TrackKind::Video)
            .map(|track| track.track_id)
            .collect();
        self.frames
            .iter()
            .filter(|frame| video_tracks.contains(&frame.track_id))
            .flat_map(|frame| temporal_buckets(frame.start_ms, frame.end_ms).unwrap_or_default())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn matches_native_predicate(&self, predicate: &NativePredicate) -> bool {
        let NativePredicate::VideoWindow {
            start_ms,
            end_ms,
            keyframes_only,
        } = predicate
        else {
            return false;
        };
        let video_tracks: std::collections::BTreeSet<u32> = self
            .tracks
            .iter()
            .filter(|track| track.kind == TrackKind::Video)
            .map(|track| track.track_id)
            .collect();
        self.frames.iter().any(|frame| {
            video_tracks.contains(&frame.track_id)
                && frame.start_ms < *end_ms
                && *start_ms < frame.end_ms
                && (!keyframes_only || frame.keyframe)
        })
    }
}

impl ConformanceTestable for VideoData {
    fn conformance_sample() -> Self {
        VideoData::new(
            10_000,
            "deadbeefcafefeed00000000000000000deadbeefcafefeed000000000000000",
        )
        .with_frame_rate(30.0)
        .with_native_index(
            vec![VideoTrack {
                track_id: 1,
                kind: TrackKind::Video,
                codec_fourcc: *b"raw ",
                timescale: 1_000,
                width: 2,
                height: 1,
                pixel_depth: 24,
            }],
            vec![VideoFrame {
                track_id: 1,
                frame_number: 1,
                start_ms: 0,
                end_ms: 1_000,
                byte_offset: 64,
                byte_length: 6,
                keyframe: true,
            }],
        )
        .with_shots(vec![VideoShot::labeled(
            "eg:label:0000000000000001",
            0,
            4000,
        )])
    }

    #[cfg(feature = "runtime")]
    fn native_production_probe() -> Option<eg_modality::NativeProductionProbe> {
        Some(crate::runtime::production_probe())
    }
}

#[cfg(test)]
mod extra_coverage {
    use super::*;

    #[test]
    fn evidence_is_none_without_any_shot() {
        let v = VideoData::new(1000, "h");
        assert_eq!(ModalityContract::evidence_address(&v), None);
    }

    #[test]
    fn evidence_returns_the_first_shot_as_a_real_located_span() {
        let v = VideoData::new(6000, "h").with_shots(vec![
            VideoShot::labeled("a", 0, 2000),
            VideoShot::labeled("b", 2000, 6000),
        ]);
        assert_eq!(
            ModalityContract::evidence_address(&v),
            Some(EvidenceAddress::VideoTimeRange {
                start_ms: 0,
                end_ms: 2000,
            })
        );
    }

    #[test]
    fn to_rowset_stays_unranked() {
        let v = VideoData::new(1000, "h");
        assert_eq!(ModalityContract::to_rowset(&v, "video-1").score, None);
    }

    #[cfg(feature = "runtime")]
    #[test]
    fn served_video_is_production_ready_12_of_12() {
        let report = eg_modality::tck_report::<VideoData>();
        assert!(report.is_production_ready(), "{}", report.summary());
        assert_eq!(report.pass_count(), 12);
        assert_eq!(report.na_count(), 0);
    }

    #[test]
    fn governed_video_rejects_raw_labels() {
        let valid = VideoData::conformance_sample();
        assert!(GovernedModality::validate_governed_payload(&valid));
        let mut unsafe_value = valid;
        unsafe_value.shots[0].label = Some("raw-display-label".to_string());
        assert!(!GovernedModality::validate_governed_payload(&unsafe_value));
    }

    #[test]
    fn governed_video_rejects_overlapping_frame_payloads() {
        let mut unsafe_value = VideoData::conformance_sample();
        unsafe_value.frames.push(VideoFrame {
            track_id: 1,
            frame_number: 2,
            start_ms: 1_000,
            end_ms: 2_000,
            byte_offset: 68,
            byte_length: 6,
            keyframe: false,
        });
        assert!(!GovernedModality::validate_governed_payload(&unsafe_value));
    }
}

eg_modality::modality_conformance_tests!(VideoData);
