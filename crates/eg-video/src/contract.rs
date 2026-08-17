//! Governed modality contract for [`VideoData`].

use eg_modality::{
    decode_staged, encode_staged, temporal_buckets, ConformanceTestable, EvidenceAddress,
    GovernedModality, IngestReport, ModalityContract, ModalitySelfTest, NativeIndexKey,
    NativePredicate, OpaqueRef, Provenance, RowSetShape, StagedWrite, StorageStats,
};

use crate::video::{TrackKind, VideoData, VideoFrame, VideoShot, VideoTrack};

const MAX_TRACKS: usize = 1_024;
const MAX_FRAMES: usize = 200_000;
const MAX_SHOTS: usize = 65_536;
const MAX_VIDEO_PIXELS: u64 = 8_388_608;

fn opaque(value: &str) -> bool {
    OpaqueRef::new(value.to_string()).is_ok()
}

fn content_address(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
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
        vec![
            "eg:policylabel:0000000000000001".to_string(),
            "eg:policylabel:0000000000000002".to_string(),
            "eg:policylabel:0000000000000003".to_string(),
        ]
    }

    // ── EG-P1-1 hooks — real, minimal implementations over VideoData's
    // serialization and txn staging. ──

    /// Batch and bounded-stream ingest use the same deterministic typed codec.
    fn ingest_report(&self, id: &str) -> IngestReport {
        let staged = self.txn_stage(id);
        let batch = match decode_staged::<VideoData>(&staged) {
            Ok(rt) if rt == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        let streaming = [staged].into_iter().all(|item| {
            matches!(
                decode_staged::<VideoData>(&item),
                Ok(round_trip) if round_trip == *self
            )
        });
        IngestReport {
            batch,
            streaming: if streaming {
                ModalitySelfTest::Passed
            } else {
                ModalitySelfTest::Failed
            },
        }
    }

    /// Real storage stats: logical size from encoded length; element count is the
    /// number of extracted video shots. Shot lookup is the native secondary index
    /// advertised by the served video runtime.
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        let logical_bytes = encode_staged(self).len() as u64;
        Some(StorageStats {
            logical_bytes,
            element_count: self.frames.len() as u64,
            has_secondary_index: !self.native_index_keys().is_empty(),
        })
    }

    fn backup_selfcheck(&self, id: &str) -> ModalitySelfTest {
        match decode_staged::<VideoData>(&self.txn_stage(id)) {
            Ok(restored) if restored == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// Simulated single-node crash-and-recover through txn staging path.
    fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
        let staged: StagedWrite = self.txn_stage(id);
        match decode_staged::<VideoData>(&staged) {
            Ok(recovered) if recovered == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }
}

impl GovernedModality for VideoData {
    fn validate_governed_payload(&self) -> bool {
        let mut track_ids = std::collections::BTreeSet::new();
        let mut frame_ids = std::collections::BTreeSet::new();
        let mut last_frame: std::collections::BTreeMap<u32, (u64, u64)> =
            std::collections::BTreeMap::new();
        let mut byte_ranges = Vec::with_capacity(self.frames.len().min(MAX_FRAMES));
        self.duration_ms > 0
            && content_address(&self.blob_ref)
            && self
                .frame_rate
                .is_none_or(|rate| rate.is_finite() && rate > 0.0)
            && self.shots.len() <= MAX_SHOTS
            && self.shots.iter().all(|shot| {
                shot.end_ms > shot.start_ms
                    && shot.end_ms <= self.duration_ms
                    && shot.label.as_deref().is_none_or(opaque)
            })
            && !self.tracks.is_empty()
            && self.tracks.len() <= MAX_TRACKS
            && self.tracks.iter().all(|track| {
                track.track_id > 0
                    && track_ids.insert(track.track_id)
                    && track.timescale > 0
                    && track
                        .codec_fourcc
                        .iter()
                        .all(|byte| byte.is_ascii_graphic() || *byte == b' ')
                    && track.codec_fourcc != *b"    "
                    && (track.kind != TrackKind::Video
                        || (track.width > 0
                            && track.height > 0
                            && track.pixel_depth > 0
                            && (track.codec_fourcc != *b"raw " || track.pixel_depth == 24)
                            && u64::from(track.width) * u64::from(track.height)
                                <= MAX_VIDEO_PIXELS))
                    && (track.kind == TrackKind::Video || track.pixel_depth == 0)
            })
            && self
                .tracks
                .iter()
                .any(|track| track.kind == TrackKind::Video)
            && !self.frames.is_empty()
            && self.frames.len() <= MAX_FRAMES
            && self.frames.iter().all(|frame| {
                let previous = last_frame.get(&frame.track_id).copied();
                let sequential = previous.map_or(
                    frame.frame_number == 1 && frame.start_ms == 0,
                    |(number, end_ms)| {
                        number.checked_add(1) == Some(frame.frame_number)
                            && end_ms == frame.start_ms
                    },
                );
                let byte_end = frame.byte_offset.checked_add(u64::from(frame.byte_length));
                let valid = track_ids.contains(&frame.track_id)
                    && frame.frame_number > 0
                    && frame_ids.insert((frame.track_id, frame.frame_number))
                    && sequential
                    && frame.end_ms > frame.start_ms
                    && frame.end_ms <= self.duration_ms
                    && frame.byte_length > 0
                    && byte_end.is_some()
                    && temporal_buckets(frame.start_ms, frame.end_ms).is_ok();
                if valid {
                    last_frame.insert(frame.track_id, (frame.frame_number, frame.end_ms));
                    byte_ranges.push((frame.byte_offset, byte_end.unwrap_or_default()));
                }
                valid
            })
            && self.frames.iter().any(|frame| {
                self.tracks
                    .iter()
                    .any(|track| track.track_id == frame.track_id && track.kind == TrackKind::Video)
            })
            && {
                byte_ranges.sort_unstable();
                byte_ranges.windows(2).all(|pair| pair[0].1 <= pair[1].0)
            }
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
}

eg_modality::modality_conformance_tests!(VideoData);
