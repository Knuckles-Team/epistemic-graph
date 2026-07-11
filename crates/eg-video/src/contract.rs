//! `ModalityContract` retrofit for [`VideoData`] (CONCEPT:E4 follow-up — image/audio/
//! video evidence).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF), mirroring
//! `eg-tensor`/`eg-geo`/`eg-image`/`eg-audio`'s retrofits.

use eg_modality::{
    decode_staged, encode_staged, ConformanceTestable, EvidenceSpan, IngestReport,
    ModalityContract, ModalitySelfTest, Provenance, RowSetShape, StagedWrite, StorageStats,
    TckPoint,
};

use crate::video::{VideoData, VideoShot};

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

    /// Bare video values are not (yet) on the CDC/streaming surface.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// A video has no derivation history of its own — default `None` is correct
    /// as-is; no override needed.
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        None
    }

    /// The X1 evidence resolver: the FIRST shot in this video's (extractor-supplied)
    /// shot index, as a real, located `EvidenceSpan::VideoShot` under the given row
    /// `id` as `video_id`. `None` when there are no shots — never fabricated.
    fn evidence(&self, id: &str) -> Option<EvidenceSpan> {
        let shot = self.shots.first()?;
        Some(EvidenceSpan::VideoShot {
            video_id: id.to_string(),
            start_ms: shot.start_ms,
            end_ms: shot.end_ms,
        })
    }

    fn analytics_ops(&self) -> Vec<&'static str> {
        vec!["duration", "shot_index"]
    }

    // ── EG-P1-1 hooks — real, minimal implementations over VideoData's
    // serialization and txn staging. ──

    /// Batch ingest = parse a `VideoData` back from its serialized form. Streaming
    /// is N/A: a video recording is a whole media value, not an append stream.
    fn ingest_report(&self, id: &str) -> IngestReport {
        let staged = self.txn_stage(id);
        let batch = match decode_staged::<VideoData>(&staged) {
            Ok(rt) if rt == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        IngestReport {
            batch,
            streaming: ModalitySelfTest::NotApplicable(
                "a video recording is a whole media value, not an append stream",
            ),
        }
    }

    /// Real storage stats: logical size from encoded length; element count is the
    /// number of extracted video shots. Video does NOT have a secondary index, so
    /// `has_secondary_index` is `false`.
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        let logical_bytes = encode_staged(self).len() as u64;
        Some(StorageStats {
            logical_bytes,
            element_count: self.shots.len() as u64,
            has_secondary_index: false,
        })
    }

    /// N/A: VideoData is ingest-time artifact, not a durable value. Durability is
    /// maintained by the media storage layer.
    fn backup_selfcheck(&self, _id: &str) -> ModalitySelfTest {
        ModalitySelfTest::NotApplicable(
            "video data is an ingest-time artifact; backup/restore/migrate is a media-storage-layer concern",
        )
    }

    /// Simulated single-node crash-and-recover through txn staging path.
    fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
        let staged: StagedWrite = self.txn_stage(id);
        match decode_staged::<VideoData>(&staged) {
            Ok(recovered) if recovered == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// Video has no CDC or policy of its own: CDC would require materializing
    /// shot detections; policy is at the graph-node layer.
    fn tck_not_applicable(&self, point: TckPoint) -> Option<&'static str> {
        match point {
            TckPoint::CdcDeleteRetentionGc => Some(
                "video metadata is immutable post-ingest; CDC would require materializing shot detections",
            ),
            TckPoint::TenantRowRegionPolicy => Some(
                "no modality-intrinsic policy surface — policy is at graph-node/eg-core::isolation layer",
            ),
            _ => None,
        }
    }
}

impl ConformanceTestable for VideoData {
    fn conformance_sample() -> Self {
        VideoData::new(10_000, "deadbeefcafefeed00000000000000000")
            .with_frame_rate(30.0)
            .with_shots(vec![VideoShot::labeled("scene-1", 0, 4000)])
    }
}

#[cfg(test)]
mod extra_coverage {
    use super::*;

    #[test]
    fn evidence_is_none_without_any_shot() {
        let v = VideoData::new(1000, "h");
        assert_eq!(ModalityContract::evidence(&v, "video-1"), None);
    }

    #[test]
    fn evidence_returns_the_first_shot_as_a_real_located_span() {
        let v = VideoData::new(6000, "h").with_shots(vec![
            VideoShot::labeled("a", 0, 2000),
            VideoShot::labeled("b", 2000, 6000),
        ]);
        assert_eq!(
            ModalityContract::evidence(&v, "video-1"),
            Some(EvidenceSpan::VideoShot {
                video_id: "video-1".to_string(),
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
}

eg_modality::modality_conformance_tests!(VideoData);
