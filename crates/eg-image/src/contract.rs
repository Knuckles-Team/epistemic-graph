//! `ModalityContract` retrofit for [`ImageData`] (CONCEPT:E4 follow-up — image/audio/
//! video evidence).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF), mirroring
//! `eg-tensor`/`eg-geo`'s pilot retrofits.

use eg_modality::{
    decode_staged, encode_staged, ConformanceTestable, EvidenceSpan, IngestReport,
    ModalityContract, ModalitySelfTest, Provenance, RowSetShape, StagedWrite, StorageStats,
    TckPoint,
};

use crate::image::{ImageData, ImageRegion};

impl ModalityContract for ImageData {
    fn storage_kind(&self) -> &'static str {
        "image"
    }

    /// An image is a FILTER/SOURCE candidate, not an intrinsically ranked value (like
    /// `eg-geo::Geometry`) — unranked until a RANK op imposes a score.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// Bare image values are not (yet) on the CDC/streaming surface.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// An image has no derivation history of its own — default `None` is correct
    /// as-is; no override needed.
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        None
    }

    /// The X1 evidence resolver: the FIRST region in this image's (extractor-supplied)
    /// region index, as a real, located `EvidenceSpan::ImageRegion` under the given
    /// row `id` as `image_id`. `None` when there are no regions — this NEVER
    /// fabricates a whole-image fallback region; an image with no known regions has
    /// nothing located to report yet (mirrors the "never fabricated" contract on
    /// `ImageData::regions` itself).
    fn evidence(&self, id: &str) -> Option<EvidenceSpan> {
        let region = self.regions.first()?;
        Some(EvidenceSpan::ImageRegion {
            image_id: id.to_string(),
            x: region.x,
            y: region.y,
            width: region.width,
            height: region.height,
        })
    }

    fn analytics_ops(&self) -> Vec<&'static str> {
        vec!["dimensions", "region_index", "crop"]
    }

    // ── EG-P1-1 hooks — real, minimal implementations over ImageData's
    // serialization and txn staging. ──

    /// Batch ingest = parse an `ImageData` back from its serialized form. Streaming
    /// is N/A: an image is a whole media value, not an append stream.
    fn ingest_report(&self, id: &str) -> IngestReport {
        let staged = self.txn_stage(id);
        let batch = match decode_staged::<ImageData>(&staged) {
            Ok(rt) if rt == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        IngestReport {
            batch,
            streaming: ModalitySelfTest::NotApplicable(
                "an image is a whole media value, not an append stream",
            ),
        }
    }

    /// Real storage stats from the serialized ImageData: logical size from encoded
    /// length; element count is the number of extracted regions. Images do NOT have
    /// a secondary index, so `has_secondary_index` is `false`.
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        let logical_bytes = encode_staged(self).len() as u64;
        Some(StorageStats {
            logical_bytes,
            element_count: self.regions.len() as u64,
            has_secondary_index: false,
        })
    }

    /// N/A: ImageData is an ingest-time artifact, not a durable persisted value.
    /// Durability is maintained by the underlying media storage layer.
    fn backup_selfcheck(&self, _id: &str) -> ModalitySelfTest {
        ModalitySelfTest::NotApplicable(
            "image data is an ingest-time artifact; backup/restore/migrate is a media-storage-layer concern, not a modality-value capability",
        )
    }

    /// Simulated single-node crash-and-recover through the txn staging path.
    /// Stage the image as an in-txn write; the staged payload IS the WAL record;
    /// on "restart" replay-decode it and confirm the recovered image is intact.
    fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
        let staged: StagedWrite = self.txn_stage(id);
        match decode_staged::<ImageData>(&staged) {
            Ok(recovered) if recovered == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// Images have no CDC or policy of their own: CDC would require materializing
    /// region detections; policy is at the graph-node layer.
    fn tck_not_applicable(&self, point: TckPoint) -> Option<&'static str> {
        match point {
            TckPoint::CdcDeleteRetentionGc => Some(
                "image metadata is immutable post-ingest; CDC would require materializing region detections as persistent edges",
            ),
            TckPoint::TenantRowRegionPolicy => Some(
                "no modality-intrinsic policy surface — tenant/row/region policy is enforced at the graph-node/eg-core::isolation layer that owns the image",
            ),
            _ => None,
        }
    }
}

impl ConformanceTestable for ImageData {
    fn conformance_sample() -> Self {
        ImageData::new(640, 480, "deadbeefcafefeed00000000000000000")
            .with_regions(vec![ImageRegion::labeled("face", 10.0, 20.0, 100.0, 80.0)])
    }
}

// Exercise the no-regions branch (evidence() -> None) through the SAME battery, beyond
// the one sample the macro drives.
#[cfg(test)]
mod extra_coverage {
    use super::*;

    #[test]
    fn evidence_is_none_without_any_region() {
        let img = ImageData::new(10, 10, "h");
        assert_eq!(ModalityContract::evidence(&img, "img-1"), None);
    }

    #[test]
    fn evidence_returns_the_first_region_as_a_real_located_span() {
        let img = ImageData::new(10, 10, "h").with_regions(vec![
            ImageRegion::labeled("a", 1.0, 2.0, 3.0, 4.0),
            ImageRegion::labeled("b", 5.0, 6.0, 7.0, 8.0),
        ]);
        assert_eq!(
            ModalityContract::evidence(&img, "img-1"),
            Some(EvidenceSpan::ImageRegion {
                image_id: "img-1".to_string(),
                x: 1.0,
                y: 2.0,
                width: 3.0,
                height: 4.0,
            })
        );
    }

    #[test]
    fn to_rowset_stays_unranked() {
        let img = ImageData::new(10, 10, "h");
        assert_eq!(ModalityContract::to_rowset(&img, "img-1").score, None);
    }
}

eg_modality::modality_conformance_tests!(ImageData);
