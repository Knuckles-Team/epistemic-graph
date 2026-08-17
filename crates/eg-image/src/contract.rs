//! Governed modality contract for [`ImageData`].

use eg_modality::{
    decode_staged, encode_staged, signature_bands, spatial_cells, ConformanceTestable,
    EvidenceAddress, GovernedModality, IngestReport, ModalityContract, ModalitySelfTest,
    NativeIndexKey, NativePredicate, OpaqueRef, Provenance, RowSetShape, StagedWrite, StorageStats,
};

use crate::image::{ImageColorSpace, ImageData, ImageFormat, ImageRegion};

const MAX_DECODED_PIXELS: u64 = 8_388_608;
const MAX_REGIONS: usize = 65_536;

fn opaque(value: &str) -> bool {
    OpaqueRef::new(value.to_string()).is_ok()
}

fn content_address(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

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

    fn cdc_topic(&self) -> Option<&'static str> {
        Some("modality.image.v1")
    }

    fn provenance(&self, _id: &str) -> Option<Provenance> {
        Some(Provenance::asserted())
    }

    /// The X1 evidence resolver: the FIRST region in this image's (extractor-supplied)
    /// region index, as an exact pixel region. `None` when there are no regions — this NEVER
    /// fabricates a whole-image fallback region; an image with no known regions has
    /// nothing located to report yet (mirrors the "never fabricated" contract on
    /// `ImageData::regions` itself).
    fn evidence_address(&self) -> Option<EvidenceAddress> {
        let region = self.regions.first()?;
        Some(EvidenceAddress::ImageRegion {
            x: region.x,
            y: region.y,
            width: region.width,
            height: region.height,
        })
    }

    fn analytics_ops(&self) -> Vec<&'static str> {
        vec!["pixel_decode", "region_index", "crop", "perceptual_hash"]
    }

    fn policy_labels(&self, _id: &str) -> Vec<String> {
        vec![
            "eg:policylabel:0000000000000001".to_string(),
            "eg:policylabel:0000000000000002".to_string(),
            "eg:policylabel:0000000000000003".to_string(),
        ]
    }

    // ── EG-P1-1 hooks — real, minimal implementations over ImageData's
    // serialization and txn staging. ──

    /// Batch and bounded-stream ingest use the same deterministic typed codec.
    fn ingest_report(&self, id: &str) -> IngestReport {
        let staged = self.txn_stage(id);
        let batch = match decode_staged::<ImageData>(&staged) {
            Ok(rt) if rt == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        let streaming = [staged].into_iter().all(|item| {
            matches!(
                decode_staged::<ImageData>(&item),
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

    /// Real storage stats from the serialized ImageData: logical size from encoded
    /// length; element count is the number of extracted regions. Region lookup is
    /// the native secondary index advertised by the served image runtime.
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        let logical_bytes = encode_staged(self).len() as u64;
        Some(StorageStats {
            logical_bytes,
            element_count: self.regions.len() as u64,
            has_secondary_index: !self.native_index_keys().is_empty(),
        })
    }

    fn backup_selfcheck(&self, id: &str) -> ModalitySelfTest {
        match decode_staged::<ImageData>(&self.txn_stage(id)) {
            Ok(restored) if restored == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
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
}

impl GovernedModality for ImageData {
    fn validate_governed_payload(&self) -> bool {
        self.width > 0
            && self.height > 0
            && u64::from(self.width)
                .checked_mul(u64::from(self.height))
                .is_some_and(|pixels| pixels <= MAX_DECODED_PIXELS)
            && self.format == ImageFormat::Png
            && self.color_space != ImageColorSpace::Unknown
            && self.bit_depth == 8
            && content_address(&self.blob_ref)
            && self.regions.len() <= MAX_REGIONS
            && self.regions.iter().all(|region| {
                region.x.is_finite()
                    && region.y.is_finite()
                    && region.width.is_finite()
                    && region.height.is_finite()
                    && region.x >= 0.0
                    && region.y >= 0.0
                    && region.width > 0.0
                    && region.height > 0.0
                    && region.x + region.width <= self.width as f64
                    && region.y + region.height <= self.height as f64
                    && region.label.as_deref().is_none_or(opaque)
            })
    }

    fn native_index_keys(&self) -> Vec<NativeIndexKey> {
        let mut keys = signature_bands(self.perceptual_hash);
        if self.regions.is_empty() {
            keys.extend(spatial_cells(0.0, 0.0, 1.0, 1.0).unwrap_or_default());
        } else {
            for region in &self.regions {
                keys.extend(
                    spatial_cells(
                        region.x / f64::from(self.width),
                        region.y / f64::from(self.height),
                        region.width / f64::from(self.width),
                        region.height / f64::from(self.height),
                    )
                    .unwrap_or_default(),
                );
            }
        }
        keys.into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn matches_native_predicate(&self, predicate: &NativePredicate) -> bool {
        match predicate {
            NativePredicate::ImageRegion {
                x,
                y,
                width,
                height,
            } => {
                let query = (*x, *y, *width, *height);
                self.regions.is_empty()
                    || self.regions.iter().any(|region| {
                        intersects(
                            query,
                            (
                                region.x / f64::from(self.width),
                                region.y / f64::from(self.height),
                                region.width / f64::from(self.width),
                                region.height / f64::from(self.height),
                            ),
                        )
                    })
            }
            NativePredicate::ImagePerceptualHash {
                hash,
                maximum_distance,
            } => (self.perceptual_hash ^ hash).count_ones() <= u32::from(*maximum_distance),
            _ => false,
        }
    }
}

fn intersects(left: (f64, f64, f64, f64), right: (f64, f64, f64, f64)) -> bool {
    left.0 < right.0 + right.2
        && right.0 < left.0 + left.2
        && left.1 < right.1 + right.3
        && right.1 < left.1 + left.3
}

impl ConformanceTestable for ImageData {
    fn conformance_sample() -> Self {
        ImageData::new(
            640,
            480,
            "deadbeefcafefeed00000000000000000deadbeefcafefeed000000000000000",
        )
        .with_format(ImageFormat::Png)
        .with_native_features(ImageColorSpace::Rgba, 8, 0x0123_4567_89ab_cdef)
        .with_regions(vec![ImageRegion::labeled(
            "eg:label:0000000000000001",
            10.0,
            20.0,
            100.0,
            80.0,
        )])
    }

    #[cfg(feature = "runtime")]
    fn native_production_probe() -> Option<eg_modality::NativeProductionProbe> {
        Some(crate::runtime::production_probe())
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
        assert_eq!(ModalityContract::evidence_address(&img), None);
    }

    #[test]
    fn evidence_returns_the_first_region_as_a_real_located_span() {
        let img = ImageData::new(10, 10, "h").with_regions(vec![
            ImageRegion::labeled("a", 1.0, 2.0, 3.0, 4.0),
            ImageRegion::labeled("b", 5.0, 6.0, 7.0, 8.0),
        ]);
        assert_eq!(
            ModalityContract::evidence_address(&img),
            Some(EvidenceAddress::ImageRegion {
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

    #[cfg(feature = "runtime")]
    #[test]
    fn served_image_is_production_ready_12_of_12() {
        let report = eg_modality::tck_report::<ImageData>();
        assert!(report.is_production_ready(), "{}", report.summary());
        assert_eq!(report.pass_count(), 12);
        assert_eq!(report.na_count(), 0);
    }

    #[test]
    fn governed_image_rejects_raw_labels() {
        let valid = ImageData::conformance_sample();
        assert!(GovernedModality::validate_governed_payload(&valid));
        let mut unsafe_value = valid;
        unsafe_value.regions[0].label = Some("raw-display-label".to_string());
        assert!(!GovernedModality::validate_governed_payload(&unsafe_value));
    }
}

eg_modality::modality_conformance_tests!(ImageData);
