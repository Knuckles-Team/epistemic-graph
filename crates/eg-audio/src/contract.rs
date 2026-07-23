//! Governed modality contract for [`AudioData`].

use eg_modality::{
    decode_staged, encode_staged, temporal_buckets, ConformanceTestable, EvidenceAddress,
    GovernedModality, IngestReport, ModalityContract, ModalitySelfTest, NativeIndexKey,
    NativePredicate, OpaqueRef, Provenance, RowSetShape, StagedWrite, StorageStats,
};

use crate::audio::{AudioData, AudioFeatureWindow, AudioSegment};

const MAX_CHANNELS: u16 = 256;
const MAX_FEATURE_WINDOWS: usize = 4_096;
const MAX_SEGMENTS: usize = 65_536;

fn opaque(value: &str) -> bool {
    OpaqueRef::new(value.to_string()).is_ok()
}

fn content_address(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

impl ModalityContract for AudioData {
    fn storage_kind(&self) -> &'static str {
        "audio"
    }

    /// An audio recording is a FILTER/SOURCE candidate, not intrinsically ranked —
    /// unranked until a RANK op imposes a score.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    fn cdc_topic(&self) -> Option<&'static str> {
        Some("modality.audio.v1")
    }

    fn provenance(&self, _id: &str) -> Option<Provenance> {
        Some(Provenance::asserted())
    }

    /// The X1 evidence resolver: the FIRST segment in this recording's
    /// (extractor-supplied) segment index, as a real, located
    /// `EvidenceAddress::AudioRange`. `None`
    /// when there are no segments — never fabricated.
    fn evidence_address(&self) -> Option<EvidenceAddress> {
        let seg = self.segments.first()?;
        Some(EvidenceAddress::AudioRange {
            start_ms: seg.start_ms,
            end_ms: seg.end_ms,
        })
    }

    fn analytics_ops(&self) -> Vec<&'static str> {
        vec![
            "duration",
            "segment_index",
            "waveform_window",
            "voice_activity",
            "spectral_centroid",
        ]
    }

    fn policy_labels(&self, _id: &str) -> Vec<String> {
        vec![
            "eg:policylabel:0000000000000001".to_string(),
            "eg:policylabel:0000000000000002".to_string(),
            "eg:policylabel:0000000000000003".to_string(),
        ]
    }

    // ── EG-P1-1 hooks — real, minimal implementations over AudioData's
    // serialization and txn staging. ──

    /// Batch and bounded-stream ingest use the same deterministic typed codec.
    fn ingest_report(&self, id: &str) -> IngestReport {
        let staged = self.txn_stage(id);
        let batch = match decode_staged::<AudioData>(&staged) {
            Ok(rt) if rt == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        let streaming = [staged].into_iter().all(|item| {
            matches!(
                decode_staged::<AudioData>(&item),
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
    /// number of extracted audio segments. Segment lookup is the native secondary
    /// index advertised by the served audio runtime.
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        let logical_bytes = encode_staged(self).len() as u64;
        Some(StorageStats {
            logical_bytes,
            element_count: self.feature_windows.len() as u64,
            has_secondary_index: !self.native_index_keys().is_empty(),
        })
    }

    fn backup_selfcheck(&self, id: &str) -> ModalitySelfTest {
        match decode_staged::<AudioData>(&self.txn_stage(id)) {
            Ok(restored) if restored == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// Simulated single-node crash-and-recover through txn staging path.
    fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
        let staged: StagedWrite = self.txn_stage(id);
        match decode_staged::<AudioData>(&staged) {
            Ok(recovered) if recovered == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }
}

impl GovernedModality for AudioData {
    fn validate_governed_payload(&self) -> bool {
        self.sample_rate > 0
            && self.channels > 0
            && self.channels <= MAX_CHANNELS
            && matches!(self.bits_per_sample, 8 | 16)
            && self.duration_ms > 0
            && content_address(&self.blob_ref)
            && self.segments.len() <= MAX_SEGMENTS
            && self.segments.iter().all(|segment| {
                segment.end_ms > segment.start_ms
                    && segment.end_ms <= self.duration_ms
                    && segment.label.as_deref().is_none_or(opaque)
            })
            && !self.feature_windows.is_empty()
            && self.feature_windows.len() <= MAX_FEATURE_WINDOWS
            && self
                .feature_windows
                .first()
                .is_some_and(|window| window.start_ms == 0)
            && self
                .feature_windows
                .last()
                .is_some_and(|window| window.end_ms == self.duration_ms)
            && self
                .feature_windows
                .windows(2)
                .all(|pair| pair[0].end_ms == pair[1].start_ms)
            && self.feature_windows.iter().all(|window| {
                window.end_ms > window.start_ms
                    && window.end_ms <= self.duration_ms
                    && window.peak.is_finite()
                    && window.rms.is_finite()
                    && window.spectral_centroid_bin.is_finite()
                    && (0.0..=1.0).contains(&window.peak)
                    && (0.0..=1.0).contains(&window.rms)
                    && (0.0..=7.0).contains(&window.spectral_centroid_bin)
                    && temporal_buckets(window.start_ms, window.end_ms).is_ok()
            })
    }

    fn native_index_keys(&self) -> Vec<NativeIndexKey> {
        self.feature_windows
            .iter()
            .flat_map(|window| temporal_buckets(window.start_ms, window.end_ms).unwrap_or_default())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn matches_native_predicate(&self, predicate: &NativePredicate) -> bool {
        let NativePredicate::AudioWindow {
            start_ms,
            end_ms,
            minimum_rms,
        } = predicate
        else {
            return false;
        };
        self.feature_windows.iter().any(|window| {
            window.start_ms < *end_ms && *start_ms < window.end_ms && window.rms >= *minimum_rms
        })
    }
}

impl ConformanceTestable for AudioData {
    fn conformance_sample() -> Self {
        AudioData::new(
            44_100,
            5000,
            "deadbeefcafefeed00000000000000000deadbeefcafefeed00000000000000000",
        )
        .with_native_features(
            1,
            16,
            vec![AudioFeatureWindow {
                start_ms: 0,
                end_ms: 5000,
                peak: 0.8,
                rms: 0.4,
                spectral_centroid_bin: 2.0,
            }],
        )
        .with_segments(vec![AudioSegment::labeled(
            "eg:label:0000000000000001",
            0,
            2500,
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
    fn evidence_is_none_without_any_segment() {
        let a = AudioData::new(16_000, 1000, "h");
        assert_eq!(ModalityContract::evidence_address(&a), None);
    }

    #[test]
    fn evidence_returns_the_first_segment_as_a_real_located_span() {
        let a = AudioData::new(16_000, 3000, "h").with_segments(vec![
            AudioSegment::labeled("a", 0, 1000),
            AudioSegment::labeled("b", 1000, 3000),
        ]);
        assert_eq!(
            ModalityContract::evidence_address(&a),
            Some(EvidenceAddress::AudioRange {
                start_ms: 0,
                end_ms: 1000,
            })
        );
    }

    #[test]
    fn to_rowset_stays_unranked() {
        let a = AudioData::new(16_000, 1000, "h");
        assert_eq!(ModalityContract::to_rowset(&a, "audio-1").score, None);
    }

    #[cfg(feature = "runtime")]
    #[test]
    fn served_audio_is_production_ready_12_of_12() {
        let report = eg_modality::tck_report::<AudioData>();
        assert!(report.is_production_ready(), "{}", report.summary());
        assert_eq!(report.pass_count(), 12);
        assert_eq!(report.na_count(), 0);
    }

    #[test]
    fn governed_audio_rejects_raw_labels() {
        let valid = AudioData::conformance_sample();
        assert!(GovernedModality::validate_governed_payload(&valid));
        let mut unsafe_value = valid;
        unsafe_value.segments[0].label = Some("raw-display-label".to_string());
        assert!(!GovernedModality::validate_governed_payload(&unsafe_value));
    }
}

eg_modality::modality_conformance_tests!(AudioData);
