//! `ModalityContract` retrofit for [`AudioData`] (CONCEPT:E4 follow-up — image/audio/
//! video evidence).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF), mirroring
//! `eg-tensor`/`eg-geo`/`eg-image`'s retrofits.

use eg_modality::{
    encode_staged, ConformanceTestable, EvidenceSpan, ModalityContract, Provenance, RowSetShape,
    StagedWrite,
};

use crate::audio::{AudioData, AudioSegment};

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

    /// Bare audio values are not (yet) on the CDC/streaming surface.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// An audio recording has no derivation history of its own — default `None` is
    /// correct as-is; no override needed.
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        None
    }

    /// The X1 evidence resolver: the FIRST segment in this recording's
    /// (extractor-supplied) segment index, as a real, located
    /// `EvidenceSpan::AudioSegment` under the given row `id` as `audio_id`. `None`
    /// when there are no segments — never fabricated.
    fn evidence(&self, id: &str) -> Option<EvidenceSpan> {
        let seg = self.segments.first()?;
        Some(EvidenceSpan::AudioSegment {
            audio_id: id.to_string(),
            start_ms: seg.start_ms,
            end_ms: seg.end_ms,
        })
    }

    fn analytics_ops(&self) -> Vec<&'static str> {
        vec!["duration", "segment_index"]
    }
}

impl ConformanceTestable for AudioData {
    fn conformance_sample() -> Self {
        AudioData::new(44_100, 5000, "deadbeefcafefeed00000000000000000")
            .with_segments(vec![AudioSegment::labeled("speaker-1", 0, 2500)])
    }
}

#[cfg(test)]
mod extra_coverage {
    use super::*;

    #[test]
    fn evidence_is_none_without_any_segment() {
        let a = AudioData::new(16_000, 1000, "h");
        assert_eq!(ModalityContract::evidence(&a, "audio-1"), None);
    }

    #[test]
    fn evidence_returns_the_first_segment_as_a_real_located_span() {
        let a = AudioData::new(16_000, 3000, "h").with_segments(vec![
            AudioSegment::labeled("a", 0, 1000),
            AudioSegment::labeled("b", 1000, 3000),
        ]);
        assert_eq!(
            ModalityContract::evidence(&a, "audio-1"),
            Some(EvidenceSpan::AudioSegment {
                audio_id: "audio-1".to_string(),
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
}

eg_modality::modality_conformance_tests!(AudioData);
