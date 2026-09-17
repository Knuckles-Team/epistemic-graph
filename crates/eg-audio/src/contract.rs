//! Governed modality contract for [`AudioData`].

use eg_modality::{
    encode_staged, temporal_buckets, ConformanceTestable, EvidenceAddress, GovernedModality,
    ModalityContract, NativeIndexKey, NativePredicate, OpaqueRef, Provenance, RowSetShape,
    StagedWrite,
};

use crate::audio::{AudioData, AudioFeatureWindow, AudioSegment};

const MAX_CHANNELS: u16 = 256;
const MAX_FEATURE_WINDOWS: usize = 4_096;
const MAX_SEGMENTS: usize = 65_536;

fn opaque(value: &str) -> bool {
    OpaqueRef::new(value.to_string()).is_ok()
}

/// Element count for `modality_contract_runtime_hooks!` — passed as a function
/// path rather than an inline `self`-bearing expression; see that macro's docs
/// for why (the macro is invoked at item position, where `self` has no binding).
fn element_count(audio: &AudioData) -> u64 {
    audio.feature_windows.len() as u64
}

/// Secondary-index flag for `modality_contract_runtime_hooks!`.
fn has_secondary_index(audio: &AudioData) -> bool {
    !audio.native_index_keys().is_empty()
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
        eg_modality::policy_labels()
    }

    eg_modality::modality_contract_runtime_hooks!(AudioData, element_count, has_secondary_index);
}

/// Sample format, duration, and content address of a governed recording.
fn stream_format_is_valid(audio: &AudioData) -> bool {
    audio.sample_rate > 0
        && audio.channels > 0
        && audio.channels <= MAX_CHANNELS
        && matches!(audio.bits_per_sample, 8 | 16)
        && audio.duration_ms > 0
        && eg_modality::content_address(&audio.blob_ref)
}

/// A bounded segment index of non-empty spans inside the recording, each with an
/// opaque label reference (if any) rather than display text.
fn segments_are_valid(audio: &AudioData) -> bool {
    audio.segments.len() <= MAX_SEGMENTS
        && audio.segments.iter().all(|segment| {
            segment.end_ms > segment.start_ms
                && segment.end_ms <= audio.duration_ms
                && segment.label.as_deref().is_none_or(opaque)
        })
}

/// A non-empty, bounded run of contiguous windows from 0 to the recording's end.
fn feature_windows_tile_the_recording(audio: &AudioData) -> bool {
    let windows = &audio.feature_windows;
    !windows.is_empty()
        && windows.len() <= MAX_FEATURE_WINDOWS
        && windows.first().is_some_and(|window| window.start_ms == 0)
        && windows
            .last()
            .is_some_and(|window| window.end_ms == audio.duration_ms)
        && windows
            .windows(2)
            .all(|pair| pair[0].end_ms == pair[1].start_ms)
}

/// One window: a non-empty span inside the recording with finite, in-range
/// statistics and an indexable temporal extent.
fn feature_window_is_valid(window: &AudioFeatureWindow, duration_ms: u64) -> bool {
    window.end_ms > window.start_ms
        && window.end_ms <= duration_ms
        && window.peak.is_finite()
        && window.rms.is_finite()
        && window.spectral_centroid_bin.is_finite()
        && (0.0..=1.0).contains(&window.peak)
        && (0.0..=1.0).contains(&window.rms)
        && (0.0..=7.0).contains(&window.spectral_centroid_bin)
        && temporal_buckets(window.start_ms, window.end_ms).is_ok()
}

impl GovernedModality for AudioData {
    fn validate_governed_payload(&self) -> bool {
        stream_format_is_valid(self)
            && segments_are_valid(self)
            && feature_windows_tile_the_recording(self)
            && self
                .feature_windows
                .iter()
                .all(|window| feature_window_is_valid(window, self.duration_ms))
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
            "deadbeefcafefeed00000000000000000deadbeefcafefeed000000000000000",
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

    /// Every clause of the governed-payload check rejects on its own: each mutation
    /// breaks exactly one clause of an otherwise valid sample.
    #[test]
    fn governed_audio_rejects_each_invalid_clause() {
        type Mutation = (&'static str, fn(&mut AudioData));
        let mutations: [Mutation; 20] = [
            ("zero sample rate", |a| a.sample_rate = 0),
            ("zero channels", |a| a.channels = 0),
            ("too many channels", |a| a.channels = MAX_CHANNELS + 1),
            ("24-bit samples", |a| a.bits_per_sample = 24),
            ("zero duration", |a| a.duration_ms = 0),
            ("non-content-address blob", |a| a.blob_ref = "h".to_string()),
            ("empty segment", |a| {
                a.segments[0].end_ms = a.segments[0].start_ms
            }),
            ("segment past duration", |a| a.segments[0].end_ms = 5001),
            ("no feature windows", |a| a.feature_windows.clear()),
            ("first window not at zero", |a| {
                a.feature_windows[0].start_ms = 1
            }),
            ("last window short of duration", |a| {
                a.feature_windows[0].end_ms = 4999
            }),
            ("gap between windows", |a| {
                a.feature_windows[0].end_ms = 2000;
                let mut next = a.feature_windows[0].clone();
                next.start_ms = 2001;
                next.end_ms = 5000;
                a.feature_windows.push(next);
            }),
            ("non-finite peak", |a| a.feature_windows[0].peak = f32::NAN),
            ("non-finite rms", |a| {
                a.feature_windows[0].rms = f32::INFINITY
            }),
            ("non-finite centroid", |a| {
                a.feature_windows[0].spectral_centroid_bin = f32::NAN
            }),
            ("peak above one", |a| a.feature_windows[0].peak = 1.5),
            ("negative rms", |a| a.feature_windows[0].rms = -0.1),
            ("centroid above seven", |a| {
                a.feature_windows[0].spectral_centroid_bin = 7.5
            }),
            ("window too broad to index", |a| {
                a.duration_ms = 1_000_000_000_000_000;
                a.feature_windows[0].end_ms = 1_000_000_000_000_000;
            }),
            ("raw segment label", |a| {
                a.segments[0].label = Some("raw-display-label".to_string())
            }),
        ];
        for (clause, mutate) in mutations {
            let mut value = AudioData::conformance_sample();
            mutate(&mut value);
            assert!(
                !GovernedModality::validate_governed_payload(&value),
                "{clause} must be rejected"
            );
        }

        // Contiguous windows that tile the recording, and an unlabeled segment, pass.
        let mut tiled = AudioData::conformance_sample();
        tiled.feature_windows[0].end_ms = 2000;
        let mut next = tiled.feature_windows[0].clone();
        next.start_ms = 2000;
        next.end_ms = 5000;
        tiled.feature_windows.push(next);
        tiled.segments[0].label = None;
        assert!(GovernedModality::validate_governed_payload(&tiled));
    }
}

eg_modality::modality_conformance_tests!(AudioData);
