use eg_modality::{
    ApplyDisposition, ApplyOutcome, Classification, ServedEvent, ServedEventKind, ServedPage,
    ServedRuntimeStats,
};
use eg_types::result_contract::ingestion as results;
use serde::{de::DeserializeOwned, Serialize};

use super::ModalityAuthority;
use crate::protocol::ResultPayload;

pub(super) fn encode_authority(authority: &ModalityAuthority) -> Result<ResultPayload, String> {
    ResultPayload::of::<results::ServedModalityAuthority>(results::ServedModalityAuthority {
        tenant_ref: authority.scope.tenant_ref.as_str().to_owned(),
        access_policy_ref: authority.scope.access_policy_ref.as_str().to_owned(),
        purpose_ref: authority.scope.purpose_ref.as_str().to_owned(),
        maximum_classification: classification(authority.scope.maximum_classification)?,
    })
}

pub(super) fn encode_ingest_result(outcome: ApplyOutcome) -> Result<ResultPayload, String> {
    apply_outcome::<results::ServedModalityIngest>(outcome)
}

pub(super) fn encode_ingest_stream_result(
    outcomes: Vec<ApplyOutcome>,
) -> Result<ResultPayload, String> {
    let outcomes = outcomes.into_iter().map(apply_outcome_body).collect();
    ResultPayload::of::<results::ServedModalityIngestStream>(outcomes)
}

pub(super) fn encode_query_page<T>(page: ServedPage<T>) -> Result<ResultPayload, String>
where
    T: Serialize,
{
    ResultPayload::of_dynamic::<results::ServedModalityQuery, _>(&page)
}

pub(super) fn encode_native_query_page<T>(page: ServedPage<T>) -> Result<ResultPayload, String>
where
    T: Serialize,
{
    ResultPayload::of_dynamic::<results::ServedModalityNativeQuery, _>(&page)
}

pub(super) fn encode_delete_result(outcome: ApplyOutcome) -> Result<ResultPayload, String> {
    apply_outcome::<results::ServedModalityDelete>(outcome)
}

pub(super) fn encode_lifecycle_result(
    outcome: ApplyOutcome,
    restore: bool,
) -> Result<ResultPayload, String> {
    if restore {
        apply_outcome::<results::ServedModalityRestore>(outcome)
    } else {
        apply_outcome::<results::ServedModalityMoveToCold>(outcome)
    }
}

pub(super) fn encode_events(events: Vec<ServedEvent>) -> Result<ResultPayload, String> {
    let events = events
        .into_iter()
        .map(event_body)
        .collect::<Result<Vec<_>, _>>()?;
    ResultPayload::of::<results::ServedModalityEvents>(events)
}

pub(super) fn encode_runtime_stats(stats: ServedRuntimeStats) -> Result<ResultPayload, String> {
    ResultPayload::of::<results::ServedModalityStats>(results::ServedModalityStats {
        active_records: stats.active_records,
        total_records: stats.total_records,
        tombstoned_records: stats.tombstoned_records,
        modality_index_postings: stats.modality_index_postings,
        segment_index_postings: stats.segment_index_postings,
        native_index_keys: stats.native_index_keys,
        native_index_postings: stats.native_index_postings,
        events: stats.events,
        snapshot_bytes: stats.snapshot_bytes,
    })
}

pub(super) fn encode_tombstone_collection(collected: usize) -> Result<ResultPayload, String> {
    ResultPayload::of::<results::ServedModalityCollectTombstones>(
        results::ServedModalityTombstoneCollection { collected },
    )
}

pub(super) fn encode_capability_report(
    component_pass: usize,
    component_not_applicable: usize,
    component_total: usize,
) -> Result<ResultPayload, String> {
    ResultPayload::of::<results::ServedModalityCapabilities>(results::ServedModalityCapabilities {
        component_ready: true,
        component_pass,
        component_not_applicable,
        component_total,
    })
}

fn apply_outcome<M>(outcome: ApplyOutcome) -> Result<ResultPayload, String>
where
    M: eg_types::result_contract::MethodResult<Body = results::ServedModalityApplyOutcome>,
{
    ResultPayload::of::<M>(apply_outcome_body(outcome))
}

fn apply_outcome_body(outcome: ApplyOutcome) -> results::ServedModalityApplyOutcome {
    results::ServedModalityApplyOutcome {
        disposition: match outcome.disposition {
            ApplyDisposition::Applied => results::ServedModalityApplyDisposition::Applied,
            ApplyDisposition::IdempotentReplay => {
                results::ServedModalityApplyDisposition::IdempotentReplay
            }
        },
        observation_version: outcome.observation_version,
        event_sequence: outcome.event_sequence,
    }
}

fn event_body(event: ServedEvent) -> Result<results::ServedModalityEvent, String> {
    Ok(results::ServedModalityEvent {
        sequence: event.sequence,
        occurrence_id: event.occurrence_id.as_ref().as_str().to_owned(),
        observation_version: event.observation_version,
        kind: event_kind(event.kind)?,
        tenant_ref: event.tenant_ref.as_str().to_owned(),
        access_policy_ref: event.access_policy_ref.as_str().to_owned(),
    })
}

fn classification(value: Classification) -> Result<results::ServedModalityClassification, String> {
    transcode(value)
}

fn event_kind(value: ServedEventKind) -> Result<results::ServedModalityEventKind, String> {
    transcode(value)
}

pub(super) fn transcode<T, U>(value: T) -> Result<U, String>
where
    T: Serialize,
    U: DeserializeOwned,
{
    let value = serde_json::to_value(value)
        .map_err(|error| format!("result serialization failed: {error}"))?;
    serde_json::from_value(value).map_err(|error| format!("result conversion failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_audio::{AudioData, AudioFeatureWindow};
    use eg_modality::{
        ArtifactBundle, LifecycleState, OccurrenceId, OpaqueRef, PrivacyAttestation, ServedRecord,
        ARTIFACT_PROTOCOL_VERSION,
    };

    fn opaque(namespace: &str, token: &str) -> OpaqueRef {
        OpaqueRef::scoped(namespace, token).expect("golden-vector reference is valid")
    }

    fn audio_page() -> ServedPage<AudioData> {
        let occurrence_id = OccurrenceId::from_token("0123456789abcdef")
            .expect("golden-vector occurrence is valid");
        let next =
            OccurrenceId::from_token("fedcba9876543210").expect("golden-vector cursor is valid");
        let bundle = ArtifactBundle {
            protocol_version: ARTIFACT_PROTOCOL_VERSION,
            privacy: PrivacyAttestation {
                scanner_ref: opaque("scanner", "0123456789abcdef"),
                policy_version_ref: opaque("policyversion", "0123456789abcdef"),
                raw_pii_persisted: false,
                local_identifiers_persisted: false,
            },
            artifacts: Vec::new(),
            occurrences: Vec::new(),
            renditions: Vec::new(),
            segments: Vec::new(),
            features: Vec::new(),
            evidence_loci: Vec::new(),
        };
        let value = AudioData::new(48_000, 1_000, "eg:content:0123456789abcdef")
            .with_native_features(
                2,
                24,
                vec![AudioFeatureWindow {
                    start_ms: 125,
                    end_ms: 375,
                    peak: 0.25,
                    rms: 0.125,
                    spectral_centroid_bin: 42.5,
                }],
            );
        ServedPage {
            records: vec![ServedRecord {
                occurrence_id,
                observation_version: 7,
                lifecycle: LifecycleState::Active,
                bundle,
                value: Some(value),
            }],
            next: Some(next),
        }
    }

    fn assert_direct_audio_page_encoding(
        encode: fn(ServedPage<AudioData>) -> Result<ResultPayload, String>,
    ) {
        let page = audio_page();
        let expected = rmp_serde::to_vec_named(&page).expect("direct page encoding succeeds");
        let ResultPayload::Raw(actual) = encode(page.clone()).expect("query encoding succeeds")
        else {
            panic!("served-modality pages must use the compact Raw result encoding");
        };

        // The three feature fields are f32 and therefore carry MessagePack's float32
        // marker. A JSON intermediate would turn these into float64 (`0xcb`) values.
        assert_eq!(actual.iter().filter(|&&byte| byte == 0xca).count(), 3);
        assert_eq!(actual, expected);

        let decoded: ServedPage<AudioData> =
            rmp_serde::from_slice(&actual).expect("direct page bytes decode to their source type");
        assert_eq!(decoded, page);
        assert_eq!(decoded.next, page.next);
    }

    #[test]
    fn query_page_preserves_direct_audio_wire_vector() {
        assert_direct_audio_page_encoding(encode_query_page::<AudioData>);
    }

    #[test]
    fn native_query_page_preserves_direct_audio_wire_vector() {
        assert_direct_audio_page_encoding(encode_native_query_page::<AudioData>);
    }
}
