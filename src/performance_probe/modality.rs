//! Modality storage, streaming, CDC, and recovery probes.

use serde::{Deserialize, Serialize};

use super::{timed, Observation, ProbeError};
use eg_modality::{
    encode_staged, ApplyDisposition, Artifact, ArtifactBundle, ArtifactId, Classification,
    Derivation, DerivationId, EvidenceAddress, EvidenceLocus, EvidenceLocusId, Feature, FeatureId,
    FeatureKind, GovernedModality, ModalityContract, ModalityKind, NativeIndexKey, NativePredicate,
    Occurrence, OccurrenceId, OpaqueRef, PolicyEnvelope, PrivacyAttestation, Rendition,
    RenditionId, ResourceId, RowSetShape, Segment, SegmentId, SegmentKind, ServedIngest,
    ServedModalityRuntime, ServedPolicyScope, ServedQuery, StagedWrite, ARTIFACT_PROTOCOL_VERSION,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ProbeDocument {
    pages: u32,
}

impl ModalityContract for ProbeDocument {
    fn storage_kind(&self) -> &'static str {
        "document"
    }

    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    fn cdc_topic(&self) -> Option<&'static str> {
        Some("modality.document.v1")
    }
}

impl GovernedModality for ProbeDocument {
    fn validate_governed_payload(&self) -> bool {
        self.pages > 0
    }

    fn native_index_keys(&self) -> Vec<NativeIndexKey> {
        vec![NativeIndexKey::Lexeme(modality_ref(
            "lexeme",
            u64::from(self.pages),
        ))]
    }

    fn matches_native_predicate(&self, predicate: &NativePredicate) -> bool {
        matches!(
            predicate,
            NativePredicate::DocumentLexeme {
                lexeme_ref,
                page: None
            } if lexeme_ref == &modality_ref("lexeme", u64::from(self.pages))
        )
    }
}

fn modality_token(value: u64) -> String {
    format!("{value:016x}")
}

fn modality_ref(namespace: &str, value: u64) -> OpaqueRef {
    OpaqueRef::scoped(namespace, &modality_token(value))
        .expect("bounded probe opaque reference is valid")
}

fn modality_identity(index: u64, offset: u64) -> String {
    modality_token(index.saturating_mul(32).saturating_add(offset))
}

fn modality_content(index: u64, version: u64, offset: u64) -> OpaqueRef {
    modality_ref(
        "content",
        index
            .saturating_mul(4_096)
            .saturating_add(version.saturating_mul(32))
            .saturating_add(offset),
    )
}

fn modality_bundle(index: u64, version: u64) -> ArtifactBundle {
    let artifact_id =
        ArtifactId::from_token(&modality_identity(index, 1)).expect("valid artifact id");
    let occurrence_id =
        OccurrenceId::from_token(&modality_identity(index, 2)).expect("valid occurrence id");
    let rendition_id =
        RenditionId::from_token(&modality_identity(index, 3)).expect("valid rendition id");
    let segment_id = SegmentId::from_token(&modality_identity(index, 4)).expect("valid segment id");
    let derivation = Derivation {
        id: DerivationId::from_token(&modality_identity(index, 5)).expect("valid derivation id"),
        transform_ref: modality_ref("transform", 6),
        implementation_ref: modality_ref("implementation", 7),
        version_ref: modality_ref("version", 8),
        model_ref: None,
        inputs: vec![ResourceId::Occurrence(occurrence_id.clone())],
    };
    ArtifactBundle {
        protocol_version: ARTIFACT_PROTOCOL_VERSION,
        privacy: PrivacyAttestation {
            scanner_ref: modality_ref("scanner", 9),
            policy_version_ref: modality_ref("policyversion", 10),
            raw_pii_persisted: false,
            local_identifiers_persisted: false,
        },
        artifacts: vec![Artifact {
            id: artifact_id.clone(),
            content_ref: modality_content(index, version, 20),
            modality: ModalityKind::Document,
            schema_ref: modality_ref("schema", 11),
            content_version: version,
        }],
        occurrences: vec![Occurrence {
            id: occurrence_id.clone(),
            artifact_id,
            source_ref: modality_ref("source", 12),
            observation_version: version,
            policy: PolicyEnvelope {
                tenant_ref: modality_ref("tenant", 13),
                access_policy_ref: modality_ref("policy", 14),
                classification: Classification::Internal,
                retention_policy_ref: modality_ref("retention", 15),
                deletion_policy_ref: modality_ref("deletion", 16),
                legal_hold_ref: None,
                purpose_refs: vec![modality_ref("purpose", 17)],
            },
        }],
        renditions: vec![Rendition {
            id: rendition_id.clone(),
            occurrence_id,
            content_ref: modality_content(index, version, 21),
            modality: ModalityKind::Document,
            schema_ref: modality_ref("schema", 18),
            derivation: derivation.clone(),
        }],
        segments: vec![Segment {
            id: segment_id.clone(),
            rendition_id,
            parent_segment_id: None,
            kind: SegmentKind::Page,
            ordinal: 0,
            schema_ref: modality_ref("schema", 19),
        }],
        features: vec![Feature {
            id: FeatureId::from_token(&modality_identity(index, 6)).expect("valid feature id"),
            subject: ResourceId::Segment(segment_id.clone()),
            kind: FeatureKind::Statistic,
            value_ref: modality_ref("value", index.saturating_add(21)),
            schema_ref: modality_ref("schema", 22),
            derivation: derivation.clone(),
        }],
        evidence_loci: vec![EvidenceLocus {
            id: EvidenceLocusId::from_token(&modality_identity(index, 7))
                .expect("valid evidence locus id"),
            subject: ResourceId::Segment(segment_id),
            address: EvidenceAddress::CharacterRange { start: 0, end: 4 },
            policy_ref: modality_ref("policy", 14),
            derivation_ref: derivation.id,
        }],
    }
}

fn modality_ingest(
    index: u64,
    version: u64,
    expected_version: Option<u64>,
    idempotency_offset: u64,
) -> ServedIngest<ProbeDocument> {
    ServedIngest {
        idempotency_ref: modality_ref(
            "idempotency",
            index.saturating_mul(32).saturating_add(idempotency_offset),
        ),
        target_occurrence_id: OccurrenceId::from_token(&modality_identity(index, 2))
            .expect("valid occurrence id"),
        expected_version,
        bundle: modality_bundle(index, version),
        value: ProbeDocument {
            pages: u32::try_from((index % 64).saturating_add(version)).unwrap_or(1),
        },
    }
}

fn modality_scope() -> ServedPolicyScope {
    ServedPolicyScope {
        tenant_ref: modality_ref("tenant", 13),
        access_policy_ref: modality_ref("policy", 14),
        purpose_ref: modality_ref("purpose", 17),
        maximum_classification: Classification::Internal,
    }
}

fn populated_modality_runtime(
    scale: usize,
) -> Result<
    (
        ServedModalityRuntime<ProbeDocument>,
        Vec<ServedIngest<ProbeDocument>>,
    ),
    ProbeError,
> {
    let commands: Vec<_> = (1..=scale as u64)
        .map(|index| modality_ingest(index, 1, None, 8))
        .collect();
    let mut runtime = ServedModalityRuntime::new();
    runtime.ingest_stream(commands.clone())?;
    Ok((runtime, commands))
}

pub(super) fn probe_modality_kernel(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-001" => probe_modality_stream_rollback(scale),
        "G37-HP-002" => probe_modality_cursor_page(scale),
        "G37-HP-003" => probe_modality_cdc_suffix(scale),
        "G37-HP-004" => probe_modality_snapshot_recovery(scale),
        "G37-HP-005" => probe_modality_active_count(scale),
        _ => Err("invalid modality probe row".into()),
    }
}

/// G37-HP-001: streamed ingest matches sequential ingest, and rolling back a stream
/// whose second command is invalid restores the pre-rollback snapshot exactly.
/// Extracted from [`probe_modality_kernel`].
fn probe_modality_stream_rollback(scale: usize) -> Result<Observation, ProbeError> {
    let commands: Vec<_> = (1..=scale as u64)
        .map(|index| modality_ingest(index, 1, None, 8))
        .collect();
    let mut streamed = ServedModalityRuntime::new();
    let (outcomes, latency) = timed(|| streamed.ingest_stream(commands.clone()));
    let outcomes = outcomes?;

    let mut sequential = ServedModalityRuntime::new();
    for command in commands {
        sequential.ingest(command)?;
    }
    let before_rollback = streamed.snapshot()?;
    let rollback_result = streamed.ingest_stream([
        modality_ingest(1, 2, Some(1), 9),
        modality_ingest(2, 2, Some(u64::MAX), 10),
    ]);
    let rollback_restored = rollback_result.is_err()
        && streamed.snapshot()? == before_rollback
        && streamed == sequential;
    Ok(Observation {
        work_units: (scale.saturating_add(2)).max(1) as u64,
        memory_bytes: before_rollback.capacity().max(1) as u64,
        latency_ns: latency,
        equivalent: outcomes.len() == scale && rollback_restored,
    })
}

/// G37-HP-002: a cursor-paged query matches the reference (in-order, filtered,
/// take(16)) computation over the same commands. Extracted from
/// [`probe_modality_kernel`].
fn probe_modality_cursor_page(scale: usize) -> Result<Observation, ProbeError> {
    let (runtime, commands) = populated_modality_runtime(scale)?;
    let cursor = commands[scale / 2].target_occurrence_id.clone();
    let query = ServedQuery {
        scope: modality_scope(),
        modality: Some(ModalityKind::Document),
        segment_kind: Some(SegmentKind::Page),
        after: Some(cursor.clone()),
        limit: 16,
        include_cold: false,
    };
    let (page, latency) = timed(|| runtime.query(&query));
    let page = page?;
    let expected: Vec<_> = commands
        .iter()
        .map(|command| command.target_occurrence_id.clone())
        .filter(|occurrence_id| occurrence_id > &cursor)
        .take(16)
        .collect();
    let actual: Vec<_> = page
        .records
        .iter()
        .map(|record| record.occurrence_id.clone())
        .collect();
    let memory = serde_json::to_vec(&page)?.capacity().max(1) as u64;
    Ok(Observation {
        work_units: (scale.ilog2() as u64 + actual.len() as u64 + 1).max(1),
        memory_bytes: memory,
        latency_ns: latency,
        equivalent: actual == expected,
    })
}

/// G37-HP-003: the CDC suffix (`events_after`) matches the reference slice, and every
/// event's sequence number is exact. Extracted from [`probe_modality_kernel`].
fn probe_modality_cdc_suffix(scale: usize) -> Result<Observation, ProbeError> {
    let (runtime, commands) = populated_modality_runtime(scale)?;
    let sequence = (scale / 2) as u64;
    let (events, latency) = timed(|| runtime.events_after(sequence, 16));
    let expected_occurrences: Vec<_> = commands
        .iter()
        .skip(scale / 2)
        .take(16)
        .map(|command| command.target_occurrence_id.clone())
        .collect();
    let actual_occurrences: Vec<_> = events
        .iter()
        .map(|event| event.occurrence_id.clone())
        .collect();
    let sequences_are_exact = events
        .iter()
        .enumerate()
        .all(|(index, event)| event.sequence == sequence + index as u64 + 1);
    let memory = serde_json::to_vec(&events)?.capacity().max(1) as u64;
    Ok(Observation {
        work_units: events.len().max(1) as u64,
        memory_bytes: memory,
        latency_ns: latency,
        equivalent: actual_occurrences == expected_occurrences && sequences_are_exact,
    })
}

/// G37-HP-004: recovering a runtime from its own snapshot round-trips exactly, and
/// replaying the first already-applied command is recognized as an idempotent replay.
/// Extracted from [`probe_modality_kernel`].
fn probe_modality_snapshot_recovery(scale: usize) -> Result<Observation, ProbeError> {
    let (runtime, commands) = populated_modality_runtime(scale)?;
    let (recovered, latency) = timed(|| -> Result<_, eg_modality::ServedError> {
        let snapshot = runtime.snapshot()?;
        let recovered = ServedModalityRuntime::recover(&snapshot)?;
        Ok((snapshot, recovered))
    });
    let (snapshot, mut recovered) = recovered?;
    let replay = recovered.ingest(commands[0].clone())?;
    Ok(Observation {
        work_units: (scale.saturating_mul(2)).max(1) as u64,
        memory_bytes: snapshot.capacity().max(1) as u64,
        latency_ns: latency,
        equivalent: replay.disposition == ApplyDisposition::IdempotentReplay
            && recovered == runtime,
    })
}

/// G37-HP-005: the runtime's active count matches a full paginated scan. Extracted
/// from [`probe_modality_kernel`].
fn probe_modality_active_count(scale: usize) -> Result<Observation, ProbeError> {
    let (runtime, _) = populated_modality_runtime(scale)?;
    let (active, latency) = timed(|| runtime.len());
    let mut scanned = 0usize;
    let mut after = None;
    loop {
        let page = runtime.query(&ServedQuery {
            scope: modality_scope(),
            modality: None,
            segment_kind: None,
            after,
            limit: 1_000,
            include_cold: true,
        })?;
        if page.records.is_empty() {
            break;
        }
        scanned = scanned.saturating_add(page.records.len());
        after = page.next;
        if page.records.len() < 1_000 {
            break;
        }
    }
    Ok(Observation {
        work_units: 1,
        memory_bytes: std::mem::size_of::<usize>() as u64,
        latency_ns: latency,
        equivalent: active == scanned && active == scale,
    })
}
