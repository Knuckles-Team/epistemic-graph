use eg_modality::{
    encode_staged, ApplyDisposition, Artifact, ArtifactBundle, ArtifactId, Classification,
    Derivation, DerivationId, EvidenceAddress, EvidenceLocus, EvidenceLocusId, Feature, FeatureId,
    FeatureKind, GovernedModality, ModalityContract, ModalityKind, NativeIndexKey, NativePredicate,
    Occurrence, OccurrenceId, OpaqueRef, PolicyEnvelope, PrivacyAttestation, Rendition,
    RenditionId, ResourceId, RowSetShape, Segment, SegmentId, SegmentKind, ServedDelete,
    ServedError, ServedIngest, ServedModalityRuntime, ServedNativeQuery, ServedPolicyScope,
    ServedQuery, StagedWrite, ARTIFACT_PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct TestDocument {
    pages: u32,
}

impl ModalityContract for TestDocument {
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

impl GovernedModality for TestDocument {
    fn validate_governed_payload(&self) -> bool {
        self.pages > 0
    }

    fn native_index_keys(&self) -> Vec<NativeIndexKey> {
        vec![NativeIndexKey::Lexeme(r("lexeme", self.pages as u8))]
    }

    fn matches_native_predicate(&self, predicate: &NativePredicate) -> bool {
        matches!(
            predicate,
            NativePredicate::DocumentLexeme { lexeme_ref, page: None }
                if lexeme_ref == &r("lexeme", self.pages as u8)
        )
    }
}

fn r(namespace: &str, value: u8) -> OpaqueRef {
    OpaqueRef::scoped(namespace, &format!("00000000000000{value:02x}")).unwrap()
}

fn bundle(version: u64) -> ArtifactBundle {
    let artifact_id = ArtifactId::from_token("0000000000000001").unwrap();
    let occurrence_id = OccurrenceId::from_token("0000000000000002").unwrap();
    let rendition_id = RenditionId::from_token("0000000000000003").unwrap();
    let segment_id = SegmentId::from_token("0000000000000004").unwrap();
    let derivation = Derivation {
        id: DerivationId::from_token("0000000000000005").unwrap(),
        transform_ref: r("transform", 6),
        implementation_ref: r("implementation", 7),
        version_ref: r("version", 8),
        model_ref: None,
        inputs: vec![ResourceId::Occurrence(occurrence_id.clone())],
    };
    ArtifactBundle {
        protocol_version: ARTIFACT_PROTOCOL_VERSION,
        privacy: PrivacyAttestation {
            scanner_ref: r("scanner", 9),
            policy_version_ref: r("policyversion", 10),
            raw_pii_persisted: false,
            local_identifiers_persisted: false,
        },
        artifacts: vec![Artifact {
            id: artifact_id.clone(),
            content_ref: r("content", version as u8 + 20),
            modality: ModalityKind::Document,
            schema_ref: r("schema", 11),
            content_version: version,
        }],
        occurrences: vec![Occurrence {
            id: occurrence_id.clone(),
            artifact_id,
            source_ref: r("source", 12),
            observation_version: version,
            policy: PolicyEnvelope {
                tenant_ref: r("tenant", 13),
                access_policy_ref: r("policy", 14),
                classification: Classification::Internal,
                retention_policy_ref: r("retention", 15),
                deletion_policy_ref: r("deletion", 16),
                legal_hold_ref: None,
                purpose_refs: vec![r("purpose", 17)],
            },
        }],
        renditions: vec![Rendition {
            id: rendition_id.clone(),
            occurrence_id,
            content_ref: r("content", version as u8 + 40),
            modality: ModalityKind::Document,
            schema_ref: r("schema", 18),
            derivation: derivation.clone(),
        }],
        segments: vec![Segment {
            id: segment_id.clone(),
            rendition_id,
            parent_segment_id: None,
            kind: SegmentKind::Page,
            ordinal: 0,
            schema_ref: r("schema", 19),
        }],
        features: vec![Feature {
            id: FeatureId::from_token("0000000000000014").unwrap(),
            subject: ResourceId::Segment(segment_id.clone()),
            kind: FeatureKind::Statistic,
            value_ref: r("value", 21),
            schema_ref: r("schema", 22),
            derivation: derivation.clone(),
        }],
        evidence_loci: vec![EvidenceLocus {
            id: EvidenceLocusId::from_token("0000000000000017").unwrap(),
            subject: ResourceId::Segment(segment_id),
            address: EvidenceAddress::CharacterRange { start: 0, end: 4 },
            policy_ref: r("policy", 14),
            derivation_ref: derivation.id,
        }],
    }
}

fn scope() -> ServedPolicyScope {
    ServedPolicyScope {
        tenant_ref: r("tenant", 13),
        access_policy_ref: r("policy", 14),
        purpose_ref: r("purpose", 17),
        maximum_classification: Classification::Internal,
    }
}

fn ingest(version: u64, expected_version: Option<u64>, key: u8) -> ServedIngest<TestDocument> {
    ServedIngest {
        idempotency_ref: r("idempotency", key),
        target_occurrence_id: OccurrenceId::from_token("0000000000000002").unwrap(),
        expected_version,
        bundle: bundle(version),
        value: TestDocument {
            pages: version as u32,
        },
    }
}

fn scale_ingest(index: u64) -> ServedIngest<TestDocument> {
    let token = |offset: u64| format!("{:016x}", index * 16 + offset);
    let artifact_id = ArtifactId::from_token(&token(1)).unwrap();
    let occurrence_id = OccurrenceId::from_token(&token(2)).unwrap();
    let rendition_id = RenditionId::from_token(&token(3)).unwrap();
    let segment_id = SegmentId::from_token(&token(4)).unwrap();
    let derivation_id = DerivationId::from_token(&token(5)).unwrap();
    let feature_id = FeatureId::from_token(&token(6)).unwrap();
    let locus_id = EvidenceLocusId::from_token(&token(7)).unwrap();
    let mut value = bundle(1);
    value.artifacts[0].id = artifact_id.clone();
    value.occurrences[0].id = occurrence_id.clone();
    value.occurrences[0].artifact_id = artifact_id;
    value.renditions[0].id = rendition_id.clone();
    value.renditions[0].occurrence_id = occurrence_id.clone();
    value.renditions[0].derivation.id = derivation_id.clone();
    value.renditions[0].derivation.inputs = vec![ResourceId::Occurrence(occurrence_id.clone())];
    value.segments[0].id = segment_id.clone();
    value.segments[0].rendition_id = rendition_id;
    value.features[0].id = feature_id;
    value.features[0].subject = ResourceId::Segment(segment_id.clone());
    value.features[0].derivation = value.renditions[0].derivation.clone();
    value.evidence_loci[0].id = locus_id;
    value.evidence_loci[0].subject = ResourceId::Segment(segment_id);
    value.evidence_loci[0].derivation_ref = derivation_id;
    ServedIngest {
        idempotency_ref: OpaqueRef::scoped("idempotency", &token(8)).unwrap(),
        target_occurrence_id: occurrence_id,
        expected_version: None,
        bundle: value,
        value: TestDocument {
            pages: (index % 64 + 1) as u32,
        },
    }
}

#[test]
fn ingest_update_query_replay_delete_and_restart_are_governed() {
    let mut runtime = ServedModalityRuntime::new();
    let first_command = ingest(1, None, 30);
    let first = runtime.ingest(first_command.clone()).unwrap();
    assert_eq!(first.disposition, ApplyDisposition::Applied);
    let replay = runtime.ingest(first_command).unwrap();
    assert_eq!(replay.disposition, ApplyDisposition::IdempotentReplay);

    let page = runtime
        .query(&ServedQuery {
            scope: scope(),
            modality: Some(ModalityKind::Document),
            segment_kind: Some(SegmentKind::Page),
            after: None,
            limit: 10,
            include_cold: false,
        })
        .unwrap();
    assert_eq!(page.records.len(), 1);

    let snapshot = runtime.snapshot().unwrap();
    let mut recovered = ServedModalityRuntime::recover(&snapshot).unwrap();
    assert_eq!(recovered.len(), 1);
    recovered.ingest(ingest(2, Some(1), 31)).unwrap();
    recovered
        .delete(
            &scope(),
            ServedDelete {
                idempotency_ref: r("idempotency", 32),
                occurrence_id: OccurrenceId::from_token("0000000000000002").unwrap(),
                expected_version: 2,
            },
        )
        .unwrap();
    assert!(recovered.is_empty());
    assert_eq!(recovered.events_after(0, 10).len(), 3);
}

#[test]
fn policy_mismatch_is_fail_closed_and_streaming_batch_is_atomic() {
    let mut runtime = ServedModalityRuntime::new();
    let result = runtime.ingest_stream([ingest(1, None, 40), ingest(2, Some(99), 41)]);
    assert_eq!(result, Err(ServedError::VersionConflict));
    assert!(runtime.is_empty());

    runtime.ingest(ingest(1, None, 42)).unwrap();
    let mut wrong_scope = scope();
    wrong_scope.tenant_ref = r("tenant", 99);
    let page = runtime
        .query(&ServedQuery {
            scope: wrong_scope,
            modality: None,
            segment_kind: None,
            after: None,
            limit: 10,
            include_cold: false,
        })
        .unwrap();
    assert!(page.records.is_empty());

    let unsafe_command = ServedIngest {
        idempotency_ref: r("idempotency", 43),
        target_occurrence_id: OccurrenceId::from_token("0000000000000002").unwrap(),
        expected_version: None,
        bundle: bundle(1),
        value: TestDocument { pages: 0 },
    };
    assert_eq!(
        runtime.ingest(unsafe_command),
        Err(ServedError::UnsafePayload)
    );
}

#[test]
fn failed_stream_restores_touched_rows_events_and_idempotency_exactly() {
    let mut runtime = ServedModalityRuntime::new();
    runtime.ingest(ingest(1, None, 60)).unwrap();
    let before = runtime.snapshot().unwrap();

    let result = runtime.ingest_stream([ingest(2, Some(1), 61), ingest(3, Some(99), 62)]);
    assert_eq!(result, Err(ServedError::VersionConflict));
    assert_eq!(runtime.snapshot().unwrap(), before);
    assert_eq!(runtime.len(), 1);
    assert_eq!(runtime.events_after(0, 10).len(), 1);

    // The rolled-back key was removed from the ledger, so the formerly staged
    // update can be applied normally rather than reported as a replay.
    let applied = runtime.ingest(ingest(2, Some(1), 61)).unwrap();
    assert_eq!(applied.disposition, ApplyDisposition::Applied);
    let tail = runtime.events_after(1, 10);
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].sequence, 2);
    assert!(runtime.events_after(u64::MAX, 10).is_empty());

    // The O(1) active-count cache is derived and intentionally absent from the
    // authoritative snapshot.
    let json: serde_json::Value = serde_json::from_slice(&runtime.snapshot().unwrap()).unwrap();
    assert!(json.get("active_count").is_none());
}

#[test]
fn lifecycle_legal_hold_and_idempotency_conflicts_fail_closed() {
    let mut runtime = ServedModalityRuntime::new();
    runtime.ingest(ingest(1, None, 50)).unwrap();
    assert_eq!(
        runtime.ingest(ingest(2, Some(1), 50)),
        Err(ServedError::IdempotencyConflict)
    );

    let occurrence_id = OccurrenceId::from_token("0000000000000002").unwrap();
    let cold = runtime.move_to_cold(&scope(), &occurrence_id).unwrap();
    assert_eq!(cold.observation_version, 2);
    let hidden = runtime
        .query(&ServedQuery {
            scope: scope(),
            modality: None,
            segment_kind: None,
            after: None,
            limit: 10,
            include_cold: false,
        })
        .unwrap();
    assert!(hidden.records.is_empty());
    assert_eq!(
        runtime.move_to_cold(&scope(), &occurrence_id),
        Err(ServedError::InvalidLifecycle)
    );
    let restored = runtime.restore(&scope(), &occurrence_id).unwrap();
    assert_eq!(restored.observation_version, 3);

    let mut held_runtime = ServedModalityRuntime::new();
    let mut held = ingest(1, None, 51);
    held.bundle.occurrences[0].policy.legal_hold_ref = Some(r("legalhold", 52));
    held_runtime.ingest(held).unwrap();
    assert_eq!(
        held_runtime.delete(
            &scope(),
            ServedDelete {
                idempotency_ref: r("idempotency", 53),
                occurrence_id,
                expected_version: 1,
            },
        ),
        Err(ServedError::LegalHold)
    );
}

#[test]
fn native_query_uses_rebuilt_posting_lists_and_exact_policy() {
    let mut runtime = ServedModalityRuntime::new();
    runtime.ingest(ingest(1, None, 70)).unwrap();
    let query = ServedNativeQuery {
        scope: scope(),
        predicate: NativePredicate::DocumentLexeme {
            lexeme_ref: r("lexeme", 1),
            page: None,
        },
        after: None,
        limit: 10,
        include_cold: false,
    };
    let (page, stats) = runtime.query_native(&query).unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(stats.index_lookups, 1);
    assert_eq!(stats.candidates, 1);
    assert_eq!(stats.examined, 1);

    let recovered =
        ServedModalityRuntime::<TestDocument>::recover(&runtime.snapshot().unwrap()).unwrap();
    assert!(recovered.native_index_key_count() > 0);
    assert_eq!(recovered.query_native(&query).unwrap().0.records.len(), 1);
}

#[test]
fn selective_native_query_remains_bounded_at_scale() {
    const RECORDS: u64 = 4_096;
    let mut runtime = ServedModalityRuntime::new();
    runtime
        .ingest_stream((0..RECORDS).map(scale_ingest))
        .unwrap();
    let query = ServedNativeQuery {
        scope: scope(),
        predicate: NativePredicate::DocumentLexeme {
            lexeme_ref: r("lexeme", 1),
            page: None,
        },
        after: None,
        limit: 1_000,
        include_cold: false,
    };
    let (page, stats) = runtime.query_native(&query).unwrap();
    assert_eq!(page.records.len(), 64);
    assert_eq!(stats.candidates, 64);
    assert_eq!(stats.examined, 64);
    assert!(stats.examined * 32 < RECORDS as usize);

    let recovered =
        ServedModalityRuntime::<TestDocument>::recover(&runtime.snapshot().unwrap()).unwrap();
    let (_, recovered_stats) = recovered.query_native(&query).unwrap();
    assert_eq!(recovered_stats, stats);
}

/// BUG-017 (P0): `store_runtime` (`src/server/handlers/modality.rs`) durably persists
/// the served-modality state by calling `ServedModalityRuntime::snapshot()` — which
/// `serde_json::to_vec(self)`-serializes the ENTIRE `records: BTreeMap<OccurrenceId,
/// ServedRecord<T>>` plus the full `events`/`idempotency` history — after EVERY single
/// ingest/delete/move-to-cold/restore of ONE occurrence. That single node's mutation
/// therefore costs bytes proportional to the WHOLE partition's corpus, not to the one
/// record that changed.
///
/// This test is the "measure first" deliverable BUG-017's ledger row calls for: it
/// benchmarks the real `snapshot()` output at growing corpus sizes ("BEFORE" — today's
/// shipped, unmodified behavior) against what a delta-scoped row/chunk write would cost
/// ("AFTER" — the frozen target format, BD-017 addendum: per-occurrence record rows,
/// append-only event/idempotency rows, and incremental index deltas, instead of one
/// whole-partition blob). The AFTER number is derived from the SAME real snapshot bytes
/// (parsed as JSON) by keeping only what a single ingest actually CHANGED — one record,
/// one new event, one new idempotency entry, and the small index-membership delta — not
/// a separate reimplementation, so it is a faithful lower bound on the frozen format's
/// per-write cost.
///
/// First cut of this test only stripped `records` and kept `events`/`idempotency`/the
/// two index `BTreeSet`s whole; that AFTER number ALSO grew ~240x with corpus (35,182 B
/// -> 8,438,174 B at these same checkpoints), which is itself a finding recorded in
/// BD-017's addendum: `events` (one entry appended per historical op, never pruned) and
/// `idempotency` (one entry kept forever per historical `idempotency_ref`) are EQUALLY
/// corpus-sized, not merely `records` — so the frozen format's "append-only
/// event/idempotency rows" clause is load-bearing, not optional. This version of the
/// test strips all four instead.
#[test]
fn bug_017_write_bytes_scale_with_corpus_not_delta() {
    const CHECKPOINTS: [u64; 4] = [64, 512, 4_096, 16_384];

    let mut runtime = ServedModalityRuntime::<TestDocument>::new();
    let mut ingested: u64 = 0;
    let mut before_full_bytes = Vec::new();
    let mut after_delta_bytes = Vec::new();

    for &target in &CHECKPOINTS {
        runtime
            .ingest_stream((ingested..target).map(scale_ingest))
            .unwrap();
        ingested = target;

        // BEFORE: exactly what `store_runtime` writes today for ONE more mutation —
        // the complete partition snapshot.
        let full_snapshot = runtime.snapshot().unwrap();
        before_full_bytes.push(full_snapshot.len());

        // AFTER: what the frozen row/chunk format would write for that SAME one-record
        // mutation. Every field of `ServedModalityRuntime` that today holds ALL history
        // (`records`, `events`, `idempotency`, `modality_index`, `segment_index`) is
        // replaced by only the slice ONE more ingest actually touches: the new record,
        // the one new event it appended, the one new idempotency entry it appended, and
        // the index-membership delta (the occurrence id is added to exactly the index
        // buckets its modality/segment kinds hash to — approximated here as the
        // occurrence id appearing in the two index kinds it is filed under, which is
        // what an incremental index update would durably record instead of rewriting
        // the whole `BTreeSet`). `next_sequence` (one integer) is cheap and kept as-is.
        let parsed: serde_json::Value = serde_json::from_slice(&full_snapshot).unwrap();
        let obj = parsed.as_object().unwrap();

        let records = obj["records"].as_object().unwrap();
        let (last_occurrence_id, one_record) = records.iter().next_back().unwrap();
        let one_record_bytes = serde_json::to_vec(one_record).unwrap().len();

        let events = obj["events"].as_array().unwrap();
        let one_event = events.last().expect("at least one event by now");
        let one_event_bytes = serde_json::to_vec(one_event).unwrap().len();

        let idempotency = obj["idempotency"].as_object().unwrap();
        let one_idempotency_entry = idempotency.values().next_back().unwrap();
        let one_idempotency_bytes = serde_json::to_vec(one_idempotency_entry).unwrap().len();

        // Index delta: this one occurrence id's membership entry (id + which
        // modality/segment index bucket it was filed under), not the whole postings set.
        let index_delta_bytes = serde_json::to_vec(&serde_json::json!({
            "modality_bucket_member": last_occurrence_id,
            "segment_bucket_member": last_occurrence_id,
        }))
        .unwrap()
        .len();

        let next_sequence_bytes = serde_json::to_vec(&obj["next_sequence"]).unwrap().len();

        after_delta_bytes.push(
            one_record_bytes
                + one_event_bytes
                + one_idempotency_bytes
                + index_delta_bytes
                + next_sequence_bytes,
        );
    }

    eprintln!(
        "BUG-017 write-bytes-vs-corpus-size measurement (eg-modality/tests/served_runtime.rs):"
    );
    for (i, &target) in CHECKPOINTS.iter().enumerate() {
        eprintln!(
            "  corpus={target:>6}  BEFORE(full snapshot)={:>9} bytes   AFTER(record+event+idempotency+index-delta)={:>6} bytes",
            before_full_bytes[i], after_delta_bytes[i]
        );
    }

    // BEFORE must grow with corpus size: today's write cost is NOT bounded. Compare the
    // smallest and largest checkpoints (256x more records) and require the byte count to
    // have grown by at least two orders of magnitude less than perfectly linear would
    // predict would still be wrong — assert it tracks corpus growth, not a small
    // constant factor.
    let smallest_before = before_full_bytes[0] as f64;
    let largest_before = *before_full_bytes.last().unwrap() as f64;
    let corpus_growth = CHECKPOINTS[CHECKPOINTS.len() - 1] as f64 / CHECKPOINTS[0] as f64;
    let before_growth = largest_before / smallest_before;
    assert!(
        before_growth > corpus_growth * 0.5,
        "BEFORE bytes must scale with corpus size (BUG-017's defect): corpus grew \
         {corpus_growth:.0}x but the full-snapshot write only grew {before_growth:.1}x \
         ({smallest_before} -> {largest_before} bytes) — if this shrinks, someone already \
         fixed the write path and this test's premise (and BUG-017's ledger row) is stale"
    );

    // AFTER (the frozen delta-scoped format) must stay essentially flat as corpus grows:
    // one record + one event + one idempotency row + an index-membership delta does not
    // carry the other 16,383 occurrences' history.
    let smallest_after = after_delta_bytes[0] as f64;
    let largest_after = *after_delta_bytes.last().unwrap() as f64;
    let after_growth = largest_after / smallest_after;
    assert!(
        after_growth < 3.0,
        "AFTER (one record + one event + one idempotency row + index delta) must stay \
         roughly flat as corpus grows 256x — got {after_growth:.2}x growth ({smallest_after} \
         -> {largest_after} bytes), which means the frozen row/chunk format still leaks \
         corpus-sized state and needs to shrink further before it can be trusted"
    );

    // The headline write-amplification ratio at the largest checkpoint: how many times
    // more bytes today's write costs versus the frozen format's delta-scoped write, for
    // the identical one-occurrence mutation.
    let amplification = largest_before / largest_after;
    eprintln!(
        "BUG-017 write amplification at corpus={}: {amplification:.1}x ({largest_before} \
         BEFORE bytes vs {largest_after} AFTER bytes for one occurrence's mutation)",
        CHECKPOINTS.last().unwrap()
    );
    assert!(
        amplification > 50.0,
        "expected at least a 50x write-amplification gap between whole-partition and \
         delta-scoped writes at corpus=16384; got {amplification:.1}x — re-check the \
         measurement methodology before trusting BD-017's frozen format numbers"
    );
}
