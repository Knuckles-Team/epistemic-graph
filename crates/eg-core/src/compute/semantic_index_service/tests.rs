use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};

use super::{
    coalesce_sql_source_record_to_s1, source_reconciliation_wakeup_digest,
    sql_source_deletion_proof, sql_source_revision_for_epoch, sql_source_stage_artifact,
    validate_page_revision_against_binding, SemanticIndexService,
    SemanticSourceReconciliationCheckpoint, SemanticSourceReconciliationPhase,
    SemanticSqlSourceReadPage, SemanticSqlSourceReadPort, SemanticSqlSourceRecord,
    SemanticSqlSourceValue,
};
use crate::test_scope_grant::{TEST_PRINCIPAL, TEST_PROOF};
use eg_types::mutation_batch::{
    CommittedVersion, DurabilityDomain, MutationOutboxIntent, MutationOutboxRecord,
    MutationScopeIdentity, MUTATION_BATCH_VERSION,
};
use eg_types::semantic_index::{
    SemanticBinding, SemanticDigest, SemanticIndexError, SemanticSourceDirtyIntent,
    SemanticStageArtifact, SemanticStageIntent, SemanticStageOutcome, SemanticStageReceipt,
    SemanticStageTransition, SqlColumnRef, SEMANTIC_SOURCE_DIRTY_TOPIC, SEMANTIC_SQL_CATALOG_ID,
};

fn digest(byte: u8) -> SemanticDigest {
    SemanticDigest::from_bytes([byte; 32])
}

fn content_digest(bytes: &[u8]) -> SemanticDigest {
    SemanticDigest::from_bytes(Sha256::digest(bytes).into())
}

fn binding(source_revision: &str) -> SemanticBinding {
    binding_for("tenant-a", "binding:articles-body", source_revision)
}

fn binding_for(tenant_id: &str, binding_id: &str, source_revision: &str) -> SemanticBinding {
    super::semantic_binding_test_fixture(
        tenant_id,
        binding_id,
        "articles",
        source_revision,
        "sha256:schema",
        "sha256:field-set",
        "2026-09-08T00:00:00Z",
    )
}

fn open_tenant_a_service(dir: &std::path::Path) -> SemanticIndexService {
    SemanticIndexService::open(
        dir,
        Arc::new(super::test_fixture::TestSemanticScopeVerifier {
            tenant: "tenant-a",
            principal: TEST_PRINCIPAL,
            proof: TEST_PROOF,
        }),
        TEST_PRINCIPAL,
        TEST_PROOF,
        "tenant-a",
        "binding:articles-body",
    )
    .unwrap()
}

fn open_admitted_tenant_a_service(
    dir: &std::path::Path,
    binding: &SemanticBinding,
) -> SemanticIndexService {
    let service = open_tenant_a_service(dir);
    service.admit_binding(binding, 1).unwrap();
    service
}

fn source_record(
    binding: &SemanticBinding,
    record_identity_byte: u8,
    source_revision: &str,
    value: SemanticSqlSourceValue,
) -> SemanticSqlSourceRecord {
    let selector = SqlColumnRef {
        catalog_id: SEMANTIC_SQL_CATALOG_ID.into(),
        schema_id: "public".into(),
        table_id: "articles".into(),
        column_id: "body".into(),
    };
    super::test_fixture::source_record(
        binding,
        &selector,
        digest(record_identity_byte),
        source_revision,
        value,
    )
}

fn dirty_record(method_input_byte: u8, source: u64, target: u64) -> MutationOutboxRecord {
    dirty_record_for("tenant-a", method_input_byte, source, target)
}

fn dirty_record_for(
    tenant: &str,
    method_input_byte: u8,
    source: u64,
    target: u64,
) -> MutationOutboxRecord {
    let identity = MutationScopeIdentity::fixed_native(
        tenant,
        DurabilityDomain::SqlCatalog,
        "catalog",
        "authority-a",
    )
    .unwrap();
    let wakeup = SemanticSourceDirtyIntent::new(
        SemanticDigest::from_bytes(*identity.binding_digest().as_bytes()),
        digest(method_input_byte),
    )
    .to_canonical_cbor()
    .unwrap();
    MutationOutboxRecord {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: "batch-1".into(),
        ordinal: 0,
        identity,
        committed_version: CommittedVersion::Native { source, target },
        commit_sequence: None,
        intent: MutationOutboxIntent {
            topic: SEMANTIC_SOURCE_DIRTY_TOPIC.into(),
            key: "batch-1".into(),
            payload: wakeup,
            headers: BTreeMap::new(),
        },
        created_at_ms: 1,
    }
}

fn record_revision(record: &MutationOutboxRecord) -> String {
    let _ = record;
    sql_source_revision_for_epoch(digest(170), 2).unwrap()
}

#[test]
fn tenant_source_revision_binds_authority_and_epoch() {
    let revision = sql_source_revision_for_epoch(digest(170), 42).unwrap();
    assert_eq!(
            revision,
            "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:42"
        );
    assert!(sql_source_revision_for_epoch(digest(170), 0).is_err());
    assert!(sql_source_revision_for_epoch(digest(171), 43).is_ok());
}

#[test]
fn source_bytes_change_without_changing_the_resolved_entity() {
    let authority = digest(170);
    let first_revision = sql_source_revision_for_epoch(authority, 1).unwrap();
    let first_binding = binding(&first_revision);
    let first = source_record(
        &first_binding,
        11,
        &first_revision,
        SemanticSqlSourceValue::Present {
            source_bytes: b"article body at source revision one".to_vec(),
        },
    );
    let changed = source_record(
        &first_binding,
        11,
        &sql_source_revision_for_epoch(authority, 2).unwrap(),
        SemanticSqlSourceValue::Present {
            source_bytes: b"article body at source revision two".to_vec(),
        },
    );
    assert_eq!(first.source_entity_id(), changed.source_entity_id());
    assert_ne!(
        first.source_content_digest(),
        changed.source_content_digest()
    );
}

#[test]
fn repeated_dirty_notifications_use_authoritative_content_once() {
    let record = dirty_record(31, 1, 2);
    let revision = record_revision(&record);
    let binding = binding(&revision);
    let source = source_record(
        &binding,
        11,
        &revision,
        SemanticSqlSourceValue::Present {
            source_bytes: b"authoritative article body".to_vec(),
        },
    );
    let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
    let first = coalesce_sql_source_record_to_s1(&binding, scope_digest, &record, &source).unwrap();

    let repeated = dirty_record(33, 1, 2);
    let second =
        coalesce_sql_source_record_to_s1(&binding, scope_digest, &repeated, &source).unwrap();
    assert_eq!(first, second);
    assert_eq!(
        first.input_digest,
        content_digest(b"authoritative article body")
    );
    assert_ne!(first.input_digest, digest(31));
    assert_ne!(first.input_digest, digest(33));
}

#[test]
fn sql_acl_decision_is_wrapped_before_s1_manifest_artifact_creation() {
    let revision = sql_source_revision_for_epoch(digest(170), 2).unwrap();
    let binding = binding(&revision);
    let source = source_record(
        &binding,
        11,
        &revision,
        SemanticSqlSourceValue::Present {
            source_bytes: b"authorized article body".to_vec(),
        },
    );
    let raw_decision_digest = source.authorization_receipt_digest;
    let authorization = source
        .authorization_receipt(&binding, "2026-09-08T00:00:05Z")
        .unwrap();
    assert_eq!(
        authorization.policy_decision_digest,
        raw_decision_digest.to_string()
    );
    assert_ne!(
        authorization.authorization_receipt_digest, raw_decision_digest,
        "the ACL decision digest is not itself the full receipt digest"
    );

    let record = dirty_record(91, 1, 2);
    let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
    let intent =
        coalesce_sql_source_record_to_s1(&binding, scope_digest, &record, &source).unwrap();
    let transition = SemanticStageTransition {
        intent: intent.clone(),
        receipt: SemanticStageReceipt {
            intent_digest: intent.intent_digest,
            output_digest: source.source_content_digest(),
            cursor: "source:complete".to_string(),
            completed_at: "2026-09-08T00:00:06Z".to_string(),
            outcome: SemanticStageOutcome::Completed,
        },
        generation_checkpoint: None,
    };
    let artifact =
        sql_source_stage_artifact(&binding, &transition, &source, "2026-09-08T00:00:05Z").unwrap();
    let SemanticStageArtifact::SqlSourceManifest {
        manifest,
        authorization,
    } = artifact
    else {
        panic!("S1 source completion must create a SQL manifest artifact");
    };
    assert_eq!(
        authorization.policy_decision_digest,
        raw_decision_digest.to_string()
    );
    assert_eq!(
        manifest.authorization_receipt_digest,
        authorization.authorization_receipt_digest
    );
    assert_ne!(
        manifest.authorization_receipt_digest, raw_decision_digest,
        "the persisted manifest must point at the canonical full receipt"
    );
}

#[test]
fn s1_artifact_rejects_source_bytes_different_from_admitted_intent() {
    let revision = sql_source_revision_for_epoch(digest(170), 2).unwrap();
    let binding = binding(&revision);
    let source_a = source_record(
        &binding,
        11,
        &revision,
        SemanticSqlSourceValue::Present {
            source_bytes: b"source bytes admitted by S1".to_vec(),
        },
    );
    let source_b = source_record(
        &binding,
        11,
        &revision,
        SemanticSqlSourceValue::Present {
            source_bytes: b"different bytes read during completion".to_vec(),
        },
    );
    let record = dirty_record(92, 1, 2);
    let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
    let intent =
        coalesce_sql_source_record_to_s1(&binding, scope_digest, &record, &source_a).unwrap();
    let transition = SemanticStageTransition {
        intent: intent.clone(),
        receipt: SemanticStageReceipt {
            intent_digest: intent.intent_digest,
            output_digest: source_b.source_content_digest(),
            cursor: "source:complete".to_string(),
            completed_at: "2026-09-08T00:00:06Z".to_string(),
            outcome: SemanticStageOutcome::Completed,
        },
        generation_checkpoint: None,
    };
    assert!(matches!(
        sql_source_stage_artifact(&binding, &transition, &source_b, "2026-09-08T00:00:05Z"),
        Err(SemanticIndexError::StageArtifactMismatch)
    ));
}

#[test]
fn sql_source_s1_rejects_idempotent_noop_for_fresh_manifest() {
    let revision = sql_source_revision_for_epoch(digest(170), 2).unwrap();
    let binding = binding(&revision);
    let source = source_record(
        &binding,
        11,
        &revision,
        SemanticSqlSourceValue::Present {
            source_bytes: b"source bytes for completed S1".to_vec(),
        },
    );
    let record = dirty_record(93, 1, 2);
    let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
    let intent =
        coalesce_sql_source_record_to_s1(&binding, scope_digest, &record, &source).unwrap();
    let transition = SemanticStageTransition {
        intent: intent.clone(),
        receipt: SemanticStageReceipt {
            intent_digest: intent.intent_digest,
            output_digest: source.source_content_digest(),
            cursor: "source:idempotent-noop".to_string(),
            completed_at: "2026-09-08T00:00:07Z".to_string(),
            outcome: SemanticStageOutcome::IdempotentNoop,
        },
        generation_checkpoint: None,
    };

    // A SQL source S1 must produce a manifest with a completed receipt;
    // the generic no-op/None-artifact path cannot stand in for that proof.
    assert!(matches!(
        sql_source_stage_artifact(&binding, &transition, &source, "2026-09-08T00:00:05Z"),
        Err(SemanticIndexError::StageArtifactMismatch)
    ));
}

struct SingleSourcePage {
    source: SemanticSqlSourceRecord,
}

impl SemanticSqlSourceReadPort for SingleSourcePage {
    fn read_current_sql_source_page(
        &self,
        _binding: &SemanticBinding,
        _wakeup: &SemanticSourceDirtyIntent,
        _record: &MutationOutboxRecord,
        cursor: Option<&[u8]>,
    ) -> Result<SemanticSqlSourceReadPage, SemanticIndexError> {
        if cursor.is_some() {
            return Err(SemanticIndexError::SourceManifestMismatch);
        }
        Ok(SemanticSqlSourceReadPage {
            source_revision: self.source.source_revision.clone(),
            complete_snapshot_receipt_digest: Some(digest(77)),
            sources: vec![self.source.clone()],
            next_cursor: None,
            complete: true,
        })
    }
}

#[test]
fn durable_service_replays_repeated_dirty_after_authoritative_read() {
    let dir = std::env::temp_dir().join(format!(
        "eg-semantic-source-service-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let record = dirty_record_for("native", 61, 1, 2);
    let revision = record_revision(&record);
    let binding = binding_for("native", "binding:articles-body", &revision);
    let source = source_record(
        &binding,
        11,
        &revision,
        SemanticSqlSourceValue::Present {
            source_bytes: b"durable authoritative body".to_vec(),
        },
    );
    let service = SemanticIndexService::open(
        &dir,
        Arc::new(super::test_fixture::TestSemanticScopeVerifier {
            tenant: "native",
            principal: TEST_PRINCIPAL,
            proof: TEST_PROOF,
        }),
        TEST_PRINCIPAL,
        TEST_PROOF,
        "native",
        "binding:articles-body",
    )
    .unwrap();
    service.admit_binding(&binding, 1).unwrap();
    let read_port = SingleSourcePage { source };

    let first = service
        .admit_sql_source_dirty_record(&record, &read_port, 2)
        .unwrap();
    let repeated = dirty_record_for("native", 63, 1, 2);
    let second = service
        .admit_sql_source_dirty_record(&repeated, &read_port, 3)
        .unwrap();
    assert!(!first.replayed);
    assert!(second.replayed);
    assert_eq!(first.mutation_digest, second.mutation_digest);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn newer_dirty_wakeup_waits_for_an_older_finalizing_checkpoint() {
    let dir = std::env::temp_dir().join(format!(
        "eg-semantic-source-reconcile-r2-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let record_r1 = dirty_record(81, 41, 42);
    let record_r2 = dirty_record(81, 42, 43);
    let revision_r1 = sql_source_revision_for_epoch(digest(170), 41).unwrap();
    let revision_r2 = sql_source_revision_for_epoch(digest(170), 42).unwrap();
    let binding = binding(&revision_r1);
    let service = open_admitted_tenant_a_service(&dir, &binding);
    let wakeup_r1 =
        SemanticSourceDirtyIntent::from_canonical_cbor(&record_r1.intent.payload).unwrap();
    let checkpoint = SemanticSourceReconciliationCheckpoint {
        source_wakeup_digest: source_reconciliation_wakeup_digest(&record_r1, &wakeup_r1),
        source_revision: revision_r1.clone(),
        phase: SemanticSourceReconciliationPhase::FinalizingTombstones,
        source_cursor: None,
        prior_cursor: None,
        rows_seen: 1,
        source_bytes_seen: 2,
        pages_seen: 1,
        complete_snapshot_receipt_digest: Some(digest(77)),
    };
    service
        .store
        .write_source_reconciliation_checkpoint(binding.generation, None, &checkpoint)
        .unwrap();

    let calls_r2 = Arc::new(Mutex::new(Vec::new()));
    let read_port_r2 =
        super::test_fixture::ScriptedSourcePort::empty(revision_r2, Arc::clone(&calls_r2));
    let pending = service
        .admit_sql_source_dirty_reconcile(&record_r2, &read_port_r2, 2)
        .unwrap();
    assert!(!pending.complete);
    assert!(!pending.wakeup_consumed);
    assert_eq!(pending.source_revision, revision_r1);
    assert_eq!(pending.rows_seen, 1);
    assert!(calls_r2.lock().unwrap().is_empty());
    assert!(service
        .store
        .read_source_reconciliation_checkpoint(binding.generation)
        .unwrap()
        .is_some());

    let calls_r1 = Arc::new(Mutex::new(Vec::new()));
    let read_port_r1 = super::test_fixture::ScriptedSourcePort::empty(
        sql_source_revision_for_epoch(digest(170), 41).unwrap(),
        Arc::clone(&calls_r1),
    );
    let admission_r1 = service
        .admit_sql_source_dirty_reconcile(&record_r1, &read_port_r1, 3)
        .unwrap();
    assert!(admission_r1.complete);
    assert!(admission_r1.wakeup_consumed);
    assert!(calls_r1.lock().unwrap().is_empty());
    assert!(service
        .store
        .read_source_reconciliation_checkpoint(binding.generation)
        .unwrap()
        .is_none());

    let admission_r2 = service
        .admit_sql_source_dirty_reconcile(&record_r2, &read_port_r2, 4)
        .unwrap();
    assert!(admission_r2.complete);
    assert!(admission_r2.wakeup_consumed);
    assert_eq!(calls_r2.lock().unwrap().as_slice(), &[None]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn same_dirty_wakeup_resumes_finalization_and_stale_clear_is_rejected() {
    let dir = std::env::temp_dir().join(format!(
        "eg-semantic-source-reconcile-replay-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let record = dirty_record(83, 51, 52);
    let revision = sql_source_revision_for_epoch(digest(170), 51).unwrap();
    let binding = binding(&revision);
    let service = open_admitted_tenant_a_service(&dir, &binding);
    let wakeup = SemanticSourceDirtyIntent::from_canonical_cbor(&record.intent.payload).unwrap();
    let checkpoint = SemanticSourceReconciliationCheckpoint {
        source_wakeup_digest: source_reconciliation_wakeup_digest(&record, &wakeup),
        source_revision: revision.clone(),
        phase: SemanticSourceReconciliationPhase::FinalizingTombstones,
        source_cursor: None,
        prior_cursor: None,
        rows_seen: 1,
        source_bytes_seen: 2,
        pages_seen: 1,
        complete_snapshot_receipt_digest: Some(digest(79)),
    };
    service
        .store
        .write_source_reconciliation_checkpoint(binding.generation, None, &checkpoint)
        .unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let read_port = super::test_fixture::ScriptedSourcePort::empty(revision, Arc::clone(&calls));
    let admission = service
        .admit_sql_source_dirty_reconcile(&record, &read_port, 2)
        .unwrap();
    assert!(admission.complete);
    assert!(calls.lock().unwrap().is_empty());
    // The resumed finalization above already cleared this exact checkpoint, and a
    // clear's batch id is a digest OF the expected predecessor -- so re-issuing the
    // IDENTICAL clear is a replay of an already-committed batch, which the native
    // ledger answers idempotently by contract (`commit_metadata_fenced`'s
    // `Begin::Replay` arm) and must NOT be read as the staleness guard firing.
    // Asserting `is_err()` here pinned the replay path while claiming to pin
    // staleness, and pinned it to the opposite of the contract.
    assert!(
        service
            .store
            .clear_source_reconciliation_checkpoint(binding.generation, &checkpoint)
            .is_ok(),
        "re-issuing the identical committed clear must replay idempotently"
    );
    assert!(
        service
            .store
            .read_source_reconciliation_checkpoint(binding.generation)
            .unwrap()
            .is_none(),
        "an idempotent clear replay leaves the checkpoint cleared"
    );
    // The staleness guard itself: a clear naming a predecessor that is NOT what is
    // durably present is refused. A different predecessor is a different batch id,
    // so this genuinely reaches the compare-and-clear rather than the replay arm.
    let stale = SemanticSourceReconciliationCheckpoint {
        rows_seen: checkpoint.rows_seen + 1,
        ..checkpoint.clone()
    };
    assert!(
        service
            .store
            .clear_source_reconciliation_checkpoint(binding.generation, &stale)
            .is_err(),
        "a clear whose expected predecessor is stale must be refused"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn complete_snapshot_replays_s1_after_reopen_and_emits_cross_epoch_tombstone() {
    let dir = std::env::temp_dir().join(format!(
        "eg-semantic-source-tombstone-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let revision_r1 = sql_source_revision_for_epoch(digest(170), 41).unwrap();
    let revision_r2 = sql_source_revision_for_epoch(digest(170), 42).unwrap();
    let binding = binding(&revision_r1);
    let record_r1 = dirty_record(84, 41, 42);
    let record_r2 = dirty_record(84, 42, 43);
    let source_r1 = source_record(
        &binding,
        11,
        &revision_r1,
        SemanticSqlSourceValue::Present {
            source_bytes: b"row present at epoch one".to_vec(),
        },
    );

    let service = open_admitted_tenant_a_service(&dir, &binding);

    // Seed the completed R1 progress through the public source admission
    // and consumer lifecycle. The test must exercise the same durable
    // stage/receipt rows as production so the R2 absence proof cannot
    // depend on private store tables or test-only mutation injection.
    let read_port_r1 = SingleSourcePage {
        source: source_r1.clone(),
    };
    service
        .admit_sql_source_dirty_record(&record_r1, &read_port_r1, 2)
        .unwrap();
    let lease = super::test_fixture::claim_single_source_stage(&service, binding.generation);
    let intent = service
        .validate_stage_lease(&lease, "semantic-s1-worker", 5)
        .unwrap();
    let transition = SemanticStageTransition {
        intent: intent.clone(),
        receipt: SemanticStageReceipt {
            intent_digest: intent.intent_digest,
            output_digest: source_r1.source_content_digest(),
            cursor: "source:old-complete".to_string(),
            completed_at: "2026-09-08T00:00:41Z".to_string(),
            outcome: SemanticStageOutcome::Completed,
        },
        generation_checkpoint: None,
    };
    let first_receipt = service
        .complete_sql_source_stage(
            &lease,
            &transition,
            &source_r1,
            "2026-09-08T00:00:40Z",
            None,
            6,
        )
        .unwrap();

    // The durable stage mutation retains the cursor and authorization
    // completion time. Reopen the service and replay with only the
    // durable lease envelope plus an intent decoded from its durable
    // payload; the caller does not retain or reconstruct the transition.
    let durable_lease = lease.clone();
    let durable_intent =
        SemanticStageIntent::from_canonical_cbor(&durable_lease.record.intent.payload).unwrap();
    drop(transition);
    drop(intent);
    drop(service);
    let service = open_tenant_a_service(&dir);
    let (replayed_transition, replayed_receipt) = service
        .replay_completed_sql_source_stage(&durable_lease, &durable_intent, 7)
        .unwrap()
        .expect("the reopened service must recover the retained S1 result");
    assert_eq!(replayed_transition.intent, durable_intent);
    assert_eq!(replayed_transition.receipt.cursor, "source:old-complete");
    assert_eq!(
        replayed_transition.receipt.completed_at,
        "2026-09-08T00:00:41Z"
    );
    assert!(replayed_receipt.replayed);
    assert_eq!(
        replayed_receipt.mutation_digest,
        first_receipt.mutation_digest
    );

    let prior_manifest = service
        .sql_source_manifest(binding.generation, &source_r1.source_entity_id())
        .unwrap()
        .unwrap();
    assert_eq!(
        prior_manifest.source_entity_id,
        source_r1.source_entity_id()
    );
    assert_eq!(prior_manifest.source_identity, source_r1.source_identity);
    assert_eq!(prior_manifest.source_revision, revision_r1);
    assert_eq!(
        prior_manifest.source_content_digest,
        source_r1.source_content_digest()
    );

    let read_port = super::test_fixture::ScriptedSourcePort::empty(
        revision_r2.clone(),
        Arc::new(Mutex::new(Vec::new())),
    );
    let admission = service
        .admit_sql_source_dirty_reconcile(&record_r2, &read_port, 43)
        .unwrap();
    assert!(admission.complete);
    assert!(admission.wakeup_consumed);
    assert_eq!(admission.source_revision, revision_r2);
    assert_eq!(admission.receipts.len(), 1);
    assert!(service
        .store
        .source_entity_seen_at_revision(
            binding.generation,
            &source_r1.source_entity_id(),
            &revision_r2,
        )
        .unwrap());
    assert!(service
        .store
        .read_source_reconciliation_checkpoint(binding.generation)
        .unwrap()
        .is_none());
    let _ = std::fs::remove_dir_all(dir);
}

mod validation;
