use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use eg_storage::{OwnerLayout, PhysicalStoreIdentity, ScopeGrantVerifier};
use eg_transaction::OutboxClaimBudget;
use eg_types::contract::Nonce;
use eg_types::mutation_batch::{
    CommittedVersion, DurabilityDomain, MutationOutboxIntent, MutationOutboxRecord,
    MutationScopeIdentity, MUTATION_BATCH_VERSION,
};
use eg_types::semantic_index::{
    SemanticBinding, SemanticBindingState, SemanticDigest, SemanticIndexError,
    SemanticSourceDirtyIntent, SemanticSourceSelector, SemanticSqlSourceIdentity,
    SemanticStageArtifact, SemanticStageOutcome, SemanticStageReceipt, SemanticStageTransition,
    SEMANTIC_SOURCE_DIRTY_TOPIC,
};
use sha2::{Digest, Sha256};

use super::{
    sql_source_revision_for_epoch, sql_source_stage_artifact, SemanticIndexService,
    SemanticSqlSourceReadPage, SemanticSqlSourceReadPort, SemanticSqlSourceRecord,
    SemanticSqlSourceValue,
};
const TENANT: &str = "tenant-reconciliation-regression";
const BINDING: &str = "binding:documents-body-reconciliation";
const FIRST_CURSOR: &[u8] = b"after-first-source";
const PRINCIPAL: &str =
    "principal:sha256:3aa1ceaf4fe702f5451e6e93096e8ee6c4c1dbb71bbce468105043b435a5b82a";
const PROOF: &[u8] = b"semantic-reconciliation-regression-proof";

struct ReconciliationVerifier;

impl ScopeGrantVerifier for ReconciliationVerifier {
    fn verify(
        &self,
        _physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        identity: &eg_types::MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        if layout != OwnerLayout::SemanticIndex
            || identity.tenant().as_str() != TENANT
            || principal != PRINCIPAL
            || proof != PROOF
        {
            return Err("semantic reconciliation test scope was refused".to_string());
        }
        Ok(())
    }
}

fn digest(byte: u8) -> SemanticDigest {
    SemanticDigest::from_bytes([byte; 32])
}

fn source_revision(authority: u8, epoch: u64) -> String {
    sql_source_revision_for_epoch(digest(authority), epoch).unwrap()
}

fn binding(revision: &str) -> SemanticBinding {
    super::semantic_binding_test_fixture(
        TENANT,
        BINDING,
        "documents",
        revision,
        "sha256:documents-schema",
        "sha256:documents-body",
        "2026-09-08T00:00:00Z",
    )
}

fn source(binding: &SemanticBinding, identity: u8, bytes: &[u8]) -> SemanticSqlSourceRecord {
    let SemanticSourceSelector::SqlColumnRef(selector) = &binding.source_selector else {
        panic!("fixture source selector is SQL");
    };
    SemanticSqlSourceRecord {
        source_identity: SemanticSqlSourceIdentity::create(
            selector,
            binding.tenant_id.clone(),
            digest(identity),
        ),
        source_revision: binding.source_revision.clone(),
        value: SemanticSqlSourceValue::Present {
            source_bytes: bytes.to_vec(),
        },
        source_schema_revision: 9,
        source_schema_digest: binding.source_schema_digest.clone(),
        source_field_set_digest: binding.source_field_set_digest.clone(),
        source_acl_revision: binding.policy_identity.components.source_acl_revision,
        source_acl_digest: binding.policy_identity.components.source_acl_digest.clone(),
        authorization_receipt_digest: digest(40),
    }
}

fn dirty_record(batch_id: &str, input: u8, source: u64, target: u64) -> MutationOutboxRecord {
    let identity = MutationScopeIdentity::fixed_native(
        TENANT,
        DurabilityDomain::SqlCatalog,
        "catalog",
        "authority-a",
    )
    .unwrap();
    let wakeup = SemanticSourceDirtyIntent::new(
        SemanticDigest::from_bytes(*identity.binding_digest().as_bytes()),
        digest(input),
    )
    .to_canonical_cbor()
    .unwrap();
    MutationOutboxRecord {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.to_string(),
        ordinal: 0,
        identity,
        committed_version: CommittedVersion::Native { source, target },
        commit_sequence: None,
        intent: MutationOutboxIntent {
            topic: SEMANTIC_SOURCE_DIRTY_TOPIC.to_string(),
            key: batch_id.to_string(),
            payload: wakeup,
            headers: BTreeMap::new(),
        },
        created_at_ms: source,
    }
}

fn temp_dir(label: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    std::env::temp_dir().join(format!(
        "eg-semantic-{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}

fn open_service(dir: &Path) -> SemanticIndexService {
    SemanticIndexService::open(
        dir,
        Arc::new(ReconciliationVerifier),
        PRINCIPAL,
        PROOF,
        TENANT,
        BINDING,
    )
    .unwrap()
}

fn complete_source_stages(
    service: &SemanticIndexService,
    binding: &SemanticBinding,
    sources: &[SemanticSqlSourceRecord],
    now_ms: u64,
) {
    service
        .transition_binding_operation(
            binding.generation,
            SemanticBindingState::Building,
            now_ms,
            "semantic:index-maintainer",
            "open-source-stage-consumer",
            Nonce::from_bytes([now_ms as u8; 32]),
        )
        .unwrap();
    service
        .subscribe_stage_consumer("semantic-source-worker")
        .unwrap();
    let mut budget = OutboxClaimBudget::new(sources.len(), 5_000, now_ms + 1).unwrap();
    let outcome = service
        .claim_stage_leases("semantic-source-worker", &mut budget)
        .unwrap();
    assert_eq!(outcome.claims.len(), sources.len());
    for lease in outcome.claims {
        let intent = service
            .validate_stage_lease(&lease, "semantic-source-worker", now_ms + 2)
            .unwrap();
        let source_entity_id = intent
            .scope
            .source_entity_id()
            .expect("fixture S1 lease is entity scoped");
        let source = sources
            .iter()
            .find(|source| source.source_entity_id() == source_entity_id)
            .expect("fixture retains the raw source for every S1 lease");
        let output_digest = match &source.value {
            SemanticSqlSourceValue::Present { source_bytes } => {
                SemanticDigest::from_bytes(Sha256::digest(source_bytes).into())
            }
            SemanticSqlSourceValue::Tombstone {
                deletion_proof_digest,
            } => *deletion_proof_digest,
        };
        let transition = SemanticStageTransition {
            intent: intent.clone(),
            receipt: SemanticStageReceipt {
                intent_digest: intent.intent_digest,
                output_digest,
                cursor: "sql-source:complete".to_string(),
                completed_at: format!("unix-ms:{}", now_ms + 3),
                outcome: SemanticStageOutcome::Completed,
            },
            generation_checkpoint: None,
        };
        service
            .complete_sql_source_stage(
                &lease,
                &transition,
                source,
                &format!("unix-ms:{now_ms}"),
                None,
                now_ms + 3,
            )
            .unwrap();
    }
}

struct InterruptedPagedPort {
    revision: String,
    first: SemanticSqlSourceRecord,
    second: SemanticSqlSourceRecord,
    fail_second_read: bool,
    calls: Arc<Mutex<Vec<Option<Vec<u8>>>>>,
}

impl SemanticSqlSourceReadPort for InterruptedPagedPort {
    fn read_current_sql_source_page(
        &self,
        _binding: &SemanticBinding,
        _wakeup: &SemanticSourceDirtyIntent,
        _record: &MutationOutboxRecord,
        cursor: Option<&[u8]>,
    ) -> Result<SemanticSqlSourceReadPage, SemanticIndexError> {
        self.calls.lock().unwrap().push(cursor.map(<[u8]>::to_vec));
        match cursor {
            None => Ok(SemanticSqlSourceReadPage {
                source_revision: self.revision.clone(),
                complete_snapshot_receipt_digest: None,
                sources: vec![self.first.clone()],
                next_cursor: Some(FIRST_CURSOR.to_vec()),
                complete: false,
            }),
            Some(FIRST_CURSOR) if self.fail_second_read => {
                Err(SemanticIndexError::SourceManifestMismatch)
            }
            Some(FIRST_CURSOR) => Ok(SemanticSqlSourceReadPage {
                source_revision: self.revision.clone(),
                complete_snapshot_receipt_digest: Some(digest(77)),
                sources: vec![self.second.clone()],
                next_cursor: None,
                complete: true,
            }),
            Some(_) => Err(SemanticIndexError::SourceManifestMismatch),
        }
    }
}

struct CompletePage {
    revision: String,
    complete_snapshot_receipt: SemanticDigest,
    sources: Vec<SemanticSqlSourceRecord>,
}

struct InterruptedEmptySnapshot {
    revision: String,
    fail_final_read: bool,
    calls: Arc<Mutex<Vec<Option<Vec<u8>>>>>,
}

impl SemanticSqlSourceReadPort for InterruptedEmptySnapshot {
    fn read_current_sql_source_page(
        &self,
        _binding: &SemanticBinding,
        _wakeup: &SemanticSourceDirtyIntent,
        _record: &MutationOutboxRecord,
        cursor: Option<&[u8]>,
    ) -> Result<SemanticSqlSourceReadPage, SemanticIndexError> {
        self.calls.lock().unwrap().push(cursor.map(<[u8]>::to_vec));
        match cursor {
            None => Ok(SemanticSqlSourceReadPage {
                source_revision: self.revision.clone(),
                complete_snapshot_receipt_digest: None,
                sources: Vec::new(),
                next_cursor: Some(FIRST_CURSOR.to_vec()),
                complete: false,
            }),
            Some(FIRST_CURSOR) if self.fail_final_read => {
                Err(SemanticIndexError::SourceManifestMismatch)
            }
            Some(FIRST_CURSOR) => Ok(SemanticSqlSourceReadPage {
                source_revision: self.revision.clone(),
                complete_snapshot_receipt_digest: Some(digest(89)),
                sources: Vec::new(),
                next_cursor: None,
                complete: true,
            }),
            Some(_) => Err(SemanticIndexError::SourceManifestMismatch),
        }
    }
}

impl SemanticSqlSourceReadPort for CompletePage {
    fn read_current_sql_source_page(
        &self,
        _binding: &SemanticBinding,
        _wakeup: &SemanticSourceDirtyIntent,
        _record: &MutationOutboxRecord,
        _cursor: Option<&[u8]>,
    ) -> Result<SemanticSqlSourceReadPage, SemanticIndexError> {
        Ok(SemanticSqlSourceReadPage {
            source_revision: self.revision.clone(),
            complete_snapshot_receipt_digest: Some(self.complete_snapshot_receipt),
            sources: self.sources.clone(),
            next_cursor: None,
            complete: true,
        })
    }
}

#[test]
fn partial_checkpoint_resumes_after_reopen_and_replays_exact_s1() {
    let dir = temp_dir("checkpoint-reopen");
    let revision = source_revision(170, 1);
    let binding = binding(&revision);
    let first = source(&binding, 11, b"first document");
    let second = source(&binding, 12, b"second document");
    let first_entity = first.source_entity_id();
    let second_entity = second.source_entity_id();
    let record = dirty_record("source-dirty-r1", 61, 1, 2);
    let first_calls = Arc::new(Mutex::new(Vec::new()));

    let service = open_service(&dir);
    service.admit_binding(&binding, 1).unwrap();
    let interrupted = InterruptedPagedPort {
        revision: revision.clone(),
        first: first.clone(),
        second: second.clone(),
        fail_second_read: true,
        calls: Arc::clone(&first_calls),
    };
    assert!(service
        .admit_sql_source_dirty_reconcile(&record, &interrupted, 2)
        .is_err());
    assert_eq!(
        first_calls.lock().unwrap().as_slice(),
        &[None, Some(FIRST_CURSOR.to_vec())]
    );
    assert!(service
        .store
        .source_entity_seen_at_revision(1, &first_entity, &revision)
        .unwrap());
    assert!(!service
        .store
        .source_entity_exists(1, &second_entity)
        .unwrap());
    drop(service);

    let service = open_service(&dir);
    let foreign = CompletePage {
        revision: source_revision(171, 99),
        complete_snapshot_receipt: digest(88),
        sources: Vec::new(),
    };
    assert!(service
        .admit_sql_source_dirty_reconcile(&record, &foreign, 3)
        .is_err());
    assert!(service
        .store
        .read_source_reconciliation_checkpoint(1)
        .unwrap()
        .is_some());

    let resumed_calls = Arc::new(Mutex::new(Vec::new()));
    let resumed = InterruptedPagedPort {
        revision: revision.clone(),
        first: first.clone(),
        second: second.clone(),
        fail_second_read: false,
        calls: Arc::clone(&resumed_calls),
    };
    let completed = service
        .admit_sql_source_dirty_reconcile(&record, &resumed, 4)
        .unwrap();
    assert!(completed.complete);
    assert!(completed.wakeup_consumed);
    assert_eq!(completed.rows_seen, 2);
    assert_eq!(completed.page_count, 2);
    assert_eq!(
        resumed_calls.lock().unwrap().as_slice(),
        &[Some(FIRST_CURSOR.to_vec())]
    );
    assert!(service
        .store
        .read_source_reconciliation_checkpoint(1)
        .unwrap()
        .is_none());
    assert!(service
        .store
        .source_entity_seen_at_revision(1, &first_entity, &revision)
        .unwrap());
    assert!(service
        .store
        .source_entity_seen_at_revision(1, &second_entity, &revision)
        .unwrap());

    let exact = CompletePage {
        revision: revision.clone(),
        complete_snapshot_receipt: digest(77),
        sources: vec![first, second],
    };
    let replay = service
        .admit_sql_source_dirty_reconcile(&record, &exact, 5)
        .unwrap();
    assert!(replay.complete);
    assert!(replay.wakeup_consumed);
    assert_eq!(replay.receipts.len(), 2);
    assert!(replay.receipts.iter().all(|receipt| receipt.replayed));
    let (entities, cursor) = service
        .store
        .list_source_entities_page(1, None, 256)
        .unwrap();
    assert_eq!(entities.len(), 2);
    assert!(cursor.is_none());

    drop(service);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn missing_sources_become_tombstones_only_after_complete_snapshot_proof() {
    let dir = temp_dir("complete-proof-tombstones");
    let first_revision = source_revision(170, 1);
    let binding = binding(&first_revision);
    let first = source(&binding, 31, b"first disappearing document");
    let second = source(&binding, 32, b"second disappearing document");
    let first_entity = first.source_entity_id();
    let second_entity = second.source_entity_id();
    let service = open_service(&dir);
    service.admit_binding(&binding, 1).unwrap();
    service
        .admit_sql_source_dirty_reconcile(
            &dirty_record("source-dirty-before-delete", 81, 1, 2),
            &CompletePage {
                revision: first_revision,
                complete_snapshot_receipt: digest(87),
                sources: vec![first.clone(), second.clone()],
            },
            2,
        )
        .unwrap();
    complete_source_stages(&service, &binding, &[first.clone(), second.clone()], 3);

    let deletion_revision = source_revision(170, 2);
    let deletion_wakeup = dirty_record("source-dirty-after-delete", 82, 2, 3);
    let interrupted_calls = Arc::new(Mutex::new(Vec::new()));
    let interrupted = InterruptedEmptySnapshot {
        revision: deletion_revision.clone(),
        fail_final_read: true,
        calls: Arc::clone(&interrupted_calls),
    };
    assert!(service
        .admit_sql_source_dirty_reconcile(&deletion_wakeup, &interrupted, 7)
        .is_err());
    assert_eq!(
        interrupted_calls.lock().unwrap().as_slice(),
        &[None, Some(FIRST_CURSOR.to_vec())]
    );
    for entity in [&first_entity, &second_entity] {
        assert!(!service
            .store
            .source_entity_seen_at_revision(1, entity, &deletion_revision)
            .unwrap());
    }
    drop(service);

    let service = open_service(&dir);
    let final_calls = Arc::new(Mutex::new(Vec::new()));
    let completed = service
        .admit_sql_source_dirty_reconcile(
            &deletion_wakeup,
            &InterruptedEmptySnapshot {
                revision: deletion_revision.clone(),
                fail_final_read: false,
                calls: Arc::clone(&final_calls),
            },
            8,
        )
        .unwrap();
    assert!(completed.complete);
    assert!(completed.wakeup_consumed);
    assert_eq!(completed.receipts.len(), 2);
    assert_eq!(
        final_calls.lock().unwrap().as_slice(),
        &[Some(FIRST_CURSOR.to_vec())]
    );
    for entity in [&first_entity, &second_entity] {
        assert!(service
            .store
            .source_entity_seen_at_revision(1, entity, &deletion_revision)
            .unwrap());
    }
    assert!(service
        .store
        .read_source_reconciliation_checkpoint(1)
        .unwrap()
        .is_none());

    drop(service);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn leased_s1_completion_persists_the_explicit_authorization_time() {
    const AUTHORIZED_AT: &str = "2026-09-08T18:31:07.125Z";
    let dir = temp_dir("leased-s1-completion");
    let revision = source_revision(170, 2);
    let binding = binding(&revision);
    let source = source(&binding, 21, b"truthfully authorized document");
    let record = dirty_record("source-dirty-s1-completion", 71, 2, 3);
    let service = open_service(&dir);
    service.admit_binding(&binding, 1).unwrap();
    let port = CompletePage {
        revision,
        complete_snapshot_receipt: digest(79),
        sources: vec![source.clone()],
    };
    service
        .admit_sql_source_dirty_reconcile(&record, &port, 2)
        .unwrap();
    service
        .transition_binding_operation(
            1,
            SemanticBindingState::Building,
            3,
            "semantic:index-maintainer",
            "open-stage-consumer",
            Nonce::from_bytes([91; 32]),
        )
        .unwrap();
    service
        .subscribe_stage_consumer("semantic-s1-worker")
        .unwrap();
    let mut budget = OutboxClaimBudget::new(1, 5_000, 4).unwrap();
    let outcome = service
        .claim_stage_leases("semantic-s1-worker", &mut budget)
        .unwrap();
    assert_eq!(outcome.claims.len(), 1);
    let lease = &outcome.claims[0];
    let intent = service
        .validate_stage_lease(lease, "semantic-s1-worker", 5)
        .unwrap();
    let output_digest = match &source.value {
        SemanticSqlSourceValue::Present { source_bytes } => {
            SemanticDigest::from_bytes(Sha256::digest(source_bytes).into())
        }
        SemanticSqlSourceValue::Tombstone { .. } => panic!("fixture source is present"),
    };
    let transition = SemanticStageTransition {
        intent: intent.clone(),
        receipt: SemanticStageReceipt {
            intent_digest: intent.intent_digest,
            output_digest,
            cursor: "sql-source:complete".to_string(),
            completed_at: "2026-09-08T18:31:08Z".to_string(),
            outcome: SemanticStageOutcome::Completed,
        },
        generation_checkpoint: None,
    };
    let artifact = sql_source_stage_artifact(&binding, &transition, &source, AUTHORIZED_AT)
        .expect("the typed source claim builds a complete S1 artifact");
    let SemanticStageArtifact::SqlSourceManifest { authorization, .. } = artifact else {
        panic!("S1 completion must produce a SQL source manifest");
    };
    assert_eq!(authorization.authorized_at, AUTHORIZED_AT);

    let receipt = service
        .complete_sql_source_stage(lease, &transition, &source, AUTHORIZED_AT, None, 6)
        .unwrap();
    assert!(!receipt.replayed);
    let retained = service
        .store
        .recorded_sql_source_artifact(intent.intent_digest)
        .unwrap()
        .expect("the committed S1 retains its canonical artifact");
    let SemanticStageArtifact::SqlSourceManifest {
        manifest,
        authorization,
    } = &retained
    else {
        panic!("the retained S1 artifact is a SQL source manifest");
    };
    assert_eq!(authorization.authorized_at, AUTHORIZED_AT);
    assert_eq!(
        manifest.authorization_receipt_digest,
        authorization.authorization_receipt_digest
    );
    assert_eq!(
        manifest.completed_receipt_digest,
        transition.receipt.receipt_digest()
    );
    drop(service);

    let service = open_service(&dir);
    let (retained_transition, replay) = service
        .replay_completed_sql_source_stage(lease, &intent, 7)
        .unwrap()
        .expect("the committed S1 resolves before another source decision");
    assert!(replay.replayed);
    assert_eq!(replay.batch_id, receipt.batch_id);
    assert_eq!(retained_transition, transition);
    assert_eq!(
        service
            .store
            .recorded_sql_source_artifact(intent.intent_digest)
            .unwrap(),
        Some(retained),
        "replay retains exactly one canonical authorization time and artifact"
    );
    let status = service.stage_status("semantic-s1-worker", 8).unwrap();
    assert_eq!(status.inflight, 0);
    assert_eq!(status.pending, 0);
    assert_eq!(status.delivered, 1);

    drop(service);
    let _ = std::fs::remove_dir_all(dir);
}
