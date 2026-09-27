use std::path::Path;

use super::*;
use crate::protocol::Method;
use crate::server::auth::VerifiedRequestContext;
use crate::server::mutation_batch::{compile_opaque_method, CompileBatch};
use crate::server::sql_catalog_acl::{
    create_owned_table, grant, revoke, with_source_authority_write,
};
use crate::server::sql_tables::{tenant_table_store, test_persist_dir};
use eg_query::{Column, ColumnType, TableSchema, TableStore, TableTxn, TxnOp};
// `CmpOp` exists in BOTH eg_query and eg_types and they are distinct types.
// `TxnOp::Update`'s selector is an `eg_types::RowPredicate`, so this is the
// one that belongs here; importing eg_query's next to it is the mistake that
// kept this module's tests from compiling.
use eg_storage::{OwnerLayout, PhysicalStoreIdentity, ScopeGrantVerifier};
use eg_types::contract::Nonce;
use eg_types::mutation_batch::{DurabilityDomain, MutationSurface};
use eg_types::semantic_index::{
    SemanticAnnIndexMethod, SemanticAnnIndexSpec, SemanticBindingDraft, SemanticDeadLetter,
    SemanticDeadLetterDraft, SemanticLexicalIndexSpec, SemanticModelIdentity,
    SemanticPolicyComponents, SemanticSourceSelector, SemanticStage, SemanticStageArtifact,
    SemanticStageOutcome, SemanticStagePredecessor, SemanticStageReceipt, SemanticVectorMetric,
    SqlColumnRef, SEMANTIC_SOURCE_DIRTY_TOPIC, SEMANTIC_SQL_CATALOG_ID, SEMANTIC_SQL_SCHEMA_ID,
};
use eg_types::CmpOp;
use eg_types::RowPredicate;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

const CURSOR_SECRET: [u8; 32] = *b"semantic-cursor-adapter-test-001";
const SEMANTIC_PROOF: &[u8] = b"semantic-server-adapter-scope-proof";

struct ExactSemanticScopeVerifier {
    tenant: String,
}

impl ScopeGrantVerifier for ExactSemanticScopeVerifier {
    fn verify(
        &self,
        _physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        identity: &eg_types::MutationScopeIdentity,
        _principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        if layout != OwnerLayout::SemanticIndex
            || identity.tenant().as_str() != self.tenant
            || proof != SEMANTIC_PROOF
        {
            return Err("semantic server test scope was refused".to_string());
        }
        Ok(())
    }
}

/// `verified_for_test_in_tenant` grants `kg:read` ONLY, so a carrier minted
/// from it fails `authorize_binding_worker`'s `can_write()` on the first
/// durable stage write -- the second failure this module's own tests hit
/// the moment it was first compiled. A semantic stage worker writes by
/// definition, so the fixture asks for the scopes the thing under test
/// actually needs.
pub(super) fn authority(agent_id: &str, tenant: &str) -> CarrierAuthority {
    CarrierAuthority::from_verified(&VerifiedRequestContext::verified_for_test_with_scopes(
        agent_id,
        tenant,
        &["kg:read", "kg:write"],
    ))
    .unwrap()
}

pub(super) fn selector() -> SemanticSourceSelector {
    SemanticSourceSelector::SqlColumnRef(SqlColumnRef {
        catalog_id: SEMANTIC_SQL_CATALOG_ID.to_string(),
        schema_id: SEMANTIC_SQL_SCHEMA_ID.to_string(),
        table_id: "documents".to_string(),
        column_id: "body".to_string(),
    })
}

pub(super) fn binding(
    authority: &CarrierAuthority,
    snapshot: &SemanticTextSnapshot,
) -> SemanticBinding {
    SemanticBinding::create(binding_draft(authority, snapshot)).unwrap()
}

/// The DRAFT behind [`binding`].
///
/// Split out because the wire contract admits a binding from its draft --
/// `Method::SemanticIndex`'s `AdmitBinding` hands the engine a draft and the
/// engine calls `SemanticBinding::create` -- so a dispatch-driven test needs
/// the draft, while the direct-service tests around it need the built
/// binding. One fixture, both shapes.
pub(super) fn binding_draft(
    authority: &CarrierAuthority,
    snapshot: &SemanticTextSnapshot,
) -> SemanticBindingDraft {
    let source_revision = sql_source_revision_for_epoch(
        SemanticDigest::from_bytes(snapshot.source_authority_digest),
        snapshot.source_epoch,
    )
    .unwrap();
    SemanticBindingDraft {
        binding_id: "binding:documents-body".to_string(),
        tenant_id: authority.tenant_scope().to_string(),
        actor_scope: authority.actor_scope().to_string(),
        effective_actor_scope: authority.agent_id().to_string(),
        purpose_id: "retrieval".to_string(),
        policy: SemanticPolicyComponents {
            rbac_policy_revision: 1,
            rbac_policy_digest: "sha256:rbac".to_string(),
            row_policy_revision: 1,
            row_policy_digest: "sha256:row-policy".to_string(),
            source_acl_revision: snapshot.source_acl_revision,
            source_acl_digest: snapshot.source_acl_digest.clone(),
        },
        source_selector: selector(),
        source_schema_digest: snapshot.schema_digest.clone(),
        source_revision,
        source_field_set_digest: "sha256:documents-body".to_string(),
        dimension: 3,
        metric: SemanticVectorMetric::Cosine,
        model: SemanticModelIdentity {
            model_id: "embedding-model".to_string(),
            model_revision: "revision-1".to_string(),
            preprocess_digest: "sha256:preprocess".to_string(),
            model_digest: "sha256:model".to_string(),
        },
        generation: 1,
        maintenance_policy_id: "semantic-maintenance".to_string(),
        lexical_index: SemanticLexicalIndexSpec {
            analyzer_id: "standard".to_string(),
            analyzer_revision: "1".to_string(),
            analyzer_config_digest: "sha256:analyzer".to_string(),
        },
        ann_index: SemanticAnnIndexSpec {
            method: SemanticAnnIndexMethod::IvfPq,
            parameters_digest: "sha256:ann-parameters".to_string(),
        },
        created_at: "2026-09-08T00:00:00Z".to_string(),
    }
}

/// Everything a DISPATCH-driven test needs to exist before a semantic
/// request is legal: an owned SQL table, a SELECT grant to the worker, the
/// authoritative snapshot the binding is minted against, one committed row,
/// and the source-dirty outbox record that commit emitted.
///
/// The connector contract deliberately cannot carry any of this -- the row
/// and its ACL decision are re-read engine-side -- so a wire-level test has
/// to establish it the way production does, through the SQL owner.
pub(super) struct DispatchTableFixture {
    pub(super) persist_dir: std::path::PathBuf,
    pub(super) snapshot: SemanticTextSnapshot,
    pub(super) dirty: MutationOutboxRecord,
    pub(super) source_digest: SemanticDigest,
}

pub(super) fn dispatch_table_fixture(
    worker: &CarrierAuthority,
    tenant: &str,
) -> DispatchTableFixture {
    let persist_dir = test_persist_dir();
    // The RAW tenant, not `worker.tenant_scope()`. A carrier's tenant scope
    // is already the opaque derivation of the raw name, so feeding it back
    // through `authority()` derives a second, different scope and the table
    // owner would land in another tenant's catalog than the worker reads.
    let owner = authority("semantic-dispatch-owner", tenant);
    let schema = TableSchema::new(
        "documents",
        vec![
            Column::new("id", ColumnType::Text, false, true),
            Column::new("body", ColumnType::Text, false, false),
        ],
    );
    assert!(create_owned_table(&owner, &persist_dir, &schema, false).unwrap());
    grant(
        &persist_dir,
        &owner,
        "documents",
        worker.agent_id(),
        &[SqlPrivilege::Select],
        uuid::Uuid::from_u128(97),
    )
    .unwrap();
    let table =
        open_authorized_table(worker, &persist_dir, "documents", SqlPrivilege::Select).unwrap();
    let snapshot = table
        .semantic_text_snapshot(&selector(), &CURSOR_SECRET, None)
        .unwrap();
    let binding = binding(worker, &snapshot);
    let store = tenant_table_store(owner.tenant_scope(), &persist_dir).unwrap();
    let mut insert = TableTxn::new();
    insert.push(TxnOp::Insert {
        table: "documents".to_string(),
        col_order: vec!["id".to_string(), "body".to_string()],
        rows: vec![vec![
            Value::String("doc-dispatch".to_string()),
            Value::String("dispatch pipeline bytes".to_string()),
        ]],
    });
    let dirty = commit_sql_change(
        &persist_dir,
        &owner,
        &store,
        91,
        Method::Sql {
            query: "INSERT INTO documents (id, body) VALUES ('doc-dispatch', 'dispatch pipeline bytes')"
                .to_string(),
            params_msgpack: Vec::new(),
        },
        insert,
    );
    let read_port =
        AuthorizedSqlSourceReadPort::new(persist_dir.clone(), worker.clone(), CURSOR_SECRET);
    let page = read_dirty(&read_port, &binding, &dirty);
    let source_digest = match &page.sources[0].value {
        SemanticSqlSourceValue::Present { source_bytes } => {
            SemanticDigest::from_bytes(Sha256::digest(source_bytes).into())
        }
        SemanticSqlSourceValue::Tombstone { .. } => {
            panic!("the dispatch fixture commits one present row")
        }
    };
    DispatchTableFixture {
        persist_dir,
        snapshot,
        dirty,
        source_digest,
    }
}

pub(super) fn commit_sql_change(
    persist_dir: &Path,
    authority: &CarrierAuthority,
    store: &TableStore,
    request_id: u64,
    method: Method,
    mut txn: TableTxn,
) -> MutationOutboxRecord {
    let resource = "semantic-read-adapter-sql";
    let batch_id = format!("semantic-read-adapter-batch-{request_id}");
    let idempotency_key = format!("semantic-read-adapter-key-{request_id}");
    let expected_version = store
        .mutation_version(authority.tenant_scope(), resource)
        .unwrap();
    let batch = compile_opaque_method(
        CompileBatch {
            batch_id: &batch_id,
            request_id,
            attempt_nonce: Some(Nonce::from_bytes([request_id as u8; 32])),
            principal: Some(authority.actor_scope()),
            tenant: authority.tenant_scope(),
            graph: resource,
            placement_epoch: 0,
            idempotency_key: &idempotency_key,
            expected_graph_version: Some(expected_version),
            fencing_token: None,
            created_at_ms: request_id,
            default_surface: MutationSurface::Query,
            authoritative_state: None,
        },
        &method,
        MutationSurface::Query,
        DurabilityDomain::SqlCatalog,
        "sql_catalog_operation",
    )
    .unwrap();
    with_source_authority_write(persist_dir, authority, |source| {
        crate::server::wire::authorize_table_txn(source, store, &mut txn, false)
            .map_err(|error| error.message)?;
        store.commit_txn_batch(&txn, &batch, request_id).map(|_| ())
    })
    .unwrap();
    store
        .mutation_outbox(&batch.identity, &batch.batch_id)
        .unwrap()
        .into_iter()
        .find(|record| record.intent.topic == SEMANTIC_SOURCE_DIRTY_TOPIC)
        .expect("the actual SQL owner commit includes one source-dirty event")
}

pub(super) fn read_dirty(
    port: &AuthorizedSqlSourceReadPort,
    binding: &SemanticBinding,
    dirty: &MutationOutboxRecord,
) -> SemanticSqlSourceReadPage {
    let wakeup = SemanticSourceDirtyIntent::from_canonical_cbor(&dirty.intent.payload).unwrap();
    port.read_current_sql_source_page(binding, &wakeup, dirty, None)
        .unwrap()
}

#[test]
fn committed_sql_r1_to_r2_dirty_reads_current_authoritative_source() {
    let persist_dir = test_persist_dir();
    let authority = authority("semantic-reader", "tenant-semantic-reader");
    let schema = TableSchema::new(
        "documents",
        vec![
            Column::new("id", ColumnType::Text, false, true),
            Column::new("body", ColumnType::Text, false, false),
        ],
    );
    assert!(create_owned_table(&authority, &persist_dir, &schema, false).unwrap());
    let table =
        open_authorized_table(&authority, &persist_dir, "documents", SqlPrivilege::Select).unwrap();
    let initial = table
        .semantic_text_snapshot(&selector(), &CURSOR_SECRET, None)
        .unwrap();
    let binding = binding(&authority, &initial);
    let store = tenant_table_store(authority.tenant_scope(), &persist_dir).unwrap();
    let port =
        AuthorizedSqlSourceReadPort::new(persist_dir.clone(), authority.clone(), CURSOR_SECRET);

    let mut insert = TableTxn::new();
    insert.push(TxnOp::Insert {
        table: "documents".to_string(),
        col_order: vec!["id".to_string(), "body".to_string()],
        rows: vec![vec![
            Value::String("doc-1".to_string()),
            Value::String("r1".to_string()),
        ]],
    });
    let r1_dirty = commit_sql_change(
        &persist_dir,
        &authority,
        &store,
        41,
        Method::Sql {
            query: "INSERT INTO documents (id, body) VALUES ('doc-1', 'r1')".to_string(),
            params_msgpack: Vec::new(),
        },
        insert,
    );
    let r1 = read_dirty(&port, &binding, &r1_dirty);
    assert!(r1.complete);
    assert!(r1.complete_snapshot_receipt_digest.is_some());
    assert_eq!(r1.sources.len(), 1);
    assert_eq!(
        r1.sources[0].value,
        SemanticSqlSourceValue::Present {
            source_bytes: b"r1".to_vec()
        }
    );
    let source_identity = r1.sources[0].source_identity.clone();

    let mut set = Map::new();
    set.insert("body".to_string(), Value::String("r2".to_string()));
    let mut update = TableTxn::new();
    update.push(TxnOp::Update {
        table: "documents".to_string(),
        set,
        selector: RowPredicate::Cmp {
            col: "id".to_string(),
            op: CmpOp::Eq,
            value: Value::String("doc-1".to_string()),
        },
    });
    let r2_dirty = commit_sql_change(
        &persist_dir,
        &authority,
        &store,
        42,
        Method::Sql {
            query: "UPDATE documents SET body = 'r2' WHERE id = 'doc-1'".to_string(),
            params_msgpack: Vec::new(),
        },
        update,
    );
    let r2 = read_dirty(&port, &binding, &r2_dirty);
    assert_eq!(r2.sources.len(), 1);
    assert_eq!(r2.sources[0].source_identity, source_identity);
    assert_eq!(
        r2.sources[0].value,
        SemanticSqlSourceValue::Present {
            source_bytes: b"r2".to_vec()
        }
    );
    assert_ne!(r2.source_revision, r1.source_revision);
    assert_eq!(r2.sources[0].source_revision, r2.source_revision);

    let delayed_r1 = read_dirty(&port, &binding, &r1_dirty);
    assert_eq!(delayed_r1.source_revision, r2.source_revision);
    assert_eq!(delayed_r1.sources, r2.sources);
}

#[tokio::test]
async fn leased_sql_source_completion_replays_after_reopen_and_acl_change() {
    let persist_dir = test_persist_dir();
    let owner = authority("semantic-source-owner", "tenant-semantic-stage");
    let worker = authority("semantic-stage-worker", "tenant-semantic-stage");
    let schema = TableSchema::new(
        "documents",
        vec![
            Column::new("id", ColumnType::Text, false, true),
            Column::new("body", ColumnType::Text, false, false),
        ],
    );
    assert!(create_owned_table(&owner, &persist_dir, &schema, false).unwrap());
    grant(
        &persist_dir,
        &owner,
        "documents",
        worker.agent_id(),
        &[SqlPrivilege::Select],
        uuid::Uuid::from_u128(71),
    )
    .unwrap();
    let table =
        open_authorized_table(&worker, &persist_dir, "documents", SqlPrivilege::Select).unwrap();
    let initial = table
        .semantic_text_snapshot(&selector(), &CURSOR_SECRET, None)
        .unwrap();
    let binding = binding(&worker, &initial);
    let store = tenant_table_store(owner.tenant_scope(), &persist_dir).unwrap();
    let mut insert = TableTxn::new();
    insert.push(TxnOp::Insert {
        table: "documents".to_string(),
        col_order: vec!["id".to_string(), "body".to_string()],
        rows: vec![vec![
            Value::String("doc-stage".to_string()),
            Value::String("authorized stage bytes".to_string()),
        ]],
    });
    let dirty = commit_sql_change(
        &persist_dir,
        &owner,
        &store,
        51,
        Method::Sql {
            query:
                "INSERT INTO documents (id, body) VALUES ('doc-stage', 'authorized stage bytes')"
                    .to_string(),
            params_msgpack: Vec::new(),
        },
        insert,
    );
    let read_port =
        AuthorizedSqlSourceReadPort::new(persist_dir.clone(), worker.clone(), CURSOR_SECRET);
    let page = read_dirty(&read_port, &binding, &dirty);
    let source_entity_id = page.sources[0].source_entity_id();

    let semantic_dir = persist_dir.join("semantic-stage-service");
    let open_service = || {
        SemanticIndexService::open(
            &semantic_dir,
            Arc::new(ExactSemanticScopeVerifier {
                tenant: worker.tenant_scope().to_string(),
            }),
            // NOT `worker.agent_id()`: the mutation kernel refuses any
            // serving principal that is not `principal:sha256:<64 hex>`
            // ("mutation principal authority must be an opaque digest").
            // This fixture passed a plain agent id, which is the failure
            // this test hit the first time it was ever compiled.
            super::semantic_owner_principal(),
            SEMANTIC_PROOF,
            worker.tenant_scope(),
            &binding.binding_id,
        )
        .unwrap()
    };
    let service = Arc::new(open_service());
    service.admit_binding(&binding, 1).unwrap();
    service
        .admit_sql_source_dirty_reconcile(&dirty, &read_port, 2)
        .unwrap();
    service
        .transition_binding_operation(
            binding.generation,
            eg_types::semantic_index::SemanticBindingState::Building,
            3,
            worker.agent_id(),
            "semantic-stage-build",
            Nonce::from_bytes([73; 32]),
        )
        .unwrap();
    let adapter = SemanticIndexServerAdapter::new(Arc::clone(&service));
    adapter.subscribe_stage_consumer(&binding, &worker).unwrap();
    let mut budget = OutboxClaimBudget::new(1, 5_000, 4).unwrap();
    let outcome = adapter
        .claim_stage_leases(&binding, &worker, &mut budget)
        .unwrap();
    assert_eq!(outcome.claims.len(), 1);
    let lease = outcome.claims.into_iter().next().unwrap();
    let intent = service
        .validate_stage_lease(&lease, worker.agent_id(), 5)
        .unwrap();
    assert_eq!(
        intent.scope.source_entity_id(),
        Some(source_entity_id.as_str())
    );
    let claim = adapter
        .claim_sql_source(
            52,
            AuthorizedSqlSourceReadPort::new(persist_dir.clone(), worker.clone(), CURSOR_SECRET),
            binding.clone(),
            intent.clone(),
            None,
        )
        .await
        .unwrap();
    let source_digest = match &claim.source.value {
        SemanticSqlSourceValue::Present { source_bytes } => {
            SemanticDigest::from_bytes(Sha256::digest(source_bytes).into())
        }
        SemanticSqlSourceValue::Tombstone { .. } => panic!("fixture source is present"),
    };
    assert!(claim.decision_at_ms > 0);
    let transition = SemanticStageTransition {
        intent: intent.clone(),
        receipt: SemanticStageReceipt {
            intent_digest: intent.intent_digest,
            output_digest: source_digest,
            cursor: "sql-source:complete".to_string(),
            completed_at: format!("unix-ms:{}", claim.decision_at_ms.saturating_add(1)),
            outcome: SemanticStageOutcome::Completed,
        },
        generation_checkpoint: None,
    };
    let committed = adapter
        .complete_sql_source_stage(
            53,
            AuthorizedSqlSourceReadPort::new(persist_dir.clone(), worker.clone(), CURSOR_SECRET),
            binding.clone(),
            SqlSourceStageCompletion {
                lease: lease.clone(),
                transition: transition.clone(),
                claim: claim.clone(),
                successor: None,
            },
            6,
        )
        .await
        .unwrap();
    assert!(!committed.replayed);
    let present_manifest = service
        .sql_source_manifest(binding.generation, &source_entity_id)
        .unwrap()
        .expect("S1 stores the retained SQL source identity");
    assert_eq!(
        present_manifest.source_identity,
        claim.source.source_identity
    );
    assert_eq!(
        present_manifest.completed_receipt_digest,
        transition.receipt.receipt_digest()
    );
    assert_ne!(
        present_manifest.authorization_receipt_digest, claim.source.authorization_receipt_digest,
        "the durable full receipt is distinct from the raw ACL decision digest"
    );

    // S1 completion is NOT the end of this entity's pipeline, and the
    // fixture may not behave as if it were. `successor: None` above does
    // not mean "no successor": `validate_successor_intent`'s `SourceCommit`
    // arm DERIVES the S2 GraphProjection intent from the committed receipt
    // and enqueues it on the stage-intent topic in the same mutation.
    //
    // The stage outbox is one commit-ordered stream per
    // `(scope, consumer)`, and `ack_mutation_outbox` refuses a watermark
    // that would skip an undelivered predecessor -- `OUTBOX_ORDER_GAP`, in
    // `outbox::cursor::require_no_earlier_gap`. So this worker cannot
    // acknowledge ANY later event on that stream, the deletion's own S1
    // included, until the derived S2 is resolved. Claiming two leases and
    // completing only the later one, as this fixture first did, is exactly
    // the corrupt watermark that rule exists to refuse.
    //
    // The S2 must also be resolved BEFORE the deletion is admitted: the
    // tombstone admission REPLACES the single
    // `(generation, source_entity_id)` source-progress row with the
    // deletion revision, and `validate_stage_predecessor_in` then refuses
    // every terminal outcome offered for the superseded revision --
    // `RejectedDeadLetter` included.
    //
    // This fixture serves only the S1 SQL-source tier, so it retires the
    // derived S2 through the pipeline's own dead-letter terminal, the one
    // terminal outcome that does not itself publish a further successor.
    let mut budget = OutboxClaimBudget::new(1, 5_000, 7).unwrap();
    let outcome = adapter
        .claim_stage_leases(&binding, &worker, &mut budget)
        .unwrap();
    assert_eq!(outcome.claims.len(), 1);
    let projection_lease = outcome.claims.into_iter().next().unwrap();
    let projection_intent = service
        .validate_stage_lease(&projection_lease, worker.agent_id(), 7)
        .unwrap();
    // The proof that completing S1 ADVANCED the pipeline rather than merely
    // acknowledging a row: the next claimable event on the stream is the S2
    // derived from the exact S1 receipt just committed.
    assert_eq!(projection_intent.stage, SemanticStage::GraphProjection);
    assert_eq!(
        projection_intent.scope.source_entity_id(),
        Some(source_entity_id.as_str())
    );
    assert_eq!(projection_intent.source_revision, intent.source_revision);
    assert_eq!(
        projection_intent.predecessor,
        SemanticStagePredecessor::EntityReceipt {
            stage: SemanticStage::SourceCommit,
            receipt_digest: transition.receipt.receipt_digest(),
        }
    );
    let projection_failed_at = "unix-ms:7".to_string();
    let projection_dead_letter = SemanticDeadLetter::create(SemanticDeadLetterDraft {
        intent: projection_intent.clone(),
        attempt: projection_lease.attempt,
        error_code: "stage_tier_not_served".to_string(),
        reason: "this fixture serves only the S1 SQL source tier".to_string(),
        failed_at: projection_failed_at.clone(),
    })
    .unwrap();
    service
        .complete_stage(
            &projection_lease,
            &SemanticStageTransition {
                intent: projection_intent.clone(),
                receipt: SemanticStageReceipt {
                    intent_digest: projection_intent.intent_digest,
                    output_digest: projection_dead_letter.failure_digest,
                    cursor: "graph-projection:not-served".to_string(),
                    completed_at: projection_failed_at,
                    outcome: SemanticStageOutcome::RejectedDeadLetter,
                },
                generation_checkpoint: None,
            },
            &SemanticStageArtifact::DeadLetter {
                dead_letter: Box::new(projection_dead_letter),
            },
            None,
            7,
        )
        .unwrap();

    let mut delete = TableTxn::new();
    delete.push(TxnOp::Delete {
        table: "documents".to_string(),
        selector: RowPredicate::Cmp {
            col: "id".to_string(),
            op: CmpOp::Eq,
            value: Value::String("doc-stage".to_string()),
        },
    });
    let deletion_dirty = commit_sql_change(
        &persist_dir,
        &owner,
        &store,
        55,
        Method::Sql {
            query: "DELETE FROM documents WHERE id = 'doc-stage'".to_string(),
            params_msgpack: Vec::new(),
        },
        delete,
    );
    let deletion_page = read_dirty(&read_port, &binding, &deletion_dirty);
    assert!(deletion_page.complete);
    assert!(deletion_page.complete_snapshot_receipt_digest.is_some());
    assert!(deletion_page.sources.is_empty());
    let deletion_revision = deletion_page.source_revision.clone();
    let deletion_admission = service
        .admit_sql_source_dirty_reconcile(&deletion_dirty, &read_port, 8)
        .unwrap();
    assert!(deletion_admission.complete);
    assert_eq!(deletion_admission.receipts.len(), 1);

    // Eight, not one. `OutboxClaimBudget::allowance` bounds every claim by
    // `consecutive_cap()` = `(limit / 4).max(1)` even for a lone,
    // uncontended tenant, so a budget of one would cap this page at one row
    // whatever the queue holds -- and the point of this claim is that the
    // queue holds EXACTLY one row. With the derived S2 resolved above, the
    // deletion's S1 is now the head of the consumer's resolved prefix.
    let mut budget = OutboxClaimBudget::new(8, 5_000, 9).unwrap();
    let outcome = adapter
        .claim_stage_leases(&binding, &worker, &mut budget)
        .unwrap();
    assert_eq!(outcome.claims.len(), 1);
    let tombstone_lease = outcome.claims.into_iter().next().unwrap();
    let tombstone_intent = service
        .validate_stage_lease(&tombstone_lease, worker.agent_id(), 9)
        .unwrap();
    assert_eq!(tombstone_intent.stage, SemanticStage::SourceCommit);
    assert_eq!(tombstone_intent.source_revision, deletion_revision);
    let tombstone_claim = adapter
        .claim_sql_tombstone(
            56,
            AuthorizedSqlSourceReadPort::new(persist_dir.clone(), worker.clone(), CURSOR_SECRET),
            binding.clone(),
            tombstone_intent.clone(),
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        &tombstone_claim.source.value,
        SemanticSqlSourceValue::Tombstone { .. }
    ));
    let tombstone_transition = SemanticStageTransition {
        intent: tombstone_intent.clone(),
        receipt: SemanticStageReceipt {
            intent_digest: tombstone_intent.intent_digest,
            output_digest: tombstone_intent.input_digest,
            cursor: "sql-source:tombstone-complete".to_string(),
            completed_at: format!(
                "unix-ms:{}",
                tombstone_claim.decision_at_ms.saturating_add(1)
            ),
            outcome: SemanticStageOutcome::Completed,
        },
        generation_checkpoint: None,
    };
    let tombstone_committed = adapter
        .complete_sql_source_stage(
            57,
            AuthorizedSqlSourceReadPort::new(persist_dir.clone(), worker.clone(), CURSOR_SECRET),
            binding.clone(),
            SqlSourceStageCompletion {
                lease: tombstone_lease.clone(),
                transition: tombstone_transition.clone(),
                claim: tombstone_claim.clone(),
                successor: None,
            },
            10,
        )
        .await
        .unwrap();
    assert!(!tombstone_committed.replayed);
    let retained_tombstone = service
        .sql_source_manifest(binding.generation, &source_entity_id)
        .unwrap()
        .expect("the complete deletion replaces the prior SQL source manifest");
    assert_eq!(
        retained_tombstone.source_identity, present_manifest.source_identity,
        "the tombstone retains the authoritative prior row identity"
    );
    assert_eq!(
        retained_tombstone.source_revision, deletion_revision,
        "the tombstone is bound to the authenticated complete snapshot"
    );
    assert_eq!(
        retained_tombstone.source_content_digest,
        tombstone_transition.intent.input_digest
    );
    assert_eq!(
        retained_tombstone.completed_receipt_digest,
        tombstone_transition.receipt.receipt_digest()
    );
    drop(tombstone_claim);
    drop(tombstone_transition);
    drop(adapter);
    drop(service);

    revoke(
        &persist_dir,
        &owner,
        "documents",
        worker.agent_id(),
        &[SqlPrivilege::Select],
        uuid::Uuid::from_u128(72),
    )
    .unwrap();
    let service = Arc::new(open_service());
    let adapter = SemanticIndexServerAdapter::new(Arc::clone(&service));
    let replay = adapter
        .replay_sql_source_stage(54, worker.clone(), tombstone_lease, tombstone_intent, 11)
        .await
        .unwrap()
        .expect("durable S1 replay resolves from lease and intent alone");
    assert!(replay.replayed);
    assert_eq!(replay.batch_id, tombstone_committed.batch_id);
    assert_eq!(replay.mutation_digest, tombstone_committed.mutation_digest);
    assert_eq!(replay.source_version, tombstone_committed.source_version);
    assert_eq!(replay.target_version, tombstone_committed.target_version);
    assert_eq!(
        service
            .sql_source_manifest(binding.generation, &source_entity_id)
            .unwrap(),
        Some(retained_tombstone),
        "restart and replay retain one exact tombstone manifest"
    );
    // Each queue class is its own consumer (`<worker>#<class>`); S1 and
    // S2 run in different classes, so the worker's totals are the sum.
    let per_class = service.worker_stage_status(worker.agent_id(), 12).unwrap();
    let total = |field: fn(&eg_transaction::OutboxStatus) -> u64| -> u64 {
        per_class.iter().map(field).sum()
    };
    // Three resolved rows, not two: the present source's S1, the S2 it
    // derived (retired as a dead letter), and the deletion's S1. A
    // semantic `RejectedDeadLetter` is still an ACK of its outbox row --
    // `complete_stage` acknowledges the lease through the ordinary cursor
    // -- so it counts as delivered here, and `dead_lettered` stays zero
    // because that counter belongs to the outbox's own retry-exhaustion
    // path, not to a semantic rejection.
    assert_eq!(total(|status| status.delivered), 3);
    assert_eq!(total(|status| status.dead_lettered), 0);
    assert_eq!(total(|status| u64::from(status.inflight)), 0);
    // ONE pending row, not zero: completing the tombstone S1 derived its
    // own S2 GraphProjection intent, exactly as the present source's S1
    // did. A zero here would be asserting that a completed S1 is a dead
    // end, which is the assumption this whole fixture was built on and
    // which the outbox order gap refuted.
    assert_eq!(total(|status| status.pending), 1);
}
