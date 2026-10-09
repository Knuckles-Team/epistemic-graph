//! Branch proofs for stage admission, lease fencing, completion replay,
//! owner-row write-once and checkpoint compare-and-swap, and reconciliation
//! tombstone refusals.
//!
//! Every test drives the real store ports. Where a rule guards durable state,
//! the rows it must refuse are seeded through the store's own serving-scope
//! door, and each refusal is identified by the rule's own message.

use eg_storage::{
    SemanticIndexOwner, SEMANTIC_AUTH_RECEIPTS, SEMANTIC_CHECKPOINTS, SEMANTIC_SOURCE_PROGRESS,
    SEMANTIC_SQL_SOURCES, SEMANTIC_STAGES,
};
use eg_transaction::{AdmittedMutation, AdmittedOwnerWrite, OutboxClaimBudget};
use eg_types::mutation_batch::MutationOutboxLease;
use eg_types::semantic_index::{
    SemanticBinding, SemanticDigest, SemanticGenerationCheckpoint, SemanticGenerationDependency,
    SemanticGenerationMember, SemanticIndexMutation, SemanticSourceProgress, SemanticStage,
    SemanticStageArtifact, SemanticStageIntent, SemanticStageIntentDraft, SemanticStageOutcome,
    SemanticStagePredecessor, SemanticStageReceipt, SemanticStageScope,
};

use super::batch::{semantic_digest, MetadataMutation};
use super::persist::{advance_checkpoint_head, put_bytes_once, store_checkpoint};
use super::reconciliation::{
    SemanticSourceReconciliationCheckpoint, SemanticSourceReconciliationPhase,
    SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY,
};
use super::reconciliation_codec::encode_reconciliation_checkpoint;
use super::record::encode;
use super::stage::stage_receipt_index_key;
use super::stage_pipeline_tests::{
    one_entity_checkpoint, receipt, transition, Pipeline, CONSUMER, REVISION,
};
use super::tests::{
    completed_sql_source_artifact, open_store, pending_binding, tmp_dir, BINDING, TENANT,
};
use super::tombstone::{is_reconciled_sql_tombstone, reconciliation_tombstone_input_digest};
use super::{SemanticCodeError, SemanticCodeStore, SEMANTIC_STAGE_RECEIPT_TOPIC};

fn digest(seed: u8) -> SemanticDigest {
    SemanticDigest::from_bytes([seed; 32])
}

/// A canonical SQL source revision of one authority at `epoch`.
pub(super) fn revision(authority: char, epoch: u64) -> String {
    format!(
        "sql-source:sha256:{}:epoch:{epoch}",
        authority.to_string().repeat(64)
    )
}

/// The message of a refused operation.
pub(super) fn refusal<T, E: std::fmt::Display>(result: Result<T, E>) -> String {
    match result {
        Ok(_) => panic!("the operation must be refused"),
        Err(error) => error.to_string(),
    }
}

/// Commit owner rows as one maintenance batch through the store's own door.
fn seed_batch_rows<F>(
    codes: &SemanticCodeStore,
    batch_id: &str,
    mutation_digest: SemanticDigest,
    apply: F,
) where
    F: FnOnce(
        &AdmittedMutation<'_, SemanticIndexOwner>,
        &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    ) -> Result<(), SemanticCodeError>,
{
    let mutation = MetadataMutation {
        batch_id,
        event_type: "semantic_refusal_seed",
        subject: "generation:1",
        mutation_digest,
    };
    codes
        .door
        .commit_metadata_fenced(
            |version| codes.metadata_batch(codes.door.owner(), version, mutation, Vec::new(), 12),
            mutation_digest,
            12,
            None,
            apply,
        )
        .unwrap();
}

/// Seed owner rows under a batch named by `tag`.
pub(super) fn seed_rows<F>(codes: &SemanticCodeStore, tag: &str, seed: u8, apply: F)
where
    F: FnOnce(
        &AdmittedMutation<'_, SemanticIndexOwner>,
        &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    ) -> Result<(), SemanticCodeError>,
{
    let batch_id = format!("semantic-index:refusal-seed:{tag}");
    seed_batch_rows(codes, &batch_id, digest(seed), apply);
}

/// Replace one generation-one source progress row.
pub(super) fn put_progress_row(
    codes: &SemanticCodeStore,
    tag: &str,
    seed: u8,
    entity: &str,
    bytes: &[u8],
) {
    seed_rows(codes, tag, seed, |_, rows| {
        rows.open_table(SEMANTIC_SOURCE_PROGRESS)
            .unwrap()
            .insert((TENANT, BINDING, 1, entity), bytes)
            .unwrap();
        Ok(())
    });
}

/// Replace one stage row, keyed by an intent digest or a receipt index key.
pub(super) fn put_stage_row(
    codes: &SemanticCodeStore,
    tag: &str,
    seed: u8,
    key: &str,
    bytes: &[u8],
) {
    seed_rows(codes, tag, seed, |_, rows| {
        rows.open_table(SEMANTIC_STAGES)
            .unwrap()
            .insert((TENANT, BINDING, key), bytes)
            .unwrap();
        Ok(())
    });
}

/// A generation-one S1 completed as an idempotent no-op at `revision`: its
/// completed progress, its stage row bytes, and its receipt index key. The
/// cursor seed changes the receipt, and so the receipt digest.
pub(super) fn completed_prior(
    binding: &SemanticBinding,
    entity: &str,
    revision: &str,
    cursor_seed: u8,
) -> (SemanticSourceProgress, Vec<u8>, String) {
    let intent = s1_of(binding, entity, revision, digest(102));
    let receipt = SemanticStageReceipt {
        intent_digest: intent.intent_digest,
        output_digest: intent.input_digest,
        cursor: format!("cursor:{cursor_seed}"),
        completed_at: "2026-09-08T00:06:00Z".to_string(),
        outcome: SemanticStageOutcome::IdempotentNoop,
    };
    let receipt_digest = receipt.receipt_digest();
    let row = SemanticIndexMutation::RecordStageTransition {
        transition: Box::new(transition(&intent, receipt)),
        artifact: SemanticStageArtifact::None,
    }
    .to_canonical_cbor()
    .unwrap();
    let progress = SemanticSourceProgress {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        source_entity_id: entity.to_string(),
        source_revision: revision.to_string(),
        completed_stage: Some(SemanticStage::SourceCommit),
        completed_receipt_digest: Some(receipt_digest),
        superseded_by_revision: None,
        updated_at: "unix-ms:1".to_string(),
    };
    (progress, row, stage_receipt_index_key(receipt_digest))
}

fn draft_of(
    binding: &SemanticBinding,
    entity: &str,
    revision: &str,
    input: SemanticDigest,
) -> SemanticStageIntentDraft {
    SemanticStageIntentDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: entity.to_string(),
        },
        source_revision: revision.to_string(),
        stage: SemanticStage::SourceCommit,
        predecessor: SemanticStagePredecessor::None,
        input_digest: input,
    }
}

pub(super) fn s1_of(
    binding: &SemanticBinding,
    entity: &str,
    revision: &str,
    input: SemanticDigest,
) -> SemanticStageIntent {
    SemanticStageIntent::create(draft_of(binding, entity, revision, input)).unwrap()
}

fn graph_projection_draft(draft: SemanticStageIntentDraft) -> SemanticStageIntentDraft {
    SemanticStageIntentDraft {
        stage: SemanticStage::GraphProjection,
        predecessor: SemanticStagePredecessor::EntityReceipt {
            stage: SemanticStage::SourceCommit,
            receipt_digest: digest(41),
        },
        ..draft
    }
}

#[test]
fn s1_admission_refuses_intents_outside_this_binding_or_stage() {
    let pipeline = Pipeline::start("refusal-s1-outside");
    let binding = &pipeline.binding;
    let entity = pipeline.entity();
    let enqueue = |draft: SemanticStageIntentDraft| {
        let intent = SemanticStageIntent::create(draft).unwrap();
        refusal(pipeline.codes.enqueue_stage_intent(&intent, pipeline.now()))
    };
    let other_binding = enqueue(SemanticStageIntentDraft {
        binding_id: "semantic-binding-b".to_string(),
        ..draft_of(binding, &entity, REVISION, digest(40))
    });
    assert!(
        other_binding.contains("intent is outside this binding"),
        "{other_binding}"
    );
    let not_s1 = enqueue(graph_projection_draft(draft_of(
        binding,
        &entity,
        REVISION,
        digest(40),
    )));
    assert!(
        not_s1.contains("accepts only an S1 intent with no predecessor"),
        "{not_s1}"
    );
    let stale = enqueue(SemanticStageIntentDraft {
        generation: 2,
        ..draft_of(binding, &entity, REVISION, digest(40))
    });
    assert!(
        stale.contains("binding digest or generation is stale"),
        "{stale}"
    );

    let dir = tmp_dir("refusal-s1-unbound");
    let empty = open_store(&dir);
    let orphan = s1_of(binding, &entity, REVISION, digest(42));
    let unbound = refusal(empty.enqueue_stage_intent(&orphan, 1));
    assert!(unbound.contains("names no durable binding"), "{unbound}");
    let mut budget = OutboxClaimBudget::new(1, 5_000, 1).unwrap();
    let unclaimable = refusal(empty.claim_stage_leases(CONSUMER, &mut budget));
    assert!(
        unclaimable.contains("claim has no durable binding authority"),
        "{unclaimable}"
    );
    drop(empty);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn s1_admission_names_why_existing_progress_refuses_it() {
    let pipeline = Pipeline::start("refusal-s1-progress");
    let entity = pipeline.entity();
    let enqueue = |revision: &str, seed: u8| {
        let intent = s1_of(&pipeline.binding, &entity, revision, digest(seed));
        pipeline.codes.enqueue_stage_intent(&intent, pipeline.now())
    };
    enqueue(&revision('a', 2), 41).unwrap();
    let again = refusal(enqueue(&revision('a', 2), 42));
    assert!(
        again.contains("already admitted; retry the same intent"),
        "{again}"
    );
    let older = refusal(enqueue(REVISION, 43));
    assert!(older.contains("stale or not strictly newer"), "{older}");
    let incomplete = refusal(enqueue(&revision('a', 3), 44));
    assert!(
        incomplete.contains("has no completed predecessor"),
        "{incomplete}"
    );

    let completed = Pipeline::start("refusal-s1-supersession");
    completed.run_s1();
    let newer = s1_of(
        &completed.binding,
        &completed.entity(),
        &revision('a', 2),
        digest(45),
    );
    let supersede = refusal(
        completed
            .codes
            .enqueue_stage_intent(&newer, completed.now()),
    );
    assert!(
        supersede.contains("supersession requires a replacement binding generation"),
        "{supersede}"
    );
}

#[test]
fn a_stage_lease_is_fenced_by_its_record_consumer_expiry_owner_and_event() {
    let pipeline = Pipeline::start("refusal-lease-fence");
    let s1 = pipeline.enqueue_s1();
    let (lease, _) = pipeline.claim();
    let now = pipeline.now();
    let fenced = |lease: &MutationOutboxLease, consumer: &str, now: u64| {
        refusal(pipeline.codes.validate_stage_lease(lease, consumer, now))
    };
    let consumer = fenced(&lease, "another-consumer", now);
    assert!(consumer.contains("consumer does not match"), "{consumer}");
    // Deliberately at the boundary: `now_ms == lease.lease_until_ms` must
    // already be refused as expired -- `lease_until_ms` is an EXCLUSIVE
    // upper bound (see its doc comment in eg-types' MutationOutboxLease /
    // eg-transaction's OutboxDelivery), so this millisecond is the first
    // INVALID one, not the last valid one. EH-315: this used to check for
    // the old collapsed message text ("...absent, unissued, or expired"),
    // which stopped existing when that message was split into distinct
    // ABSENT/UNISSUED/EXPIRED refusals -- the boundary semantics this test
    // actually exercises did not change, only the message it reads.
    let expired = fenced(&lease, CONSUMER, lease.lease_until_ms);
    assert!(expired.contains("has EXPIRED"), "{expired}");
    let mut keyless = lease.clone();
    keyless.record.intent.key = String::new();
    let invalid = fenced(&keyless, CONSUMER, now);
    assert!(
        invalid.contains("invalid semantic lease record"),
        "{invalid}"
    );
    let mut receipt_topic = lease.clone();
    receipt_topic.record.intent.topic = SEMANTIC_STAGE_RECEIPT_TOPIC.to_string();
    let topic = fenced(&receipt_topic, CONSUMER, now);
    assert!(topic.contains("outside this serving owner"), "{topic}");
    let mut rekeyed = lease.clone();
    rekeyed.record.intent.key = "semantic-stage-intent:rekeyed".to_string();
    let key = fenced(&rekeyed, CONSUMER, now);
    assert!(
        key.contains("lease key does not match its canonical intent"),
        "{key}"
    );
    let mut relabeled = lease.clone();
    relabeled
        .record
        .intent
        .headers
        .insert("stage".to_string(), "graph_projection".to_string());
    let header = fenced(&relabeled, CONSUMER, now);
    assert!(
        header.contains("lease header stage does not match its canonical intent"),
        "{header}"
    );

    let dir = tmp_dir("refusal-lease-unbound");
    let empty = open_store(&dir);
    let unbound = refusal(empty.validate_stage_lease(&lease, CONSUMER, now));
    assert!(
        unbound.contains("lease has no durable binding authority"),
        "{unbound}"
    );
    drop(empty);
    let _ = std::fs::remove_dir_all(&dir);

    let fenced_intent = pipeline
        .codes
        .validate_stage_lease(&lease, CONSUMER, pipeline.now())
        .unwrap();
    assert_eq!(fenced_intent, s1);
}

#[test]
fn a_reclaimed_lease_replays_its_committed_stage_and_refuses_other_bytes() {
    let pipeline = Pipeline::start("refusal-stage-replay");
    pipeline.enqueue_s1();
    let (lease, intent) = pipeline.claim();
    let (completed, artifact) = pipeline.s1_completion(&intent);
    let bytes = SemanticIndexMutation::RecordStageTransition {
        transition: Box::new(completed.clone()),
        artifact: artifact.clone(),
    }
    .to_canonical_cbor()
    .unwrap();
    let batch_id = format!("semantic-index:stage:{}", intent.intent_digest);
    let stage_key = intent.intent_digest.to_string();
    // The first attempt committed its stage row, then died before its
    // delivery acknowledgement.
    seed_batch_rows(
        &pipeline.codes,
        &batch_id,
        semantic_digest(&bytes),
        |_, rows| {
            rows.open_table(SEMANTIC_STAGES)
                .unwrap()
                .insert((TENANT, BINDING, stage_key.as_str()), bytes.as_slice())
                .unwrap();
            Ok(())
        },
    );
    let other = completed_sql_source_artifact(
        &pipeline.binding,
        &completed,
        &pipeline.identity,
        &completed.receipt.completed_at,
        77,
    );
    let conflict = refusal(pipeline.complete(&lease, &completed, &other, None));
    assert!(
        conflict.contains("already has a different durable transition"),
        "{conflict}"
    );

    let replayed = pipeline
        .complete(&lease, &completed, &artifact, None)
        .unwrap();
    assert!(replayed.replayed, "a reclaimed lease replays the stage");
    assert_eq!(replayed.batch_id, batch_id);
    assert_eq!(replayed.mutation_digest, semantic_digest(&bytes));
    let mut budget = OutboxClaimBudget::new(1, 60_000, pipeline.now()).unwrap();
    let outcome = pipeline
        .codes
        .claim_stage_leases(CONSUMER, &mut budget)
        .unwrap();
    assert!(
        outcome.claims.is_empty(),
        "the replay acknowledged the reclaimed lease"
    );
}

#[test]
fn a_sql_source_replay_reads_only_the_retained_s1_it_recorded() {
    let pipeline = Pipeline::start("refusal-sql-replay");
    pipeline.enqueue_s1();
    let (lease, intent) = pipeline.claim();
    let codes = &pipeline.codes;
    let replay = || codes.replay_completed_sql_source_stage(&lease, &intent, pipeline.now());
    assert!(replay().unwrap().is_none());
    assert!(codes
        .recorded_sql_source_artifact(intent.intent_digest)
        .unwrap()
        .is_none());

    let stage_key = intent.intent_digest.to_string();
    let binding_row = SemanticIndexMutation::StoreBinding {
        binding: Box::new(pending_binding(REVISION)),
    }
    .to_canonical_cbor()
    .unwrap();
    put_stage_row(codes, "non-stage", 81, &stage_key, &binding_row);
    let non_stage = refusal(replay());
    assert!(
        non_stage.contains("retained a non-stage mutation"),
        "{non_stage}"
    );
    let non_stage_artifact = refusal(codes.recorded_sql_source_artifact(intent.intent_digest));
    assert!(
        non_stage_artifact.contains("names a non-stage mutation"),
        "{non_stage_artifact}"
    );

    let mut noop = transition(&intent, receipt(&intent, intent.input_digest, 82));
    noop.receipt.outcome = SemanticStageOutcome::IdempotentNoop;
    let noop_row = SemanticIndexMutation::RecordStageTransition {
        transition: Box::new(noop),
        artifact: SemanticStageArtifact::None,
    }
    .to_canonical_cbor()
    .unwrap();
    put_stage_row(codes, "manifestless", 83, &stage_key, &noop_row);
    let manifestless = refusal(codes.recorded_sql_source_artifact(intent.intent_digest));
    assert!(
        manifestless.contains("is not a SQL manifest"),
        "{manifestless}"
    );

    let (completed, artifact) = pipeline.s1_completion(&intent);
    let retained = SemanticIndexMutation::RecordStageTransition {
        transition: Box::new(completed.clone()),
        artifact,
    }
    .to_canonical_cbor()
    .unwrap();
    let receipt_key = stage_receipt_index_key(completed.receipt.receipt_digest());
    let batch_id = format!("semantic-index:stage:{}", intent.intent_digest);
    // The ledger names a different mutation digest than the retained rows.
    seed_batch_rows(codes, &batch_id, digest(84), |_, rows| {
        let mut stages = rows.open_table(SEMANTIC_STAGES).unwrap();
        stages
            .insert((TENANT, BINDING, stage_key.as_str()), retained.as_slice())
            .unwrap();
        stages
            .insert((TENANT, BINDING, receipt_key.as_str()), retained.as_slice())
            .unwrap();
        Ok(())
    });
    let diverged = refusal(replay());
    assert!(
        diverged.contains("ledger digest differs from the retained stage"),
        "{diverged}"
    );
    assert!(codes
        .recorded_sql_source_artifact(intent.intent_digest)
        .unwrap()
        .is_some());
}

fn lexical_checkpoint(pipeline: &Pipeline, second: u8) -> SemanticGenerationCheckpoint {
    let intent = s1_of(&pipeline.binding, &pipeline.entity(), REVISION, digest(60));
    let member = SemanticGenerationMember {
        source_entity_id: pipeline.entity(),
        source_revision: REVISION.to_string(),
        receipt_digest: digest(61),
        artifact_digest: digest(62),
    };
    one_entity_checkpoint(
        &intent,
        SemanticStage::LexicalIndex,
        member,
        SemanticGenerationDependency::None,
        &format!("2026-09-10T00:01:{second:02}Z"),
    )
}

fn head_refusal(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    successor: &SemanticGenerationCheckpoint,
    previous: Option<&SemanticGenerationCheckpoint>,
) -> String {
    refusal(advance_checkpoint_head(
        rows, TENANT, BINDING, successor, previous,
    ))
}

#[test]
fn a_checkpoint_head_moves_only_by_compare_and_swap_from_its_durable_row() {
    let pipeline = Pipeline::start("refusal-checkpoint-heads");
    let [first, second, third] = [1, 2, 3].map(|second| lexical_checkpoint(&pipeline, second));
    let first_key = first.checkpoint_digest.to_string();
    let second_key = second.checkpoint_digest.to_string();
    let third_key = third.checkpoint_digest.to_string();
    let first_bytes = encode(&first).unwrap();
    let second_bytes = encode(&second).unwrap();
    seed_rows(&pipeline.codes, "checkpoint-heads", 63, |_, rows| {
        let rowless = head_refusal(rows, &first, None);
        assert!(rowless.contains("has no checkpoint row"), "{rowless}");
        rows.open_table(SEMANTIC_CHECKPOINTS)
            .unwrap()
            .insert(
                (TENANT, BINDING, 1, third_key.as_str()),
                first_bytes.as_slice(),
            )
            .unwrap();
        let misfiled = head_refusal(rows, &third, None);
        assert!(
            misfiled.contains("does not match checkpoint row bytes"),
            "{misfiled}"
        );
        rows.open_table(SEMANTIC_CHECKPOINTS)
            .unwrap()
            .insert(
                (TENANT, BINDING, 1, second_key.as_str()),
                second_bytes.as_slice(),
            )
            .unwrap();
        let absent = head_refusal(rows, &second, Some(&first));
        assert!(absent.contains("CAS predecessor is absent"), "{absent}");

        store_checkpoint(rows, TENANT, BINDING, &first, None).unwrap();
        advance_checkpoint_head(rows, TENANT, BINDING, &first, None).unwrap();
        let another = head_refusal(rows, &second, None);
        assert!(
            another.contains("already names another successor"),
            "{another}"
        );
        let stale = head_refusal(rows, &second, Some(&third));
        assert!(stale.contains("CAS predecessor is stale"), "{stale}");
        advance_checkpoint_head(rows, TENANT, BINDING, &second, Some(&first)).unwrap();

        let mut checkpoints = rows.open_table(SEMANTIC_CHECKPOINTS).unwrap();
        put_bytes_once(
            &mut checkpoints,
            (TENANT, BINDING, 1, first_key.as_str()),
            &first_bytes,
        )
        .unwrap();
        let overwrite = refusal(put_bytes_once(
            &mut checkpoints,
            (TENANT, BINDING, 1, first_key.as_str()),
            &second_bytes,
        ));
        assert!(
            overwrite.contains("already names different bytes"),
            "{overwrite}"
        );
        Ok(())
    });
}

fn put_entity_artifacts(
    codes: &SemanticCodeStore,
    tag: &str,
    seed: u8,
    entity: &str,
    manifest: &[u8],
    authorization: &[u8],
) {
    seed_rows(codes, tag, seed, |_, rows| {
        let key = (TENANT, BINDING, 1, entity);
        rows.open_table(SEMANTIC_SQL_SOURCES)
            .unwrap()
            .insert(key, manifest)
            .unwrap();
        rows.open_table(SEMANTIC_AUTH_RECEIPTS)
            .unwrap()
            .insert(key, authorization)
            .unwrap();
        Ok(())
    });
}

#[test]
fn a_stage_artifact_key_keeps_its_first_authorization_receipt() {
    let pipeline = Pipeline::start("refusal-auth-receipt");
    pipeline.enqueue_s1();
    let (lease, intent) = pipeline.claim();
    let (completed, artifact) = pipeline.s1_completion(&intent);
    let SemanticStageArtifact::SqlSourceManifest {
        manifest,
        authorization,
    } = &artifact
    else {
        panic!("S1 completes with a SQL source manifest");
    };
    let other = completed_sql_source_artifact(
        &pipeline.binding,
        &completed,
        &pipeline.identity,
        &completed.receipt.completed_at,
        78,
    );
    let SemanticStageArtifact::SqlSourceManifest {
        authorization: other_authorization,
        ..
    } = &other
    else {
        panic!("S1 completes with a SQL source manifest");
    };
    let entity = pipeline.entity();
    let manifest_bytes = encode(manifest.as_ref()).unwrap();
    let other_receipt = encode(other_authorization.as_ref()).unwrap();
    put_entity_artifacts(
        &pipeline.codes,
        "foreign-receipt",
        85,
        &entity,
        &manifest_bytes,
        &other_receipt,
    );
    let conflict = refusal(pipeline.complete(&lease, &completed, &artifact, None));
    assert!(
        conflict.contains("authorization receipt key already names different bytes"),
        "{conflict}"
    );
    assert!(pipeline.progress().completed_stage.is_none());

    let exact_receipt = encode(authorization.as_ref()).unwrap();
    put_entity_artifacts(
        &pipeline.codes,
        "exact-receipt",
        86,
        &entity,
        &manifest_bytes,
        &exact_receipt,
    );
    pipeline
        .complete(&lease, &completed, &artifact, None)
        .unwrap();
    assert_eq!(
        pipeline.progress().completed_stage,
        Some(SemanticStage::SourceCommit)
    );
}

fn tombstone_entity() -> String {
    format!("semantic-sql-source:{}", digest(42))
}

fn finalizing(revision: &str) -> SemanticSourceReconciliationCheckpoint {
    SemanticSourceReconciliationCheckpoint {
        source_wakeup_digest: digest(44),
        source_revision: revision.to_string(),
        phase: SemanticSourceReconciliationPhase::FinalizingTombstones,
        source_cursor: None,
        prior_cursor: None,
        rows_seen: 2,
        source_bytes_seen: 128,
        pages_seen: 1,
        complete_snapshot_receipt_digest: Some(digest(45)),
    }
}

fn tombstone_intent(pipeline: &Pipeline, entity: &str, revision: &str) -> SemanticStageIntent {
    let proof = reconciliation_tombstone_input_digest(entity, revision, digest(45));
    s1_of(&pipeline.binding, entity, revision, proof)
}

#[test]
fn a_reconciliation_tombstone_request_must_carry_its_exact_deletion_proof() {
    let pipeline = Pipeline::start("refusal-tombstone-request");
    let entity = tombstone_entity();
    let epoch_two = revision('a', 2);
    let checkpoint = finalizing(&epoch_two);
    let enqueue = |intent: &SemanticStageIntent,
                   checkpoint: &SemanticSourceReconciliationCheckpoint| {
        refusal(
            pipeline
                .codes
                .enqueue_reconciliation_tombstone(intent, checkpoint, pipeline.now()),
        )
    };
    let proof = tombstone_intent(&pipeline, &entity, &epoch_two);
    let not_s1 = SemanticStageIntent::create(graph_projection_draft(draft_of(
        &pipeline.binding,
        &entity,
        &epoch_two,
        proof.input_digest,
    )))
    .unwrap();
    let stage = enqueue(&not_s1, &checkpoint);
    assert!(
        stage.contains("must be an S1 intent for this binding"),
        "{stage}"
    );
    let plain = tombstone_intent(&pipeline, "entity:plain", &epoch_two);
    let named = enqueue(&plain, &checkpoint);
    assert!(named.contains("non-canonical source entity"), "{named}");
    let scanning = SemanticSourceReconciliationCheckpoint {
        phase: SemanticSourceReconciliationPhase::Scanning,
        source_cursor: Some(b"cursor".to_vec()),
        rows_seen: 1,
        source_bytes_seen: 2,
        pages_seen: 3,
        complete_snapshot_receipt_digest: None,
        ..finalizing(&epoch_two)
    };
    let unfinished = enqueue(&proof, &scanning);
    assert!(
        unfinished.contains("lacks an exact finalizing source proof"),
        "{unfinished}"
    );
    let elsewhere = enqueue(&proof, &finalizing(&revision('a', 3)));
    assert!(
        elsewhere.contains("lacks an exact finalizing source proof"),
        "{elsewhere}"
    );
    let arbitrary = s1_of(&pipeline.binding, &entity, &epoch_two, digest(71));
    let input = enqueue(&arbitrary, &checkpoint);
    assert!(
        input.contains("input is not the exact deletion proof"),
        "{input}"
    );
}

#[test]
fn a_reconciliation_tombstone_is_reproved_against_every_durable_row_it_replaces() {
    let pipeline = Pipeline::start("refusal-tombstone-write");
    let codes = &pipeline.codes;
    let entity = tombstone_entity();
    let epoch_two = revision('a', 2);
    let checkpoint = finalizing(&epoch_two);
    let intent = tombstone_intent(&pipeline, &entity, &epoch_two);
    let enqueue = |intent: &SemanticStageIntent,
                   checkpoint: &SemanticSourceReconciliationCheckpoint| {
        codes.enqueue_reconciliation_tombstone(intent, checkpoint, pipeline.now())
    };
    let refused_with = |message: &str| {
        let text = refusal(enqueue(&intent, &checkpoint));
        assert!(text.contains(message), "expected {message:?}, got {text}");
    };

    let foreign_revision = revision('b', 2);
    let foreign = tombstone_intent(&pipeline, &entity, &foreign_revision);
    let authority = refusal(enqueue(&foreign, &finalizing(&foreign_revision)));
    assert!(
        authority.contains("tombstone belongs to another source authority"),
        "{authority}"
    );
    let next_generation = SemanticStageIntent::create(SemanticStageIntentDraft {
        generation: 2,
        ..draft_of(&pipeline.binding, &entity, &epoch_two, intent.input_digest)
    })
    .unwrap();
    let stale = refusal(enqueue(&next_generation, &checkpoint));
    assert!(stale.contains("binding proof is stale"), "{stale}");

    refused_with("has no durable checkpoint");
    let moved = SemanticSourceReconciliationCheckpoint {
        rows_seen: 3,
        ..finalizing(&epoch_two)
    };
    let checkpoint_row = SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY;
    let moved_bytes = encode_reconciliation_checkpoint(&moved).unwrap();
    put_progress_row(codes, "moved-checkpoint", 72, checkpoint_row, &moved_bytes);
    refused_with("checkpoint proof is stale");
    let exact_bytes = encode_reconciliation_checkpoint(&checkpoint).unwrap();
    put_progress_row(codes, "exact-checkpoint", 73, checkpoint_row, &exact_bytes);
    refused_with("names no durable prior entity");

    let (prior, prior_row, receipt_key) = completed_prior(&pipeline.binding, &entity, REVISION, 74);
    let put_prior = |tag: &str, seed: u8, progress: &SemanticSourceProgress| {
        let bytes = progress.to_canonical_cbor().unwrap();
        put_progress_row(codes, tag, seed, &entity, &bytes);
    };
    put_prior(
        "foreign-prior",
        75,
        &SemanticSourceProgress {
            source_revision: revision('b', 1),
            ..prior.clone()
        },
    );
    refused_with("progress belongs to another source authority");
    put_prior(
        "superseded-prior",
        76,
        &SemanticSourceProgress {
            superseded_by_revision: Some(revision('a', 3)),
            ..prior.clone()
        },
    );
    refused_with("prior progress is outside the current generation");
    put_prior(
        "newer-prior",
        77,
        &SemanticSourceProgress {
            source_revision: revision('a', 3),
            ..prior.clone()
        },
    );
    refused_with("tombstone source revision is stale");
    put_prior(
        "incomplete-prior",
        78,
        &SemanticSourceProgress {
            completed_stage: None,
            completed_receipt_digest: None,
            ..prior.clone()
        },
    );
    refused_with("requires completed prior progress");
    put_prior("completed-prior", 79, &prior);
    refused_with("prior receipt is not indexed");
    let (_, other_row, _) = completed_prior(&pipeline.binding, &entity, REVISION, 80);
    put_stage_row(codes, "inexact-receipt", 81, &receipt_key, &other_row);
    refused_with("prior receipt is not exact");

    put_stage_row(codes, "exact-receipt", 82, &receipt_key, &prior_row);
    let admitted = enqueue(&intent, &checkpoint).unwrap();
    assert!(!admitted.replayed);
}

#[test]
fn only_a_tombstone_lease_with_its_snapshot_proof_marks_a_reconciled_s1() {
    let pipeline = Pipeline::start("refusal-reconciled-lease");
    let entity = pipeline.entity();
    let tombstone = tombstone_intent(&pipeline, &entity, REVISION);
    pipeline
        .codes
        .enqueue_stage_intent(&tombstone, pipeline.now())
        .unwrap();
    let (lease, leased) = pipeline.claim();
    let completed = transition(&leased, receipt(&leased, leased.input_digest, 41));
    let marks =
        |lease: Option<&MutationOutboxLease>| is_reconciled_sql_tombstone(&completed, lease);
    assert!(!marks(None).unwrap());
    assert!(!marks(Some(&lease)).unwrap());

    let with_headers = |headers: &[(&str, &str)]| {
        let mut marked = lease.clone();
        for (name, value) in headers {
            marked
                .record
                .intent
                .headers
                .insert((*name).to_string(), (*value).to_string());
        }
        marked
    };
    let proof = ("reconciliation_proof", "semantic-source-tombstone/v1");
    let receiptless = refusal(marks(Some(&with_headers(&[proof]))));
    assert!(
        receiptless.contains("has no complete snapshot receipt"),
        "{receiptless}"
    );
    let receipt_header = "complete_snapshot_receipt_digest";
    let garbled = refusal(marks(Some(&with_headers(&[
        proof,
        (receipt_header, "not-a-digest"),
    ]))));
    assert!(garbled.contains("receipt is not canonical"), "{garbled}");
    let other_receipt = digest(46).to_string();
    let other = refusal(marks(Some(&with_headers(&[
        proof,
        (receipt_header, other_receipt.as_str()),
    ]))));
    assert!(
        other.contains("input is not its complete snapshot proof"),
        "{other}"
    );
    let exact_receipt = digest(45).to_string();
    let exact = with_headers(&[proof, (receipt_header, exact_receipt.as_str())]);
    assert!(marks(Some(&exact)).unwrap());

    let mut deferred = completed.clone();
    deferred.receipt.outcome = SemanticStageOutcome::DeferredBackpressured;
    let invalid = refusal(is_reconciled_sql_tombstone(&deferred, Some(&exact)));
    assert!(
        invalid.contains("has an invalid S1 transition"),
        "{invalid}"
    );
}
