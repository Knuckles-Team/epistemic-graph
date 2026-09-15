//! Stage completion: recording a terminal S1-S5 transition with its
//! successor intent, and replaying a completed SQL source stage.
//!
//! A completion is proved against the leased intent and a serving snapshot,
//! then recorded inside the admitted write: the predecessor is re-proved, the
//! stage row and its receipt index are written once, and only a first record
//! persists checkpoints, artifacts and the entity's advanced progress.

use super::batch::{mutation_row, semantic_digest, MetadataMutation};
use super::persist::{
    persist_generation_artifact, persist_stage_artifact_with_lease, persist_transition_checkpoints,
    put_bytes_once,
};
use super::predecessor::validate_stage_predecessor_write;
use super::record::{decode_valid, encode_valid, row, row_bytes};
use super::stage::{
    is_terminal, stage_intent_outbox, stage_receipt_event, stage_receipt_index_key,
    validate_successor_intent,
};
use super::{
    corrupt, ensure, kernel_error, refused, semantic_contract_error, SemanticCodeError,
    SemanticCodeStore, SemanticMutationReceipt,
};
use eg_storage::{SemanticIndexOwner, SEMANTIC_SOURCE_PROGRESS, SEMANTIC_STAGES};
use eg_transaction::{AdmittedMutation, AdmittedOwnerWrite};
use eg_types::mutation_batch::{MutationOutboxIntent, MutationOutboxLease};
use eg_types::semantic_index::{
    SemanticDigest, SemanticGenerationArtifact, SemanticIndexMutation, SemanticSourceProgress,
    SemanticStage, SemanticStageArtifact, SemanticStageIntent, SemanticStageOutcome,
    SemanticStageTransition,
};
use redb::ReadableTable;

/// One proved stage completion, recorded inside the admitted write.
struct Completion<'a> {
    lease: &'a MutationOutboxLease,
    transition: &'a SemanticStageTransition,
    artifact: &'a SemanticStageArtifact,
    successor: Option<&'a SemanticStageIntent>,
    generation_artifact: Option<&'a SemanticGenerationArtifact>,
    mutation: &'a SemanticIndexMutation,
    mutation_bytes: &'a [u8],
    now_ms: u64,
}

impl SemanticCodeStore {
    /// Record one terminal S1-S6 transition and enqueue its successor in the
    /// same semantic owner mutation.  The delivery acknowledgement follows the
    /// committed transition and is replay-safe: if a process dies between the
    /// two durable transactions, the next lease finds the byte-identical stage
    /// mutation and only retries the acknowledgement.
    pub(crate) fn complete_stage(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        artifact: &SemanticStageArtifact,
        successor: Option<&SemanticStageIntent>,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        self.complete_stage_inner(lease, transition, artifact, successor, None, now_ms)
    }

    /// Complete S3 or S5 with its canonical generation manifest. The
    /// checkpoint and manifest are persisted beside the stage transition and
    /// successor intent, so a replay cannot observe a completed stage without
    /// the artifact that its checkpoint names.
    pub(crate) fn complete_generation_stage(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        artifact: &SemanticGenerationArtifact,
        successor: Option<&SemanticStageIntent>,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        ensure(
            matches!(
                transition.intent.stage,
                SemanticStage::LexicalIndex | SemanticStage::AnnIndex
            ),
            "generation manifests are only completed by S3 or S5",
        )?;
        let checkpoint = transition.generation_checkpoint.as_ref().ok_or_else(|| {
            refused("generation manifest completion requires its exact checkpoint")
        })?;
        artifact
            .validate_against(&checkpoint.successor)
            .map_err(semantic_contract_error)?;
        let no_entity_artifact = SemanticStageArtifact::None;
        self.complete_stage_inner(
            lease,
            transition,
            &no_entity_artifact,
            successor,
            Some(artifact),
            now_ms,
        )
    }

    fn complete_stage_inner(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        artifact: &SemanticStageArtifact,
        successor: Option<&SemanticStageIntent>,
        generation_artifact: Option<&SemanticGenerationArtifact>,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        transition.validate().map_err(semantic_contract_error)?;
        ensure_source_commit_output(transition)?;
        if is_deferred(transition.receipt.outcome) {
            return self.defer_stage(lease, successor.is_some());
        }
        let mutation = SemanticIndexMutation::RecordStageTransition {
            transition: Box::new(transition.clone()),
            artifact: artifact.clone(),
        };
        mutation.validate().map_err(semantic_contract_error)?;
        let intent = self.parse_stage_lease(lease, &lease.consumer, now_ms)?;
        ensure(
            intent == transition.intent,
            "stage transition intent does not equal the leased canonical intent",
        )?;
        if let Some(stored) = self.read_stage_mutation(intent.intent_digest)? {
            ensure(
                stored == mutation,
                "stage intent already has a different durable transition",
            )?;
            // This path is specifically for a reclaimed lease after the stage
            // rows committed.  It still validates the supplied lease envelope;
            // outbox_ack performs the authoritative epoch/record fence.
            self.ack_stage_lease(lease, now_ms)?;
            return self
                .stage_receipt(transition, true)
                .map_err(SemanticCodeError::Corrupt);
        }
        let read = self.door.serving_read()?;
        self.validate_stage_predecessor_in(&read, &intent, true)?;
        let successor = validate_successor_intent(transition, successor)?;
        let (mutation_bytes, mutation_digest) = mutation_row(&mutation)?;
        let outbox = stage_outbox(transition, successor.as_ref())?;
        let batch_id = format!("semantic-index:stage:{}", transition.intent.intent_digest);
        let subject = format!("stage:{}", transition.intent.intent_digest);
        let completion = Completion {
            lease,
            transition,
            artifact,
            successor: successor.as_ref(),
            generation_artifact,
            mutation: &mutation,
            mutation_bytes: &mutation_bytes,
            now_ms,
        };
        self.commit_maintenance(
            MetadataMutation {
                batch_id: &batch_id,
                event_type: "semantic_stage_transition_recorded",
                subject: &subject,
                mutation_digest,
            },
            outbox,
            now_ms,
            Some(lease),
            |write, rows| self.record_completion(write, rows, &completion),
        )
    }

    /// Backpressure and an unavailable predecessor are delivery decisions,
    /// not terminal semantic transitions. Release the exact lease so the
    /// existing durable outbox can claim it again; no stage, checkpoint,
    /// receipt, or successor rows are written.
    fn defer_stage(
        &self,
        lease: &MutationOutboxLease,
        publishes_successor: bool,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        ensure(
            !publishes_successor,
            "deferred semantic stage cannot publish a successor",
        )?;
        self.release_stage_lease(lease)?;
        Err(refused(
            "deferred semantic stage remains pending and unacknowledged",
        ))
    }

    fn record_completion(
        &self,
        write: &AdmittedMutation<'_, SemanticIndexOwner>,
        rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
        completion: &Completion<'_>,
    ) -> Result<(), SemanticCodeError> {
        let (tenant, binding) = (self.tenant.as_str(), self.binding.as_str());
        let transition = completion.transition;
        // Re-read the predecessor rows through the admitted write. A serving
        // snapshot checked before admission is useful for early refusal, but
        // this is the decision that is serialized with the write.
        validate_stage_predecessor_write(write, rows, tenant, binding, &transition.intent, true)?;
        let recorded_before = self.record_stage_rows(
            write,
            rows,
            transition,
            completion.mutation,
            completion.mutation_bytes,
            "stage intent already has a different durable transition",
        )?;
        if recorded_before {
            return Ok(());
        }
        persist_transition_checkpoints(rows, tenant, binding, transition, completion.successor)?;
        persist_stage_artifact_with_lease(
            rows,
            tenant,
            binding,
            transition,
            Some(completion.lease),
            completion.artifact,
        )?;
        if let Some(generation_artifact) = completion.generation_artifact {
            persist_generation_artifact(rows, tenant, binding, transition, generation_artifact)?;
        }
        if let Some(source_entity_id) = transition.intent.scope.source_entity_id() {
            update_source_progress(
                rows,
                tenant,
                binding,
                transition,
                source_entity_id,
                completion.now_ms,
            )?;
        }
        Ok(())
    }

    /// Write the stage row of `transition` and its receipt index once. A stage
    /// row that already holds exactly `mutation` is a committed first attempt:
    /// only the receipt index is (re)asserted and `true` is returned. Any other
    /// bytes under the intent are refused with `conflict`.
    pub(super) fn record_stage_rows(
        &self,
        write: &AdmittedMutation<'_, SemanticIndexOwner>,
        rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
        transition: &SemanticStageTransition,
        mutation: &SemanticIndexMutation,
        mutation_bytes: &[u8],
        conflict: &str,
    ) -> Result<bool, SemanticCodeError> {
        let owner = (self.tenant.as_str(), self.binding.as_str());
        let stage_key = transition.intent.intent_digest.to_string();
        let existing: Option<SemanticIndexMutation> = {
            let stages = write
                .open_read_table(SEMANTIC_STAGES)
                .map_err(kernel_error)?;
            row(stages.get((owner.0, owner.1, stage_key.as_str())))?
        };
        let recorded_before = existing.is_some();
        ensure(
            existing.is_none_or(|existing| existing == *mutation),
            conflict,
        )?;
        let mut stages = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
        if !recorded_before {
            stages
                .insert((owner.0, owner.1, stage_key.as_str()), mutation_bytes)
                .map_err(kernel_error)?;
        }
        let receipt_key = stage_receipt_index_key(transition.receipt.receipt_digest());
        put_bytes_once(
            &mut stages,
            (owner.0, owner.1, receipt_key.as_str()),
            mutation_bytes,
        )?;
        Ok(recorded_before)
    }

    pub(super) fn read_stage_mutation(
        &self,
        intent_digest: SemanticDigest,
    ) -> Result<Option<SemanticIndexMutation>, SemanticCodeError> {
        let key = intent_digest.to_string();
        let read = self.door.serving_read()?;
        let stages = read
            .open_owner_table(SEMANTIC_STAGES)
            .map_err(kernel_error)?;
        row(stages.get((self.tenant.as_str(), self.binding.as_str(), key.as_str())))
    }

    /// Reconcile a reclaimed SQL S1 lease against the already committed
    /// source transition before the caller performs another source or ACL
    /// read.  The stage row and its receipt-index row are the canonical
    /// retained evidence; both must contain the same bytes, and the mutation
    /// must carry a complete SQL manifest plus authorization receipt for the
    /// leased S1 intent.  The caller supplies only that durable intent:
    /// cursor, completion time and receipt digest are server-owned and are
    /// read from the retained transition.  A missing stage row means the
    /// first attempt did not commit and leaves the caller free to perform
    /// fresh source work.
    ///
    /// Lease parsing is deliberately done before the read and ACK repeats the
    /// authoritative outbox fence afterward.  Thus a fabricated, expired,
    /// reclaimed, wrong-consumer, or wrong-owner lease cannot acknowledge a
    /// retained result, while a changed client intent conflicts before any
    /// delivery cursor is advanced.
    pub(crate) fn replay_completed_sql_source_stage(
        &self,
        lease: &MutationOutboxLease,
        expected_intent: &SemanticStageIntent,
        now_ms: u64,
    ) -> Result<Option<(SemanticStageTransition, SemanticMutationReceipt)>, SemanticCodeError> {
        expected_intent
            .validate()
            .map_err(semantic_contract_error)?;
        ensure(
            expected_intent.stage == SemanticStage::SourceCommit
                && expected_intent.scope.source_entity_id().is_some(),
            "semantic SQL source replay requires one completed entity-scoped S1",
        )?;
        let intent = self.parse_stage_lease(lease, &lease.consumer, now_ms)?;
        ensure(
            intent == *expected_intent,
            "semantic SQL source replay intent does not equal the leased intent",
        )?;
        let Some((raw, transition)) = self.retained_sql_source_stage(&intent)? else {
            return Ok(None);
        };
        let batch_id = format!("semantic-index:stage:{}", intent.intent_digest);
        let receipt = self
            .door
            .stage_receipt_for_batch(&batch_id, true)
            .map_err(SemanticCodeError::Corrupt)?;
        if receipt.mutation_digest != semantic_digest(&raw) {
            return Err(corrupt(
                "semantic SQL source replay ledger digest differs from the retained stage",
            ));
        }
        self.ack_stage_lease(lease, now_ms)?;
        Ok(Some((transition, receipt)))
    }

    /// Test-only readback for restart proofs.  The production replay route
    /// above returns the retained transition and durable receipt; tests that
    /// verify the retained authorization time use this narrow artifact accessor instead of
    /// reaching into the semantic owner's tables or kernel fields.
    #[cfg(test)]
    pub(crate) fn recorded_sql_source_artifact(
        &self,
        intent_digest: SemanticDigest,
    ) -> Result<Option<SemanticStageArtifact>, SemanticCodeError> {
        let Some(mutation) = self.read_stage_mutation(intent_digest)? else {
            return Ok(None);
        };
        let SemanticIndexMutation::RecordStageTransition {
            transition,
            artifact,
        } = mutation
        else {
            return Err(refused(
                "recorded SQL source artifact names a non-stage mutation",
            ));
        };
        ensure(
            transition.intent.stage == SemanticStage::SourceCommit,
            "recorded SQL source artifact names a non-S1 stage",
        )?;
        ensure(
            matches!(&artifact, SemanticStageArtifact::SqlSourceManifest { .. }),
            "recorded SQL source artifact is not a SQL manifest",
        )?;
        artifact
            .validate_against(&transition)
            .map_err(semantic_contract_error)?;
        Ok(Some(artifact))
    }

    pub(super) fn stage_receipt(
        &self,
        transition: &SemanticStageTransition,
        replayed: bool,
    ) -> Result<SemanticMutationReceipt, String> {
        let batch_id = format!("semantic-index:stage:{}", transition.intent.intent_digest);
        self.door.stage_receipt_for_batch(&batch_id, replayed)
    }

    /// The retained stage row of a completed SQL S1 and the transition it
    /// records, or `None` when the first attempt never committed. Its receipt
    /// index must hold the same bytes.
    fn retained_sql_source_stage(
        &self,
        intent: &SemanticStageIntent,
    ) -> Result<Option<(Vec<u8>, SemanticStageTransition)>, SemanticCodeError> {
        let owner = (self.tenant.as_str(), self.binding.as_str());
        let read = self.door.serving_read()?;
        let stages = read
            .open_owner_table(SEMANTIC_STAGES)
            .map_err(kernel_error)?;
        let stage_key = intent.intent_digest.to_string();
        let Some(raw) = row_bytes(stages.get((owner.0, owner.1, stage_key.as_str())))? else {
            return Ok(None);
        };
        let transition = retained_sql_source_transition(decode_valid(&raw)?, intent)?;
        let receipt_key = stage_receipt_index_key(transition.receipt.receipt_digest());
        let indexed = row_bytes(stages.get((owner.0, owner.1, receipt_key.as_str())))?
            .ok_or_else(|| refused("semantic SQL source replay has no retained receipt index"))?;
        ensure(
            indexed == raw,
            "semantic SQL source replay receipt index differs from the retained stage",
        )?;
        Ok(Some((raw, transition)))
    }
}

/// A completed SQL S1 outputs exactly the source input it was admitted with.
fn ensure_source_commit_output(
    transition: &SemanticStageTransition,
) -> Result<(), SemanticCodeError> {
    let completed_source_commit = transition.intent.stage == SemanticStage::SourceCommit
        && transition.receipt.outcome == SemanticStageOutcome::Completed;
    ensure(
        !completed_source_commit
            || transition.receipt.output_digest == transition.intent.input_digest,
        "completed SQL source S1 output must equal its admitted input",
    )
}

fn is_deferred(outcome: SemanticStageOutcome) -> bool {
    matches!(
        outcome,
        SemanticStageOutcome::DeferredBackpressured
            | SemanticStageOutcome::ParkedAwaitingPredecessor
    )
}

fn stage_outbox(
    transition: &SemanticStageTransition,
    successor: Option<&SemanticStageIntent>,
) -> Result<Vec<MutationOutboxIntent>, SemanticCodeError> {
    let mut outbox = vec![stage_receipt_event(transition)?];
    if let Some(successor) = successor {
        outbox.push(stage_intent_outbox(successor)?);
    }
    Ok(outbox)
}

/// The transition a retained SQL S1 stage row must record: the leased intent,
/// completed with its source input, carrying a valid SQL source manifest.
fn retained_sql_source_transition(
    mutation: SemanticIndexMutation,
    expected_intent: &SemanticStageIntent,
) -> Result<SemanticStageTransition, SemanticCodeError> {
    let SemanticIndexMutation::RecordStageTransition {
        transition,
        artifact,
    } = mutation
    else {
        return Err(refused(
            "semantic SQL source replay retained a non-stage mutation",
        ));
    };
    ensure(
        transition.intent == *expected_intent,
        "semantic SQL source replay retained a different intent",
    )?;
    ensure(
        transition.receipt.outcome == SemanticStageOutcome::Completed,
        "semantic SQL source replay retained a non-completed S1 result",
    )?;
    ensure(
        transition.receipt.output_digest == expected_intent.input_digest,
        "semantic SQL source replay output does not equal the leased source input",
    )?;
    ensure(
        matches!(&artifact, SemanticStageArtifact::SqlSourceManifest { .. }),
        "semantic SQL source replay retained no SQL source manifest",
    )?;
    artifact
        .validate_against(&transition)
        .map_err(semantic_contract_error)?;
    Ok(*transition)
}

fn update_source_progress(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    source_entity_id: &str,
    now_ms: u64,
) -> Result<(), SemanticCodeError> {
    let key = (
        tenant,
        binding,
        transition.intent.generation,
        source_entity_id,
    );
    let mut table = rows
        .open_table(SEMANTIC_SOURCE_PROGRESS)
        .map_err(kernel_error)?;
    let mut progress: SemanticSourceProgress = row(table.get(key))?
        .ok_or_else(|| refused("stage transition source progress disappeared"))?;
    if is_terminal(transition.receipt.outcome) {
        progress.completed_stage = Some(transition.intent.stage);
        progress.completed_receipt_digest = Some(transition.receipt.receipt_digest());
    }
    progress.updated_at = format!("unix-ms:{now_ms}");
    let bytes = encode_valid(&progress)?;
    table.insert(key, bytes.as_slice()).map_err(kernel_error)?;
    Ok(())
}
