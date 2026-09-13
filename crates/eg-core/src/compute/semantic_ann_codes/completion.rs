//! Stage completion: recording a terminal S1-S5 transition with its
//! successor intent, and replaying a completed SQL source stage.

use super::batch::{semantic_digest, MetadataMutation};
use super::persist::{
    persist_generation_artifact, persist_stage_artifact_with_lease, persist_transition_checkpoints,
    put_bytes_once,
};
use super::predecessor::validate_stage_predecessor_write;
use super::stage::{
    stage_intent_outbox, stage_receipt_headers, stage_receipt_index_key, validate_successor_intent,
};
use super::{
    kernel_error, semantic_contract_error, SemanticCodeError, SemanticCodeStore,
    SemanticMutationReceipt, SEMANTIC_STAGE_RECEIPT_TOPIC,
};
use eg_storage::{SemanticIndexOwner, SEMANTIC_SOURCE_PROGRESS, SEMANTIC_STAGES};
use eg_transaction::AdmittedOwnerWrite;
use eg_types::mutation_batch::{MutationOutboxIntent, MutationOutboxLease};
use eg_types::semantic_index::{
    SemanticDigest, SemanticGenerationArtifact, SemanticIndexMutation, SemanticSourceProgress,
    SemanticStage, SemanticStageArtifact, SemanticStageIntent, SemanticStageOutcome,
    SemanticStageTransition,
};
use redb::ReadableTable;

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
        if !matches!(
            transition.intent.stage,
            SemanticStage::LexicalIndex | SemanticStage::AnnIndex
        ) {
            return Err(SemanticCodeError::Refused(
                "generation manifests are only completed by S3 or S5".to_string(),
            ));
        }
        let checkpoint = transition.generation_checkpoint.as_ref().ok_or_else(|| {
            SemanticCodeError::Refused(
                "generation manifest completion requires its exact checkpoint".to_string(),
            )
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
        if transition.intent.stage == SemanticStage::SourceCommit
            && transition.receipt.outcome == SemanticStageOutcome::Completed
            && transition.receipt.output_digest != transition.intent.input_digest
        {
            return Err(SemanticCodeError::Refused(
                "completed SQL source S1 output must equal its admitted input".to_string(),
            ));
        }
        if matches!(
            transition.receipt.outcome,
            SemanticStageOutcome::DeferredBackpressured
                | SemanticStageOutcome::ParkedAwaitingPredecessor
        ) {
            if successor.is_some() {
                return Err(SemanticCodeError::Refused(
                    "deferred semantic stage cannot publish a successor".to_string(),
                ));
            }
            // Backpressure and an unavailable predecessor are delivery
            // decisions, not terminal semantic transitions. Release the exact
            // lease so the existing durable outbox can claim it again; no
            // stage, checkpoint, receipt, or successor rows are written.
            self.release_stage_lease(lease)?;
            return Err(SemanticCodeError::Refused(
                "deferred semantic stage remains pending and unacknowledged".to_string(),
            ));
        }
        let mutation = SemanticIndexMutation::RecordStageTransition {
            transition: Box::new(transition.clone()),
            artifact: artifact.clone(),
        };
        mutation.validate().map_err(semantic_contract_error)?;
        let intent = self.parse_stage_lease(lease, &lease.consumer, now_ms)?;
        if intent != transition.intent {
            return Err(SemanticCodeError::Refused(
                "stage transition intent does not equal the leased canonical intent".to_string(),
            ));
        }
        let stored = self.read_stage_mutation(intent.intent_digest)?;
        if let Some(stored) = stored {
            if stored != mutation {
                return Err(SemanticCodeError::Refused(
                    "stage intent already has a different durable transition".to_string(),
                ));
            }
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
        let mutation_bytes = mutation
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation_digest = semantic_digest(&mutation_bytes);
        let transition_payload = transition
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mut outbox = vec![MutationOutboxIntent {
            topic: SEMANTIC_STAGE_RECEIPT_TOPIC.to_string(),
            key: transition.intent.intent_digest.to_string(),
            payload: transition_payload,
            headers: stage_receipt_headers(transition),
        }];
        if let Some(successor) = &successor {
            outbox.push(stage_intent_outbox(successor)?);
        }
        let batch_id = format!("semantic-index:stage:{}", transition.intent.intent_digest);
        let source_entity_id = transition
            .intent
            .scope
            .source_entity_id()
            .map(str::to_string);
        let binding = self.binding.clone();
        let tenant = self.tenant.clone();
        let transition_for_write = transition.clone();
        let mutation_for_write = mutation.clone();
        let now = now_ms;
        let receipt = self.door.commit_metadata_fenced(
            |version| {
                self.metadata_batch(
                    self.door.owner(),
                    version,
                    MetadataMutation {
                        batch_id: &batch_id,
                        event_type: "semantic_stage_transition_recorded",
                        subject: &format!("stage:{}", transition.intent.intent_digest),
                        mutation_digest,
                    },
                    outbox,
                    now,
                )
            },
            mutation_digest,
            now_ms,
            Some(lease),
            |write, rows| {
                // Re-read the predecessor rows through the admitted write. A
                // serving snapshot checked above is useful for early refusal,
                // but this is the decision that is serialized with the write.
                validate_stage_predecessor_write(
                    write,
                    rows,
                    &tenant,
                    &binding,
                    &transition_for_write.intent,
                    true,
                )?;
                let stage_key = transition_for_write.intent.intent_digest.to_string();
                if let Some(existing) = write
                    .open_read_table(SEMANTIC_STAGES)
                    .map_err(kernel_error)?
                    .get((tenant.as_str(), binding.as_str(), stage_key.as_str()))
                    .map_err(kernel_error)?
                    .map(|value| value.value().to_vec())
                {
                    let existing = SemanticIndexMutation::from_canonical_cbor(&existing)
                        .map_err(semantic_contract_error)?;
                    if existing != mutation_for_write {
                        return Err(SemanticCodeError::Refused(
                            "stage intent already has a different durable transition".to_string(),
                        ));
                    }
                    let receipt_key =
                        stage_receipt_index_key(transition_for_write.receipt.receipt_digest());
                    let mut stages = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
                    put_bytes_once(
                        &mut stages,
                        (tenant.as_str(), binding.as_str(), receipt_key.as_str()),
                        &mutation_bytes,
                    )?;
                    return Ok(());
                }
                let mut stages = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
                stages
                    .insert(
                        (tenant.as_str(), binding.as_str(), stage_key.as_str()),
                        mutation_bytes.as_slice(),
                    )
                    .map_err(kernel_error)?;
                let receipt_key =
                    stage_receipt_index_key(transition_for_write.receipt.receipt_digest());
                put_bytes_once(
                    &mut stages,
                    (tenant.as_str(), binding.as_str(), receipt_key.as_str()),
                    &mutation_bytes,
                )?;
                drop(stages);
                persist_transition_checkpoints(
                    rows,
                    &tenant,
                    &binding,
                    &transition_for_write,
                    successor.as_ref(),
                )?;
                persist_stage_artifact_with_lease(
                    rows,
                    &tenant,
                    &binding,
                    &transition_for_write,
                    Some(lease),
                    artifact,
                )?;
                if let Some(generation_artifact) = generation_artifact {
                    persist_generation_artifact(
                        rows,
                        &tenant,
                        &binding,
                        &transition_for_write,
                        generation_artifact,
                    )?;
                }
                if let Some(source_entity_id) = &source_entity_id {
                    update_source_progress(
                        rows,
                        &tenant,
                        &binding,
                        &transition_for_write,
                        source_entity_id,
                        now,
                    )?;
                }
                Ok(())
            },
        )?;
        Ok(receipt)
    }

    pub(super) fn read_stage_mutation(
        &self,
        intent_digest: SemanticDigest,
    ) -> Result<Option<SemanticIndexMutation>, SemanticCodeError> {
        let key = intent_digest.to_string();
        let read = self.door.serving_read()?;
        let raw = read
            .open_owner_table(SEMANTIC_STAGES)
            .map_err(kernel_error)?
            .get((self.tenant.as_str(), self.binding.as_str(), key.as_str()))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec());
        raw.map(|bytes| {
            SemanticIndexMutation::from_canonical_cbor(&bytes).map_err(semantic_contract_error)
        })
        .transpose()
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
        if expected_intent.stage != SemanticStage::SourceCommit
            || expected_intent.scope.source_entity_id().is_none()
        {
            return Err(SemanticCodeError::Refused(
                "semantic SQL source replay requires one completed entity-scoped S1".to_string(),
            ));
        }
        let intent = self.parse_stage_lease(lease, &lease.consumer, now_ms)?;
        if intent != *expected_intent {
            return Err(SemanticCodeError::Refused(
                "semantic SQL source replay intent does not equal the leased intent".to_string(),
            ));
        }

        let stage_key = intent.intent_digest.to_string();
        let read = self.door.serving_read()?;
        let stages = read
            .open_owner_table(SEMANTIC_STAGES)
            .map_err(kernel_error)?;
        let Some(raw) = stages
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                stage_key.as_str(),
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
        else {
            return Ok(None);
        };
        let mutation =
            SemanticIndexMutation::from_canonical_cbor(&raw).map_err(semantic_contract_error)?;
        mutation.validate().map_err(semantic_contract_error)?;
        let SemanticIndexMutation::RecordStageTransition {
            transition: stored_transition,
            artifact,
        } = &mutation
        else {
            return Err(SemanticCodeError::Refused(
                "semantic SQL source replay retained a non-stage mutation".to_string(),
            ));
        };
        if stored_transition.intent != *expected_intent {
            return Err(SemanticCodeError::Refused(
                "semantic SQL source replay retained a different intent".to_string(),
            ));
        }
        if stored_transition.receipt.outcome != SemanticStageOutcome::Completed {
            return Err(SemanticCodeError::Refused(
                "semantic SQL source replay retained a non-completed S1 result".to_string(),
            ));
        }
        if stored_transition.receipt.output_digest != expected_intent.input_digest {
            return Err(SemanticCodeError::Refused(
                "semantic SQL source replay output does not equal the leased source input"
                    .to_string(),
            ));
        }
        if !matches!(&artifact, SemanticStageArtifact::SqlSourceManifest { .. }) {
            return Err(SemanticCodeError::Refused(
                "semantic SQL source replay retained no SQL source manifest".to_string(),
            ));
        }
        artifact
            .validate_against(stored_transition)
            .map_err(semantic_contract_error)?;
        let receipt_key = stage_receipt_index_key(stored_transition.receipt.receipt_digest());
        let indexed = stages
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                receipt_key.as_str(),
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| {
                SemanticCodeError::Refused(
                    "semantic SQL source replay has no retained receipt index".to_string(),
                )
            })?;
        if indexed != raw {
            return Err(SemanticCodeError::Refused(
                "semantic SQL source replay receipt index differs from the retained stage"
                    .to_string(),
            ));
        }
        drop(stages);
        drop(read);

        let batch_id = format!("semantic-index:stage:{}", intent.intent_digest);
        let receipt = self
            .door
            .stage_receipt_for_batch(&batch_id, true)
            .map_err(SemanticCodeError::Corrupt)?;
        let retained_digest = semantic_digest(&raw);
        if receipt.mutation_digest != retained_digest {
            return Err(SemanticCodeError::Corrupt(
                "semantic SQL source replay ledger digest differs from the retained stage"
                    .to_string(),
            ));
        }
        self.ack_stage_lease(lease, now_ms)?;
        // `stored_transition` binds a `&Box<_>` from the retained mutation, so
        // clone THROUGH the box: this returns the transition itself, which is
        // what the caller's signature promises.
        Ok(Some(((**stored_transition).clone(), receipt)))
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
            return Err(SemanticCodeError::Refused(
                "recorded SQL source artifact names a non-stage mutation".to_string(),
            ));
        };
        if transition.intent.stage != SemanticStage::SourceCommit {
            return Err(SemanticCodeError::Refused(
                "recorded SQL source artifact names a non-S1 stage".to_string(),
            ));
        }
        if !matches!(&artifact, SemanticStageArtifact::SqlSourceManifest { .. }) {
            return Err(SemanticCodeError::Refused(
                "recorded SQL source artifact is not a SQL manifest".to_string(),
            ));
        }
        artifact
            .validate_against(&transition)
            .map_err(semantic_contract_error)?;
        Ok(Some(artifact))
    }
}

fn update_source_progress(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    source_entity_id: &str,
    now_ms: u64,
) -> Result<(), SemanticCodeError> {
    let mut table = rows
        .open_table(SEMANTIC_SOURCE_PROGRESS)
        .map_err(kernel_error)?;
    let raw = table
        .get((
            tenant,
            binding,
            transition.intent.generation,
            source_entity_id,
        ))
        .map_err(kernel_error)?
        .map(|value| value.value().to_vec())
        .ok_or_else(|| {
            SemanticCodeError::Refused("stage transition source progress disappeared".to_string())
        })?;
    let mut progress =
        SemanticSourceProgress::from_canonical_cbor(&raw).map_err(semantic_contract_error)?;
    if matches!(
        transition.receipt.outcome,
        SemanticStageOutcome::Completed | SemanticStageOutcome::IdempotentNoop
    ) {
        progress.completed_stage = Some(transition.intent.stage);
        progress.completed_receipt_digest = Some(transition.receipt.receipt_digest());
    }
    progress.updated_at = format!("unix-ms:{now_ms}");
    progress.validate().map_err(semantic_contract_error)?;
    let bytes = progress
        .to_canonical_cbor()
        .map_err(semantic_contract_error)?;
    table
        .insert(
            (
                tenant,
                binding,
                transition.intent.generation,
                source_entity_id,
            ),
            bytes.as_slice(),
        )
        .map_err(kernel_error)?;
    Ok(())
}
