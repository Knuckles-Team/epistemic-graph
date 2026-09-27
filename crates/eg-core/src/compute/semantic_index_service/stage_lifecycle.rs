use super::*;

impl SemanticIndexService {
    /// Check the existing outbox lease and canonical S1-S6 identity before an
    /// executor starts.  The executor must still persist the stage transition
    /// and acknowledge through the existing durable consumer port.
    pub fn validate_stage_lease(
        &self,
        lease: &eg_types::mutation_batch::MutationOutboxLease,
        consumer: &str,
        now_ms: u64,
    ) -> Result<SemanticStageIntent, SemanticCodeError> {
        self.store.validate_stage_lease(lease, consumer, now_ms)
    }

    pub fn subscribe_stage_consumer(&self, consumer: &str) -> Result<(), SemanticCodeError> {
        self.store.subscribe_stage_consumer(consumer)
    }

    pub fn claim_stage_leases(
        &self,
        consumer: &str,
        budget: &mut OutboxClaimBudget,
    ) -> Result<OutboxClaimOutcome, SemanticCodeError> {
        self.store.claim_stage_leases(consumer, budget)
    }

    /// Claim one queue class for one worker: its own consumer on its own
    /// class topic, so no other class's rows are ever leased to it.
    pub fn claim_stage_class(
        &self,
        worker: &str,
        class: eg_types::semantic_index::SemanticQueueClass,
        budget: &mut OutboxClaimBudget,
    ) -> Result<OutboxClaimOutcome, SemanticCodeError> {
        self.store.claim_stage_class(worker, class, budget)
    }

    pub fn stage_status(
        &self,
        consumer: &str,
        now_ms: u64,
    ) -> Result<OutboxStatus, SemanticCodeError> {
        self.store.stage_status(consumer, now_ms)
    }

    /// One worker's stage status in every queue class. Each class is its own
    /// consumer (`<worker>#<class>`), so a worker's figures are the sum of
    /// these rows.
    pub fn worker_stage_status(
        &self,
        worker: &str,
        now_ms: u64,
    ) -> Result<Vec<OutboxStatus>, SemanticCodeError> {
        crate::compute::semantic_ann_codes::SEMANTIC_QUEUE_CLASSES
            .iter()
            .map(|class| {
                let consumer = crate::compute::semantic_ann_codes::stage_consumer(worker, *class);
                self.stage_status(&consumer, now_ms)
            })
            .collect()
    }

    /// Persist one terminal transition, publish the successor intent when its
    /// predecessor proof is complete, then acknowledge the exact lease through
    /// the shared outbox cursor.
    pub fn complete_stage(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        artifact: &SemanticStageArtifact,
        successor: Option<&SemanticStageIntent>,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        self.store
            .complete_stage(lease, transition, artifact, successor, now_ms)
    }

    /// Complete an S1 source transition from its authenticated raw SQL claim.
    /// The claim's ACL decision is converted into a full authorization receipt
    /// before the manifest and receipt are handed to the durable store. The
    /// caller must provide the decision's authoritative authorization time;
    /// this method never fabricates one from the admission clock.
    pub fn complete_sql_source_stage(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        source: &SemanticSqlSourceRecord,
        authorized_at: &str,
        successor: Option<&SemanticStageIntent>,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        let binding = self.store.read_binding()?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "SQL source completion has no durable semantic binding".to_string(),
            )
        })?;
        let artifact = sql_source_stage_artifact(&binding, transition, source, authorized_at)
            .map_err(|error| {
                SemanticCodeError::Refused(format!(
                    "semantic SQL source completion rejected: {error:?}"
                ))
            })?;
        self.store
            .complete_stage(lease, transition, &artifact, successor, now_ms)
    }

    /// Replay a previously committed SQL S1 completion before the caller
    /// re-reads the authoritative source or ACL.  The store owns the durable
    /// mutation and lease lookup: it only acknowledges a current, fenced
    /// lease when the persisted `RecordStageTransition` and SQL manifest
    /// artifact match `expected_intent`, then returns the durable transition
    /// and ledger receipt.  The cursor and completion time are read from the
    /// retained transition, so a restarted caller does not reconstruct them.
    /// `None` means the S1 mutation is not committed yet and the caller may
    /// proceed through [`Self::complete_sql_source_stage`].
    pub fn replay_completed_sql_source_stage(
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
        self.store
            .replay_completed_sql_source_stage(lease, expected_intent, now_ms)
    }

    /// Complete S3 or S5 and persist its canonical lexical/ANN manifest in
    /// the same owner mutation as the transition, checkpoint, successor and
    /// lease acknowledgement.
    pub fn complete_generation_stage(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        artifact: &SemanticGenerationArtifact,
        successor: Option<&SemanticStageIntent>,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        self.store
            .complete_generation_stage(lease, transition, artifact, successor, now_ms)
    }

    pub fn release_stage_lease(
        &self,
        lease: &MutationOutboxLease,
    ) -> Result<(), SemanticCodeError> {
        self.store.release_stage_lease(lease)
    }

    /// The sole semantic publication entry point. S6 must be a completed
    /// generation-scoped transition whose activation target/pointer and both
    /// lexical/ANN checkpoints validate before the durable code tier is made
    /// live. The lower-level code writer remains an implementation primitive;
    /// request and consumer routes call this method instead.
    pub fn activate_s6(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        checkpoint: &SemanticGenerationCheckpoint,
        artifact: &SemanticGenerationArtifact,
        image: &SemanticGenerationImage,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        if transition.intent.stage != SemanticStage::ReconcileAndActivate
            || !matches!(transition.intent.scope, SemanticStageScope::Generation)
        {
            return Err(SemanticCodeError::Refused(
                "ANN publication requires a generation-scoped S6 transition".to_string(),
            ));
        }
        let SemanticGenerationArtifact::Activation { target, pointer } = artifact else {
            return Err(SemanticCodeError::Refused(
                "S6 publication requires the canonical activation artifact".to_string(),
            ));
        };
        target.validate().map_err(|error| {
            SemanticCodeError::Refused(format!("activation target rejected: {error:?}"))
        })?;
        pointer.validate().map_err(|error| {
            SemanticCodeError::Refused(format!("active pointer rejected: {error:?}"))
        })?;
        let receipt = self
            .store
            .finalize_generation(lease, transition, checkpoint, artifact, image, now_ms)?;
        Ok(receipt)
    }
}
