//! Stage intents and the stage outbox: S1 admission, lease parsing, the
//! outbox consumer port and successor-intent validation.

use super::batch::MetadataMutation;
use super::reconciliation::compare_source_revision;
use super::{
    kernel_error, semantic_contract_error, SemanticCodeError, SemanticCodeStore,
    SemanticMutationReceipt, SEMANTIC_STAGE_INTENT_TOPIC,
};
use eg_storage::SEMANTIC_SOURCE_PROGRESS;
use eg_transaction::{OutboxClaimBudget, OutboxClaimOutcome};
use eg_types::mutation_batch::{MutationOutboxIntent, MutationOutboxLease};
use eg_types::semantic_index::{
    SemanticBindingState, SemanticDigest, SemanticSourceProgress, SemanticStage,
    SemanticStageIntent, SemanticStageOutcome, SemanticStagePredecessor, SemanticStageTransition,
};
use std::collections::BTreeMap;

impl SemanticCodeStore {
    /// Admit an S1 source intent and persist its source cursor atomically with
    /// the claimable outbox row.  Only S1 is accepted here; S2-S6 are produced
    /// by the existing durable consumer/executor ports after their predecessor
    /// receipts are present.  In particular, this method never activates an
    /// ANN generation.
    pub fn enqueue_stage_intent(
        &self,
        intent: &SemanticStageIntent,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        intent.validate().map_err(semantic_contract_error)?;
        if intent.binding_id != self.binding || intent.generation == 0 {
            return Err(SemanticCodeError::Refused(
                "semantic stage intent is outside this binding".to_string(),
            ));
        }
        if intent.stage != SemanticStage::SourceCommit
            || !matches!(&intent.predecessor, SemanticStagePredecessor::None)
        {
            return Err(SemanticCodeError::Refused(
                "native semantic admission accepts only an S1 intent with no predecessor"
                    .to_string(),
            ));
        }
        let source_entity_id = intent.scope.source_entity_id().ok_or_else(|| {
            SemanticCodeError::Refused("S1 semantic intent must name one source entity".to_string())
        })?;
        let payload = intent
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        // The canonical intent digest covers every identity field, including
        // delimiters and predecessor data.  A concatenated transport key could
        // alias two otherwise distinct intents when a caller supplied a
        // delimiter-containing component.
        let key = stage_intent_key(intent);
        let mut headers = BTreeMap::new();
        headers.insert(
            "schema".to_string(),
            eg_types::semantic_index::SEMANTIC_STAGE_INTENT_SCHEMA.to_string(),
        );
        headers.insert("binding_id".to_string(), intent.binding_id.clone());
        headers.insert(
            "binding_digest".to_string(),
            intent.binding_digest.to_string(),
        );
        headers.insert("generation".to_string(), intent.generation.to_string());
        headers.insert("source_entity_id".to_string(), source_entity_id.to_string());
        headers.insert(
            "source_revision".to_string(),
            intent.source_revision.clone(),
        );
        headers.insert("stage".to_string(), intent.stage.as_str().to_string());
        headers.insert(
            "intent_digest".to_string(),
            intent.intent_digest.to_string(),
        );
        let outbox = MutationOutboxIntent {
            topic: SEMANTIC_STAGE_INTENT_TOPIC.to_string(),
            key: key.clone(),
            payload,
            headers,
        };
        let batch_id = format!("semantic-index:intent:{}", intent.intent_digest);
        let owner = self.door.owner();
        let intent_digest = intent.intent_digest;
        self.door.commit_metadata(
            |version| {
                self.metadata_batch(
                         owner,
                         version,
                         MetadataMutation {
                             batch_id: &batch_id,
                             event_type: "semantic_stage_intent_enqueued",
                             subject: &format!("intent:{}", intent.intent_digest),
                             mutation_digest: intent_digest,
                         },
                         vec![outbox],
                         now_ms,
                     )
            },
            intent_digest,
            now_ms,
            |write, rows| {
                let binding = self.read_binding_in_write(write)?.ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "semantic stage intent names no durable binding".to_string(),
                    )
                })?;
                if binding.binding_digest != intent.binding_digest
                    || binding.generation != intent.generation
                {
                    return Err(SemanticCodeError::Refused(
                        "semantic stage intent binding digest or generation is stale".to_string(),
                    ));
                }
                let old = write
                    .open_read_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?
                    .get((
                        self.tenant.as_str(),
                        self.binding.as_str(),
                        intent.generation,
                        source_entity_id,
                    ))
                    .map_err(kernel_error)?
                    .map(|value| value.value().to_vec());
                if let Some(old) = old {
                    let old = SemanticSourceProgress::from_canonical_cbor(&old)
                        .map_err(semantic_contract_error)?;
                    if old.source_revision == intent.source_revision {
                        return Err(SemanticCodeError::Refused(
                            "semantic source revision is already admitted; retry the same intent"
                                .to_string(),
                        ));
                    }
                    if compare_source_revision(&intent.source_revision, &old.source_revision)
                        != std::cmp::Ordering::Greater
                    {
                        return Err(SemanticCodeError::Refused(
                            "semantic source revision is stale or not strictly newer".to_string(),
                        ));
                    }
                    if old.completed_stage.is_none() {
                        return Err(SemanticCodeError::Refused(
                            "semantic source revision has no completed predecessor; retain the row and refresh the binding generation"
                                .to_string(),
                        ));
                    }
                    return Err(SemanticCodeError::Refused(
                        "semantic source revision supersession requires a replacement binding generation"
                            .to_string(),
                    ));
                }
                let progress = SemanticSourceProgress {
                    binding_id: intent.binding_id.clone(),
                    binding_digest: intent.binding_digest,
                    generation: intent.generation,
                    source_entity_id: source_entity_id.to_string(),
                    source_revision: intent.source_revision.clone(),
                    completed_stage: None,
                    completed_receipt_digest: None,
                    superseded_by_revision: None,
                    updated_at: format!("unix-ms:{now_ms}"),
                };
                progress.validate().map_err(semantic_contract_error)?;
                let bytes = progress
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                rows.open_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?
                    .insert(
                        (
                            self.tenant.as_str(),
                            self.binding.as_str(),
                            intent.generation,
                            source_entity_id,
                        ),
                        bytes.as_slice(),
                    )
                    .map_err(kernel_error)?;
                Ok(())
            },
        )
    }

    /// Validate a leased stage row before a consumer may run work. The
    /// durable lease is the executor's fencing proof and the current binding
    /// must already be durably `Building`; the canonical payload, key, headers,
    /// owner identity, attempt and expiry are all checked before any stage
    /// transition can be constructed. Actual S2-S6 execution and ack/receipt
    /// persistence remain on the existing outbox consumer port.
    pub fn validate_stage_lease(
        &self,
        lease: &MutationOutboxLease,
        consumer: &str,
        now_ms: u64,
    ) -> Result<SemanticStageIntent, SemanticCodeError> {
        let intent = self.parse_stage_lease(lease, consumer, now_ms)?;
        let read = self.door.serving_read()?;
        self.validate_stage_predecessor_in(&read, &intent, true)?;
        Ok(intent)
    }

    /// Parse and fence the transport-visible lease fields. This helper is
    /// deliberately separate from predecessor validation so a retry can find
    /// an already-recorded transition and acknowledge a newly reclaimed lease
    /// without treating a completed stage as fresh work.
    pub(super) fn parse_stage_lease(
        &self,
        lease: &MutationOutboxLease,
        consumer: &str,
        now_ms: u64,
    ) -> Result<SemanticStageIntent, SemanticCodeError> {
        lease.record.validate().map_err(|error| {
            SemanticCodeError::Refused(format!("invalid semantic lease record: {error}"))
        })?;
        if consumer.trim().is_empty() || lease.consumer != consumer {
            return Err(SemanticCodeError::Refused(
                "semantic stage lease consumer does not match".to_string(),
            ));
        }
        if lease.lease_epoch == 0 || lease.attempt == 0 || lease.lease_until_ms <= now_ms {
            return Err(SemanticCodeError::Refused(
                "semantic stage lease is absent, unissued, or expired".to_string(),
            ));
        }
        if lease.record.identity != *self.door.owner().identity()
            || lease.record.intent.topic != SEMANTIC_STAGE_INTENT_TOPIC
        {
            return Err(SemanticCodeError::Refused(
                "semantic stage lease is outside this serving owner".to_string(),
            ));
        }
        let intent = SemanticStageIntent::from_canonical_cbor(&lease.record.intent.payload)
            .map_err(semantic_contract_error)?;
        if intent.binding_id != self.binding {
            return Err(SemanticCodeError::Refused(
                "semantic stage lease names another binding".to_string(),
            ));
        }
        let scope_key = match &intent.scope {
            eg_types::semantic_index::SemanticStageScope::Entity { source_entity_id } => {
                source_entity_id.clone()
            }
            eg_types::semantic_index::SemanticStageScope::Generation => "generation".to_string(),
        };
        let expected_key = stage_intent_key(&intent);
        if lease.record.intent.key != expected_key {
            return Err(SemanticCodeError::Refused(
                "semantic stage lease key does not match its canonical intent".to_string(),
            ));
        }
        let headers = &lease.record.intent.headers;
        for (name, expected) in [
            (
                "schema",
                eg_types::semantic_index::SEMANTIC_STAGE_INTENT_SCHEMA.to_string(),
            ),
            ("binding_id", intent.binding_id.clone()),
            ("binding_digest", intent.binding_digest.to_string()),
            ("generation", intent.generation.to_string()),
            ("source_entity_id", scope_key),
            ("source_revision", intent.source_revision.clone()),
            ("stage", intent.stage.as_str().to_string()),
            ("intent_digest", intent.intent_digest.to_string()),
        ] {
            if headers.get(name).map(String::as_str) != Some(expected.as_str()) {
                return Err(SemanticCodeError::Refused(format!(
                    "semantic stage lease header {name} does not match its canonical intent"
                )));
            }
        }
        let read = self.door.serving_read()?;
        let binding = self.read_binding_in(&read)?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "semantic stage lease has no durable binding authority".to_string(),
            )
        })?;
        if binding.binding_digest != intent.binding_digest
            || binding.generation != intent.generation
        {
            return Err(SemanticCodeError::Refused(
                "semantic stage lease binding generation proof is stale".to_string(),
            ));
        }
        if binding.durable_state != SemanticBindingState::Building {
            return Err(SemanticCodeError::Refused(
                "semantic stage lease requires a durable Building binding state".to_string(),
            ));
        }
        Ok(intent)
    }

    /// Install the one durable subscription used by semantic executors.  The
    /// outbox remains the shared queue; this method only declares the topic on
    /// this owner scope and is idempotent for the same consumer/topic pair.
    pub(crate) fn subscribe_stage_consumer(&self, consumer: &str) -> Result<(), SemanticCodeError> {
        self.door
            .outbox_subscribe(consumer, SEMANTIC_STAGE_INTENT_TOPIC)
            .map_err(kernel_error)
    }

    /// Bounded, non-blocking claim port for the existing durable outbox.  A
    /// scheduler owns the budget and tenant order; this adapter never loops or
    /// creates an in-memory queue.
    pub(crate) fn claim_stage_leases(
        &self,
        consumer: &str,
        budget: &mut OutboxClaimBudget,
    ) -> Result<OutboxClaimOutcome, SemanticCodeError> {
        if consumer.trim().is_empty() || budget.limit() > 256 {
            return Err(SemanticCodeError::Refused(
                "semantic stage claim is empty or exceeds the bounded consumer budget".to_string(),
            ));
        }
        let read = self.door.serving_read()?;
        let binding = self.read_binding_in(&read)?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "semantic stage claim has no durable binding authority".to_string(),
            )
        })?;
        if binding.durable_state != SemanticBindingState::Building {
            return Err(SemanticCodeError::Refused(
                "semantic stage claim requires a durable Building binding state".to_string(),
            ));
        }
        self.door
            .outbox_claim(consumer, budget)
            .map_err(kernel_error)
    }

    pub(crate) fn stage_status(
        &self,
        consumer: &str,
        now_ms: u64,
    ) -> Result<eg_transaction::OutboxStatus, SemanticCodeError> {
        let read = self.door.serving_read()?;
        eg_transaction::outbox_status(&read, consumer, now_ms).map_err(kernel_error)
    }

    /// Acknowledge only a semantic stage topic lease.  The transaction kernel
    /// rechecks the durable epoch/expiry/record and advances the watermark in
    /// the same delivery transaction; fabricated or reclaimed leases therefore
    /// cannot be accepted by this port.
    pub(crate) fn ack_stage_lease(
        &self,
        lease: &MutationOutboxLease,
        now_ms: u64,
    ) -> Result<(), SemanticCodeError> {
        let _ = self.parse_stage_lease(lease, &lease.consumer, now_ms)?;
        self.door
            .outbox_ack(lease, now_ms)
            .map(|_| ())
            .map_err(kernel_error)
    }

    pub(crate) fn release_stage_lease(
        &self,
        lease: &MutationOutboxLease,
    ) -> Result<(), SemanticCodeError> {
        if lease.record.identity != *self.door.owner().identity()
            || lease.record.intent.topic != SEMANTIC_STAGE_INTENT_TOPIC
        {
            return Err(SemanticCodeError::Refused(
                "semantic stage release is outside this serving owner".to_string(),
            ));
        }
        self.door.outbox_release(lease).map_err(kernel_error)
    }

    pub(super) fn stage_receipt(
        &self,
        transition: &SemanticStageTransition,
        replayed: bool,
    ) -> Result<SemanticMutationReceipt, String> {
        let batch_id = format!("semantic-index:stage:{}", transition.intent.intent_digest);
        self.door.stage_receipt_for_batch(&batch_id, replayed)
    }
}

fn stage_scope_key(intent: &SemanticStageIntent) -> String {
    match &intent.scope {
        eg_types::semantic_index::SemanticStageScope::Entity { source_entity_id } => {
            source_entity_id.clone()
        }
        eg_types::semantic_index::SemanticStageScope::Generation => "generation".to_string(),
    }
}

pub(super) fn stage_intent_key(intent: &SemanticStageIntent) -> String {
    format!("semantic-stage-intent:{}", intent.intent_digest)
}

/// Secondary lookup key for a completed receipt. It lives in the canonical
/// `SEMANTIC_STAGES` table and stores the exact same mutation bytes as the
/// intent row, so refresh proof is a bounded direct read without introducing a
/// second stage authority.
pub(super) fn stage_receipt_index_key(receipt_digest: SemanticDigest) -> String {
    format!("semantic-stage-receipt:{}", receipt_digest)
}

pub(super) fn is_stage_receipt_index_key(key: &str) -> bool {
    key.starts_with("semantic-stage-receipt:")
}

pub(super) fn stage_intent_outbox(
    intent: &SemanticStageIntent,
) -> Result<MutationOutboxIntent, SemanticCodeError> {
    let payload = intent
        .to_canonical_cbor()
        .map_err(semantic_contract_error)?;
    let mut headers = BTreeMap::new();
    headers.insert(
        "schema".to_string(),
        eg_types::semantic_index::SEMANTIC_STAGE_INTENT_SCHEMA.to_string(),
    );
    headers.insert("binding_id".to_string(), intent.binding_id.clone());
    headers.insert(
        "binding_digest".to_string(),
        intent.binding_digest.to_string(),
    );
    headers.insert("generation".to_string(), intent.generation.to_string());
    headers.insert("source_entity_id".to_string(), stage_scope_key(intent));
    headers.insert(
        "source_revision".to_string(),
        intent.source_revision.clone(),
    );
    headers.insert("stage".to_string(), intent.stage.as_str().to_string());
    headers.insert(
        "intent_digest".to_string(),
        intent.intent_digest.to_string(),
    );
    Ok(MutationOutboxIntent {
        topic: SEMANTIC_STAGE_INTENT_TOPIC.to_string(),
        key: stage_intent_key(intent),
        payload,
        headers,
    })
}

pub(super) fn stage_receipt_headers(
    transition: &SemanticStageTransition,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "schema".to_string(),
            eg_types::semantic_index::SEMANTIC_STAGE_TRANSITION_SCHEMA.to_string(),
        ),
        (
            "binding_id".to_string(),
            transition.intent.binding_id.clone(),
        ),
        (
            "binding_digest".to_string(),
            transition.intent.binding_digest.to_string(),
        ),
        (
            "generation".to_string(),
            transition.intent.generation.to_string(),
        ),
        (
            "stage".to_string(),
            transition.intent.stage.as_str().to_string(),
        ),
        (
            "intent_digest".to_string(),
            transition.intent.intent_digest.to_string(),
        ),
        (
            "receipt_digest".to_string(),
            transition.receipt.receipt_digest().to_string(),
        ),
    ])
}

pub(super) fn validate_successor_intent(
    transition: &SemanticStageTransition,
    successor: Option<&SemanticStageIntent>,
) -> Result<Option<SemanticStageIntent>, SemanticCodeError> {
    if !matches!(
        transition.receipt.outcome,
        SemanticStageOutcome::Completed | SemanticStageOutcome::IdempotentNoop
    ) {
        if successor.is_some() {
            return Err(SemanticCodeError::Refused(
                "non-terminal semantic stage outcome cannot publish a successor".to_string(),
            ));
        }
        return Ok(None);
    }
    let derived = match transition.intent.stage {
        SemanticStage::SourceCommit => Some(SemanticStageIntent::create(
            eg_types::semantic_index::SemanticStageIntentDraft {
                binding_id: transition.intent.binding_id.clone(),
                binding_digest: transition.intent.binding_digest,
                generation: transition.intent.generation,
                scope: transition.intent.scope.clone(),
                source_revision: transition.intent.source_revision.clone(),
                stage: SemanticStage::GraphProjection,
                predecessor: SemanticStagePredecessor::EntityReceipt {
                    stage: SemanticStage::SourceCommit,
                    receipt_digest: transition.receipt.receipt_digest(),
                },
                input_digest: transition.receipt.output_digest,
            },
        )),
        SemanticStage::GraphProjection => Some(SemanticStageIntent::create(
            eg_types::semantic_index::SemanticStageIntentDraft {
                binding_id: transition.intent.binding_id.clone(),
                binding_digest: transition.intent.binding_digest,
                generation: transition.intent.generation,
                scope: transition.intent.scope.clone(),
                source_revision: transition.intent.source_revision.clone(),
                stage: SemanticStage::LexicalIndex,
                predecessor: SemanticStagePredecessor::EntityReceipt {
                    stage: SemanticStage::GraphProjection,
                    receipt_digest: transition.receipt.receipt_digest(),
                },
                input_digest: transition.receipt.output_digest,
            },
        )),
        SemanticStage::LexicalIndex => {
            let checkpoint = transition.generation_checkpoint.as_ref().ok_or_else(|| {
                SemanticCodeError::Refused(
                    "completed S3 transition has no lexical checkpoint".to_string(),
                )
            })?;
            Some(SemanticStageIntent::create(
                eg_types::semantic_index::SemanticStageIntentDraft {
                    binding_id: transition.intent.binding_id.clone(),
                    binding_digest: transition.intent.binding_digest,
                    generation: transition.intent.generation,
                    scope: transition.intent.scope.clone(),
                    source_revision: transition.intent.source_revision.clone(),
                    stage: SemanticStage::Vector,
                    predecessor: SemanticStagePredecessor::GenerationCheckpoint {
                        stage: SemanticStage::LexicalIndex,
                        checkpoint_digest: checkpoint.successor.checkpoint_digest,
                    },
                    input_digest: transition.receipt.output_digest,
                },
            ))
        }
        SemanticStage::Vector => None,
        SemanticStage::AnnIndex => None,
        SemanticStage::ReconcileAndActivate => None,
    }
    .transpose()
    .map_err(|error| SemanticCodeError::Refused(format!("successor intent rejected: {error:?}")))?;
    let Some(successor) = successor else {
        if matches!(
            transition.intent.stage,
            SemanticStage::Vector | SemanticStage::AnnIndex
        ) {
            return Err(SemanticCodeError::Refused(
                "completed S4/S5 transition requires an explicit successor proof".to_string(),
            ));
        }
        return Ok(derived);
    };
    successor.validate().map_err(semantic_contract_error)?;
    if successor.binding_id != transition.intent.binding_id
        || successor.binding_digest != transition.intent.binding_digest
        || successor.generation != transition.intent.generation
        || successor.source_revision != transition.intent.source_revision
        || successor.input_digest != transition.receipt.output_digest
    {
        return Err(SemanticCodeError::Refused(
            "successor intent coordinates or predecessor input are stale".to_string(),
        ));
    }
    let expected = derived.as_ref();
    match transition.intent.stage {
        SemanticStage::SourceCommit
        | SemanticStage::GraphProjection
        | SemanticStage::LexicalIndex => {
            if expected != Some(successor) {
                return Err(SemanticCodeError::Refused(
                    "successor intent does not carry the exact stage receipt proof".to_string(),
                ));
            }
        }
        SemanticStage::Vector => {
            let complete = match &successor.predecessor {
                SemanticStagePredecessor::GenerationCoverage { checkpoint } => {
                    checkpoint.require_complete().is_ok()
                        && checkpoint.binding_id == successor.binding_id
                        && checkpoint.binding_digest == successor.binding_digest
                        && checkpoint.generation == successor.generation
                        && checkpoint.source_revision == successor.source_revision
                }
                _ => false,
            };
            if successor.stage != SemanticStage::AnnIndex || !complete {
                return Err(SemanticCodeError::Refused(
                    "S4 successor must be a complete S5 generation-coverage intent".to_string(),
                ));
            }
        }
        SemanticStage::AnnIndex => {
            if successor.stage != SemanticStage::ReconcileAndActivate
                || !matches!(
                    successor.scope,
                    eg_types::semantic_index::SemanticStageScope::Generation
                )
                || !matches!(
                    successor.predecessor,
                    SemanticStagePredecessor::Activation { .. }
                )
            {
                return Err(SemanticCodeError::Refused(
                    "S5 successor must be the generation-scoped S6 activation intent".to_string(),
                ));
            }
        }
        SemanticStage::ReconcileAndActivate => {
            return Err(SemanticCodeError::Refused(
                "S6 is terminal and cannot publish another stage".to_string(),
            ));
        }
    }
    Ok(Some(successor.clone()))
}
