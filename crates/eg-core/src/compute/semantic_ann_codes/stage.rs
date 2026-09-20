//! Stage intents and the stage outbox: S1 admission, lease parsing, the
//! outbox consumer port and successor-intent validation.

use super::batch::MetadataMutation;
use super::reconciliation::compare_source_revision;
use super::record::{decode, encode, encode_valid, row};
use super::{
    ensure, kernel_error, refused, semantic_contract_error, SemanticCodeError, SemanticCodeStore,
    SemanticMutationReceipt, SEMANTIC_STAGE_INTENT_TOPIC, SEMANTIC_STAGE_RECEIPT_TOPIC,
};
use eg_storage::{SemanticIndexOwner, SEMANTIC_SOURCE_PROGRESS};
use eg_transaction::{AdmittedMutation, OutboxClaimBudget, OutboxClaimOutcome};
use eg_types::mutation_batch::{MutationOutboxIntent, MutationOutboxLease};
use eg_types::semantic_index::{
    SemanticBindingState, SemanticDigest, SemanticIndexMutation, SemanticSourceProgress,
    SemanticStage, SemanticStageIntent, SemanticStageIntentDraft, SemanticStageOutcome,
    SemanticStagePredecessor, SemanticStageScope, SemanticStageTransition,
};
use eg_types::MutationScopeIdentity;
use std::cmp::Ordering;
use std::collections::BTreeMap;

/// A completed transition's intent coordinates, as a retained receipt proves
/// them: binding, digest, generation, source revision, stage and entity.
pub(super) type CompletionCoordinates<'a> = (
    &'a str,
    SemanticDigest,
    u64,
    &'a str,
    SemanticStage,
    Option<&'a str>,
);

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
        ensure(
            intent.binding_id == self.binding && intent.generation != 0,
            "semantic stage intent is outside this binding",
        )?;
        ensure(
            intent.stage == SemanticStage::SourceCommit
                && matches!(&intent.predecessor, SemanticStagePredecessor::None),
            "native semantic admission accepts only an S1 intent with no predecessor",
        )?;
        let source_entity_id = intent
            .scope
            .source_entity_id()
            .ok_or_else(|| refused("S1 semantic intent must name one source entity"))?;
        // The canonical intent digest covers every identity field, including
        // delimiters and predecessor data.  A concatenated transport key could
        // alias two otherwise distinct intents when a caller supplied a
        // delimiter-containing component.
        let outbox = stage_intent_outbox(intent)?;
        let batch_id = format!("semantic-index:intent:{}", intent.intent_digest);
        let subject = format!("intent:{}", intent.intent_digest);
        self.commit_maintenance(
            MetadataMutation {
                batch_id: &batch_id,
                event_type: "semantic_stage_intent_enqueued",
                subject: &subject,
                mutation_digest: intent.intent_digest,
            },
            vec![outbox],
            now_ms,
            None,
            |write, rows| {
                self.ensure_first_source_revision(write, intent, source_entity_id)?;
                let progress =
                    encode_valid(&fresh_source_progress(intent, source_entity_id, now_ms))?;
                rows.open_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?
                    .insert(
                        (
                            self.tenant.as_str(),
                            self.binding.as_str(),
                            intent.generation,
                            source_entity_id,
                        ),
                        progress.as_slice(),
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
        self.fence_lease_envelope(lease, consumer, now_ms)?;
        let intent: SemanticStageIntent = decode(&lease.record.intent.payload)?;
        ensure(
            intent.binding_id == self.binding,
            "semantic stage lease names another binding",
        )?;
        ensure_canonical_intent_event(&lease.record.intent, &intent)?;
        self.ensure_building_binding_for(&intent)?;
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
        ensure(
            !consumer.trim().is_empty() && budget.limit() <= 256,
            "semantic stage claim is empty or exceeds the bounded consumer budget",
        )?;
        let read = self.door.serving_read()?;
        let binding = self
            .read_binding_in(&read)?
            .ok_or_else(|| refused("semantic stage claim has no durable binding authority"))?;
        ensure(
            binding.durable_state == SemanticBindingState::Building,
            "semantic stage claim requires a durable Building binding state",
        )?;
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
        ensure_stage_topic_owner(
            lease,
            self.door.owner().identity(),
            "semantic stage release is outside this serving owner",
        )?;
        self.door.outbox_release(lease).map_err(kernel_error)
    }

    /// S1 admission starts an entity's progress: any existing row for the
    /// generation is either this revision again, a stale one, or a supersession
    /// that belongs to a replacement binding generation.
    fn ensure_first_source_revision(
        &self,
        write: &AdmittedMutation<'_, SemanticIndexOwner>,
        intent: &SemanticStageIntent,
        source_entity_id: &str,
    ) -> Result<(), SemanticCodeError> {
        let binding = self
            .read_binding_in_write(write)?
            .ok_or_else(|| refused("semantic stage intent names no durable binding"))?;
        ensure(
            (binding.binding_digest, binding.generation)
                == (intent.binding_digest, intent.generation),
            "semantic stage intent binding digest or generation is stale",
        )?;
        let progress_rows = write
            .open_read_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?;
        let key = (
            self.tenant.as_str(),
            self.binding.as_str(),
            intent.generation,
            source_entity_id,
        );
        let old: Option<SemanticSourceProgress> = row(progress_rows.get(key))?;
        match old {
            None => Ok(()),
            Some(old) => Err(refused(existing_progress_refusal(&old, intent))),
        }
    }

    /// The lease envelope: a valid record for this consumer, an issued and
    /// unexpired lease, and this owner's stage topic. "Issued" is proven by
    /// `lease_epoch != 0` alone (see EH-315 below) — `attempt` is a per-head
    /// fairness counter, not an issuance signal, and is not checked here.
    fn fence_lease_envelope(
        &self,
        lease: &MutationOutboxLease,
        consumer: &str,
        now_ms: u64,
    ) -> Result<(), SemanticCodeError> {
        lease.record.validate().map_err(|error| {
            SemanticCodeError::Refused(format!("invalid semantic lease record: {error}"))
        })?;
        ensure(
            !consumer.trim().is_empty() && lease.consumer == consumer,
            "semantic stage lease consumer does not match",
        )?;
        // EH-315: this used to be one `ensure` over three ANDed conditions
        // (`lease_epoch != 0 && attempt != 0 && lease_until_ms > now_ms`) with
        // one message ("...is absent, unissued, or expired") that could not
        // say which had actually fired. A real R820 failure (`lease_epoch: 1,
        // attempt: 0, lease_until_ms: 5004` at `now_ms: 3`) needed manual
        // arithmetic against the Debug-printed lease to work out that only
        // the middle condition (`attempt != 0`) was false — exactly the class
        // of defect EH-331's guard hit: an error that collapses distinct
        // cases into one string.
        //
        // Splitting it (first pass of this fix) surfaced that the middle
        // condition was not just under-explained but WRONG. `lease_epoch`
        // (`eg-transaction`'s `next_lease`, outbox/claim.rs) increments
        // UNCONDITIONALLY on every claim, so `lease_epoch != 0` is already
        // complete proof this lease came from `claim_stage_leases` and was
        // never hand-fabricated. `attempt` is a different thing: per X10-R1
        // (`install_leases`/`head_only_attempt`, same file), it only advances
        // for the HEAD row of a scope's ordering queue — every OTHER row a
        // single claim call returns in the same pass is a legitimate
        // successor whose `attempt` is deliberately left at 0 until it
        // becomes the head. `claim_stage_leases` can and does return more
        // than one lease per call (this store's own `OutboxClaimBudget`
        // comment: budget is `4 * row_count` so `consecutive_cap()` admits
        // the whole batch) — so requiring `attempt != 0` here rejected every
        // non-head lease in a legitimately claimed batch, which is exactly
        // what `complete_source_stages` does (claim once, validate each).
        // `attempt` is never checked again after this: nothing downstream of
        // `validate_stage_lease` reads it, so dropping the requirement adds
        // no way to bypass this gate — it only stops the gate from bypassing
        // a real, correctly-issued lease.
        ensure(
            lease.lease_epoch != 0,
            &format!(
                "semantic stage lease is ABSENT: lease_epoch=0 for consumer \
                 {consumer:?} — no lease has ever been issued for this outbox row"
            ),
        )?;
        ensure(
            lease.lease_until_ms > now_ms,
            &format!(
                "semantic stage lease has EXPIRED: lease_until_ms={} <= \
                 now_ms={now_ms} for consumer {consumer:?}",
                lease.lease_until_ms
            ),
        )?;
        ensure_stage_topic_owner(
            lease,
            self.door.owner().identity(),
            "semantic stage lease is outside this serving owner",
        )
    }

    /// A leased intent may only run while its binding generation is durably
    /// `Building`.
    fn ensure_building_binding_for(
        &self,
        intent: &SemanticStageIntent,
    ) -> Result<(), SemanticCodeError> {
        let read = self.door.serving_read()?;
        let binding = self
            .read_binding_in(&read)?
            .ok_or_else(|| refused("semantic stage lease has no durable binding authority"))?;
        ensure(
            (binding.binding_digest, binding.generation)
                == (intent.binding_digest, intent.generation),
            "semantic stage lease binding generation proof is stale",
        )?;
        ensure(
            binding.durable_state == SemanticBindingState::Building,
            "semantic stage lease requires a durable Building binding state",
        )
    }
}

/// Why an S1 admission is refused when its entity already has progress.
fn existing_progress_refusal(
    old: &SemanticSourceProgress,
    intent: &SemanticStageIntent,
) -> &'static str {
    if old.source_revision == intent.source_revision {
        "semantic source revision is already admitted; retry the same intent"
    } else if compare_source_revision(&intent.source_revision, &old.source_revision)
        != Ordering::Greater
    {
        "semantic source revision is stale or not strictly newer"
    } else if old.completed_stage.is_none() {
        "semantic source revision has no completed predecessor; retain the row and refresh the binding generation"
    } else {
        "semantic source revision supersession requires a replacement binding generation"
    }
}

fn ensure_stage_topic_owner(
    lease: &MutationOutboxLease,
    owner: &MutationScopeIdentity,
    message: &str,
) -> Result<(), SemanticCodeError> {
    ensure(
        lease.record.identity == *owner && lease.record.intent.topic == SEMANTIC_STAGE_INTENT_TOPIC,
        message,
    )
}

/// A leased event must carry exactly the key and headers its canonical intent
/// derives, checked in the canonical header order.
fn ensure_canonical_intent_event(
    event: &MutationOutboxIntent,
    intent: &SemanticStageIntent,
) -> Result<(), SemanticCodeError> {
    ensure(
        event.key == stage_intent_key(intent),
        "semantic stage lease key does not match its canonical intent",
    )?;
    for (name, expected) in intent_headers(intent) {
        if event.headers.get(name).map(String::as_str) != Some(expected.as_str()) {
            return Err(SemanticCodeError::Refused(format!(
                "semantic stage lease header {name} does not match its canonical intent"
            )));
        }
    }
    Ok(())
}

fn stage_scope_key(intent: &SemanticStageIntent) -> String {
    match &intent.scope {
        SemanticStageScope::Entity { source_entity_id } => source_entity_id.clone(),
        SemanticStageScope::Generation => "generation".to_string(),
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

/// The canonical headers of a stage-intent event, in the order a lease check
/// reports the first mismatch.
fn intent_headers(intent: &SemanticStageIntent) -> [(&'static str, String); 8] {
    [
        (
            "schema",
            eg_types::semantic_index::SEMANTIC_STAGE_INTENT_SCHEMA.to_string(),
        ),
        ("binding_id", intent.binding_id.clone()),
        ("binding_digest", intent.binding_digest.to_string()),
        ("generation", intent.generation.to_string()),
        ("source_entity_id", stage_scope_key(intent)),
        ("source_revision", intent.source_revision.clone()),
        ("stage", intent.stage.as_str().to_string()),
        ("intent_digest", intent.intent_digest.to_string()),
    ]
}

pub(super) fn stage_intent_outbox(
    intent: &SemanticStageIntent,
) -> Result<MutationOutboxIntent, SemanticCodeError> {
    Ok(MutationOutboxIntent {
        topic: SEMANTIC_STAGE_INTENT_TOPIC.to_string(),
        key: stage_intent_key(intent),
        payload: encode(intent)?,
        headers: intent_headers(intent)
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect(),
    })
}

pub(super) fn stage_receipt_headers(
    transition: &SemanticStageTransition,
) -> BTreeMap<String, String> {
    let intent = &transition.intent;
    BTreeMap::from([
        (
            "schema".to_string(),
            eg_types::semantic_index::SEMANTIC_STAGE_TRANSITION_SCHEMA.to_string(),
        ),
        ("binding_id".to_string(), intent.binding_id.clone()),
        (
            "binding_digest".to_string(),
            intent.binding_digest.to_string(),
        ),
        ("generation".to_string(), intent.generation.to_string()),
        ("stage".to_string(), intent.stage.as_str().to_string()),
        (
            "intent_digest".to_string(),
            intent.intent_digest.to_string(),
        ),
        (
            "receipt_digest".to_string(),
            transition.receipt.receipt_digest().to_string(),
        ),
    ])
}

/// The receipt event every recorded stage transition publishes.
pub(super) fn stage_receipt_event(
    transition: &SemanticStageTransition,
) -> Result<MutationOutboxIntent, SemanticCodeError> {
    Ok(MutationOutboxIntent {
        topic: SEMANTIC_STAGE_RECEIPT_TOPIC.to_string(),
        key: transition.intent.intent_digest.to_string(),
        payload: encode(transition)?,
        headers: stage_receipt_headers(transition),
    })
}

/// `Completed` and `IdempotentNoop` are the terminal outcomes that advance an
/// entity's progress and may publish a successor.
pub(super) fn is_terminal(outcome: SemanticStageOutcome) -> bool {
    matches!(
        outcome,
        SemanticStageOutcome::Completed | SemanticStageOutcome::IdempotentNoop
    )
}

/// The progress row an admitted S1 intent starts for its entity.
pub(super) fn fresh_source_progress(
    intent: &SemanticStageIntent,
    source_entity_id: &str,
    now_ms: u64,
) -> SemanticSourceProgress {
    SemanticSourceProgress {
        binding_id: intent.binding_id.clone(),
        binding_digest: intent.binding_digest,
        generation: intent.generation,
        source_entity_id: source_entity_id.to_string(),
        source_revision: intent.source_revision.clone(),
        completed_stage: None,
        completed_receipt_digest: None,
        superseded_by_revision: None,
        updated_at: format!("unix-ms:{now_ms}"),
    }
}

/// The terminal stage transition a retained stage row records for
/// `receipt_digest`, if it is one.
pub(super) fn retained_completion(
    mutation: SemanticIndexMutation,
    receipt_digest: SemanticDigest,
) -> Option<Box<SemanticStageTransition>> {
    let SemanticIndexMutation::RecordStageTransition { transition, .. } = mutation else {
        return None;
    };
    let exact = is_terminal(transition.receipt.outcome)
        && transition.receipt.receipt_digest() == receipt_digest;
    exact.then_some(transition)
}

pub(super) fn completion_coordinates(intent: &SemanticStageIntent) -> CompletionCoordinates<'_> {
    (
        intent.binding_id.as_str(),
        intent.binding_digest,
        intent.generation,
        intent.source_revision.as_str(),
        intent.stage,
        intent.scope.source_entity_id(),
    )
}

pub(super) fn validate_successor_intent(
    transition: &SemanticStageTransition,
    successor: Option<&SemanticStageIntent>,
) -> Result<Option<SemanticStageIntent>, SemanticCodeError> {
    if !is_terminal(transition.receipt.outcome) {
        ensure(
            successor.is_none(),
            "non-terminal semantic stage outcome cannot publish a successor",
        )?;
        return Ok(None);
    }
    let derived = derived_successor(transition)?;
    let Some(successor) = successor else {
        ensure(
            !matches!(
                transition.intent.stage,
                SemanticStage::Vector | SemanticStage::AnnIndex
            ),
            "completed S4/S5 transition requires an explicit successor proof",
        )?;
        return Ok(derived);
    };
    successor.validate().map_err(semantic_contract_error)?;
    let intent = &transition.intent;
    ensure(
        (
            successor.binding_id.as_str(),
            successor.binding_digest,
            successor.generation,
            successor.source_revision.as_str(),
            successor.input_digest,
        ) == (
            intent.binding_id.as_str(),
            intent.binding_digest,
            intent.generation,
            intent.source_revision.as_str(),
            transition.receipt.output_digest,
        ),
        "successor intent coordinates or predecessor input are stale",
    )?;
    ensure_successor_proof(intent.stage, successor, derived.as_ref())?;
    Ok(Some(successor.clone()))
}

/// The successor S1-S3 derive from their own receipt; S4-S6 derive none.
fn derived_successor(
    transition: &SemanticStageTransition,
) -> Result<Option<SemanticStageIntent>, SemanticCodeError> {
    let receipt_digest = transition.receipt.receipt_digest();
    let next = match transition.intent.stage {
        SemanticStage::SourceCommit => Some((
            SemanticStage::GraphProjection,
            SemanticStagePredecessor::EntityReceipt {
                stage: SemanticStage::SourceCommit,
                receipt_digest,
            },
        )),
        SemanticStage::GraphProjection => Some((
            SemanticStage::LexicalIndex,
            SemanticStagePredecessor::EntityReceipt {
                stage: SemanticStage::GraphProjection,
                receipt_digest,
            },
        )),
        SemanticStage::LexicalIndex => {
            let checkpoint = transition
                .generation_checkpoint
                .as_ref()
                .ok_or_else(|| refused("completed S3 transition has no lexical checkpoint"))?;
            Some((
                SemanticStage::Vector,
                SemanticStagePredecessor::GenerationCheckpoint {
                    stage: SemanticStage::LexicalIndex,
                    checkpoint_digest: checkpoint.successor.checkpoint_digest,
                },
            ))
        }
        SemanticStage::Vector | SemanticStage::AnnIndex | SemanticStage::ReconcileAndActivate => {
            None
        }
    };
    next.map(|(stage, predecessor)| {
        let intent = &transition.intent;
        SemanticStageIntent::create(SemanticStageIntentDraft {
            binding_id: intent.binding_id.clone(),
            binding_digest: intent.binding_digest,
            generation: intent.generation,
            scope: intent.scope.clone(),
            source_revision: intent.source_revision.clone(),
            stage,
            predecessor,
            input_digest: transition.receipt.output_digest,
        })
    })
    .transpose()
    .map_err(|error| SemanticCodeError::Refused(format!("successor intent rejected: {error:?}")))
}

/// The proof each completed stage's explicit successor must carry.
fn ensure_successor_proof(
    stage: SemanticStage,
    successor: &SemanticStageIntent,
    derived: Option<&SemanticStageIntent>,
) -> Result<(), SemanticCodeError> {
    match stage {
        SemanticStage::SourceCommit
        | SemanticStage::GraphProjection
        | SemanticStage::LexicalIndex => ensure(
            derived == Some(successor),
            "successor intent does not carry the exact stage receipt proof",
        ),
        SemanticStage::Vector => ensure(
            successor.stage == SemanticStage::AnnIndex && has_complete_coverage(successor),
            "S4 successor must be a complete S5 generation-coverage intent",
        ),
        SemanticStage::AnnIndex => ensure(
            is_activation_successor(successor),
            "S5 successor must be the generation-scoped S6 activation intent",
        ),
        SemanticStage::ReconcileAndActivate => {
            Err(refused("S6 is terminal and cannot publish another stage"))
        }
    }
}

/// An S5 intent whose predecessor is the complete vector coverage checkpoint
/// of its own generation.
fn has_complete_coverage(successor: &SemanticStageIntent) -> bool {
    let SemanticStagePredecessor::GenerationCoverage { checkpoint } = &successor.predecessor else {
        return false;
    };
    checkpoint.require_complete().is_ok()
        && (
            checkpoint.binding_id.as_str(),
            checkpoint.binding_digest,
            checkpoint.generation,
            checkpoint.source_revision.as_str(),
        ) == (
            successor.binding_id.as_str(),
            successor.binding_digest,
            successor.generation,
            successor.source_revision.as_str(),
        )
}

fn is_activation_successor(successor: &SemanticStageIntent) -> bool {
    successor.stage == SemanticStage::ReconcileAndActivate
        && matches!(successor.scope, SemanticStageScope::Generation)
        && matches!(
            successor.predecessor,
            SemanticStagePredecessor::Activation { .. }
        )
}
