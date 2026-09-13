//! Refreshing a binding to its next source generation together with its
//! replacement S1 intent, in one caller-attributed mutation.

use super::batch::{semantic_digest, MetadataMutation};
use super::persist::{put_bytes_once, replace_bytes};
use super::reconciliation::{
    compare_source_revision, sql_source_revision_parts, validate_sql_source_revision,
};
use super::stage::{stage_intent_outbox, stage_receipt_index_key};
use super::{
    kernel_error, semantic_contract_error, OperationAttribution, SemanticCodeError,
    SemanticCodeStore, SemanticMutationReceipt, SEMANTIC_BINDING_CREATED_TOPIC,
    SEMANTIC_STAGE_INTENT_TOPIC,
};
use eg_storage::{SEMANTIC_BINDINGS, SEMANTIC_HEADS, SEMANTIC_SOURCE_PROGRESS, SEMANTIC_STAGES};
use eg_types::mutation_batch::MutationOutboxIntent;
use eg_types::semantic_index::{
    SemanticBinding, SemanticBindingState, SemanticIndexMutation, SemanticSourceProgress,
    SemanticSqlSourceManifest, SemanticStage, SemanticStageIntent, SemanticStageOutcome,
    SemanticStagePredecessor,
};
use std::collections::BTreeMap;

impl SemanticCodeStore {
    /// Refresh a source generation and admit its replacement S1 intent in the
    /// same caller-attributed mutation. The query-plan service uses this seam
    /// when it has already read the authoritative source snapshot. There is no
    /// head-moving refresh operation without this intent: a later standalone
    /// wakeup would otherwise leave a crash window between the moved
    /// generation head and its first claimable work.
    pub(crate) fn refresh_binding_operation_with_s1(
        &self,
        expected_generation: u64,
        replacement: &SemanticBinding,
        source_manifest: &SemanticSqlSourceManifest,
        replacement_intent: &SemanticStageIntent,
        now_ms: u64,
        attribution: OperationAttribution<'_>,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        let OperationAttribution {
            actor,
            idempotency_key,
            nonce,
        } = attribution;
        self.refresh_binding_operation_inner(
            expected_generation,
            replacement,
            source_manifest,
            replacement_intent,
            now_ms,
            OperationAttribution {
                actor,
                idempotency_key,
                nonce,
            },
        )
    }

    fn refresh_binding_operation_inner(
        &self,
        expected_generation: u64,
        replacement: &SemanticBinding,
        source_manifest: &SemanticSqlSourceManifest,
        replacement_intent: &SemanticStageIntent,
        now_ms: u64,
        attribution: OperationAttribution<'_>,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        let OperationAttribution {
            actor,
            idempotency_key,
            nonce,
        } = attribution;
        if actor.trim().is_empty() || idempotency_key.trim().is_empty() {
            return Err(SemanticCodeError::Refused(
                "semantic refresh requires verified actor and idempotency key".to_string(),
            ));
        }
        let source_manifest_digest = source_manifest.manifest_digest.to_string();
        // These are pure content checks, so they may run before replay lookup.
        // They prevent a caller from preserving an old manifest_digest field
        // while changing the manifest body and accidentally taking the replay
        // path without proving the new body is canonical.
        replacement.validate().map_err(semantic_contract_error)?;
        source_manifest
            .validate_against_binding(replacement)
            .map_err(semantic_contract_error)?;
        replacement_intent
            .validate()
            .map_err(semantic_contract_error)?;
        if replacement_intent.binding_id != replacement.binding_id
            || replacement_intent.binding_digest != replacement.binding_digest
            || replacement_intent.generation != replacement.generation
            || replacement_intent.source_revision != replacement.source_revision
            || replacement_intent.input_digest != source_manifest.source_content_digest
            || replacement_intent.scope.source_entity_id()
                != Some(source_manifest.source_entity_id.as_str())
            || replacement_intent.stage != SemanticStage::SourceCommit
            || !matches!(
                &replacement_intent.predecessor,
                SemanticStagePredecessor::None
            )
        {
            return Err(SemanticCodeError::Refused(
                "refresh S1 intent is not bound to the replacement source manifest".to_string(),
            ));
        }
        if let Some(receipt) = self.door.replay_operation_if_recorded(
            actor,
            idempotency_key,
            nonce,
            now_ms,
            |batch| {
                let event = batch.outbox.first().ok_or_else(|| {
                    SemanticCodeError::Corrupt(
                        "semantic refresh replay batch has no binding event".to_string(),
                    )
                })?;
                let existing = SemanticBinding::from_canonical_cbor(&event.payload)
                    .map_err(semantic_contract_error)?;
                if existing != *replacement
                    || event.headers.get("source_entity_id").map(String::as_str)
                        != Some(source_manifest.source_entity_id.as_str())
                    || event
                        .headers
                        .get("source_manifest_digest")
                        .map(String::as_str)
                        != Some(source_manifest_digest.as_str())
                {
                    return Err(SemanticCodeError::Refused(
                        "semantic refresh idempotency key names different content".to_string(),
                    ));
                }
                let event = batch
                    .outbox
                    .iter()
                    .find(|event| event.topic == SEMANTIC_STAGE_INTENT_TOPIC)
                    .ok_or_else(|| {
                        SemanticCodeError::Corrupt(
                            "semantic refresh replay batch has no replacement S1 event".to_string(),
                        )
                    })?;
                let existing_intent = SemanticStageIntent::from_canonical_cbor(&event.payload)
                    .map_err(semantic_contract_error)?;
                if existing_intent != *replacement_intent {
                    return Err(SemanticCodeError::Refused(
                        "semantic refresh idempotency key names a different replacement S1"
                            .to_string(),
                    ));
                }
                Ok(())
            },
        )? {
            return Ok(receipt);
        }
        if replacement.tenant_id != self.tenant
            || replacement.binding_id != self.binding
            || replacement.durable_state != SemanticBindingState::Pending
        {
            return Err(SemanticCodeError::Refused(
                "semantic refresh replacement is outside this pending owner".to_string(),
            ));
        }
        let current = self.read_binding()?.ok_or_else(|| {
            SemanticCodeError::Refused("semantic refresh has no durable binding".to_string())
        })?;
        if current.generation != expected_generation
            || current.durable_state != SemanticBindingState::Live
        {
            return Err(SemanticCodeError::Refused(
                "semantic refresh requires the expected live generation".to_string(),
            ));
        }
        let expected_generation = current.generation.checked_add(1).ok_or_else(|| {
            SemanticCodeError::Refused("semantic refresh generation exhausted".to_string())
        })?;
        if replacement.generation != expected_generation
            || replacement.actor_scope != current.actor_scope
            || replacement.effective_actor_scope != current.effective_actor_scope
            || replacement.purpose_id != current.purpose_id
            || replacement.policy_identity != current.policy_identity
            || replacement.policy_digest != current.policy_digest
            || replacement.source_selector != current.source_selector
            || replacement.vector_target_id != current.vector_target_id
            || replacement.dimension != current.dimension
            || replacement.metric != current.metric
            || replacement.model_id != current.model_id
            || replacement.model_revision != current.model_revision
            || replacement.preprocess_digest != current.preprocess_digest
            || replacement.model_digest != current.model_digest
            || replacement.maintenance_policy_id != current.maintenance_policy_id
            || replacement.queue_profile_id != current.queue_profile_id
            || replacement.lexical_index_identity.spec() != current.lexical_index_identity.spec()
            || replacement.ann_index_identity.spec() != current.ann_index_identity.spec()
        {
            return Err(SemanticCodeError::Refused(
                "semantic refresh changed an immutable binding authority".to_string(),
            ));
        }
        validate_sql_source_revision(&replacement.source_revision)?;
        if current.source_revision.starts_with("sql-source:") {
            let current_revision =
                sql_source_revision_parts(&current.source_revision).ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "semantic refresh live head has an invalid SQL source revision".to_string(),
                    )
                })?;
            let replacement_revision = sql_source_revision_parts(&replacement.source_revision)
                .ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "semantic refresh replacement has an invalid SQL source revision"
                            .to_string(),
                    )
                })?;
            if current_revision.authority != replacement_revision.authority
                || replacement_revision.epoch <= current_revision.epoch
            {
                return Err(SemanticCodeError::Refused(
                    "semantic refresh source revision is not a newer epoch in the live SQL authority"
                        .to_string(),
                ));
            }
        } else if compare_source_revision(&replacement.source_revision, &current.source_revision)
            != std::cmp::Ordering::Greater
        {
            return Err(SemanticCodeError::Refused(
                "semantic refresh source revision is not newer than the live head".to_string(),
            ));
        }
        let read = self.door.serving_read()?;
        let progress = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                current.generation,
                source_manifest.source_entity_id.as_str(),
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| {
                SemanticCodeError::Refused(
                    "semantic refresh lacks the old generation source progress proof".to_string(),
                )
            })?;
        let progress = SemanticSourceProgress::from_canonical_cbor(&progress)
            .map_err(semantic_contract_error)?;
        let retained_receipt_digest = progress.completed_receipt_digest.ok_or_else(|| {
            SemanticCodeError::Refused(
                "semantic refresh requires a completed predecessor receipt".to_string(),
            )
        })?;
        let completed_stage = progress.completed_stage.ok_or_else(|| {
            SemanticCodeError::Refused(
                "semantic refresh predecessor has no completed stage identity".to_string(),
            )
        })?;
        let retained_receipt_key = stage_receipt_index_key(retained_receipt_digest);
        let retained_receipt = read
            .open_owner_table(SEMANTIC_STAGES)
            .map_err(kernel_error)?
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                retained_receipt_key.as_str(),
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| {
                SemanticCodeError::Refused(
                    "semantic refresh predecessor receipt has no indexed stage row".to_string(),
                )
            })?;
        let retained_receipt = SemanticIndexMutation::from_canonical_cbor(&retained_receipt)
            .map_err(semantic_contract_error)?;
        let retained_receipt_verified = match retained_receipt {
            SemanticIndexMutation::RecordStageTransition { transition, .. } => {
                transition.intent.binding_id == current.binding_id
                    && transition.intent.binding_digest == current.binding_digest
                    && transition.intent.generation == current.generation
                    && transition.intent.source_revision == progress.source_revision
                    && transition.intent.stage == completed_stage
                    && transition.intent.scope.source_entity_id()
                        == Some(source_manifest.source_entity_id.as_str())
                    && matches!(
                        transition.receipt.outcome,
                        SemanticStageOutcome::Completed | SemanticStageOutcome::IdempotentNoop
                    )
                    && transition.receipt.receipt_digest() == retained_receipt_digest
            }
            _ => false,
        };
        if !retained_receipt_verified {
            return Err(SemanticCodeError::Refused(
                "semantic refresh predecessor receipt is not the exact durable stage receipt"
                    .to_string(),
            ));
        }
        if progress.binding_id != current.binding_id
            || progress.binding_digest != current.binding_digest
            || progress.generation != current.generation
            || progress.source_revision != current.source_revision
            || compare_source_revision(&replacement.source_revision, &progress.source_revision)
                != std::cmp::Ordering::Greater
        {
            return Err(SemanticCodeError::Refused(
                "semantic refresh predecessor proof is stale".to_string(),
            ));
        }
        let mutation = SemanticIndexMutation::SupersedeSourceRevision {
            binding_id: replacement.binding_id.clone(),
            binding_digest: replacement.binding_digest,
            generation: replacement.generation,
            source_entity_id: source_manifest.source_entity_id.clone(),
            superseded_revision: progress.source_revision.clone(),
            replacement_revision: replacement.source_revision.clone(),
            retained_receipt_digest,
        };
        mutation.validate().map_err(semantic_contract_error)?;
        let mutation_bytes = mutation
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation_digest = semantic_digest(&mutation_bytes);
        let replacement_bytes = replacement
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mut headers = BTreeMap::from([
            (
                "schema".to_string(),
                eg_types::semantic_index::SEMANTIC_BINDING_SCHEMA.to_string(),
            ),
            ("actor".to_string(), actor.to_string()),
            ("binding_id".to_string(), replacement.binding_id.clone()),
            (
                "source_entity_id".to_string(),
                source_manifest.source_entity_id.clone(),
            ),
            (
                "binding_digest".to_string(),
                replacement.binding_digest.to_string(),
            ),
            ("generation".to_string(), replacement.generation.to_string()),
            (
                "superseded_revision".to_string(),
                progress.source_revision.clone(),
            ),
            (
                "replacement_revision".to_string(),
                replacement.source_revision.clone(),
            ),
        ]);
        headers.insert(
            "retained_receipt_digest".to_string(),
            retained_receipt_digest.to_string(),
        );
        headers.insert("source_manifest_digest".to_string(), source_manifest_digest);
        let mut outbox = vec![MutationOutboxIntent {
            // A refresh creates a new durable binding generation; reuse the
            // existing binding-created topic so composition has one governed
            // binding-definition event rather than a parallel compatibility
            // route. The operation event type and headers carry supersession
            // proof for consumers that need to distinguish the cause.
            topic: SEMANTIC_BINDING_CREATED_TOPIC.to_string(),
            key: format!("{}:{}", replacement.binding_id, replacement.generation),
            payload: replacement_bytes.clone(),
            headers,
        }];
        outbox.push(stage_intent_outbox(replacement_intent)?);
        let batch_id = format!("semantic-index:operation:{idempotency_key}");
        let tenant = self.tenant.clone();
        let binding_id = self.binding.clone();
        let current_for_write = current.clone();
        let progress_for_write = progress.clone();
        let source_entity_id_for_write = source_manifest.source_entity_id.clone();
        let replacement_for_write = replacement.clone();
        let replacement_intent_for_write = replacement_intent.clone();
        let refresh_at = now_ms;
        self.door.commit_metadata(
            |version| {
                self.metadata_operation_batch(
                    self.door.owner(),
                    version,
                    MetadataMutation {
                        batch_id: &batch_id,
                        event_type: "semantic_binding_refreshed",
                        subject: &format!("binding:{}", replacement.binding_digest),
                        mutation_digest,
                    },
                    outbox,
                    now_ms,
                    OperationAttribution {
                        actor,
                        idempotency_key,
                        nonce,
                    },
                )
            },
            mutation_digest,
            now_ms,
            |write, rows| {
                let current = self.read_binding_in_write(write)?.ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "semantic refresh head disappeared during admission".to_string(),
                    )
                })?;
                if current != current_for_write {
                    return Err(SemanticCodeError::Refused(
                        "semantic refresh head changed during admission".to_string(),
                    ));
                }
                let progress_raw = write
                    .open_read_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?
                    .get((
                        tenant.as_str(),
                        binding_id.as_str(),
                        current_for_write.generation,
                        source_entity_id_for_write.as_str(),
                    ))
                    .map_err(kernel_error)?
                    .map(|value| value.value().to_vec())
                    .ok_or_else(|| {
                        SemanticCodeError::Refused(
                            "semantic refresh source progress disappeared during admission"
                                .to_string(),
                        )
                    })?;
                let durable_progress = SemanticSourceProgress::from_canonical_cbor(&progress_raw)
                    .map_err(semantic_contract_error)?;
                if durable_progress != progress_for_write {
                    return Err(SemanticCodeError::Refused(
                        "semantic refresh source progress changed during admission".to_string(),
                    ));
                }
                if durable_progress.superseded_by_revision.is_some() {
                    return Err(SemanticCodeError::Refused(
                        "semantic refresh source progress was already superseded".to_string(),
                    ));
                }
                let binding_key = (
                    tenant.as_str(),
                    binding_id.as_str(),
                    replacement_for_write.generation,
                );
                let mut bindings = rows.open_table(SEMANTIC_BINDINGS).map_err(kernel_error)?;
                put_bytes_once(&mut bindings, binding_key, &replacement_bytes)?;
                drop(bindings);
                let progress = SemanticSourceProgress {
                    binding_id: replacement_intent_for_write.binding_id.clone(),
                    binding_digest: replacement_intent_for_write.binding_digest,
                    generation: replacement_intent_for_write.generation,
                    source_entity_id: source_entity_id_for_write.clone(),
                    source_revision: replacement_intent_for_write.source_revision.clone(),
                    completed_stage: None,
                    completed_receipt_digest: None,
                    superseded_by_revision: None,
                    updated_at: format!("unix-ms:{refresh_at}"),
                };
                progress.validate().map_err(semantic_contract_error)?;
                let progress_bytes = progress
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                rows.open_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?
                    .insert(
                        (
                            tenant.as_str(),
                            binding_id.as_str(),
                            replacement_intent_for_write.generation,
                            source_entity_id_for_write.as_str(),
                        ),
                        progress_bytes.as_slice(),
                    )
                    .map_err(kernel_error)?;
                let mut superseded_progress = durable_progress;
                superseded_progress.superseded_by_revision =
                    Some(replacement_for_write.source_revision.clone());
                superseded_progress.updated_at = format!("unix-ms:{refresh_at}");
                superseded_progress
                    .validate()
                    .map_err(semantic_contract_error)?;
                let superseded_progress_bytes = superseded_progress
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut progress_rows = rows
                    .open_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?;
                replace_bytes(
                    &mut progress_rows,
                    (
                        tenant.as_str(),
                        binding_id.as_str(),
                        current_for_write.generation,
                        source_entity_id_for_write.as_str(),
                    ),
                    &superseded_progress_bytes,
                )?;
                drop(progress_rows);
                let mut heads = rows.open_table(SEMANTIC_HEADS).map_err(kernel_error)?;
                heads
                    .insert(
                        (tenant.as_str(), binding_id.as_str()),
                        replacement_for_write.generation,
                    )
                    .map_err(kernel_error)?;
                Ok(())
            },
        )
    }
}
