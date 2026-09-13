//! Refreshing a binding to its next source generation together with its
//! replacement S1 intent, in one caller-attributed mutation.
//!
//! A refresh is proved, then applied. `refresh_proof` reads the live binding,
//! the old generation's source progress and the retained receipt that
//! completed it from one serving snapshot; the admitted write then re-reads
//! the binding and progress it decided against and, only if neither moved,
//! writes the replacement binding, its fresh progress, the superseded old
//! progress and the moved head.

use super::batch::{
    binding_subject, mutation_row, operation_batch_id, require_attribution, MetadataMutation,
};
use super::persist::{put_bytes_once, replace_bytes};
use super::reconciliation::{
    compare_source_revision, sql_source_revision_parts, validate_sql_source_revision,
};
use super::record::{decode, encode, encode_valid, row};
use super::stage::{
    completion_coordinates, fresh_source_progress, retained_completion, stage_intent_outbox,
    stage_receipt_index_key,
};
use super::{
    corrupt, ensure, kernel_error, refused, semantic_contract_error, OperationAttribution,
    SemanticCodeError, SemanticCodeStore, SemanticMutationReceipt, SEMANTIC_BINDING_CREATED_TOPIC,
    SEMANTIC_STAGE_INTENT_TOPIC,
};
use eg_storage::{
    SemanticIndexOwner, SEMANTIC_BINDINGS, SEMANTIC_HEADS, SEMANTIC_SOURCE_PROGRESS,
    SEMANTIC_STAGES,
};
use eg_transaction::{AdmittedMutation, AdmittedOwnerWrite};
use eg_types::mutation_batch::MutationOutboxIntent;
use eg_types::semantic_index::{
    SemanticBinding, SemanticBindingState, SemanticDigest, SemanticIndexMutation,
    SemanticSourceProgress, SemanticSqlSourceManifest, SemanticStage, SemanticStageIntent,
    SemanticStagePredecessor,
};
use eg_types::MutationBatch;
use std::cmp::Ordering;
use std::collections::BTreeMap;

/// What a refresh proved from one serving snapshot before its admitted write.
struct RefreshProof<'a> {
    /// The live binding being replaced.
    current: SemanticBinding,
    /// The old generation's completed progress for the refreshed entity.
    progress: SemanticSourceProgress,
    /// The stage receipt that completed `progress`, retained by the refresh.
    retained_receipt_digest: SemanticDigest,
    replacement: &'a SemanticBinding,
    replacement_intent: &'a SemanticStageIntent,
    source_entity_id: &'a str,
}

impl RefreshProof<'_> {
    fn supersession(&self) -> SemanticIndexMutation {
        SemanticIndexMutation::SupersedeSourceRevision {
            binding_id: self.replacement.binding_id.clone(),
            binding_digest: self.replacement.binding_digest,
            generation: self.replacement.generation,
            source_entity_id: self.source_entity_id.to_string(),
            superseded_revision: self.progress.source_revision.clone(),
            replacement_revision: self.replacement.source_revision.clone(),
            retained_receipt_digest: self.retained_receipt_digest,
        }
    }
}

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
        require_attribution(
            actor,
            idempotency_key,
            "semantic refresh requires verified actor and idempotency key",
        )?;
        // These are pure content checks, so they may run before replay lookup.
        // They prevent a caller from preserving an old manifest_digest field
        // while changing the manifest body and accidentally taking the replay
        // path without proving the new body is canonical.
        check_refresh_content(replacement, source_manifest, replacement_intent)?;
        let replayed = self.door.replay_operation_if_recorded(
            actor,
            idempotency_key,
            nonce,
            now_ms,
            |batch| refresh_replay_matches(batch, replacement, source_manifest, replacement_intent),
        )?;
        if let Some(receipt) = replayed {
            return Ok(receipt);
        }
        let proof = self.refresh_proof(
            expected_generation,
            replacement,
            source_manifest,
            replacement_intent,
        )?;
        let (_, mutation_digest) = mutation_row(&proof.supersession())?;
        let replacement_bytes = encode(replacement)?;
        let outbox = refresh_outbox(
            &proof,
            actor,
            source_manifest.manifest_digest.to_string(),
            replacement_bytes.clone(),
        )?;
        let batch_id = operation_batch_id(idempotency_key);
        let subject = binding_subject(replacement.binding_digest);
        self.commit_operation(
            MetadataMutation {
                batch_id: &batch_id,
                event_type: "semantic_binding_refreshed",
                subject: &subject,
                mutation_digest,
            },
            outbox,
            now_ms,
            OperationAttribution {
                actor,
                idempotency_key,
                nonce,
            },
            |write, rows| {
                self.ensure_refresh_unchanged(write, &proof)?;
                self.write_refresh_rows(rows, &proof, &replacement_bytes, now_ms)
            },
        )
    }

    /// Prove, from one serving snapshot, that `replacement` is the next
    /// generation of the expected live binding with its immutable authority
    /// intact, a strictly newer source revision, and an exact retained receipt
    /// for the progress it supersedes.
    fn refresh_proof<'a>(
        &self,
        expected_generation: u64,
        replacement: &'a SemanticBinding,
        source_manifest: &'a SemanticSqlSourceManifest,
        replacement_intent: &'a SemanticStageIntent,
    ) -> Result<RefreshProof<'a>, SemanticCodeError> {
        if (
            replacement.tenant_id.as_str(),
            replacement.binding_id.as_str(),
            replacement.durable_state,
        ) != (
            self.tenant.as_str(),
            self.binding.as_str(),
            SemanticBindingState::Pending,
        ) {
            return Err(refused(
                "semantic refresh replacement is outside this pending owner",
            ));
        }
        let current = self
            .read_binding()?
            .ok_or_else(|| refused("semantic refresh has no durable binding"))?;
        if (current.generation, current.durable_state)
            != (expected_generation, SemanticBindingState::Live)
        {
            return Err(refused(
                "semantic refresh requires the expected live generation",
            ));
        }
        let next_generation = current
            .generation
            .checked_add(1)
            .ok_or_else(|| refused("semantic refresh generation exhausted"))?;
        if replacement.generation != next_generation
            || refresh_changes_authority(&current, replacement)
        {
            return Err(refused(
                "semantic refresh changed an immutable binding authority",
            ));
        }
        validate_sql_source_revision(&replacement.source_revision)?;
        ensure_newer_source_revision(&current.source_revision, &replacement.source_revision)?;
        let source_entity_id = source_manifest.source_entity_id.as_str();
        let (progress, retained_receipt_digest) =
            self.refresh_predecessor(&current, replacement, source_entity_id)?;
        Ok(RefreshProof {
            current,
            progress,
            retained_receipt_digest,
            replacement,
            replacement_intent,
            source_entity_id,
        })
    }

    /// The old generation's completed progress for `source_entity_id`, proved
    /// by the exact retained stage receipt its completion names.
    fn refresh_predecessor(
        &self,
        current: &SemanticBinding,
        replacement: &SemanticBinding,
        source_entity_id: &str,
    ) -> Result<(SemanticSourceProgress, SemanticDigest), SemanticCodeError> {
        let owner = (self.tenant.as_str(), self.binding.as_str());
        let read = self.door.serving_read()?;
        let progress_rows = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?;
        let progress: SemanticSourceProgress =
            row(progress_rows.get((owner.0, owner.1, current.generation, source_entity_id)))?
                .ok_or_else(|| {
                    refused("semantic refresh lacks the old generation source progress proof")
                })?;
        let retained_receipt_digest = progress
            .completed_receipt_digest
            .ok_or_else(|| refused("semantic refresh requires a completed predecessor receipt"))?;
        let completed_stage = progress.completed_stage.ok_or_else(|| {
            refused("semantic refresh predecessor has no completed stage identity")
        })?;
        let receipt_key = stage_receipt_index_key(retained_receipt_digest);
        let stages = read
            .open_owner_table(SEMANTIC_STAGES)
            .map_err(kernel_error)?;
        let retained: SemanticIndexMutation =
            row(stages.get((owner.0, owner.1, receipt_key.as_str())))?.ok_or_else(|| {
                refused("semantic refresh predecessor receipt has no indexed stage row")
            })?;
        let expected = (
            current.binding_id.as_str(),
            current.binding_digest,
            current.generation,
            progress.source_revision.as_str(),
            completed_stage,
            Some(source_entity_id),
        );
        let exact = retained_completion(retained, retained_receipt_digest)
            .is_some_and(|transition| completion_coordinates(&transition.intent) == expected);
        if !exact {
            return Err(refused(
                "semantic refresh predecessor receipt is not the exact durable stage receipt",
            ));
        }
        if progress_generation(&progress) != binding_generation(current)
            || compare_source_revision(&replacement.source_revision, &progress.source_revision)
                != Ordering::Greater
        {
            return Err(refused("semantic refresh predecessor proof is stale"));
        }
        Ok((progress, retained_receipt_digest))
    }

    /// Inside the admitted write: the binding and progress the proof decided
    /// against must still be exactly the durable rows.
    fn ensure_refresh_unchanged(
        &self,
        write: &AdmittedMutation<'_, SemanticIndexOwner>,
        proof: &RefreshProof<'_>,
    ) -> Result<(), SemanticCodeError> {
        let current = self
            .read_binding_in_write(write)?
            .ok_or_else(|| refused("semantic refresh head disappeared during admission"))?;
        if current != proof.current {
            return Err(refused("semantic refresh head changed during admission"));
        }
        let progress_rows = write
            .open_read_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?;
        let key = (
            self.tenant.as_str(),
            self.binding.as_str(),
            proof.current.generation,
            proof.source_entity_id,
        );
        let durable: SemanticSourceProgress = row(progress_rows.get(key))?.ok_or_else(|| {
            refused("semantic refresh source progress disappeared during admission")
        })?;
        if durable != proof.progress {
            return Err(refused(
                "semantic refresh source progress changed during admission",
            ));
        }
        if durable.superseded_by_revision.is_some() {
            return Err(refused(
                "semantic refresh source progress was already superseded",
            ));
        }
        Ok(())
    }

    fn write_refresh_rows(
        &self,
        rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
        proof: &RefreshProof<'_>,
        replacement_bytes: &[u8],
        now_ms: u64,
    ) -> Result<(), SemanticCodeError> {
        let owner = (self.tenant.as_str(), self.binding.as_str());
        let mut bindings = rows.open_table(SEMANTIC_BINDINGS).map_err(kernel_error)?;
        put_bytes_once(
            &mut bindings,
            (owner.0, owner.1, proof.replacement.generation),
            replacement_bytes,
        )?;
        drop(bindings);
        let intent = proof.replacement_intent;
        let fresh = fresh_source_progress(intent, proof.source_entity_id, now_ms);
        let fresh_bytes = encode_valid(&fresh)?;
        let mut progress_rows = rows
            .open_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?;
        progress_rows
            .insert(
                (owner.0, owner.1, intent.generation, proof.source_entity_id),
                fresh_bytes.as_slice(),
            )
            .map_err(kernel_error)?;
        let mut superseded = proof.progress.clone();
        superseded.superseded_by_revision = Some(proof.replacement.source_revision.clone());
        superseded.updated_at = format!("unix-ms:{now_ms}");
        let superseded_bytes = encode_valid(&superseded)?;
        replace_bytes(
            &mut progress_rows,
            (
                owner.0,
                owner.1,
                proof.current.generation,
                proof.source_entity_id,
            ),
            &superseded_bytes,
        )?;
        drop(progress_rows);
        rows.open_table(SEMANTIC_HEADS)
            .map_err(kernel_error)?
            .insert(owner, proof.replacement.generation)
            .map_err(kernel_error)?;
        Ok(())
    }
}

/// The request's own content: a canonical replacement, a manifest bound to
/// it, and the S1 intent that manifest's source entity starts.
fn check_refresh_content(
    replacement: &SemanticBinding,
    source_manifest: &SemanticSqlSourceManifest,
    intent: &SemanticStageIntent,
) -> Result<(), SemanticCodeError> {
    replacement.validate().map_err(semantic_contract_error)?;
    source_manifest
        .validate_against_binding(replacement)
        .map_err(semantic_contract_error)?;
    intent.validate().map_err(semantic_contract_error)?;
    let bound = (
        intent.binding_id.as_str(),
        intent.binding_digest,
        intent.generation,
        intent.source_revision.as_str(),
        intent.input_digest,
        intent.scope.source_entity_id(),
        intent.stage,
    ) == (
        replacement.binding_id.as_str(),
        replacement.binding_digest,
        replacement.generation,
        replacement.source_revision.as_str(),
        source_manifest.source_content_digest,
        Some(source_manifest.source_entity_id.as_str()),
        SemanticStage::SourceCommit,
    );
    if !bound || !matches!(&intent.predecessor, SemanticStagePredecessor::None) {
        return Err(refused(
            "refresh S1 intent is not bound to the replacement source manifest",
        ));
    }
    Ok(())
}

/// A replayed refresh must name the same replacement, source manifest and
/// replacement S1 intent as the operation it recorded.
fn refresh_replay_matches(
    batch: &MutationBatch,
    replacement: &SemanticBinding,
    source_manifest: &SemanticSqlSourceManifest,
    replacement_intent: &SemanticStageIntent,
) -> Result<(), SemanticCodeError> {
    let event = batch
        .outbox
        .first()
        .ok_or_else(|| corrupt("semantic refresh replay batch has no binding event"))?;
    let existing: SemanticBinding = decode(&event.payload)?;
    let manifest_digest = source_manifest.manifest_digest.to_string();
    let header = |name: &str| event.headers.get(name).map(String::as_str);
    let recorded = (header("source_entity_id"), header("source_manifest_digest"));
    let expected = (
        Some(source_manifest.source_entity_id.as_str()),
        Some(manifest_digest.as_str()),
    );
    ensure(
        existing == *replacement && recorded == expected,
        "semantic refresh idempotency key names different content",
    )?;
    let intent_event = batch
        .outbox
        .iter()
        .find(|event| event.topic == SEMANTIC_STAGE_INTENT_TOPIC)
        .ok_or_else(|| corrupt("semantic refresh replay batch has no replacement S1 event"))?;
    let existing_intent: SemanticStageIntent = decode(&intent_event.payload)?;
    ensure(
        existing_intent == *replacement_intent,
        "semantic refresh idempotency key names a different replacement S1",
    )
}

/// Everything about a binding except its generation, revision and state is
/// immutable across a refresh.
fn refresh_changes_authority(current: &SemanticBinding, replacement: &SemanticBinding) -> bool {
    let scope = |binding: &SemanticBinding| {
        (
            binding.actor_scope.clone(),
            binding.effective_actor_scope.clone(),
            binding.purpose_id.clone(),
            binding.policy_identity.clone(),
            binding.policy_digest.clone(),
            binding.source_selector.clone(),
            binding.vector_target_id.clone(),
            binding.maintenance_policy_id.clone(),
            binding.queue_profile_id.clone(),
        )
    };
    let model = |binding: &SemanticBinding| {
        (
            binding.dimension,
            binding.metric,
            binding.model_id.clone(),
            binding.model_revision.clone(),
            binding.preprocess_digest.clone(),
            binding.model_digest.clone(),
            binding.lexical_index_identity.spec(),
            binding.ann_index_identity.spec(),
        )
    };
    scope(replacement) != scope(current) || model(replacement) != model(current)
}

/// A SQL-sourced live head may only be refreshed to a newer epoch of the same
/// source authority; any other head only to a revision that sorts after it.
pub(super) fn ensure_newer_source_revision(
    current: &str,
    replacement: &str,
) -> Result<(), SemanticCodeError> {
    if !current.starts_with("sql-source:") {
        if compare_source_revision(replacement, current) != Ordering::Greater {
            return Err(refused(
                "semantic refresh source revision is not newer than the live head",
            ));
        }
        return Ok(());
    }
    let current_revision = sql_source_revision_parts(current)
        .ok_or_else(|| refused("semantic refresh live head has an invalid SQL source revision"))?;
    let replacement_revision = sql_source_revision_parts(replacement).ok_or_else(|| {
        refused("semantic refresh replacement has an invalid SQL source revision")
    })?;
    if current_revision.authority != replacement_revision.authority
        || replacement_revision.epoch <= current_revision.epoch
    {
        return Err(refused(
            "semantic refresh source revision is not a newer epoch in the live SQL authority",
        ));
    }
    Ok(())
}

fn refresh_outbox(
    proof: &RefreshProof<'_>,
    actor: &str,
    source_manifest_digest: String,
    replacement_bytes: Vec<u8>,
) -> Result<Vec<MutationOutboxIntent>, SemanticCodeError> {
    let replacement = proof.replacement;
    let headers = BTreeMap::from([
        (
            "schema".to_string(),
            eg_types::semantic_index::SEMANTIC_BINDING_SCHEMA.to_string(),
        ),
        ("actor".to_string(), actor.to_string()),
        ("binding_id".to_string(), replacement.binding_id.clone()),
        (
            "source_entity_id".to_string(),
            proof.source_entity_id.to_string(),
        ),
        (
            "binding_digest".to_string(),
            replacement.binding_digest.to_string(),
        ),
        ("generation".to_string(), replacement.generation.to_string()),
        (
            "superseded_revision".to_string(),
            proof.progress.source_revision.clone(),
        ),
        (
            "replacement_revision".to_string(),
            replacement.source_revision.clone(),
        ),
        (
            "retained_receipt_digest".to_string(),
            proof.retained_receipt_digest.to_string(),
        ),
        ("source_manifest_digest".to_string(), source_manifest_digest),
    ]);
    Ok(vec![
        MutationOutboxIntent {
            // A refresh creates a new durable binding generation; reuse the
            // existing binding-created topic so composition has one governed
            // binding-definition event rather than a parallel compatibility
            // route. The operation event type and headers carry supersession
            // proof for consumers that need to distinguish the cause.
            topic: SEMANTIC_BINDING_CREATED_TOPIC.to_string(),
            key: format!("{}:{}", replacement.binding_id, replacement.generation),
            payload: replacement_bytes,
            headers,
        },
        stage_intent_outbox(proof.replacement_intent)?,
    ])
}

fn progress_generation(progress: &SemanticSourceProgress) -> (&str, SemanticDigest, u64, &str) {
    (
        progress.binding_id.as_str(),
        progress.binding_digest,
        progress.generation,
        progress.source_revision.as_str(),
    )
}

fn binding_generation(binding: &SemanticBinding) -> (&str, SemanticDigest, u64, &str) {
    (
        binding.binding_id.as_str(),
        binding.binding_digest,
        binding.generation,
        binding.source_revision.as_str(),
    )
}
