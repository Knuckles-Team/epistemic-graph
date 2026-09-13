//! S6 activation and the serving reads.
//!
//! `finalize_generation` is the one writer of "which generation is live": it
//! publishes a generation's code rows, binding authority and active pointer in
//! one admitted mutation. `activate` and `retire` are closed doors, and the
//! serving reads resolve only the live generation the pointer names.

use super::batch::{semantic_digest, MetadataMutation};
use super::binding::demote_prior_live_binding_in_write;
use super::persist::{advance_checkpoint_head, put_bytes_once, replace_bytes};
use super::predecessor::{validate_six_checkpoint_write, validate_stage_predecessor_write};
use super::rows::{read_part, BoundCodeRows, DIGEST_PART, PARTS};
use super::stage::{stage_receipt_headers, stage_receipt_index_key};
use super::{
    kernel_error, semantic_contract_error, SemanticCodeError, SemanticCodeStore,
    SemanticMutationReceipt, SEMANTIC_STAGE_RECEIPT_TOPIC,
};
use crate::compute::semantic::SemanticGenerationImage;
use eg_storage::{
    ScopedRead, SemanticIndexOwner, ANN_CODES, SEMANTIC_BINDINGS, SEMANTIC_CHECKPOINTS,
    SEMANTIC_HEADS, SEMANTIC_POINTERS, SEMANTIC_STAGES, SEMANTIC_STATES,
};
use eg_types::mutation_batch::{MutationOutboxIntent, MutationOutboxLease};
use eg_types::semantic_index::{
    SemanticActivePointer, SemanticBindingState, SemanticBindingStateTransition,
    SemanticGenerationArtifact, SemanticGenerationCheckpoint, SemanticIndexMutation, SemanticStage,
    SemanticStageOutcome, SemanticStageTransition,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

impl SemanticCodeStore {
    /// Finalize a generation-scoped S6 transition. The final checkpoint,
    /// canonical active pointer, binding state, ANN image and exact replay
    /// mutation are admitted through one serving-owner write. This is kept
    /// separate from `complete_stage` because S6 is a
    /// `SemanticIndexMutation::FinalizeGeneration`, not an entity artifact.
    pub(crate) fn finalize_generation(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        checkpoint: &SemanticGenerationCheckpoint,
        artifact: &SemanticGenerationArtifact,
        image: &SemanticGenerationImage,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        transition.validate().map_err(semantic_contract_error)?;
        if transition.intent.stage != SemanticStage::ReconcileAndActivate
            || !matches!(
                transition.intent.scope,
                eg_types::semantic_index::SemanticStageScope::Generation
            )
            || transition.receipt.outcome != SemanticStageOutcome::Completed
        {
            return Err(SemanticCodeError::Refused(
                "generation finalization requires a completed generation-scoped S6 transition"
                    .to_string(),
            ));
        }
        checkpoint
            .require_complete()
            .map_err(semantic_contract_error)?;
        if checkpoint.binding_id != transition.intent.binding_id
            || checkpoint.binding_digest != transition.intent.binding_digest
            || checkpoint.generation != transition.intent.generation
            || checkpoint.source_revision != transition.intent.source_revision
            || checkpoint.stage != SemanticStage::ReconcileAndActivate
        {
            return Err(SemanticCodeError::Refused(
                "S6 checkpoint does not match its leased transition".to_string(),
            ));
        }
        let mutation = SemanticIndexMutation::FinalizeGeneration {
            checkpoint: Box::new(checkpoint.clone()),
            artifact: artifact.clone(),
        };
        mutation.validate().map_err(semantic_contract_error)?;
        let intent = self.parse_stage_lease(lease, &lease.consumer, now_ms)?;
        if intent != transition.intent {
            return Err(SemanticCodeError::Refused(
                "S6 transition intent does not equal the leased canonical intent".to_string(),
            ));
        }
        let binding = self.read_binding()?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "S6 publication requires a durable semantic binding".to_string(),
            )
        })?;
        if binding.binding_digest != transition.intent.binding_digest
            || binding.generation != transition.intent.generation
        {
            return Err(SemanticCodeError::Refused(
                "S6 binding identity is stale".to_string(),
            ));
        }
        let (dimensions, model_digest) = image.identity()?;
        if binding.dimension as usize != dimensions
            || Some(binding.model_digest.as_str()) != model_digest.as_deref()
        {
            return Err(SemanticCodeError::Refused(
                "S6 ANN image does not match the durable binding model".to_string(),
            ));
        }
        let SemanticGenerationArtifact::Activation { target, pointer } = artifact else {
            return Err(SemanticCodeError::Refused(
                "S6 publication requires the canonical activation artifact".to_string(),
            ));
        };
        target
            .validate_against_binding(&binding)
            .map_err(semantic_contract_error)?;
        pointer.validate().map_err(semantic_contract_error)?;
        if pointer.binding_id != binding.binding_id
            || pointer.binding_digest != binding.binding_digest
            || pointer.generation != binding.generation
            || pointer.source_revision != binding.source_revision
            || pointer.activation_receipt_digest != checkpoint.checkpoint_digest
            || pointer.activated_at != checkpoint.completed_at
        {
            return Err(SemanticCodeError::Refused(
                "S6 active pointer is not bound to the current checkpoint".to_string(),
            ));
        }
        // Validate the durable predecessor, generation manifests, and S6
        // checkpoint before taking the replay shortcut. A stage row without
        // these canonical proofs is incomplete and cannot be acknowledged
        // merely because its mutation bytes happen to decode.
        let read = self.door.serving_read()?;
        self.validate_stage_predecessor_in(&read, &intent, true)?;
        self.validate_generation_manifests_in(&read, &binding, &intent)?;
        self.validate_six_checkpoint_in(&read, checkpoint)?;
        if let Some(stored) = self.read_stage_mutation(intent.intent_digest)? {
            if stored != mutation {
                return Err(SemanticCodeError::Refused(
                    "S6 intent already has a different durable finalization".to_string(),
                ));
            }
            if self.live_generation_in(&read)? != Some(intent.generation) {
                return Err(SemanticCodeError::Refused(
                    "S6 replay has no matching durable live pointer".to_string(),
                ));
            }
            self.ack_stage_lease(lease, now_ms)?;
            let batch_id = format!("semantic-index:finalize:{}", intent.intent_digest);
            return self
                .door
                .stage_receipt_for_batch(&batch_id, true)
                .map_err(SemanticCodeError::Corrupt);
        }
        let mutation_bytes = mutation
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation_digest = semantic_digest(&mutation_bytes);
        let batch_id = format!("semantic-index:finalize:{}", intent.intent_digest);
        let transition_payload = transition
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let outbox = vec![MutationOutboxIntent {
            topic: SEMANTIC_STAGE_RECEIPT_TOPIC.to_string(),
            key: intent.intent_digest.to_string(),
            payload: transition_payload,
            headers: stage_receipt_headers(transition),
        }];
        let tenant = self.tenant.clone();
        let binding_id = self.binding.clone();
        let transition_for_write = transition.clone();
        let mutation_for_write = mutation.clone();
        let checkpoint_for_write = checkpoint.clone();
        let pointer_for_write = pointer.clone();
        let image_for_write = image.clone();
        let receipt = self.door.commit_metadata_fenced(
            |version| {
                self.metadata_batch(
                         self.door.owner(),
                         version,
                         MetadataMutation {
                             batch_id: &batch_id,
                             event_type: "semantic_generation_finalized",
                             subject: &format!("generation:{}", intent.generation),
                             mutation_digest,
                         },
                         outbox,
                         now_ms,
                     )
            },
            mutation_digest,
            now_ms,
            Some(lease),
            |write, rows| {
                validate_stage_predecessor_write(
                    write,
                    rows,
                    &tenant,
                    &binding_id,
                    &transition_for_write.intent,
                    true,
                )?;
                validate_six_checkpoint_write(rows, &tenant, &binding_id, &checkpoint_for_write)?;
                let stage_key = transition_for_write.intent.intent_digest.to_string();
                if let Some(existing) = write
                    .open_read_table(SEMANTIC_STAGES)
                    .map_err(kernel_error)?
                    .get((tenant.as_str(), binding_id.as_str(), stage_key.as_str()))
                    .map_err(kernel_error)?
                    .map(|value| value.value().to_vec())
                {
                    let existing = SemanticIndexMutation::from_canonical_cbor(&existing)
                        .map_err(semantic_contract_error)?;
                    if existing != mutation_for_write {
                        return Err(SemanticCodeError::Refused(
                            "S6 intent already has a different durable finalization".to_string(),
                        ));
                    }
                    let receipt_key = stage_receipt_index_key(
                        transition_for_write.receipt.receipt_digest(),
                    );
                    let mut stages = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
                    put_bytes_once(
                        &mut stages,
                        (tenant.as_str(), binding_id.as_str(), receipt_key.as_str()),
                        &mutation_bytes,
                    )?;
                    return Ok(());
                }
                let mut stages = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
                stages
                    .insert(
                        (tenant.as_str(), binding_id.as_str(), stage_key.as_str()),
                        mutation_bytes.as_slice(),
                    )
                    .map_err(kernel_error)?;
                let receipt_key =
                    stage_receipt_index_key(transition_for_write.receipt.receipt_digest());
                put_bytes_once(
                    &mut stages,
                    (tenant.as_str(), binding_id.as_str(), receipt_key.as_str()),
                    &mutation_bytes,
                )?;
                drop(stages);
                let checkpoint_key = checkpoint_for_write.checkpoint_digest.to_string();
                let checkpoint_bytes = checkpoint_for_write
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut checkpoints = rows
                    .open_table(SEMANTIC_CHECKPOINTS)
                    .map_err(kernel_error)?;
                put_bytes_once(
                    &mut checkpoints,
                    (
                        tenant.as_str(),
                        binding_id.as_str(),
                        checkpoint_for_write.generation,
                        checkpoint_key.as_str(),
                    ),
                    &checkpoint_bytes,
                )?;
                drop(checkpoints);
                advance_checkpoint_head(
                    rows,
                    &tenant,
                    &binding_id,
                    &checkpoint_for_write,
                    None,
                )?;

                let current = self.read_binding_in_write(write)?.ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "S6 binding disappeared during admitted finalization".to_string(),
                    )
                })?;
                if current.binding_digest != transition_for_write.intent.binding_digest
                    || current.generation != transition_for_write.intent.generation
                {
                    return Err(SemanticCodeError::Refused(
                        "S6 binding changed during admitted finalization".to_string(),
                    ));
                }
                if current.durable_state == SemanticBindingState::Dropping
                    || current.durable_state == SemanticBindingState::Disabled
                    || current.durable_state == SemanticBindingState::Failed
                {
                    return Err(SemanticCodeError::Refused(
                        "S6 cannot publish a disabled, dropping, or failed binding".to_string(),
                    ));
                }
                let prior_live_generation = write
                    .open_read_table(SEMANTIC_POINTERS)
                    .map_err(kernel_error)?
                    .get((tenant.as_str(), binding_id.as_str()))
                    .map_err(kernel_error)?
                    .map(|value| {
                        SemanticActivePointer::from_canonical_cbor(value.value())
                            .map_err(semantic_contract_error)
                    })
                    .transpose()?
                    .map(|pointer| pointer.generation);
                if let Some(prior_live_generation) = prior_live_generation {
                    if prior_live_generation > current.generation {
                        return Err(SemanticCodeError::Corrupt(
                            "S6 active pointer names a future generation".to_string(),
                        ));
                    }
                    if prior_live_generation < current.generation {
                        demote_prior_live_binding_in_write(
                            rows,
                            &tenant,
                            &binding_id,
                            prior_live_generation,
                        )?;
                    }
                }
                let (state_transition, updated) = match current.durable_state {
                    SemanticBindingState::Pending => {
                        return Err(SemanticCodeError::Refused(
                            "S6 requires a durable Pending-to-Building transition before publication"
                                .to_string(),
                        ));
                    }
                    SemanticBindingState::Building => {
                        let live = SemanticBindingStateTransition::create(
                            &current,
                            SemanticBindingState::Live,
                            "semantic_generation_activated",
                        )
                        .map_err(semantic_contract_error)?;
                        let updated = current
                            .apply_state_transition(&live)
                            .map_err(semantic_contract_error)?;
                        (live, updated)
                    }
                    SemanticBindingState::Live => {
                        return Err(SemanticCodeError::Refused(
                            "S6 binding is already live without an exact replay record".to_string(),
                        ));
                    }
                    SemanticBindingState::Disabled
                    | SemanticBindingState::Dropping
                    | SemanticBindingState::Failed => unreachable!(),
                };
                let state_bytes = state_transition
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut states = rows.open_table(SEMANTIC_STATES).map_err(kernel_error)?;
                replace_bytes(
                    &mut states,
                    (tenant.as_str(), binding_id.as_str()),
                    &state_bytes,
                )?;
                drop(states);
                let binding_bytes = updated
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut bindings = rows.open_table(SEMANTIC_BINDINGS).map_err(kernel_error)?;
                replace_bytes(
                    &mut bindings,
                    (tenant.as_str(), binding_id.as_str(), updated.generation),
                    &binding_bytes,
                )?;
                drop(bindings);

                let pointer_bytes = pointer_for_write
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut pointers = rows.open_table(SEMANTIC_POINTERS).map_err(kernel_error)?;
                replace_bytes(
                    &mut pointers,
                    (tenant.as_str(), binding_id.as_str()),
                    &pointer_bytes,
                )?;
                drop(pointers);

                let digest = image_digest(&image_for_write);
                let (image_dimensions, image_model) = image_for_write.identity()?;
                if image_dimensions != dimensions || image_model != model_digest {
                    return Err(SemanticCodeError::Refused(
                        "S6 image identity changed during finalization".to_string(),
                    ));
                }
                let mut codes = BoundCodeRows::new(
                    rows.open_table(ANN_CODES).map_err(kernel_error)?,
                    &tenant,
                    &binding_id,
                    transition_for_write.intent.generation,
                );
                for (part, bytes) in [
                    ("meta", image_for_write.index.codes.meta.as_slice()),
                    ("codes", image_for_write.index.codes.codes.as_slice()),
                    ("refine", image_for_write.index.codes.refine.as_slice()),
                    ("ids", image_for_write.index.ids.as_slice()),
                    ("manifest", image_for_write.manifest.as_slice()),
                ] {
                    codes.put_part(part, bytes)?;
                }
                codes.insert(
                    (
                        &tenant,
                        &binding_id,
                        transition_for_write.intent.generation,
                        DIGEST_PART,
                    ),
                    digest.as_bytes(),
                )?;
                drop(codes);

                Ok(())
            },
        )?;
        Ok(receipt)
    }

    /// Persist one generation's image and make it live, as ONE admitted
    /// maintenance mutation.
    ///
    /// Deliberately closed. Publication is the admitted S6 activation
    /// transition (`finalize_generation`), which writes the generation's code
    /// rows, its `semantic_bindings` authority and its active pointer in ONE
    /// mutation on the serving scope. A direct publication door would be a
    /// second writer of "which generation is live".
    pub fn activate(
        &self,
        _generation: u64,
        _image: &SemanticGenerationImage,
    ) -> Result<(), SemanticCodeError> {
        Err(SemanticCodeError::Refused(
            "direct ANN publication is disabled; use the admitted S6 activation transition"
                .to_string(),
        ))
    }

    /// The live generation's image, or `None` when this binding has none.
    ///
    /// This is the serving read, and it serves ONLY the live generation: a
    /// generation that has been superseded or retired is not reachable here.
    pub fn read_live(&self) -> Result<Option<(u64, SemanticGenerationImage)>, SemanticCodeError> {
        let read = self.door.serving_read()?;
        let Some(generation) = self.live_generation_in(&read)? else {
            return Ok(None);
        };
        Ok(self
            .read_generation_in(&read, generation)?
            .map(|image| (generation, image)))
    }

    /// One generation's image whether or not it is live -- the MAINTENANCE
    /// read, for building, verifying and retiring a generation. Writes nothing:
    /// an unactivated or retired generation is simply `None`.
    pub fn read_generation(
        &self,
        generation: u64,
    ) -> Result<Option<SemanticGenerationImage>, SemanticCodeError> {
        let read = self.door.serving_read()?;
        self.read_generation_in(&read, generation)
    }

    /// The live generation number, if any.
    pub fn live_generation(&self) -> Result<Option<u64>, SemanticCodeError> {
        let read = self.door.serving_read()?;
        self.live_generation_in(&read)
    }

    /// Legacy direct retirement is deliberately closed. Retirement must be
    /// admitted by the binding lifecycle path so a pending delete/tombstone
    /// proof and the active pointer change share one mutation.
    pub fn retire(&self, _generation: u64) -> Result<(), SemanticCodeError> {
        Err(SemanticCodeError::Refused(
            "direct ANN retirement is disabled; use the admitted binding lifecycle transition"
                .to_string(),
        ))
    }

    fn live_generation_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
    ) -> Result<Option<u64>, SemanticCodeError> {
        let pointers = read
            .open_owner_table(SEMANTIC_POINTERS)
            .map_err(kernel_error)?;
        let raw = pointers
            .get((self.tenant.as_str(), self.binding.as_str()))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec());
        let Some(raw) = raw else {
            return Ok(None);
        };
        let pointer =
            SemanticActivePointer::from_canonical_cbor(&raw).map_err(semantic_contract_error)?;
        pointer.validate().map_err(semantic_contract_error)?;
        let head_generation = read
            .open_owner_table(SEMANTIC_HEADS)
            .map_err(kernel_error)?
            .get((self.tenant.as_str(), self.binding.as_str()))
            .map_err(kernel_error)?
            .map(|value| value.value())
            .ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "semantic active pointer has no durable binding head".to_string(),
                )
            })?;
        if pointer.generation > head_generation {
            return Err(SemanticCodeError::Corrupt(
                "semantic active pointer names a future binding generation".to_string(),
            ));
        }
        let binding = self
            .read_binding_generation_in(read, pointer.generation)?
            .ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "semantic active pointer has no durable binding authority".to_string(),
                )
            })?;
        if pointer.tenant_id != binding.tenant_id
            || pointer.binding_id != binding.binding_id
            || pointer.binding_digest != binding.binding_digest
            || pointer.generation != binding.generation
            || pointer.source_revision != binding.source_revision
            || pointer.vector_target_id != binding.vector_target_id
            || pointer.lexical_index_identity != binding.lexical_index_identity
            || pointer.ann_index_identity != binding.ann_index_identity
            || pointer.composite_policy_digest != binding.policy_digest
            || binding.durable_state != SemanticBindingState::Live
        {
            return Err(SemanticCodeError::Refused(
                "semantic active pointer is not bound to the serving binding".to_string(),
            ));
        }
        Ok(Some(pointer.generation))
    }

    fn read_generation_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        generation: u64,
    ) -> Result<Option<SemanticGenerationImage>, SemanticCodeError> {
        let codes = read.open_owner_table(ANN_CODES).map_err(kernel_error)?;
        let mut parts = BTreeMap::new();
        for part in PARTS {
            let Some(bytes) = read_part(&codes, &self.tenant, &self.binding, generation, part)?
            else {
                return Ok(None);
            };
            parts.insert(part, bytes);
        }
        let mut take = |name: &str| parts.remove(name).unwrap_or_default();
        Ok(Some(SemanticGenerationImage {
            index: crate::compute::semantic_ann::AnnIndexImage {
                codes: eg_ann::durable_codes::AnnCodeArtifact {
                    meta: take("meta"),
                    codes: take("codes"),
                    refine: take("refine"),
                },
                ids: take("ids"),
            },
            manifest: take("manifest"),
        }))
    }
}

/// `sha256` over the image in a fixed part order, so the batch identity and the
/// "already durable" decision are both the content.
fn image_digest(image: &SemanticGenerationImage) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/semantic-ann-generation/v1\0");
    for bytes in [
        image.index.codes.meta.as_slice(),
        image.index.codes.codes.as_slice(),
        image.index.codes.refine.as_slice(),
        image.index.ids.as_slice(),
        image.manifest.as_slice(),
    ] {
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }
    hex::encode(hasher.finalize())
}
