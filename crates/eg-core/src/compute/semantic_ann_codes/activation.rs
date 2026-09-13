//! S6 activation.
//!
//! `finalize_generation` is the one writer of "which generation is live": it
//! publishes a generation's code rows, binding authority and active pointer in
//! one admitted mutation. `activate` and `retire` are closed doors.
//!
//! Finalization is proved, then published. The proof runs against the leased
//! intent and one serving snapshot; the publication re-proves the predecessor
//! and S6 checkpoint inside the admitted write, records the stage rows, and
//! only then moves the binding to `Live`, the pointer to this generation and
//! the image into its code rows.

use super::batch::{mutation_row, MetadataMutation};
use super::binding::demote_prior_live_binding_in_write;
use super::persist::{replace_bytes, store_checkpoint};
use super::predecessor::{validate_six_checkpoint_write, validate_stage_predecessor_write};
use super::record::{encode, row};
use super::rows::{BoundCodeRows, DIGEST_PART};
use super::stage::stage_receipt_event;
use super::{
    corrupt, ensure, kernel_error, refused, semantic_contract_error, SemanticCodeError,
    SemanticCodeStore, SemanticMutationReceipt,
};
use crate::compute::semantic::SemanticGenerationImage;
use eg_storage::{SemanticIndexOwner, ANN_CODES, SEMANTIC_POINTERS};
use eg_transaction::{AdmittedMutation, AdmittedOwnerWrite};
use eg_types::mutation_batch::MutationOutboxLease;
use eg_types::semantic_index::{
    SemanticActivePointer, SemanticBinding, SemanticBindingState, SemanticBindingStateTransition,
    SemanticGenerationArtifact, SemanticGenerationCheckpoint, SemanticIndexMutation, SemanticStage,
    SemanticStageIntent, SemanticStageOutcome, SemanticStageScope, SemanticStageTransition,
};
use sha2::{Digest, Sha256};

/// The dimension and model digest an ANN image describes itself with.
type ImageIdentity = (usize, Option<String>);

/// One proved S6 publication, applied inside the admitted write.
struct Publication<'a> {
    transition: &'a SemanticStageTransition,
    checkpoint: &'a SemanticGenerationCheckpoint,
    pointer: &'a SemanticActivePointer,
    image: &'a SemanticGenerationImage,
    image_identity: &'a ImageIdentity,
    mutation: &'a SemanticIndexMutation,
    mutation_bytes: &'a [u8],
}

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
        let mutation = finalization_mutation(transition, checkpoint, artifact)?;
        let intent = self.parse_stage_lease(lease, &lease.consumer, now_ms)?;
        ensure(
            intent == transition.intent,
            "S6 transition intent does not equal the leased canonical intent",
        )?;
        let (binding, image_identity) = self.finalization_binding(&intent, image)?;
        let pointer = activation_pointer(artifact, &binding, checkpoint)?;
        // Validate the durable predecessor, generation manifests, and S6
        // checkpoint before taking the replay shortcut. A stage row without
        // these canonical proofs is incomplete and cannot be acknowledged
        // merely because its mutation bytes happen to decode.
        let read = self.door.serving_read()?;
        self.validate_stage_predecessor_in(&read, &intent, true)?;
        self.validate_generation_manifests_in(&read, &binding, &intent)?;
        self.validate_six_checkpoint_in(&read, checkpoint)?;
        let batch_id = format!("semantic-index:finalize:{}", intent.intent_digest);
        if let Some(stored) = self.read_stage_mutation(intent.intent_digest)? {
            ensure(
                stored == mutation,
                "S6 intent already has a different durable finalization",
            )?;
            ensure(
                self.live_generation_in(&read)? == Some(intent.generation),
                "S6 replay has no matching durable live pointer",
            )?;
            self.ack_stage_lease(lease, now_ms)?;
            return self
                .door
                .stage_receipt_for_batch(&batch_id, true)
                .map_err(SemanticCodeError::Corrupt);
        }
        let (mutation_bytes, mutation_digest) = mutation_row(&mutation)?;
        let subject = format!("generation:{}", intent.generation);
        let outbox = vec![stage_receipt_event(transition)?];
        let publication = Publication {
            transition,
            checkpoint,
            pointer,
            image,
            image_identity: &image_identity,
            mutation: &mutation,
            mutation_bytes: &mutation_bytes,
        };
        self.commit_maintenance(
            MetadataMutation {
                batch_id: &batch_id,
                event_type: "semantic_generation_finalized",
                subject: &subject,
                mutation_digest,
            },
            outbox,
            now_ms,
            Some(lease),
            |write, rows| self.publish_generation(write, rows, &publication),
        )
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

    /// Legacy direct retirement is deliberately closed. Retirement must be
    /// admitted by the binding lifecycle path so a pending delete/tombstone
    /// proof and the active pointer change share one mutation.
    pub fn retire(&self, _generation: u64) -> Result<(), SemanticCodeError> {
        Err(SemanticCodeError::Refused(
            "direct ANN retirement is disabled; use the admitted binding lifecycle transition"
                .to_string(),
        ))
    }

    /// The durable binding S6 publishes, and the identity of the image it
    /// publishes, which must name the binding's own model.
    fn finalization_binding(
        &self,
        intent: &SemanticStageIntent,
        image: &SemanticGenerationImage,
    ) -> Result<(SemanticBinding, ImageIdentity), SemanticCodeError> {
        let binding = self
            .read_binding()?
            .ok_or_else(|| refused("S6 publication requires a durable semantic binding"))?;
        ensure(
            (binding.binding_digest, binding.generation)
                == (intent.binding_digest, intent.generation),
            "S6 binding identity is stale",
        )?;
        let image_identity = image.identity()?;
        ensure(
            (
                binding.dimension as usize,
                Some(binding.model_digest.as_str()),
            ) == (image_identity.0, image_identity.1.as_deref()),
            "S6 ANN image does not match the durable binding model",
        )?;
        Ok((binding, image_identity))
    }

    fn publish_generation(
        &self,
        write: &AdmittedMutation<'_, SemanticIndexOwner>,
        rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
        publication: &Publication<'_>,
    ) -> Result<(), SemanticCodeError> {
        let (tenant, binding) = (self.tenant.as_str(), self.binding.as_str());
        let intent = &publication.transition.intent;
        validate_stage_predecessor_write(write, rows, tenant, binding, intent, true)?;
        validate_six_checkpoint_write(rows, tenant, binding, publication.checkpoint)?;
        let recorded_before = self.record_stage_rows(
            write,
            rows,
            publication.transition,
            publication.mutation,
            publication.mutation_bytes,
            "S6 intent already has a different durable finalization",
        )?;
        if recorded_before {
            return Ok(());
        }
        store_checkpoint(rows, tenant, binding, publication.checkpoint, None)?;
        self.activate_binding_in_write(write, rows, intent)?;
        let pointer_bytes = encode(publication.pointer)?;
        let mut pointers = rows.open_table(SEMANTIC_POINTERS).map_err(kernel_error)?;
        replace_bytes(&mut pointers, (tenant, binding), &pointer_bytes)?;
        drop(pointers);
        self.write_generation_image(rows, intent.generation, publication)
    }

    /// Move the leased generation's binding to `Live`, demoting the generation
    /// the active pointer named before it.
    fn activate_binding_in_write(
        &self,
        write: &AdmittedMutation<'_, SemanticIndexOwner>,
        rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
        intent: &SemanticStageIntent,
    ) -> Result<(), SemanticCodeError> {
        let current = self
            .read_binding_in_write(write)?
            .ok_or_else(|| refused("S6 binding disappeared during admitted finalization"))?;
        ensure(
            (current.binding_digest, current.generation)
                == (intent.binding_digest, intent.generation),
            "S6 binding changed during admitted finalization",
        )?;
        ensure(!is_retired(current.durable_state), RETIRED_BINDING)?;
        self.demote_superseded_live(write, rows, current.generation)?;
        let live = live_transition(&current)?;
        self.write_binding_state(rows, &current, &live)
    }

    /// The generation the active pointer names before this publication moves
    /// it: a later generation is corruption, an earlier live one is demoted.
    fn demote_superseded_live(
        &self,
        write: &AdmittedMutation<'_, SemanticIndexOwner>,
        rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
        generation: u64,
    ) -> Result<(), SemanticCodeError> {
        let pointers = write
            .open_read_table(SEMANTIC_POINTERS)
            .map_err(kernel_error)?;
        let prior: Option<SemanticActivePointer> =
            row(pointers.get((self.tenant.as_str(), self.binding.as_str())))?;
        drop(pointers);
        let Some(prior_generation) = prior.map(|pointer| pointer.generation) else {
            return Ok(());
        };
        if prior_generation > generation {
            return Err(corrupt("S6 active pointer names a future generation"));
        }
        if prior_generation < generation {
            demote_prior_live_binding_in_write(
                rows,
                &self.tenant,
                &self.binding,
                prior_generation,
            )?;
        }
        Ok(())
    }

    fn write_generation_image(
        &self,
        rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
        generation: u64,
        publication: &Publication<'_>,
    ) -> Result<(), SemanticCodeError> {
        let image = publication.image;
        let digest = image_digest(image);
        ensure(
            image.identity()? == *publication.image_identity,
            "S6 image identity changed during finalization",
        )?;
        let mut codes = BoundCodeRows::new(
            rows.open_table(ANN_CODES).map_err(kernel_error)?,
            &self.tenant,
            &self.binding,
            generation,
        );
        for (part, bytes) in image_parts(image) {
            codes.put_part(part, bytes)?;
        }
        codes.insert(
            (&self.tenant, &self.binding, generation, DIGEST_PART),
            digest.as_bytes(),
        )
    }
}

const RETIRED_BINDING: &str = "S6 cannot publish a disabled, dropping, or failed binding";

fn is_retired(state: SemanticBindingState) -> bool {
    matches!(
        state,
        SemanticBindingState::Dropping
            | SemanticBindingState::Disabled
            | SemanticBindingState::Failed
    )
}

/// The only binding state S6 publishes from is `Building`.
pub(super) fn live_transition(
    current: &SemanticBinding,
) -> Result<SemanticBindingStateTransition, SemanticCodeError> {
    match current.durable_state {
        SemanticBindingState::Building => SemanticBindingStateTransition::create(
            current,
            SemanticBindingState::Live,
            "semantic_generation_activated",
        )
        .map_err(semantic_contract_error),
        SemanticBindingState::Pending => Err(refused(
            "S6 requires a durable Pending-to-Building transition before publication",
        )),
        SemanticBindingState::Live => Err(refused(
            "S6 binding is already live without an exact replay record",
        )),
        SemanticBindingState::Disabled
        | SemanticBindingState::Dropping
        | SemanticBindingState::Failed => Err(refused(RETIRED_BINDING)),
    }
}

/// The canonical `FinalizeGeneration` mutation of a completed,
/// generation-scoped S6 transition and the complete checkpoint it names.
fn finalization_mutation(
    transition: &SemanticStageTransition,
    checkpoint: &SemanticGenerationCheckpoint,
    artifact: &SemanticGenerationArtifact,
) -> Result<SemanticIndexMutation, SemanticCodeError> {
    transition.validate().map_err(semantic_contract_error)?;
    let intent = &transition.intent;
    ensure(
        (
            intent.stage,
            matches!(intent.scope, SemanticStageScope::Generation),
            transition.receipt.outcome,
        ) == (
            SemanticStage::ReconcileAndActivate,
            true,
            SemanticStageOutcome::Completed,
        ),
        "generation finalization requires a completed generation-scoped S6 transition",
    )?;
    checkpoint
        .require_complete()
        .map_err(semantic_contract_error)?;
    ensure(
        (
            checkpoint.binding_id.as_str(),
            checkpoint.binding_digest,
            checkpoint.generation,
            checkpoint.source_revision.as_str(),
            checkpoint.stage,
        ) == (
            intent.binding_id.as_str(),
            intent.binding_digest,
            intent.generation,
            intent.source_revision.as_str(),
            SemanticStage::ReconcileAndActivate,
        ),
        "S6 checkpoint does not match its leased transition",
    )?;
    let mutation = SemanticIndexMutation::FinalizeGeneration {
        checkpoint: Box::new(checkpoint.clone()),
        artifact: artifact.clone(),
    };
    mutation.validate().map_err(semantic_contract_error)?;
    Ok(mutation)
}

/// The active pointer an activation artifact publishes, bound to the durable
/// binding and to the checkpoint that completes it.
fn activation_pointer<'a>(
    artifact: &'a SemanticGenerationArtifact,
    binding: &SemanticBinding,
    checkpoint: &SemanticGenerationCheckpoint,
) -> Result<&'a SemanticActivePointer, SemanticCodeError> {
    let SemanticGenerationArtifact::Activation { target, pointer } = artifact else {
        return Err(refused(
            "S6 publication requires the canonical activation artifact",
        ));
    };
    target
        .validate_against_binding(binding)
        .map_err(semantic_contract_error)?;
    pointer.validate().map_err(semantic_contract_error)?;
    ensure(
        (
            pointer.binding_id.as_str(),
            pointer.binding_digest,
            pointer.generation,
            pointer.source_revision.as_str(),
            pointer.activation_receipt_digest,
            pointer.activated_at.as_str(),
        ) == (
            binding.binding_id.as_str(),
            binding.binding_digest,
            binding.generation,
            binding.source_revision.as_str(),
            checkpoint.checkpoint_digest,
            checkpoint.completed_at.as_str(),
        ),
        "S6 active pointer is not bound to the current checkpoint",
    )?;
    Ok(pointer)
}

/// The image's five parts, in the fixed order the digest and the code rows
/// both use.
fn image_parts(image: &SemanticGenerationImage) -> [(&'static str, &[u8]); 5] {
    [
        ("meta", image.index.codes.meta.as_slice()),
        ("codes", image.index.codes.codes.as_slice()),
        ("refine", image.index.codes.refine.as_slice()),
        ("ids", image.index.ids.as_slice()),
        ("manifest", image.manifest.as_slice()),
    ]
}

/// `sha256` over the image in a fixed part order, so the batch identity and the
/// "already durable" decision are both the content.
fn image_digest(image: &SemanticGenerationImage) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/semantic-ann-generation/v1\0");
    for (_, bytes) in image_parts(image) {
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }
    hex::encode(hasher.finalize())
}
