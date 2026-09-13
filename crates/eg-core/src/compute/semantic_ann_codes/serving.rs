//! The serving reads: the live generation the active pointer names, and one
//! generation's ANN image. They write nothing and create no authority.

use super::record::{decode_valid, row_bytes};
use super::rows::{read_part, PARTS};
use super::{corrupt, kernel_error, refused, SemanticCodeError, SemanticCodeStore};
use crate::compute::semantic::SemanticGenerationImage;
use eg_storage::{ScopedRead, SemanticIndexOwner, ANN_CODES, SEMANTIC_HEADS, SEMANTIC_POINTERS};
use eg_types::semantic_index::{SemanticActivePointer, SemanticBinding, SemanticBindingState};
use std::collections::BTreeMap;

impl SemanticCodeStore {
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

    pub(super) fn live_generation_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
    ) -> Result<Option<u64>, SemanticCodeError> {
        let owner = (self.tenant.as_str(), self.binding.as_str());
        let pointers = read
            .open_owner_table(SEMANTIC_POINTERS)
            .map_err(kernel_error)?;
        let Some(raw) = row_bytes(pointers.get(owner))? else {
            return Ok(None);
        };
        let pointer: SemanticActivePointer = decode_valid(&raw)?;
        let head_generation = read
            .open_owner_table(SEMANTIC_HEADS)
            .map_err(kernel_error)?
            .get(owner)
            .map_err(kernel_error)?
            .map(|value| value.value())
            .ok_or_else(|| corrupt("semantic active pointer has no durable binding head"))?;
        if pointer.generation > head_generation {
            return Err(corrupt(
                "semantic active pointer names a future binding generation",
            ));
        }
        let binding = self
            .read_binding_generation_in(read, pointer.generation)?
            .ok_or_else(|| corrupt("semantic active pointer has no durable binding authority"))?;
        if !pointer_serves(&pointer, &binding) {
            return Err(refused(
                "semantic active pointer is not bound to the serving binding",
            ));
        }
        Ok(Some(pointer.generation))
    }

    pub(super) fn read_generation_in(
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

/// An active pointer serves only the live binding row it names, field for
/// field.
fn pointer_serves(pointer: &SemanticActivePointer, binding: &SemanticBinding) -> bool {
    let named = (
        (
            pointer.tenant_id.as_str(),
            pointer.binding_id.as_str(),
            pointer.binding_digest,
            pointer.generation,
            pointer.source_revision.as_str(),
        ),
        (
            &pointer.vector_target_id,
            &pointer.lexical_index_identity,
            &pointer.ann_index_identity,
            &pointer.composite_policy_digest,
        ),
    );
    let bound = (
        (
            binding.tenant_id.as_str(),
            binding.binding_id.as_str(),
            binding.binding_digest,
            binding.generation,
            binding.source_revision.as_str(),
        ),
        (
            &binding.vector_target_id,
            &binding.lexical_index_identity,
            &binding.ann_index_identity,
            &binding.policy_digest,
        ),
    );
    named == bound && binding.durable_state == SemanticBindingState::Live
}
