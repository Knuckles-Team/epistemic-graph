//! Binding reads: the serving head, one historical binding row, and the
//! binding a lifecycle write decides against and re-reads inside its write.

use super::record::{decode, row_bytes};
use super::{
    corrupt, kernel_error, refused, semantic_contract_error, SemanticCodeError, SemanticCodeStore,
};
use eg_storage::{
    ScopedRead, SemanticIndexOwner, SEMANTIC_BINDINGS, SEMANTIC_HEADS, SEMANTIC_SOURCE_PROGRESS,
};
use eg_transaction::AdmittedMutation;
use eg_types::semantic_index::{
    SemanticBinding, SemanticBindingState, SemanticDigest, SemanticIndexFilter,
    SemanticSourceProgress,
};
use redb::AccessGuard;

/// What a caller-attributed lifecycle write refuses when the binding it
/// decided against is gone, or no longer the binding it decided against, by
/// the time its admitted write re-reads it.
pub(super) struct BindingRace {
    pub(super) missing: &'static str,
    pub(super) changed: &'static str,
}

impl SemanticCodeStore {
    /// Read the binding authority selected by the serving head.  The head and
    /// binding rows are read from one kernel snapshot, so a caller cannot
    /// observe a generation from one binding paired with bytes from another.
    pub fn read_binding(&self) -> Result<Option<SemanticBinding>, SemanticCodeError> {
        let read = self.door.serving_read()?;
        self.read_binding_in(&read)
    }

    /// Apply the closed list filter against the authenticated owner snapshot.
    /// The service currently owns one binding file, so this is a bounded
    /// catalog lookup over at most the filter's validated entity set. A filter
    /// must never be silently ignored: source visibility and revision are
    /// proven from durable source-progress rows before the binding is returned.
    pub(crate) fn binding_matches_filter(
        &self,
        filter: &SemanticIndexFilter,
    ) -> Result<bool, SemanticCodeError> {
        filter.validate().map_err(semantic_contract_error)?;
        let Some(binding) = self.read_binding()? else {
            return Ok(false);
        };
        let required_revision = filter.required_source_revision.as_deref();
        if filter.source_entity_ids.is_empty() {
            return Ok(required_revision.is_none_or(|revision| revision == binding.source_revision));
        }
        let read = self.door.serving_read()?;
        let table = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?;
        for source_entity_id in &filter.source_entity_ids {
            let key = (
                self.tenant.as_str(),
                self.binding.as_str(),
                binding.generation,
                source_entity_id.as_str(),
            );
            let Some(raw) = row_bytes(table.get(key))? else {
                return Ok(false);
            };
            let progress: SemanticSourceProgress = decode(&raw)?;
            let in_generation = (
                progress.binding_id.as_str(),
                progress.binding_digest,
                progress.generation,
            ) == (
                binding.binding_id.as_str(),
                binding.binding_digest,
                binding.generation,
            );
            let at_revision =
                required_revision.is_none_or(|revision| progress.source_revision == revision);
            if !(in_generation && at_revision) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn read_binding_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
    ) -> Result<Option<SemanticBinding>, SemanticCodeError> {
        let heads = read
            .open_owner_table(SEMANTIC_HEADS)
            .map_err(kernel_error)?;
        let Some(generation) = head_value(heads.get(self.owner_key()))? else {
            return Ok(None);
        };
        self.read_binding_generation_in(read, generation)
    }

    /// Read one historical binding row from the serving snapshot.  The head
    /// may have advanced during a refresh while the active pointer still
    /// intentionally names the previous live generation.
    pub(super) fn read_binding_generation_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        generation: u64,
    ) -> Result<Option<SemanticBinding>, SemanticCodeError> {
        let bindings = read
            .open_owner_table(SEMANTIC_BINDINGS)
            .map_err(kernel_error)?;
        self.binding_row(
            bindings.get((self.tenant.as_str(), self.binding.as_str(), generation)),
            generation,
        )
        .map(Some)
    }

    pub(super) fn read_binding_in_write(
        &self,
        write: &AdmittedMutation<'_, SemanticIndexOwner>,
    ) -> Result<Option<SemanticBinding>, SemanticCodeError> {
        let generation = {
            let heads = write
                .open_read_table(SEMANTIC_HEADS)
                .map_err(kernel_error)?;
            head_value(heads.get(self.owner_key()))?
        };
        let Some(generation) = generation else {
            return Ok(None);
        };
        let bindings = write
            .open_read_table(SEMANTIC_BINDINGS)
            .map_err(kernel_error)?;
        self.binding_row(
            bindings.get((self.tenant.as_str(), self.binding.as_str(), generation)),
            generation,
        )
        .map(Some)
    }

    /// The `(tenant, binding)` key of every per-binding owner row.
    pub(super) fn owner_key(&self) -> (&str, &str) {
        (self.tenant.as_str(), self.binding.as_str())
    }

    /// The durable binding a caller state operation decides against.
    pub(super) fn caller_transition_source(
        &self,
        expected_generation: u64,
        next: SemanticBindingState,
    ) -> Result<SemanticBinding, SemanticCodeError> {
        let binding = self
            .read_binding()?
            .ok_or_else(|| refused("semantic state transition has no durable binding"))?;
        if binding.generation != expected_generation {
            return Err(refused("semantic state transition generation is stale"));
        }
        if !matches!(
            (binding.durable_state, next),
            (
                SemanticBindingState::Pending,
                SemanticBindingState::Building,
            ) | (SemanticBindingState::Live, SemanticBindingState::Disabled)
        ) {
            return Err(refused(
                "semantic state transition is not valid for the durable binding state",
            ));
        }
        Ok(binding)
    }

    /// The durable binding a caller drop decides against.
    pub(super) fn droppable_binding(
        &self,
        expected_generation: u64,
    ) -> Result<SemanticBinding, SemanticCodeError> {
        let binding = self
            .read_binding()?
            .ok_or_else(|| refused("semantic drop has no durable binding"))?;
        let droppable = matches!(
            binding.durable_state,
            SemanticBindingState::Disabled | SemanticBindingState::Failed
        );
        if binding.generation != expected_generation || !droppable {
            return Err(refused(
                "semantic drop requires the expected disabled or failed generation",
            ));
        }
        Ok(binding)
    }

    /// Re-read the binding inside the admitted write and require that it is
    /// still exactly the `(generation, digest, state)` the caller decided on.
    pub(super) fn binding_unchanged_in_write(
        &self,
        write: &AdmittedMutation<'_, SemanticIndexOwner>,
        decided: &SemanticBinding,
        race: &BindingRace,
    ) -> Result<SemanticBinding, SemanticCodeError> {
        let current = self
            .read_binding_in_write(write)?
            .ok_or_else(|| refused(race.missing))?;
        if binding_state_key(&current) != binding_state_key(decided) {
            return Err(refused(race.changed));
        }
        Ok(current)
    }

    fn binding_row<E: std::fmt::Display>(
        &self,
        found: Result<Option<AccessGuard<'_, &'static [u8]>>, E>,
        generation: u64,
    ) -> Result<SemanticBinding, SemanticCodeError> {
        let raw = row_bytes(found)?
            .ok_or_else(|| corrupt("semantic binding head names a missing binding row"))?;
        let binding: SemanticBinding = decode(&raw)?;
        if (
            binding.tenant_id.as_str(),
            binding.binding_id.as_str(),
            binding.generation,
        ) != (self.tenant.as_str(), self.binding.as_str(), generation)
        {
            return Err(corrupt(
                "semantic binding row does not match its serving head",
            ));
        }
        Ok(binding)
    }
}

/// The generation a binding head lookup found, if any.
pub(super) fn head_value<E: std::fmt::Display>(
    found: Result<Option<AccessGuard<'_, u64>>, E>,
) -> Result<Option<u64>, SemanticCodeError> {
    Ok(found.map_err(kernel_error)?.map(|value| value.value()))
}

/// The identity a lifecycle write decided against: re-reading any other value
/// means another writer moved the binding in between.
fn binding_state_key(
    binding: &SemanticBinding,
) -> (&str, u64, SemanticDigest, SemanticBindingState) {
    (
        binding.binding_id.as_str(),
        binding.generation,
        binding.binding_digest,
        binding.durable_state,
    )
}
