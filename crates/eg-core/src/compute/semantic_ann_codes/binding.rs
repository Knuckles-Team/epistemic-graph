//! The binding lifecycle: admission, caller state transitions and drop.

use super::batch::{semantic_digest, MetadataMutation};
use super::persist::{put_bytes_once, replace_bytes};
use super::{
    kernel_error, semantic_contract_error, OperationAttribution, SemanticCodeError,
    SemanticCodeStore, SemanticMutationReceipt, SEMANTIC_BINDING_CREATED_TOPIC,
    SEMANTIC_BINDING_DROPPED_TOPIC, SEMANTIC_BINDING_STATE_TOPIC,
};
use eg_storage::{
    ScopedRead, SemanticIndexOwner, SEMANTIC_BINDINGS, SEMANTIC_HEADS, SEMANTIC_POINTERS,
    SEMANTIC_SOURCE_PROGRESS, SEMANTIC_STATES, SEMANTIC_TOMBSTONES,
};
use eg_transaction::{AdmittedMutation, AdmittedOwnerWrite};
use eg_types::contract::Nonce;
use eg_types::mutation_batch::MutationOutboxIntent;
use eg_types::semantic_index::{
    SemanticBinding, SemanticBindingState, SemanticBindingStateTransition, SemanticIndexFilter,
    SemanticIndexMutation, SemanticSourceProgress, SemanticTombstone, SemanticTombstoneDraft,
};
use redb::ReadableTable;
use std::collections::BTreeMap;

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
        if filter.source_entity_ids.is_empty() {
            return Ok(filter
                .required_source_revision
                .as_deref()
                .is_none_or(|revision| revision == binding.source_revision.as_str()));
        }
        let read = self.door.serving_read()?;
        let table = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?;
        for source_entity_id in &filter.source_entity_ids {
            let Some(raw) = table
                .get((
                    self.tenant.as_str(),
                    self.binding.as_str(),
                    binding.generation,
                    source_entity_id.as_str(),
                ))
                .map_err(kernel_error)?
                .map(|value| value.value().to_vec())
            else {
                return Ok(false);
            };
            let progress = SemanticSourceProgress::from_canonical_cbor(&raw)
                .map_err(semantic_contract_error)?;
            if progress.binding_id != binding.binding_id
                || progress.binding_digest != binding.binding_digest
                || progress.generation != binding.generation
                || filter
                    .required_source_revision
                    .as_deref()
                    .is_some_and(|revision| progress.source_revision.as_str() != revision)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Admit the immutable binding definition and publish the first typed
    /// semantic outbox event.  This is the native admission half of S1: it
    /// persists the binding before any consumer can claim work, and it never
    /// performs projection, embedding, ANN, or activation work inline.
    pub(crate) fn store_binding(
        &self,
        binding: &SemanticBinding,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        binding.validate().map_err(semantic_contract_error)?;
        if binding.tenant_id != self.tenant || binding.binding_id != self.binding {
            return Err(SemanticCodeError::Refused(
                "semantic binding is outside this store's authenticated owner".to_string(),
            ));
        }
        if binding.durable_state != eg_types::semantic_index::SemanticBindingState::Pending {
            return Err(SemanticCodeError::Refused(
                "semantic binding admission requires pending durable state".to_string(),
            ));
        }
        let payload = binding
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation = SemanticIndexMutation::StoreBinding {
            binding: Box::new(binding.clone()),
        };
        mutation.validate().map_err(semantic_contract_error)?;
        let mutation_bytes = mutation
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation_digest = semantic_digest(&mutation_bytes);
        let batch_id = format!("semantic-index:binding:{}", mutation_digest);
        let mut headers = BTreeMap::new();
        headers.insert(
            "schema".to_string(),
            eg_types::semantic_index::SEMANTIC_BINDING_SCHEMA.to_string(),
        );
        headers.insert("binding_id".to_string(), binding.binding_id.clone());
        headers.insert(
            "binding_digest".to_string(),
            binding.binding_digest.to_string(),
        );
        headers.insert("generation".to_string(), binding.generation.to_string());
        headers.insert(
            "source_revision".to_string(),
            binding.source_revision.clone(),
        );
        let outbox = MutationOutboxIntent {
            topic: SEMANTIC_BINDING_CREATED_TOPIC.to_string(),
            key: format!("{}:{}", binding.binding_id, binding.generation),
            payload: payload.clone(),
            headers,
        };
        let owner = self.door.owner();
        let payload_digest = mutation_digest;
        let binding_bytes = payload;
        self.door.commit_metadata(
            |version| {
                self.metadata_batch(
                    owner,
                    version,
                    MetadataMutation {
                        batch_id: &batch_id,
                        event_type: "semantic_index_binding_stored",
                        subject: &format!("binding:{}", binding.binding_digest),
                        mutation_digest: payload_digest,
                    },
                    vec![outbox],
                    now_ms,
                )
            },
            mutation_digest,
            now_ms,
            |write, rows| {
                if let Some(existing) = self.read_binding_in_write(write)? {
                    if existing != *binding {
                        return Err(SemanticCodeError::Refused(
                            "semantic binding identity already names different bytes".to_string(),
                        ));
                    }
                }
                let heads = write
                    .open_read_table(SEMANTIC_HEADS)
                    .map_err(kernel_error)?;
                if let Some(head) = heads
                    .get((self.tenant.as_str(), self.binding.as_str()))
                    .map_err(kernel_error)?
                    .map(|value| value.value())
                {
                    if head != binding.generation {
                        return Err(SemanticCodeError::Refused(
                            "semantic binding head already names another generation".to_string(),
                        ));
                    }
                }
                drop(heads);
                rows.open_table(SEMANTIC_BINDINGS)
                    .map_err(kernel_error)?
                    .insert(
                        (
                            self.tenant.as_str(),
                            self.binding.as_str(),
                            binding.generation,
                        ),
                        binding_bytes.as_slice(),
                    )
                    .map_err(kernel_error)?;
                rows.open_table(SEMANTIC_HEADS)
                    .map_err(kernel_error)?
                    .insert(
                        (self.tenant.as_str(), self.binding.as_str()),
                        binding.generation,
                    )
                    .map_err(kernel_error)?;
                Ok(())
            },
        )
    }

    /// Caller-attributed binding admission. Unlike engine maintenance (used
    /// only by internal lifecycle producers), this path carries the verified
    /// actor, stable idempotency key and attempt nonce into the kernel's
    /// operation replay ledger.
    pub(crate) fn store_binding_operation(
        &self,
        binding: &SemanticBinding,
        now_ms: u64,
        actor: &str,
        idempotency_key: &str,
        nonce: Nonce,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        binding.validate().map_err(semantic_contract_error)?;
        if binding.tenant_id != self.tenant || binding.binding_id != self.binding {
            return Err(SemanticCodeError::Refused(
                "semantic binding is outside this store's authenticated owner".to_string(),
            ));
        }
        if actor.trim().is_empty() || idempotency_key.trim().is_empty() {
            return Err(SemanticCodeError::Refused(
                "semantic operation requires verified actor and idempotency key".to_string(),
            ));
        }
        if let Some(receipt) = self.door.replay_operation_if_recorded(
            actor,
            idempotency_key,
            nonce,
            now_ms,
            |batch| {
                let existing = batch.outbox.first().ok_or_else(|| {
                    SemanticCodeError::Corrupt(
                        "semantic binding replay batch has no binding event".to_string(),
                    )
                })?;
                let existing_binding = SemanticBinding::from_canonical_cbor(&existing.payload)
                    .map_err(semantic_contract_error)?;
                if existing_binding == *binding {
                    Ok(())
                } else {
                    Err(SemanticCodeError::Refused(
                        "semantic binding idempotency key names different content".to_string(),
                    ))
                }
            },
        )? {
            return Ok(receipt);
        }
        if binding.durable_state != eg_types::semantic_index::SemanticBindingState::Pending {
            return Err(SemanticCodeError::Refused(
                "semantic binding admission requires pending durable state".to_string(),
            ));
        }
        let payload = binding
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation = SemanticIndexMutation::StoreBinding {
            binding: Box::new(binding.clone()),
        };
        mutation.validate().map_err(semantic_contract_error)?;
        let mutation_bytes = mutation
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation_digest = semantic_digest(&mutation_bytes);
        let mut headers = BTreeMap::new();
        headers.insert(
            "schema".to_string(),
            eg_types::semantic_index::SEMANTIC_BINDING_SCHEMA.to_string(),
        );
        headers.insert("binding_id".to_string(), binding.binding_id.clone());
        headers.insert(
            "binding_digest".to_string(),
            binding.binding_digest.to_string(),
        );
        headers.insert("generation".to_string(), binding.generation.to_string());
        headers.insert(
            "source_revision".to_string(),
            binding.source_revision.clone(),
        );
        headers.insert("actor".to_string(), actor.to_string());
        let outbox = vec![MutationOutboxIntent {
            topic: SEMANTIC_BINDING_CREATED_TOPIC.to_string(),
            key: format!("{}:{}", binding.binding_id, binding.generation),
            payload,
            headers,
        }];
        let batch_id = format!("semantic-index:operation:{idempotency_key}");
        let binding_bytes = binding
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let owner = self.door.owner();
        self.door.commit_metadata(
            |version| {
                self.metadata_operation_batch(
                    owner,
                    version,
                    MetadataMutation {
                        batch_id: &batch_id,
                        event_type: "semantic_binding_stored",
                        subject: &format!("binding:{}", binding.binding_digest),
                        mutation_digest,
                    },
                    outbox.clone(),
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
                if let Some(existing) = self.read_binding_in_write(write)? {
                    if existing != *binding {
                        return Err(SemanticCodeError::Refused(
                            "semantic binding identity already names different bytes".to_string(),
                        ));
                    }
                    return Err(SemanticCodeError::Refused(
                        "semantic binding is already admitted; retry its original idempotency key"
                            .to_string(),
                    ));
                }
                rows.open_table(SEMANTIC_BINDINGS)
                    .map_err(kernel_error)?
                    .insert(
                        (
                            self.tenant.as_str(),
                            self.binding.as_str(),
                            binding.generation,
                        ),
                        binding_bytes.as_slice(),
                    )
                    .map_err(kernel_error)?;
                rows.open_table(SEMANTIC_HEADS)
                    .map_err(kernel_error)?
                    .insert(
                        (self.tenant.as_str(), self.binding.as_str()),
                        binding.generation,
                    )
                    .map_err(kernel_error)?;
                Ok(())
            },
        )
    }

    /// Apply one caller-attributed binding state transition. The current
    /// binding row is re-read through the admitted write before the transition
    /// and pointer change, so an expected generation/state cannot be bypassed
    /// by a stale request body.
    pub(crate) fn transition_binding_operation(
        &self,
        expected_generation: u64,
        next: SemanticBindingState,
        now_ms: u64,
        actor: &str,
        idempotency_key: &str,
        nonce: Nonce,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        if actor.trim().is_empty() || idempotency_key.trim().is_empty() {
            return Err(SemanticCodeError::Refused(
                "semantic state transition requires verified actor and idempotency key".to_string(),
            ));
        }
        if !matches!(
            next,
            SemanticBindingState::Building | SemanticBindingState::Disabled
        ) {
            return Err(SemanticCodeError::Refused(
                "caller state operation may only start a pending build or disable a live binding"
                    .to_string(),
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
                        "semantic state replay batch has no transition event".to_string(),
                    )
                })?;
                let transition =
                    SemanticBindingStateTransition::from_canonical_cbor(&event.payload)
                        .map_err(semantic_contract_error)?;
                if transition.binding_id == self.binding
                    && transition.generation == expected_generation
                    && transition.next == next
                {
                    Ok(())
                } else {
                    Err(SemanticCodeError::Refused(
                        "semantic state idempotency key names different content".to_string(),
                    ))
                }
            },
        )? {
            return Ok(receipt);
        }
        let binding = self.read_binding()?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "semantic state transition has no durable binding".to_string(),
            )
        })?;
        if binding.generation != expected_generation {
            return Err(SemanticCodeError::Refused(
                "semantic state transition generation is stale".to_string(),
            ));
        }
        if !matches!(
            (binding.durable_state, next),
            (
                SemanticBindingState::Pending,
                SemanticBindingState::Building,
            ) | (SemanticBindingState::Live, SemanticBindingState::Disabled)
        ) {
            return Err(SemanticCodeError::Refused(
                "semantic state transition is not valid for the durable binding state".to_string(),
            ));
        }
        let transition = SemanticBindingStateTransition::create(
            &binding,
            next,
            "caller_requested_semantic_binding_state",
        )
        .map_err(semantic_contract_error)?;
        let mutation = SemanticIndexMutation::SetBindingState {
            transition: transition.clone(),
        };
        mutation.validate().map_err(semantic_contract_error)?;
        let mutation_bytes = mutation
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation_digest = semantic_digest(&mutation_bytes);
        let payload = transition
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mut headers = BTreeMap::new();
        headers.insert(
            "schema".to_string(),
            eg_types::semantic_index::SEMANTIC_BINDING_STATE_TRANSITION_SCHEMA.to_string(),
        );
        headers.insert("binding_id".to_string(), binding.binding_id.clone());
        headers.insert("generation".to_string(), binding.generation.to_string());
        headers.insert("actor".to_string(), actor.to_string());
        let outbox = vec![MutationOutboxIntent {
            topic: SEMANTIC_BINDING_STATE_TOPIC.to_string(),
            key: format!(
                "{}:{}:{}",
                binding.binding_id,
                binding.generation,
                next.as_str()
            ),
            payload,
            headers,
        }];
        let batch_id = format!("semantic-index:operation:{idempotency_key}");
        let binding_id = self.binding.clone();
        let tenant = self.tenant.clone();
        self.door.commit_metadata(
            |version| {
                self.metadata_operation_batch(
                    self.door.owner(),
                    version,
                    MetadataMutation {
                        batch_id: &batch_id,
                        event_type: "semantic_binding_state_transition",
                        subject: &format!("binding:{}", binding.binding_digest),
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
                        "semantic binding disappeared during state transition".to_string(),
                    )
                })?;
                if current.binding_id != binding_id
                    || current.generation != expected_generation
                    || current.binding_digest != binding.binding_digest
                    || current.durable_state != transition.expected
                {
                    return Err(SemanticCodeError::Refused(
                        "semantic binding state or generation changed during transition"
                            .to_string(),
                    ));
                }
                let updated = current
                    .apply_state_transition(&transition)
                    .map_err(semantic_contract_error)?;
                let binding_bytes = updated
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut bindings = rows.open_table(SEMANTIC_BINDINGS).map_err(kernel_error)?;
                replace_bytes(
                    &mut bindings,
                    (tenant.as_str(), binding_id.as_str(), expected_generation),
                    &binding_bytes,
                )?;
                drop(bindings);
                let state_bytes = transition
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut states = rows.open_table(SEMANTIC_STATES).map_err(kernel_error)?;
                replace_bytes(
                    &mut states,
                    (tenant.as_str(), binding_id.as_str()),
                    &state_bytes,
                )?;
                drop(states);
                if next == SemanticBindingState::Disabled {
                    rows.open_table(SEMANTIC_POINTERS)
                        .map_err(kernel_error)?
                        .remove((tenant.as_str(), binding_id.as_str()))
                        .map_err(kernel_error)?;
                }
                Ok(())
            },
        )
    }

    /// Drop is a two-proof lifecycle operation: the binding must first be in
    /// Disabled/Failed state, then the canonical tombstone and Dropping state
    /// are written in the same caller-attributed mutation. The old binding row
    /// remains as the durable tombstone's identity anchor.
    pub(crate) fn drop_binding_operation(
        &self,
        expected_generation: u64,
        now_ms: u64,
        actor: &str,
        idempotency_key: &str,
        nonce: Nonce,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        if actor.trim().is_empty() || idempotency_key.trim().is_empty() {
            return Err(SemanticCodeError::Refused(
                "semantic drop requires verified actor and idempotency key".to_string(),
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
                        "semantic drop replay batch has no tombstone event".to_string(),
                    )
                })?;
                let tombstone = SemanticTombstone::from_canonical_cbor(&event.payload)
                    .map_err(semantic_contract_error)?;
                if tombstone.tenant_id == self.tenant
                    && tombstone.binding_id == self.binding
                    && tombstone.generation == expected_generation
                {
                    Ok(())
                } else {
                    Err(SemanticCodeError::Refused(
                        "semantic drop idempotency key names different content".to_string(),
                    ))
                }
            },
        )? {
            return Ok(receipt);
        }
        let binding = self.read_binding()?.ok_or_else(|| {
            SemanticCodeError::Refused("semantic drop has no durable binding".to_string())
        })?;
        if binding.generation != expected_generation
            || !matches!(
                binding.durable_state,
                SemanticBindingState::Disabled | SemanticBindingState::Failed
            )
        {
            return Err(SemanticCodeError::Refused(
                "semantic drop requires the expected disabled or failed generation".to_string(),
            ));
        }
        let tombstone = SemanticTombstone::create(SemanticTombstoneDraft {
            tenant_id: binding.tenant_id.clone(),
            binding_id: binding.binding_id.clone(),
            binding_digest: binding.binding_digest,
            generation: binding.generation,
            deleted_at: format!("unix-ms:{now_ms}"),
        })
        .map_err(semantic_contract_error)?;
        let state_transition = SemanticBindingStateTransition::create(
            &binding,
            SemanticBindingState::Dropping,
            "caller_requested_semantic_binding_drop",
        )
        .map_err(semantic_contract_error)?;
        let mutation = SemanticIndexMutation::DeleteBinding {
            tombstone: Box::new(tombstone.clone()),
        };
        mutation.validate().map_err(semantic_contract_error)?;
        let mutation_bytes = mutation
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation_digest = semantic_digest(&mutation_bytes);
        let payload = tombstone
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let outbox = vec![MutationOutboxIntent {
            topic: SEMANTIC_BINDING_DROPPED_TOPIC.to_string(),
            key: format!("{}:{}", binding.binding_id, binding.generation),
            payload,
            headers: BTreeMap::from([
                (
                    "schema".to_string(),
                    eg_types::semantic_index::SEMANTIC_TOMBSTONE_SCHEMA.to_string(),
                ),
                ("actor".to_string(), actor.to_string()),
            ]),
        }];
        let batch_id = format!("semantic-index:operation:{idempotency_key}");
        let binding_id = self.binding.clone();
        let tenant = self.tenant.clone();
        self.door.commit_metadata(
            |version| {
                self.metadata_operation_batch(
                    self.door.owner(),
                    version,
                    MetadataMutation {
                        batch_id: &batch_id,
                        event_type: "semantic_binding_dropped",
                        subject: &format!("binding:{}", binding.binding_digest),
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
                        "semantic binding disappeared during drop".to_string(),
                    )
                })?;
                if current.binding_id != binding_id
                    || current.generation != expected_generation
                    || current.binding_digest != binding.binding_digest
                    || current.durable_state != binding.durable_state
                {
                    return Err(SemanticCodeError::Refused(
                        "semantic binding changed during drop".to_string(),
                    ));
                }
                let updated = current
                    .apply_state_transition(&state_transition)
                    .map_err(semantic_contract_error)?;
                let binding_bytes = updated
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut bindings = rows.open_table(SEMANTIC_BINDINGS).map_err(kernel_error)?;
                replace_bytes(
                    &mut bindings,
                    (tenant.as_str(), binding_id.as_str(), expected_generation),
                    &binding_bytes,
                )?;
                drop(bindings);
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
                let tombstone_bytes = tombstone
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut tombstones = rows.open_table(SEMANTIC_TOMBSTONES).map_err(kernel_error)?;
                put_bytes_once(
                    &mut tombstones,
                    (tenant.as_str(), binding_id.as_str(), expected_generation),
                    &tombstone_bytes,
                )?;
                drop(tombstones);
                rows.open_table(SEMANTIC_POINTERS)
                    .map_err(kernel_error)?
                    .remove((tenant.as_str(), binding_id.as_str()))
                    .map_err(kernel_error)?;
                Ok(())
            },
        )
    }

    pub(super) fn read_binding_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
    ) -> Result<Option<SemanticBinding>, SemanticCodeError> {
        let generation = read
            .open_owner_table(SEMANTIC_HEADS)
            .map_err(kernel_error)?
            .get((self.tenant.as_str(), self.binding.as_str()))
            .map_err(kernel_error)?
            .map(|value| value.value());
        let Some(generation) = generation else {
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
        let raw = read
            .open_owner_table(SEMANTIC_BINDINGS)
            .map_err(kernel_error)?
            .get((self.tenant.as_str(), self.binding.as_str(), generation))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "semantic binding head names a missing binding row".to_string(),
                )
            })?;
        let binding =
            SemanticBinding::from_canonical_cbor(&raw).map_err(semantic_contract_error)?;
        if binding.tenant_id != self.tenant
            || binding.binding_id != self.binding
            || binding.generation != generation
        {
            return Err(SemanticCodeError::Corrupt(
                "semantic binding row does not match its serving head".to_string(),
            ));
        }
        Ok(Some(binding))
    }

    pub(super) fn read_binding_in_write(
        &self,
        write: &AdmittedMutation<'_, SemanticIndexOwner>,
    ) -> Result<Option<SemanticBinding>, SemanticCodeError> {
        let generation = write
            .open_read_table(SEMANTIC_HEADS)
            .map_err(kernel_error)?
            .get((self.tenant.as_str(), self.binding.as_str()))
            .map_err(kernel_error)?
            .map(|value| value.value());
        let Some(generation) = generation else {
            return Ok(None);
        };
        let raw = write
            .open_read_table(SEMANTIC_BINDINGS)
            .map_err(kernel_error)?
            .get((self.tenant.as_str(), self.binding.as_str(), generation))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "semantic binding head names a missing binding row".to_string(),
                )
            })?;
        let binding =
            SemanticBinding::from_canonical_cbor(&raw).map_err(semantic_contract_error)?;
        if binding.tenant_id != self.tenant
            || binding.binding_id != self.binding
            || binding.generation != generation
        {
            return Err(SemanticCodeError::Corrupt(
                "semantic binding row does not match its serving head".to_string(),
            ));
        }
        Ok(Some(binding))
    }
}

/// Move the generation named by an existing active pointer out of `Live`
/// while the successor generation is being published. This helper is called
/// inside the same admitted S6 write as the successor binding and pointer;
/// callers cannot observe a pointer that names two live generations.
pub(super) fn demote_prior_live_binding_in_write(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    generation: u64,
) -> Result<(), SemanticCodeError> {
    let prior_raw = rows
        .open_table(SEMANTIC_BINDINGS)
        .map_err(kernel_error)?
        .get((tenant, binding, generation))
        .map_err(kernel_error)?
        .map(|value| value.value().to_vec())
        .ok_or_else(|| {
            SemanticCodeError::Corrupt(
                "S6 active pointer names a missing prior binding".to_string(),
            )
        })?;
    let mut prior =
        SemanticBinding::from_canonical_cbor(&prior_raw).map_err(semantic_contract_error)?;
    if prior.durable_state != SemanticBindingState::Live {
        return Err(SemanticCodeError::Refused(
            "S6 prior active generation is not durably live".to_string(),
        ));
    }
    prior.durable_state = SemanticBindingState::Disabled;
    prior.validate().map_err(semantic_contract_error)?;
    let prior_bytes = prior.to_canonical_cbor().map_err(semantic_contract_error)?;
    let mut bindings = rows.open_table(SEMANTIC_BINDINGS).map_err(kernel_error)?;
    replace_bytes(&mut bindings, (tenant, binding, generation), &prior_bytes)
}
