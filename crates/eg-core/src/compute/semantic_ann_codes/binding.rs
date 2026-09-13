//! The binding lifecycle: admission, caller state transitions and drop.

use super::batch::{
    binding_subject, mutation_row, operation_batch_id, replayed_event, require_attribution,
    MetadataMutation,
};
use super::binding_read::{head_value, BindingRace};
use super::persist::{put_bytes_once, replace_bytes};
use super::record::{decode, encode, encode_valid, row_bytes};
use super::{
    corrupt, ensure, kernel_error, refused, semantic_contract_error, OperationAttribution,
    SemanticCodeError, SemanticCodeStore, SemanticMutationReceipt, SEMANTIC_BINDING_CREATED_TOPIC,
    SEMANTIC_BINDING_DROPPED_TOPIC, SEMANTIC_BINDING_STATE_TOPIC,
};
use eg_storage::{
    SemanticIndexOwner, SEMANTIC_BINDINGS, SEMANTIC_HEADS, SEMANTIC_POINTERS, SEMANTIC_STATES,
    SEMANTIC_TOMBSTONES,
};
use eg_transaction::{AdmittedMutation, AdmittedOwnerWrite};
use eg_types::contract::Nonce;
use eg_types::mutation_batch::MutationOutboxIntent;
use eg_types::semantic_index::{
    SemanticBinding, SemanticBindingState, SemanticBindingStateTransition, SemanticDigest,
    SemanticIndexMutation, SemanticTombstone, SemanticTombstoneDraft,
};
use eg_types::MutationBatch;
use redb::ReadableTable;
use std::collections::BTreeMap;

impl SemanticCodeStore {
    /// Admit the immutable binding definition and publish the first typed
    /// semantic outbox event.  This is the native admission half of S1: it
    /// persists the binding before any consumer can claim work, and it never
    /// performs projection, embedding, ANN, or activation work inline.
    pub(crate) fn store_binding(
        &self,
        binding: &SemanticBinding,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        self.ensure_binding_owner(binding)?;
        let (payload, mutation_digest) = pending_binding_admission(binding)?;
        let batch_id = format!("semantic-index:binding:{}", mutation_digest);
        let subject = binding_subject(binding.binding_digest);
        let outbox = binding_created_event(binding, payload.clone(), None);
        self.commit_maintenance(
            MetadataMutation {
                batch_id: &batch_id,
                event_type: "semantic_index_binding_stored",
                subject: &subject,
                mutation_digest,
            },
            vec![outbox],
            now_ms,
            None,
            |write, rows| {
                self.ensure_head_admits(write, binding)?;
                self.insert_binding_head(rows, binding, &payload)
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
        self.ensure_binding_owner(binding)?;
        require_attribution(
            actor,
            idempotency_key,
            "semantic operation requires verified actor and idempotency key",
        )?;
        let replayed = self.door.replay_operation_if_recorded(
            actor,
            idempotency_key,
            nonce,
            now_ms,
            |batch| replayed_binding_matches(batch, binding),
        )?;
        if let Some(receipt) = replayed {
            return Ok(receipt);
        }
        let (payload, mutation_digest) = pending_binding_admission(binding)?;
        let batch_id = operation_batch_id(idempotency_key);
        let subject = binding_subject(binding.binding_digest);
        let outbox = vec![binding_created_event(binding, payload.clone(), Some(actor))];
        self.commit_operation(
            MetadataMutation {
                batch_id: &batch_id,
                event_type: "semantic_binding_stored",
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
                if let Some(existing) = self.read_binding_in_write(write)? {
                    return Err(refused(if existing != *binding {
                        "semantic binding identity already names different bytes"
                    } else {
                        "semantic binding is already admitted; retry its original idempotency key"
                    }));
                }
                self.insert_binding_head(rows, binding, &payload)
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
        require_attribution(
            actor,
            idempotency_key,
            "semantic state transition requires verified actor and idempotency key",
        )?;
        if !matches!(
            next,
            SemanticBindingState::Building | SemanticBindingState::Disabled
        ) {
            return Err(refused(
                "caller state operation may only start a pending build or disable a live binding",
            ));
        }
        let replayed = self.door.replay_operation_if_recorded(
            actor,
            idempotency_key,
            nonce,
            now_ms,
            |batch| self.replayed_state_matches(batch, expected_generation, next),
        )?;
        if let Some(receipt) = replayed {
            return Ok(receipt);
        }
        let binding = self.caller_transition_source(expected_generation, next)?;
        let transition = SemanticBindingStateTransition::create(
            &binding,
            next,
            "caller_requested_semantic_binding_state",
        )
        .map_err(semantic_contract_error)?;
        let (_, mutation_digest) = mutation_row(&SemanticIndexMutation::SetBindingState {
            transition: transition.clone(),
        })?;
        let outbox = vec![binding_state_event(&binding, &transition, actor)?];
        let batch_id = operation_batch_id(idempotency_key);
        let subject = binding_subject(binding.binding_digest);
        self.commit_operation(
            MetadataMutation {
                batch_id: &batch_id,
                event_type: "semantic_binding_state_transition",
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
                let race = BindingRace {
                    missing: "semantic binding disappeared during state transition",
                    changed: "semantic binding state or generation changed during transition",
                };
                let current = self.binding_unchanged_in_write(write, &binding, &race)?;
                self.write_binding_state(rows, &current, &transition)?;
                if next == SemanticBindingState::Disabled {
                    self.remove_active_pointer(rows)?;
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
        require_attribution(
            actor,
            idempotency_key,
            "semantic drop requires verified actor and idempotency key",
        )?;
        let replayed = self.door.replay_operation_if_recorded(
            actor,
            idempotency_key,
            nonce,
            now_ms,
            |batch| self.replayed_drop_matches(batch, expected_generation),
        )?;
        if let Some(receipt) = replayed {
            return Ok(receipt);
        }
        let binding = self.droppable_binding(expected_generation)?;
        let tombstone = SemanticTombstone::create(SemanticTombstoneDraft {
            tenant_id: binding.tenant_id.clone(),
            binding_id: binding.binding_id.clone(),
            binding_digest: binding.binding_digest,
            generation: binding.generation,
            deleted_at: format!("unix-ms:{now_ms}"),
        })
        .map_err(semantic_contract_error)?;
        let dropping = SemanticBindingStateTransition::create(
            &binding,
            SemanticBindingState::Dropping,
            "caller_requested_semantic_binding_drop",
        )
        .map_err(semantic_contract_error)?;
        let (_, mutation_digest) = mutation_row(&SemanticIndexMutation::DeleteBinding {
            tombstone: Box::new(tombstone.clone()),
        })?;
        let outbox = vec![binding_dropped_event(&binding, &tombstone, actor)?];
        let batch_id = operation_batch_id(idempotency_key);
        let subject = binding_subject(binding.binding_digest);
        self.commit_operation(
            MetadataMutation {
                batch_id: &batch_id,
                event_type: "semantic_binding_dropped",
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
                let race = BindingRace {
                    missing: "semantic binding disappeared during drop",
                    changed: "semantic binding changed during drop",
                };
                let current = self.binding_unchanged_in_write(write, &binding, &race)?;
                self.write_binding_state(rows, &current, &dropping)?;
                let tombstone_bytes = encode(&tombstone)?;
                let mut tombstones = rows.open_table(SEMANTIC_TOMBSTONES).map_err(kernel_error)?;
                put_bytes_once(
                    &mut tombstones,
                    (
                        self.tenant.as_str(),
                        self.binding.as_str(),
                        expected_generation,
                    ),
                    &tombstone_bytes,
                )?;
                drop(tombstones);
                self.remove_active_pointer(rows)
            },
        )
    }

    /// Persist one binding authority row, then move this binding's changed
    /// state into the durable state machine: the binding row and its state
    /// receipt are replaced together inside the caller's admitted write.
    pub(super) fn write_binding_state(
        &self,
        rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
        current: &SemanticBinding,
        transition: &SemanticBindingStateTransition,
    ) -> Result<(), SemanticCodeError> {
        let updated = current
            .apply_state_transition(transition)
            .map_err(semantic_contract_error)?;
        let binding_bytes = encode(&updated)?;
        let mut bindings = rows.open_table(SEMANTIC_BINDINGS).map_err(kernel_error)?;
        replace_bytes(
            &mut bindings,
            (
                self.tenant.as_str(),
                self.binding.as_str(),
                updated.generation,
            ),
            &binding_bytes,
        )?;
        drop(bindings);
        let state_bytes = encode(transition)?;
        let mut states = rows.open_table(SEMANTIC_STATES).map_err(kernel_error)?;
        replace_bytes(
            &mut states,
            (self.tenant.as_str(), self.binding.as_str()),
            &state_bytes,
        )
    }

    fn ensure_binding_owner(&self, binding: &SemanticBinding) -> Result<(), SemanticCodeError> {
        binding.validate().map_err(semantic_contract_error)?;
        if (binding.tenant_id.as_str(), binding.binding_id.as_str())
            != (self.tenant.as_str(), self.binding.as_str())
        {
            return Err(refused(
                "semantic binding is outside this store's authenticated owner",
            ));
        }
        Ok(())
    }

    /// A maintenance admission may re-admit the same bytes but never replace a
    /// binding or move a head that names another generation.
    fn ensure_head_admits(
        &self,
        write: &AdmittedMutation<'_, SemanticIndexOwner>,
        binding: &SemanticBinding,
    ) -> Result<(), SemanticCodeError> {
        if self
            .read_binding_in_write(write)?
            .is_some_and(|existing| existing != *binding)
        {
            return Err(refused(
                "semantic binding identity already names different bytes",
            ));
        }
        let heads = write
            .open_read_table(SEMANTIC_HEADS)
            .map_err(kernel_error)?;
        if head_value(heads.get(self.owner_key()))?.is_some_and(|head| head != binding.generation) {
            return Err(refused(
                "semantic binding head already names another generation",
            ));
        }
        Ok(())
    }

    fn insert_binding_head(
        &self,
        rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
        binding: &SemanticBinding,
        binding_bytes: &[u8],
    ) -> Result<(), SemanticCodeError> {
        let owner = (self.tenant.as_str(), self.binding.as_str());
        rows.open_table(SEMANTIC_BINDINGS)
            .map_err(kernel_error)?
            .insert((owner.0, owner.1, binding.generation), binding_bytes)
            .map_err(kernel_error)?;
        rows.open_table(SEMANTIC_HEADS)
            .map_err(kernel_error)?
            .insert(owner, binding.generation)
            .map_err(kernel_error)?;
        Ok(())
    }

    fn replayed_state_matches(
        &self,
        batch: &MutationBatch,
        expected_generation: u64,
        next: SemanticBindingState,
    ) -> Result<(), SemanticCodeError> {
        let transition: SemanticBindingStateTransition =
            replayed_event(batch, "semantic state replay batch has no transition event")?;
        ensure(
            (
                transition.binding_id.as_str(),
                transition.generation,
                transition.next,
            ) == (self.binding.as_str(), expected_generation, next),
            "semantic state idempotency key names different content",
        )
    }

    fn replayed_drop_matches(
        &self,
        batch: &MutationBatch,
        expected_generation: u64,
    ) -> Result<(), SemanticCodeError> {
        let tombstone: SemanticTombstone =
            replayed_event(batch, "semantic drop replay batch has no tombstone event")?;
        ensure(
            (
                tombstone.tenant_id.as_str(),
                tombstone.binding_id.as_str(),
                tombstone.generation,
            ) == (
                self.tenant.as_str(),
                self.binding.as_str(),
                expected_generation,
            ),
            "semantic drop idempotency key names different content",
        )
    }

    fn remove_active_pointer(
        &self,
        rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    ) -> Result<(), SemanticCodeError> {
        rows.open_table(SEMANTIC_POINTERS)
            .map_err(kernel_error)?
            .remove((self.tenant.as_str(), self.binding.as_str()))
            .map_err(kernel_error)?;
        Ok(())
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
    let mut bindings = rows.open_table(SEMANTIC_BINDINGS).map_err(kernel_error)?;
    let prior_raw = row_bytes(bindings.get((tenant, binding, generation)))?
        .ok_or_else(|| corrupt("S6 active pointer names a missing prior binding"))?;
    let mut prior: SemanticBinding = decode(&prior_raw)?;
    if prior.durable_state != SemanticBindingState::Live {
        return Err(refused("S6 prior active generation is not durably live"));
    }
    prior.durable_state = SemanticBindingState::Disabled;
    let prior_bytes = encode_valid(&prior)?;
    replace_bytes(&mut bindings, (tenant, binding, generation), &prior_bytes)
}

/// A binding admission starts pending; its content digest addresses the
/// `StoreBinding` mutation the ledger records.
fn pending_binding_admission(
    binding: &SemanticBinding,
) -> Result<(Vec<u8>, SemanticDigest), SemanticCodeError> {
    if binding.durable_state != SemanticBindingState::Pending {
        return Err(refused(
            "semantic binding admission requires pending durable state",
        ));
    }
    let payload = encode(binding)?;
    let (_, mutation_digest) = mutation_row(&SemanticIndexMutation::StoreBinding {
        binding: Box::new(binding.clone()),
    })?;
    Ok((payload, mutation_digest))
}

fn replayed_binding_matches(
    batch: &MutationBatch,
    binding: &SemanticBinding,
) -> Result<(), SemanticCodeError> {
    let existing: SemanticBinding =
        replayed_event(batch, "semantic binding replay batch has no binding event")?;
    ensure(
        existing == *binding,
        "semantic binding idempotency key names different content",
    )
}

fn binding_created_event(
    binding: &SemanticBinding,
    payload: Vec<u8>,
    actor: Option<&str>,
) -> MutationOutboxIntent {
    let mut headers = BTreeMap::from([
        (
            "schema".to_string(),
            eg_types::semantic_index::SEMANTIC_BINDING_SCHEMA.to_string(),
        ),
        ("binding_id".to_string(), binding.binding_id.clone()),
        (
            "binding_digest".to_string(),
            binding.binding_digest.to_string(),
        ),
        ("generation".to_string(), binding.generation.to_string()),
        (
            "source_revision".to_string(),
            binding.source_revision.clone(),
        ),
    ]);
    if let Some(actor) = actor {
        headers.insert("actor".to_string(), actor.to_string());
    }
    MutationOutboxIntent {
        topic: SEMANTIC_BINDING_CREATED_TOPIC.to_string(),
        key: format!("{}:{}", binding.binding_id, binding.generation),
        payload,
        headers,
    }
}

fn binding_state_event(
    binding: &SemanticBinding,
    transition: &SemanticBindingStateTransition,
    actor: &str,
) -> Result<MutationOutboxIntent, SemanticCodeError> {
    Ok(MutationOutboxIntent {
        topic: SEMANTIC_BINDING_STATE_TOPIC.to_string(),
        key: format!(
            "{}:{}:{}",
            binding.binding_id,
            binding.generation,
            transition.next.as_str()
        ),
        payload: encode(transition)?,
        headers: BTreeMap::from([
            (
                "schema".to_string(),
                eg_types::semantic_index::SEMANTIC_BINDING_STATE_TRANSITION_SCHEMA.to_string(),
            ),
            ("binding_id".to_string(), binding.binding_id.clone()),
            ("generation".to_string(), binding.generation.to_string()),
            ("actor".to_string(), actor.to_string()),
        ]),
    })
}

fn binding_dropped_event(
    binding: &SemanticBinding,
    tombstone: &SemanticTombstone,
    actor: &str,
) -> Result<MutationOutboxIntent, SemanticCodeError> {
    Ok(MutationOutboxIntent {
        topic: SEMANTIC_BINDING_DROPPED_TOPIC.to_string(),
        key: format!("{}:{}", binding.binding_id, binding.generation),
        payload: encode(tombstone)?,
        headers: BTreeMap::from([
            (
                "schema".to_string(),
                eg_types::semantic_index::SEMANTIC_TOMBSTONE_SCHEMA.to_string(),
            ),
            ("actor".to_string(), actor.to_string()),
        ]),
    })
}
