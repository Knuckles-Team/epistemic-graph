//! Durable Agent Component write and pin-admission phases.
//!
//! The public store type remains in the parent module; this child keeps the
//! mutation lifecycle together so validation, replay admission, owner rows,
//! receipts, and commit stay reviewable without one monolithic owner file.

use super::*;

use eg_types::mutation::MutationReceipt;

struct ComponentAdmission {
    nonce: eg_types::authority::NonceReplayKey,
    next_revision: u64,
    replay_context: AgentLibraryMutationContext,
    operation: eg_types::authority::OperationReplayIdentity,
    replay: ReplayResolution,
}

struct ComponentAdmissionRequest<'a> {
    owner: &'a OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &'a AgentLibraryMutationContext,
    purpose_id: &'a str,
    operation_kind: &'a str,
    component_id: &'a str,
    expected_revision: u64,
    definition_digest: Option<&'a str>,
}

fn prepare_component_admission(
    mutations: &eg_transaction::MutationKernel,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    request: ComponentAdmissionRequest<'_>,
) -> Result<ComponentAdmission, String> {
    let nonce = resolve_nonce_first(mutations, txn, request.context)?;
    let next_revision = next_revision(request.expected_revision)?;
    let replay_context = admitted_context(request.context, request.purpose_id)?;
    let operation = agent_library_operation_identity(
        request.owner,
        &replay_context,
        request.operation_kind,
        request.component_id,
        request.expected_revision,
        request.definition_digest,
    )?;
    let replay = mutations.resolve_replay(txn, &operation, &nonce)?;
    Ok(ComponentAdmission {
        nonce,
        next_revision,
        replay_context,
        operation,
        replay,
    })
}

fn prepare_component_entry(
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    request: &AgentComponentPublishRequest,
    next_revision: u64,
    replay_context: &AgentLibraryMutationContext,
) -> Result<AgentComponentEntry, String> {
    super::pins::resolve_component_pins_in_write(
        txn,
        &request.context.tenant_id,
        "agent component",
        &request.component.pinned_components(),
    )?;
    AgentComponentEntry::create(
        request.component.clone(),
        next_revision,
        AgentLibraryLifecycle::Published,
        replay_context.created_at_ms,
        replay_context.created_at_ms,
    )
}

fn prepare_component_retirement(
    store: &AgentLibraryStore,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    request: &AgentComponentRetireRequest,
    expected_revision: u64,
    next_revision: u64,
    replay_context: &AgentLibraryMutationContext,
) -> Result<AgentComponentEntry, String> {
    let current = store
        .component_at_revision_in_write(
            txn,
            &request.context.tenant_id,
            &request.component_id,
            expected_revision,
        )?
        .ok_or_else(|| "agent component has no revision to retire".to_string())?;
    current.retire(next_revision, replay_context.created_at_ms)
}

struct ComponentCommitPlan {
    admitted_context: AgentLibraryMutationContext,
    expected_revision: u64,
    entry: AgentComponentEntry,
    entry_bytes: Vec<u8>,
    event_bytes: Vec<u8>,
    batch: MutationBatch,
    source_version: u64,
    committed_version: u64,
    stable_result: AgentComponentCommittedResult,
    result_bytes: Vec<u8>,
}

/// The caller-supplied facts a component commit plan is built from, grouped
/// into one type so `build_component_commit_plan` takes one request
/// parameter instead of five separate ones the compiler cannot help keep in
/// order.
struct ComponentCommitRequest<'a> {
    expected_revision: u64,
    kind: AgentComponentMutationKind,
    entry: AgentComponentEntry,
    operation: &'a eg_types::authority::OperationReplayIdentity,
    nonce: &'a eg_types::authority::NonceReplayKey,
}

fn build_component_commit_plan(
    store: &AgentLibraryStore,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    request: ComponentCommitRequest<'_>,
) -> Result<ComponentCommitPlan, String> {
    let ComponentCommitRequest {
        expected_revision,
        kind,
        entry,
        operation,
        nonce,
    } = request;
    let operations = component_operations(kind, &entry);
    let policy_digest = effective_agent_library_policy_digest(&operations)?;
    let mut admitted_context = context.clone();
    admitted_context.policy_digest = format!("sha256:{}", policy_digest.to_hex());
    let event = AgentComponentOutboxEvent {
        schema_version: AGENT_COMPONENT_SCHEMA_VERSION,
        kind,
        component: entry.clone(),
        performing_actor: admitted_context.caller_principal.clone(),
        action_actor_scope: admitted_context.actor_scope.clone(),
    };
    event.validate()?;
    let event_bytes = eg_storage::encode_bounded(&event, "agent component outbox event")?;
    let entry_bytes = eg_storage::encode_bounded(&entry, "agent component revision")?;
    let batch_id = batch_id(&admitted_context.idempotency_key)?;
    let authoritative_version = store.mutations.current_version(txn, owner)?;
    let batch = build_component_batch(
        owner,
        &admitted_context,
        kind,
        &entry,
        authoritative_version,
        &batch_id,
        event_bytes.clone(),
    )?;
    let source_version =
        begin_component_commit(txn, &batch, operation, nonce, authoritative_version)?;
    let committed_version = source_version
        .checked_add(1)
        .ok_or_else(|| "agent component committed version overflow".to_string())?;
    let stable_result = AgentComponentCommittedResult {
        schema_version: AGENT_COMPONENT_SCHEMA_VERSION,
        component: entry.clone(),
        batch_id,
        committed_version,
    };
    let result_bytes = encode_component_domain_result(&stable_result)?;
    Ok(ComponentCommitPlan {
        admitted_context,
        expected_revision,
        entry,
        entry_bytes,
        event_bytes,
        batch,
        source_version,
        committed_version,
        stable_result,
        result_bytes,
    })
}

fn begin_component_commit(
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    batch: &MutationBatch,
    operation: &eg_types::authority::OperationReplayIdentity,
    nonce: &eg_types::authority::NonceReplayKey,
    authoritative_version: u64,
) -> Result<u64, String> {
    let begun = txn.begin_with_replay_identity(batch, operation, nonce)?;
    let source_version = match begun {
        Begin::Apply {
            source_version: Some(source_version),
        } => source_version,
        Begin::Apply {
            source_version: None,
        } => return Err("agent component admission has no native source version".to_string()),
        Begin::Replay(_) => {
            return Err(
                "CORRUPT_MUTATION_LEDGER: replay became visible after a fresh admission decision"
                    .to_string(),
            );
        }
    };
    if source_version != authoritative_version {
        return Err("agent component source version changed while admitting write".to_string());
    }
    Ok(source_version)
}

fn build_component_receipt(
    plan: &ComponentCommitPlan,
    operation: &eg_types::authority::OperationReplayIdentity,
    nonce: &eg_types::authority::NonceReplayKey,
) -> Result<MutationReceipt, String> {
    let key = format!(
        "{}:{}:{}",
        plan.entry.tenant_id, plan.entry.component_id, plan.entry.entry_revision
    );
    let headers = component_outbox_headers(&plan.entry);
    owner_receipt(
        operation,
        nonce,
        &plan.batch,
        OwnerReceiptInput {
            slug: "agent-component",
            topic: AGENT_COMPONENT_OUTBOX_TOPIC,
            key: &key,
            event_bytes: &plan.event_bytes,
            headers: &headers,
            mutation_result: component_domain_result(&plan.stable_result)?,
            committed_version: plan.committed_version,
            committed_at_ms: plan.admitted_context.created_at_ms,
        },
    )
}

fn apply_component_rows_phase(
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    plan: &ComponentCommitPlan,
) -> Result<(), String> {
    let owner_write = txn.owner_rows(owner, &plan.batch)?;
    apply_component_rows(
        &owner_write,
        &plan.admitted_context,
        plan.expected_revision,
        &plan.entry,
        plan.entry_bytes.as_slice(),
    )?;
    owner_write.finish_owner()
}

fn finish_component_ledger(
    mutations: &eg_transaction::MutationKernel,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    plan: &ComponentCommitPlan,
    operation: &eg_types::authority::OperationReplayIdentity,
    nonce: &eg_types::authority::NonceReplayKey,
    receipt: &MutationReceipt,
) -> Result<eg_types::MutationBatchRecord, String> {
    mutations.finish_with_replay(
        txn,
        &plan.batch,
        Some(plan.result_bytes.clone()),
        plan.admitted_context.created_at_ms,
        Some(plan.source_version),
        (operation, nonce, receipt),
    )
}

fn validate_component_commit_version(
    record: &eg_types::MutationBatchRecord,
    expected_version: u64,
) -> Result<(), String> {
    let recorded_version = record
        .committed_version
        .target()
        .ok_or_else(|| "agent component commit has no target version".to_string())?;
    if recorded_version != expected_version {
        return Err(
            "agent component result version differs from the committed version".to_string(),
        );
    }
    Ok(())
}

fn finish_replayed_component(
    mutations: &eg_transaction::MutationKernel,
    txn: eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    operation: &eg_types::authority::OperationReplayIdentity,
    nonce: &eg_types::authority::NonceReplayKey,
    result: AgentComponentWriteResult,
    receipt: MutationReceipt,
) -> Result<AgentComponentWriteResult, String> {
    if let Err(error) = mutations.finalize_replay_receipt(&txn, operation, nonce, &receipt) {
        txn.abort()?;
        return Err(error);
    }
    mutations.commit_replay_receipt(txn)?;
    Ok(result)
}

impl AgentLibraryStore {
    /// Publish the next graph revision.
    pub fn publish_component(
        &self,
        request: AgentComponentPublishRequest,
    ) -> Result<AgentComponentWriteResult, String> {
        validate_context(self, &request.context)?;
        request.component.validate()?;
        if request.context.tenant_id != request.component.tenant_id {
            return Err(
                "agent component publish context tenant does not match the graph's tenant"
                    .to_string(),
            );
        }
        let expected_revision = request.context.expected_revision.ok_or_else(|| {
            "agent component writes require an explicit expected_revision".to_string()
        })?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;
        let admission = match prepare_component_admission(
            &self.mutations,
            &txn,
            ComponentAdmissionRequest {
                owner: &owner,
                context: &request.context,
                purpose_id: "agent-component:publish",
                operation_kind: "component-publish",
                component_id: &request.component.component_id,
                expected_revision,
                definition_digest: Some(&request.component.content_digest),
            },
        ) {
            Ok(admission) => admission,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let ComponentAdmission {
            nonce,
            next_revision,
            replay_context,
            operation,
            replay,
        } = admission;
        let replayed = match replayed_component(replay, &request.component.component_id) {
            Ok(replayed) => replayed,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        if let Some((result, receipt)) = replayed {
            return finish_replayed_component(
                &self.mutations,
                txn,
                &operation,
                &nonce,
                result,
                receipt,
            );
        }
        let entry = match prepare_component_entry(&txn, &request, next_revision, &replay_context) {
            Ok(entry) => entry,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        self.commit_component_in_write(
            txn,
            &owner,
            &replay_context,
            expected_revision,
            AgentComponentMutationKind::Publish,
            entry,
            &operation,
            &nonce,
        )
    }

    /// Retain the current graph revision as a durable tombstone.
    pub fn retire_component(
        &self,
        request: AgentComponentRetireRequest,
    ) -> Result<AgentComponentWriteResult, String> {
        validate_context(self, &request.context)?;
        let expected_revision = request.context.expected_revision.ok_or_else(|| {
            "agent component writes require an explicit expected_revision".to_string()
        })?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;
        let admission = match prepare_component_admission(
            &self.mutations,
            &txn,
            ComponentAdmissionRequest {
                owner: &owner,
                context: &request.context,
                purpose_id: "agent-component:retire",
                operation_kind: "component-retire",
                component_id: &request.component_id,
                expected_revision,
                definition_digest: None,
            },
        ) {
            Ok(admission) => admission,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let ComponentAdmission {
            nonce,
            next_revision,
            replay_context,
            operation,
            replay,
        } = admission;
        let replayed = match replayed_component(replay, &request.component_id) {
            Ok(replayed) => replayed,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        if let Some((result, receipt)) = replayed {
            return finish_replayed_component(
                &self.mutations,
                txn,
                &operation,
                &nonce,
                result,
                receipt,
            );
        }
        let tombstone = match prepare_component_retirement(
            self,
            &txn,
            &request,
            expected_revision,
            next_revision,
            &replay_context,
        ) {
            Ok(tombstone) => tombstone,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        self.commit_component_in_write(
            txn,
            &owner,
            &replay_context,
            expected_revision,
            AgentComponentMutationKind::Retire,
            tombstone,
            &operation,
            &nonce,
        )
    }

    /// Resolve every pinned L1 component reference in the admitting write.
    pub(crate) fn resolve_component_pins_in_write(
        &self,
        write: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        tenant_id: &str,
        subject: &str,
        pins: &[&eg_types::agent_component::ComponentDependency],
    ) -> Result<(), String> {
        super::pins::resolve_component_pins_in_write(write, tenant_id, subject, pins)
    }

    fn component_at_revision_in_write(
        &self,
        write: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        tenant_id: &str,
        component_id: &str,
        revision: u64,
    ) -> Result<Option<AgentComponentEntry>, String> {
        if revision == 0 {
            return Ok(None);
        }
        let revisions = write.open_read_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
        let Some(value) = revisions.get((tenant_id, component_id, revision))? else {
            return Ok(None);
        };
        let entry = decode_component(value.value())?;
        if entry.tenant_id != tenant_id
            || entry.component_id != component_id
            || entry.entry_revision != revision
        {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent component row does not match its physical key"
                    .to_string(),
            );
        }
        Ok(Some(entry))
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_component_in_write(
        &self,
        txn: eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
        context: &AgentLibraryMutationContext,
        expected_revision: u64,
        kind: AgentComponentMutationKind,
        entry: AgentComponentEntry,
        operation: &eg_types::authority::OperationReplayIdentity,
        nonce: &eg_types::authority::NonceReplayKey,
    ) -> Result<AgentComponentWriteResult, String> {
        let plan = match build_component_commit_plan(
            self,
            &txn,
            owner,
            context,
            ComponentCommitRequest {
                expected_revision,
                kind,
                entry,
                operation,
                nonce,
            },
        ) {
            Ok(plan) => plan,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        if let Err(error) = apply_component_rows_phase(&txn, owner, &plan) {
            txn.abort()?;
            return Err(error);
        }
        let receipt = match build_component_receipt(&plan, operation, nonce) {
            Ok(receipt) => receipt,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let record =
            match finish_component_ledger(&self.mutations, &txn, &plan, operation, nonce, &receipt)
            {
                Ok(record) => record,
                Err(error) => {
                    txn.abort()?;
                    return Err(error);
                }
            };
        if let Err(error) = validate_component_commit_version(&record, plan.committed_version) {
            txn.abort()?;
            return Err(error);
        }
        self.mutations.commit(txn, &plan.batch)?;
        Ok(AgentComponentWriteResult {
            result: plan.stable_result,
            replayed: false,
        })
    }
}
