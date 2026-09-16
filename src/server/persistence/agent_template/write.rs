//! Agent template mutation admission and commit phases.
//!
//! The template parent owns reads and instantiation. This child keeps the
//! write lifecycle together: replay admission, base-reference checks, owner
//! rows, the typed receipt, and the native commit all stay on one path.

use super::*;

use eg_types::mutation::MutationReceipt;

struct TemplateAdmission {
    nonce: eg_types::authority::NonceReplayKey,
    next_revision: u64,
    replay_context: AgentLibraryMutationContext,
    operation: eg_types::authority::OperationReplayIdentity,
    replay: ReplayResolution,
}

struct TemplateAdmissionRequest<'a> {
    owner: &'a OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &'a AgentLibraryMutationContext,
    purpose_id: &'a str,
    operation_kind: &'a str,
    template_id: &'a str,
    expected_revision: u64,
    definition_digest: Option<&'a str>,
}

struct TemplateCommitRequest<'a> {
    owner: &'a OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &'a AgentLibraryMutationContext,
    expected_revision: u64,
    kind: AgentTemplateMutationKind,
    entry: AgentTemplateEntry,
    operation: &'a eg_types::authority::OperationReplayIdentity,
    nonce: &'a eg_types::authority::NonceReplayKey,
}

fn prepare_template_admission(
    mutations: &eg_transaction::MutationKernel,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    request: TemplateAdmissionRequest<'_>,
) -> Result<TemplateAdmission, String> {
    let nonce = resolve_nonce_first(mutations, txn, request.context)?;
    let next_revision = next_revision(request.expected_revision)?;
    let replay_context = admitted_context(request.context, request.purpose_id)?;
    let operation = agent_library_operation_identity(
        request.owner,
        &replay_context,
        request.operation_kind,
        request.template_id,
        request.expected_revision,
        request.definition_digest,
    )?;
    let replay = mutations.resolve_replay(txn, &operation, &nonce)?;
    Ok(TemplateAdmission {
        nonce,
        next_revision,
        replay_context,
        operation,
        replay,
    })
}

fn prepare_template_entry(
    store: &AgentLibraryStore,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    request: &AgentTemplatePublishRequest,
    next_revision: u64,
    replay_context: &AgentLibraryMutationContext,
) -> Result<AgentTemplateEntry, String> {
    store.admit_entry_references_in_write(
        txn,
        &request.context.tenant_id,
        "agent template base",
        &request.template.base,
    )?;
    AgentTemplateEntry::create(
        request.template.clone(),
        next_revision,
        AgentLibraryLifecycle::Published,
        replay_context.created_at_ms,
        replay_context.created_at_ms,
    )
}

fn prepare_template_retirement(
    store: &AgentLibraryStore,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    request: &AgentTemplateRetireRequest,
    expected_revision: u64,
    next_revision: u64,
    replay_context: &AgentLibraryMutationContext,
) -> Result<AgentTemplateEntry, String> {
    let current = store
        .template_at_revision_in_write(
            txn,
            &request.context.tenant_id,
            &request.template_id,
            expected_revision,
        )?
        .ok_or_else(|| "agent template has no revision to retire".to_string())?;
    current.retire(next_revision, replay_context.created_at_ms)
}

struct TemplateCommitPlan {
    admitted_context: AgentLibraryMutationContext,
    expected_revision: u64,
    entry: AgentTemplateEntry,
    entry_bytes: Vec<u8>,
    event_bytes: Vec<u8>,
    batch: MutationBatch,
    source_version: u64,
    committed_version: u64,
    stable_result: AgentTemplateCommittedResult,
    result_bytes: Vec<u8>,
}

fn build_template_commit_plan(
    store: &AgentLibraryStore,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    request: TemplateCommitRequest<'_>,
) -> Result<TemplateCommitPlan, String> {
    let operations = template_operations(request.kind, &request.entry);
    let policy_digest = effective_agent_library_policy_digest(&operations)?;
    let mut admitted_context = request.context.clone();
    admitted_context.policy_digest = format!("sha256:{}", policy_digest.to_hex());
    let event = AgentTemplateOutboxEvent {
        schema_version: AGENT_TEMPLATE_SCHEMA_VERSION,
        kind: request.kind,
        template: request.entry.clone(),
        performing_actor: admitted_context.caller_principal.clone(),
        action_actor_scope: admitted_context.actor_scope.clone(),
    };
    event.validate()?;
    let event_bytes = eg_storage::encode_bounded(&event, "agent template outbox event")?;
    let entry_bytes = eg_storage::encode_bounded(&request.entry, "agent template revision")?;
    let batch_id = batch_id(&admitted_context.idempotency_key)?;
    let authoritative_version = store.mutations.current_version(txn, request.owner)?;
    let batch = build_template_batch(
        request.owner,
        &admitted_context,
        request.kind,
        &request.entry,
        authoritative_version,
        &batch_id,
        event_bytes.clone(),
    )?;
    let source_version = begin_template_commit(
        txn,
        &batch,
        request.operation,
        request.nonce,
        authoritative_version,
    )?;
    let committed_version = source_version
        .checked_add(1)
        .ok_or_else(|| "agent template committed version overflow".to_string())?;
    let stable_result = AgentTemplateCommittedResult {
        schema_version: AGENT_TEMPLATE_SCHEMA_VERSION,
        template: request.entry.clone(),
        batch_id,
        committed_version,
    };
    let result_bytes = encode_template_domain_result(&stable_result)?;
    Ok(TemplateCommitPlan {
        admitted_context,
        expected_revision: request.expected_revision,
        entry: request.entry,
        entry_bytes,
        event_bytes,
        batch,
        source_version,
        committed_version,
        stable_result,
        result_bytes,
    })
}

fn begin_template_commit(
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
        } => return Err("agent template admission has no native source version".to_string()),
        Begin::Replay(_) => {
            return Err(
                "CORRUPT_MUTATION_LEDGER: replay became visible after a fresh admission decision"
                    .to_string(),
            );
        }
    };
    if source_version != authoritative_version {
        return Err("agent template source version changed while admitting write".to_string());
    }
    Ok(source_version)
}

fn build_template_receipt(
    plan: &TemplateCommitPlan,
    operation: &eg_types::authority::OperationReplayIdentity,
    nonce: &eg_types::authority::NonceReplayKey,
) -> Result<MutationReceipt, String> {
    let key = format!(
        "{}:{}:{}",
        plan.entry.tenant_id, plan.entry.template_id, plan.entry.entry_revision
    );
    let headers = template_outbox_headers(&plan.entry);
    owner_receipt(
        operation,
        nonce,
        &plan.batch,
        OwnerReceiptInput {
            slug: "agent-template",
            topic: AGENT_TEMPLATE_OUTBOX_TOPIC,
            key: &key,
            event_bytes: &plan.event_bytes,
            headers: &headers,
            mutation_result: template_domain_result(&plan.stable_result)?,
            committed_version: plan.committed_version,
            committed_at_ms: plan.admitted_context.created_at_ms,
        },
    )
}

fn apply_template_rows_phase(
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    plan: &TemplateCommitPlan,
) -> Result<(), String> {
    let owner_write = txn.owner_rows(owner, &plan.batch)?;
    apply_template_rows(
        &owner_write,
        &plan.admitted_context,
        plan.expected_revision,
        &plan.entry,
        plan.entry_bytes.as_slice(),
    )?;
    owner_write.finish_owner()
}

fn finish_template_ledger(
    mutations: &eg_transaction::MutationKernel,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    plan: &TemplateCommitPlan,
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

fn validate_template_commit_version(
    record: &eg_types::MutationBatchRecord,
    expected_version: u64,
) -> Result<(), String> {
    let recorded_version = record
        .committed_version
        .target()
        .ok_or_else(|| "agent template commit has no target version".to_string())?;
    if recorded_version != expected_version {
        return Err("agent template result version differs from the committed version".to_string());
    }
    Ok(())
}

fn finish_replayed_template(
    mutations: &eg_transaction::MutationKernel,
    txn: eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    operation: &eg_types::authority::OperationReplayIdentity,
    nonce: &eg_types::authority::NonceReplayKey,
    result: AgentTemplateWriteResult,
    receipt: MutationReceipt,
) -> Result<AgentTemplateWriteResult, String> {
    if let Err(error) = mutations.finalize_replay_receipt(&txn, operation, nonce, &receipt) {
        txn.abort()?;
        return Err(error);
    }
    mutations.commit_replay_receipt(txn)?;
    Ok(result)
}

impl AgentLibraryStore {
    /// Publish the next template revision.
    pub fn publish_template(
        &self,
        request: AgentTemplatePublishRequest,
    ) -> Result<AgentTemplateWriteResult, String> {
        validate_context(self, &request.context)?;
        request.template.validate()?;
        if request.context.tenant_id != request.template.tenant_id {
            return Err(
                "agent template publish context tenant does not match the template's tenant"
                    .to_string(),
            );
        }
        let expected_revision = request.context.expected_revision.ok_or_else(|| {
            "agent template writes require an explicit expected_revision".to_string()
        })?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;
        let definition_digest =
            eg_types::agent_template::draft_definition_digest(&request.template);
        let admission = match prepare_template_admission(
            &self.mutations,
            &txn,
            TemplateAdmissionRequest {
                owner: &owner,
                context: &request.context,
                purpose_id: "agent-template:publish",
                operation_kind: "template-publish",
                template_id: &request.template.template_id,
                expected_revision,
                definition_digest: Some(&definition_digest),
            },
        ) {
            Ok(admission) => admission,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let TemplateAdmission {
            nonce,
            next_revision,
            replay_context,
            operation,
            replay,
        } = admission;
        let replayed = match replayed_template(replay, &request.template.template_id) {
            Ok(replayed) => replayed,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        if let Some((result, receipt)) = replayed {
            return finish_replayed_template(
                &self.mutations,
                txn,
                &operation,
                &nonce,
                result,
                receipt,
            );
        }
        let entry =
            match prepare_template_entry(self, &txn, &request, next_revision, &replay_context) {
                Ok(entry) => entry,
                Err(error) => {
                    txn.abort()?;
                    return Err(error);
                }
            };
        self.commit_template_in_write(
            txn,
            &owner,
            &replay_context,
            expected_revision,
            AgentTemplateMutationKind::Publish,
            entry,
            &operation,
            &nonce,
        )
    }

    /// Retain the current template revision as a durable tombstone.
    pub fn retire_template(
        &self,
        request: AgentTemplateRetireRequest,
    ) -> Result<AgentTemplateWriteResult, String> {
        validate_context(self, &request.context)?;
        let expected_revision = request.context.expected_revision.ok_or_else(|| {
            "agent template writes require an explicit expected_revision".to_string()
        })?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;
        let admission = match prepare_template_admission(
            &self.mutations,
            &txn,
            TemplateAdmissionRequest {
                owner: &owner,
                context: &request.context,
                purpose_id: "agent-template:retire",
                operation_kind: "template-retire",
                template_id: &request.template_id,
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
        let TemplateAdmission {
            nonce,
            next_revision,
            replay_context,
            operation,
            replay,
        } = admission;
        let replayed = match replayed_template(replay, &request.template_id) {
            Ok(replayed) => replayed,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        if let Some((result, receipt)) = replayed {
            return finish_replayed_template(
                &self.mutations,
                txn,
                &operation,
                &nonce,
                result,
                receipt,
            );
        }
        let tombstone = match prepare_template_retirement(
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
        self.commit_template_in_write(
            txn,
            &owner,
            &replay_context,
            expected_revision,
            AgentTemplateMutationKind::Retire,
            tombstone,
            &operation,
            &nonce,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_template_in_write(
        &self,
        txn: eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
        context: &AgentLibraryMutationContext,
        expected_revision: u64,
        kind: AgentTemplateMutationKind,
        entry: AgentTemplateEntry,
        operation: &eg_types::authority::OperationReplayIdentity,
        nonce: &eg_types::authority::NonceReplayKey,
    ) -> Result<AgentTemplateWriteResult, String> {
        let plan = match build_template_commit_plan(
            self,
            &txn,
            TemplateCommitRequest {
                owner,
                context,
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
        if let Err(error) = apply_template_rows_phase(&txn, owner, &plan) {
            txn.abort()?;
            return Err(error);
        }
        let receipt = match build_template_receipt(&plan, operation, nonce) {
            Ok(receipt) => receipt,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let record = match finish_template_ledger(
            &self.mutations,
            &txn,
            &plan,
            operation,
            nonce,
            &receipt,
        ) {
            Ok(record) => record,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        if let Err(error) = validate_template_commit_version(&record, plan.committed_version) {
            txn.abort()?;
            return Err(error);
        }
        self.mutations.commit(txn, &plan.batch)?;
        Ok(AgentTemplateWriteResult {
            result: plan.stable_result,
            replayed: false,
        })
    }
}
