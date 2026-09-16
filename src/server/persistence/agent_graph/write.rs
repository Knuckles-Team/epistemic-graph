//! Agent graph mutation admission and commit phases.
//!
//! The graph parent owns reads and reference resolution. This child keeps the
//! write lifecycle together: replay admission, composition, owner rows, the
//! typed receipt, and the native commit all stay on one path.

use super::*;

use eg_types::mutation::MutationReceipt;

struct GraphAdmission {
    nonce: eg_types::authority::NonceReplayKey,
    next_revision: u64,
    replay_context: AgentLibraryMutationContext,
    operation: eg_types::authority::OperationReplayIdentity,
    replay: ReplayResolution,
}

struct GraphAdmissionRequest<'a> {
    owner: &'a OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &'a AgentLibraryMutationContext,
    purpose_id: &'a str,
    operation_kind: &'a str,
    graph_id: &'a str,
    expected_revision: u64,
    definition_digest: Option<&'a str>,
}

struct GraphCommitRequest<'a> {
    owner: &'a OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &'a AgentLibraryMutationContext,
    expected_revision: u64,
    kind: AgentGraphMutationKind,
    entry: AgentGraphEntry,
    operation: &'a eg_types::authority::OperationReplayIdentity,
    nonce: &'a eg_types::authority::NonceReplayKey,
}

fn prepare_graph_admission(
    mutations: &eg_transaction::MutationKernel,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    request: GraphAdmissionRequest<'_>,
) -> Result<GraphAdmission, String> {
    let nonce = resolve_nonce_first(mutations, txn, request.context)?;
    let next_revision = next_revision(request.expected_revision)?;
    let replay_context = admitted_context(request.context, request.purpose_id)?;
    let operation = agent_library_operation_identity(
        request.owner,
        &replay_context,
        request.operation_kind,
        request.graph_id,
        request.expected_revision,
        request.definition_digest,
    )?;
    let replay = mutations.resolve_replay(txn, &operation, &nonce)?;
    Ok(GraphAdmission {
        nonce,
        next_revision,
        replay_context,
        operation,
        replay,
    })
}

fn prepare_graph_entry(
    store: &AgentLibraryStore,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    request: &AgentGraphPublishRequest,
    next_revision: u64,
    replay_context: &AgentLibraryMutationContext,
) -> Result<AgentGraphEntry, String> {
    let composition =
        store.admit_graph_references_in_write(txn, &request.context.tenant_id, &request.graph)?;
    AgentGraphEntry::create(
        request.graph.clone(),
        composition.total_work,
        next_revision,
        AgentLibraryLifecycle::Published,
        replay_context.created_at_ms,
        replay_context.created_at_ms,
    )
}

fn prepare_graph_retirement(
    store: &AgentLibraryStore,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    request: &AgentGraphRetireRequest,
    expected_revision: u64,
    next_revision: u64,
    replay_context: &AgentLibraryMutationContext,
) -> Result<AgentGraphEntry, String> {
    let current = store
        .graph_at_revision_in_write(
            txn,
            &request.context.tenant_id,
            &request.graph_id,
            expected_revision,
        )?
        .ok_or_else(|| "agent graph has no revision to retire".to_string())?;
    current.retire(next_revision, replay_context.created_at_ms)
}

struct GraphCommitPlan {
    admitted_context: AgentLibraryMutationContext,
    expected_revision: u64,
    entry: AgentGraphEntry,
    entry_bytes: Vec<u8>,
    event_bytes: Vec<u8>,
    batch: MutationBatch,
    source_version: u64,
    committed_version: u64,
    stable_result: AgentGraphCommittedResult,
    result_bytes: Vec<u8>,
}

fn build_graph_commit_plan(
    store: &AgentLibraryStore,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    request: GraphCommitRequest<'_>,
) -> Result<GraphCommitPlan, String> {
    let operations = graph_operations(request.kind, &request.entry);
    let policy_digest = effective_agent_library_policy_digest(&operations)?;
    let mut admitted_context = request.context.clone();
    admitted_context.policy_digest = format!("sha256:{}", policy_digest.to_hex());
    let event = AgentGraphOutboxEvent {
        schema_version: AGENT_GRAPH_ENTRY_SCHEMA_VERSION,
        kind: request.kind,
        graph: request.entry.clone(),
        performing_actor: admitted_context.caller_principal.clone(),
        action_actor_scope: admitted_context.actor_scope.clone(),
    };
    event.validate()?;
    let event_bytes = eg_storage::encode_bounded(&event, "agent graph outbox event")?;
    let entry_bytes = eg_storage::encode_bounded(&request.entry, "agent graph revision")?;
    let batch_id = batch_id(&admitted_context.idempotency_key)?;
    let authoritative_version = store.mutations.current_version(txn, request.owner)?;
    let batch = build_graph_batch(
        request.owner,
        &admitted_context,
        request.kind,
        &request.entry,
        authoritative_version,
        &batch_id,
        event_bytes.clone(),
    )?;
    let source_version = begin_graph_commit(
        txn,
        &batch,
        request.operation,
        request.nonce,
        authoritative_version,
    )?;
    let committed_version = source_version
        .checked_add(1)
        .ok_or_else(|| "agent graph committed version overflow".to_string())?;
    let stable_result = AgentGraphCommittedResult {
        schema_version: AGENT_GRAPH_ENTRY_SCHEMA_VERSION,
        graph: request.entry.clone(),
        batch_id,
        committed_version,
    };
    let result_bytes = encode_graph_domain_result(&stable_result)?;
    Ok(GraphCommitPlan {
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

fn begin_graph_commit(
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
        } => return Err("agent graph admission has no native source version".to_string()),
        Begin::Replay(_) => {
            return Err(
                "CORRUPT_MUTATION_LEDGER: replay became visible after a fresh admission decision"
                    .to_string(),
            );
        }
    };
    if source_version != authoritative_version {
        return Err("agent graph source version changed while admitting write".to_string());
    }
    Ok(source_version)
}

fn build_graph_receipt(
    plan: &GraphCommitPlan,
    operation: &eg_types::authority::OperationReplayIdentity,
    nonce: &eg_types::authority::NonceReplayKey,
) -> Result<MutationReceipt, String> {
    let key = format!(
        "{}:{}:{}",
        plan.entry.tenant_id, plan.entry.graph_id, plan.entry.entry_revision
    );
    let headers = graph_outbox_headers(&plan.entry);
    owner_receipt(
        operation,
        nonce,
        &plan.batch,
        OwnerReceiptInput {
            slug: "agent-graph",
            topic: AGENT_GRAPH_OUTBOX_TOPIC,
            key: &key,
            event_bytes: &plan.event_bytes,
            headers: &headers,
            mutation_result: graph_domain_result(&plan.stable_result)?,
            committed_version: plan.committed_version,
            committed_at_ms: plan.admitted_context.created_at_ms,
        },
    )
}

fn apply_graph_rows_phase(
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    plan: &GraphCommitPlan,
) -> Result<(), String> {
    let owner_write = txn.owner_rows(owner, &plan.batch)?;
    apply_graph_rows(
        &owner_write,
        &plan.admitted_context,
        plan.expected_revision,
        &plan.entry,
        plan.entry_bytes.as_slice(),
    )?;
    owner_write.finish_owner()
}

fn finish_graph_ledger(
    mutations: &eg_transaction::MutationKernel,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    plan: &GraphCommitPlan,
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

fn validate_graph_commit_version(
    record: &eg_types::MutationBatchRecord,
    expected_version: u64,
) -> Result<(), String> {
    let recorded_version = record
        .committed_version
        .target()
        .ok_or_else(|| "agent graph commit has no target version".to_string())?;
    if recorded_version != expected_version {
        return Err("agent graph result version differs from the committed version".to_string());
    }
    Ok(())
}

fn finish_replayed_graph(
    mutations: &eg_transaction::MutationKernel,
    txn: eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    operation: &eg_types::authority::OperationReplayIdentity,
    nonce: &eg_types::authority::NonceReplayKey,
    result: AgentGraphWriteResult,
    receipt: MutationReceipt,
) -> Result<AgentGraphWriteResult, String> {
    if let Err(error) = mutations.finalize_replay_receipt(&txn, operation, nonce, &receipt) {
        txn.abort()?;
        return Err(error);
    }
    mutations.commit_replay_receipt(txn)?;
    Ok(result)
}

impl AgentLibraryStore {
    /// Publish the next graph revision.
    pub fn publish_graph(
        &self,
        request: AgentGraphPublishRequest,
    ) -> Result<AgentGraphWriteResult, String> {
        validate_context(self, &request.context)?;
        request.graph.validate()?;
        if request.context.tenant_id != request.graph.tenant_id {
            return Err(
                "agent graph publish context tenant does not match the graph's tenant".to_string(),
            );
        }
        let expected_revision = request.context.expected_revision.ok_or_else(|| {
            "agent graph writes require an explicit expected_revision".to_string()
        })?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;
        let draft_digest = eg_types::agent_graph::draft_definition_digest(&request.graph);
        let admission = match prepare_graph_admission(
            &self.mutations,
            &txn,
            GraphAdmissionRequest {
                owner: &owner,
                context: &request.context,
                purpose_id: "agent-graph:publish",
                operation_kind: "graph-publish",
                graph_id: &request.graph.graph_id,
                expected_revision,
                definition_digest: Some(&draft_digest),
            },
        ) {
            Ok(admission) => admission,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let GraphAdmission {
            nonce,
            next_revision,
            replay_context,
            operation,
            replay,
        } = admission;
        let replayed = match replayed_graph(replay, &request.graph.graph_id) {
            Ok(replayed) => replayed,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        if let Some((result, receipt)) = replayed {
            return finish_replayed_graph(
                &self.mutations,
                txn,
                &operation,
                &nonce,
                result,
                receipt,
            );
        }
        let entry = match prepare_graph_entry(self, &txn, &request, next_revision, &replay_context)
        {
            Ok(entry) => entry,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        self.commit_graph_in_write(
            txn,
            &owner,
            &replay_context,
            expected_revision,
            AgentGraphMutationKind::Publish,
            entry,
            &operation,
            &nonce,
        )
    }

    /// Retain the current graph revision as a durable tombstone.
    pub fn retire_graph(
        &self,
        request: AgentGraphRetireRequest,
    ) -> Result<AgentGraphWriteResult, String> {
        validate_context(self, &request.context)?;
        let expected_revision = request.context.expected_revision.ok_or_else(|| {
            "agent graph writes require an explicit expected_revision".to_string()
        })?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;
        let admission = match prepare_graph_admission(
            &self.mutations,
            &txn,
            GraphAdmissionRequest {
                owner: &owner,
                context: &request.context,
                purpose_id: "agent-graph:retire",
                operation_kind: "graph-retire",
                graph_id: &request.graph_id,
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
        let GraphAdmission {
            nonce,
            next_revision,
            replay_context,
            operation,
            replay,
        } = admission;
        let replayed = match replayed_graph(replay, &request.graph_id) {
            Ok(replayed) => replayed,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        if let Some((result, receipt)) = replayed {
            return finish_replayed_graph(
                &self.mutations,
                txn,
                &operation,
                &nonce,
                result,
                receipt,
            );
        }
        let tombstone = match prepare_graph_retirement(
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
        self.commit_graph_in_write(
            txn,
            &owner,
            &replay_context,
            expected_revision,
            AgentGraphMutationKind::Retire,
            tombstone,
            &operation,
            &nonce,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_graph_in_write(
        &self,
        txn: eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
        context: &AgentLibraryMutationContext,
        expected_revision: u64,
        kind: AgentGraphMutationKind,
        entry: AgentGraphEntry,
        operation: &eg_types::authority::OperationReplayIdentity,
        nonce: &eg_types::authority::NonceReplayKey,
    ) -> Result<AgentGraphWriteResult, String> {
        let plan = match build_graph_commit_plan(
            self,
            &txn,
            GraphCommitRequest {
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
        if let Err(error) = apply_graph_rows_phase(&txn, owner, &plan) {
            txn.abort()?;
            return Err(error);
        }
        let receipt = match build_graph_receipt(&plan, operation, nonce) {
            Ok(receipt) => receipt,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let record =
            match finish_graph_ledger(&self.mutations, &txn, &plan, operation, nonce, &receipt) {
                Ok(record) => record,
                Err(error) => {
                    txn.abort()?;
                    return Err(error);
                }
            };
        if let Err(error) = validate_graph_commit_version(&record, plan.committed_version) {
            txn.abort()?;
            return Err(error);
        }
        self.mutations.commit(txn, &plan.batch)?;
        Ok(AgentGraphWriteResult {
            result: plan.stable_result,
            replayed: false,
        })
    }
}
