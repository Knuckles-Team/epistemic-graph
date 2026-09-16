//! Agent Library publish/retire transaction phases.

use super::batch::build_batch;
use super::replay::{expected_headers_from_event, replayed_receipt, ExpectedAgentLibraryMutation};
use super::*;

/// The pair the mutation kernel treats as one replay identity: an attempt is
/// the same attempt only when BOTH the operation class and the attempt nonce
/// match.  The kernel's own API says so -- `begin_with_replay_identity`,
/// `resolve_replay` and `finish_with_replay` all take the two together (the
/// last one already as an inline tuple) -- so they are threaded as one value
/// rather than as two positional arguments that could drift apart.
struct ReplayIdentity<'a> {
    operation: &'a eg_types::authority::OperationReplayIdentity,
    nonce: &'a eg_types::authority::NonceReplayKey,
}

/// One append-only revision transition to commit: the entry as it will be
/// retained, the `kind` of lifecycle change that produced it, and the revision
/// the caller observed before it.  All three are needed together to stay
/// consistent -- `expected_revision` is what the head row is compact-and-swapped
/// against, and `kind` + `entry` decide the operation, outbox event and
/// retained row -- so a caller cannot supply one without the others.
struct AgentLibraryRevisionCommit {
    expected_revision: u64,
    kind: AgentLibraryMutationKind,
    entry: AgentLibraryEntry,
}

struct AgentLibraryAdmission {
    nonce: eg_types::authority::NonceReplayKey,
    next_revision: u64,
    replay_context: AgentLibraryMutationContext,
    operation: eg_types::authority::OperationReplayIdentity,
    replay: ReplayResolution,
}

/// The outcome of admitting one publish/retire before anything is committed.
/// The variant-specific payloads are large and differ in size, so they are
/// boxed; the enum itself stays small whichever path is taken.
enum PreparedLibraryWrite {
    Replay {
        operation: eg_types::authority::OperationReplayIdentity,
        nonce: eg_types::authority::NonceReplayKey,
        result: Box<AgentLibraryWriteResult>,
        receipt: Box<MutationReceipt>,
    },
    Fresh {
        operation: eg_types::authority::OperationReplayIdentity,
        nonce: eg_types::authority::NonceReplayKey,
        context: Box<AgentLibraryMutationContext>,
        entry: Box<AgentLibraryEntry>,
    },
}

struct AgentLibraryAdmissionRequest<'a> {
    owner: &'a OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &'a AgentLibraryMutationContext,
    purpose_id: &'a str,
    kind_name: &'a str,
    record_id: &'a str,
    expected_revision: u64,
    definition_digest: Option<&'a str>,
}

fn prepare_admission(
    mutations: &MutationKernel,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    request: AgentLibraryAdmissionRequest<'_>,
    nonce: eg_types::authority::NonceReplayKey,
    next_revision: u64,
) -> Result<AgentLibraryAdmission, String> {
    let replay_context = admitted_context(request.context, request.purpose_id)?;
    let operation = agent_library_operation_identity(
        request.owner,
        &replay_context,
        request.kind_name,
        request.record_id,
        request.expected_revision,
        request.definition_digest,
    )?;
    let replay = mutations.resolve_replay(txn, &operation, &nonce)?;
    Ok(AgentLibraryAdmission {
        nonce,
        next_revision,
        replay_context,
        operation,
        replay,
    })
}

fn finish_replayed(
    mutations: &MutationKernel,
    txn: eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    operation: &eg_types::authority::OperationReplayIdentity,
    nonce: &eg_types::authority::NonceReplayKey,
    result: AgentLibraryWriteResult,
    receipt: MutationReceipt,
) -> Result<AgentLibraryWriteResult, String> {
    if let Err(error) = mutations.finalize_replay_receipt(&txn, operation, nonce, &receipt) {
        txn.abort()?;
        return Err(error);
    }
    mutations.commit_replay_receipt(txn)?;
    Ok(result)
}

fn prepare_publish(
    store: &AgentLibraryStore,
    mutations: &MutationKernel,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    request: &AgentLibraryPublishRequest,
    expected_revision: u64,
) -> Result<PreparedLibraryWrite, String> {
    let nonce = resolve_nonce_first(mutations, txn, &request.context)?;
    let next_revision = next_revision(expected_revision)?;
    let definition_digest = AgentLibraryEntry::publish(request.entry.clone(), next_revision, 0)
        .map(|entry| entry.definition_digest)?;
    let admission = prepare_admission(
        mutations,
        txn,
        AgentLibraryAdmissionRequest {
            owner,
            context: &request.context,
            purpose_id: "agent-library:publish",
            kind_name: "publish",
            record_id: &request.entry.agent_id,
            expected_revision,
            definition_digest: Some(&definition_digest),
        },
        nonce,
        next_revision,
    )?;
    let AgentLibraryAdmission {
        nonce,
        next_revision,
        replay_context,
        operation,
        replay,
    } = admission;
    if let Some((result, receipt)) = replayed_receipt(
        mutations,
        txn,
        replay,
        &operation,
        &replay_context,
        ExpectedAgentLibraryMutation {
            kind: AgentLibraryMutationKind::Publish,
            agent_id: request.entry.agent_id.as_str(),
            draft: Some(&request.entry),
        },
    )? {
        return Ok(PreparedLibraryWrite::Replay {
            operation,
            nonce,
            result: Box::new(result),
            receipt: Box::new(receipt),
        });
    }
    let (context, entry) = prepare_publish_entry(
        store,
        txn,
        request,
        expected_revision,
        next_revision,
        &replay_context,
    )?;
    Ok(PreparedLibraryWrite::Fresh {
        operation,
        nonce,
        context: Box::new(context),
        entry: Box::new(entry),
    })
}

fn prepare_retire(
    store: &AgentLibraryStore,
    mutations: &MutationKernel,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    request: &AgentLibraryRetireRequest,
    expected_revision: u64,
) -> Result<PreparedLibraryWrite, String> {
    let nonce = resolve_nonce_first(mutations, txn, &request.context)?;
    let next_revision = next_revision(expected_revision)?;
    let admission = prepare_admission(
        mutations,
        txn,
        AgentLibraryAdmissionRequest {
            owner,
            context: &request.context,
            purpose_id: "agent-library:retire",
            kind_name: "retire",
            record_id: &request.agent_id,
            expected_revision,
            definition_digest: None,
        },
        nonce,
        next_revision,
    )?;
    let AgentLibraryAdmission {
        nonce,
        next_revision,
        replay_context,
        operation,
        replay,
    } = admission;
    if let Some((result, receipt)) = replayed_receipt(
        mutations,
        txn,
        replay,
        &operation,
        &replay_context,
        ExpectedAgentLibraryMutation {
            kind: AgentLibraryMutationKind::Retire,
            agent_id: request.agent_id.as_str(),
            draft: None,
        },
    )? {
        return Ok(PreparedLibraryWrite::Replay {
            operation,
            nonce,
            result: Box::new(result),
            receipt: Box::new(receipt),
        });
    }
    let (context, entry) = prepare_retire_entry(
        store,
        txn,
        request,
        expected_revision,
        next_revision,
        &replay_context,
    )?;
    Ok(PreparedLibraryWrite::Fresh {
        operation,
        nonce,
        context: Box::new(context),
        entry: Box::new(entry),
    })
}

fn prepare_publish_entry(
    store: &AgentLibraryStore,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    request: &AgentLibraryPublishRequest,
    expected_revision: u64,
    next_revision: u64,
    replay_context: &AgentLibraryMutationContext,
) -> Result<(AgentLibraryMutationContext, AgentLibraryEntry), String> {
    store.admit_entry_references_in_write(
        txn,
        &request.context.tenant_id,
        "agent library entry",
        &request.entry,
    )?;
    let previous_updated_at_ms = if expected_revision == 0 {
        0
    } else {
        store
            .entry_at_revision_in_write(
                txn,
                &request.context.tenant_id,
                &request.entry.agent_id,
                expected_revision,
            )?
            .map(|entry| entry.updated_at_ms)
            .unwrap_or(0)
    };
    let mut write_context = replay_context.clone();
    write_context.created_at_ms =
        crate::server::dispatch::authoritative_now_ms().max(previous_updated_at_ms);
    let entry = match store.entry_at_revision_in_write(
        txn,
        &request.context.tenant_id,
        &request.entry.agent_id,
        next_revision,
    )? {
        Some(existing) => {
            if existing.as_draft() != request.entry {
                return Err(
                    "IDEMPOTENCY_CONFLICT: target Agent Library revision has different definition"
                        .to_string(),
                );
            }
            existing
        }
        None => AgentLibraryEntry::publish(
            request.entry.clone(),
            next_revision,
            write_context.created_at_ms,
        )?,
    };
    Ok((write_context, entry))
}

fn prepare_retire_entry(
    store: &AgentLibraryStore,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    request: &AgentLibraryRetireRequest,
    expected_revision: u64,
    next_revision: u64,
    replay_context: &AgentLibraryMutationContext,
) -> Result<(AgentLibraryMutationContext, AgentLibraryEntry), String> {
    let retained = store
        .entry_at_revision_in_write(
            txn,
            &request.context.tenant_id,
            &request.agent_id,
            expected_revision,
        )?
        .ok_or_else(|| "agent library entry does not exist".to_string())?;
    if retained.is_retired() {
        return Err("agent library entry is already retired".to_string());
    }
    let mut write_context = replay_context.clone();
    write_context.created_at_ms =
        crate::server::dispatch::authoritative_now_ms().max(retained.updated_at_ms);
    let entry = match store.entry_at_revision_in_write(
        txn,
        &request.context.tenant_id,
        &request.agent_id,
        next_revision,
    )? {
        Some(existing) if existing.is_retired() && existing.as_draft() == retained.as_draft() => {
            existing
        }
        Some(_) => {
            return Err(
                "IDEMPOTENCY_CONFLICT: target Agent Library revision has different lifecycle"
                    .to_string(),
            );
        }
        None => retained.retire(next_revision, write_context.created_at_ms)?,
    };
    Ok((write_context, entry))
}

struct EntryCommitPlan {
    expected_revision: u64,
    admitted_context: AgentLibraryMutationContext,
    entry: AgentLibraryEntry,
    entry_bytes: Vec<u8>,
    event: AgentLibraryOutboxEvent,
    event_bytes: Vec<u8>,
    batch: MutationBatch,
    source_version: u64,
    committed_version: u64,
    stable_result: AgentLibraryCommittedResult,
    result_bytes: Vec<u8>,
}

fn build_entry_commit_plan(
    store: &AgentLibraryStore,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    commit: AgentLibraryRevisionCommit,
    replay: &ReplayIdentity<'_>,
) -> Result<EntryCommitPlan, String> {
    let AgentLibraryRevisionCommit {
        expected_revision,
        kind,
        entry,
    } = commit;
    let operations = agent_library_operations(kind, &entry);
    let policy_digest = effective_agent_library_policy_digest(&operations)?;
    let mut admitted_context = context.clone();
    admitted_context.policy_digest = format!("sha256:{}", policy_digest.to_hex());
    let event = AgentLibraryOutboxEvent::new(kind, entry.clone(), &admitted_context)?;
    let event_bytes = eg_storage::encode_bounded(&event, "agent library outbox event")?;
    let entry_bytes = eg_storage::encode_bounded(&entry, "agent library revision")?;
    let batch_id = batch_id(&admitted_context.idempotency_key)?;
    let authoritative_version = store.mutations.current_version(txn, owner)?;
    let batch = build_batch(
        owner,
        &admitted_context,
        kind,
        &entry,
        authoritative_version,
        &batch_id,
        event_bytes.clone(),
    )?;
    let source_version = begin_entry_commit(txn, &batch, replay, authoritative_version)?;
    let committed_version = source_version
        .checked_add(1)
        .ok_or_else(|| "agent library committed version overflow".to_string())?;
    let stable_result =
        AgentLibraryCommittedResult::new(entry.clone(), batch_id, committed_version)?;
    let result_bytes = encode_domain_result(&stable_result)?;
    Ok(EntryCommitPlan {
        expected_revision,
        admitted_context,
        entry,
        entry_bytes,
        event,
        event_bytes,
        batch,
        source_version,
        committed_version,
        stable_result,
        result_bytes,
    })
}

fn begin_entry_commit(
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    batch: &MutationBatch,
    replay: &ReplayIdentity<'_>,
    authoritative_version: u64,
) -> Result<u64, String> {
    let begun = txn.begin_with_replay_identity(batch, replay.operation, replay.nonce)?;
    let source_version = match begun {
        Begin::Apply {
            source_version: Some(source_version),
        } => source_version,
        Begin::Apply {
            source_version: None,
        } => return Err("agent library admission has no native source version".to_string()),
        Begin::Replay(_) => {
            return Err(
                "CORRUPT_MUTATION_LEDGER: replay became visible after a fresh admission decision"
                    .to_string(),
            );
        }
    };
    if source_version != authoritative_version {
        return Err("agent library source version changed while admitting write".to_string());
    }
    Ok(source_version)
}

fn build_entry_receipt(
    plan: &EntryCommitPlan,
    operation: &eg_types::authority::OperationReplayIdentity,
    nonce: &eg_types::authority::NonceReplayKey,
) -> Result<MutationReceipt, String> {
    let key = format!(
        "{}:{}:{}",
        plan.entry.tenant_id, plan.entry.agent_id, plan.entry.entry_revision
    );
    let headers = expected_headers_from_event(&plan.event);
    owner_receipt(
        operation,
        nonce,
        &plan.batch,
        OwnerReceiptInput {
            slug: "agent-library",
            topic: AGENT_LIBRARY_OUTBOX_TOPIC,
            key: &key,
            event_bytes: &plan.event_bytes,
            headers: &headers,
            mutation_result: domain_result_for(&plan.stable_result)?,
            committed_version: plan.committed_version,
            committed_at_ms: plan.admitted_context.created_at_ms,
        },
    )
}

fn finish_entry_commit(
    mutations: &MutationKernel,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    plan: &EntryCommitPlan,
    operation: &eg_types::authority::OperationReplayIdentity,
    nonce: &eg_types::authority::NonceReplayKey,
    receipt: &MutationReceipt,
) -> Result<eg_types::MutationBatchRecord, String> {
    let owner_write = txn.owner_rows(owner, &plan.batch)?;
    apply_entry_rows(
        &owner_write,
        &plan.admitted_context,
        plan.expected_revision,
        &plan.entry,
        plan.entry_bytes.as_slice(),
    )?;
    owner_write.finish_owner()?;
    mutations.finish_with_replay(
        txn,
        &plan.batch,
        Some(plan.result_bytes.clone()),
        plan.admitted_context.created_at_ms,
        Some(plan.source_version),
        (operation, nonce, receipt),
    )
}

fn validate_entry_commit_version(
    record: &eg_types::MutationBatchRecord,
    expected_version: u64,
) -> Result<(), String> {
    let recorded_version = record
        .committed_version
        .target()
        .ok_or_else(|| "agent library commit has no target version".to_string())?;
    if recorded_version != expected_version {
        return Err("agent library result version differs from the committed version".to_string());
    }
    Ok(())
}

impl AgentLibraryStore {
    fn entry_at_revision_in_write(
        &self,
        write: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        tenant_id: &str,
        agent_id: &str,
        revision: u64,
    ) -> Result<Option<AgentLibraryEntry>, String> {
        if revision == 0 {
            return Ok(None);
        }
        let revisions = write.open_read_table(eg_storage::AGENT_LIBRARY_REVISIONS)?;
        let Some(value) = revisions.get((tenant_id, agent_id, revision))? else {
            return Ok(None);
        };
        let entry = decode_entry(value.value())?;
        validate_entry_key(&entry, tenant_id, agent_id, revision)?;
        Ok(Some(entry))
    }

    /// Resolve every cross-record reference ONE AGENT DRAFT makes.
    ///
    /// L2 -> L1: every component the agent is assembled from, plus the values a
    /// template instantiation bound into it. Both are pins, and a pin nothing
    /// resolves is a claim nothing checks whichever list it sits in -- but they
    /// are gathered here rather than folded into `dependencies()`, which
    /// answers "what is this agent ASSEMBLED from" and must not start answering
    /// a different question.
    ///
    /// L2 -> TEMPLATE: `instantiated_from` is what makes "which agents came
    /// from this template?" a traversal rather than a guess. Unresolved it was
    /// a free-text provenance claim that `definition_digest` then attested to.
    ///
    /// # Why an agent DRAFT rather than an Agent Library publish
    ///
    /// Two admissions put an `AgentLibraryEntryDraft` on durable storage: this
    /// layer's own publish, and a TEMPLATE publish, whose `base` is one. They
    /// ask the identical question of it, so they ask it in one place -- `subject`
    /// is the only thing that differs, and only so a refusal names the record
    /// the caller was actually publishing.
    ///
    /// Sharing it also keeps the template side honest as the contract moves.
    /// `AgentTemplateDraft::validate` refuses a `base` that is itself a
    /// template instance, so today a base carries no `instantiated_from` and
    /// the template and binding halves below are unreachable from that caller.
    /// A resolver written to today's reachability -- just
    /// `base.dependencies()` -- would silently stop covering the base the day
    /// that rule relaxed. This one would not.
    pub(crate) fn admit_entry_references_in_write(
        &self,
        write: &super::super::agent_pin_resolution::Write<'_>,
        tenant_id: &str,
        subject: &str,
        entry: &AgentLibraryEntryDraft,
    ) -> Result<(), String> {
        let mut components = entry.dependencies();
        components.extend(
            entry
                .instantiated_from
                .iter()
                .flat_map(|instance| instance.bindings.values()),
        );
        self.resolve_component_pins_in_write(write, tenant_id, subject, &components)?;
        let templates: Vec<super::super::agent_pin_resolution::TemplatePin<'_>> = entry
            .instantiated_from
            .iter()
            .map(|instance| super::super::agent_pin_resolution::TemplatePin {
                template_id: &instance.template_id,
                definition_digest: &instance.definition_digest,
                entry_revision: Some(instance.entry_revision),
            })
            .collect();
        super::super::agent_pin_resolution::resolve_template_pins_in_write(
            write, tenant_id, subject, &templates,
        )
    }

    /// Publish a new immutable Agent Library definition revision.
    pub fn publish(
        &self,
        request: AgentLibraryPublishRequest,
    ) -> Result<AgentLibraryWriteResult, String> {
        validate_context(self, &request.context)?;
        request.entry.validate()?;
        validate_context_matches_draft(&request.context, &request.entry)?;
        let expected_revision = request.context.expected_revision.ok_or_else(|| {
            "agent library writes require an explicit expected_revision".to_string()
        })?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;
        let prepared = match prepare_publish(
            self,
            &self.mutations,
            &txn,
            &owner,
            &request,
            expected_revision,
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        match prepared {
            PreparedLibraryWrite::Replay {
                operation,
                nonce,
                result,
                receipt,
            } => finish_replayed(&self.mutations, txn, &operation, &nonce, *result, *receipt),
            PreparedLibraryWrite::Fresh {
                operation,
                nonce,
                context,
                entry,
            } => self.commit_entry_in_write(
                txn,
                &owner,
                &context,
                AgentLibraryRevisionCommit {
                    expected_revision,
                    kind: AgentLibraryMutationKind::Publish,
                    entry: *entry,
                },
                ReplayIdentity {
                    operation: &operation,
                    nonce: &nonce,
                },
            ),
        }
    }

    /// Retire the current definition with a durable tombstone revision.
    pub fn retire(
        &self,
        request: AgentLibraryRetireRequest,
    ) -> Result<AgentLibraryWriteResult, String> {
        validate_context(self, &request.context)?;
        eg_types::agent_library::validate_key(&request.context.tenant_id, &request.agent_id)?;
        let expected_revision = request.context.expected_revision.ok_or_else(|| {
            "agent library writes require an explicit expected_revision".to_string()
        })?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;
        let prepared = match prepare_retire(
            self,
            &self.mutations,
            &txn,
            &owner,
            &request,
            expected_revision,
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        match prepared {
            PreparedLibraryWrite::Replay {
                operation,
                nonce,
                result,
                receipt,
            } => finish_replayed(&self.mutations, txn, &operation, &nonce, *result, *receipt),
            PreparedLibraryWrite::Fresh {
                operation,
                nonce,
                context,
                entry,
            } => self.commit_entry_in_write(
                txn,
                &owner,
                &context,
                AgentLibraryRevisionCommit {
                    expected_revision,
                    kind: AgentLibraryMutationKind::Retire,
                    entry: *entry,
                },
                ReplayIdentity {
                    operation: &operation,
                    nonce: &nonce,
                },
            ),
        }
    }

    fn commit_entry_in_write(
        &self,
        txn: eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
        context: &AgentLibraryMutationContext,
        commit: AgentLibraryRevisionCommit,
        replay: ReplayIdentity<'_>,
    ) -> Result<AgentLibraryWriteResult, String> {
        let replay_operation = replay.operation;
        let replay_nonce = replay.nonce;
        let plan = match build_entry_commit_plan(self, &txn, owner, context, commit, &replay) {
            Ok(plan) => plan,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let receipt = match build_entry_receipt(&plan, replay_operation, replay_nonce) {
            Ok(receipt) => receipt,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let record = match finish_entry_commit(
            &self.mutations,
            &txn,
            owner,
            &plan,
            replay_operation,
            replay_nonce,
            &receipt,
        ) {
            Ok(record) => record,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        if let Err(error) = validate_entry_commit_version(&record, plan.committed_version) {
            txn.abort()?;
            return Err(error);
        }
        self.mutations.commit(txn, &plan.batch)?;
        Ok(plan.stable_result.response(false))
    }
}
