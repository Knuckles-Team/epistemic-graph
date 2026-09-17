//! Agent Library publish/retire transaction phases.
//!
//! The admitted-write steps that are identical across the agent layers --
//! replay prelude, begin, owner rows, ledger finish and version check -- are the
//! shared ones in [`super::super::agent_revision`]. What stays here is what the
//! Agent Library does differently: its replay check binds the submitted draft
//! and the recorded outbox effect, it stamps an authoritative commit time, and
//! it builds its receipt before the owner rows are written.

use super::batch::build_batch;
use super::replay::{expected_headers_from_event, replayed_receipt, ExpectedAgentLibraryMutation};
use super::*;
use crate::server::persistence::agent_revision::{
    apply_owner_rows, finish_committed_ledger, finish_replayed, policy_admitted_context,
    revision_scope, stage_batch, within_write, RevisionVerb, StagedBatch,
};

type Write<'a> = eg_transaction::AdmittedMutation<'a, eg_storage::AgentLibraryOwner>;

/// How refusals shared with the other layers name this one.
const LIBRARY_NOUN: &str = "agent library";

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

/// One publish or retire as the caller asked for it.
#[derive(Clone, Copy)]
enum LibraryRequest<'r> {
    Publish(&'r AgentLibraryPublishRequest),
    Retire(&'r AgentLibraryRetireRequest),
}

impl<'r> LibraryRequest<'r> {
    fn context(self) -> &'r AgentLibraryMutationContext {
        match self {
            Self::Publish(request) => &request.context,
            Self::Retire(request) => &request.context,
        }
    }

    /// What the caller believes its idempotency key commits.
    fn expected(self) -> ExpectedAgentLibraryMutation<'r> {
        match self {
            Self::Publish(request) => ExpectedAgentLibraryMutation {
                kind: AgentLibraryMutationKind::Publish,
                agent_id: &request.entry.agent_id,
                draft: Some(&request.entry),
            },
            Self::Retire(request) => ExpectedAgentLibraryMutation {
                kind: AgentLibraryMutationKind::Retire,
                agent_id: &request.agent_id,
                draft: None,
            },
        }
    }
}

pub(super) fn library_verb(kind: AgentLibraryMutationKind) -> RevisionVerb {
    match kind {
        AgentLibraryMutationKind::Publish => RevisionVerb::Publish,
        AgentLibraryMutationKind::Retire => RevisionVerb::Retire,
    }
}

fn prepare_library_write(
    store: &AgentLibraryStore,
    txn: &Write<'_>,
    owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    request: LibraryRequest<'_>,
    expected_revision: u64,
) -> Result<PreparedLibraryWrite, String> {
    let mutations = &store.mutations;
    let expected = request.expected();
    let nonce = resolve_nonce_first(mutations, txn, request.context())?;
    let next_revision = next_revision(expected_revision)?;
    let definition_digest = expected
        .draft
        .map(|draft| {
            AgentLibraryEntry::publish(draft.clone(), next_revision, 0)
                .map(|entry| entry.definition_digest)
        })
        .transpose()?;
    let verb = library_verb(expected.kind).as_str();
    let replay_context = admitted_context(request.context(), &format!("agent-library:{verb}"))?;
    let operation = agent_library_operation_identity(
        owner,
        &replay_context,
        verb,
        expected.agent_id,
        expected_revision,
        definition_digest.as_deref(),
    )?;
    let replay = mutations.resolve_replay(txn, &operation, &nonce)?;
    if let Some((result, receipt)) = replayed_receipt(
        mutations,
        txn,
        replay,
        &operation,
        &replay_context,
        expected,
    )? {
        return Ok(PreparedLibraryWrite::Replay {
            operation,
            nonce,
            result: Box::new(result),
            receipt: Box::new(receipt),
        });
    }
    let (context, entry) = match request {
        LibraryRequest::Publish(publish) => prepare_publish_entry(
            store,
            txn,
            publish,
            expected_revision,
            next_revision,
            &replay_context,
        )?,
        LibraryRequest::Retire(retire) => prepare_retire_entry(
            store,
            txn,
            retire,
            expected_revision,
            next_revision,
            &replay_context,
        )?,
    };
    Ok(PreparedLibraryWrite::Fresh {
        operation,
        nonce,
        context: Box::new(context),
        entry: Box::new(entry),
    })
}

/// Admit and commit one publish or retire. The write is opened here, once, for
/// both.
fn write_library_entry(
    store: &AgentLibraryStore,
    request: LibraryRequest<'_>,
) -> Result<AgentLibraryWriteResult, String> {
    let (expected_revision, owner) = revision_scope(store, request.context(), LIBRARY_NOUN)?;
    let txn = store.mutations.open_write(&owner)?;
    let (txn, prepared) = within_write(txn, |txn| {
        prepare_library_write(store, txn, &owner, request, expected_revision)
    })?;
    match prepared {
        PreparedLibraryWrite::Replay {
            operation,
            nonce,
            result,
            receipt,
        } => finish_replayed(&store.mutations, txn, &operation, &nonce, *result, *receipt),
        PreparedLibraryWrite::Fresh {
            operation,
            nonce,
            context,
            entry,
        } => commit_entry_in_write(
            store,
            txn,
            &owner,
            &context,
            AgentLibraryRevisionCommit {
                expected_revision,
                kind: request.expected().kind,
                entry: *entry,
            },
            ReplayIdentity {
                operation: &operation,
                nonce: &nonce,
            },
        ),
    }
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
    staged: StagedBatch,
    stable_result: AgentLibraryCommittedResult,
    result_bytes: Vec<u8>,
}

fn build_entry_commit_plan(
    store: &AgentLibraryStore,
    txn: &Write<'_>,
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
    let admitted_context =
        policy_admitted_context(context, &agent_library_operations(kind, &entry))?;
    let event = AgentLibraryOutboxEvent::new(kind, entry.clone(), &admitted_context)?;
    let event_bytes = eg_storage::encode_bounded(&event, "agent library outbox event")?;
    let entry_bytes = eg_storage::encode_bounded(&entry, "agent library revision")?;
    let (batch_id, staged) = stage_batch(
        store,
        txn,
        owner,
        &admitted_context,
        (replay.operation, replay.nonce),
        LIBRARY_NOUN,
        |version, batch_id| {
            build_batch(
                owner,
                &admitted_context,
                kind,
                &entry,
                version,
                batch_id,
                event_bytes.clone(),
            )
        },
    )?;
    let stable_result =
        AgentLibraryCommittedResult::new(entry.clone(), batch_id, staged.committed_version)?;
    let result_bytes = encode_domain_result(&stable_result)?;
    Ok(EntryCommitPlan {
        expected_revision,
        admitted_context,
        entry,
        entry_bytes,
        event,
        event_bytes,
        staged,
        stable_result,
        result_bytes,
    })
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
        &plan.staged.batch,
        OwnerReceiptInput {
            slug: "agent-library",
            topic: AGENT_LIBRARY_OUTBOX_TOPIC,
            key: &key,
            event_bytes: &plan.event_bytes,
            headers: &headers,
            mutation_result: domain_result_for(&plan.stable_result)?,
            committed_version: plan.staged.committed_version,
            committed_at_ms: plan.admitted_context.created_at_ms,
        },
    )
}

/// Commit one fresh revision. Unlike the other layers, the receipt is built
/// before the owner rows are written.
fn commit_entry_in_write(
    store: &AgentLibraryStore,
    txn: Write<'_>,
    owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    commit: AgentLibraryRevisionCommit,
    replay: ReplayIdentity<'_>,
) -> Result<AgentLibraryWriteResult, String> {
    let (txn, plan) = within_write(txn, |txn| {
        let plan = build_entry_commit_plan(store, txn, owner, context, commit, &replay)?;
        let receipt = build_entry_receipt(&plan, replay.operation, replay.nonce)?;
        apply_owner_rows(txn, owner, &plan.staged.batch, |owner_write| {
            apply_entry_rows(
                owner_write,
                &plan.admitted_context,
                plan.expected_revision,
                &plan.entry,
                plan.entry_bytes.as_slice(),
            )
        })?;
        finish_committed_ledger(
            &store.mutations,
            txn,
            (&plan.staged, &plan.result_bytes),
            plan.admitted_context.created_at_ms,
            (replay.operation, replay.nonce, &receipt),
            LIBRARY_NOUN,
        )?;
        Ok(plan)
    })?;
    store.mutations.commit(txn, &plan.staged.batch)?;
    Ok(plan.stable_result.response(false))
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
        write_library_entry(self, LibraryRequest::Publish(&request))
    }

    /// Retire the current definition with a durable tombstone revision.
    pub fn retire(
        &self,
        request: AgentLibraryRetireRequest,
    ) -> Result<AgentLibraryWriteResult, String> {
        validate_context(self, &request.context)?;
        eg_types::agent_library::validate_key(&request.context.tenant_id, &request.agent_id)?;
        write_library_entry(self, LibraryRequest::Retire(&request))
    }
}
